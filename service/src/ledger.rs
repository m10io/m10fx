use crate::config::LiquidityConfig;
use crate::event::{Event, Execute, Quote, Request};
use crate::LedgerDB;
use futures_util::StreamExt;
use m10_sdk::account::AccountId;
use m10_sdk::client::Channel;
use m10_sdk::sdk::transaction_data::Data;
use m10_sdk::sdk::{
    CreateTransfer, GetAccountRequest, IndexedAccount, InvokeAction, ObserveAccountsRequest,
    ObserveActionsRequest, Target, TransferStep,
};
use m10_sdk::{sdk, Ed25519, LedgerClient, Signer, TransactionExt};
use rust_decimal::Decimal;
use service::{FX_SWAP_ACTION, FX_SWAP_METADATA};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tracing::{error, info, info_span, Instrument};

#[derive(Clone)]
pub struct Ledger {
    currency: String,
    signer: Arc<Ed25519>,
    client: LedgerClient,
    liquidity: AccountId,
    base_rate: Decimal,
}

impl Ledger {
    pub fn new(address: String, currency: String, config: LiquidityConfig) -> anyhow::Result<Self> {
        let channel = Channel::from_shared(address)?
            .keep_alive_while_idle(true)
            .http2_keep_alive_interval(Duration::from_secs(30))
            .timeout(Duration::from_secs(30))
            .connect_lazy()?;
        let client = LedgerClient::new(channel);
        let signer = Arc::new(Ed25519::load_key_pair(
            config
                .key_pair
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("Invalid key path"))?,
        )?);
        Ok(Self {
            currency: currency.to_lowercase(),
            client,
            signer,
            liquidity: AccountId::try_from_be_slice(&hex::decode(config.account)?)?,
            base_rate: config.base_rate,
        })
    }

    async fn publish_quote(&self, context_id: Vec<u8>, quote: Quote) -> anyhow::Result<()> {
        info!(?quote, "Publishing quote");
        let mut client = self.client.clone();
        let req = LedgerClient::transaction_request(
            sdk::InvokeAction {
                name: FX_SWAP_ACTION.to_string(),
                from_account: self.liquidity.to_be_bytes().to_vec(),
                target: Some(Target {
                    target: Some(sdk::target::Target::AccountId(
                        quote.request.from.to_be_bytes().to_vec(),
                    )),
                }),
                payload: serde_json::to_vec(&Event::Quote(quote))?,
            },
            context_id,
        );
        let signed = self.signer.sign_request(req).await?;
        client.create_transaction(signed).await?.tx_error()?;
        Ok(())
    }

    pub async fn get_account(&self, account_id: AccountId) -> anyhow::Result<IndexedAccount> {
        let req = self
            .signer
            .sign_request(GetAccountRequest {
                id: account_id.to_be_bytes().to_vec(),
            })
            .await?;
        let account = self.client.clone().get_indexed_account(req).await?;
        Ok(account)
    }

    pub async fn observe_transfer(self, db: LedgerDB) -> anyhow::Result<()> {
        // Sign the request to observe all actions named `FX_SWAP_ACTION`
        let req = self
            .signer
            .sign_request(ObserveAccountsRequest {
                starting_from: None,
                involved_accounts: vec![self.liquidity.to_be_bytes().to_vec()],
            })
            .await?;
        info!("Observing transfers");
        let mut actions = self.client.observe_transfers(req).await.map_err(|err| {
            error!(%err);
            err
        })?;

        info!("Started observations");
        while let Some(msg) = actions.next().await {
            let sdk::FinalizedTransactions { transactions } = msg?;
            for txn in transactions {
                if let Some(Data::Transfer(transfer)) = txn.data() {
                    if let Some(metadata) = transfer
                        .transfer_steps
                        .iter()
                        .flat_map(|step| &step.metadata)
                        .find(|meta| meta.type_url == FX_SWAP_METADATA)
                    {
                        if let Ok(event) = serde_json::from_slice::<Event>(&metadata.value) {
                            info!(?event);
                            if let Event::Execute(execute) = event {
                                launch_swap_task(
                                    self.clone(),
                                    db.clone(),
                                    execute,
                                    txn.request.map(|t| t.context_id).unwrap_or_default(),
                                )
                                .await;
                            } else {
                                error!("invalid event type");
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn observe_requests(mut self, db: LedgerDB) -> anyhow::Result<()> {
        // Sign the request to observe all actions named `FX_SWAP_ACTION`
        let req = self
            .signer
            .sign_request(ObserveActionsRequest {
                starting_from: None,
                name: FX_SWAP_ACTION.to_string(),
                involves_accounts: vec![self.liquidity.to_be_bytes().to_vec()],
            })
            .await?;
        info!("Observing actions");
        let mut actions = self.client.observe_actions(req).await.map_err(|err| {
            error!(%err);
            err
        })?;

        info!(action = %FX_SWAP_ACTION, "Started observations");
        while let Some(msg) = actions.next().await {
            let sdk::FinalizedTransactions { transactions } = msg?;
            for txn in transactions {
                if let Some(Data::InvokeAction(action)) = txn.data() {
                    if let Ok(event) = serde_json::from_slice::<Event>(&action.payload) {
                        info!(?event);
                        match event {
                            Event::Request(request) => {
                                if let Ok((from_currency, to_currency)) =
                                    get_currencies(&mut self, &request).await
                                {
                                    if from_currency != self.currency {
                                        continue;
                                    }
                                    if let Ok(rate) =
                                        get_fx_rate(&db, &from_currency, &to_currency).await
                                    {
                                        let intermediary =
                                            db.get(&from_currency).unwrap().liquidity;
                                        if let Err(err) = self
                                            .publish_quote(
                                                txn.request
                                                    .map(|req| req.context_id)
                                                    .unwrap_or_default(),
                                                Quote {
                                                    request,
                                                    rate,
                                                    intermediary,
                                                },
                                            )
                                            .await
                                        {
                                            error!(%err, "Could not publish quote");
                                        }
                                    }
                                }
                            }
                            Event::Quote(_) | Event::Execute(_) | Event::Completed => {}
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

async fn get_currencies(
    ledger: &mut Ledger,
    request: &Request,
) -> anyhow::Result<(String, String)> {
    let from = ledger.get_account(request.from).await?;
    let from_currency = from.instrument.unwrap().code.to_lowercase();
    let to = ledger.get_account(request.to).await?;
    let to_currency = to.instrument.unwrap().code.to_lowercase();
    Ok((from_currency, to_currency))
}

async fn get_fx_rate(
    db: &LedgerDB,
    from_currency: &str,
    to_currency: &str,
) -> anyhow::Result<Decimal> {
    info!("Getting Fx rate");
    let from_ledger = db
        .get(from_currency)
        .ok_or_else(|| anyhow::anyhow!("Missing ledger for currency {}", from_currency))?;
    let to_ledger = db
        .get(to_currency)
        .ok_or_else(|| anyhow::anyhow!("Missing ledger for currency {}", to_currency))?;

    Ok(to_ledger.base_rate / from_ledger.base_rate)
}

async fn launch_swap_task(mut ledger: Ledger, db: LedgerDB, execute: Execute, context_id: Vec<u8>) {
    let mut interval = tokio::time::interval(Duration::from_secs(10));
    let valid_until = SystemTime::UNIX_EPOCH + Duration::from_secs(execute.valid_until);
    let limits = execute.lower_limits..execute.upper_limit;
    let from = execute.request.from;
    let to = execute.request.to;
    let (from_currency, to_currency) = get_currencies(&mut ledger, &execute.request)
        .await
        .expect("Could not find FX rate");
    let mut to_ledger = db.get(&to_currency).unwrap().clone();
    tokio::spawn(
        async move {
            info!("Start");
            loop {
                info!("Polling");
                if let Ok(rate) = get_fx_rate(&db, &from_currency, &to_currency).await {
                    let limits_exceeded = !limits.contains(&rate);
                    let time_exceeded = SystemTime::now() > valid_until;
                    if limits_exceeded || time_exceeded {
                        let amount = (execute.request.amount * rate)
                            .try_into()
                            .expect("invalid amount");
                        info!("Executing swap");
                        let req = to_ledger
                            .signer
                            .sign_request(LedgerClient::transaction_request(
                                CreateTransfer {
                                    transfer_steps: vec![TransferStep {
                                        from_account_id: to_ledger.liquidity.to_be_bytes().to_vec(),
                                        to_account_id: to.to_be_bytes().to_vec(),
                                        amount,
                                        metadata: vec![],
                                    }],
                                },
                                context_id.clone(),
                            ))
                            .await
                            .unwrap();
                        to_ledger
                            .client
                            .create_transaction(req)
                            .await
                            .expect("Could not execute transfer")
                            .tx_error()
                            .expect("transaction error");

                        info!("Publishing completion");
                        let req = to_ledger
                            .signer
                            .sign_request(LedgerClient::transaction_request(
                                InvokeAction {
                                    name: FX_SWAP_ACTION.to_string(),
                                    from_account: to_ledger.liquidity.to_be_bytes().to_vec(),
                                    target: Some(Target {
                                        target: Some(sdk::target::Target::AccountId(
                                            from.to_be_bytes().to_vec(),
                                        )),
                                    }),
                                    payload: serde_json::to_vec(&Event::Completed).unwrap(),
                                },
                                context_id,
                            ))
                            .await
                            .unwrap();
                        to_ledger
                            .client
                            .create_transaction(req)
                            .await
                            .expect("Could not publish completion")
                            .tx_error()
                            .expect("transaction error");
                        break;
                    }
                }
                interval.tick().await;
            }

            info!("Done");
        }
        .instrument(info_span!("swap", %from, %to)),
    );
}
