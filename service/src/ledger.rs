use crate::config::LiquidityConfig;
use crate::event::{Event, Quote, Request};
use crate::LedgerDB;
use futures_util::StreamExt;
use m10_fx::FX_SWAP_ACTION;
use m10_sdk::account::AccountId;
use m10_sdk::client::Channel;
use m10_sdk::sdk::transaction_data::Data;
use m10_sdk::sdk::{GetAccountRequest, IndexedAccount, ObserveActionsRequest, Target};
use m10_sdk::{sdk, Ed25519, LedgerClient, Signer, TransactionExt};
use rust_decimal::Decimal;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info};

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

    pub async fn observe_requests(self, db: LedgerDB) -> anyhow::Result<()> {
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
                                match get_fx_rate(&self.currency, &db, &request).await {
                                    Ok(Some(rate)) => {
                                        if let Err(err) = self
                                            .publish_quote(
                                                txn.request
                                                    .map(|req| req.context_id)
                                                    .unwrap_or_default(),
                                                Quote { request, rate },
                                            )
                                            .await
                                        {
                                            error!(%err, "Could not publish quote");
                                        }
                                    }
                                    Ok(None) => {}
                                    Err(err) => error!(%err, "Could not retrieve quote"),
                                }
                            }
                            Event::Quote(_) => {}
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

async fn get_fx_rate(
    my_currency: &str,
    db: &LedgerDB,
    request: &Request,
) -> anyhow::Result<Option<Decimal>> {
    info!("Getting rate");
    let ledger = db.iter().next().unwrap().1;

    info!("Getting from account");
    let from = ledger.get_account(request.from).await?;
    let from_currency = &from.instrument.unwrap().code.to_lowercase();
    if from_currency != my_currency {
        return Ok(None);
    }

    info!("Getting to account");
    let to = ledger.get_account(request.to).await?;
    let to_currency = &to.instrument.unwrap().code.to_lowercase();
    let from_ledger = db
        .get(from_currency)
        .ok_or_else(|| anyhow::anyhow!("Missing ledger for currency {}", from_currency))?;
    let to_ledger = db
        .get(to_currency)
        .ok_or_else(|| anyhow::anyhow!("Missing ledger for currency {}", to_currency))?;

    Ok(Some(to_ledger.base_rate / from_ledger.base_rate))
}
