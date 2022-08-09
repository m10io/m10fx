use crate::config::LiquidityConfig;
use crate::event::{Event, Execute, Quote, Request};
use crate::LedgerDB;
use futures_util::StreamExt;
use m10_sdk::account::AccountId;
use m10_sdk::client::Channel;
use m10_sdk::{
    AccountFilter, Action, ActionBuilder, Ed25519, M10Client, MetadataExt, StepBuilder, Transfer,
    TransferBuilder, WithContext,
};
use rust_decimal::Decimal;
use service::{FxSwapMetadata, FX_SWAP_ACTION};
use std::time::{Duration, SystemTime};
use tracing::{error, info, info_span, Instrument};

#[derive(Clone)]
pub struct Ledger {
    currency: String,
    client: M10Client<Ed25519>,
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
        let signer = Ed25519::load_key_pair(
            config
                .key_pair
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("Invalid key path"))?,
        )?;
        let client = M10Client::new(signer, channel);

        Ok(Self {
            currency: currency.to_lowercase(),
            client,
            liquidity: AccountId::try_from_be_slice(&hex::decode(&config.account)?)?,
            base_rate: config.base_rate,
        })
    }

    async fn get_currencies(&self, request: &Request) -> anyhow::Result<(String, String)> {
        let from = self.client.get_account_info(request.from).await?;
        let to = self.client.get_account_info(request.to).await?;
        Ok((from.code.to_lowercase(), to.code.to_lowercase()))
    }

    pub async fn observe_transfers(self, db: LedgerDB) -> anyhow::Result<()> {
        // Sign the request to observe all transfer from & to the liquidity account
        let mut transfers = self
            .client
            .observe_transfers(AccountFilter::default().involves(self.liquidity))
            .await?;
        info!("Observing transfers");

        while let Some(Ok(transfers)) = transfers.next().await {
            for transfer in transfers {
                if let Err(err) = self.handle_transfer(db.clone(), transfer).await {
                    error!(%err);
                }
            }
        }
        Ok(())
    }

    async fn handle_transfer(&self, ledger: LedgerDB, transfer: Transfer) -> anyhow::Result<()> {
        if let Some(payload) = transfer.with_type::<FxSwapMetadata>() {
            let event = serde_json::from_slice::<Event>(payload)?;
            info!(?event);
            if let Event::Execute(execute) = event {
                let from = execute.request.from;
                let to = execute.request.to;
                let this = self.clone();
                tokio::spawn(
                    async move {
                        info!("Start");
                        if let Err(err) =
                            swap_task(this, ledger, execute, transfer.context_id).await
                        {
                            error!(%err);
                        }
                        info!("Done");
                    }
                    .instrument(info_span!("swap", %from, %to)),
                );
            } else {
                error!("invalid event type");
            }
        }
        Ok(())
    }

    async fn handle_request(&self, db: &LedgerDB, action: Action) -> anyhow::Result<()> {
        let event = serde_json::from_slice::<Event>(&action.payload)?;
        info!(?event);
        let request = match event {
            Event::Request(request) => request,
            Event::Quote(_) | Event::Execute(_) | Event::Completed => return Ok(()),
        };
        let (from_currency, to_currency) = self.get_currencies(&request).await?;
        if from_currency != self.currency {
            return Ok(());
        }
        let rate = get_fx_rate(db, &from_currency, &to_currency).await?;
        let intermediary = db
            .get(&from_currency)
            .ok_or_else(|| anyhow::anyhow!("Missing currency"))?
            .liquidity;
        let quote = Quote {
            request,
            rate,
            intermediary,
        };
        info!(?quote, "Publishing quote");
        self.client
            .action(
                ActionBuilder::for_account(
                    FX_SWAP_ACTION.to_string(),
                    self.liquidity,
                    quote.request.from,
                )
                .payload(serde_json::to_vec(&Event::Quote(quote))?),
                action.context_id,
            )
            .await?;
        Ok(())
    }

    pub async fn observe_actions(self, db: LedgerDB) -> anyhow::Result<()> {
        // Sign the request to observe all actions named `FX_SWAP_ACTION`
        let mut actions = self
            .client
            .observe_actions(AccountFilter::name(FX_SWAP_ACTION).involves(self.liquidity))
            .await?;
        info!(action = %FX_SWAP_ACTION, "Started observations");
        while let Some(Ok(actions)) = actions.next().await {
            for action in actions {
                if let Err(err) = self.handle_request(&db, action).await {
                    error!(%err);
                }
            }
        }
        Ok(())
    }
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

async fn swap_task(
    ledger: Ledger,
    db: LedgerDB,
    execute: Execute,
    context_id: Vec<u8>,
) -> anyhow::Result<()> {
    let mut interval = tokio::time::interval(Duration::from_secs(10));
    let valid_until = SystemTime::UNIX_EPOCH + Duration::from_secs(execute.valid_until);
    let limits = execute.lower_limits..execute.upper_limit;
    let (from_currency, to_currency) = ledger.get_currencies(&execute.request).await?;
    let to_ledger = db
        .get(&to_currency)
        .ok_or_else(|| anyhow::anyhow!("Missing currency"))?
        .clone();

    loop {
        info!("Polling");
        if let Ok(rate) = get_fx_rate(&db, &from_currency, &to_currency).await {
            let limits_exceeded = !limits.contains(&rate);
            let time_exceeded = SystemTime::now() > valid_until;
            if limits_exceeded || time_exceeded {
                let amount = (execute.request.amount * rate).try_into()?;
                info!("Executing swap");
                to_ledger
                    .client
                    .transfer(
                        TransferBuilder::new()
                            .step(StepBuilder::new(
                                to_ledger.liquidity,
                                execute.request.to,
                                amount,
                            ))
                            .context_id(context_id.clone()),
                    )
                    .await?;

                info!("Publishing completion");
                to_ledger
                    .client
                    .action(
                        ActionBuilder::for_account(
                            FX_SWAP_ACTION,
                            to_ledger.liquidity,
                            execute.request.from,
                        )
                        .payload(serde_json::to_vec(&Event::Completed).unwrap()),
                        context_id,
                    )
                    .await?;
                break;
            }
        }
        interval.tick().await;
    }
    Ok(())
}
