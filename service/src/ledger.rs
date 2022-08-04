use crate::config::LiquidityConfig;
use crate::event::{Event, Execute, Quote, Request};
use crate::LedgerDB;
use futures_util::StreamExt;
use m10_sdk::account::AccountId;
use m10_sdk::client::Channel;
use m10_sdk::{
    AccountFilter, ActionBuilder, Ed25519, M10Client, MetadataExt, StepBuilder, TransferBuilder,
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
            liquidity: config.account,
            base_rate: config.base_rate,
        })
    }

    pub async fn observe_transfers(self, db: LedgerDB) -> anyhow::Result<()> {
        // Sign the request to observe all actions named `FX_SWAP_ACTION`
        let mut transfers = self
            .client
            .observe_transfers(AccountFilter::default().involves(self.liquidity))
            .await?;
        info!("Observing transfers");

        while let Some(Ok(transfers)) = transfers.next().await {
            for transfer in transfers {
                if let Some(payload) = transfer.with_type::<FxSwapMetadata>() {
                    if let Ok(event) = serde_json::from_slice::<Event>(payload) {
                        info!(?event);
                        if let Event::Execute(execute) = event {
                            launch_swap_task(
                                self.clone(),
                                db.clone(),
                                execute,
                                transfer.context_id,
                            )
                            .await;
                        } else {
                            error!("invalid event type");
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn observe_actions(mut self, db: LedgerDB) -> anyhow::Result<()> {
        // Sign the request to observe all actions named `FX_SWAP_ACTION`
        let mut actions = self
            .client
            .observe_actions(AccountFilter::name(FX_SWAP_ACTION).involves(self.liquidity))
            .await?;
        info!(action = %FX_SWAP_ACTION, "Started observations");
        while let Some(Ok(actions)) = actions.next().await {
            for action in actions {
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
                                    let intermediary = db.get(&from_currency).unwrap().liquidity;
                                    let quote = Quote {
                                        request,
                                        rate,
                                        intermediary,
                                    };
                                    info!(?quote, "Publishing quote");
                                    if let Err(err) = self
                                        .client
                                        .action(
                                            ActionBuilder::for_account(
                                                FX_SWAP_ACTION.to_string(),
                                                self.liquidity,
                                                quote.request.from,
                                            )
                                            .payload(serde_json::to_vec(&Event::Quote(quote))?),
                                            action.context_id,
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

        Ok(())
    }
}

async fn get_currencies(
    ledger: &mut Ledger,
    request: &Request,
) -> anyhow::Result<(String, String)> {
    let from = ledger.client.get_account(request.from).await?;
    let from_currency = from.code.to_lowercase();
    let to = ledger.client.get_account(request.to).await?;
    let to_currency = to.code.to_lowercase();
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
    let to_ledger = db.get(&to_currency).unwrap().clone();
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
                        to_ledger
                            .client
                            .transfer(
                                TransferBuilder::new().step(StepBuilder::new(
                                    to_ledger.liquidity,
                                    to,
                                    amount,
                                )),
                                context_id.clone(),
                            )
                            .await
                            .expect("Could not execute swap");

                        info!("Publishing completion");
                        to_ledger
                            .client
                            .action(
                                ActionBuilder::for_account(
                                    FX_SWAP_ACTION,
                                    to_ledger.liquidity,
                                    from,
                                )
                                .payload(serde_json::to_vec(&Event::Completed).unwrap()),
                                context_id,
                            )
                            .await
                            .expect("Could not publish completion");
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
