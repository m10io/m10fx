use crate::config::LedgerConfig;
use crate::event::{Event, Quote, Request};
use crate::LedgerDB;
use futures_util::StreamExt;
use m10_sdk::account::AccountId;
use m10_sdk::client::Channel;
use m10_sdk::sdk::transaction_data::Data;
use m10_sdk::sdk::{ObserveActionsRequest, Target};
use m10_sdk::{sdk, Ed25519, LedgerClient, Signer, TransactionExt};
use rust_decimal::Decimal;
use std::sync::Arc;
use std::time::Duration;

const FX_SWAP_ACTION: &str = "m10.fx.swap";

#[derive(Clone)]
pub struct Ledger {
    signer: Arc<Ed25519>,
    client: LedgerClient,
    liquidity: AccountId,
    base_rate: Decimal,
}

impl Ledger {
    pub fn new(config: LedgerConfig) -> anyhow::Result<Self> {
        let channel = Channel::from_shared(config.address)?
            .timeout(Duration::from_secs(30))
            .connect_lazy()?;
        let client = LedgerClient::new(channel);
        let signer = Arc::new(Ed25519::new_key_pair(None).unwrap());
        Ok(Self {
            client,
            signer,
            liquidity: config.liquidity_account,
            base_rate: config.base_rate,
        })
    }

    async fn publish_quote(&self, context_id: Vec<u8>, quote: Quote) -> anyhow::Result<()> {
        let mut client = self.client.clone();
        let req = LedgerClient::transaction_request(
            sdk::InvokeAction {
                name: FX_SWAP_ACTION.to_string(),
                from_account: self.liquidity.to_be_bytes().to_vec(),
                target: Some(Target {
                    target: Some(sdk::target::Target::AccountId(
                        quote.request.from.account.to_be_bytes().to_vec(),
                    )),
                }),
                payload: serde_json::to_vec(&quote)?,
            },
            context_id,
        );
        let signed = self.signer.sign_request(req).await?;
        client.create_transaction(signed).await?.tx_error()?;
        Ok(())
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
        let mut actions = self.client.observe_actions(req).await?;

        while let Some(msg) = actions.next().await {
            let sdk::FinalizedTransactions { transactions } = msg?;
            for txn in transactions {
                if let Some(Data::InvokeAction(action)) = txn.data() {
                    if let Ok(event) = serde_json::from_slice::<Event>(&action.payload) {
                        match event {
                            Event::Request(request) => {
                                let rate = get_fx_rate(&db, &request)?;
                                if let Err(err) = self
                                    .publish_quote(
                                        txn.request.map(|req| req.context_id).unwrap_or_default(),
                                        Quote { request, rate },
                                    )
                                    .await
                                {
                                    eprintln!("Could not publish quote: {}", err);
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

fn get_fx_rate(db: &LedgerDB, request: &Request) -> anyhow::Result<Decimal> {
    let from_ledger = db
        .get(&request.from.currency)
        .ok_or_else(|| anyhow::anyhow!("Missing ledger for currency {}", request.from.currency))?;
    let to_ledger = db
        .get(&request.to.currency)
        .ok_or_else(|| anyhow::anyhow!("Missing ledger for currency {}", request.to.currency))?;

    Ok(to_ledger.base_rate / from_ledger.base_rate)
}
