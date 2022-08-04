mod config;
mod event;
mod ledger;

use crate::config::CurrencyCode;
use crate::ledger::Ledger;
use futures_util::future::select_all;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{info_span, Instrument};

pub type LedgerDB = Arc<HashMap<CurrencyCode, Ledger>>;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let config = config::parse()?;
    let address = config.address;

    let ledgers = config
        .liquidity
        .into_iter()
        .map(|(currency, config)| {
            Ok((
                currency.to_lowercase(),
                Ledger::new(address.clone(), currency, config)?,
            ))
        })
        .collect::<anyhow::Result<HashMap<CurrencyCode, Ledger>>>()?;

    let ledger_db = Arc::new(ledgers);

    let mut futures = vec![];
    for (currency, ledger) in ledger_db.iter() {
        // Observe actions
        futures.push(tokio::spawn(
            ledger
                .clone()
                .observe_actions(ledger_db.clone())
                .instrument(info_span!("actions",%currency)),
        ));

        // Observe transfers
        futures.push(tokio::spawn(
            ledger
                .clone()
                .observe_transfers(ledger_db.clone())
                .instrument(info_span!("transfers",%currency)),
        ));
    }

    select_all(futures).await.0??;

    Ok(())
}
