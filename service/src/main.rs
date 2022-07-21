mod config;
mod event;
mod ledger;

use crate::config::CurrencyCode;
use crate::ledger::Ledger;
use futures_util::future::select_all;
use std::collections::HashMap;
use std::sync::Arc;

pub type LedgerDB = Arc<HashMap<CurrencyCode, Ledger>>;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = config::parse()?;

    let ledgers = config
        .ledgers
        .into_iter()
        .map(|(currency, config)| Ok((currency, Ledger::new(config)?)))
        .collect::<anyhow::Result<HashMap<CurrencyCode, Ledger>>>()?;

    let ledger_db = Arc::new(ledgers);

    let futures = ledger_db
        .values()
        .cloned()
        .map(|ledger| ledger.observe_requests(ledger_db.clone()))
        .map(tokio::spawn)
        .collect::<Vec<_>>();

    select_all(futures).await.0??;

    Ok(())
}
