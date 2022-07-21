use config::{Environment, FileFormat};
use m10_sdk::account::AccountId;
use rust_decimal::Decimal;
use serde::Deserialize;
use std::collections::HashMap;

pub type CurrencyCode = String;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub ledgers: HashMap<CurrencyCode, LedgerConfig>,
}

#[derive(Debug, Deserialize)]
pub struct LedgerConfig {
    /// Ledger address, e.g. https://develop.m10.net
    pub address: String,
    /// Currency account
    pub root_account: AccountId,
    /// Account ID of the liquidity provider for that currency
    pub liquidity_account: AccountId,
    /// Currency value in base amount (~ USD)
    pub base_rate: Decimal,
}

pub fn parse() -> Result<Config, config::ConfigError> {
    let config = config::Config::builder()
        .add_source(config::File::from_str(
            "/etc/m10/config.toml",
            FileFormat::Toml,
        ))
        .add_source(config::File::from_str(
            "/root/.config/m10/config.toml",
            FileFormat::Toml,
        ))
        .add_source(config::File::from_str("./config.toml", FileFormat::Toml))
        .add_source(Environment::with_prefix("APP"))
        .build()?;
    config.try_deserialize()
}
