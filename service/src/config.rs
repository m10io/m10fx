use config::Environment;
use m10_sdk::account::AccountId;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

pub type CurrencyCode = String;

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    /// Ledger address, e.g. https://develop.m10.net
    #[serde(default = "default_address")]
    pub address: String,
    /// Liquidity config
    pub liquidity: HashMap<CurrencyCode, LiquidityConfig>,
}

fn default_address() -> String {
    "https://develop.m10.net".to_string()
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LiquidityConfig {
    /// Account ID of the liquidity provider for that currency
    pub account: AccountId,
    /// Currency value in base amount (~ USD)
    pub base_rate: Decimal,
    /// Liquidity key pair
    pub key_pair: PathBuf,
}

pub fn parse() -> Result<Config, config::ConfigError> {
    let config = config::Config::builder()
        .add_source(config::File::from(Path::new("./config.toml")))
        .add_source(Environment::with_prefix("APP"))
        .build()?;
    config.try_deserialize()
}
