use crate::CurrencyCode;
use m10_sdk::account::AccountId;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Event {
    Request(Request),
    Quote(Quote),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Request {
    pub from: LedgerAccount,
    pub to: LedgerAccount,
    pub amount: Decimal,
    pub upper_limit: Decimal,
    pub lower_limit: Decimal,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LedgerAccount {
    pub account: AccountId,
    pub currency: CurrencyCode,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Quote {
    pub request: Request,
    pub rate: Decimal,
}
