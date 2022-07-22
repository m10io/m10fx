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
    pub from: AccountId,
    pub to: AccountId,
    pub amount: Decimal,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Quote {
    pub request: Request,
    pub rate: Decimal,
}
