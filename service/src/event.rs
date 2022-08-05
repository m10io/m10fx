use m10_sdk::account::AccountId;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Display, Formatter};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Event {
    Request(Request),
    Quote(Quote),
    Execute(Execute),
    Completed,
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
    pub intermediary: AccountId,
}

impl Display for Quote {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "from={} to={} amount={} rate={} intermediary={}",
            self.request.from, self.request.to, self.request.amount, self.rate, self.intermediary
        )
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Execute {
    pub request: Request,
    /// [EPOCH TIME] Execution will run until this time
    pub valid_until: u64,
    /// Fx rate limits. If exceeded will exchange for this rate immediately
    pub upper_limit: Decimal,
    /// Fx rate limits. If exceeded will exchange for this rate immediately
    pub lower_limits: Decimal,
}
