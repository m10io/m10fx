#![allow(dead_code)]

use m10_sdk::MetadataType;

pub mod config;
pub mod event;

pub const FX_SWAP_ACTION: &str = "m10.fx.swap";
pub const FX_SWAP_METADATA: &str = "m10.fx.execute";

pub struct FxSwapMetadata;

impl MetadataType for FxSwapMetadata {
    const TYPE_URL: &'static str = FX_SWAP_METADATA;
}
