//! Logic for working with Perpl DEX on the tokio runtime.
pub mod block_logs;

use alloy::primitives::{Address, address};
use std::time::Duration;

/// Monad time of each block.
const LATEST_MONAD_BLOCK_RATE: Duration = Duration::from_millis(300);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(12);
/// `perpl_sdk::Chain::mainnet().exchange()`.
const PERPL_MAINNET_EXCHANGE: Address = address!("0x34B6552d57a35a1D042CcAe1951BD1C370112a6F");
