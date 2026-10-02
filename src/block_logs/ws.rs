use super::BlockTime;
use crate::{CONNECT_TIMEOUT, block_logs::PERPL_MAINNET_EXCHANGE};
use alloy::{
    primitives::{Address, Bloom, BloomInput},
    providers::{DynProvider, Provider},
};
use anyhow::anyhow;
use futures_util::{FutureExt, Stream, StreamExt};
use std::time::Duration;
use tokio::time::{Instant, timeout_at};

#[derive(Debug, Clone)]
pub(super) struct SafeBlock {
    pub block: BlockTime,
    pub logs_bloom: Bloom,
    pub base_fee_per_gas: Option<u64>,
}

impl From<alloy::rpc::types::Header> for SafeBlock {
    fn from(head: alloy::rpc::types::Header) -> Self {
        Self {
            block: BlockTime::new(head.number, head.timestamp),
            logs_bloom: head.logs_bloom,
            base_fee_per_gas: head.base_fee_per_gas,
        }
    }
}

impl SafeBlock {
    pub fn contains_perpl_logs(&self) -> bool {
        self.contains_logs_from(PERPL_MAINNET_EXCHANGE)
    }

    /// A negative EVM bloom lookup cannot omit a matching log, so it safely avoids an `eth_getLogs`.
    pub fn contains_logs_from(&self, address: Address) -> bool {
        self.logs_bloom
            .contains_input(BloomInput::Raw(address.as_slice()))
    }
}

/// Pubsub stream of new "safe" block headers.
///
/// Monad's standard `newHeads` subscription emits once a block is `Voted`, unlike
/// `monadNewHeads`, which also delivers speculative lifecycle updates.
/// Monad commitment semantics are documented at
/// <https://docs.monad.xyz/reference/json-rpc/overview.md#websocket-subscriptions>.
///
/// 1st connection + 1st yield has a [`CONNECT_TIMEOUT`] timeout.
/// After that `yield_timeout` max wait for each successive yield.
///
/// Connection or yield timeouts will yield an error and end the stream.
pub fn safe_block_stream(
    pubsub: DynProvider,
    yield_timeout: Duration,
) -> impl Stream<Item = anyhow::Result<SafeBlock>> {
    async_stream::try_stream! {
        let mut latest = None;
        let mut deadline = Instant::now() + CONNECT_TIMEOUT;

        loop {
            let mut heads = timeout_at(deadline, pubsub.subscribe_blocks())
                .await
                .map_err(|_| anyhow!("safe-block subscribe_blocks timed out (latest: {latest:?})"))??
                .into_stream()
                .fuse();
            while let Some(mut head) = timeout_at(deadline, heads.next())
                .await
                .map_err(|_| anyhow!("safe-block heads.next() timed out (latest: {latest:?})"))?
            {
                while let Some(Some(h)) = heads.next().now_or_never() {
                    // eagerly read any later ready and discard older ones
                    if h.number > head.number {
                        head = h;
                    }
                }

                let block_number = head.number;
                if latest.is_none_or(|latest| latest < block_number) {
                    latest = Some(block_number);
                    deadline = Instant::now() + yield_timeout;
                    yield SafeBlock::from(head);
                }
            }
            // stream ended; try to resubscribe within the existing deadline
        }
    }
}

#[test]
fn checks_exchange_address_against_header_bloom() {
    let mut block = SafeBlock {
        block: BlockTime::new(1, 1),
        logs_bloom: Bloom::default(),
        base_fee_per_gas: None,
    };
    assert!(!block.contains_logs_from(PERPL_MAINNET_EXCHANGE));

    block
        .logs_bloom
        .accrue(BloomInput::Raw(PERPL_MAINNET_EXCHANGE.as_slice()));
    assert!(block.contains_logs_from(PERPL_MAINNET_EXCHANGE));
}
