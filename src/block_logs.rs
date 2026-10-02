//! Logic for streaming block logs.
//!
//! Assumptions:
//! * Only logs from the Perpl mainnet exchange contract are streamed.
//! * RPC `eth_getLogs` responses include `blockTimestamp`, otherwise streams will error.
//! * Live streams follow Monad `safe` (`Voted`) blocks, which are not finalized.
//!   Reorgs are not handled.
mod share;
#[cfg(test)]
mod tests;
mod ws;

pub use share::{ArcLogsResult, single_process_live_stream};

use crate::PERPL_MAINNET_EXCHANGE;
use alloy::{
    eips::{BlockId, BlockNumberOrTag},
    primitives::B256,
    providers::{DynProvider, Provider},
    rpc::types::{Filter, Log},
};
use anyhow::{Context, anyhow};
use futures_util::{Stream, StreamExt};
use share::PROCESS_LIVE_STREAM_CHANNEL_LEN;
use std::{pin::pin, time::Duration};

/// Logs from a single block.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct BlockLogs {
    /// Block number.
    pub number: u64,
    /// Block timestamp (seconds).
    pub timestamp_s: u64,
    /// EIP1559 base fee.
    ///
    /// Always `None` from [`batch_stream`] and the historic blocks of
    /// [`single_process_live_stream`]. [`live_stream`] fills it from the latest safe block,
    /// so for catch-up blocks it may differ from the block's own base fee.
    pub base_fee_per_gas: Option<u64>,
    /// The logs for this block. May be empty if no logs.
    pub logs: Vec<Log>,
}

impl BlockLogs {
    /// Block time of these logs.
    pub fn block_time(&self) -> BlockTime {
        BlockTime::new(self.number, self.timestamp_s)
    }
}

/// Block number and timestamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlockTime {
    /// Block number.
    pub number: u64,
    /// Timestamp, seconds since unix epoch.
    pub timestamp_s: u64,
}

impl BlockTime {
    pub fn new(number: u64, timestamp_s: u64) -> Self {
        Self {
            number,
            timestamp_s,
        }
    }
}

impl TryFrom<&Log> for BlockTime {
    type Error = anyhow::Error;

    fn try_from(log: &Log) -> Result<Self, Self::Error> {
        Ok(Self {
            number: log.block_number.context("log missing block_number")?,
            timestamp_s: log.block_timestamp.context("log missing block_timestamp")?,
        })
    }
}

/// Batched fetch of block events suitable for querying historic data.
///
/// On success this stream yields one item for each block in the inclusive range,
/// including blocks with no events.
/// Failed ranges are halved; after four consecutive successful batches the range
/// doubles, up to `BLOCK_BATCH_SIZE`. A failing single-block range is retried once
/// before terminating with an error.
///
/// Note: Empty log blocks have estimated `timestamp_s`.
///
/// If `any_known_block` is not provided it may result in some empty log
/// event items having `0` timestamp. This may also happen if `monad_block_rate`
/// and block numbers cause overflow (not expected unless invalid config).
pub fn batch_stream<const BLOCK_BATCH_SIZE: u64>(
    rest: DynProvider,
    event_signatures: Option<Vec<B256>>,
    from_block: u64,
    to_block: u64,
    monad_block_rate: Duration,
    any_known_block: Option<BlockTime>,
) -> impl Stream<Item = anyhow::Result<BlockLogs>> {
    const GROW_AFTER_SUCCESSFUL_BATCHES: u8 = 4;
    const {
        assert!(
            BLOCK_BATCH_SIZE > 0,
            "block batch size must be greater than zero"
        );
    }

    async_stream::try_stream! {
        if from_block > to_block {
            Err(anyhow!("invalid get_logs block range: {from_block}..={to_block}"))?;
        }

        let mut next_from_block = Some(from_block);
        let mut block_batch_size = BLOCK_BATCH_SIZE;
        let mut successful_batches = 0u8;
        while let Some(next_from) = next_from_block {
            let batch_to_block = next_from
                .saturating_add(block_batch_size.saturating_sub(1))
                .min(to_block);
            let mut filter = Filter::new()
                .address(PERPL_MAINNET_EXCHANGE)
                .from_block(next_from)
                .to_block(batch_to_block);
            if let Some(event_signatures) = &event_signatures {
                filter = filter.event_signature(event_signatures.clone());
            }

            let mut logs = match get_logs(&rest, &filter).await {
                Ok(logs) => logs,
                Err(_) if next_from < batch_to_block => {
                    // Shrink the attempted range, which may be shorter than block_batch_size at the end.
                    block_batch_size = batch_to_block.saturating_sub(next_from).saturating_add(1) / 2;
                    successful_batches = 0;
                    tokio::task::yield_now().await;
                    continue;
                }
                Err(_) => {
                    successful_batches = 0;
                    tokio::task::yield_now().await;
                    get_logs(&rest, &filter)
                        .await
                        .with_context(|| format!("get_logs failed for block {next_from} after retry"))?
                }
            };
            logs.sort_unstable_by_key(|log| (log.block_number, log.log_index));

            let mut event_block_n = Some(next_from);
            let mut known_block = any_known_block;
            let mut start_idx = 0usize;
            while let Some(log) = logs.get(start_idx) {
                let block = BlockTime::try_from(log)?;
                if !(next_from..=batch_to_block).contains(&block.number) {
                    Err(anyhow!(
                        "get_logs returned block {} outside requested range {next_from}..={batch_to_block}",
                        block.number,
                    ))?;
                }
                known_block = Some(block);

                // handle any empty logs preceding this one
                if let Some(event_block_n) = event_block_n {
                    for empty_block_n in event_block_n..block.number {
                        let estimated_timestamp =
                            estimate_timestamp_s(empty_block_n, block, monad_block_rate).unwrap_or(0);
                        yield BlockLogs {
                            number: empty_block_n,
                            timestamp_s: estimated_timestamp,
                            ..<_>::default()
                        };
                    }
                }

                let logs: Vec<_> = logs[start_idx..]
                    .iter()
                    .take_while(|l| l.block_number == Some(block.number))
                    .cloned()
                    .collect();
                let logs_count = logs.len();

                yield BlockLogs {
                    number: block.number,
                    timestamp_s: block.timestamp_s,
                    base_fee_per_gas: None,
                    logs,
                };
                start_idx = start_idx.saturating_add(logs_count);
                event_block_n = block.number.checked_add(1);
            }

            // handle any empty logs after last yield
            if let Some(event_block_n) = event_block_n {
                for empty_block_n in event_block_n..=batch_to_block {
                    let estimated_timestamp = known_block
                        .and_then(|k| estimate_timestamp_s(empty_block_n, k, monad_block_rate))
                        .unwrap_or(0);
                    yield BlockLogs {
                        number: empty_block_n,
                        timestamp_s: estimated_timestamp,
                        ..<_>::default()
                    };
                }
            }

            next_from_block = batch_to_block.checked_add(1).filter(|next| *next <= to_block);
            successful_batches = successful_batches.saturating_add(1);
            if successful_batches == GROW_AFTER_SUCCESSFUL_BATCHES {
                block_batch_size = block_batch_size.saturating_mul(2).min(BLOCK_BATCH_SIZE);
                successful_batches = 0;
            }
        }
    }
}

/// Fetch eth_getLogs with 3s timeout.
async fn get_logs(rest: &DynProvider, filter: &Filter) -> anyhow::Result<Vec<Log>> {
    const GET_LOGS_TIMEOUT: Duration = Duration::from_secs(3);

    tokio::time::timeout(GET_LOGS_TIMEOUT, rest.get_logs(filter))
        .await
        .with_context(|| format!("get_logs timed out after {GET_LOGS_TIMEOUT:?}"))
        .and_then(|r| r.map_err(|e| rm_alchemy_key_from_err(e.into())))
}

/// Use http rpc to fetch the latest safe block.
async fn fetch_safe_block(rest: &DynProvider) -> anyhow::Result<ws::SafeBlock> {
    let block = rest
        .get_block(BlockId::Number(BlockNumberOrTag::Safe))
        .await?
        .context("get_block returned None")?;
    Ok(block.header.into())
}

/// Optimised version of `perpl_sdk::stream::raw`.
///
/// * Does not poll, instead uses `ws::safe_block_stream` ws to stream new `Voted` blocks
/// * Results in no usage of `get_block` (except once on init) only necessary usage of `get_logs`.
/// * If under-running logs will be batch fetched. These catch-up blocks use the latest
///   safe block's `base_fee_per_gas`, not their own.
///
/// Yields an error if `from_block_n` is more than 5 blocks ahead of the current safe block.
pub fn live_stream(
    rest: DynProvider,
    pubsub: DynProvider,
    from_block_n: u64,
    monad_block_rate: Duration,
) -> impl Stream<Item = anyhow::Result<BlockLogs>> {
    /// Max catch-up batch size to fetch if under-running.
    /// Note: This shouldn't be higher than `PROCESS_LIVE_STREAM_CHANNEL_LEN`.
    const MAX_BATCH_SIZE: u64 = PROCESS_LIVE_STREAM_CHANNEL_LEN as u64 / 2;
    /// Allowed max `from_block_n` to be ahead of the current safe block.
    const MAX_FROM_AHEAD: u64 = 5;
    const SAFE_BLOCK_TIMEOUT: Duration = Duration::from_secs(6);

    async_stream::try_stream! {
        let mut new_safe_blocks = pin!(ws::safe_block_stream(pubsub, SAFE_BLOCK_TIMEOUT));

        // Race the initial safe-block http vs ws this helps ensure better progress
        // when ws perf is degraded causing retries but http is working. Since each retry
        // we'll at least batch catch up to the latest http safe-block before hitting
        // ws timeouts.
        let safe_block = tokio::select! {
            ws_next = new_safe_blocks.next() => {
                ws_next.context("safe_block_stream immediately ended").flatten()
            }
            // Note: No timeout needed as we're racing new_safe_blocks's 12s connect timeout
            // Only use Ok(_), on err just prefer new_safe_blocks
            Ok(http_safe_block) = fetch_safe_block(&rest) => Ok(http_safe_block),
        };
        let mut safe_block = safe_block?;
        if safe_block.block.number.saturating_add(MAX_FROM_AHEAD) < from_block_n {
            Err(anyhow!(
                "from_block_n is newer than current safe block ({from_block_n} > {})",
                safe_block.block.number
            ))?;
        }
        let mut block_num = from_block_n;

        loop {
            while safe_block.block.number < block_num {
                safe_block = new_safe_blocks
                    .next()
                    .await
                    .context("safe_block_stream ended")??
            }

            let contains_perpl_logs = safe_block.contains_perpl_logs();
            let batch_to_block = match contains_perpl_logs {
                true => Some(safe_block.block.number),
                false => safe_block
                    .block
                    .number
                    .checked_sub(1)
                    .filter(|batch_to_block| *batch_to_block >= block_num),
            };

            if let Some(batch_to_block) = batch_to_block {
                // Batch catch-up requests so an under-running stream can recover faster.
                let mut logs = pin!(batch_stream::<MAX_BATCH_SIZE>(
                    rest.clone(),
                    None,
                    block_num,
                    batch_to_block,
                    monad_block_rate,
                    Some(safe_block.block),
                ));
                while let Some(next) = logs.next().await {
                    let mut logs = next?;
                    logs.base_fee_per_gas = safe_block.base_fee_per_gas;
                    yield logs;
                }
            }

            if !contains_perpl_logs {
                yield BlockLogs {
                    number: safe_block.block.number,
                    timestamp_s: safe_block.block.timestamp_s,
                    base_fee_per_gas: safe_block.base_fee_per_gas,
                    ..<_>::default()
                };
            }
            block_num = safe_block.block.number + 1;
        }
    }
}

/// Estimate the given `block_n` timestamp using the other args.
///
/// Return `None` on overflows.
fn estimate_timestamp_s(block_n: u64, known: BlockTime, monad_block_rate: Duration) -> Option<u64> {
    Some(
        if block_n > known.number {
            Duration::from_secs(known.timestamp_s).checked_add(
                monad_block_rate.checked_mul(u32::try_from(block_n - known.number).ok()?)?,
            )?
        } else {
            Duration::from_secs(known.timestamp_s).checked_sub(
                monad_block_rate.checked_mul(u32::try_from(known.number - block_n).ok()?)?,
            )?
        }
        .as_secs(),
    )
}

/// Try to remove alchemy api key from given `msg`.
fn rm_alchemy_key_from_msg(msg: String) -> String {
    const KEY: &str = "alchemy.com/";

    let Some(alchemy_idx) = msg.find(KEY) else {
        return msg;
    };

    let prefix = &msg[..alchemy_idx];
    if let Some(idx) = msg[alchemy_idx + KEY.len()..]
        .find(|c: char| c == '\'' || c == '"' || c == ')' || c.is_ascii_whitespace())
    {
        let end_idx = alchemy_idx + KEY.len() + idx;
        let suffix = &msg[end_idx..];
        return format!("{prefix}{KEY}***{suffix}");
    }
    format!("{prefix}{KEY}***")
}

fn rm_alchemy_key_from_err(e: anyhow::Error) -> anyhow::Error {
    anyhow!("{}", rm_alchemy_key_from_msg(e.to_string()))
}

#[test]
fn test_estimate_timestamp() {
    let estimated = estimate_timestamp_s(
        500,
        BlockTime::new(400, 1_000_000),
        Duration::from_millis(300),
    )
    .unwrap();
    assert_eq!(estimated, 1_000_030);

    let estimated = estimate_timestamp_s(
        500,
        BlockTime::new(600, 1_000_000),
        Duration::from_millis(300),
    )
    .unwrap();
    assert_eq!(estimated, 999_970);

    let estimated = estimate_timestamp_s(500, BlockTime::new(600, 1_000_000), Duration::MAX);
    assert!(estimated.is_none());
}

#[test]
fn test_rm_alchemy_key_from_transport_err() {
    const FAKE_API_KEY: &str = "0aAAA1aAaA-aaAAaAaA_A";

    let anon = rm_alchemy_key_from_err(anyhow!(
        "transport error: error sending request for url (https://monad-mainnet.g.alchemy.com/v2/{FAKE_API_KEY})"
    ));
    assert_eq!(
        anon.to_string(),
        "transport error: error sending request for url (https://monad-mainnet.g.alchemy.com/***)",
    );
}
