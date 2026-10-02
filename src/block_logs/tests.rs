use super::*;
use alloy::{
    providers::{ProviderBuilder, RootProvider, mock::Asserter},
    transports::TransportResult,
};
use antidote::Mutex;
use futures_util::FutureExt;
use std::sync::Arc;
use tokio::time::timeout;

#[derive(Clone)]
struct MockLogsProvider {
    inner: DynProvider,
    responses: Asserter,
    requests: Arc<Mutex<Vec<Filter>>>,
}

impl MockLogsProvider {
    fn new() -> Self {
        let responses = Asserter::new();
        Self {
            inner: ProviderBuilder::new()
                .connect_mocked_client(responses.clone())
                .erased(),
            responses,
            requests: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn assert_requests(&self, ranges: &[(u64, u64)], event_signatures: Option<&[B256]>) {
        let expected: Vec<_> = ranges
            .iter()
            .map(|&(from_block, to_block)| {
                let filter = Filter::new()
                    .address(PERPL_MAINNET_EXCHANGE)
                    .from_block(from_block)
                    .to_block(to_block);
                match event_signatures {
                    Some(signatures) => filter.event_signature(signatures.to_vec()),
                    None => filter,
                }
            })
            .collect();
        assert_eq!(*self.requests.lock(), expected);
    }
}

// Record filters while retaining Alloy's real RPC serialization and mocked transport.
#[async_trait::async_trait]
impl Provider for MockLogsProvider {
    fn root(&self) -> &RootProvider {
        self.inner.root()
    }

    async fn get_logs(&self, filter: &Filter) -> TransportResult<Vec<Log>> {
        self.requests.lock().push(filter.clone());
        self.inner.get_logs(filter).await
    }
}

fn make_log(block_number: u64, log_index: u64) -> Log {
    Log {
        inner: alloy::primitives::Log {
            address: PERPL_MAINNET_EXCHANGE,
            ..<_>::default()
        },
        block_number: Some(block_number),
        block_timestamp: Some(block_number),
        log_index: Some(log_index),
        ..<_>::default()
    }
}

async fn collect_results<const BLOCK_BATCH_SIZE: u64>(
    provider: &MockLogsProvider,
    event_signatures: Option<Vec<B256>>,
    from_block: u64,
    to_block: u64,
) -> anyhow::Result<Vec<anyhow::Result<BlockLogs>>> {
    let stream = batch_stream::<BLOCK_BATCH_SIZE>(
        provider.clone().erased(),
        event_signatures,
        from_block,
        to_block,
        Duration::from_secs(1),
        Some(BlockTime::new(from_block, from_block)),
    );
    timeout(Duration::from_secs(1), stream.collect())
        .await
        .context("batch stream did not terminate")
}

#[tokio::test]
async fn shrinks_dense_ranges_and_preserves_filters_blocks_and_log_order() -> anyhow::Result<()> {
    let provider = MockLogsProvider::new();
    let signatures = vec![B256::repeat_byte(1), B256::repeat_byte(2)];
    provider.responses.push_failure_msg("block range too wide");
    provider
        .responses
        .push_failure_msg("query timeout exceeded");
    provider.responses.push_success(&vec![
        make_log(11, 2),
        make_log(10, 3),
        make_log(11, 0),
        make_log(10, 1),
    ]);
    provider.responses.push_success(&vec![make_log(13, 1)]);
    provider.responses.push_success(&Vec::<Log>::new());
    provider.responses.push_success(&vec![make_log(16, 0)]);
    provider
        .responses
        .push_failure_msg("log response size exceeded");
    provider.responses.push_success(&vec![make_log(19, 0)]);
    provider.responses.push_success(&Vec::<Log>::new());
    provider.responses.push_success(&Vec::<Log>::new());
    provider.responses.push_success(&vec![make_log(25, 0)]);

    let blocks = collect_results::<8>(&provider, Some(signatures.clone()), 10, 25)
        .await?
        .into_iter()
        .collect::<anyhow::Result<Vec<_>>>()?;
    assert_eq!(
        blocks.iter().map(|block| block.number).collect::<Vec<_>>(),
        (10..=25).collect::<Vec<_>>()
    );
    assert!(blocks.iter().all(|block| block.timestamp_s == block.number));
    assert_eq!(
        blocks
            .iter()
            .flat_map(|block| block
                .logs
                .iter()
                .map(move |log| (block.number, log.log_index)))
            .collect::<Vec<_>>(),
        vec![
            (10, Some(1)),
            (10, Some(3)),
            (11, Some(0)),
            (11, Some(2)),
            (13, Some(1)),
            (16, Some(0)),
            (19, Some(0)),
            (25, Some(0)),
        ]
    );
    provider.assert_requests(
        &[
            (10, 17),
            (10, 13),
            (10, 11),
            (12, 13),
            (14, 15),
            (16, 17),
            (18, 21),
            (18, 19),
            (20, 21),
            (22, 23),
            (24, 25),
        ],
        Some(&signatures),
    );
    assert!(provider.responses.read_q().is_empty());
    Ok(())
}

#[tokio::test]
async fn regrows_after_successes_without_exceeding_the_configured_limit() -> anyhow::Result<()> {
    let provider = MockLogsProvider::new();
    provider
        .responses
        .push_failure_msg("response size exceeded");
    for _ in 0..9 {
        provider.responses.push_success(&Vec::<Log>::new());
    }
    let blocks = collect_results::<8>(&provider, None, 0, 55)
        .await?
        .into_iter()
        .collect::<anyhow::Result<Vec<_>>>()?;
    assert_eq!(
        blocks.iter().map(|block| block.number).collect::<Vec<_>>(),
        (0..=55).collect::<Vec<_>>()
    );
    assert!(blocks.iter().all(|block| block.logs.is_empty()));
    provider.assert_requests(
        &[
            (0, 7),
            (0, 3),
            (4, 7),
            (8, 11),
            (12, 15),
            (16, 23),
            (24, 31),
            (32, 39),
            (40, 47),
            (48, 55),
        ],
        None,
    );
    assert!(provider.responses.read_q().is_empty());
    Ok(())
}

#[tokio::test]
async fn shrinks_the_clipped_tail_and_stops_at_a_permanently_failing_block() -> anyhow::Result<()> {
    let provider = MockLogsProvider::new();
    provider.responses.push_success(&Vec::<Log>::new());
    provider
        .responses
        .push_failure_msg("response size exceeded");
    provider.responses.push_success(&vec![make_log(8, 0)]);
    provider.responses.push_failure_msg("permanent failure");
    provider.responses.push_failure_msg("permanent failure");
    let mut results = collect_results::<8>(&provider, None, 0, 10)
        .await?
        .into_iter();
    for number in 0..=8 {
        assert_eq!(results.next().context("missing block")??.number, number);
    }
    let error = results
        .next()
        .context("missing terminal error")?
        .err()
        .context("expected terminal error")?;
    assert!(error.to_string().contains("block 9"));
    assert!(results.next().is_none());
    provider.assert_requests(&[(0, 7), (8, 10), (8, 8), (9, 9), (9, 9)], None);
    assert!(provider.responses.read_q().is_empty());
    Ok(())
}

#[tokio::test]
async fn bounds_persistent_errors_even_for_the_maximum_batch_size() -> anyhow::Result<()> {
    let provider = MockLogsProvider::new();
    for _ in 0..65 {
        provider.responses.push_failure_msg(
            "request failed (https://monad-mainnet.g.alchemy.com/v2/fake-secret)",
        );
    }
    let results = collect_results::<{ u64::MAX }>(&provider, None, 0, u64::MAX).await?;
    assert_eq!(results.len(), 1);
    let error = results
        .into_iter()
        .next()
        .context("missing result")?
        .err()
        .context("expected terminal error")?;
    let message = format!("{error:#}");
    assert!(message.contains("block 0"));
    assert!(message.contains("alchemy.com/***"));
    assert!(!message.contains("fake-secret"));
    let expected_ranges: Vec<_> = (0..64)
        .map(|shift| (0, (u64::MAX >> shift).saturating_sub(1)))
        .chain(std::iter::once((0, 0)))
        .collect();
    provider.assert_requests(&expected_ranges, None);
    assert!(provider.responses.read_q().is_empty());
    Ok(())
}

#[tokio::test]
async fn handles_exact_inclusive_boundaries_with_and_without_logs() -> anyhow::Result<()> {
    let maximum = u64::MAX;
    for (from_block, to_block, requests) in [
        (0, 0, vec![(0, 0)]),
        (5, 12, vec![(5, 12)]),
        (5, 13, vec![(5, 12), (13, 13)]),
        (maximum, maximum, vec![(maximum, maximum)]),
        (
            maximum.saturating_sub(8),
            maximum,
            vec![
                (maximum.saturating_sub(8), maximum.saturating_sub(1)),
                (maximum, maximum),
            ],
        ),
    ] {
        for with_logs in [false, true] {
            let provider = MockLogsProvider::new();
            for &(_, batch_to) in &requests {
                let logs = if with_logs {
                    vec![make_log(batch_to, 0)]
                } else {
                    Vec::new()
                };
                provider.responses.push_success(&logs);
            }
            let blocks = collect_results::<8>(&provider, None, from_block, to_block)
                .await?
                .into_iter()
                .collect::<anyhow::Result<Vec<_>>>()?;
            assert_eq!(
                blocks.iter().map(|block| block.number).collect::<Vec<_>>(),
                (from_block..=to_block).collect::<Vec<_>>()
            );
            assert!(blocks.iter().all(|block| block.timestamp_s == block.number));
            assert_eq!(
                blocks
                    .iter()
                    .filter(|block| !block.logs.is_empty())
                    .map(|block| block.number)
                    .collect::<Vec<_>>(),
                if with_logs {
                    requests.iter().map(|&(_, to)| to).collect::<Vec<_>>()
                } else {
                    Vec::new()
                }
            );
            provider.assert_requests(&requests, None);
            assert!(provider.responses.read_q().is_empty());
        }
    }
    Ok(())
}

#[tokio::test]
async fn retries_a_transient_single_block_failure_once() -> anyhow::Result<()> {
    let provider = MockLogsProvider::new();
    provider
        .responses
        .push_failure_msg("temporarily unavailable");
    provider
        .responses
        .push_success(&vec![make_log(u64::MAX, 2), make_log(u64::MAX, 0)]);
    let blocks = collect_results::<8>(&provider, None, u64::MAX, u64::MAX)
        .await?
        .into_iter()
        .collect::<anyhow::Result<Vec<_>>>()?;
    assert_eq!(blocks.len(), 1);
    let block = blocks.first().context("missing block")?;
    assert_eq!(block.number, u64::MAX);
    assert_eq!(
        block
            .logs
            .iter()
            .map(|log| log.log_index)
            .collect::<Vec<_>>(),
        vec![Some(0), Some(2)]
    );
    provider.assert_requests(&[(u64::MAX, u64::MAX), (u64::MAX, u64::MAX)], None);
    assert!(provider.responses.read_q().is_empty());
    Ok(())
}

#[tokio::test]
async fn rejects_invalid_input_before_requesting_logs() -> anyhow::Result<()> {
    let provider = MockLogsProvider::new();
    let results = collect_results::<8>(&provider, None, 2, 1).await?;
    assert!(matches!(results.as_slice(), [Err(_)]));
    provider.assert_requests(&[], None);
    Ok(())
}

#[tokio::test]
async fn rejects_logs_outside_the_requested_range_or_missing_block_metadata() -> anyhow::Result<()>
{
    for log in [
        make_log(9, 0),
        make_log(12, 0),
        Log::default(),
        Log {
            block_timestamp: None,
            ..make_log(10, 0)
        },
    ] {
        let provider = MockLogsProvider::new();
        provider.responses.push_success(&vec![log]);
        let results = collect_results::<8>(&provider, None, 10, 11).await?;
        assert!(matches!(results.as_slice(), [Err(_)]));
        provider.assert_requests(&[(10, 11)], None);
        assert!(provider.responses.read_q().is_empty());
    }
    Ok(())
}

#[tokio::test]
async fn dropping_during_range_reduction_cancels_further_requests() {
    let provider = MockLogsProvider::new();
    provider
        .responses
        .push_failure_msg("response size exceeded");
    provider.responses.push_success(&Vec::<Log>::new());
    let mut stream = Box::pin(batch_stream::<8>(
        provider.clone().erased(),
        None,
        0,
        7,
        Duration::from_secs(1),
        None,
    ));
    assert!(stream.next().now_or_never().is_none());
    drop(stream);
    provider.assert_requests(&[(0, 7)], None);
    assert_eq!(provider.responses.read_q().len(), 1);
}
