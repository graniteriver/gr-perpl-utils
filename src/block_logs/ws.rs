use super::BlockTime;
use crate::{CONNECT_TIMEOUT, block_logs::PERPL_MAINNET_EXCHANGE};
use alloy::{
    primitives::{Address, Bloom, BloomInput},
    providers::{DynProvider, Provider},
    rpc::types::Header,
};
use anyhow::anyhow;
use futures_util::{FutureExt, Stream, StreamExt, future::ready};
use serde::Deserialize;
use std::time::Duration;
use tokio::time::{Instant, timeout_at};

#[derive(Debug, Clone)]
pub(super) struct SafeBlock {
    pub block: BlockTime,
    pub logs_bloom: Bloom,
    pub base_fee_per_gas: Option<u64>,
}

impl From<Header> for SafeBlock {
    fn from(head: Header) -> Self {
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

/// Monad block commit state, see <https://docs.monad.xyz/monad-arch/consensus/block-states>.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
enum CommitState {
    Proposed,
    Voted,
    Finalized,
    Verified,
}

/// A `monadNewHeads` subscription update.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MonadHead {
    commit_state: CommitState,
    #[serde(flatten)]
    header: Header,
}

impl MonadHead {
    /// Monad may skip `Voted` and go straight to `Finalized`, so accept any non-speculative state.
    fn is_voted_or_better(&self) -> bool {
        self.commit_state != CommitState::Proposed
    }
}

/// Pubsub stream of new "safe" (`Voted` or better) block headers.
///
/// Uses `monadNewHeads` as the standard `newHeads` emits speculative `Proposed` blocks:
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
            let mut heads = timeout_at(deadline, pubsub.subscribe::<_, MonadHead>(("monadNewHeads",)))
                .await
                .map_err(|_| anyhow!("safe-block monadNewHeads subscribe timed out (latest: {latest:?})"))??
                .into_stream()
                .filter(|head| ready(head.is_voted_or_better()))
                .fuse();
            while let Some(mut head) = timeout_at(deadline, heads.next())
                .await
                .map_err(|_| anyhow!("safe-block heads.next() timed out (latest: {latest:?})"))?
            {
                while let Some(Some(h)) = heads.next().now_or_never() {
                    // eagerly read any later ready and discard older ones
                    if h.header.number > head.header.number {
                        head = h;
                    }
                }

                let block_number = head.header.number;
                if latest.is_none_or(|latest| latest < block_number) {
                    latest = Some(block_number);
                    deadline = Instant::now() + yield_timeout;
                    yield SafeBlock::from(head.header);
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

#[test]
fn deserializes_monad_new_heads_update() {
    // Captured from Alchemy Monad mainnet `monadNewHeads`.
    const VOTED: &str = r#"{"blockId":"0x5be35d3b65750e98a81e6b0dec382b6a6898ad06c10a52b2d1f43eeb2a7d7c65","commitState":"Voted","hash":"0x84fd43ce4059b9fb7564b609bc60c45d24980976dc91c65b55054e7a62c1b12c","parentHash":"0x7392f71981015f138083a9b88c9b4653319ad477b2f7acc10b399099f199ac72","sha3Uncles":"0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347","miner":"0xd3ffe0ba84f4a1af5fd3275f86b0a035f8de98f0","stateRoot":"0xc5dce67902b3a74079fff973a30caca045b97bba3a7dc535bd06f735de8e1165","transactionsRoot":"0xcf0ffe9696fa0072139be9711679573816ff8acac9514c27168c5bb2d237025a","receiptsRoot":"0xa609c809f4f61bf3dd358653d4efb2574b215ef35bce57c3bce0922f0e8819e8","logsBloom":"0x800241008026004020200810210000a00002000402040000801082882000002008000910011300c0221000040000820000011000150c020208202010806460000010000000200000000812080800000120000400026480a000008008088000000006002002005404080020402102080080000000c54041016002001210124401491812442000800009400024200002000200060040a00008001012000000040034024e000000002002008000004200e00003000480040000284040088400002240201262010500000000040000840100980802c830000480002d400002006004054001000130000430000000100000840002000c200043000000001080008203","difficulty":"0x0","number":"0x6bcf4e2","gasLimit":"0x8f0d180","gasUsed":"0xa36d17","timestamp":"0x6ace2ad1","extraData":"0x0000000000000000000000000000000000000000000000000000000000000000","mixHash":"0xd6b0cc00bfb4bd85023dc6be7824efe93393fbd7b5b8932e984dd42f3cf5ae87","nonce":"0x0000000000000000","baseFeePerGas":"0x174876e800","withdrawalsRoot":"0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421","blobGasUsed":"0x0","excessBlobGas":"0x0","parentBeaconBlockRoot":"0x0000000000000000000000000000000000000000000000000000000000000000","requestsHash":"0x0000000000000000000000000000000000000000000000000000000000000000","totalDifficulty":"0x0","size":"0x320"}"#;

    let head: MonadHead = serde_json::from_str(VOTED).unwrap();
    assert_eq!(head.commit_state, CommitState::Voted);
    assert!(head.is_voted_or_better());
    assert_eq!(head.header.number, 0x6bcf4e2);
    assert_eq!(head.header.timestamp, 0x6ace2ad1);
    assert_eq!(head.header.base_fee_per_gas, Some(0x174876e800));
    assert_ne!(head.header.logs_bloom, Bloom::default());

    let proposed: MonadHead =
        serde_json::from_str(&VOTED.replace(r#""Voted""#, r#""Proposed""#)).unwrap();
    assert!(!proposed.is_voted_or_better());
    for state in ["Finalized", "Verified"] {
        let head: MonadHead =
            serde_json::from_str(&VOTED.replace(r#""Voted""#, &format!("\"{state}\""))).unwrap();
        assert!(head.is_voted_or_better());
    }
}
