use crate::{
    CONNECT_TIMEOUT, LATEST_MONAD_BLOCK_RATE,
    block_logs::{BlockLogs, batch_stream, live_stream},
};
use alloy::{
    providers::{DynProvider, ProviderBuilder},
    rpc::client::WsConnect,
};
use anyhow::anyhow;
use futures_util::{Stream, StreamExt};
use std::{pin::pin, sync::Arc};
use tokio::sync::broadcast;

pub type ArcLogsResult = Result<Arc<BlockLogs>, Arc<anyhow::Error>>;
pub(crate) const PROCESS_LIVE_STREAM_CHANNEL_LEN: usize = 256;

/// Returns a live stream of mainnet events powered by a single process [`live_stream`]
/// all events are Arc-shared and sent using a broadcast channel.
///
/// Broadcast channel errors, including lag, will cause the stream to error and end.
/// To recover, call again with `from_block_n` set after the last received block.
/// Long historic catch-ups may lag, but each call will still make progress.
///
/// The advantage is that within a process multiple consumers can stream live logs
/// without any `get_logs` fetch duplication.
///
/// A WebSocket connection is only established when no matching task exists.
///
/// When this call starts the shared task, the stream yields an error if `from_block_n` is
/// more than 5 blocks ahead of the current safe block, see [`live_stream`].
///
/// # Panics
/// May panic if called outside a tokio runtime, see [`tokio::spawn`].
pub fn single_process_live_stream(
    rest: DynProvider,
    pubsub_connect: WsConnect,
    from_block_n: u64,
) -> impl Stream<Item = ArcLogsResult> {
    static TASKS: std::sync::Mutex<Vec<SingleStreamHandle>> = std::sync::Mutex::new(vec![]);

    let mut tasks = TASKS.lock().unwrap_or_else(|error| error.into_inner());
    tasks.retain(|handle| handle.tx.upgrade().is_some());
    let existing_sender = tasks
        .iter()
        .find(|handle| handle.node_url == pubsub_connect.url())
        .and_then(|handle| handle.tx.upgrade());
    let mut rx = if let Some(tx) = existing_sender {
        tx.subscribe()
    } else {
        let node_url = pubsub_connect.url().to_owned();
        let (tx, rx) = new_live_stream_task(rest.clone(), pubsub_connect, from_block_n);
        tasks.push(SingleStreamHandle { tx, node_url });
        rx
    };

    async_stream::stream! {
        let first = match rx.recv().await.map_err(|e| Arc::new(anyhow!("{e}"))).flatten() {
            Ok(v) => v,
            Err(e) => {
                // end early on any kind of error, we need a first result to figure out historic streaming needs
                yield Err(e);
                return;
            }
        };

        let first_n = first.number;
        if first_n > from_block_n {
            // stream older events first
            let mut history = pin!(batch_stream::<256>(
                rest,
                None,
                from_block_n,
                first_n - 1,
                LATEST_MONAD_BLOCK_RATE,
                Some(first.block_time()),
            ));
            while let Some(next) = history.next().await {
                yield arc(next);
            }
        }

        if first_n >= from_block_n {
            yield Ok(first);
        }

        loop {
            match rx.recv().await {
                Ok(Ok(event)) if event.number < from_block_n => continue,
                Ok(e) => yield e,
                Err(e) => {
                    // Note: lagging is also fatal, consumer is expected to re-call
                    // `single_process_live_stream` to recover. Assuming we can batch_stream
                    // faster than realtime this should eventually recover.
                    yield Err(Arc::new(anyhow!("{e}")));
                    return;
                }
            };
        }
    }
}

struct SingleStreamHandle {
    tx: broadcast::WeakSender<ArcLogsResult>,
    node_url: String,
}

fn new_live_stream_task(
    rest: DynProvider,
    pubsub_connect: WsConnect,
    from_block_n: u64,
) -> (
    broadcast::WeakSender<ArcLogsResult>,
    broadcast::Receiver<ArcLogsResult>,
) {
    let (tx, rx) = broadcast::channel(PROCESS_LIVE_STREAM_CHANNEL_LEN);
    let handle = tx.downgrade();
    tokio::spawn(async move {
        let pubsub = match tokio::time::timeout(
            CONNECT_TIMEOUT,
            ProviderBuilder::new().connect_pubsub_with(pubsub_connect),
        )
        .await
        {
            Ok(Ok(provider)) => DynProvider::new(provider),
            Ok(Err(error)) => {
                _ = tx.send(Err(Arc::new(anyhow!("pubsub connection: {error}"))));
                return;
            }
            Err(_) => {
                _ = tx.send(Err(Arc::new(anyhow!("timeout"))));
                return;
            }
        };
        let mut stream = pin!(live_stream(
            rest,
            pubsub,
            from_block_n,
            LATEST_MONAD_BLOCK_RATE
        ));

        while let Some(event) = stream.next().await {
            if tx.send(arc(event)).is_err() {
                return; // all receivers dropped
            }
        }
    });
    (handle, rx)
}

fn arc(event: anyhow::Result<BlockLogs>) -> ArcLogsResult {
    event.map(Arc::new).map_err(Arc::new)
}
