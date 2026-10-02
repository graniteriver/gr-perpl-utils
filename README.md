# gr-perpl-utils
Logic for working with Perpl DEX on the tokio runtime.

Includes some helpers not currently provided by the [upstream SDK](https://github.com/PerplFoundation/dex-sdk):
* Batch streaming of block logs.
* Optimised live streaming of safe/voted block logs without polling.
* Multi-consumer log stream sharing to deduplicate log fetching in the same process.

## Notes
Only Perpl mainnet exchange logs are currently supported.

Tested with a small number of Monad RPC providers. When trying a new provider, check it supports:
* `blockTimestamp` in `eth_getLogs` responses.
* `newHeads` websocket subscriptions with populated `logsBloom` headers. Live streams skip
  `eth_getLogs` for blocks whose bloom excludes the exchange, so a zeroed bloom silently drops logs.

### Compressed fetching
Consider enabling a _reqwest_ compression feature, e.g. `zstd`, for HTTP log fetching. Check provider
support. This only takes effect if your reqwest is semver-compatible with alloy's (currently `0.13`):
```toml
reqwest = { version = "0.13", default-features = false, features = ["zstd"] }
```

## Minimum supported rust compiler
Maintained with latest stable rust.
