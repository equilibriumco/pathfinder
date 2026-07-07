# Changelog

All notable changes to this project will be documented in this file.

More expansive patch notes and explanations may be found in the specific [pathfinder release notes](https://github.com/equilibriumco/pathfinder/releases).

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

- RPC metrics now carry a `block_target` label on `rpc_method_calls_total` (how a request targeted a block) and an `error_kind` label on `rpc_method_calls_failed_total` (the error variant name). The bundled Grafana dashboard (`utils/grafana/dashboard.json`) gains an expanded JSON-RPC view, and Prometheus recording rules for RPC latency quantiles are provided in `utils/prometheus/rpc-recording-rules.yml`.

### Changed

- RPC method-call latency is now exported as a Prometheus histogram in seconds, `rpc_method_calls_duration_seconds` (`_bucket`/`_sum`/`_count`), replacing the summary `rpc_method_calls_duration_milliseconds`. Update alerts and dashboards to the new name and derive quantiles with `histogram_quantile(...)` over the `_bucket` series; see `docs/docs/monitoring-and-metrics.md`.

### Fixed

- The default `--rpc.compiler.concurrency-limit` now respects the container's cgroup memory limit instead of the host's total RAM, so it no longer over-provisions compiler concurrency (risking OOM) when running in a memory-limited container.

## [0.22.7] - 2026-06-23

### Fixed

- Missing versioned constants for 0.14.3 causing some `starknet_traceBlockTransactions` calls to fail.

## [0.22.6] - 2026-06-18

### Added

- Pre-confirmed polling start threshold is now configurable with the `--sync.poll-pre-confirmed-threshold` CLI option.

### Changed

- `CANDIDATE` transaction status is deprecated and will never be returned by gateway or pathfinder. Using it as a filter in `subscribeNewTransactions` will fall back to `PRE_CONFIRMED` with a warning.
- The `blockifier` and `starknet_api` crates have been upgraded to 0.19.0-rc.2.
- The default value of `--rpc.compiler.concurrency-limit` is now bound to system memory via this equation: `min(floor((total_ram - margin) / compiler_max_memory_usage), num_cpu_cores)`, where:
  - `margin` is the value of the new CLI option `--compiler.concurrency-memory-margin-mib`, which defaults to 4GiB,
  - `compiler_max_memory_usage` is the value of the `--compiler.max-memory-usage-mib` CLI option, which defaults to 4GiB.

## [0.22.5] - 2026-06-08

### Added

- Parallel Sierra to CASM compilation processes during RPC requests are now limited to the number of CPU cores available by default, configurable with `--rpc.compiler.concurrency-limit` CLI option.
- Blocking writes to RPC WS connections now time out and are reported as errors. The timeout is configurable with `--rpc.websocket.send-timeout` CLI option.

### Fixed

- Pre-confirmed block polling re-fetching the full block on every poll due to a `block_identifier` field being deserialized under the wrong name.

## [0.22.4] - 2026-06-08

### Changed

- The limit for bytecode size in compilation is now defaulting to 81920 bytes and configurable with the new `--max-bytecode-size` CLI option.
- Committed-block sync now fetches block, state update, and signature in a single feeder-gateway request. Requires a feeder gateway that supports `includeSignature` on `get_state_update`.
- RPC request size and timeout limits are now configurable with `--rpc.request-max-size` and `--rpc.request-timeout` CLI options.
- RPC request batch size is now configurable with `--rpc.batch-size-limit` CLI option.
- The `blockifier` and `starknet_api` crates have been upgraded to 0.19.0-rc.0.

### Added

- Parallel subscriptions on a single WS connection are now limited to 1024 by default, configurable with `--rpc.websocket.max-subscriptions` CLI option.

## [0.22.3] - 2026-04-20

### Changed

- `starknet_getEvents` now returns `block_number` for events from pre-confirmed and pre-latest blocks.
- JSON-RPC input no longer accepts hex strings that are missing the `0x` prefix, in line with the Starknet API spec.

### Fixed

- `starknet_estimateFee` and `starknet_simulateTransactions` rejecting transactions with a non-zero tip when the account balance could cover the fee. The maximum L2 gas bound now accounts for the tip, matching the blockifier's fee calculation (`max_amount * (max_price_per_unit + tip)`).

## [0.22.2] - 2026-04-07

### Changed

- The `blockifier` and `starknet_api` crates have been upgraded to 0.18.0-rc.1.

### Added

- Blockifier libfunc list used for compilation verification is now defaulting to `audited` and configurable with the new `--blockifier.libfunc-list` CLI option.

## [0.22.1] - 2026-03-31

### Fixed

- The running Bloom filter for block events is now correctly rebuilt after unexpected shutdowns when blockchain pruning is enabled.
- Preconfirmed classes are not downloaded because `get_class_by_hash` for `pending` is now mapped to `latest`.

### Changed

- The `proof` field in broadcasted invoke v3 transactions is now a base64-encoded byte blob (`Vec<u8>`) instead of base64-encoded packed `u32` values (`Vec<u32>`). This reflects the upstream change to use compressed proofs.
- The `blockifier` crate has been upgraded to 0.18.0-rc.1 (and its dependencies to match that).
- HTTP request bodies for `add_invoke_transaction` are compressed with `gzip` if the transaction contains a nonempty proof. Compression can be disabled in custom networks using the `gateway.compress-requests` CLI flag.

## [0.22.0] - 2026-03-19

### Fixed

- The pre-latest block (introduced in Starknet 0.14.0) is now reported with `PRE_CONFIRMED` finality
  status instead of `ACCEPTED_ON_L2`. This aligns with the changes to block hash and commitment
  calculation in Starknet 0.14.2, where the pre-latest block can no longer be treated as fully
  accepted on L2. Affected RPC methods:
  - `starknet_getTransactionReceipt`
  - `starknet_getTransactionStatus`
  - `starknet_subscribeEvents`
  - `starknet_subscribeNewTransactionReceipts`
  - `starknet_subscribeNewTransactions`
  - `starknet_subscribeTransactionStatus`

### Changed

- The v10 JSON-RPC endpoint now supports final JSON-RPC v0.10.1 spec.
- `blockifier` has been upgraded to 0.18.0-dev.1, ensuring correctness of execution results on Starknet 0.14.2.

## [0.22.0-beta.3] - 2026-03-11

### Fixed

- `starknet_traceBlockTransactions` parameter `trace_flags` is not optional (as required by the spec).
- Starknet 0.14.2 blocks are now using the correct versioned constants.

### Changed

- The `--ethereum.url` option now requires a WebSocket URL (`ws://` or `wss://`). HTTP/HTTPS URLs are no longer automatically converted to WebSocket and will result in an error.
- The Pathfinder binary now has two subcommands:
  - `node` - runs the Pathfinder node as before, serving JSON-RPC and syncing with the network.
    This is the default subcommand and will run if no subcommand is passed.
  - `compile` - compile a Sierra class (passed via `stdin`) to CASM (output to `stdout`).

### Added

- Forwarding gateway HTTP error 413 when handling `starknet_addDeclareTransaction`, `starknet_addDeployAccountTransaction` and `starknet_addInvokeTransaction`.
- Two new CLI options for the `node` subcommand:
  - `compiler.max-memory-usage-mib` - maximum memory usage for the compiler process, in MiB.
  - `compiler.max-cpu-time-secs` - maximum (active) CPU time for the compiler process, in seconds.
- Pathfinder is now polling for DNS changes for the feeder gateway and gateway host name. By default the host names are resolved every 60s and the HTTP client connection pool is re-created to force reconnecting to the new address. The interval is configurable with the new `--gateway.check-for-dns-updates-interval` CLI option.
- `starknet_getStorageAt` now returns the last update block (in addition to storage value) if the `INCLUDE_LAST_UPDATE_BLOCK` flag was set in its input.
- `starknet_getStateUpdate` now supports an address filter.

## [0.22.0-beta.2] - 2026-02-16

### Fixed

- JSON-RPC serialization of `INITIAL_READS` is not compliant with the specification. Pathfinder returns `storage_key` properties for storage reads instead of the `key` property required by the spec.
- `starknet_addInvokeTransaction` is not forwarding `proof_facts` property to the Starknet gateway.

## [0.22.0-beta.1] - 2026-01-30

### Added

- Added support for the "return initial reads" feature introduced in JSON-RPC version 0.10.1:
  - Added `RETURN_INITIAL_READS` flag to `starknet_simulateTransactions` input simulation flags.
    Only supported if JSON-RPC version is 0.10.0 or higher.
  - Added a `trace_flags` field to `starknet_traceBlockTransactions` input. Currently the only available flag is `RETURN_INITIAL_READS`.
    Only supported if JSON-RPC version is V10 or higher.

- Preliminary support for JSON-RPC 0.10.1 `proof_facts` and `proof` transaction properties.

### Changed

- `starknet_simulateTransactions` now has a different response format based on whether or not
  the `RETURN_INITIAL_READS` flag was set in the input:
  1. If the flag was not set, the response is identical to previous RPC versions (an array of transaction simulations).
  2. If the flag was set, the response is an object with two fields:
     - "simulated_transactions" - an array of transaction simulations (previous RPC version output).
     - "initial_reads" - an `INITIAL_READS` object, containing an aggregate of all initial reads for the simulated transactions.

- `starknet_traceBlockTransactions` now has a different response format based on whether or not
  the `RETURN_INITIAL_READS` flag was set in the input:
  1. If the flag was not set, the response is identical to previous RPC versions (an array of transaction traces).
  2. If the flag was set, the response is an object with two fields:
     - "traces" - an array of transaction traces (previous RPC version output).
     - "initial_reads" - an `INITIAL_READS` object, containing an aggregate of all initial reads for the traced transactions.

## [0.21.5] - 2026-01-12

### Added

- Support for `deflate`-compressed responses from the feeder gateway.

### Fixed

- Pathfinder stops syncing on networks where response compression has been enabled on the feeder gateway.

## [0.21.4] - 2026-01-08

### Added

- Support for gzip-compressed responses from the feeder gateway.
- The new `--rpc.native-execution-force-use-for-incompatible-classes` CLI option can be used to force use of native execution even for pre-1.7.0 Sierra classes (where fee calculation is known to be inaccurate). Use this flag at your own risk.

### Changed

- `blockifier` has been upgraded to 0.16.0-rc.3.

## [0.21.3] - 2025-12-03

### Added

- The new histogram metric `rpc_method_calls_duration_milliseconds` has been added to expose JSON-RPC method call latency data.

### Changed

- `blockifier` has been upgraded to 0.16.0-rc.2.
- Pathfinder no longer returns `event_commitment` and `transaction_commitment` values for Starknet blocks older than Starknet version 0.13.2.

### Fixed

- `starknet_traceTransaction` times out for some transactions because fetching transaction traces from the feeder gateway fails due to some unknown fields in the response.

## [0.21.2] - 2025-11-27

### Changed

- Pathfinder now serves the JSON-RPC 0.10.0 API on the `v0_10` routes.
- The size of the block trace cache is now configurable by the new `--rpc.block-trace-cache-size` CLI argument.

## [0.21.1] - 2025-11-20

### Fixed

- Pathfinder exits after receiving an internal server error from the feeder gateway.
- `starknet_estimateFee` and `starknet_simulateTransactions` fails if one of the transactions is using a class that has been declared by a simulated DECLARE transaction in the batch.
- Cairo Native is not working correctly with Docker images published on Docker Hub due to a linker error.

## [0.21.0] - 2025-11-11

### Added

- Support for Starknet 0.14.1 networks.

### Fixed

- Inconsistent state updates when using "pre_confirmed" block identifier with `starknet_call`, `starknet_estimateFee` and `starknet_simulateTransactions` JSON-RPC methods.

## [0.20.5] - 2025-10-16

### Fixed

- `starknet_estimateFee` fails if the validate entry point runs ouf of gas during L2 fee binary search.
- Pathfinder stops syncing for a minute after logging an "L2 sync process terminated with: Download block from sequencer" error.
- `starknet_simulateTransactions` sometimes fails for `DEPLOY_ACCOUNT` transactions if the `SKIP_FEE_CHARGE` simulation flag is not set. This happens even though the account address has the sufficient funds to execute the transaction.

## [0.20.4] - 2025-09-25

### Fixed

- Pathfinder panics with "Block number Z is not in the range X..=Y" after starting up from an old Sepolia testnet database snapshot.
- Pathfinder returns internal errors for some `starknet_getTransactionStatus` / `starknet_getTransactionReceipt` queries for transactions in the pre-confirmed block.
- L2 sync stalls for a while after logging an "L2 sync process terminated with: Fetch signature for BlockNumber(XXX) from sequencer" error.

## [0.20.3] - 2025-09-09

### Changed

- Polling of pre-confirmed data has been improved to reduce end-to-end latency. Pathfinder now polls both the pre-latest and pre-confirmed data from the feeder gateway.

## [0.20.2] - 2025-09-05

### Added

- reorg notifications for `starknet_subscribeNewTransactions` and `starknet_subscribeNewTransactionReceipts`

### Changed

- `blockifier` has been upgraded to version 0.15.0-rc.4.

### Fixed

- `starknet_subscribeNewTransactions` doesn't accept the `RECEIVED` finality status filter.
- Pathfinder gets stuck in a loop and prints "State root mismatch" errors after starting up from a database with current state that has been re-orged.
- `starknet_traceTransaction` and `starknet_traceBlockTransactions` falls back to fetching transaction traces for Starknet mainnet block range 1943704-1952704 (inclusive). Local re-execution would lead to a different result due to a sequencer issue that was present when these blocks were produced.
- Pathfinder gets stuck syncing and stops responding to JSON-RPC requests.

## [0.20.1] - 2025-09-02

### Added

- `--rpc.disable-batch-requests` CLI option, for instances not wishing to support batch requests.

### Fixed

- Pathfinder cannot recover after a reorg involving more than 1000 blocks. L2 reorg fails with error "Reorg exceeded local blockchain cache".

## [0.20.0] - 2025-08-26

### Changed

- `blockifier` has been upgraded to version 0.15.0-rc.3.

### Fixed

- JSON-RPC response reflects an inconsistent state after receiving a notification over a Websocket subscription.
- `starknet_getClassAt` now returns `DEPRECATED_CAIRO_ENTRY_POINT.offset` as a hex string instead of an integer. This change aligns the response format with the specification, which expects `NUM_AS_HEX` for this field.
- Make dialing procedure of bootnodes more permissive.

## [0.19.0] - 2025-08-12

### Changed

- Pathfinder now implements the final JSON-RPC 0.9.0 specification.
- `blockifier` has been upgraded to version 0.15.0-rc.2.
- The default JSON-RPC listen address has been changed to the IPv6 wildcard address in our Docker images. This avoids problems on IPv6-enabled hosts where `localhost` resolves to `::1`.
- The default JSON-RPC version (served on the '/' route) has been changed to v08.
- JSON-RPC `starknet_estimateFee` and `starknet_simulateTransactions` now use non-strict nonce checking when using the `SKIP_VALIDATE` flag. That is, the nonce value needs to be larger than the last used value but no exact match is required.
- `starknet_getTransactionStatus` now returns ACCEPTED\_\* only when that status is known locally, not when it's received from the gateway for an otherwise-unknown transaction.
- value of the `--sync.poll-interval` command-line option can now specify fractional seconds

### Fixed

- `starknet_getEvents` returns an incomplete result set if the block range includes empty blocks.

## [0.18.0] - 2025-07-14

### Added

- `pathfinder_lastL1AcceptedBlockHashAndNumber`, a pathfinder extension analogous to `starknet_blockHashAndNumber`
- Pathfinder now accepts the `l1_accepted` `BLOCK_TAG` on the JSON-RPC interface. This can be used to reference the latest L1 accepted block known by the node.

### Changed

- All WebSocket API routes (served on `/ws`) are now deprecated and will be removed on the next release. Additionally, Pathfinder no longer supports `pathfinder_subscribe` and `pathfinder_unsubscribe` methods on these routes.
- Some of the CLI options that are no longer needed have also been removed:
  - `rpc.websocket.buffer-capacity`
  - `rpc.websocket.topic-capacity`

### Fixed

- The default value of L2 gas price for historical blocks has been changed from 0 to 1 (for both WEI and FRI), to agree with the feeder gateway default.
- Trying to enable blockchain pruning on an archive database does indeed enable pruning in the database despite printing an error that pruning cannot be enabled.
- When running on Starknet < 0.14.0 and querying for the "pre_confirmed" block header Pathfinder returns a non-conformant response that has "parent_hash" instead of "block_number".
- Transaction execution errors and contract errors are returning unstructured error information on the JSON-RPC 0.9.0 API.

## [0.17.0] - 2025-06-29

### Added

- Pathfinder now supports _syncing_ from Starknet 0.14.0. Support is still incomplete, execution and compilation of new classes will likely fail for new classes until a further upgrade.
- Pathfinder now supports storing only the latest state of the blockchain history. This can be configured with the '--storage.blockchain-history' CLI option.
  - Accepted values are:
    - "archive" (default) – Full history of the blockchain is stored.
    - "N" – An integer specifying the number of historical blocks to store, in addition to the latest block (N + 1 blocks will be stored).

  - Affected JSON-RPC methods are:
    - `starknet_call`
    - `starknet_estimateFee`
    - `starknet_estimateMessageFee`
    - `starknet_getBlockTransactionCount`
    - `starknet_getBlockWithTxHashes`
    - `starknet_getBlockWithTxs`
    - `starknet_getBlockWithReceipts`
    - `starknet_getClass`
    - `starknet_getClassAt`
    - `starknet_getClassHashAt`
    - `starknet_getEvents`
    - `starknet_getNonce`
    - `starknet_getStateUpdate`
    - `starknet_getStorageAt`
    - `starknet_getStorageProof`
    - `starknet_getTransactionByBlockIdAndIndex`
    - `starknet_simulateTransactions`
    - `starknet_traceBlockTransactions`

  - With pruning enabled, affected JSON-RPC method requests will only succeed if the requested block is within the last N + 1 blocks.
  - The choice between `archive` and `pruned` mode is made once, when creating the database. Once chosen, it cannot be changed without creating a new database.
  - It is possible to change the number of blocks stored in pruned mode between runs, using the same CLI option with a different value for N.
  - Note that the number of blocks stored is relative to:
    a. The latest L1 checkpoint if it exists and the latest L2 block is ahead of it
    b. The latest L2 block if it is behind the latest L1 checkpoint or no L1 checkpoints have been received by the node (practically unreachable)

- `starknet_getTransactionStatus` now returns RECEIVED even when the gateway cannot find the transaction, provided the transaction was successfully sent by the responding node within the last 5 minutes.
- Pathfinder now allows the users to configure the number of historical messages to be streamed via the [webscoket API](https://equilibriumco.github.io/pathfinder/interacting-with-pathfinder/websocket-api). This can be done using the `--rpc.websocket.max-history` CLI option.
  - Accepted values are:
    - "unlimited" - All historical messages will be streamed.
    - "N" - An integer specifying the number of historical messages to be streamed.

  - This option defaults to N = 1024 if not specified.

### Fixed

- `starknet_unsubscribe` does not accept subscription IDs as strings.
- `starknet_estimateFee` is failing for Braavos DEPLOY_ACCOUNT transactions involving a new Sierra 1.7.0 class.
- `starknet_traceBlockTransactions` fails for blocks <= 2687.
- JSON-RPC v0.8.1 `starknet_estimateFee` method call fails if the account balance is zero.
- Websockets can be used even when `--rpc.websocket.enabled` isn't set.

### Changed

- `--rpc.get-events-max-uncached-event-filters-to-load` CLI option has been replaced with `rpc.get-events-event-filter-block-range-limit`. The new option serves the same purpose of preventing queries from taking too long, but it should be clearer in its intent.
- `blockifier` has been upgraded to version 0.15.0-rc.1, adding initial support for Starknet 0.14.0 execution.
- The default value of the `--sync.poll-interval` configuration parameter has been changed to 1 seconds. Pathfinder is now polling both the latest block information and the pending/pre-confirmed block every second by default.

### Removed

- `pathfinder_getProof`, `pathfinder_getClassProof` and `pathfinder_getTransactionStatus` (obsoleted by standardized `starknet_*` APIs).

## [0.16.5] - 2025-05-22

### Fixed

- `starknet_estimateFee` is failing for Braavos DEPLOY_ACCOUNT transactions involving a new Sierra 1.7.0 class.
- `starknet_unsubscribe` does not accept subscription IDs as strings.
- `starknet_traceBlockTransactions` fails for blocks <= 2687.

## [0.16.4] - 2025-04-15

### Fixed

- `starknet_simulateTransactions` returns an error instead of the trace of the reverted transaction if the L2 gas cap is insufficient.
- `starknet_traceTransaction` and `starknet_traceBlockTransactions` returns an internal error with no details upon encountering a transaction execution error.
- `starknet_getEvents` returns an incomplete set of events for some queries over a block range larger than 106k blocks.

## [0.16.3] - 2025-04-03

### Added

- The JSON-RPC 0.8.0 API can now be accessed under `/ws/rpc/v0_8` as well if Websockets are enabled. This is equivalent to the `/rpc/v0_8` path and is provided only as a convenience feature.

### Fixed

- `starknet_subscribeEvents` subscriptions stop sending notifications.
- Broken aggregate bloom filter migration has been updated to work properly. If you migrated from a database running in archived mode, please [re-download our latest snapshot](https://equilibriumco.github.io/pathfinder/database-snapshots) and re-run the migrations.
- `starknet_getStateUpdate` has `new_root` and `old_root` swapped.

### Changed

- JSON-RPC 0.8 `subscription_id` is now a string.
- Pathfinder now supports the JSON-RPC 0.8.1 specification. In this new version, the Websocket `subscription_id` type has been changed to `string`.

## [0.16.2] - 2025-03-12

### Added

- Support for custom versioned constants for multiple Starknet versions.
- Support for Starknet 0.13.5.

### Fixed

- `starknet_estimateFee` returns an internal error for v3 transactions with L2 gas `max_price_per_unit` set to zero.
- `starknet_getCompiledCasm` returns CASM wrapped in a `casm` property.
- `starknet_traceBlockTransactions` fails on Starknet 0.13.4 when a fallback to fetching from the feeder gateway is required.
- Websocket subscriptions to the `latest` block do not send notifications for the current latest block.
- `starknet_subscribeEvents` subscriptions send matching events only from the `latest` block, not as soon as those show up in `pending`.
- Pathfinder stops receiving L1 state updates.

## [0.16.1] - 2025-02-24

### Fixed

- Pathfinder does not return `transaction_hash` in `starknet_getBlockWithTxns` response.
- Custom networks cannot be configured for pre-0.13.4 Starknet versions,

## [0.16.0] - 2025-02-19

### Added

- Support for Starknet 0.13.4.
- Support for the JSON-RPC 0.8.0-rc3 API, including Websocket subscriptions.
- Graceful shutdown upon SIGINT and SIGTERM with a default grace period of 10 seconds, configurable via `--shutdown.grace-period`.

### Removed

- `storage_commitment` and `class_commitment` fields from the `pathfinder_subscribe_newHeads` method response.

### Fixed

- `pathfinder_getProof`, `pathfinder_getClassProof` return `ProofMissing` (10001) when Pathfinder is in `archive` mode and queried block's tries are empty.
- `starknet_syncing` returns `u64::MAX` as the starting block number when starting from scratch.

### Changed

- Use aggregate Bloom filters for `starknet_getEvents` to improve performance.

## [0.15.3] - 2025-01-10

### Changed

- Cairo 0 class definition size is now capped at 4 MiB.

## [0.15.2] - 2024-12-04

### Fixed

- Pathfinder fails to properly do a reorg due to a SQL statement referring a table that does not exist.
- `--rpc.get-events-max-uncached-bloom-filters-to-load` setting is ineffective.

## [0.15.1] - 2024-12-02

### Fixed

- `starknet_getBlockWithReceipts` returns `transaction_hash` within the `transaction` object making the response not strictly spec compliant. Fixed on the JSON-RPC 0.8 interface.
- JSON-RPC 0.7 methods returning block headers are including the `l2_gas` property.

### Changed

- Pathfinder is now compiled with arithmetic overflow checks enabled in release mode to mitigate potential issues.

## [0.15.0] - 2024-11-21

### Added

- Pathfinder now fetches data concurrently from the feeder gateway when catching up. The `--gateway.fetch-concurrency` CLI option can be used to limit how many blocks are fetched concurrently (the default is 8).
- `--disable-version-update-check` CLI option has been added to disable the periodic checking for a new version.
- Add `pathfinder_getClassProof` endpoint to retrieve the Merkle proof of any class hash in the class trie.
- add `process_start_time_seconds` metric showing the unix timestamp when the process started.
- `--log-output-json` CLI option has been added to output the Pathfinder log in line-delimited JSON.
- Preliminary support has been added for the new JSON-RPC 0.8.0-rc1 specification.

### Changed

- Ethereum RPC API now requires Websocket endpoints (prev. HTTP). If an HTTP url is provided instead, Pathfinder will attempt to connect vía Websocket protocol at that same url.
- JSON-RPC 0.4.0 and 0.5.0 API support has been removed.
- JSON-RPC API version 0.7 is now served by default on the `/` path.

### Fixed

- `starknet_getBlockWithTxs` works with empty blocks`

## [0.14.5] - 2024-12-02

### Fixed

- Pathfinder is now compiled with arithmetic overflow checks enabled in release mode to mitigate potential issues.

## [0.14.4] - 2024-10-03

### Fixed

- Pathfinder stops syncing Sepolia testnet at block 218484 because of a block hash mismatch.

## [0.14.3] - 2024-09-23

### Fixed

- Pathfinder occasionally corrupts its Merkle trie storage during reorgs and then stops later with a "Node X at height Y is missing" or "Stored node's hash is missing" error.

## [0.14.2] - 2024-09-03

### Fixed

- Pathfinder sometimes returns an INVALID_CONTINUATION_TOKEN error when requesting events from the pending block and providing a continuation token.
- `starknet_getEvents` incorrectly returns pending events if `from_block` is greater than latest_block_number + 1.
- `starknet_getEvents` incorrectly does not return pending events if `from_block` is `pending` and `to_block` is missing.

### Added

- `--sync.l1-poll-interval` CLI option has been added to set the poll interval for L1 state. Defaults to 30s.
- Support for Starknet 0.13.2.1.

## [0.14.1] - 2024-07-29

### Fixed

- Pathfinder does not properly limit the number of concurrent executors when using the `--rpc.execution-concurrency` CLI option.
- Pathfinder returns non-conforming `STRUCT_ABI_ENTRY` objects in response to `starknet_getClass` requests.
- Pathfinder returns `starknet_getStateUpdate` responses that are non-conformant with the specification if there are replaced classes in the response.

## [0.14.0] - 2024-07-22

### Added

- Support for Starknet v0.13.2.
- Pathfinder now creates a new directory if the database path specified does not exist.
- Pathfinder now has a CLI option (`--rpc.custom-versioned-constants-json-path`) to allow loading a custom versioned constants JSON file. When specified the contents of the file is then used instead of the _latest_ constants built into the blockifier crate during execution of Cairo code.

### Fixed

- Pathfinder exits with an error when detecting a one-block reorg if `--storage.state-tries` is set to `0`.
- Pathfinder returns an internal error for `starknet_getTransactionReceipt` requests where `steps` would be zero in COMPUTATION_RESOURCES.

## [0.13.2] - 2024-06-24

### Fixed

- `starknet_getTransactionReceipt` responses are missing the `payload` property in `MSG_TO_L1` objects on the JSON-RPC 0.7 interface.
- `starknet_traceTransaction` and `starknet_traceBlockTransactions` returns L2 to L1 messages from inner calls duplicated.

## [0.13.1] - 2024-06-19

### Fixed

- `starknet_getTransactionReceipt` responses are not compliant with the JSON-RPC specification:
  - L1 handler receipts throw an internal error
  - `execution_status` and `revert_reason` properties are missing

## [0.13.0] - 2024-06-18

### Fixed

- `starknet_getBlockWithTxHashes` and `starknet_getBlockWithTxs` returns the pending block with a `status` property that's not in the JSON-RPC specification. This has been fixed for the JSON-RPC 0.7 API endpoint.
- `starknet_traceBlockTransactions` and `starknet_traceTransaction` now falls back to fetching the trace from the feeder gateway for all blocks before Starknet 0.13.1.1.

### Added

- `/ready/synced` endpoint to check if the JSON RPC API is ready _and_ also check if the node is synced. Useful for Docker nodes which only want to be available after syncing.
- Websocket endpoints now serve all JSON-RPC methods, not just `pathfinder_subscribe` and `pathfinder_unsubscribe`. Version-specific endpoints have been added for the 0.6 and 0.7 JSON-RPC API and the Pathfinder extension API.
- Websocket endpoint for subscribing to events and transaction status changes.

### Changed

- Improved compactness of transaction and nonce data, resulting in noticeable storage savings.

## [0.12.0] - 2024-04-23

### Added

- Pathfinder now supports storing only the latest state of the Merkle tries. This can be enabled by specifying '--storage.prune-state-tries true' on the command line when creating a new database.
  - When enabled, storage proofs can be generated only for the latest block.
  - Pruned merkle tries take significantly less disk space than full ones.
  - Pathfinder stores this setting in its database and defaults to using that.
  - Once set pruning cannot be enabled/disabled for non-empty databases.
  - Pruning achieves a ~75% reduction in overall storage compared to archive.

### Removed

- Support for Goerli testnet

### Changed

- Improved compactness of state diff and transaction data, resulting in noticeable storage savings.
  - Roughly a 20% reduction for a state trie archive node, or a 45% reduction for a pruned variant.

## [0.11.6] - 2024-04-10

### Changed

- blockifier has been upgraded to version 0.6.0-rc.2.

## [0.11.5] - 2024-04-02

### Changed

- blockifier has been upgraded to version 0.6.0-rc.1.

## [0.11.4] - 2024-03-28

### Added

- Support for Starknet v0.13.1.1 (lowered declare transaction fees).

### Fixed

- `starknet_estimateFee` and `starknet_simulateTransactions` can return fee estimates below the minimum fee expected by the sequencer for trivial transactions.

## [0.11.3] - 2024-03-13

### Fixed

- `starknet_estimateFee` and `starknet_simulateTransactions` always uses CALLDATA L1 DA mode if there is no pending block.
- `starknet_getTransactionStatus` reports gateway errors as `TxnNotFound`. These are now reported as internal errors.
- `starknet_addXXX` requests to the gateway use the configured gateway timeout, often causing these to timeout while waiting for
  a gateway response. These instead now use a much longer timeout.

## [0.11.2] - 2024-03-07

### Fixed

- `starknet_getTransactionStatus` reports gateway errors as `TxnNotFound`. These are now reported as internal errors.
- Sync process leaves a zombie task behind each time it restarts, wasting resources.
- `starknet_getEvents` does not return a continuation token if not all events from the last block fit into the result page.
- `starknet_addXXX` requests to the gateway use the configured gateway timeout, often causing these to timeout while waiting for
  a gateway response. These instead now use a much longer timeout.

### Changed

- Default sync poll reduced from 5s to 2s. This is more appropriate given the lower block times on mainnet.

## [0.11.1] - 2024-03-01

### Fixed

- Transaction hash calculation for transactions using the "query version" flag is broken for `starknet_estimateFee` and `starknet_simulateTransactions`.

## [0.11.0] - 2024-02-27

### Changed

- `starknet_getEvents` implementation is now using a much simpler implementation that no longer relies on SQLite queries. In general this leads to more consistent query times and a roughly 20% smaller database.
  - The migration step involves computing Bloom filters for all blocks and dropping database tables no longer needed. This takes more than one hour for a mainnet database.
  - The new `storage.event-bloom-filter-cache-size`, `rpc.get-events-max-blocks-to-scan` and `rpc.get-events-max-bloom-filters-to-load` arguments control some aspects of the algorithm.
- The memory allocator used by pathfinder has been changed to jemalloc, leading to improved JSON-RPC performance.
- Improved poseidon hash performance.
- Default RPC version changed to v0.6.

### Added

- Support for Starknet v0.13.1.
- Support for RPC v0.7.
- The request timeout for gateway and feeder-gateway queries is now configurable using `gateway.request-timeout` (`"PATHFINDER_GATEWAY_REQUEST_TIMEOUT"`).

### Fixed

- Websocket control frames aren't handled.

## [0.10.6] - 2024-02-03

### Added

- Performance improvements for `starknet_traceTransaction` and `starknet_traceBlockTransactions` via caching
- Performance improvements for the sync process's interaction with the feeder gateway
  - more aggressive timeouts and retry strategy
  - polling pending and latest blocks concurrently
  - using combined requests where possible

## [0.10.5] - 2024-02-02

**YANKED**

This release accidentally contained a mixture of unreleased code and performance improvements, including a somewhat broken database migration.

Users should not use this version.

## [0.10.4] - 2024-02-02

### Fixed

- `starknet_getEvents` incorrectly evaluates empty sub-lists in key filters for pending events
- The RPC error UNEXPECTED_ERROR is an object not a string

## [0.10.3] - 2024-01-04

### Added

- RPC parsing failures now include the error reason when its an invalid JSON-RPC request (invalid request params already include the error reason).

## [0.10.3-rc1] - 2023-12-22

### Added

- `gateway-api-key API_KEY` configuration option. If enabled, each time a request is sent to the Starknet gateway or the feeder gateway a `X-Throttling-Bypass: API_KEY` header will be set.

## [0.10.3-rc0] - 2023-12-14

### Changed

- Fee estimations are now compatible with starknet v0.13 and _incompatible_ with starknet v0.12.3.

### Added

- Added `x-request-id` header to RPC responses. If the request does not have the header set then an ID is generated. This can be used to identify a specific caller's request/response within the node's logs. Duplicate IDs are possible since they can be set by the caller, so we recommend making your's identifiable with a prefix or using a GUID.
- Improved tracing for RPC requests. These are all logged on `trace` level under the `pathfinder_rpc` module. Additional information can also be obtained from `tower_http` module. These can be enabled by appending `pathfinder_rpc=trace,tower_http=trace` to `RUST_LOG` environment variable.
  - Request payload is now logged before execution begins.
  - Logs now include `x-request-id` header value which can be used to correlate with client requests/responses.
  - Batch logs also include the index within a batch.
- RPC parsing errors now expose the failure reason as part of the errors `data` field.

### Fixed

- v0.5 `starknet_simulateTransactions` returns internal error instead of `ContractError` for reverted transactions.
- v0.6 `starknet_getTransactionReceipt`
  - `EXECUTION_RESOURCES` fields are hex-strings instead of integers
  - `segment_arena_builtin` resource is missing
  - v3 transaction price unit type is `STRK` instead of `FRI`
- v0.6 `starknet_estimateFee`, `starknet_simulateTransactions`
  - v3 transaction hashes are computed incorrectly when using the "query" flag, causing validation errors
  - `unit` field is missing from fee estimation results
- v0.6 `starknet_addDeployAccountTransaction`
  - the gateway does not properly return address information for v3 transactions, we now compute the address ourselves
- `starknet_getEvents`
  - query strategy selection for some events involving a filter on very common keys is slow
- Execution performance for calls involving the `pending` blocks is much better for trivial calls (like `balanceOf`).

### Changed

- JSON-RPC v0.6 now serves `0.6.0` for `starknet_specVersion`.

## [0.10.1] - 2023-12-05

### Fixed

- Execution errors are opaque and don't always include the root cause.
- Pathfinder uses incorrect fee token gas price for `gas_consumed` calculation for v3 transactions.
- `starknet_traceTransaction` sometimes returns an "Invalid order number for L2-to-L1 message" error.
- `starknet_getTransactionByHash` and `starknet_getTransactionByBlockIdAndIndex` return v3 transactions mapped to v1.

### Changed

- JSON-RPC v0.6 support has been updated to v0.6.0-rc5 of the specification.

## [0.10.0] - 2023-11-29

### Added

- Support for RPC v0.6.0-rc4 via the `/rpc/v0_6` endpoint. Note that this does not include the `/rpc/v0.6` endpoint as the underscore is now the standard across node implementations.
- Configuration options to selectively enable/disable parts of the node. This can be useful to run tests or benchmarks with isolated components e.g. test RPC methods without the sync process updating the database.
  - `rpc.enable` configuration option to enable/disable the RPC server. Defaults to enabled.
  - `sync.enable` configuration option to enable/disable the sync process. Defaults to enabled.
- Support for Sepolia testnet via `--network testnet-sepolia`
- Support for Sepolia integration via `--network integration-sepolia`
- Support for Starknet 0.13.0.

### Changed

- Default RPC version is now v0.5 (was v0.4). This can be manually configured to any version.
- Goerli testnet network selection is now `--network testnet-goerli`, removed `--network testnet`
- Goerli integration network selection is now `--network integration-goerli`, removed `--network testnet`
- Reworked `newHeads` subscription output to more accurately represent header data.

### Removed

- Support for `testnet2`
- Support for RPC v0.3
- `--poll-pending` configuration option. This is now always enabled and set to 2s.
- `--python-subprocesses` configuration option. This is superseded by `--rpc.execution-concurrency`.

## [0.9.7] - 2023-11-21

### Fixed

- `get_block_hash` syscall returns `0x0` for the latest available block (current - 10) when executing `starknet_trace*` methods

## [0.9.6] - 2023-11-20

### Fixed

- RPC v0.5 incorrectly has a status field in pending `starknet_getBlockWithXXX` responses.
- Error details for many execution-related issues were not properly sent back to the JSON client and were logged on WARN level instead.

## [0.9.5] - 2023-11-09

### Added

- Host JSON-RPC on `/rpc/v0_x` in addition to the existing `/rpc/v0.x` endpoints. This applies to all supported JSON-RPC versions.

### Changed

- RPC errors now only include the root cause if white-listed as non-sensitive
- RPC v0.5 updated from v0.5.0 to v0.5.1

### Fixed

- JSON-RPC v0.5 transaction traces now have the required `type` property.
- JSON-RPC v0.5 L1 Handler receipt is missing the `message_hash` property.
- JSON-RPC v0.5 use wrong field names in ExecutionResources

## [0.9.4] - 2023-11-02

### Changed

- RPC methods now use an empty block for pending data if no viable pending data is present. Requests for pending data will no longer fail with `BlockNotFound` if there is no pending data available, but rather use an empty block on-top of the latest local block.

### Fixed

- RPC errors do not always include the root cause. For example, some gateway error messages are not output when pathfinder forwards the request.
- RPC trace object uses wrong property `reverted_reason` instead of `revert_reason`.
- RPC execution steps limits have been updated to match the setup of the Starknet sequencer.
- RPC query version bit is now correctly passed through to the blockifier.

### Added

- RPC v0.5.0 support
- Added the ability to concurrently process RPC batches, see the `rpc.batch-concurrency-limit` CLI argument.
- The `pathfinder_build_info` metric having `version` label to report current version of Pathfinder.

## [0.9.3] - 2023-10-16

### Fixed

- RPC server does not accept `charset=utf-8` in the `Content-Type` header
- Out-of-memory crash caused by rare execution queries

## [0.9.2] - 2023-10-13

### Fixed

- RPC server does not set `content-type: application/json`
- Restored the Websocket subscription features with new configuration keys: `rpc.websocket.enabled`
  `rpc.websocket.buffer-capacity`, `rpc.websocket.topic-capacity`

## [0.9.1] - 2023-10-11

### Fixed

- A storage regression causing reorgs to be slow has been fixed.

## [0.9.0] - 2023-10-10

### Fixed

- State tree updates are slow on disks with low disk IO or high latency (e.g. network attached storage).
- Pathfinder now exits with a non-zero exit status if any of the service tasks (sync/RPC/monitoring) terminates.
- Rare edge case where duplicate blocks caused the sync process to halt due to a `A PRIMARY KEY constraint failed` error.
- Querying a descync'd feeder gateway causes sync process to end due to missing classes.
- `starknet_getStorageAt` no longer returns ContractNotFound when querying for non-existent keys for contracts deployed in the pending block.
- `starknet_getNonce` no longer returns ContractNotFound when querying for nonce of contracts deployed in the pending block.

### Changed

- Reworked state tree storage schema. This is not backwards compatible and requires a re-sync.
- Switched to a custom JSON-RPC framework to more easily support multiple specification versions. This may lead to some unexpected changes in behaviour.

### Removed

- JSON-RPC subscription support (`pathfinder_newHeads`). This is temporary while we re-add support to our new JSON-RPC framework.

## [0.8.2] - 2023-09-28

### Fixed

- JSON-RPC requests containing a Cairo 0 class definition were requiring the `debug_info` property to be present in the input program. This was a regression caused by the execution engine change.
- Performance for the `starknet_getEvents` JSON-RPC method has been improved for queries involving the pending block.

### Added

- `--sync.verify_tree_node_data` which enables verifies state tree nodes as they are loaded from disk. This is a debugging tool to identify disk corruption impacting tree node data. This should only be enabled when debugging a state root mismatch.

- RPC v0.4 methods:
  - `starknet_traceTransaction`
  - `starknet_traceBlockTransactions`
- Class cache for execution queries which provides a modest increase in performance speed.

### Changed

- `starknet_getEvents` continuation token formatting. The new format is incompatible with the previous format used v0.8.1 and older.

## [0.8.1] - 2023-09-07

### Fixed

- JSON-RPC requests with unknown parameters are rejected (unknown params were previously ignored)

### Changed

- Execution is backed by a Rust-based VM improving performance. We no longer depend on Python code in pathfinder.

## [0.8.0] - 2023-08-30

### Changed

- `cairo-lang` upgraded to 0.12.2
- Cairo compiler upgraded to 2.1.1
- default RPC API version changed from v0.3 to v0.4

### Fixed

- RPC v0.3 `starknet_estimateFee` example
- RPC method names could be prefixed with API version
- `starknet_getNonce` returns invalid values when queried by hash

### Added

- Added the `rpc.root-version` command-line option (and the corresponding PATHFINDER_RPC_ROOT_VERSION environment variable)
  to control the version of the JSON-RPC API pathfinder serves on the `/` path

## [0.7.2] - 2023-08-16

### Fixed

- RPC v0.4 `starknet_getTransactionByHash` uses the wrong error code for `TXN_HASH_NOT_FOUND`
- Querying `starknet_getClassAt` and `starknet_getClassHashAt` by block hash incorrectly returns contract not found
- On Starknet 0.12.2 pathfinder now provides consistent pending data
- RPC v0.4 Declare v0 and Invoke v0 contain the nonce field

## [0.7.1] - 2023-08-08

### Fixed

- RPC v0.4 `starknet_getTransactionReceipt` incorrect execution and finality status names
- `pathfinder_getTransactionStatus` fails to parse v0.12.1 gateway replies

### Changed

- RPC v0.4.0 support (previously supported v0.4.0-rc3)

## [0.7.0] - 2023-07-27

### Added

- RPC v0.4 support on `/rpc/v0.4/`
- control log color output via `--color auto|always|never`
- if Sierra to CASM compilation fails we now fall back to fetching CASM from the gateway
- Negate bot spam on response metrics by returning `Ok(200)` on `/` RPC queries. Web crawlers and bots often poke this endpoint which previously skewed response failure metrics when these were rejected.

### Fixed

- system contract updates are not correctly stored
- `starknet_simulateTransaction` fails for transactions sending L2->L1 messages
- deprecated error code 21 `INVALID_MESSAGE_SELECTOR` is used in RPC v0.3

### Changed

- `cairo-lang` upgraded to 12.2.1a0
- Cairo compiler upgraded from 2.0.2 to 2.1.0-rc1

### Removed

- support for RPC v0.2

## [0.6.7] - 2023-07-17

### Fixed

- some cairo 0 classes are not downloaded which can cause execution methods to fail
  - this bug was introduced in v0.6.4 and requires a resync to fix
- gateway error messages are not passed through for `add_xxx_transaction` methods
- fee estimation is under-estimating most declare transactions by factor 2
- `pathfinder_getTransactionStatus` still returns `PENDING` instead of `ACCEPTED_ON_L2`

### Added

- `cairo-lang` upgraded to 0.12.0

## [0.6.6] - 2023-07-10

### Fixed

- stack overflow while compiling Sierra to CASM

## [0.6.5] - 2023-07-07

### Fixed

- pending data from the gateway is inconsistent
  - this could exhibit as RPC data changing status between `pending | L2 accepted | not found`, especially noticeable for transactions.

### Changed

- substantially increase the character limit of execution errors
  - previously, the RPC would return a highly truncated error message from the execution vm

## [0.6.4] - 2023-07-05

### Fixed

- Pending data is not polled for starknet v0.12 due to an HTTP error code change from the gateway.
- Transaction receipts missing `from_address` in `MSG_TO_L1`.

## [0.6.3] - 2023-06-29

### Fixed

- Sierra class hash not in declared classes sync bug

### Changed

- use all libfunc list instead of experimental for sierra compilation

## [0.6.2] - 2023-06-29

### Added

- `starknet_estimateMessageFee` for JSON-RPC v0.3.1 to estimate message fee from L1 handler.
- sync-related metrics
  - `current_block`: the currently sync'd block height of the node
  - `highest_block`: the height of the block chain
  - `block_time`: timestamp difference between the current block and its parent
  - `block_latency`: delay between current block being published and sync'd locally
  - `block_download`: time taken to download current block's data excluding classes
  - `block_processing`: time taken to process and store the current block
- configuration for new block polling interval: `--sync.poll-interval <seconds>`
- Starknet v0.12.0 support
  - sierra v2.0.0 support
  - `cairo-lang` upgraded to 0.12.0a0

### Fixed

- reorgs fail if a class declaration is included in the reorg
- sync can fail if db connection pool is held saturated by rpc queries
- uses `finalized` (reorg-safe) L1 state instead of `latest`
- `starknet_getEvents` times out for queries involving a large block range

### Changed

- dropped upgrade support for pathfinder v0.4 and earlier
- separate db connection pools rpc, sync and storage
- increased the number of rpc db connections

## [0.6.1] - 2023-06-18

### Fixed

- class hash mismatch for cairo 0 classes with non-ascii text

## [0.6.0] - 2023-06-14

### Fixed

- `starknet_simulateTransaction` requires `transactions` instead of `transaction` as input field.
- gateway's error message is hidden when submitting a failed transaction
- `starknet_getEvents` is very slow for certain filter combinations

### Changed

- default RPC API version changed from v0.2 to v0.3
- disallow JSON-RPC notification-style requests

## [0.5.6] - 2023-05-25

### Added

- Starknet v0.11.2 support
  - Sierra compiler v1.1.0-rc0
  - `cairo-lang` upgraded to 0.11.2a0
- Subscription to `newHead` events via websocket using the method `pathfinder_subscribe_newHeads`, which can
  be managed by the following command line options
  - `rpc.websocket`, which enables websocket transport
  - `rpc.websocket.capacity`, which sets the maximum number of websocket subscriptions per subscription type

  Authors: [Shramee Srivastav](https://github.com/shramee) and [Matthieu Auger](https://github.com/matthieuauger)

## [0.5.5] - 2023-05-18

### Added

- `cairo-lang` upgraded to 0.11.1.1

### Fixed

- RPC emits connection logs and warnings
- Fee estimate mismatch between gateway and pathfinder
  - Gateway uses a new gas price sampling algorithm which was incompatible with pathfinders.
- Fee estimate returns error when submitting Cairo 1.0.0-rc0 classes.
- Historic L1 handler transactions are served as Invoke V0
  - Older databases contain L1 handler transactions from before L1 handler was a specific transaction type. These were
    stored as Invoke V0. These are now correctly identified as being L1 Handler transactions.

### Fixed

- RPC emits connection logs and warnings
- Fee estimate mismatch between gateway and pathfinder
  - Gateway uses a new gas price sampling algorithm which was incompatible with pathfinders.
- Historic L1 handler transactions are served as Invoke V0
  - Older databases contain L1 handler transactions from before L1 handler was a specific transaction type. These were
    stored as Invoke V0. These are now correctly identified as being L1 Handler transactions.

## [0.5.4] - 2023-05-09

### Added

- Starknet v0.11.1 support
  - Sierra compiler v1.0.0.rc0 (while keeping previous compiler for older contracts)
  - new block hash calculation
  - new L1 contract
- CORS support for the RPC server, enabled via the `rpc.cors-domains` command line argument
- transaction hash verification, excluding older L1 handler transactions, i.e. in blocks older than
  - 4400 for mainnet
  - 306008 for testnet

### Fixed

- rpc server panic for unprefixed unregistered method names
- remove a small time window where data which is transitioning from pending to latest block was not available for RPC queries.
  - this was commonly seen when rapidly monitoring a new transaction, which would go from `PENDING` to `TXN_HASH_NOT_FOUND` to `ACCEPTED_ON_L2`.

## [0.5.3] - 2023-04-12

### Added

- `max-rpc-connections` command-line argument
- `cairo-lang` upgraded to 0.11.0.2

### Fixed

- `starknet_simulateTransaction` data model inconsistency
- `poll-pending` default value restored to `false`
- handling of invalid JSON-RPC requests

### Removed

- support for `BROADCASTED` transactions version 0

## [0.5.2] - 2023-03-28

### Added

- support `starknet_estimateFee` in the JSON-RPC v0.3 API
  - supports estimating multiple transactions
  - this includes declaring and immediately using a class (not currently possible via the gateway)
- support `starknet_simulateTransaction` for JSON-RPC v0.3
  - supports simulating multiple transactions
  - this includes declaring and immediately using a class (not currently possible via the gateway)
- support `pathfinder_getTransactionStatus` which is exposed on all RPC routes
  - this enables querying a transactions current status, including whether the gateway has received or rejected it

### Fixed

- RPC returns int for entrypoint offsets instead of hex
- RPC rejects Fee values with more than 32 digits
- RPC does not expose `pathfinder_getProof` on v0.3 route

## [0.5.1] - 2023-03-23

### Fixed

- pathfinder can spam nethermind L1 nodes
- pathfinder stops syncing testnet2 at block 95220 due to a Sierra class compilation issue

## [0.5.0] - 2023-03-20

### Added

- support for state commitment and class commitment in pathfinder_getProof
- support for starknet v0.11
- partial support for RPC specification v0.3
  - exposed on `/rpc/v0.3/` route
  - missing support for `starknet_estimateFee` and `starknet_simulate`

### Changed

- `starknet_call` and `starknet_estimateFee` JSON-RPC methods return more detailed error messages
- `python` version requirement has changed to `3.9` or `3.10` (was `3.8` or `3.9`)

### Fixed

- RPC accepts hex inputs for Felt without '0x' prefix. This led to confusion especially when passing in a decimal string which would get silently interpreted as hex.
- using a Nethermind Ethereum endpoint occasionally causes errors such as `<block-number> could not be found` to be logged.
- sync can miss new block events by getting stuck waiting for pending data.

### Removed

- `--config` configuration option (deprecated in [v0.4.1](https://github.com/equilibriumco/pathfinder/releases/tag/v0.4.1))
- `--integration` configuration option (deprecated in [v0.4.1](https://github.com/equilibriumco/pathfinder/releases/tag/v0.4.1))
- `--sequencer-url` configuration option (deprecated in [v0.4.1](https://github.com/equilibriumco/pathfinder/releases/tag/v0.4.1))
- `--testnet2` configuration option (deprecated in [v0.4.1](https://github.com/equilibriumco/pathfinder/releases/tag/v0.4.1))
- `starknet_addDeployTransaction` as this is no longer an allowed transaction
- RPC api version `0.1`, which used to be served on path `/rpc/v0.1`

## [0.4.5] - 2022-12-21

### Added

- added Newton FAQ links to readme (thanks @SecurityQQ)

### Fixed

- node fails to sync really old blocks

## [0.4.4] - 2022-12-20

### Added

- storage proofs via pathfinder_getProof by @pscott

### Changed

- improved performance for starknet_call and starknet_estimateFee by caching classes
- improved performance for starknet_call and starknet_estimateFee by using Rust for hashing

### Fixed

- starknet_getEvents returns all events when from_block="latest"
- v0.1 starknet_getStateUpdate does not contain nonces

## [0.4.3] - 2022-12-7

### Changed

- updated to cairo-lang 0.10.3

### Fixed

- testnet2 and integration flags are ignored
- starknet_estimateFee uses wrong chain ID for testnet2

## [0.4.2] - 2022-12-2

### Added

- document that --chain-id expects text as input

### Fixed

- testnet2 and integration L1 addresses are swopped (bug introduced in v0.4.1)
- proxy network setups can't sync historical blocks (bug introduced in v0.4.1)
- ABI serialization for starknet_estimateFee for declare transactions

## [0.4.1] - 2022-11-30

### Added

- custom Starknet support (see above for details)
- pathfinder specific RPC extensions hosted at <rpc-url>/rpc/pathfinder/v0.1. Currently this only contains pathfinder_version which returns the pathfinder version of the node.

### Changed

- The following configuration options are now marked as deprecated: --testnet2, --integration, --config, --sequencer-url
- Optimised starknet_events for queries with both a block range and a from address

### Fixed

- block timestamps for pending in starknet_call and starknet_estimateFee were using the latest timestamp instead of the pending one. This meant contracts relying on accurate timestamps could sometimes fail unexpectedly.

## [0.4.0] - 2022-11-30

### Added

- support for Starknet v0.10.2

### Changed

- default RPC API version changed from v0.1 to v0.2

## Ancient History

Older history may be found in the [pathfinder release notes](https://github.com/equilibriumco/pathfinder/releases).
