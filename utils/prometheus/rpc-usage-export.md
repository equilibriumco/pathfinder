# RPC usage export

`rpc-usage-export.sh` produces a small, standardized JSON snapshot of how a
Pathfinder node's JSON-RPC interface is being used, read straight from a
Prometheus that scrapes the node.

## What it contains

The snapshot is **method names and counts only**. It is derived entirely from
three aggregate counters/histograms:

- `rpc_method_calls_total{method, version, block_target}`
- `rpc_method_calls_failed_total{method, version, error_kind}`
- `rpc_method_calls_duration_seconds_bucket{le, method, version}`

It contains **no request parameters, no contract addresses or storage keys, no
client identifiers, no IP addresses, and no response payloads**. It is safe to
share for usage analysis.

The output has four sections:

- `calls_by_method`: total calls per method and RPC version over the window.
- `block_target_split`: for each method, how requests targeted a block
  (`latest`, `pending`, `by_number`, `by_hash`, `none`, `unknown`). Separates
  cheap hot-tip reads from expensive historical-state lookups.
- `errors_by_kind`: failures per method, broken down by error variant.
- `latency_ms`: p50/p95/p99 latency per method, estimated from the histogram.

## Running it

```bash
./rpc-usage-export.sh --url http://localhost:9090 --provider acme --window 7d --out acme-rpc-usage.json
```

- `--url`: base URL of the Prometheus that scrapes the node (required).
- `--provider`: a name to stamp on the snapshot so multiple exports are
  distinguishable when merged.
- `--window`: look-back window as a PromQL range (default `7d`). It must fit
  within the Prometheus retention period.
- `--out`: output file; omit to write to stdout.

If the Prometheus requires authentication, pass it via the environment:

```bash
PROM_BEARER_TOKEN=... ./rpc-usage-export.sh --url https://prometheus.internal ...
# or
PROM_BASIC_AUTH=user:password ./rpc-usage-export.sh --url https://prometheus.internal ...
```

## Output shape

```json
{
  "provider": "acme",
  "window": "7d",
  "generated_at": "2026-06-25T09:00:00Z",
  "prometheus": "http://localhost:9090",
  "metrics": {
    "calls_by_method": [
      { "method": "starknet_getStorageAt", "version": "v0.8", "calls": 1048576 }
    ],
    "block_target_split": [
      { "method": "starknet_getStorageAt", "version": "v0.8", "block_target": "latest", "calls": 900000 }
    ],
    "errors_by_kind": [
      { "method": "starknet_getStorageAt", "version": "v0.8", "error_kind": "contract_not_found", "errors": 12 }
    ],
    "latency_ms": [
      { "method": "starknet_getStorageAt", "version": "v0.8", "p50": 0.6, "p95": 1.8, "p99": 4.4 }
    ]
  }
}
```
