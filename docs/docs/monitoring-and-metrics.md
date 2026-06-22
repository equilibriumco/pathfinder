---
sidebar_position: 5
---

# Monitoring API

Pathfinder provides a dedicated monitoring API that can be enabled at startup. This API helps operators verify node health, confirm readiness, gauge synchronization progress, and capture metrics for observability.

To enable the monitoring endpoints, use the `--monitor-address` configuration option (or the `MONITOR_ADDRESS` environment variable). For example, when running Pathfinder from source:

```bash
cargo run --release --bin pathfinder -- \
  --monitor-address 0.0.0.0:9000 \
  --some-other-options
```

Or when using Docker:

```bash
docker run \
  --name pathfinder \
  --restart unless-stopped \
  --detach \
  -p 9545:9545 \
  -p 9000:9000 \
  --user "$(id -u):$(id -g)" \
  -e RUST_LOG=info \
  -e PATHFINDER_ETHEREUM_API_URL="wss://sepolia.infura.io/ws/v3/<project-id>" \
  eqlabs/pathfinder \
  --monitor-address 0.0.0.0:9000
```

Once the node is running, you can access the following endpoints on the specified port (in the examples above, `9000`).

## Health Check

The Health Check endpoint (`/health`) confirms that the Pathfinder process is active and not in a fatal error state. Although a successful response indicates that the process is running, it does not guarantee synchronization or readiness for requests.

**Example**:
```bash
curl -i http://localhost:9000/health
```

**Expected Responses:**
- `200 OK`: The node process is running and can respond to HTTP requests.  
- `4xx or 5xx`: The endpoint is unreachable, or the node is encountering a critical error.

## Readiness Check

The Readiness Check endpoint (`/ready`) indicates whether Pathfinder has completed all startup tasks (such as database migrations and network checks).

**Example**:
```bash
curl -i http://localhost:9000/ready
```

**Expected Responses:**
- `200 OK`: The node is fully initialized and ready to serve JSON-RPC requests.  
- `503 Service Unavailable`: The node is still starting up or failing a prerequisite task.

## Synced Status

The Synced Status endpoint (`/ready/synced`) extends the readiness check by ensuring that Pathfinder is within six blocks of the network’s current tip. This guarantees that the node is both ready and nearly fully synced.

**Example**:
```bash
curl -i http://localhost:9000/ready/synced
```

**Expected Responses:**
- `200 OK`: The node is ready for requests and closely tracking the chain’s latest blocks.  
- `503 Service Unavailable`: The node is still starting or more than six blocks behind the network tip.

---

## Prometheus Metrics

The Prometheus Metrics endpoint (`/metrics`) exposes real-time operational data in the [Prometheus](https://prometheus.io/) format. These metrics cover various aspects of Pathfinder’s performance and can be scraped periodically by Prometheus for monitoring and alerting.

<details>
<summary>Example Metrics</summary>

  **Process Metrics**  
    - `process_start_time_seconds` - UNIX timestamp at which Pathfinder started.
  
  **RPC-Related Metrics**  
    - `rpc_method_calls_total{method="<methodName>", version="<rpcVersion>", block_target="<blockTarget>"}`  
      Counts how many times each JSON-RPC method is called. `block_target` records how the
      request targeted a block, derived from its `block_id` parameter: `latest`, `pending`,
      `by_number`, `by_hash`, `none` (no `block_id` parameter), or `unknown` (positional
      params or an unrecognised shape).
    - `rpc_method_calls_failed_total{method="<methodName>", version="<rpcVersion>", error_kind="<errorKind>"}`  
      Counts how many times each method call resulted in an error. `error_kind` is the error
      variant name (e.g. `block_not_found`, `contract_not_found`, `invalid_params`,
      `internal_error`).
    - `rpc_method_calls_duration_milliseconds{method="<methodName>", version="<rpcVersion>"}` —
      histogram of JSON-RPC method call latency. Exported as Prometheus histogram buckets
      (`rpc_method_calls_duration_milliseconds_bucket` / `_sum` / `_count`). Compute quantiles
      with `histogram_quantile()`:

      ```promql
      histogram_quantile(0.95, sum by (le, method, version) (rate(rpc_method_calls_duration_milliseconds_bucket[5m])))
      ```

      Pathfinder ships ready-made recording rules for p50/p95/p99 at
      `utils/prometheus/rpc-recording-rules.yml`.

  **Gateway Request Metrics**  
    - `gateway_requests_total{method="<sequencerRequestType>", tag="<latest|pending>", reason="<optionalFailureReason>"}`  
    - `gateway_requests_failed_total{method="<sequencerRequestType>", ...}`
    - `gateway_request_duration_seconds{method="<sequencerRequestType>", ...}`

  **Sync-Related Metrics**  
    - `current_block` - The latest block the node has synced.
    - `highest_block` - The highest known block in the network.
    - `block_time` - The timestamp difference between the current block and its parent
    - `block_latency` - How long after block publication the node processed the block.
    - `block_download` - Time taken to download current block's data excluding classes
    - `block_processing` - Time taken to process and store the current block
    - `block_processing_duration_seconds` - Histogram of block processing times.

  **Build Info Metrics**  
    - `pathfinder_build_info{version="<currentVersion>"}` - Reports the Pathfinder version.

  **Labels:**
  - `method`, to retrieve a counter for a particular sequencer request type
  - `tag`
      - works with methods: `get_block`, `get_state_update`
      - valid values:
          - `pending`
          - `latest`
  - `reason`
      - works with: `gateway_requests_failed_total`
      - valid values:
          - `decode`
          - `starknet`
          - `rate_limiting`
          - `timeout`

  **Valid examples:**
  ```
  gateway_requests_total{method="get_block"}
  gateway_requests_total{method="get_block", tag="latest"}
  gateway_requests_failed_total{method="get_state_update"}
  gateway_requests_failed_total{method="get_state_update", tag="pending"}
  gateway_requests_failed_total{method="get_state_update", tag="pending", reason="starknet"}
  gateway_requests_failed_total{method="get_state_update", reason="rate_limiting"}
  ```

  **These __will not work__:**
  - `gateway_requests_total{method="get_transaction", tag="latest"}`, `tag` is not supported for that `method`
  - `gateway_requests_total{method="get_transaction", reason="decode"}`, `reason` is only supported for failures.
</details>

### Prometheus Configuration
- Follow the [Prometheus guide](https://prometheus.io/docs/introduction/first_steps/) to install Prometheus.
- Add a job to your `prometheus.yml` file so that Prometheus scrapes metrics from your Pathfinder node:

```yaml title="prometheus.yml"
scrape_configs:
  - job_name: 'pathfinder'
    static_configs:
      - targets: ['localhost:9000']
```

After updating the configuration, restart Prometheus. It will begin collecting and storing metrics for visualization and analysis.
