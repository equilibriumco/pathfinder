---
sidebar_position: 1
---

# JSON-RPC API

The JSON-RPC interface allows you to query Starknet data, send transactions, and perform contract calls without going through a formal transaction on-chain. Pathfinder currently supports multiple API versions and a distinct set of custom extensions.

## Supported Versions
- **JSON-RPC v0.9.0**
  Accessible at the `/rpc/v0_9` endpoint.
- **JSON-RPC v0.10**
  Accessible at the `/rpc/v0_10` endpoint.
- **Pathfinder Extension**
  Exposed via `/rpc/pathfinder/v0_1`.

:::note
The API served at the root path (`/` for HTTP and `/ws` for WebSocket) can be set via the `--rpc.root-version` parameter (or `RPC_ROOT_VERSION` environment variable). Since a version upgrade _might_ change the version of the JSON-RPC API exposed on this path using this path is not recommended. Please use one of the explicitly versioned paths above.
:::

## API Endpoints and Usage
Below is a typical JSON-RPC request structure:

```json
{
  "jsonrpc": "2.0",
  "method": "<method_name>",
  "params": [...],
  "id": 1
}
```

You’ll receive a response in a similar JSON-RPC 2.0 format containing `result` or `error` fields. For instance, to query the chain ID using v0.9:

```bash
curl -X POST \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"starknet_chainId","params":[],"id":1}' \
  http://127.0.0.1:9545/rpc/v0_9
```

A successful response might look like this:

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": "0x534e5f4d41494e"
}
```

<details>
  <summary>Example: Calling a Contract Function</summary>

  ```bash
  curl -X POST \
    -H "Content-Type: application/json" \
    -d '{
          "jsonrpc":"2.0",
          "method":"starknet_call",
          "params":[{
             "request": {
               "contract_address":"0x1234...",
               "entry_point_selector":"0xabc...",
               "calldata":[ "0x1", "0x2" ]
             },
             "block_id":"latest"
          }],
          "id":1
        }' \
    http://127.0.0.1:9545/rpc/v0_9
  ```
</details>

## Pathfinder JSON Extensions

For advanced use cases like verifying storage proofs or generating special debug information, Pathfinder provides additional endpoints under:
```
/rpc/pathfinder/v0_1
```
The complete specification for these JSON-only extension methods can be found in the [Pathfinder repository](https://github.com/equilibriumco/pathfinder/blob/main/specs/rpc/pathfinder_rpc_api.json).

