---
sidebar_position: 2
---

# WebSocket API

The WebSocket interface serves the same API versions and extension endpoints as HTTP, but in a stateful, two-way communication channel. This can be especially useful for real-time notifications, subscription-based events, or building interactive dashboards.

## Supported Versions
- **JSON-RPC v0.9.0**
  Accessible at `/rpc/v0_9` and `/ws/rpc/v0_9`.
- **JSON-RPC v0.10**
  Accessible at `/rpc/v0_10` and `/ws/rpc/v0_10`.
- **Pathfinder Extension**
  Exposed via `/ws/rpc/pathfinder/v0_1`

> **Note:** The WebSocket interface is disabled by default. To enable it, use the `--rpc.websocket.enabled` CLI flag. The default root endpoint (i.e., `/ws`) can be configured using the `--rpc.root-version` parameter.

## WebSocket Endpoints and Usage
A typical WebSocket connection can be opened using libraries like `ws`, `websockets`, or the native browser WebSocket API. The RPC payload structure remains the same (JSON-RPC 2.0), but it is sent over a persistent socket connection:

```js title="WebSocket Connection Example in Node.js"
const ws = new WebSocket("ws://127.0.0.1:9545/ws/rpc/v0_9");

ws.onopen = () => {
  const message = JSON.stringify({
    jsonrpc: "2.0",
    method: "starknet_chainId",
    params: [],
    id: 1
  });
  ws.send(message);
};

ws.onmessage = (event) => {
  console.log("Received response:", event.data);
};
```

## Pathfinder WebSocket Extensions

As with the [JSON extensions](json-rpc-api#pathfinder-json-extensions), Pathfinder provides Websocket equivalents of their custom endpoints. They are served under:
```
/ws/rpc/pathfinder/v0_1
```

You can find the complete list of WebSocket extensions in the [Pathfinder repository](https://github.com/equilibriumco/pathfinder/blob/main/specs/rpc/pathfinder_ws.json).
