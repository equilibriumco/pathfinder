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

## Connection Limits and Keepalive

Pathfinder limits how many WebSocket connections it serves at once and drops connections it
believes are idle or unresponsive.

| Option | Default | Effect |
| --- | --- | --- |
| `--rpc.websocket.max-connections` | 1024 | Maximum concurrent connections allowed. Upgrade requests over the limit are rejected with HTTP 503. |
| `--rpc.websocket.initial-frame-timeout` | 30 seconds | A connection that sends no request within this window after being established is closed. |
| `--rpc.websocket.ping-interval` | 30 seconds | How long the server waits for a frame from the client before sending a ping. |
| `--rpc.websocket.max-missed-pings` | 2 | Consecutive unanswered pings tolerated before the connection is closed. |
| `--rpc.websocket.max-subscriptions` | 1024 | Maximum number of subscriptions per connection. |
| `--rpc.websocket.subscription-request-max-size` | 1048576 bytes | Largest accepted subscription request. |
| `--rpc.websocket.send-timeout` | 1 second | How long a send may block before the output buffer is considered full and the connection is closed. |

### What clients need to do

**Answer pings.** RFC 6455 requires it, and browsers and the mainstream WebSocket libraries do it for you - but only while your code is reading from the connection.

With the defaults, Pathfinder sends its first ping after 30 seconds of silence from the client and closes the connection once two consecutive pings go unanswered, so an unresponsive client is dropped roughly 90 seconds after its last frame.

**Send a request promptly.** The initial-frame deadline is absolute and only counts JSON-RPC messages. Answering pings does not extend it, so a connection that is opened and left unused is closed even if it is answering pings.

**Reconnect on 503 and on close.** Treat an HTTP 503 on the upgrade as backpressure and retry with backoff. Closed connections take their subscriptions with them. Re-subscribe after reconnecting.

Pathfinder closes with code `1000` and reason `Connection idle` when it gives up on a connection, and `Server shutdown` when the node is shutting down.

:::note
The keepalive cannot be disabled. `--rpc.websocket.ping-interval`, `--rpc.websocket.initial-frame-timeout` and `--rpc.websocket.max-missed-pings` all reject `0`. Raise the ping interval if the default is too aggressive for your clients.
:::

Operators can watch `rpc_websocket_connections`, `rpc_websocket_connections_rejected_total` and `rpc_websocket_connections_closed_total` on the [metrics endpoint](../monitoring-and-metrics).
