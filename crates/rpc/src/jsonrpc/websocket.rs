//! A websocket subscription service, inspired by
//! [Ethereum](https://ethereum.org/en/developers/tutorials/using-websockets/)
//! See also the [Alchemy subscription API
//! doc](https://docs.alchemy.com/reference/subscription-api)
//!
//! See the [OpenRPC](../../../specs/rpc/v10/starknet_ws_api.json)
//! spec for the supported methods.
//!
//! Requires the `--rpc.websocket.enabled` cli option.
//!
//!
//! Manual testing can be performed using `wscat`:
//! ```ignore
//! > ~/pathfinder$ wscat -c ws://localhost:9545/rpc/v0_10
//! Connected (press CTRL+C to quit)
//! > {"jsonrpc":"2.0", "id": 1, "method": "starknet_subscribeNewHeads", "params": []}
//! < {"id":1,"jsonrpc":"2.0","result":"0"}
//! < {"jsonrpc":"2.0","method":"starknet_subscriptionNewHeads","params":{"result":{"block_hash":"0x66626f1f0038c608f8d4ee10c39c9d4f0b98a9866b086cb32e8f919ee6b43aa","block_number":11834642,"event_commitment":"0x0","event_count":0,"l1_da_mode":"BLOB","l1_data_gas_price":{"price_in_fri":"0x10a3324ecd","price_in_wei":"0x11b45d"},"l1_gas_price":{"price_in_fri":"0x411936095dbe","price_in_wei":"0x45460c02"},"l2_gas_price":{"price_in_fri":"0x712f2ffc7","price_in_wei":"0x78718"},"new_root":"0x4cfd667f0ab6ab2e6041564b0b11288e0eb4b81b53d5301c53ea21667d937be","parent_hash":"0x668a2e83e0daeb3036a80a3007d44f0b8153d1f21209bb82fabdeaf45238a8b","receipt_commitment":"0x0","sequencer_address":"0x1176a1bd84444c89232ec27754698e5d2e7e1a7f1539f12027f28b23ec9f3d8","starknet_version":"0.14.3","state_diff_commitment":"0x5d2704f07aae8cbd3cfcfe831d9d735198b7499a38817cb5dc5f23217340b91","state_diff_length":1,"timestamp":1784019750,"transaction_commitment":"0x0","transaction_count":0},"subscription_id":"0"}}
//! ```

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum WebsocketHistory {
    Limited(u64),
    Unlimited,
}

#[derive(Clone)]
pub struct WebsocketContext {
    pub max_history: WebsocketHistory,
    /// Maximum number of subscriptions per connection.
    pub max_subscriptions: usize,
    pub subscription_max_size: usize,
    pub send_timeout: Duration,
    /// Connections that don't send any frame within this duration after the
    /// upgrade are closed. [`Duration::ZERO`] disables the timeout.
    pub initial_frame_timeout: Duration,
    /// How long to wait for a frame from the peer before sending a ping.
    /// [`Duration::ZERO`] disables the keepalive.
    pub ping_interval: Duration,
    /// Number of consecutive unanswered pings after which the connection is
    /// closed.
    pub max_missed_pings: u32,
    /// Limits the number of concurrently open websocket connections. Shared by
    /// all clones of this context, so the number of permits it was created with
    /// is the only source of truth for the limit.
    pub connection_limit: Arc<Semaphore>,
}

impl Default for WebsocketContext {
    fn default() -> Self {
        Self {
            max_history: WebsocketHistory::Limited(1024),
            max_subscriptions: 1024,
            subscription_max_size: 1024 * 1024,
            send_timeout: Duration::from_secs(1),
            initial_frame_timeout: Duration::from_secs(30),
            ping_interval: Duration::from_secs(30),
            max_missed_pings: 2,
            connection_limit: Arc::new(Semaphore::new(1024)),
        }
    }
}

impl WebsocketContext {
    /// Reserves a slot for a new websocket connection, or returns [`None`] if
    /// the limit has been reached. The slot is released when the returned
    /// [`ConnectionGuard`] is dropped.
    pub fn try_acquire_connection(&self) -> Option<ConnectionGuard> {
        self.connection_limit
            .clone()
            .try_acquire_owned()
            .ok()
            .map(ConnectionGuard::new)
    }

    #[cfg(test)]
    pub fn for_test(max_history: WebsocketHistory) -> Self {
        Self {
            max_history,
            send_timeout: Duration::from_secs_f64(0.1),
            ..Default::default()
        }
    }
}

/// Holds a websocket connection's slot in
/// [`WebsocketContext::connection_limit`] for as long as the connection's
/// socket is open.
pub struct ConnectionGuard(
    #[expect(dead_code, reason = "held to keep the permit alive")] OwnedSemaphorePermit,
);

impl ConnectionGuard {
    fn new(permit: OwnedSemaphorePermit) -> Self {
        metrics::gauge!("rpc_websocket_connections").increment(1.0);
        Self(permit)
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        metrics::gauge!("rpc_websocket_connections").decrement(1.0);
    }
}
