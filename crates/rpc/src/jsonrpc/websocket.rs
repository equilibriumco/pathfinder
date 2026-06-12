//! A websocket subscription service, inspired by [Ethereum](https://ethereum.org/en/developers/tutorials/using-websockets/)
//! See also the [Infura Ethereum API spec](https://docs.infura.io/networks/ethereum/json-rpc-methods/subscription-methods/eth_subscribe)
//! as well as the [Alchemy subscription API doc](https://docs.alchemy.com/reference/subscription-api)
//!
//! See the [OpenRPC](../../../specs/rpc/pathfinder_ws.json) spec for this
//! implementation.
//!
//! Requires the `--rpc.websocket.enabled` cli option.
//!
//!
//! Manual testing can be performed using `wscat`:
//! ```ignore
//! > pierre:~/pathfinder$ wscat -c ws://localhost:9545/ws
//! Connected (press CTRL+C to quit)
//! > {"jsonrpc":"2.0", "id": 1, "method": "pathfinder_subscribe", "params": ["newHeads"]}
//! < {"jsonrpc":"2.0","result":0,"id":1}
//! < {"jsonrpc":"2.0","method":"pathfinder_subscription","result":{"subscription":0,"event":{"class_commitment":"0x4a1c4c3cd477eb052655963781fd7ae0cd647752f01595e4e33fed2ab0eff90","eth_l1_gas_price":1000000015,"event_commitment":"0x79789afccc8f0cac4a3992b2b52cc15f560b4f5a997d883b29d73236b2dfce7","event_count":387,"hash":"0x412edf5929693f8d6bb29512d1a777066dfbf493f3ee64bcb14c64165f5006b","number":908104,"parent_hash":"0x16562de7d258e27809ec6b3d3da5edaedc6526a046442f2f5d72fe7c5dc0a1d","sequencer_address":"0x1176a1bd84444c89232ec27754698e5d2e7e1a7f1539f12027f28b23ec9f3d8","starknet_version":"0.12.3","state_commitment":"0x1d00410c349e70996834a144598bc762602df09cd38a51c25528fb2fd662403","storage_commitment":"0x5129d4a27efa0429975f67440314ab921cc554681ac3ecf476850c1f6b723bf","strk_l1_gas_price":0,"timestamp":1700823087,"transaction_commitment":"0x273bfec6af3c812b59a864e67334132d5bd26c570a9b202e0adce2bb4d6b0cf","transaction_count":36}}}
//! ```
//!
//! Subscriptions may lag behind because of a slow network or slow client and
//! result in an error:
//! ```ignore
//! > pierre:~/pathfinder$ wscat -c ws://localhost:9545/ws
//! Connected (press CTRL+C to quit)
//! > {"jsonrpc":"2.0", "id": 1, "method": "pathfinder_subscribe", "params":
//! > ["newHeads"]}
//! < {"jsonrpc":"2.0","result":0,"id":1}
//! < {"jsonrpc":"2.0","error":{"code":-32099,"message":"Websocket subscription
//! closed","data":{"id":0,"reason":"Lagging stream, some headers were skipped.
//! Closing subscription."}},"id":null}
//! ```

use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum WebsocketHistory {
    Limited(u64),
    Unlimited,
}

#[derive(Clone)]
pub struct WebsocketContext {
    pub max_history: WebsocketHistory,
    pub max_subscriptions: usize,
    pub send_timeout: Duration,
}

impl WebsocketContext {
    pub fn new(
        max_history: WebsocketHistory,
        max_subscriptions: usize,
        send_timeout: Duration,
    ) -> Self {
        Self {
            max_history,
            max_subscriptions,
            send_timeout,
        }
    }

    #[cfg(test)]
    pub fn for_test(max_history: WebsocketHistory) -> Self {
        Self {
            max_history,
            max_subscriptions: 1024,
            send_timeout: Duration::from_secs_f64(0.1),
        }
    }
}
