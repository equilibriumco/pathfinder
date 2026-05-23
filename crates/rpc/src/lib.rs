//! Starknet node JSON-RPC related modules.
pub mod context;
mod dto;
mod error;
mod executor;
mod felt;
pub mod jsonrpc;
pub(crate) mod method;
pub mod middleware;
mod pathfinder;
mod pending;
#[cfg(test)]
mod test_setup;
pub mod tracker;
pub mod types;
pub mod v06;
pub mod v07;
pub mod v08;
pub mod v09;
pub mod v10;

use std::net::SocketAddr;
use std::path::Path;
use std::result::Result;

use anyhow::Context;
use axum::error_handling::HandleErrorLayer;
use axum::extract::DefaultBodyLimit;
use axum::response::IntoResponse;
use context::RpcContext;
pub use executor::compose_executor_transaction;
use http_body::Body;
pub use jsonrpc::{Notifications, Reorg};
use pathfinder_common::{integration_testing, AllowedOrigins};
pub use pending::{FinalizedTxData, PendingBlocks, PendingData};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tower_http::cors::CorsLayer;
use tower_http::ServiceBuilderExt;

use crate::jsonrpc::rpc_handler;
use crate::types::syncing::Syncing;

const DEFAULT_MAX_CONNECTIONS: usize = 1024;

#[derive(Copy, Clone, Debug, Default, PartialEq, PartialOrd)]
pub enum RpcVersion {
    V06,
    #[default]
    V07,
    V08,
    V09,
    V10,
    PathfinderV01,
}

impl RpcVersion {
    fn to_str(self) -> &'static str {
        match self {
            RpcVersion::V06 => "v0.6",
            RpcVersion::V07 => "v0.7",
            RpcVersion::V08 => "v0.8",
            RpcVersion::V09 => "v0.9",
            RpcVersion::V10 => "v0.10",
            RpcVersion::PathfinderV01 => "v0.1",
        }
    }
}

// TODO: make this configurable
const REQUEST_MAX_SIZE: usize = 10 * 1024 * 1024;
// TODO: make this configurable
const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);

pub struct RpcServer {
    addr: SocketAddr,
    context: RpcContext,
    max_connections: usize,
    cors: Option<CorsLayer>,
    default_version: RpcVersion,
}

impl RpcServer {
    pub fn new(addr: SocketAddr, context: RpcContext, default_version: RpcVersion) -> Self {
        Self {
            addr,
            context,
            max_connections: DEFAULT_MAX_CONNECTIONS,
            cors: None,
            default_version,
        }
    }

    pub fn with_max_connections(mut self, max_connections: usize) -> Self {
        self.max_connections = max_connections;
        self
    }

    pub fn with_cors(self, allowed_origins: AllowedOrigins) -> Self {
        Self {
            cors: Some(middleware::cors::with_allowed_origins(allowed_origins)),
            ..self
        }
    }

    /// Starts the HTTP-RPC server.
    pub async fn spawn(
        self,
        data_directory: &Path,
    ) -> Result<(JoinHandle<anyhow::Result<()>>, SocketAddr), anyhow::Error> {
        use axum::routing::{get, post};

        let listener = match tokio::net::TcpListener::bind(self.addr).await {
            Ok(listener) => listener,
            Err(e) => {
                return Err(e).context(format!(
                    "RPC address {} is already in use.

            Hint: This usually means you are already running another instance of pathfinder.
            Hint: If this happens when upgrading, make sure to shut down the first one first.
            Hint: If you are looking to run two instances of pathfinder, you must configure them \
                     with different http rpc addresses.",
                    self.addr
                ))
            }
        };
        let addr = listener
            .local_addr()
            .context("Getting local address from listener")?;
        integration_testing::debug_create_port_marker_file("rpc", addr.port(), data_directory);

        async fn handle_middleware_errors(err: axum::BoxError) -> (http::StatusCode, String) {
            use http::StatusCode;
            if err.is::<tower::timeout::error::Elapsed>() {
                (
                    StatusCode::REQUEST_TIMEOUT,
                    "Request took too long".to_string(),
                )
            } else {
                // TODO: confirm this isn't too verbose.
                tracing::warn!(error = err, "Unhandled middleware error");

                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal error".to_string(),
                )
            }
        }

        let middleware = tower::ServiceBuilder::new()
            // Convert errors created by middleware layers into responses.
            // This is required by axum -- axum doesn't deal with Result, errors
            // must be responses as well.
            .layer(HandleErrorLayer::new(handle_middleware_errors))
            // make sure to set request ids before the request reaches `TraceLayer`
            .set_x_request_id(middleware::request_id::RequestIdSource::default())
            .concurrency_limit(self.max_connections)
            .layer(DefaultBodyLimit::max(REQUEST_MAX_SIZE))
            .timeout(REQUEST_TIMEOUT)
            .layer(middleware::tracing::trace_layer())
            .option_layer(self.cors)
            .propagate_x_request_id();

        /// Returns success for requests with an empty body without reading
        /// the entire body.
        async fn empty_body(request: axum::extract::Request) -> impl IntoResponse {
            if request.body().is_end_stream() {
                axum::http::StatusCode::OK
            } else {
                axum::http::StatusCode::METHOD_NOT_ALLOWED
            }
        }

        let v06_routes = v06::register_routes().build(self.context.clone());
        let v07_routes = v07::register_routes().build(self.context.clone());
        let v08_routes = v08::register_routes().build(self.context.clone());
        let v09_routes = v09::register_routes().build(self.context.clone());
        let v10_routes = v10::register_routes().build(self.context.clone());
        let pathfinder_routes = pathfinder::register_routes().build(self.context.clone());
        let unstable_routes = pathfinder::unstable::register_routes().build(self.context.clone());

        let default_router = match self.default_version {
            RpcVersion::V06 => v06_routes.clone(),
            RpcVersion::V07 => v07_routes.clone(),
            RpcVersion::V08 => v08_routes.clone(),
            RpcVersion::V09 => v09_routes.clone(),
            RpcVersion::V10 => v10_routes.clone(),
            RpcVersion::PathfinderV01 => {
                anyhow::bail!("Did not expect default RPC version to be Pathfinder v0.1")
            }
        };

        let router = axum::Router::new()
            // Also return success for get's with an empty body. These are often
            // used by monitoring bots to check service health.
            .route("/", get(empty_body).post(rpc_handler))
            .with_state(default_router.clone())
            .route("/rpc/v0_6", post(rpc_handler))
            .with_state(v06_routes.clone())
            .route("/rpc/v0_7", post(rpc_handler))
            .with_state(v07_routes.clone())
            .route("/rpc/v0_8", post(rpc_handler).get(rpc_handler))
            .with_state(v08_routes.clone())
            .route("/rpc/v0_9", post(rpc_handler).get(rpc_handler))
            .with_state(v09_routes.clone())
            .route("/rpc/v0_10", post(rpc_handler).get(rpc_handler))
            .with_state(v10_routes.clone())
            .route("/rpc/pathfinder/v0.1", post(rpc_handler))
            .route("/rpc/pathfinder/v0_1", post(rpc_handler))
            .with_state(pathfinder_routes.clone())
            .route("/rpc/pathfinder/unstable", post(rpc_handler))
            .with_state(unstable_routes.clone());

        let router = if self.context.websocket.is_some() {
            router
                .route("/ws", get(rpc_handler))
                .with_state(default_router)
                .route("/ws/rpc/v0_6", get(rpc_handler))
                .with_state(v06_routes)
                .route("/ws/rpc/v0_7", get(rpc_handler))
                .with_state(v07_routes)
                .route("/ws/rpc/v0_8", post(rpc_handler).get(rpc_handler))
                .with_state(v08_routes)
                .route("/ws/rpc/v0_9", post(rpc_handler).get(rpc_handler))
                .with_state(v09_routes)
                .route("/ws/rpc/v0_10", post(rpc_handler).get(rpc_handler))
                .with_state(v10_routes)
                .route("/ws/rpc/pathfinder/v0_1", get(rpc_handler))
                .with_state(pathfinder_routes)
        } else {
            router.with_state(default_router)
        };

        let router = router.layer(middleware);

        let server_handle = util::task::spawn(async move {
            axum::serve(listener, router.into_make_service())
                .with_graceful_shutdown(util::task::cancellation_token().cancelled_owned())
                .await
                .map_err(Into::into)
        });

        Ok((server_handle, addr))
    }
}

pub struct SyncState {
    pub status: RwLock<Syncing>,
}

impl Default for SyncState {
    fn default() -> Self {
        Self {
            status: RwLock::new(Syncing::False),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Deserialize)]
pub(crate) struct SubscriptionId(pub u32);

impl crate::dto::SerializeForVersion for SubscriptionId {
    fn serialize(
        &self,
        serializer: crate::dto::Serializer,
    ) -> Result<crate::dto::Ok, crate::dto::Error> {
        serializer.serialize_str(self.0.to_string().as_str())
    }
}

impl crate::dto::DeserializeForVersion for SubscriptionId {
    fn deserialize(value: crate::dto::Value) -> Result<Self, serde_json::Error> {
        let id: String = value.deserialize()?;
        let id: u32 = id.parse().map_err(|_| {
            use serde::de::Error;
            serde_json::Error::custom(format!("Failed to parse subscription id: {id:?}"))
        })?;
        Ok(Self(id))
    }
}

#[cfg(test)]
pub mod test_utils {
    use std::collections::HashMap;

    use pathfinder_common::class_definition::{
        SerializedCairoDefinition,
        SerializedCasmDefinition,
        SerializedSierraDefinition,
    };
    use pathfinder_common::event::Event;
    use pathfinder_common::macro_prelude::*;
    use pathfinder_common::prelude::*;
    use pathfinder_common::receipt::{
        BuiltinCounters,
        ExecutionResources,
        ExecutionStatus,
        L2ToL1Message,
        Receipt,
    };
    use pathfinder_common::transaction::*;
    use pathfinder_common::BlockId;
    use pathfinder_merkle_tree::{ClassCommitmentTree, StorageCommitmentTree};
    use pathfinder_storage::{Storage, StorageBuilder};
    use starknet_gateway_types::reply::GasPrices;

    use crate::pending::{PendingData, PreLatestBlock, PreLatestData};

    #[macro_export]
    macro_rules! fixture {
        ($version:expr, $file_name:literal) => {{
            match $version {
                $crate::RpcVersion::V06 => {
                    include_str!(concat!("../../fixtures/0.6.0/", $file_name))
                }
                $crate::RpcVersion::V07 => {
                    include_str!(concat!("../../fixtures/0.7.0/", $file_name))
                }
                $crate::RpcVersion::V08 => {
                    include_str!(concat!("../../fixtures/0.8.0/", $file_name))
                }
                $crate::RpcVersion::V09 => {
                    include_str!(concat!("../../fixtures/0.9.0/", $file_name))
                }
                $crate::RpcVersion::V10 => {
                    include_str!(concat!("../../fixtures/0.10.0/", $file_name))
                }
                _ => unreachable!(),
            }
        }};
    }

    #[macro_export]
    macro_rules! assert_json_matches_fixture {
        ($output_json:expr, $version:expr, $file_name:literal) => {{
            let expected_str = $crate::fixture!($version, $file_name);
            let expected_json: serde_json::Value =
                serde_json::from_str(expected_str).expect("Failed to parse fixture as JSON");

            pretty_assertions_sorted::assert_eq!(
                $output_json,
                expected_json,
                "\nExpected fixture content from {}\nGot output",
                $file_name
            );
        }};
    }

    // Creates storage for tests
    pub fn setup_storage(trie_prune_mode: pathfinder_storage::TriePruneMode) -> Storage {
        use pathfinder_merkle_tree::contract_state::update_contract_state;

        let storage = StorageBuilder::in_memory_with_trie_pruning(trie_prune_mode).unwrap();
        let mut connection = storage.connection().unwrap();
        let db_txn = connection.transaction().unwrap();

        let class_commitment0 = class_commitment_bytes!(b"class commitment 0");
        let class_commitment1 = class_commitment_bytes!(b"class commitment 1");
        let class_commitment2 = class_commitment_bytes!(b"class commitment 2");

        let contract0_addr = contract_address_bytes!(b"contract 0");
        let contract1_addr = contract_address_bytes!(b"contract 1");
        let contract2_addr = contract_address_bytes!(b"contract 2 (sierra)");

        let class0_hash = class_hash_bytes!(b"class 0 hash");
        let class1_hash = class_hash_bytes!(b"class 1 hash");
        let class2_hash = class_hash_bytes!(b"class 2 hash (sierra)");
        let class_hash_pending = class_hash_bytes!(b"class pending hash");
        let sierra_class = SierraHash(class2_hash.0);
        let sierra_casm_hash = casm_hash_bytes!(b"casm hash");
        let sierra_casm_hash_v2 = casm_hash_bytes!(b"casm hash blake");

        let storage_addr = storage_address_bytes!(b"storage addr 0");

        let state_update0 = StateUpdate::default()
            .with_deployed_contract(contract0_addr, class0_hash)
            .with_contract_nonce(contract0_addr, contract_nonce!("0x1"));

        let state_update1 = StateUpdate::default()
            .with_deployed_contract(contract1_addr, class1_hash)
            .with_storage_update(
                contract1_addr,
                storage_addr,
                storage_value_bytes!(b"storage value 1"),
            );

        let state_update2 = StateUpdate::default()
            .with_declared_sierra_class(sierra_class, sierra_casm_hash)
            .with_deployed_contract(contract2_addr, class2_hash)
            .with_contract_nonce(contract1_addr, contract_nonce!("0x10"))
            .with_contract_nonce(contract2_addr, contract_nonce!("0xfeed"))
            .with_storage_update(
                contract1_addr,
                storage_addr,
                storage_value_bytes!(b"storage value 2"),
            );

        let contract0_update = HashMap::new();

        let storage_addr = storage_address_bytes!(b"storage addr 0");
        let contract1_update1 =
            HashMap::from([(storage_addr, storage_value_bytes!(b"storage value 1"))]);
        let contract1_update2 =
            HashMap::from([(storage_addr, storage_value_bytes!(b"storage value 2"))]);

        let class0_definition = SerializedCairoDefinition::from_slice(
            starknet_gateway_test_fixtures::class_definitions::CONTRACT_DEFINITION,
        );
        let class1_definition = &class0_definition;
        let sierra_class_definition = SerializedSierraDefinition::from_slice(
            starknet_gateway_test_fixtures::class_definitions::CAIRO_0_11_SIERRA,
        );

        db_txn
            .insert_cairo_class_definition(class0_hash, &class0_definition)
            .unwrap();
        db_txn
            .insert_cairo_class_definition(class1_hash, class1_definition)
            .unwrap();
        db_txn
            .insert_sierra_class_definition(
                &sierra_class,
                &sierra_class_definition,
                &SerializedCasmDefinition::from_slice(&[]),
                &sierra_casm_hash_v2,
            )
            .unwrap();
        db_txn
            .insert_cairo_class_definition(class_hash_pending, &class0_definition)
            .unwrap();

        // Update block 0
        let update_results = update_contract_state(
            contract0_addr,
            (&contract0_update).into(),
            Some(contract_nonce!("0x1")),
            Some(class0_hash),
            &db_txn,
            false,
            BlockNumber::GENESIS,
        )
        .unwrap();
        let contract_state_hash = update_results.state_hash;
        update_results
            .insert(BlockNumber::GENESIS, &db_txn)
            .unwrap();
        let mut storage_commitment_tree = StorageCommitmentTree::empty(&db_txn);
        storage_commitment_tree
            .set(contract0_addr, contract_state_hash)
            .unwrap();

        let (storage_commitment0, trie_update) = storage_commitment_tree.commit().unwrap();
        let storage_root_idx = db_txn
            .insert_storage_trie(&trie_update, BlockNumber::GENESIS)
            .unwrap();
        db_txn
            .insert_storage_root(BlockNumber::GENESIS, storage_root_idx)
            .unwrap();
        let header0 = BlockHeader::builder()
            .number(BlockNumber::GENESIS)
            .calculated_state_commitment(storage_commitment0, class_commitment0)
            .event_commitment(event_commitment!("0xec00"))
            .event_count(0)
            .receipt_commitment(receipt_commitment!("0xdc00"))
            .transaction_commitment(transaction_commitment!("0xac00"))
            .transaction_count(0)
            .state_diff_commitment(state_diff_commitment!("0xfc00"))
            .state_diff_length(0)
            .starknet_version(StarknetVersion::V_0_13_2)
            .finalize_with_hash(block_hash_bytes!(b"genesis"));
        db_txn.insert_block_header(&header0).unwrap();
        db_txn
            .insert_state_update(header0.number, &state_update0)
            .unwrap();

        // Update block 1
        let mut storage_commitment_tree =
            StorageCommitmentTree::load(&db_txn, BlockNumber::GENESIS).unwrap();
        storage_commitment_tree
            .set(contract1_addr, contract_state_hash)
            .unwrap();
        let update_results = update_contract_state(
            contract1_addr,
            (&contract1_update1).into(),
            None,
            Some(class1_hash),
            &db_txn,
            false,
            BlockNumber::GENESIS + 1,
        )
        .unwrap();
        let contract_state_hash = update_results.state_hash;
        update_results
            .insert(BlockNumber::GENESIS + 1, &db_txn)
            .unwrap();
        storage_commitment_tree
            .set(contract1_addr, contract_state_hash)
            .unwrap();
        let (storage_commitment1, trie_update) = storage_commitment_tree.commit().unwrap();
        let storage_root_idx = db_txn
            .insert_storage_trie(&trie_update, BlockNumber::GENESIS + 1)
            .unwrap();
        db_txn
            .insert_storage_root(BlockNumber::GENESIS + 1, storage_root_idx)
            .unwrap();
        let header1 = header0
            .child_builder()
            .timestamp(BlockTimestamp::new_or_panic(1))
            .calculated_state_commitment(storage_commitment1, class_commitment1)
            .eth_l1_gas_price(GasPrice::from(1))
            .sequencer_address(sequencer_address_bytes!(&[1u8]))
            .event_commitment(event_commitment!("0xec01"))
            .event_count(1)
            .receipt_commitment(receipt_commitment!("0xdc01"))
            .transaction_commitment(transaction_commitment!("0xac01"))
            .transaction_count(1)
            .state_diff_commitment(state_diff_commitment!("0xfc01"))
            .state_diff_length(1)
            .starknet_version(StarknetVersion::V_0_13_2)
            .finalize_with_hash(block_hash_bytes!(b"block 1"));
        db_txn.insert_block_header(&header1).unwrap();
        db_txn
            .insert_state_update(header1.number, &state_update1)
            .unwrap();

        // Update block 2
        let mut storage_commitment_tree =
            StorageCommitmentTree::load(&db_txn, BlockNumber::GENESIS + 1).unwrap();
        let update_results = update_contract_state(
            contract1_addr,
            (&contract1_update2).into(),
            Some(contract_nonce!("0x10")),
            None,
            &db_txn,
            false,
            BlockNumber::GENESIS + 2,
        )
        .unwrap();
        let contract_state_hash = update_results.state_hash;
        update_results
            .insert(BlockNumber::GENESIS + 2, &db_txn)
            .unwrap();
        storage_commitment_tree
            .set(contract1_addr, contract_state_hash)
            .unwrap();

        let mut class_commitment_tree =
            ClassCommitmentTree::load(&db_txn, BlockNumber::GENESIS + 2).unwrap();
        let sierra_leaf_hash =
            pathfinder_common::calculate_class_commitment_leaf_hash(sierra_casm_hash);

        db_txn
            .insert_class_commitment_leaf(
                BlockNumber::GENESIS + 2,
                &sierra_leaf_hash,
                &sierra_casm_hash,
            )
            .unwrap();

        class_commitment_tree
            .set(sierra_class, sierra_leaf_hash)
            .unwrap();

        let (_, trie_update) = class_commitment_tree.commit().unwrap();

        let class_root_idx = db_txn
            .insert_class_trie(&trie_update, BlockNumber::GENESIS + 2)
            .unwrap();

        db_txn
            .insert_class_root(BlockNumber::GENESIS + 2, class_root_idx)
            .unwrap();

        let update_results = update_contract_state(
            contract2_addr,
            (&HashMap::new()).into(),
            Some(contract_nonce!("0xfeed")),
            Some(class2_hash),
            &db_txn,
            false,
            BlockNumber::GENESIS + 2,
        )
        .unwrap();
        let contract_state_hash = update_results.state_hash;
        update_results
            .insert(BlockNumber::GENESIS + 2, &db_txn)
            .unwrap();
        storage_commitment_tree
            .set(contract2_addr, contract_state_hash)
            .unwrap();
        let (storage_commitment2, trie_update) = storage_commitment_tree.commit().unwrap();
        let storage_root_idx = db_txn
            .insert_storage_trie(&trie_update, BlockNumber::GENESIS + 2)
            .unwrap();
        db_txn
            .insert_storage_root(BlockNumber::GENESIS + 2, storage_root_idx)
            .unwrap();
        let header2 = header1
            .child_builder()
            .timestamp(BlockTimestamp::new_or_panic(2))
            .calculated_state_commitment(storage_commitment2, class_commitment2)
            .eth_l1_gas_price(GasPrice::from(2))
            .sequencer_address(sequencer_address_bytes!(&[2u8]))
            .event_commitment(event_commitment!("0xec02"))
            .event_count(2)
            .receipt_commitment(receipt_commitment!("0xdc02"))
            .transaction_commitment(transaction_commitment!("0xac02"))
            .transaction_count(2)
            .state_diff_commitment(state_diff_commitment!("0xfc02"))
            .state_diff_length(2)
            .starknet_version(StarknetVersion::V_0_13_2)
            .finalize_with_hash(block_hash_bytes!(b"latest"));

        db_txn.insert_block_header(&header2).unwrap();
        db_txn
            .insert_state_update(header2.number, &state_update2)
            .unwrap();

        // TODO introduce other types of transactions too
        let txn0 = Transaction {
            hash: transaction_hash_bytes!(b"txn 0"),
            variant: TransactionVariant::InvokeV0(InvokeTransactionV0 {
                sender_address: contract0_addr,
                ..Default::default()
            }),
        };
        let receipt0 = Receipt {
            execution_resources: ExecutionResources {
                builtins: BuiltinCounters {
                    output: 33,
                    pedersen: 32,
                    ..Default::default()
                },
                n_memory_holes: 5,
                n_steps: 10,
                ..Default::default()
            },
            transaction_hash: txn0.hash,
            ..Default::default()
        };

        let txn1 = Transaction {
            hash: transaction_hash_bytes!(b"txn 1"),
            variant: TransactionVariant::InvokeV0(InvokeTransactionV0 {
                sender_address: contract1_addr,
                ..Default::default()
            }),
        };
        let txn2 = Transaction {
            hash: transaction_hash_bytes!(b"txn 2"),
            ..txn1.clone()
        };
        let txn3 = Transaction {
            hash: transaction_hash_bytes!(b"txn 3"),
            ..txn1.clone()
        };
        let txn4 = Transaction {
            hash: transaction_hash_bytes!(b"txn 4"),
            variant: TransactionVariant::InvokeV0(Default::default()),
        };
        let txn5 = Transaction {
            hash: transaction_hash_bytes!(b"txn 5"),
            ..txn1.clone()
        };
        let txn6 = Transaction {
            hash: transaction_hash_bytes!(b"txn 6"),
            variant: TransactionVariant::InvokeV3(InvokeTransactionV3 {
                sender_address: contract2_addr,
                proof_facts: vec![
                    proof_fact_elem_bytes!(b"proof fact 1"),
                    proof_fact_elem_bytes!(b"proof fact 2"),
                ],
                ..Default::default()
            }),
        };
        let txn_reverted = Transaction {
            hash: transaction_hash_bytes!(b"txn reverted"),
            ..txn1.clone()
        };
        let mut receipt1 = receipt0.clone();
        let mut receipt2 = receipt0.clone();
        let mut receipt3 = receipt0.clone();
        let mut receipt4 = receipt0.clone();
        let mut receipt5 = receipt0.clone();
        let mut receipt_reverted = receipt0.clone();
        let mut receipt6 = Receipt {
            l2_to_l1_messages: vec![L2ToL1Message {
                from_address: contract_address!("0xcafebabe"),
                payload: vec![
                    l2_to_l1_message_payload_elem!("0x1"),
                    l2_to_l1_message_payload_elem!("0x2"),
                    l2_to_l1_message_payload_elem!("0x3"),
                ],
                to_address: ContractAddress::ZERO,
            }],
            ..receipt0.clone()
        };
        let events0 = vec![Event {
            data: vec![event_data_bytes!(b"event 0 data")],
            from_address: contract_address_bytes!(b"event 0 from addr"),
            keys: vec![event_key_bytes!(b"event 0 key")],
        }];
        receipt1.transaction_hash = txn1.hash;
        receipt2.transaction_hash = txn2.hash;
        receipt3.transaction_hash = txn3.hash;
        receipt4.transaction_hash = txn4.hash;
        receipt5.transaction_hash = txn5.hash;
        receipt6.transaction_hash = txn6.hash;
        receipt_reverted.transaction_hash = txn_reverted.hash;
        receipt_reverted.execution_status = ExecutionStatus::Reverted {
            reason: "Reverted because".to_owned(),
        };

        let transactions0 = vec![(txn0, receipt0)];
        let events0 = vec![events0];
        let transactions1 = vec![(txn1, receipt1), (txn2, receipt2)];
        let events1 = vec![vec![], vec![]];
        let transactions2 = vec![
            (txn3, receipt3),
            (txn4, receipt4),
            (txn5, receipt5),
            (txn6, receipt6),
            (txn_reverted, receipt_reverted),
        ];
        let events2 = vec![vec![], vec![], vec![], vec![], vec![]];
        db_txn
            .insert_transaction_data(header0.number, &transactions0, Some(&events0))
            .unwrap();
        db_txn
            .insert_transaction_data(header1.number, &transactions1, Some(&events1))
            .unwrap();
        db_txn
            .insert_transaction_data(header2.number, &transactions2, Some(&events2))
            .unwrap();

        // Mark block 1 as L1 accepted.
        db_txn.update_l1_l2_pointer(Some(header1.number)).unwrap();

        db_txn.commit().unwrap();
        storage
    }

    /// Creates [PendingData] which correctly links to the provided [Storage].
    ///
    /// For pre-confirmed blocks that means that the block number is the next
    /// block number after latest.
    pub async fn create_pre_confirmed_data(storage: Storage) -> PendingData {
        let storage2 = storage.clone();
        let latest = tokio::task::spawn_blocking(move || {
            let mut db = storage2.connection().unwrap();
            let tx = db.transaction().unwrap();

            tx.block_header(BlockId::Latest)
                .unwrap()
                .expect("Storage should contain a latest block")
        })
        .await
        .unwrap();

        let transactions: Vec<Transaction> = vec![
            Transaction {
                hash: transaction_hash_bytes!(b"preconfirmed tx hash 0"),
                variant: TransactionVariant::InvokeV0(InvokeTransactionV0 {
                    sender_address: contract_address_bytes!(b"preconfirmed contract addr 0"),
                    entry_point_selector: entry_point_bytes!(b"entry point 0"),
                    entry_point_type: Some(EntryPointType::External),
                    ..Default::default()
                }),
            },
            Transaction {
                hash: transaction_hash_bytes!(b"preconfirmed tx hash 1"),
                variant: TransactionVariant::DeployV0(DeployTransactionV0 {
                    contract_address: contract_address!("0x1122355"),
                    contract_address_salt: contract_address_salt_bytes!(b"salty"),
                    class_hash: class_hash_bytes!(b"preconfirmed class hash 1"),
                    ..Default::default()
                }),
            },
            Transaction {
                hash: transaction_hash_bytes!(b"preconfirmed reverted"),
                variant: TransactionVariant::InvokeV0(InvokeTransactionV0 {
                    sender_address: contract_address_bytes!(b"preconfirmed contract addr 0"),
                    entry_point_selector: entry_point_bytes!(b"entry point 0"),
                    entry_point_type: Some(EntryPointType::External),
                    ..Default::default()
                }),
            },
        ];

        let candidate_transactions = vec![Transaction {
            hash: transaction_hash_bytes!(b"candidate tx hash 0"),
            variant: TransactionVariant::InvokeV0(InvokeTransactionV0 {
                sender_address: contract_address_bytes!(b"candidate contract addr 0"),
                entry_point_selector: entry_point_bytes!(b"entry point 0"),
                entry_point_type: Some(EntryPointType::External),
                ..Default::default()
            }),
        }];

        let transaction_receipts = vec![
            (
                Receipt {
                    actual_fee: Fee::ZERO,
                    execution_resources: ExecutionResources::default(),
                    transaction_hash: transactions[0].hash,
                    transaction_index: TransactionIndex::new_or_panic(0),
                    ..Default::default()
                },
                vec![
                    Event {
                        data: vec![],
                        from_address: contract_address!("0xabcddddddd"),
                        keys: vec![event_key_bytes!(b"preconfirmed key")],
                    },
                    Event {
                        data: vec![],
                        from_address: contract_address!("0xabcddddddd"),
                        keys: vec![
                            event_key_bytes!(b"preconfirmed key"),
                            event_key_bytes!(b"second preconfirmed key"),
                        ],
                    },
                    Event {
                        data: vec![],
                        from_address: contract_address!("0xabcaaaaaaa"),
                        keys: vec![event_key_bytes!(b"preconfirmed key 2")],
                    },
                ],
            ),
            (
                Receipt {
                    execution_resources: ExecutionResources::default(),
                    transaction_hash: transactions[1].hash,
                    transaction_index: TransactionIndex::new_or_panic(1),
                    ..Default::default()
                },
                vec![],
            ),
            // Reverted and without events
            (
                Receipt {
                    execution_resources: ExecutionResources::default(),
                    transaction_hash: transactions[2].hash,
                    transaction_index: TransactionIndex::new_or_panic(2),
                    execution_status: ExecutionStatus::Reverted {
                        reason: "Reverted!".to_owned(),
                    },
                    ..Default::default()
                },
                vec![],
            ),
        ];

        let transactions = transactions.into_iter().collect();
        let transaction_receipts = transaction_receipts.into_iter().collect();

        let contract1 = contract_address_bytes!(b"preconfirmed contract 1 address");
        let state_update = StateUpdate::default()
            .with_parent_state_commitment(latest.state_commitment)
            .with_declared_cairo_class(class_hash_bytes!(b"preconfirmed class 0 hash"))
            .with_declared_cairo_class(class_hash_bytes!(b"preconfirmed class 1 hash"))
            .with_deployed_contract(
                contract_address_bytes!(b"preconfirmed contract 0 address"),
                class_hash_bytes!(b"preconfirmed class 0 hash"),
            )
            .with_deployed_contract(contract1, class_hash_bytes!(b"preconfirmed class 1 hash"))
            .with_storage_update(
                contract1,
                storage_address_bytes!(b"preconfirmed storage key 0"),
                storage_value_bytes!(b"preconfirmed storage value 0"),
            )
            .with_storage_update(
                contract1,
                storage_address_bytes!(b"preconfirmed storage key 1"),
                storage_value_bytes!(b"preconfirmed storage value 1"),
            )
            // This is not a real contract and should be re-worked..
            .with_replaced_class(
                contract_address_bytes!(b"preconfirmed contract 2 rplcd"),
                class_hash_bytes!(b"preconfirmed class 2 hash rplcd"),
            )
            .with_contract_nonce(
                contract_address_bytes!(b"contract 1"),
                contract_nonce_bytes!(b"preconfirmed nonce"),
            );

        let block = crate::pending::PendingBlocks {
            pre_confirmed: crate::pending::PreConfirmedBlock {
                number: latest.number + 1,
                l1_gas_price: GasPrices {
                    price_in_wei: GasPrice::from_be_slice(b"gas price").unwrap(),
                    price_in_fri: GasPrice::from_be_slice(b"strk gas price").unwrap(),
                },
                l1_data_gas_price: GasPrices {
                    price_in_wei: GasPrice::from_be_slice(b"datgasprice").unwrap(),
                    price_in_fri: GasPrice::from_be_slice(b"strk datgasprice").unwrap(),
                },
                l2_gas_price: GasPrices {
                    price_in_wei: GasPrice::from_be_slice(b"l2 gas price").unwrap(),
                    price_in_fri: GasPrice::from_be_slice(b"strk l2gas price").unwrap(),
                },
                sequencer_address: sequencer_address_bytes!(b"preconfirmed sequencer address"),
                status: starknet_gateway_types::reply::Status::Pending,
                timestamp: BlockTimestamp::new_or_panic(1234567),
                transaction_receipts,
                transactions,
                starknet_version: StarknetVersion::V_0_13_2,
                l1_da_mode: L1DataAvailabilityMode::Calldata,
            },
            pre_latest: None,
            candidate_transactions,
        };

        // The class definitions must be inserted into the database.
        let state_update_copy = state_update.clone();
        tokio::task::spawn_blocking(move || {
            let mut db = storage.connection().unwrap();
            let tx = db.transaction().unwrap();
            let class_definition =
                starknet_gateway_test_fixtures::class_definitions::CONTRACT_DEFINITION;

            for cairo in state_update_copy.declared_cairo_classes {
                tx.insert_cairo_class_definition(
                    cairo,
                    &SerializedCairoDefinition::from_slice(class_definition),
                )
                .unwrap();
            }

            for (sierra, casm) in state_update_copy.declared_sierra_classes {
                tx.insert_sierra_class_definition(
                    &sierra,
                    &SerializedSierraDefinition::from_slice(b"sierra def"),
                    &SerializedCasmDefinition::from_slice(b"casm def"),
                    &casm,
                )
                .unwrap();
            }

            tx.commit().unwrap();
        })
        .await
        .unwrap();

        // Aggregated state update is the same as state update for pre-confirmed blocks
        // as there's no pre-latest block.
        let aggregated_state_update = state_update.clone();
        PendingData::from_parts(
            block,
            state_update,
            aggregated_state_update,
            latest.number + 1,
        )
    }

    /// Creates [PendingData] which correctly links to the provided [Storage].
    ///
    /// For pre-confirmed blocks with pre-latest data that means that the block
    /// number of the pre-latest block is the next block number after latest,
    /// and the pre-confirmed block number is the one after that.
    pub async fn create_pre_confirmed_data_with_pre_latest(storage: Storage) -> PendingData {
        let storage2 = storage.clone();
        let latest = tokio::task::spawn_blocking(move || {
            let mut db = storage2.connection().unwrap();
            let tx = db.transaction().unwrap();

            tx.block_header(BlockId::Latest)
                .unwrap()
                .expect("Storage should contain a latest block")
        })
        .await
        .unwrap();

        let pre_latest_transactions: Vec<Transaction> = vec![
            Transaction {
                hash: transaction_hash_bytes!(b"prelatest tx hash 0"),
                variant: TransactionVariant::InvokeV0(InvokeTransactionV0 {
                    sender_address: contract_address_bytes!(b"prelatest contract addr 0"),
                    entry_point_selector: entry_point_bytes!(b"entry point 0"),
                    entry_point_type: Some(EntryPointType::External),
                    ..Default::default()
                }),
            },
            Transaction {
                hash: transaction_hash_bytes!(b"prelatest tx hash 1"),
                variant: TransactionVariant::DeployV0(DeployTransactionV0 {
                    contract_address: contract_address!("0x1122355"),
                    contract_address_salt: contract_address_salt_bytes!(b"salty"),
                    class_hash: class_hash_bytes!(b"prelatest class hash 1"),
                    ..Default::default()
                }),
            },
            Transaction {
                hash: transaction_hash_bytes!(b"prelatest reverted"),
                variant: TransactionVariant::InvokeV0(InvokeTransactionV0 {
                    sender_address: contract_address_bytes!(b"prelatest contract addr 0"),
                    entry_point_selector: entry_point_bytes!(b"entry point 0"),
                    entry_point_type: Some(EntryPointType::External),
                    ..Default::default()
                }),
            },
        ];

        let pre_latest_tx_receipts = vec![
            (
                Receipt {
                    actual_fee: Fee::ZERO,
                    execution_resources: ExecutionResources::default(),
                    transaction_hash: pre_latest_transactions[0].hash,
                    transaction_index: TransactionIndex::new_or_panic(0),
                    ..Default::default()
                },
                vec![
                    Event {
                        data: vec![],
                        from_address: contract_address!("0xabcddddddd"),
                        keys: vec![event_key_bytes!(b"prelatest key")],
                    },
                    Event {
                        data: vec![],
                        from_address: contract_address!("0xabcddddddd"),
                        keys: vec![
                            event_key_bytes!(b"prelatest key"),
                            event_key_bytes!(b"second prelatest key"),
                        ],
                    },
                    Event {
                        data: vec![],
                        from_address: contract_address!("0xabcaaaaaaa"),
                        keys: vec![event_key_bytes!(b"prelatest key 2")],
                    },
                ],
            ),
            (
                Receipt {
                    execution_resources: ExecutionResources::default(),
                    transaction_hash: pre_latest_transactions[1].hash,
                    transaction_index: TransactionIndex::new_or_panic(1),
                    ..Default::default()
                },
                vec![],
            ),
            // Reverted and without events
            (
                Receipt {
                    execution_resources: ExecutionResources::default(),
                    transaction_hash: pre_latest_transactions[2].hash,
                    transaction_index: TransactionIndex::new_or_panic(2),
                    execution_status: ExecutionStatus::Reverted {
                        reason: "Reverted!".to_owned(),
                    },
                    ..Default::default()
                },
                vec![],
            ),
        ];

        let pre_latest_contract1 = contract_address_bytes!(b"prelatest contract 1 address");
        let pre_latest_state_update = StateUpdate::default()
            .with_parent_state_commitment(latest.state_commitment)
            .with_declared_cairo_class(class_hash_bytes!(b"prelatest class 0 hash"))
            .with_declared_cairo_class(class_hash_bytes!(b"prelatest class 1 hash"))
            .with_deployed_contract(
                contract_address_bytes!(b"prelatest contract 0 address"),
                class_hash_bytes!(b"prelatest class 0 hash"),
            )
            .with_deployed_contract(
                pre_latest_contract1,
                class_hash_bytes!(b"prelatest class 1 hash"),
            )
            .with_storage_update(
                pre_latest_contract1,
                storage_address_bytes!(b"prelatest storage key 0"),
                storage_value_bytes!(b"prelatest storage value 0"),
            )
            .with_storage_update(
                pre_latest_contract1,
                storage_address_bytes!(b"prelatest storage key 1"),
                storage_value_bytes!(b"prelatest storage value 1"),
            )
            // This is not a real contract and should be re-worked..
            .with_replaced_class(
                contract_address_bytes!(b"prelatest contract 2 rplcd"),
                class_hash_bytes!(b"prelatest class 2 hash rplcd"),
            )
            .with_contract_nonce(
                pre_latest_contract1,
                contract_nonce_bytes!(b"prelatest nonce"),
            );

        let pre_latest_block = PreLatestBlock {
            // Pre-latest is between current latest and pre-confirmed.
            number: latest.number + 1,
            parent_hash: latest.hash,
            l1_gas_price: GasPrices {
                price_in_wei: GasPrice::from_be_slice(b"gas price").unwrap(),
                price_in_fri: GasPrice::from_be_slice(b"strk gas price").unwrap(),
            },
            l1_data_gas_price: GasPrices {
                price_in_wei: GasPrice::from_be_slice(b"datgasprice").unwrap(),
                price_in_fri: GasPrice::from_be_slice(b"strk datgasprice").unwrap(),
            },
            l2_gas_price: GasPrices {
                price_in_wei: GasPrice::from_be_slice(b"l2 gas price").unwrap(),
                price_in_fri: GasPrice::from_be_slice(b"strk l2gas price").unwrap(),
            },
            sequencer_address: sequencer_address_bytes!(b"pre-latest sequencer address"),
            status: starknet_gateway_types::reply::Status::Pending,
            timestamp: BlockTimestamp::new_or_panic(1234567),
            transaction_receipts: pre_latest_tx_receipts,
            transactions: pre_latest_transactions,
            starknet_version: StarknetVersion::V_0_13_2,
            l1_da_mode: L1DataAvailabilityMode::Calldata,
        };

        let pre_confirmed_transactions: Vec<Transaction> = vec![
            Transaction {
                hash: transaction_hash_bytes!(b"preconfirmed tx hash 0"),
                variant: TransactionVariant::InvokeV0(InvokeTransactionV0 {
                    sender_address: contract_address_bytes!(b"preconfirmed contract addr 0"),
                    entry_point_selector: entry_point_bytes!(b"entry point 0"),
                    entry_point_type: Some(EntryPointType::External),
                    ..Default::default()
                }),
            },
            Transaction {
                hash: transaction_hash_bytes!(b"preconfirmed tx hash 1"),
                variant: TransactionVariant::DeployV0(DeployTransactionV0 {
                    contract_address: contract_address!("0x1122355"),
                    contract_address_salt: contract_address_salt_bytes!(b"salty"),
                    class_hash: class_hash_bytes!(b"preconfirmed class hash 1"),
                    ..Default::default()
                }),
            },
            Transaction {
                hash: transaction_hash_bytes!(b"preconfirmed reverted"),
                variant: TransactionVariant::InvokeV0(InvokeTransactionV0 {
                    sender_address: contract_address_bytes!(b"preconfirmed contract addr 0"),
                    entry_point_selector: entry_point_bytes!(b"entry point 0"),
                    entry_point_type: Some(EntryPointType::External),
                    ..Default::default()
                }),
            },
        ];

        let candidate_transactions = vec![Transaction {
            hash: transaction_hash_bytes!(b"candidate tx hash 0"),
            variant: TransactionVariant::InvokeV0(InvokeTransactionV0 {
                sender_address: contract_address_bytes!(b"candidate contract addr 0"),
                entry_point_selector: entry_point_bytes!(b"entry point 0"),
                entry_point_type: Some(EntryPointType::External),
                ..Default::default()
            }),
        }];

        let pre_confirmed_tx_receipts = vec![
            (
                Receipt {
                    actual_fee: Fee::ZERO,
                    execution_resources: ExecutionResources::default(),
                    transaction_hash: pre_confirmed_transactions[0].hash,
                    transaction_index: TransactionIndex::new_or_panic(0),
                    ..Default::default()
                },
                vec![
                    Event {
                        data: vec![],
                        from_address: contract_address!("0xabcddddddd"),
                        keys: vec![event_key_bytes!(b"preconfirmed key")],
                    },
                    Event {
                        data: vec![],
                        from_address: contract_address!("0xabcddddddd"),
                        keys: vec![
                            event_key_bytes!(b"preconfirmed key"),
                            event_key_bytes!(b"second preconfirmed key"),
                        ],
                    },
                    Event {
                        data: vec![],
                        from_address: contract_address!("0xabcaaaaaaa"),
                        keys: vec![event_key_bytes!(b"preconfirmed key 2")],
                    },
                ],
            ),
            (
                Receipt {
                    execution_resources: ExecutionResources::default(),
                    transaction_hash: pre_confirmed_transactions[1].hash,
                    transaction_index: TransactionIndex::new_or_panic(1),
                    ..Default::default()
                },
                vec![],
            ),
            // Reverted and without events
            (
                Receipt {
                    execution_resources: ExecutionResources::default(),
                    transaction_hash: pre_confirmed_transactions[2].hash,
                    transaction_index: TransactionIndex::new_or_panic(2),
                    execution_status: ExecutionStatus::Reverted {
                        reason: "Reverted!".to_owned(),
                    },
                    ..Default::default()
                },
                vec![],
            ),
        ];

        let pre_confirmed_contract1 = contract_address_bytes!(b"preconfirmed contract 1 address");
        let pre_confirmed_state_update = StateUpdate::default()
            .with_declared_cairo_class(class_hash_bytes!(b"preconfirmed class 0 hash"))
            .with_declared_cairo_class(class_hash_bytes!(b"preconfirmed class 1 hash"))
            .with_deployed_contract(
                contract_address_bytes!(b"preconfirmed contract 0 address"),
                class_hash_bytes!(b"preconfirmed class 0 hash"),
            )
            .with_deployed_contract(
                pre_confirmed_contract1,
                class_hash_bytes!(b"preconfirmed class 1 hash"),
            )
            .with_storage_update(
                pre_confirmed_contract1,
                storage_address_bytes!(b"preconfirmed storage key 0"),
                storage_value_bytes!(b"preconfirmed storage value 0"),
            )
            .with_storage_update(
                pre_confirmed_contract1,
                storage_address_bytes!(b"preconfirmed storage key 1"),
                storage_value_bytes!(b"preconfirmed storage value 1"),
            )
            // This is not a real contract and should be re-worked..
            .with_replaced_class(
                contract_address_bytes!(b"preconfirmed contract 2 rplcd"),
                class_hash_bytes!(b"preconfirmed class 2 hash rplcd"),
            )
            .with_contract_nonce(
                pre_confirmed_contract1,
                contract_nonce_bytes!(b"preconfirmed nonce"),
            );

        let pre_confirmed_block = crate::pending::PendingBlocks {
            pre_confirmed: crate::pending::PreConfirmedBlock {
                // Pre-confirmed block is two blocks after latest when pre-latest
                // is also present.
                number: latest.number + 2,
                l1_gas_price: GasPrices {
                    price_in_wei: GasPrice::from_be_slice(b"gas price").unwrap(),
                    price_in_fri: GasPrice::from_be_slice(b"strk gas price").unwrap(),
                },
                l1_data_gas_price: GasPrices {
                    price_in_wei: GasPrice::from_be_slice(b"datgasprice").unwrap(),
                    price_in_fri: GasPrice::from_be_slice(b"strk datgasprice").unwrap(),
                },
                l2_gas_price: GasPrices {
                    price_in_wei: GasPrice::from_be_slice(b"l2 gas price").unwrap(),
                    price_in_fri: GasPrice::from_be_slice(b"strk l2gas price").unwrap(),
                },
                sequencer_address: sequencer_address_bytes!(b"preconfirmed sequencer address"),
                status: starknet_gateway_types::reply::Status::Pending,
                timestamp: BlockTimestamp::new_or_panic(1234567),
                transaction_receipts: pre_confirmed_tx_receipts,
                transactions: pre_confirmed_transactions,
                starknet_version: StarknetVersion::V_0_13_2,
                l1_da_mode: L1DataAvailabilityMode::Calldata,
            },
            pre_latest: Some(PreLatestData {
                block: pre_latest_block,
                state_update: pre_latest_state_update.clone(),
            }),
            candidate_transactions,
        };

        let aggregated_state_update = pre_latest_state_update
            .clone()
            .apply(&pre_confirmed_state_update);

        // The class definitions must be inserted into the database.
        let pre_confirmed_state_update_copy = pre_confirmed_state_update.clone();
        tokio::task::spawn_blocking(move || {
            let mut db = storage.connection().unwrap();
            let tx = db.transaction().unwrap();
            let class_definition =
                starknet_gateway_test_fixtures::class_definitions::CONTRACT_DEFINITION;

            for cairo in pre_latest_state_update.declared_cairo_classes {
                tx.insert_cairo_class_definition(
                    cairo,
                    &SerializedCairoDefinition::from_slice(class_definition),
                )
                .unwrap();
            }
            for (sierra, casm) in pre_latest_state_update.declared_sierra_classes {
                tx.insert_sierra_class_definition(
                    &sierra,
                    &SerializedSierraDefinition::from_slice(b"sierra def"),
                    &SerializedCasmDefinition::from_slice(b"casm def"),
                    &casm,
                )
                .unwrap();
            }

            for cairo in pre_confirmed_state_update_copy.declared_cairo_classes {
                tx.insert_cairo_class_definition(
                    cairo,
                    &SerializedCairoDefinition::from_slice(class_definition),
                )
                .unwrap();
            }
            for (sierra, casm) in pre_confirmed_state_update_copy.declared_sierra_classes {
                tx.insert_sierra_class_definition(
                    &sierra,
                    &SerializedSierraDefinition::from_slice(b"sierra def"),
                    &SerializedCasmDefinition::from_slice(b"casm def"),
                    &casm,
                )
                .unwrap();
            }

            tx.commit().unwrap();
        })
        .await
        .unwrap();

        PendingData::from_parts(
            pre_confirmed_block,
            pre_confirmed_state_update,
            aggregated_state_update,
            latest.number + 2,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use dto::DeserializeForVersion;
    use serde_json::json;

    use super::*;

    #[test]
    fn roundtrip_syncing() {
        use crate::types::syncing::{NumberedBlock, Status, Syncing};

        let examples = [
            (line!(), "false", Syncing::False),
            (
                line!(),
                r#"{"starting_block_hash":"0xa","starting_block_num":"0x1","current_block_hash":"0xb","current_block_num":"0x2","highest_block_hash":"0xc","highest_block_num":"0x3"}"#,
                Syncing::Status(Status {
                    starting: NumberedBlock::from(("0xa", 1)),
                    current: NumberedBlock::from(("0xb", 2)),
                    highest: NumberedBlock::from(("0xc", 3)),
                }),
            ),
        ];

        for (line, input, expected) in examples {
            let parsed =
                Syncing::deserialize(crate::dto::Value::from_str(input, RpcVersion::V07).unwrap())
                    .unwrap();
            assert_eq!(parsed, expected, "example from line {line}");
        }
    }

    #[tokio::test]
    async fn empty_get_on_root_is_ok() {
        // Monitoring bots often get query `/` with no body as a form
        // of health check. Test that we return success for such queries.
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let context = RpcContext::for_tests();
        let (_jh, addr) = RpcServer::new(addr, context, RpcVersion::V07)
            .spawn(&PathBuf::default())
            .await
            .unwrap();

        let url = format!("http://{addr}/");

        let client = reqwest::Client::new();
        // No body
        let status = client.get(url.clone()).send().await.unwrap().status();
        assert!(status.is_success());
        // Empty body - unsure if this is actually different to no body.
        let status = client
            .get(url.clone())
            .body("")
            .send()
            .await
            .unwrap()
            .status();
        assert!(status.is_success());
        // Non-empty body should fail.
        let status = client
            .get(url.clone())
            .body("x")
            .send()
            .await
            .unwrap()
            .status();
        assert!(!status.is_success());
    }

    enum Api {
        HttpOnly,
        WebsocketOnly,
        Both,
    }

    impl Api {
        fn has_websocket(&self) -> bool {
            matches!(self, Self::WebsocketOnly | Self::Both)
        }

        fn has_http(&self) -> bool {
            matches!(self, Self::HttpOnly | Self::Both)
        }
    }

    #[rustfmt::skip]
    #[rstest::rstest]
    #[case::root_api("/", "v07/starknet_api_openrpc.json",       &[], Api::HttpOnly)]
    #[case::root_api_websocket("/ws", "v07/starknet_api_openrpc.json",       &[], Api::WebsocketOnly)]
    #[case::root_trace("/", "v07/starknet_trace_api_openrpc.json", &[], Api::HttpOnly)]
    #[case::root_trace_websocket("/ws", "v07/starknet_trace_api_openrpc.json", &[], Api::WebsocketOnly)]
    #[case::root_write("/", "v07/starknet_write_api.json",         &[], Api::HttpOnly)]
    #[case::root_write_websocket("/ws", "v07/starknet_write_api.json",         &[], Api::WebsocketOnly)]
    #[case::root_pathfinder("/", "pathfinder_rpc_api.json", &["pathfinder_version"], Api::HttpOnly)]
    #[case::root_pathfinder_websocket("/ws", "pathfinder_rpc_api.json", &["pathfinder_version"], Api::WebsocketOnly)]

    #[case::v0_10_api("/rpc/v0_10", "v10/starknet_api_openrpc.json", &[], Api::Both)]
    #[case::v0_10_executables("/rpc/v0_10", "v10/starknet_executables.json", &[], Api::Both)]
    #[case::v0_10_trace("/rpc/v0_10", "v10/starknet_trace_api_openrpc.json", &[], Api::Both)]
    #[case::v0_10_write("/rpc/v0_10", "v10/starknet_write_api.json", &[], Api::Both)]
    #[case::v0_10_websocket(
        "/rpc/v0_10",
        "v10/starknet_ws_api.json",
        // "starknet_subscription*" methods are in fact notifications
        &[
            "starknet_subscriptionNewHeads",
            "starknet_subscriptionTransactionStatus",
            "starknet_subscriptionEvents",
            "starknet_subscriptionNewTransactionReceipts",
            "starknet_subscriptionNewTransaction",
            "starknet_subscriptionReorg"
        ],
        Api::WebsocketOnly)]

    #[case::v0_10_api_alternative_path("/ws/rpc/v0_10", "v10/starknet_api_openrpc.json", &[], Api::Both)]
    #[case::v0_10_executables_alternative_path("/ws/rpc/v0_10", "v10/starknet_executables.json", &[], Api::Both)]
    #[case::v0_10_trace_alternative_path("/ws/rpc/v0_10", "v10/starknet_trace_api_openrpc.json", &[], Api::Both)]
    #[case::v0_10_write_alternative_path("/ws/rpc/v0_10", "v10/starknet_write_api.json", &[], Api::Both)]
    #[case::v0_10_websocket_alternative_path(
        "/ws/rpc/v0_10",
        "v10/starknet_ws_api.json",
        // "starknet_subscription*" methods are in fact notifications
        &[
            "starknet_subscriptionNewHeads",
            "starknet_subscriptionTransactionStatus",
            "starknet_subscriptionEvents",
            "starknet_subscriptionNewTransactionReceipts",
            "starknet_subscriptionNewTransaction",
            "starknet_subscriptionReorg"
        ],
        Api::WebsocketOnly)]
    #[case::v0_10_pathfinder("/rpc/v0_10", "pathfinder_rpc_api.json", &["pathfinder_version"], Api::Both)]

    #[case::v0_9_api("/rpc/v0_9", "v09/starknet_api_openrpc.json", &[], Api::Both)]
    #[case::v0_9_executables("/rpc/v0_9", "v09/starknet_executables.json", &[], Api::Both)]
    #[case::v0_9_trace("/rpc/v0_9", "v09/starknet_trace_api_openrpc.json", &[], Api::Both)]
    #[case::v0_9_write("/rpc/v0_9", "v09/starknet_write_api.json", &[], Api::Both)]
    #[case::v0_9_websocket(
        "/rpc/v0_9",
        "v09/starknet_ws_api.json",
        // "starknet_subscription*" methods are in fact notifications
        &[
            "starknet_subscriptionNewHeads",
            "starknet_subscriptionTransactionStatus",
            "starknet_subscriptionEvents",
            "starknet_subscriptionNewTransactionReceipts",
            "starknet_subscriptionNewTransaction",
            "starknet_subscriptionReorg"
        ],
        Api::WebsocketOnly)]

    #[case::v0_9_api_alternative_path("/ws/rpc/v0_9", "v09/starknet_api_openrpc.json", &[], Api::Both)]
    #[case::v0_9_executables_alternative_path("/ws/rpc/v0_9", "v09/starknet_executables.json", &[], Api::Both)]
    #[case::v0_9_trace_alternative_path("/ws/rpc/v0_9", "v09/starknet_trace_api_openrpc.json", &[], Api::Both)]
    #[case::v0_9_write_alternative_path("/ws/rpc/v0_9", "v09/starknet_write_api.json", &[], Api::Both)]
    #[case::v0_9_websocket_alternative_path(
        "/ws/rpc/v0_9",
        "v09/starknet_ws_api.json",
        // "starknet_subscription*" methods are in fact notifications
        &[
            "starknet_subscriptionNewHeads",
            "starknet_subscriptionTransactionStatus",
            "starknet_subscriptionEvents",
            "starknet_subscriptionNewTransactionReceipts",
            "starknet_subscriptionNewTransaction",
            "starknet_subscriptionReorg"
        ],
        Api::WebsocketOnly)]
    #[case::v0_9_pathfinder("/rpc/v0_9", "pathfinder_rpc_api.json", &["pathfinder_version"], Api::Both)]

    #[case::v0_8_api("/rpc/v0_8", "v08/starknet_api_openrpc.json", &[], Api::Both)]
    #[case::v0_8_executables("/rpc/v0_8", "v08/starknet_executables.json", &[], Api::Both)]
    #[case::v0_8_trace("/rpc/v0_8", "v08/starknet_trace_api_openrpc.json", &[], Api::Both)]
    #[case::v0_8_write("/rpc/v0_8", "v08/starknet_write_api.json", &[], Api::Both)]
    #[case::v0_8_websocket(
        "/rpc/v0_8",
        "v08/starknet_ws_api.json",
        // "starknet_subscription*" methods are in fact notifications
        &[
            "starknet_subscriptionNewHeads",
            "starknet_subscriptionPendingTransactions",
            "starknet_subscriptionTransactionStatus",
            "starknet_subscriptionEvents",
            "starknet_subscriptionReorg"
        ],
        Api::WebsocketOnly)]

    #[case::v0_8_api_alternative_path("/ws/rpc/v0_8", "v08/starknet_api_openrpc.json", &[], Api::Both)]
    #[case::v0_8_executables_alternative_path("/ws/rpc/v0_8", "v08/starknet_executables.json", &[], Api::Both)]
    #[case::v0_8_trace_alternative_path("/ws/rpc/v0_8", "v08/starknet_trace_api_openrpc.json", &[], Api::Both)]
    #[case::v0_8_write_alternative_path("/ws/rpc/v0_8", "v08/starknet_write_api.json", &[], Api::Both)]
    #[case::v0_8_websocket_alternative_path(
        "/ws/rpc/v0_8",
        "v08/starknet_ws_api.json",
        // "starknet_subscription*" methods are in fact notifications
        &[
            "starknet_subscriptionNewHeads",
            "starknet_subscriptionPendingTransactions",
            "starknet_subscriptionTransactionStatus",
            "starknet_subscriptionEvents",
            "starknet_subscriptionReorg"
        ],
        Api::WebsocketOnly)]
    #[case::v0_8_pathfinder("/rpc/v0_8", "pathfinder_rpc_api.json", &["pathfinder_version"], Api::Both)]

    #[case::v0_7_api("/rpc/v0_7", "v07/starknet_api_openrpc.json", &[], Api::HttpOnly)]
    #[case::v0_7_api_websocket("/ws/rpc/v0_7", "v07/starknet_api_openrpc.json", &[], Api::WebsocketOnly)]
    #[case::v0_7_trace("/rpc/v0_7", "v07/starknet_trace_api_openrpc.json", &[], Api::HttpOnly)]
    #[case::v0_7_trace_websocket("/ws/rpc/v0_7", "v07/starknet_trace_api_openrpc.json", &[], Api::WebsocketOnly)]
    #[case::v0_7_write("/rpc/v0_7", "v07/starknet_write_api.json", &[], Api::HttpOnly)]
    #[case::v0_7_write_websocket("/ws/rpc/v0_7", "v07/starknet_write_api.json", &[], Api::WebsocketOnly)]
    #[case::v0_7_pathfinder("/rpc/v0_7", "pathfinder_rpc_api.json", &["pathfinder_version"], Api::HttpOnly)]
    #[case::v0_7_pathfinder_websocket("/ws/rpc/v0_7", "pathfinder_rpc_api.json", &["pathfinder_version"], Api::WebsocketOnly)]

    #[case::v0_6_api(
        "/rpc/v0_6",
        "v06/starknet_api_openrpc.json",
        &[],
        Api::HttpOnly)]
    #[case::v0_6_api_websocket("/ws/rpc/v0_6", "v06/starknet_api_openrpc.json", &[], Api::WebsocketOnly)]
    #[case::v0_6_trace(
        "/rpc/v0_6",
        "v06/starknet_trace_api_openrpc.json",
        &[],
        Api::HttpOnly)]
    #[case::v0_6_trace_websocket("/ws/rpc/v0_6", "v06/starknet_trace_api_openrpc.json", &[], Api::WebsocketOnly)]
    #[case::v0_6_write("/rpc/v0_6", "v06/starknet_write_api.json", &[], Api::HttpOnly)]
    #[case::v0_6_write_websocket("/ws/rpc/v0_6", "v06/starknet_write_api.json", &[], Api::WebsocketOnly)]
    #[case::v0_6_pathfinder("/rpc/v0_6", "pathfinder_rpc_api.json", &["pathfinder_version"], Api::HttpOnly)]
    #[case::v0_6_pathfinder_websocket("/ws/rpc/v0_6", "pathfinder_rpc_api.json", &["pathfinder_version"], Api::WebsocketOnly)]

    #[case::pathfinder("/rpc/pathfinder/v0.1", "pathfinder_rpc_api.json", &[], Api::HttpOnly)]
    #[case::pathfinder("/ws/rpc/pathfinder/v0_1", "pathfinder_rpc_api.json", &[], Api::WebsocketOnly)]

    #[tokio::test]
    async fn rpc_routing(
        #[case] route: &'static str,
        #[case] specification: std::path::PathBuf,
        #[case] exclude: &[&'static str],
        #[case] api: Api,
    ) {
        use crate::jsonrpc::websocket::WebsocketHistory;

        let specification = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("specs")
            .join("rpc")
            .join(specification);
        let specification = std::fs::File::open(specification).unwrap();
        let specification = serde_json::from_reader::<_, serde_json::Value>(specification).unwrap();

        let mut methods = specification["methods"]
            .as_array()
            .unwrap()
            .iter()
            .map(|x| x["name"].as_str().unwrap())
            .collect::<Vec<_>>();

        for excluded in exclude {
            assert!(
                methods.contains(excluded),
                "Excluded method {excluded} was not found in the specification"
            );
        }

        methods.retain(|x| !exclude.contains(x));

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let mut context = RpcContext::for_tests();
        if api.has_websocket() {
            context = context.with_websockets(context::WebsocketContext::new(
                WebsocketHistory::Unlimited,
            ));
        }
        let (_jh, addr) = RpcServer::new(addr, context, RpcVersion::V07)
            .spawn(&PathBuf::default())
            .await
            .unwrap();

        let method_not_found = json!(-32601);
        let invalid_params = json!(-32602);

        if api.has_http() {
            let url = format!("http://{addr}{route}");
            let client = reqwest::Client::new();
            let mut failures: Vec<&&str> = Vec::new();

            for method in &methods {
                let request = json!({
                    "jsonrpc": "2.0",
                    "method": method,
                    "id": 0,
                    "params": {
                        "invalid_param": null,
                    }
                });

                let res: serde_json::Value = client
                    .post(url.clone())
                    .json(&request)
                    .send()
                    .await
                    .unwrap()
                    .json()
                    .await
                    .unwrap();

                if res["error"]["code"] == method_not_found {
                    failures.push(method);
                }
                assert_eq!(res["error"]["code"], invalid_params);
            }

            if !failures.is_empty() {
                panic!("{failures:#?} were not found");
            }

            // Check that excluded methods are indeed not present.
            failures.clear();
            for excluded in exclude {
                let request = json!({
                    "jsonrpc": "2.0",
                    "method": excluded,
                    "id": 0,
                    "params": {
                        "invalid_param": null,
                    }
                });

                let res: serde_json::Value = client
                    .post(url.clone())
                    .json(&request)
                    .send()
                    .await
                    .unwrap()
                    .json()
                    .await
                    .unwrap();

                if res["error"]["code"] != method_not_found {
                    failures.push(excluded);
                }
            }

            if !failures.is_empty() {
                panic!("{failures:#?} were marked as excluded but are actually present");
            }
        }

        if api.has_websocket() {
            use tokio_tungstenite::tungstenite::Message;
            use tokio_tungstenite::tungstenite::client::IntoClientRequest;
            use futures::{SinkExt, StreamExt};

            let request = format!("ws://{addr}{route}").into_client_request().unwrap();
            let (mut stream, _) = tokio_tungstenite::connect_async(request).await.unwrap();

            let mut failures: Vec<&&str> = Vec::new();
            for method in &methods {
                let request = json!({
                    "jsonrpc": "2.0",
                    "method": method,
                    "id": 0,
                    "params": {
                        "invalid_param": null,
                    }
                });

                stream.send(Message::Text(request.to_string().into())).await.unwrap();
                let res: Message = stream.next().await.unwrap().unwrap();
                let res: serde_json::Value = serde_json::from_str(&res.to_string()).unwrap();

                if res["error"]["code"] == method_not_found {
                    failures.push(method);
                }
                assert_eq!(res["error"]["code"], invalid_params);
            }

            if !failures.is_empty() {
                panic!("{failures:#?} were not found");
            }

            // Check that excluded methods are indeed not present.
            failures.clear();
            for excluded in exclude {
                let request = json!({
                    "jsonrpc": "2.0",
                    "method": excluded,
                    "id": 0,
                    "params": {
                        "invalid_param": null,
                    }
                });

                stream.send(Message::Text(request.to_string().into())).await.unwrap();
                let res = stream.next().await.unwrap().unwrap();
                let res: serde_json::Value = serde_json::from_str(&res.to_string()).unwrap();

                if res["error"]["code"] != method_not_found {
                    failures.push(excluded);
                }
            }

            if !failures.is_empty() {
                panic!("{failures:#?} were marked as excluded but are actually present");
            }
        }


    }
}
