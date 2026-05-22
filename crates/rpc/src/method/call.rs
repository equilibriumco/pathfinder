use anyhow::Context;
use pathfinder_common::{CallParam, CallResultValue, ContractAddress, EntryPoint};
use pathfinder_executor::{ExecutionState, L1BlobDataAvailability};

use crate::context::RpcContext;
use crate::error::ApplicationError;
use crate::executor::CALLDATA_LIMIT;
use crate::types::BlockId;
use crate::RpcVersion;

#[derive(Debug)]
pub enum CallError {
    Internal(anyhow::Error),
    Custom(anyhow::Error),
    BlockNotFound,
    ContractNotFound,
    EntrypointNotFound,
    ContractError {
        revert_error: Option<String>,
        revert_error_stack: pathfinder_executor::ErrorStack,
    },
}

impl From<anyhow::Error> for CallError {
    fn from(e: anyhow::Error) -> Self {
        Self::Internal(e)
    }
}

impl From<pathfinder_executor::CallError> for CallError {
    fn from(value: pathfinder_executor::CallError) -> Self {
        use pathfinder_executor::CallError::*;
        match value {
            ContractNotFound => Self::ContractNotFound,
            InvalidMessageSelector => Self::EntrypointNotFound,
            ContractError(error, error_stack) => Self::ContractError {
                revert_error: Some(format!("Execution error: {error}")),
                revert_error_stack: error_stack,
            },
            Internal(e) => Self::Internal(e),
            Custom(e) => Self::Custom(e),
        }
    }
}

impl From<crate::executor::ExecutionStateError> for CallError {
    fn from(error: crate::executor::ExecutionStateError) -> Self {
        use crate::executor::ExecutionStateError::*;
        match error {
            BlockNotFound => Self::BlockNotFound,
            Internal(e) => Self::Internal(e),
        }
    }
}

impl From<CallError> for ApplicationError {
    fn from(value: CallError) -> Self {
        match value {
            CallError::BlockNotFound => ApplicationError::BlockNotFound,
            CallError::ContractNotFound => ApplicationError::ContractNotFound,
            CallError::EntrypointNotFound => ApplicationError::EntrypointNotFound,
            CallError::ContractError {
                revert_error,
                revert_error_stack,
            } => ApplicationError::ContractError {
                revert_error,
                revert_error_stack,
            },
            CallError::Internal(e) => ApplicationError::Internal(e),
            CallError::Custom(e) => ApplicationError::Custom(e),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Input {
    pub request: FunctionCall,
    pub block_id: BlockId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FunctionCall {
    pub contract_address: ContractAddress,
    pub entry_point_selector: EntryPoint,
    pub calldata: Vec<CallParam>,
}

impl crate::dto::DeserializeForVersion for FunctionCall {
    fn deserialize(value: crate::dto::Value) -> Result<Self, serde_json::Error> {
        value.deserialize_map(|value| {
            Ok(Self {
                contract_address: value.deserialize_serde("contract_address")?,
                entry_point_selector: value.deserialize_serde("entry_point_selector")?,
                calldata: value
                    .deserialize_array("calldata", crate::dto::Value::deserialize_serde)?,
            })
        })
    }
}

impl crate::dto::DeserializeForVersion for Input {
    fn deserialize(value: crate::dto::Value) -> Result<Self, serde_json::Error> {
        value.deserialize_map(|value| {
            Ok(Self {
                request: value.deserialize("request")?,
                block_id: value.deserialize("block_id")?,
            })
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Output(pub Vec<CallResultValue>);

pub async fn call(
    context: RpcContext,
    input: Input,
    rpc_version: RpcVersion,
) -> Result<Output, CallError> {
    let span = tracing::Span::current();
    if input.request.calldata.len() > CALLDATA_LIMIT {
        return Err(CallError::Custom(anyhow::anyhow!(
            "Calldata limit ({CALLDATA_LIMIT}) exceeded"
        )));
    }
    let result = util::task::spawn_blocking(move |_| {
        let _g = span.enter();

        let mut db_conn = context
            .storage
            .connection()
            .context("Creating database connection")?;
        let db_tx = db_conn
            .transaction()
            .context("Creating database transaction")?;

        let (header, pending) = match input.block_id {
            BlockId::PreConfirmed => {
                let pending = context
                    .pending_data
                    .get(&db_tx, rpc_version)
                    .context("Querying pending data")?;

                (
                    pending.pre_confirmed_header(),
                    Some(pending.aggregated_state_update()),
                )
            }
            other => {
                let block_id = other
                    .to_common_or_panic(&db_tx)
                    .map_err(|_| CallError::BlockNotFound)?;

                let header = db_tx
                    .block_header(block_id)
                    .context("Querying block header")?
                    .ok_or(CallError::BlockNotFound)?;

                (header, None)
            }
        };

        let state = ExecutionState::simulation(
            context.chain_id,
            header,
            pending,
            L1BlobDataAvailability::Disabled,
            context.config.versioned_constants_map,
            context.contract_addresses.eth_l2_token_address,
            context.contract_addresses.strk_l2_token_address,
            context.native_class_cache,
            context
                .config
                .native_execution_force_use_for_incompatible_classes,
        );

        let result = pathfinder_executor::call(
            db_tx,
            state,
            input.request.contract_address,
            input.request.entry_point_selector,
            input.request.calldata,
        )?;

        Ok(result)
    })
    .await
    .context("Executing call")?;

    result.map(Output)
}

impl crate::dto::SerializeForVersion for Output {
    fn serialize(
        &self,
        serializer: crate::dto::Serializer,
    ) -> Result<crate::dto::Ok, crate::dto::Error> {
        serializer.serialize_iter(self.0.len(), &mut self.0.iter())
    }
}

#[cfg(test)]
mod tests {
    use pathfinder_common::macro_prelude::*;

    use super::*;

    mod parsing {
        use serde_json::json;

        use super::*;
        use crate::dto::DeserializeForVersion;

        #[test]
        fn positional_args() {
            let positional_json = json!([
                { "contract_address": "0xabcde", "entry_point_selector": "0xee", "calldata": ["0x1234", "0x2345"] },
                { "block_hash": "0xbbbbbbbb" }
            ]);

            let positional = crate::dto::Value::new(positional_json, crate::RpcVersion::V08);

            let input = Input::deserialize(positional).unwrap();
            let expected = Input {
                request: FunctionCall {
                    contract_address: contract_address!("0xabcde"),
                    entry_point_selector: entry_point!("0xee"),
                    calldata: vec![call_param!("0x1234"), call_param!("0x2345")],
                },
                block_id: block_hash!("0xbbbbbbbb").into(),
            };
            assert_eq!(input, expected);
        }

        #[test]
        fn named_args() {
            let named_json = json!({
                "request": { "contract_address": "0xabcde", "entry_point_selector": "0xee", "calldata": ["0x1234", "0x2345"] },
                "block_id": { "block_hash": "0xbbbbbbbb" }
            });

            let named = crate::dto::Value::new(named_json, crate::RpcVersion::V08);

            let input = Input::deserialize(named).unwrap();
            let expected = Input {
                request: FunctionCall {
                    contract_address: contract_address!("0xabcde"),
                    entry_point_selector: entry_point!("0xee"),
                    calldata: vec![call_param!("0x1234"), call_param!("0x2345")],
                },
                block_id: block_hash!("0xbbbbbbbb").into(),
            };
            assert_eq!(input, expected);
        }
    }

    mod in_memory {
        use std::sync::Arc;

        use assert_matches::assert_matches;
        use pathfinder_common::class_definition::{
            SerializedCairoDefinition,
            SerializedCasmDefinition,
            SerializedSierraDefinition,
        };
        use pathfinder_common::felt;
        use pathfinder_common::prelude::*;
        use pathfinder_pre_confirmed::PreConfirmedCache;
        use starknet_gateway_test_fixtures::class_definitions::{
            CONTRACT_DEFINITION,
            CONTRACT_DEFINITION_CLASS_HASH,
        };
        use starknet_gateway_types::reply::{GasPrices, L1DataAvailabilityMode};

        use super::*;
        use crate::pending::PendingData;

        const RPC_VERSION: RpcVersion = RpcVersion::V09;

        async fn test_context() -> (
            RpcContext,
            BlockHeader,
            ContractAddress,
            StorageAddress,
            StorageValue,
        ) {
            let storage = pathfinder_storage::StorageBuilder::in_memory().unwrap();
            let mut db = storage.connection().unwrap();
            let tx = db.transaction().unwrap();

            // Empty genesis block
            let header = BlockHeader::builder()
                .number(BlockNumber::GENESIS)
                .timestamp(BlockTimestamp::new_or_panic(0))
                .starknet_version(StarknetVersion::V_0_13_4)
                .finalize_with_hash(BlockHash(felt!("0xb00")));
            tx.insert_block_header(&header).unwrap();

            // Declare & deploy a test class providing an entry point reading from storage
            let block1_number = BlockNumber::GENESIS + 1;
            let block1_hash = BlockHash(felt!("0xb01"));

            tx.insert_cairo_class_definition(
                CONTRACT_DEFINITION_CLASS_HASH,
                &SerializedCairoDefinition::from_slice(CONTRACT_DEFINITION),
            )
            .unwrap();

            let header = BlockHeader::builder()
                .number(block1_number)
                .timestamp(BlockTimestamp::new_or_panic(1))
                .eth_l1_gas_price(GasPrice(1))
                .starknet_version(StarknetVersion::V_0_13_4)
                .finalize_with_hash(block1_hash);
            tx.insert_block_header(&header).unwrap();

            let test_contract_address = ContractAddress::new_or_panic(felt!("0xc01"));
            let test_contract_key = StorageAddress::new_or_panic(felt!("0x123"));
            let test_contract_value = StorageValue(felt!("0x3"));

            let state_update = StateUpdate::default()
                .with_block_hash(block1_hash)
                .with_declared_cairo_class(CONTRACT_DEFINITION_CLASS_HASH)
                .with_deployed_contract(test_contract_address, CONTRACT_DEFINITION_CLASS_HASH)
                .with_storage_update(
                    test_contract_address,
                    test_contract_key,
                    test_contract_value,
                );
            tx.insert_state_update(block1_number, &state_update)
                .unwrap();

            tx.commit().unwrap();
            drop(db);

            let context =
                RpcContext::for_tests_on(pathfinder_common::Chain::Mainnet).with_storage(storage);

            (
                context,
                header,
                test_contract_address,
                test_contract_key,
                test_contract_value,
            )
        }

        #[tokio::test]
        async fn storage_access() {
            let (context, _last_block_header, contract_address, test_key, test_value) =
                test_context().await;

            let input = Input {
                request: FunctionCall {
                    contract_address,
                    entry_point_selector: EntryPoint::hashed(b"get_value"),
                    calldata: vec![CallParam(*test_key.get())],
                },
                block_id: BlockId::Latest,
            };
            let result = call(context, input, RPC_VERSION).await.unwrap();
            assert_eq!(result, Output(vec![CallResultValue(test_value.0)]));
        }

        #[tokio::test]
        async fn storage_updated_in_pre_confirmed() {
            let (context, last_block_header, contract_address, test_key, test_value) =
                test_context().await;

            let new_value = StorageValue(felt!("0x09"));
            let pending_data = pre_confirmed_data_with_update(
                last_block_header,
                StateUpdate::default().with_storage_update(contract_address, test_key, new_value),
            );

            let pre_confirmed_cache = Arc::new(PreConfirmedCache::new());
            pre_confirmed_cache.store(pending_data);
            let context = context.with_pre_confirmed_cache(pre_confirmed_cache);

            // unchanged on latest block
            let input = Input {
                request: FunctionCall {
                    contract_address,
                    entry_point_selector: EntryPoint::hashed(b"get_value"),
                    calldata: vec![CallParam(*test_key.get())],
                },
                block_id: BlockId::Latest,
            };
            let result = call(context.clone(), input, RPC_VERSION).await.unwrap();
            assert_eq!(result, Output(vec![CallResultValue(test_value.0)]));

            // updated on pending
            let input = Input {
                request: FunctionCall {
                    contract_address,
                    entry_point_selector: EntryPoint::hashed(b"get_value"),
                    calldata: vec![CallParam(*test_key.get())],
                },
                block_id: BlockId::PreConfirmed,
            };
            let result = call(context, input, RPC_VERSION).await.unwrap();
            assert_eq!(result, Output(vec![CallResultValue(new_value.0)]));
        }

        #[tokio::test]
        async fn contract_deployed_in_pre_confirmed() {
            let (context, last_block_header, _contract_address, test_key, _test_value) =
                test_context().await;

            let new_value = storage_value!("0x09");
            let new_contract_address = contract_address!("0xdeadbeef");
            let pending_data = pre_confirmed_data_with_update(
                last_block_header,
                StateUpdate::default()
                    .with_deployed_contract(new_contract_address, CONTRACT_DEFINITION_CLASS_HASH)
                    .with_storage_update(new_contract_address, test_key, new_value),
            );
            let pre_confirmed_cache = Arc::new(PreConfirmedCache::new());
            pre_confirmed_cache.store(pending_data);
            let context = context.with_pre_confirmed_cache(pre_confirmed_cache);

            let input = Input {
                request: FunctionCall {
                    contract_address: new_contract_address,
                    entry_point_selector: EntryPoint::hashed(b"get_value"),
                    calldata: vec![CallParam(*test_key.get())],
                },
                block_id: BlockId::PreConfirmed,
            };
            let result = call(context.clone(), input, RPC_VERSION).await.unwrap();
            assert_eq!(result, Output(vec![CallResultValue(new_value.0)]));
        }

        #[test_log::test(tokio::test)]
        async fn contract_declared_and_deployed_in_pre_confirmed() {
            let (context, last_block_header, _contract_address, _test_key, _test_value) =
                test_context().await;

            let sierra_definition = include_bytes!("../../fixtures/contracts/storage_access.json");
            let sierra_hash =
                sierra_hash!("0x0544b92d358447cb9e50b65092b7169f931d29e05c1404a2cd08c6fd7e32ba90");
            let casm_definition = include_bytes!("../../fixtures/contracts/storage_access.casm");
            let casm_hash =
                casm_hash!("0x069032ff71f77284e1a0864a573007108ca5cc08089416af50f03260f5d6d4d8");
            let casm_hash_v2 = casm_hash_bytes!(b"casm hash blake");

            let mut connection = context.storage.connection().unwrap();
            let tx = connection.transaction().unwrap();
            tx.insert_sierra_class_definition(
                &sierra_hash,
                &SerializedSierraDefinition::from_slice(sierra_definition),
                &SerializedCasmDefinition::from_slice(casm_definition),
                &casm_hash_v2,
            )
            .unwrap();
            tx.commit().unwrap();

            drop(connection);

            let storage_key = StorageAddress::from_name(b"my_storage_var");
            let storage_value = storage_value!("0x09");
            let new_contract_address = contract_address!("0xdeadbeef");
            let pending_data = pre_confirmed_data_with_update(
                last_block_header,
                StateUpdate::default()
                    .with_declared_sierra_class(sierra_hash, casm_hash)
                    .with_deployed_contract(new_contract_address, ClassHash(sierra_hash.0))
                    .with_storage_update(new_contract_address, storage_key, storage_value),
            );
            let pre_confirmed_cache = Arc::new(PreConfirmedCache::new());
            pre_confirmed_cache.store(pending_data);
            let context = context.with_pre_confirmed_cache(pre_confirmed_cache);

            let input = Input {
                request: FunctionCall {
                    contract_address: new_contract_address,
                    entry_point_selector: EntryPoint::hashed(b"get_data"),
                    calldata: vec![],
                },
                block_id: BlockId::PreConfirmed,
            };
            let result = call(context.clone(), input.clone(), RpcVersion::V09)
                .await
                .unwrap();
            assert_eq!(result, Output(vec![CallResultValue(storage_value.0)]));

            // We expect that JSON-RPC versions older than 0.9 do _not_ use the
            // pre-confirmed block.
            let error = call(context.clone(), input.clone(), RpcVersion::V08)
                .await
                .unwrap_err();
            assert_matches!(error, CallError::ContractNotFound);
        }

        fn pre_confirmed_data_with_update(
            last_block_header: BlockHeader,
            state_update: StateUpdate,
        ) -> PendingData {
            // Aggregated state update is the same as state update for pre-confirmed blocks
            // as there's no pre-latest block.
            let aggregated_state_update = state_update.clone();

            PendingData::from_parts(
                crate::pending::PendingBlocks {
                    pre_confirmed: crate::pending::PreConfirmedBlock {
                        number: last_block_header.number + 1,
                        l1_gas_price: GasPrices {
                            price_in_wei: last_block_header.eth_l1_gas_price,
                            price_in_fri: Default::default(),
                        },
                        l2_gas_price: GasPrices {
                            price_in_wei: last_block_header.eth_l2_gas_price,
                            price_in_fri: last_block_header.strk_l2_gas_price,
                        },
                        l1_data_gas_price: Default::default(),
                        sequencer_address: last_block_header.sequencer_address,
                        status: starknet_gateway_types::reply::Status::PreConfirmed,
                        timestamp: BlockTimestamp::new_or_panic(
                            last_block_header.timestamp.get() + 1,
                        ),
                        transaction_receipts: vec![],
                        transactions: vec![],
                        starknet_version: last_block_header.starknet_version,
                        l1_da_mode: L1DataAvailabilityMode::Blob.into(),
                    },
                    pre_latest: None,
                    candidate_transactions: vec![],
                },
                state_update,
                aggregated_state_update,
                last_block_header.number + 1,
            )
        }

        #[tokio::test]
        async fn call_sierra_class() {
            let (context, last_block_header, _contract_address, _test_key, _test_value) =
                test_context().await;

            let sierra_definition = include_bytes!("../../fixtures/contracts/storage_access.json");
            let sierra_hash =
                sierra_hash!("0x03f6241e01a5afcb81f181518d74a1d3c8fc49c2aa583f805b67732e494ba9a8");
            let casm_definition = include_bytes!("../../fixtures/contracts/storage_access.casm");
            let casm_hash =
                casm_hash!("0x069032ff71f77284e1a0864a573007108ca5cc08089416af50f03260f5d6d4d8");
            let casm_hash_v2 = casm_hash_bytes!(b"casm hash blake");

            let block_number = BlockNumber::new_or_panic(last_block_header.number.get() + 1);
            let contract_address = contract_address!("0xcaaaa");
            let storage_key = StorageAddress::from_name(b"my_storage_var");
            let storage_value = storage_value!("0xb");

            let mut connection = context.storage.connection().unwrap();
            let tx = connection.transaction().unwrap();

            tx.insert_sierra_class_definition(
                &sierra_hash,
                &SerializedSierraDefinition::from_slice(sierra_definition),
                &SerializedCasmDefinition::from_slice(casm_definition),
                &casm_hash_v2,
            )
            .unwrap();

            let header = BlockHeader::builder()
                .number(block_number)
                .starknet_version(StarknetVersion::V_0_13_4)
                .finalize_with_hash(block_hash!("0xb02"));
            tx.insert_block_header(&header).unwrap();

            let state_update = StateUpdate::default()
                .with_declared_sierra_class(sierra_hash, casm_hash)
                .with_deployed_contract(contract_address, ClassHash(*sierra_hash.get()))
                .with_storage_update(contract_address, storage_key, storage_value);
            tx.insert_state_update(block_number, &state_update).unwrap();

            tx.commit().unwrap();
            drop(connection);

            let input = Input {
                request: FunctionCall {
                    contract_address,
                    entry_point_selector: EntryPoint::hashed(b"get_data"),
                    calldata: vec![],
                },
                block_id: BlockId::Latest,
            };
            let result = call(context, input, RPC_VERSION).await.unwrap();
            assert_eq!(result, Output(vec![CallResultValue(storage_value.0)]));
        }

        #[tokio::test]
        async fn call_sierra_1_7_class_with_invalid_entry_point() {
            let (
                storage,
                _last_block_header,
                account_contract_address,
                _universal_deployer_address,
                _test_storage_value,
            ) = crate::method::simulate_transactions::tests::setup_storage_with_starknet_version(
                StarknetVersion::new(0, 13, 4, 0),
            )
            .await;
            let context = RpcContext::for_tests().with_storage(storage);

            // Our test account class is Sierra 1.7, so the easiest is just to call that.
            let input = Input {
                request: FunctionCall {
                    contract_address: account_contract_address,
                    entry_point_selector: EntryPoint::hashed(b"bogus_entry_point"),
                    calldata: vec![],
                },
                block_id: BlockId::Latest,
            };

            let error = call(context, input, RPC_VERSION).await;
            assert_matches::assert_matches!(error, Err(CallError::EntrypointNotFound));
        }

        #[tokio::test]
        async fn call_sierra_1_7_class_validate_invalid_params() {
            let (
                storage,
                _last_block_header,
                account_contract_address,
                _universal_deployer_address,
                _test_storage_value,
            ) = crate::method::simulate_transactions::tests::setup_storage_with_starknet_version(
                StarknetVersion::new(0, 13, 4, 0),
            )
            .await;
            let context = RpcContext::for_tests().with_storage(storage);

            // Our test account class is Sierra 1.7, so the easiest is just to call that.
            let validate_entry_point = EntryPoint::hashed(b"__validate__");
            let input = Input {
                request: FunctionCall {
                    contract_address: account_contract_address,
                    entry_point_selector: validate_entry_point,
                    calldata: vec![],
                },
                block_id: BlockId::Latest,
            };

            let error = call(context, input, RPC_VERSION).await;
            assert_matches::assert_matches!(error, Err(CallError::ContractError { revert_error, revert_error_stack }) => {
                assert_eq!(revert_error, Some("Execution error: Execution failed. Failure reason:\nError in contract (contract address: 0x0000000000000000000000000000000000000000000000000000000000000c01, class hash: 0x019cabebe31b9fb6bf5e7ce9a971bd7d06e9999e0b97eee943869141a46fd978, selector: 0x0162da33a4585851fe8d3af3c2a9c60b557814e221e0d4f30ff0b2189d9c7775):\n0x4661696c656420746f20646573657269616c697a6520706172616d202331 ('Failed to deserialize param #1').\n".to_owned()));
                assert_eq!(revert_error_stack.0.len(), 2);
                assert_matches::assert_matches!(&revert_error_stack.0[0], pathfinder_executor::Frame::CallFrame(pathfinder_executor::CallFrame {
                    storage_address,
                    class_hash,
                    selector,
                }) => {
                    assert_eq!(storage_address, &account_contract_address);
                    assert_eq!(class_hash, &crate::test_setup::OPENZEPPELIN_ACCOUNT_CLASS_HASH);
                    assert_matches::assert_matches!(selector, Some(entry_point) => assert_eq!(entry_point, &validate_entry_point));
                });
                assert_matches::assert_matches!(&revert_error_stack.0[1], pathfinder_executor::Frame::StringFrame(error_string) => {
                    assert_eq!(error_string, "0x4661696c656420746f20646573657269616c697a6520706172616d202331 ('Failed to deserialize param #1')");
                });
            });
        }

        #[tokio::test]
        async fn invalid_params_when_called_via_syscall() {
            let (
                storage,
                last_block_header,
                _account_contract_address,
                _universal_deployer_address,
                _test_storage_value,
            ) = crate::method::simulate_transactions::tests::setup_storage_with_starknet_version(
                StarknetVersion::new(0, 13, 4, 0),
            )
            .await;
            let context = RpcContext::for_tests().with_storage(storage);

            let sierra_definition = include_bytes!("../../fixtures/contracts/storage_access.json");
            let sierra_hash =
                sierra_hash!("0x03f6241e01a5afcb81f181518d74a1d3c8fc49c2aa583f805b67732e494ba9a8");
            let casm_definition = include_bytes!("../../fixtures/contracts/storage_access.casm");
            let casm_hash =
                casm_hash!("0x069032ff71f77284e1a0864a573007108ca5cc08089416af50f03260f5d6d4d8");
            let casm_hash_v2 = casm_hash_bytes!(b"casm hash blake");

            let block_number = BlockNumber::new_or_panic(last_block_header.number.get() + 1);
            let contract_address = contract_address!("0xcaaaa");
            let caller_contract_address = contract_address!("0xccccc");
            let storage_key = StorageAddress::from_name(b"my_storage_var");
            let storage_value = storage_value!("0xb");

            let caller_sierra_definition = include_bytes!(
                "../../fixtures/contracts/caller/target/dev/caller_Caller.contract_class.json"
            );
            let caller_sierra_json: serde_json::Value =
                serde_json::from_slice(caller_sierra_definition).unwrap();
            let caller_sierra_definition = serde_json::json!({
                "contract_class_version": caller_sierra_json["contract_class_version"],
                "sierra_program": caller_sierra_json["sierra_program"],
                "entry_points_by_type": caller_sierra_json["entry_points_by_type"],
                "abi": serde_json::to_string(&caller_sierra_json["abi"]).unwrap(),
            });
            let caller_sierra_definition = serde_json::to_vec(&caller_sierra_definition).unwrap();
            let caller_sierra_hash =
                sierra_hash!("0x050d4827b118b6bef606c6e0ad4f33738b726e387de81b5ce045eb62d161bf9b");
            let caller_casm_definition = include_bytes!(
                "../../fixtures/contracts/caller/target/dev/caller_Caller.compiled_contract_class.\
                 json"
            );
            let caller_casm_hash =
                casm_hash!("0x02027e88d6cde8be7669d1baf9ac51f47fe52e600ced31cafba80eee1972a25b");
            let caller_casm_hash_v2 = casm_hash_bytes!(b"caller casm hash blake");

            let mut connection = context.storage.connection().unwrap();
            let tx = connection.transaction().unwrap();

            tx.insert_sierra_class_definition(
                &sierra_hash,
                &SerializedSierraDefinition::from_slice(sierra_definition),
                &SerializedCasmDefinition::from_slice(casm_definition),
                &casm_hash_v2,
            )
            .unwrap();
            tx.insert_sierra_class_definition(
                &caller_sierra_hash,
                &SerializedSierraDefinition::from_vec(caller_sierra_definition),
                &SerializedCasmDefinition::from_slice(caller_casm_definition),
                &caller_casm_hash_v2,
            )
            .unwrap();

            let header = BlockHeader::child_builder(&last_block_header)
                .number(block_number)
                .starknet_version(last_block_header.starknet_version)
                .finalize_with_hash(block_hash!("0xb02"));
            tx.insert_block_header(&header).unwrap();

            let state_update = StateUpdate::default()
                .with_declared_sierra_class(sierra_hash, casm_hash)
                .with_declared_sierra_class(caller_sierra_hash, caller_casm_hash)
                .with_deployed_contract(contract_address, ClassHash(*sierra_hash.get()))
                .with_deployed_contract(
                    caller_contract_address,
                    ClassHash(*caller_sierra_hash.get()),
                )
                .with_storage_update(contract_address, storage_key, storage_value);
            tx.insert_state_update(block_number, &state_update).unwrap();

            tx.commit().unwrap();
            drop(connection);

            // Our test account class is Sierra 1.7, so the easiest is just to call that.
            let caller_entry_point = EntryPoint::hashed(b"call");
            let input = Input {
                request: FunctionCall {
                    contract_address: caller_contract_address,
                    entry_point_selector: caller_entry_point,
                    calldata: vec![
                        // Number of calls
                        call_param!("0x1"),
                        // Called contract address
                        CallParam(*caller_contract_address.get()),
                        // Entry point selector for the called contract
                        CallParam(EntryPoint::hashed(b"call").0),
                        // Length of the call data for the called contract
                        call_param!("0x1"),
                        // Number of calls, but then no more data; leads to deserialization error
                        call_param!("0x1"),
                    ],
                },
                block_id: BlockId::Latest,
            };

            let error = call(context, input, RPC_VERSION).await;

            assert_matches::assert_matches!(error, Err(CallError::ContractError { revert_error, revert_error_stack }) => {
                assert_eq!(revert_error, Some("Execution error: Execution failed. Failure reason:\nError in contract (contract address: 0x00000000000000000000000000000000000000000000000000000000000ccccc, class hash: 0x050d4827b118b6bef606c6e0ad4f33738b726e387de81b5ce045eb62d161bf9b, selector: 0x031a75a0d711dfe3639aae96eb8f9facc2fd74df5aa611067f2511cc9fefc229):\nError in contract (contract address: 0x00000000000000000000000000000000000000000000000000000000000ccccc, class hash: 0x050d4827b118b6bef606c6e0ad4f33738b726e387de81b5ce045eb62d161bf9b, selector: 0x031a75a0d711dfe3639aae96eb8f9facc2fd74df5aa611067f2511cc9fefc229):\n0x4661696c656420746f20646573657269616c697a6520706172616d202331 ('Failed to deserialize param #1').\n".to_owned()));
                assert_eq!(revert_error_stack.0.len(), 3);
                assert_matches::assert_matches!(&revert_error_stack.0[0], pathfinder_executor::Frame::CallFrame(pathfinder_executor::CallFrame {
                    storage_address,
                    class_hash,
                    selector,
                }) => {
                    assert_eq!(storage_address, &caller_contract_address);
                    assert_eq!(class_hash, &ClassHash(caller_sierra_hash.0));
                    assert_matches::assert_matches!(selector, Some(entry_point) => assert_eq!(entry_point, &caller_entry_point));
                });
                assert_matches::assert_matches!(&revert_error_stack.0[1], pathfinder_executor::Frame::CallFrame(pathfinder_executor::CallFrame {
                    storage_address,
                    class_hash,
                    selector,
                }) => {
                    assert_eq!(storage_address, &caller_contract_address);
                    assert_eq!(class_hash, &ClassHash(caller_sierra_hash.0));
                    assert_matches::assert_matches!(selector, Some(entry_point) => assert_eq!(entry_point, &caller_entry_point));
                });
                assert_matches::assert_matches!(&revert_error_stack.0[2], pathfinder_executor::Frame::StringFrame(error_string) => {
                    assert_eq!(error_string, "0x4661696c656420746f20646573657269616c697a6520706172616d202331 ('Failed to deserialize param #1')");
                });
            });
        }

        #[test_log::test(tokio::test)]
        async fn calldata_limit_exceeded() {
            let (context, _, _, _, _) = test_context().await;

            let input = Input {
                request: FunctionCall {
                    // Calldata length over the limit, the rest of the fields should not matter.
                    calldata: vec![call_param!("0x123"); CALLDATA_LIMIT + 5],

                    contract_address: contract_address!("0xdeadbeef"),
                    entry_point_selector: entry_point!("0xdeadbeef"),
                },
                block_id: BlockId::Latest,
            };

            let err = call(context, input, RPC_VERSION).await.unwrap_err();

            let error_cause = "Calldata limit (10000) exceeded";
            assert_matches!(err, CallError::Custom(e) if e.root_cause().to_string() == error_cause);
        }
    }

    mod mainnet {
        use std::num::NonZeroU32;
        use std::path::PathBuf;

        use super::*;

        const RPC_VERSION: RpcVersion = RpcVersion::V09;

        // Mainnet block number 5
        const BLOCK_5: BlockId = BlockId::Hash(block_hash!(
            "0x00dcbd2a4b597d051073f40a0329e585bb94b26d73df69f8d72798924fd097d3"
        ));

        // Data from transaction
        // 0xc52079f33dcb44a58904fac3803fd908ac28d6632b67179ee06f2daccb4b5.
        fn valid_mainnet_call() -> FunctionCall {
            FunctionCall {
                contract_address: contract_address!(
                    "0x020cfa74ee3564b4cd5435cdace0f9c4d43b939620e4a0bb5076105df0a626c6"
                ),
                entry_point_selector: entry_point!(
                    "0x03d7905601c217734671143d457f0db37f7f8883112abd34b92c4abfeafde0c3"
                ),
                calldata: vec![
                    call_param!("0xe150b6c2db6ed644483b01685571de46d2045f267d437632b508c19f3eb877"),
                    call_param!(
                        "0x0494196e88ce16bff11180d59f3c75e4ba3475d9fba76249ab5f044bcd25add6"
                    ),
                ],
            }
        }

        async fn test_context() -> (tempfile::TempDir, RpcContext) {
            let mut source_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            source_path.push("fixtures/mainnet.sqlite");

            let db_dir = tempfile::TempDir::new().unwrap();
            let mut db_path = PathBuf::from(db_dir.path());
            db_path.push("mainnet.sqlite");

            std::fs::copy(&source_path, &db_path).unwrap();

            let storage = pathfinder_storage::StorageBuilder::file(db_path)
                .migrate()
                .unwrap()
                .create_pool(NonZeroU32::new(1).unwrap())
                .unwrap();

            let context =
                RpcContext::for_tests_on(pathfinder_common::Chain::Mainnet).with_storage(storage);

            (db_dir, context)
        }

        #[tokio::test]
        async fn no_such_block() {
            let (_temp_dir, context) = test_context().await;

            let input = Input {
                request: valid_mainnet_call(),
                block_id: BlockId::Hash(block_hash_bytes!(b"nonexistent")),
            };
            let error = call(context, input, RPC_VERSION).await;
            assert_matches::assert_matches!(error, Err(CallError::BlockNotFound));
        }

        #[tokio::test]
        async fn no_such_contract() {
            let (_temp_dir, context) = test_context().await;

            let input = Input {
                request: FunctionCall {
                    contract_address: contract_address!("0xdeadbeef"),
                    ..valid_mainnet_call()
                },
                block_id: BLOCK_5,
            };
            let error = call(context, input, RPC_VERSION).await;
            assert_matches::assert_matches!(error, Err(CallError::ContractNotFound));
        }

        #[tokio::test]
        async fn invalid_message_selector() {
            let (_temp_dir, context) = test_context().await;

            let input = Input {
                request: FunctionCall {
                    entry_point_selector: EntryPoint(Default::default()),
                    ..valid_mainnet_call()
                },
                block_id: BLOCK_5,
            };
            let error = call(context, input, RPC_VERSION).await;
            assert_matches::assert_matches!(error, Err(CallError::EntrypointNotFound));
        }

        #[tokio::test]
        async fn successful_call() {
            let (_temp_dir, context) = test_context().await;

            let input = Input {
                request: valid_mainnet_call(),
                block_id: BLOCK_5,
            };

            let result = call(context, input, RPC_VERSION).await.unwrap();
            assert_eq!(result.0, vec![]);
        }
    }
}
