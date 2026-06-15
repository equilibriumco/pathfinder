use anyhow::Context;
use pathfinder_executor::{ExecutionState, L1BlobDataAvailability};
use serde::de::Error;

use crate::context::RpcContext;
use crate::error::ApplicationError;
use crate::executor::{
    calldata_limit_exceeded,
    signature_elem_limit_exceeded,
    CALLDATA_LIMIT,
    SIGNATURE_ELEMENT_LIMIT,
};
use crate::types::request::BroadcastedTransaction;
use crate::types::BlockId;
use crate::RpcVersion;

#[derive(Debug, PartialEq, Eq)]
pub struct Input {
    pub request: Vec<BroadcastedTransaction>,
    pub simulation_flags: Vec<SimulationFlag>,
    pub block_id: BlockId,
}

impl crate::dto::DeserializeForVersion for Input {
    fn deserialize(value: crate::dto::Value) -> Result<Self, serde_json::Error> {
        value.deserialize_map(|value| {
            Ok(Self {
                request: value.deserialize_array("request", BroadcastedTransaction::deserialize)?,
                simulation_flags: value
                    .deserialize_array("simulation_flags", SimulationFlag::deserialize)?,
                block_id: value.deserialize("block_id")?,
            })
        })
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum SimulationFlag {
    SkipValidate,
}

impl crate::dto::DeserializeForVersion for SimulationFlag {
    fn deserialize(value: crate::dto::Value) -> Result<Self, serde_json::Error> {
        let value: String = value.deserialize()?;
        match value.as_str() {
            "SKIP_VALIDATE" => Ok(Self::SkipValidate),
            _ => Err(serde_json::Error::custom("Invalid flag")),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Output(Vec<pathfinder_executor::types::FeeEstimate>);

pub async fn estimate_fee(
    context: RpcContext,
    input: Input,
    rpc_version: RpcVersion,
) -> Result<Output, EstimateFeeError> {
    let span = tracing::Span::current();
    if let Some(bad_tx_idx) = input.request.iter().position(calldata_limit_exceeded) {
        return Err(EstimateFeeError::Custom(anyhow::anyhow!(
            "Calldata limit ({CALLDATA_LIMIT}) exceeded by transaction at index {bad_tx_idx}"
        )));
    }
    if let Some(bad_tx_idx) = input.request.iter().position(signature_elem_limit_exceeded) {
        return Err(EstimateFeeError::Custom(anyhow::anyhow!(
            "Signature element limit ({SIGNATURE_ELEMENT_LIMIT}) exceeded by transaction at index \
             {bad_tx_idx}"
        )));
    }
    let result = util::task::spawn_blocking(move |_| {
        let _g = span.enter();

        let skip_validate = input
            .simulation_flags
            .iter()
            .any(|flag| flag == &SimulationFlag::SkipValidate);

        let transactions = input
            .request
            .into_iter()
            .map(|tx| {
                crate::executor::map_broadcasted_transaction(
                    &tx,
                    context.chain_id,
                    &context.compiler,
                    skip_validate,
                    true,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;

        let mut db_conn = context
            .execution_storage
            .connection()
            .context("Creating database connection")?;
        let db_tx = db_conn
            .transaction()
            .context("Creating database transaction")?;

        let (header, pending) = match input.block_id {
            BlockId::PreConfirmed => {
                let pending = context.pending_data.get(&db_tx, rpc_version)?;

                (
                    pending.pre_confirmed_header(),
                    Some(pending.aggregated_state_update()),
                )
            }
            other => {
                let block_id = other
                    .to_common_or_panic(&db_tx)
                    .map_err(|_| EstimateFeeError::BlockNotFound)?;

                let header = db_tx
                    .block_header(block_id)
                    .context("Querying block header")?
                    .ok_or(EstimateFeeError::BlockNotFound)?;

                (header, None)
            }
        };

        let state = ExecutionState::simulation(
            context.chain_id,
            header,
            pending,
            L1BlobDataAvailability::Enabled,
            context.config.versioned_constants_map,
            context.contract_addresses.eth_l2_token_address,
            context.contract_addresses.strk_l2_token_address,
            context.native_class_cache,
            context
                .config
                .native_execution_force_use_for_incompatible_classes,
        );

        let result = pathfinder_executor::estimate(
            db_tx,
            state,
            transactions,
            context.config.fee_estimation_epsilon,
        )?;

        Ok::<_, EstimateFeeError>(result)
    })
    .await
    .context("Executing transaction")??;

    Ok(Output(result.into_iter().collect()))
}

#[derive(Debug)]
pub enum EstimateFeeError {
    Internal(anyhow::Error),
    Custom(anyhow::Error),
    BlockNotFound,
    TransactionExecutionError {
        transaction_index: usize,
        error: String,
        error_stack: pathfinder_executor::ErrorStack,
    },
}

impl From<anyhow::Error> for EstimateFeeError {
    fn from(e: anyhow::Error) -> Self {
        Self::Internal(e)
    }
}

crate::error::impl_from_pending_read_error!(EstimateFeeError);

impl From<pathfinder_executor::TransactionExecutionError> for EstimateFeeError {
    fn from(value: pathfinder_executor::TransactionExecutionError) -> Self {
        use pathfinder_executor::TransactionExecutionError::*;
        match value {
            ExecutionError {
                transaction_index,
                error,
                error_stack,
            } => Self::TransactionExecutionError {
                transaction_index,
                error,
                error_stack,
            },
            Internal(e) => Self::Internal(e),
            Custom(e) => Self::Custom(e),
        }
    }
}

impl From<crate::executor::ExecutionStateError> for EstimateFeeError {
    fn from(error: crate::executor::ExecutionStateError) -> Self {
        use crate::executor::ExecutionStateError::*;
        match error {
            BlockNotFound => Self::BlockNotFound,
            Internal(e) => Self::Internal(e),
        }
    }
}

impl From<EstimateFeeError> for ApplicationError {
    fn from(value: EstimateFeeError) -> Self {
        match value {
            EstimateFeeError::BlockNotFound => ApplicationError::BlockNotFound,
            EstimateFeeError::TransactionExecutionError {
                transaction_index,
                error,
                error_stack,
            } => ApplicationError::TransactionExecutionError {
                transaction_index,
                error,
                error_stack,
            },
            EstimateFeeError::Internal(e) => ApplicationError::Internal(e),
            EstimateFeeError::Custom(e) => ApplicationError::Custom(e),
        }
    }
}

impl crate::dto::SerializeForVersion for Output {
    fn serialize(
        &self,
        serializer: crate::dto::Serializer,
    ) -> Result<crate::dto::Ok, crate::dto::Error> {
        serializer.serialize_iter(self.0.len(), &mut self.0.iter().cloned())
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use pathfinder_common::class_definition::SerializedOpaqueClassDefinition;
    use pathfinder_common::macro_prelude::*;
    use pathfinder_common::prelude::*;
    use pathfinder_common::transaction::{DataAvailabilityMode, ResourceBound, ResourceBounds};
    use pathfinder_common::{felt, ResourceAmount, ResourcePricePerUnit, Tip};
    use pathfinder_executor::types::{FeeEstimate, PriceUnit};
    use pretty_assertions_sorted::assert_eq;

    use super::*;
    use crate::dto::{SerializeForVersion, Serializer};
    use crate::types::class::sierra::SierraContractClass;
    use crate::types::request::{
        BroadcastedDeclareTransaction,
        BroadcastedDeclareTransactionV2,
        BroadcastedDeclareTransactionV3,
        BroadcastedDeployAccountTransactionV3,
        BroadcastedInvokeTransaction,
        BroadcastedInvokeTransactionV0,
        BroadcastedInvokeTransactionV1,
        BroadcastedInvokeTransactionV3,
        BroadcastedTransaction,
    };
    use crate::types::{BlockId, ContractClass};
    use crate::RpcVersion;

    const RPC_VERSION: RpcVersion = RpcVersion::V09;

    fn declare_transaction(account_contract_address: ContractAddress) -> BroadcastedTransaction {
        let sierra_definition = include_bytes!("../../fixtures/contracts/storage_access.json");
        let sierra_hash =
            class_hash!("0x0544b92d358447cb9e50b65092b7169f931d29e05c1404a2cd08c6fd7e32ba90");
        let casm_hash =
            casm_hash!("0x069032ff71f77284e1a0864a573007108ca5cc08089416af50f03260f5d6d4d8");

        let contract_class: SierraContractClass = ContractClass::from_serialized_def(
            &SerializedOpaqueClassDefinition::from_slice(sierra_definition),
        )
        .unwrap()
        .as_sierra()
        .unwrap();

        assert_eq!(contract_class.class_hash().unwrap().hash(), sierra_hash);

        let max_fee = Fee::default();

        BroadcastedTransaction::Declare(BroadcastedDeclareTransaction::V2(
            BroadcastedDeclareTransactionV2 {
                version: TransactionVersion::TWO,
                max_fee,
                signature: vec![],
                nonce: TransactionNonce(Default::default()),
                contract_class,
                sender_address: account_contract_address,
                compiled_class_hash: casm_hash,
            },
        ))
    }

    fn deploy_transaction(
        account_contract_address: ContractAddress,
        universal_deployer_address: ContractAddress,
    ) -> BroadcastedTransaction {
        let max_fee = Fee::default();
        let sierra_hash =
            class_hash!("0x0544b92d358447cb9e50b65092b7169f931d29e05c1404a2cd08c6fd7e32ba90");

        BroadcastedTransaction::Invoke(BroadcastedInvokeTransaction::V1(
            BroadcastedInvokeTransactionV1 {
                nonce: transaction_nonce!("0x1"),
                version: TransactionVersion::ONE,
                max_fee,
                signature: vec![],
                sender_address: account_contract_address,
                calldata: vec![
                    // Number of calls
                    call_param!("0x1"),
                    CallParam(*universal_deployer_address.get()),
                    // Entry point selector for the called contract, i.e.
                    // AccountCallArray::selector
                    CallParam(EntryPoint::hashed(b"deployContract").0),
                    // Length of the call data for the called contract, i.e.
                    // AccountCallArray::data_len
                    call_param!("0x4"),
                    // classHash
                    CallParam(sierra_hash.0),
                    // salt
                    call_param!("0x0"),
                    // unique
                    call_param!("0x0"),
                    // calldata_len
                    call_param!("0x0"),
                ],
            },
        ))
    }

    fn invoke_transaction(account_contract_address: ContractAddress) -> BroadcastedTransaction {
        let max_fee = Fee::default();

        BroadcastedTransaction::Invoke(BroadcastedInvokeTransaction::V1(
            BroadcastedInvokeTransactionV1 {
                nonce: transaction_nonce!("0x2"),
                version: TransactionVersion::ONE,
                max_fee,
                signature: vec![],
                sender_address: account_contract_address,
                calldata: vec![
                    // Number of calls
                    call_param!("0x1"),
                    // address of the deployed test contract
                    CallParam(felt!(
                        "0x012592426632af714f43ccb05536b6044fc3e897fa55288f658731f93590e7e7"
                    )),
                    // Entry point selector for the called contract, i.e.
                    // AccountCallArray::selector
                    CallParam(EntryPoint::hashed(b"get_data").0),
                    // Length of the call data for the called contract, i.e.
                    // AccountCallArray::data_len
                    call_param!("0x0"),
                ],
            },
        ))
    }

    fn invoke_v0_transaction() -> BroadcastedTransaction {
        let max_fee = Fee::default();

        BroadcastedTransaction::Invoke(BroadcastedInvokeTransaction::V0(
            BroadcastedInvokeTransactionV0 {
                version: TransactionVersion::ONE,
                max_fee,
                signature: vec![],
                contract_address: contract_address!(
                    "0x012592426632af714f43ccb05536b6044fc3e897fa55288f658731f93590e7e7"
                ),
                entry_point_selector: EntryPoint::hashed(b"get_data"),
                calldata: vec![],
            },
        ))
    }

    fn invoke_v3_transaction(account_contract_address: ContractAddress) -> BroadcastedTransaction {
        BroadcastedTransaction::Invoke(BroadcastedInvokeTransaction::V3(
            BroadcastedInvokeTransactionV3 {
                version: TransactionVersion::THREE,
                signature: vec![],
                sender_address: account_contract_address,
                calldata: vec![
                    // Number of calls
                    call_param!("0x1"),
                    // address of the deployed test contract
                    CallParam(felt!(
                        "0x012592426632af714f43ccb05536b6044fc3e897fa55288f658731f93590e7e7"
                    )),
                    // Entry point selector for the called contract, i.e.
                    // AccountCallArray::selector
                    CallParam(EntryPoint::hashed(b"get_data").0),
                    // Length of the call data for the called contract, i.e.
                    // AccountCallArray::data_len
                    call_param!("0x0"),
                ],
                nonce: transaction_nonce!("0x3"),
                resource_bounds: ResourceBounds::default(),
                tip: Tip(0),
                paymaster_data: vec![],
                account_deployment_data: vec![],
                nonce_data_availability_mode: DataAvailabilityMode::L1,
                fee_data_availability_mode: DataAvailabilityMode::L1,
                proof_facts: vec![],
                proof: Default::default(),
            },
        ))
    }

    #[rstest::rstest]
    #[case::v09(RpcVersion::V09)]
    #[case::v10(RpcVersion::V10)]
    #[tokio::test]
    async fn declare_deploy_and_invoke_sierra_class_starknet_0_13_1(#[case] version: RpcVersion) {
        let (context, last_block_header, account_contract_address, universal_deployer_address) =
            crate::test_setup::test_context_with_starknet_version(StarknetVersion::new(
                0, 13, 1, 0,
            ))
            .await;

        // declare test class
        let declare_transaction = declare_transaction(account_contract_address);
        // deploy with universal deployer contract
        let deploy_transaction =
            deploy_transaction(account_contract_address, universal_deployer_address);
        // invoke deployed contract
        let invoke_transaction = invoke_transaction(account_contract_address);
        // do the same invoke with a v0 transaction
        let invoke_v0_transaction = invoke_v0_transaction();
        // do the same invoke with a v3 transaction
        let invoke_v3_transaction = invoke_v3_transaction(account_contract_address);

        let input = Input {
            request: vec![
                declare_transaction,
                deploy_transaction,
                invoke_transaction,
                invoke_v0_transaction,
                invoke_v3_transaction,
            ],
            simulation_flags: vec![],
            block_id: BlockId::Number(last_block_header.number),
        };
        let result = estimate_fee(context, input, RPC_VERSION).await.unwrap();

        let output_json = result.serialize(Serializer { version }).unwrap();
        crate::assert_json_matches_fixture!(
            output_json,
            version,
            "fee_estimates/declare_deploy_invoke_sierra_0_13_1.json"
        );
    }

    #[rstest::rstest]
    #[case::v09(RpcVersion::V09)]
    #[case::v10(RpcVersion::V10)]
    #[tokio::test]
    async fn declare_deploy_and_invoke_sierra_class_starknet_0_13_1_1(#[case] version: RpcVersion) {
        let (context, last_block_header, account_contract_address, universal_deployer_address) =
            crate::test_setup::test_context_with_starknet_version(StarknetVersion::new(
                0, 13, 1, 1,
            ))
            .await;

        // declare test class
        let declare_transaction = declare_transaction(account_contract_address);
        // deploy with universal deployer contract
        let deploy_transaction =
            deploy_transaction(account_contract_address, universal_deployer_address);
        // invoke deployed contract
        let invoke_transaction = invoke_transaction(account_contract_address);
        // do the same invoke with a v0 transaction
        let invoke_v0_transaction = invoke_v0_transaction();
        // do the same invoke with a v3 transaction
        let invoke_v3_transaction = invoke_v3_transaction(account_contract_address);

        let input = Input {
            request: vec![
                declare_transaction,
                deploy_transaction,
                invoke_transaction,
                invoke_v0_transaction,
                invoke_v3_transaction,
            ],
            simulation_flags: vec![],
            block_id: BlockId::Number(last_block_header.number),
        };
        let result = estimate_fee(context, input, RPC_VERSION).await.unwrap();

        let output_json = result.serialize(Serializer { version }).unwrap();
        crate::assert_json_matches_fixture!(
            output_json,
            version,
            "fee_estimates/declare_deploy_invoke_sierra_0_13_1_1.json"
        );
    }

    #[rstest::rstest]
    #[case::v09(RpcVersion::V09)]
    #[case::v10(RpcVersion::V10)]
    #[tokio::test]
    async fn declare_deploy_and_invoke_sierra_class_starknet_0_13_2(#[case] version: RpcVersion) {
        let (context, last_block_header, account_contract_address, universal_deployer_address) =
            crate::test_setup::test_context_with_starknet_version(StarknetVersion::new(
                0, 13, 2, 0,
            ))
            .await;

        // declare test class
        let declare_transaction = declare_transaction(account_contract_address);
        // deploy with universal deployer contract
        let deploy_transaction =
            deploy_transaction(account_contract_address, universal_deployer_address);
        // invoke deployed contract
        let invoke_transaction = invoke_transaction(account_contract_address);
        // do the same invoke with a v0 transaction
        let invoke_v0_transaction = invoke_v0_transaction();
        // do the same invoke with a v3 transaction
        let invoke_v3_transaction = invoke_v3_transaction(account_contract_address);

        let input = Input {
            request: vec![
                declare_transaction,
                deploy_transaction,
                invoke_transaction,
                invoke_v0_transaction,
                invoke_v3_transaction,
            ],
            simulation_flags: vec![],
            block_id: BlockId::Number(last_block_header.number),
        };
        let result = super::estimate_fee(context, input, RPC_VERSION)
            .await
            .unwrap();

        let output_json = result.serialize(Serializer { version }).unwrap();
        crate::assert_json_matches_fixture!(
            output_json,
            version,
            "fee_estimates/declare_deploy_invoke_sierra_0_13_2.json"
        );
    }

    #[rstest::rstest]
    #[case::v09(RpcVersion::V09)]
    #[case::v10(RpcVersion::V10)]
    #[tokio::test]
    async fn declare_deploy_and_invoke_sierra_class_starknet_0_13_2_1(#[case] version: RpcVersion) {
        let (context, last_block_header, account_contract_address, universal_deployer_address) =
            crate::test_setup::test_context_with_starknet_version(StarknetVersion::new(
                0, 13, 2, 1,
            ))
            .await;

        // declare test class
        let declare_transaction = declare_transaction(account_contract_address);
        // deploy with universal deployer contract
        let deploy_transaction =
            deploy_transaction(account_contract_address, universal_deployer_address);
        // invoke deployed contract
        let invoke_transaction = invoke_transaction(account_contract_address);
        // do the same invoke with a v0 transaction
        let invoke_v0_transaction = invoke_v0_transaction();
        // do the same invoke with a v3 transaction
        let invoke_v3_transaction = invoke_v3_transaction(account_contract_address);

        let input = Input {
            request: vec![
                declare_transaction,
                deploy_transaction,
                invoke_transaction,
                invoke_v0_transaction,
                invoke_v3_transaction,
            ],
            simulation_flags: vec![],
            block_id: BlockId::Number(last_block_header.number),
        };
        let result = super::estimate_fee(context, input, RPC_VERSION)
            .await
            .unwrap();

        let output_json = result.serialize(Serializer { version }).unwrap();
        crate::assert_json_matches_fixture!(
            output_json,
            version,
            "fee_estimates/declare_deploy_invoke_sierra_0_13_2_1.json"
        );
    }

    fn declare_v3_transaction(sender_address: ContractAddress) -> BroadcastedTransaction {
        let sierra_definition =
            include_bytes!("../../fixtures/contracts/l2_gas_accounting/l2_gas_accounting.json");
        let sierra_hash =
            class_hash!("0x01A48FD3F75D0A7C2288AC23FB6ABA26CD375607BA63E4A3B3ED47FC8E99DC21");
        let casm_hash =
            casm_hash!("0x02F58B23F7D98FF076AE59C08125AAFFD6DECCF1A7E97378D1A303B1A4223989");

        let contract_class: SierraContractClass = ContractClass::from_serialized_def(
            &SerializedOpaqueClassDefinition::from_slice(sierra_definition),
        )
        .unwrap()
        .as_sierra()
        .unwrap();

        self::assert_eq!(contract_class.class_hash().unwrap().hash(), sierra_hash);

        BroadcastedTransaction::Declare(BroadcastedDeclareTransaction::V3(
            BroadcastedDeclareTransactionV3 {
                version: TransactionVersion::THREE,
                signature: vec![],
                nonce: transaction_nonce!("0x0"),
                resource_bounds: ResourceBounds::default(),
                tip: Tip(0),
                paymaster_data: vec![],
                account_deployment_data: vec![],
                nonce_data_availability_mode: DataAvailabilityMode::L1,
                fee_data_availability_mode: DataAvailabilityMode::L1,
                compiled_class_hash: casm_hash,
                contract_class,
                sender_address,
            },
        ))
    }

    fn declare_v3_transaction_with_blake2_casm_hash(
        sender_address: ContractAddress,
    ) -> BroadcastedTransaction {
        let sierra_definition =
            include_bytes!("../../fixtures/contracts/l2_gas_accounting/l2_gas_accounting.json");
        let sierra_hash =
            class_hash!("0x01A48FD3F75D0A7C2288AC23FB6ABA26CD375607BA63E4A3B3ED47FC8E99DC21");
        let casm_hash =
            casm_hash!("0x138cd11c6de707426665bd8b0425d7411bb8dc5cbee15867025007a933b3379");

        let contract_class: SierraContractClass = ContractClass::from_serialized_def(
            &SerializedOpaqueClassDefinition::from_slice(sierra_definition),
        )
        .unwrap()
        .as_sierra()
        .unwrap();

        self::assert_eq!(contract_class.class_hash().unwrap().hash(), sierra_hash);

        BroadcastedTransaction::Declare(BroadcastedDeclareTransaction::V3(
            BroadcastedDeclareTransactionV3 {
                version: TransactionVersion::THREE,
                signature: vec![],
                nonce: transaction_nonce!("0x0"),
                resource_bounds: ResourceBounds::default(),
                tip: Tip(0),
                paymaster_data: vec![],
                account_deployment_data: vec![],
                nonce_data_availability_mode: DataAvailabilityMode::L1,
                fee_data_availability_mode: DataAvailabilityMode::L1,
                compiled_class_hash: casm_hash,
                contract_class,
                sender_address,
            },
        ))
    }

    fn deploy_v3_transaction(
        account_contract_address: ContractAddress,
        universal_deployer_address: ContractAddress,
    ) -> BroadcastedTransaction {
        let sierra_hash =
            class_hash!("0x01A48FD3F75D0A7C2288AC23FB6ABA26CD375607BA63E4A3B3ED47FC8E99DC21");

        BroadcastedTransaction::Invoke(BroadcastedInvokeTransaction::V3(
            BroadcastedInvokeTransactionV3 {
                version: TransactionVersion::THREE,
                signature: vec![],
                nonce: transaction_nonce!("0x1"),
                resource_bounds: ResourceBounds::default(),
                tip: Tip(0),
                paymaster_data: vec![],
                account_deployment_data: vec![],
                nonce_data_availability_mode: DataAvailabilityMode::L1,
                fee_data_availability_mode: DataAvailabilityMode::L1,
                sender_address: account_contract_address,
                calldata: vec![
                    // Number of calls
                    call_param!("0x1"),
                    CallParam(*universal_deployer_address.get()),
                    // Entry point selector for the called contract, i.e.
                    // AccountCallArray::selector
                    CallParam(EntryPoint::hashed(b"deployContract").0),
                    // Length of the call data for the called contract, i.e.
                    // AccountCallArray::data_len
                    call_param!("0x4"),
                    // classHash
                    CallParam(sierra_hash.0),
                    // salt
                    call_param!("0x0"),
                    // unique
                    call_param!("0x0"),
                    // calldata_len
                    call_param!("0x0"),
                ],
                proof_facts: vec![],
                proof: Default::default(),
            },
        ))
    }

    /// Invokes a contract that calls a recursive function. Recursion depth can
    /// be set with the `depth` parameter.
    fn invoke_v3_transaction_with_data_gas(
        sender_address: ContractAddress,
        nonce: TransactionNonce,
        depth: CallParam,
    ) -> BroadcastedTransaction {
        BroadcastedTransaction::Invoke(BroadcastedInvokeTransaction::V3(
            BroadcastedInvokeTransactionV3 {
                version: TransactionVersion::THREE,
                signature: vec![],
                sender_address,
                calldata: vec![
                    // Number of calls
                    call_param!("0x1"),
                    // Address of the deployed test contract
                    CallParam(felt!(
                        "0x17c54b787c2eccfb057cf6aa2f941d612249549fff74140adc20bb949eab74b"
                    )),
                    // Entry point selector for the called contract, i.e.
                    CallParam(EntryPoint::hashed(b"test_redeposits").0),
                    // Length of the call data for the called contract, i.e.
                    call_param!("0x1"),
                    depth,
                ],
                nonce,
                resource_bounds: ResourceBounds {
                    l1_gas: ResourceBound {
                        max_amount: ResourceAmount(50),
                        max_price_per_unit: ResourcePricePerUnit(1000),
                    },
                    l1_data_gas: Some(ResourceBound {
                        max_amount: ResourceAmount(100),
                        max_price_per_unit: ResourcePricePerUnit(1000),
                    }),
                    l2_gas: ResourceBound {
                        max_amount: ResourceAmount(800_000),
                        max_price_per_unit: ResourcePricePerUnit(1000),
                    },
                },
                tip: Tip(0),
                paymaster_data: vec![],
                account_deployment_data: vec![],
                nonce_data_availability_mode: DataAvailabilityMode::L2,
                fee_data_availability_mode: DataAvailabilityMode::L2,
                proof_facts: vec![],
                proof: Default::default(),
            },
        ))
    }

    #[rstest::rstest]
    #[case::v09(RpcVersion::V09)]
    #[case::v10(RpcVersion::V10)]
    #[tokio::test]
    async fn declare_deploy_and_invoke_sierra_class_starknet_0_13_4(#[case] version: RpcVersion) {
        let (context, last_block_header, account_contract_address, universal_deployer_address) =
            crate::test_setup::test_context_with_starknet_version(StarknetVersion::new(
                0, 13, 4, 0,
            ))
            .await;

        // declare test class
        let declare_transaction = declare_v3_transaction(account_contract_address);
        // deploy with universal deployer contract
        let deploy_transaction =
            deploy_v3_transaction(account_contract_address, universal_deployer_address);
        // invoke deployed contract
        let invoke_transaction = invoke_v3_transaction_with_data_gas(
            account_contract_address,
            transaction_nonce!("0x2"),
            call_param!("0x7"),
        );
        // Invoke once more to test that the execution state updates properly with L2
        // gas accounting aware code.
        let invoke_transaction2 = invoke_v3_transaction_with_data_gas(
            account_contract_address,
            transaction_nonce!("0x3"),
            call_param!("0x7"),
        );

        let input = Input {
            request: vec![
                declare_transaction,
                deploy_transaction,
                invoke_transaction,
                invoke_transaction2,
            ],
            simulation_flags: vec![SimulationFlag::SkipValidate],
            block_id: BlockId::Number(last_block_header.number),
        };
        let result = super::estimate_fee(context, input, RPC_VERSION)
            .await
            .unwrap();

        let output_json = result.serialize(Serializer { version }).unwrap();
        crate::assert_json_matches_fixture!(
            output_json,
            version,
            "fee_estimates/declare_deploy_invoke_sierra_0_13_4.json"
        );
    }

    #[rstest::rstest]
    #[case::v09(RpcVersion::V09)]
    #[case::v10(RpcVersion::V10)]
    #[tokio::test]
    async fn declare_deploy_and_invoke_sierra_class_starknet_0_14_0(#[case] version: RpcVersion) {
        let (context, last_block_header, account_contract_address, universal_deployer_address) =
            crate::test_setup::test_context_with_starknet_version(StarknetVersion::new(
                0, 14, 0, 0,
            ))
            .await;

        // declare test class
        let declare_transaction = declare_v3_transaction(account_contract_address);
        // deploy with universal deployer contract
        let deploy_transaction =
            deploy_v3_transaction(account_contract_address, universal_deployer_address);
        // invoke deployed contract
        let invoke_transaction = invoke_v3_transaction_with_data_gas(
            account_contract_address,
            transaction_nonce!("0x2"),
            call_param!("0x7"),
        );
        // Invoke once more to test that the execution state updates properly with L2
        // gas accounting aware code.
        let invoke_transaction2 = invoke_v3_transaction_with_data_gas(
            account_contract_address,
            transaction_nonce!("0x3"),
            call_param!("0x7"),
        );

        let input = Input {
            request: vec![
                declare_transaction,
                deploy_transaction,
                invoke_transaction,
                invoke_transaction2,
            ],
            simulation_flags: vec![SimulationFlag::SkipValidate],
            block_id: BlockId::Number(last_block_header.number),
        };
        let result = super::estimate_fee(context, input, RPC_VERSION)
            .await
            .unwrap();

        let output_json = result.serialize(Serializer { version }).unwrap();
        crate::assert_json_matches_fixture!(
            output_json,
            version,
            "fee_estimates/declare_deploy_invoke_sierra_0_14_0.json"
        );
    }

    #[rstest::rstest]
    #[case::v09(RpcVersion::V09)]
    #[case::v10(RpcVersion::V10)]
    #[tokio::test]
    async fn declare_deploy_and_invoke_sierra_class_starknet_0_14_1(#[case] version: RpcVersion) {
        let (context, last_block_header, account_contract_address, universal_deployer_address) =
            crate::test_setup::test_context_with_starknet_version(StarknetVersion::new(
                0, 14, 1, 0,
            ))
            .await;

        // declare test class
        let declare_transaction =
            declare_v3_transaction_with_blake2_casm_hash(account_contract_address);
        // deploy with universal deployer contract
        let deploy_transaction =
            deploy_v3_transaction(account_contract_address, universal_deployer_address);
        // invoke deployed contract
        let invoke_transaction = invoke_v3_transaction_with_data_gas(
            account_contract_address,
            transaction_nonce!("0x2"),
            call_param!("0x7"),
        );
        // Invoke once more to test that the execution state updates properly with L2
        // gas accounting aware code.
        let invoke_transaction2 = invoke_v3_transaction_with_data_gas(
            account_contract_address,
            transaction_nonce!("0x3"),
            call_param!("0x7"),
        );

        let input = Input {
            request: vec![
                declare_transaction,
                deploy_transaction,
                invoke_transaction,
                invoke_transaction2,
            ],
            simulation_flags: vec![SimulationFlag::SkipValidate],
            block_id: BlockId::Number(last_block_header.number),
        };
        let result = super::estimate_fee(context, input, RPC_VERSION)
            .await
            .unwrap();

        let output_json = result.serialize(Serializer { version }).unwrap();
        crate::assert_json_matches_fixture!(
            output_json,
            version,
            "fee_estimates/declare_deploy_invoke_sierra_0_14_1.json"
        );
    }

    /// Invokes the test contract with an invalid entry point so that
    /// the transaction is expected to be reverted.
    fn invoke_v3_transaction_with_invalid_entry_point(
        sender_address: ContractAddress,
        nonce: TransactionNonce,
        depth: CallParam,
    ) -> BroadcastedTransaction {
        BroadcastedTransaction::Invoke(BroadcastedInvokeTransaction::V3(
            BroadcastedInvokeTransactionV3 {
                version: TransactionVersion::THREE,
                signature: vec![],
                sender_address,
                calldata: vec![
                    // Number of calls
                    call_param!("0x1"),
                    // Address of the deployed test contract
                    CallParam(felt!(
                        "0x17c54b787c2eccfb057cf6aa2f941d612249549fff74140adc20bb949eab74b"
                    )),
                    // Entry point selector for the called contract, i.e.
                    CallParam(EntryPoint::hashed(b"bogus").0),
                    // Length of the call data for the called contract, i.e.
                    call_param!("0x1"),
                    depth,
                ],
                nonce,
                resource_bounds: ResourceBounds {
                    l1_gas: ResourceBound {
                        max_amount: ResourceAmount(50),
                        max_price_per_unit: ResourcePricePerUnit(1000),
                    },
                    l1_data_gas: Some(ResourceBound {
                        max_amount: ResourceAmount(100),
                        max_price_per_unit: ResourcePricePerUnit(1000),
                    }),
                    l2_gas: ResourceBound {
                        max_amount: ResourceAmount(800_000),
                        max_price_per_unit: ResourcePricePerUnit(1000),
                    },
                },
                tip: Tip(0),
                paymaster_data: vec![],
                account_deployment_data: vec![],
                nonce_data_availability_mode: DataAvailabilityMode::L2,
                fee_data_availability_mode: DataAvailabilityMode::L2,
                proof_facts: vec![],
                proof: Default::default(),
            },
        ))
    }

    #[tokio::test]
    async fn declare_deploy_and_invoke_sierra_class_reverts_on_starknet_0_13_4() {
        let (context, last_block_header, account_contract_address, universal_deployer_address) =
            crate::test_setup::test_context_with_starknet_version(StarknetVersion::new(
                0, 13, 4, 0,
            ))
            .await;

        // declare test class
        let declare_transaction = declare_v3_transaction(account_contract_address);
        // deploy with universal deployer contract
        let deploy_transaction =
            deploy_v3_transaction(account_contract_address, universal_deployer_address);
        // invoke deployed contract
        let invoke_transaction = invoke_v3_transaction_with_invalid_entry_point(
            account_contract_address,
            transaction_nonce!("0x2"),
            call_param!("0x7"),
        );

        let input = Input {
            request: vec![declare_transaction, deploy_transaction, invoke_transaction],
            simulation_flags: vec![SimulationFlag::SkipValidate],
            block_id: BlockId::Number(last_block_header.number),
        };
        let error = super::estimate_fee(context, input, RPC_VERSION)
            .await
            .unwrap_err();

        assert_matches::assert_matches!(error, EstimateFeeError::TransactionExecutionError { transaction_index, error, error_stack } => {
            assert_eq!(transaction_index, 2);
            assert_eq!(error, "Transaction execution has failed:\n\
                0: Error in the called contract (contract address: 0x0000000000000000000000000000000000000000000000000000000000000c01, class hash: 0x019cabebe31b9fb6bf5e7ce9a971bd7d06e9999e0b97eee943869141a46fd978, selector: 0x015d40a3d6ca2ac30f4031e42be28da9b056fef9bb7357ac5e85627ee876e5ad):\n\
                Execution failed. Failure reason:\n\
                Error in contract (contract address: 0x0000000000000000000000000000000000000000000000000000000000000c01, class hash: 0x019cabebe31b9fb6bf5e7ce9a971bd7d06e9999e0b97eee943869141a46fd978, selector: 0x015d40a3d6ca2ac30f4031e42be28da9b056fef9bb7357ac5e85627ee876e5ad):\n\
                Error in contract (contract address: 0x017c54b787c2eccfb057cf6aa2f941d612249549fff74140adc20bb949eab74b, class hash: 0x01a48fd3f75d0a7c2288ac23fb6aba26cd375607ba63e4a3b3ed47fc8e99dc21, selector: 0x02a1f595e2db7bf53e1a4bc9834eef6b86d3cd66ec9c8b3588c09253d0affc51):\n\
                0x454e545259504f494e545f4e4f545f464f554e44 ('ENTRYPOINT_NOT_FOUND').\n");
            assert_eq!(error_stack, pathfinder_executor::ErrorStack(vec![
                pathfinder_executor::Frame::CallFrame(pathfinder_executor::CallFrame {
                    storage_address: account_contract_address,
                    class_hash: crate::test_setup::OPENZEPPELIN_ACCOUNT_CLASS_HASH,
                    selector: Some(EntryPoint::hashed(b"__execute__")),
                }),
                pathfinder_executor::Frame::CallFrame(pathfinder_executor::CallFrame {
                    storage_address: account_contract_address,
                    class_hash: crate::test_setup::OPENZEPPELIN_ACCOUNT_CLASS_HASH,
                    selector: Some(EntryPoint::hashed(b"__execute__")),
                }),
                pathfinder_executor::Frame::CallFrame(pathfinder_executor::CallFrame {
                    storage_address: contract_address!("0x17c54b787c2eccfb057cf6aa2f941d612249549fff74140adc20bb949eab74b"),
                    class_hash: class_hash!("0x01A48FD3F75D0A7C2288AC23FB6ABA26CD375607BA63E4A3B3ED47FC8E99DC21"),
                    selector: Some(EntryPoint::hashed(b"bogus")),
                }),
                pathfinder_executor::Frame::StringFrame(
                    "0x454e545259504f494e545f4e4f545f464f554e44 ('ENTRYPOINT_NOT_FOUND')".to_owned()
                )
            ]));
        });
    }

    #[tokio::test]
    async fn starknet_0_13_4_max_gas_exceeded() {
        let (context, last_block_header, account_contract_address, universal_deployer_address) =
            crate::test_setup::test_context_with_starknet_version(StarknetVersion::new(
                0, 13, 4, 0,
            ))
            .await;

        // declare test class
        let declare_transaction = declare_v3_transaction(account_contract_address);
        // deploy with universal deployer contract
        let deploy_transaction =
            deploy_v3_transaction(account_contract_address, universal_deployer_address);

        // invoke deployed contract
        let invoke_transaction = invoke_v3_transaction_with_data_gas(
            account_contract_address,
            transaction_nonce!("0x2"),
            call_param!("0x100000"),
        );

        let input = Input {
            request: vec![declare_transaction, deploy_transaction, invoke_transaction],
            simulation_flags: vec![SimulationFlag::SkipValidate],
            block_id: BlockId::Number(last_block_header.number),
        };
        let result = super::estimate_fee(context, input, RPC_VERSION).await;
        let expected_err = anyhow::anyhow!("Fee estimation failed, maximum gas limit exceeded");
        assert_matches::assert_matches!(result, Err(EstimateFeeError::Internal(err)) if err.to_string() == expected_err.to_string());
    }

    #[tokio::test]
    async fn starknet_0_13_4_user_provided_gas_limit_exceeded_does_not_fail_with_out_of_gas() {
        let (context, last_block_header, account_contract_address, universal_deployer_address) =
            crate::test_setup::test_context_with_starknet_version(StarknetVersion::new(
                0, 13, 4, 0,
            ))
            .await;

        // declare test class
        let declare_transaction = declare_v3_transaction(account_contract_address);
        // deploy with universal deployer contract
        let deploy_transaction =
            deploy_v3_transaction(account_contract_address, universal_deployer_address);
        // Invoke deployed contract with large depth (it is a recursive function) such
        // that the L2 gas required exceeds the user provided limit.
        let invoke_transaction = invoke_v3_transaction_with_data_gas(
            account_contract_address,
            transaction_nonce!("0x2"),
            call_param!("0x100"),
        );

        let input = Input {
            request: vec![declare_transaction, deploy_transaction, invoke_transaction],
            simulation_flags: vec![SimulationFlag::SkipValidate],
            block_id: BlockId::Number(last_block_header.number),
        };
        let result = super::estimate_fee(context, input, RPC_VERSION)
            .await
            .unwrap();
        let declare_expected = FeeEstimate {
            l1_gas_consumed: 1736.into(),
            l1_gas_price: 2.into(),
            l1_data_gas_consumed: 192.into(),
            l1_data_gas_price: 2.into(),
            l2_gas_consumed: 0.into(),
            l2_gas_price: 1.into(),
            overall_fee: 3856.into(),
            unit: PriceUnit::Fri,
        };
        let deploy_expected = FeeEstimate {
            l1_gas_consumed: 22.into(),
            l1_gas_price: 2.into(),
            l1_data_gas_consumed: 224.into(),
            l1_data_gas_price: 2.into(),
            l2_gas_consumed: 0.into(),
            l2_gas_price: 1.into(),
            overall_fee: 492.into(),
            unit: PriceUnit::Fri,
        };
        let invoke_expected = FeeEstimate {
            l1_gas_consumed: 0.into(),
            l1_gas_price: 2.into(),
            l1_data_gas_consumed: 128.into(),
            l1_data_gas_price: 2.into(),
            l2_gas_consumed: 15624585.into(),
            l2_gas_price: 1.into(),
            overall_fee: 15624841.into(),
            unit: PriceUnit::Fri,
        };
        self::assert_eq!(
            result,
            Output(vec![declare_expected, deploy_expected, invoke_expected,])
        );
    }

    #[tokio::test]
    async fn deploy_account_starknet_0_13_5() {
        let starknet_version = StarknetVersion::new(0, 13, 5, 0);
        let (context, last_block_header, _account_contract_address, _universal_deployer_address) =
            crate::test_setup::test_context_with_starknet_version(starknet_version).await;

        let deploy_account = crate::types::request::BroadcastedDeployAccountTransaction::V3(
            BroadcastedDeployAccountTransactionV3 {
                version: TransactionVersion::THREE,
                signature: vec![],
                nonce: transaction_nonce!("0x0"),
                resource_bounds: ResourceBounds {
                    l1_gas: ResourceBound {
                        max_amount: ResourceAmount(0),
                        max_price_per_unit: ResourcePricePerUnit(0),
                    },
                    l2_gas: ResourceBound {
                        max_amount: ResourceAmount(0),
                        max_price_per_unit: ResourcePricePerUnit(0),
                    },
                    l1_data_gas: Some(ResourceBound {
                        max_amount: ResourceAmount(0),
                        max_price_per_unit: ResourcePricePerUnit(0),
                    }),
                },
                tip: Tip(0),
                paymaster_data: vec![],
                nonce_data_availability_mode: DataAvailabilityMode::L1,
                fee_data_availability_mode: DataAvailabilityMode::L1,
                contract_address_salt: contract_address_salt!("0x1"),
                constructor_calldata: vec![call_param!("0xdeadbeef")],
                class_hash: crate::test_setup::OPENZEPPELIN_ACCOUNT_CLASS_HASH,
            },
        );

        let input = Input {
            request: vec![BroadcastedTransaction::DeployAccount(deploy_account)],
            simulation_flags: vec![],
            block_id: BlockId::Number(last_block_header.number),
        };
        let result = super::estimate_fee(context, input, RPC_VERSION)
            .await
            .unwrap();

        let output_json = result
            .serialize(Serializer {
                version: RpcVersion::V09,
            })
            .unwrap();
        let expected_json = serde_json::json!([
            {
                "l1_data_gas_consumed": "0x1c0",
                "l1_data_gas_price": "0x2",
                "l1_gas_consumed": "0x0",
                "l1_gas_price": "0x2",
                "l2_gas_consumed": "0xb5842",
                "l2_gas_price": "0x1",
                "overall_fee": "0xb5bc2",
                "unit": "FRI"
            }
        ]);
        pretty_assertions_sorted::assert_eq!(expected_json, output_json);
    }

    #[tokio::test]
    async fn deploy_account_starknet_0_13_1() {
        let starknet_version = StarknetVersion::new(0, 13, 1, 0);
        let (context, last_block_header, _account_contract_address, _universal_deployer_address) =
            crate::test_setup::test_context_with_starknet_version(starknet_version).await;

        let deploy_account = crate::types::request::BroadcastedDeployAccountTransaction::V3(
            BroadcastedDeployAccountTransactionV3 {
                version: TransactionVersion::THREE,
                signature: vec![],
                nonce: transaction_nonce!("0x0"),
                resource_bounds: ResourceBounds {
                    l1_gas: ResourceBound {
                        max_amount: ResourceAmount(0),
                        max_price_per_unit: ResourcePricePerUnit(0),
                    },
                    l2_gas: ResourceBound {
                        max_amount: ResourceAmount(0),
                        max_price_per_unit: ResourcePricePerUnit(0),
                    },
                    l1_data_gas: None,
                },
                tip: Tip(0),
                paymaster_data: vec![],
                nonce_data_availability_mode: DataAvailabilityMode::L1,
                fee_data_availability_mode: DataAvailabilityMode::L1,
                contract_address_salt: contract_address_salt!("0x1"),
                constructor_calldata: vec![call_param!("0xdeadbeef")],
                class_hash: crate::test_setup::OPENZEPPELIN_ACCOUNT_CLASS_HASH,
            },
        );

        let input = Input {
            request: vec![BroadcastedTransaction::DeployAccount(deploy_account)],
            simulation_flags: vec![],
            block_id: BlockId::Number(last_block_header.number),
        };
        let result = super::estimate_fee(context, input, RPC_VERSION)
            .await
            .unwrap();

        let output_json = result
            .serialize(Serializer {
                version: RpcVersion::V09,
            })
            .unwrap();
        let expected_json = serde_json::json!([
            {
                "l1_data_gas_consumed": "0x160",
                "l1_data_gas_price": "0x2",
                "l1_gas_consumed": "0xc",
                "l1_gas_price": "0x2",
                "l2_gas_consumed": "0x0",
                "l2_gas_price": "0x1",
                "overall_fee": "0x2d8",
                "unit": "FRI"
            }
        ]);
        pretty_assertions_sorted::assert_eq!(expected_json, output_json);
    }

    #[test_log::test(tokio::test)]
    async fn calldata_limit_exceeded() {
        let starknet_version = StarknetVersion::new(0, 13, 1, 0);
        let (context, last_block_header, _account_contract_address, _universal_deployer_address) =
            crate::test_setup::test_context_with_starknet_version(starknet_version).await;

        let invoke_tx = crate::types::request::BroadcastedInvokeTransaction::V3(
            BroadcastedInvokeTransactionV3 {
                // Calldata length over the limit, the rest of the fields should not matter.
                calldata: vec![call_param!("0x123"); CALLDATA_LIMIT + 5],

                version: TransactionVersion::THREE,
                signature: vec![],
                nonce: transaction_nonce!("0x1"),
                resource_bounds: ResourceBounds::default(),
                tip: Tip(0),
                paymaster_data: vec![],
                account_deployment_data: vec![],
                nonce_data_availability_mode: DataAvailabilityMode::L1,
                fee_data_availability_mode: DataAvailabilityMode::L1,
                sender_address: contract_address!("0xdeadbeef"),
                proof_facts: vec![],
                proof: Default::default(),
            },
        );

        let input = Input {
            request: vec![BroadcastedTransaction::Invoke(invoke_tx)],
            simulation_flags: vec![],
            block_id: BlockId::Number(last_block_header.number),
        };

        let err = super::estimate_fee(context, input, RPC_VERSION)
            .await
            .unwrap_err();

        let error_cause = "Calldata limit (10000) exceeded by transaction at index 0";
        assert_matches!(err, EstimateFeeError::Custom(e) if e.root_cause().to_string() == error_cause);
    }

    #[test_log::test(tokio::test)]
    async fn signature_element_limit_exceeded() {
        let starknet_version = StarknetVersion::new(0, 13, 1, 0);
        let (context, last_block_header, _account_contract_address, _universal_deployer_address) =
            crate::test_setup::test_context_with_starknet_version(starknet_version).await;

        let invoke_tx = crate::types::request::BroadcastedInvokeTransaction::V3(
            BroadcastedInvokeTransactionV3 {
                // Signature length over the limit, the rest of the fields should not matter.
                signature: vec![transaction_signature_elem!("0x123"); SIGNATURE_ELEMENT_LIMIT + 5],

                version: TransactionVersion::THREE,
                nonce: transaction_nonce!("0x1"),
                resource_bounds: ResourceBounds::default(),
                tip: Tip(0),
                paymaster_data: vec![],
                account_deployment_data: vec![],
                nonce_data_availability_mode: DataAvailabilityMode::L1,
                fee_data_availability_mode: DataAvailabilityMode::L1,
                sender_address: contract_address!("0xdeadbeef"),
                calldata: vec![],
                proof_facts: vec![],
                proof: Default::default(),
            },
        );

        let input = Input {
            request: vec![BroadcastedTransaction::Invoke(invoke_tx)],
            simulation_flags: vec![],
            block_id: BlockId::Number(last_block_header.number),
        };

        let err = super::estimate_fee(context, input, RPC_VERSION)
            .await
            .unwrap_err();

        let error_cause = "Signature element limit (10000) exceeded by transaction at index 0";
        assert_matches!(err, EstimateFeeError::Custom(e) if e.root_cause().to_string() == error_cause);
    }
}
