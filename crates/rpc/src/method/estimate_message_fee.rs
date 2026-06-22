use std::sync::Arc;

use anyhow::Context;
use pathfinder_common::prelude::*;
use pathfinder_crypto::Felt;
use pathfinder_executor::{ExecutionState, IntoStarkFelt, L1BlobDataAvailability};
use starknet_api::core::PatriciaKey;
use starknet_api::transaction::fields::{Calldata, Fee};

use crate::context::RpcContext;
use crate::error::ApplicationError;
use crate::executor::CALLDATA_LIMIT;
use crate::types::BlockId;
use crate::RpcVersion;

#[derive(Debug, PartialEq, Eq)]
pub struct EstimateMessageFeeInput {
    pub message: MsgFromL1,
    pub block_id: BlockId,
}

impl crate::dto::DeserializeForVersion for EstimateMessageFeeInput {
    fn deserialize(value: crate::dto::Value) -> Result<Self, serde_json::Error> {
        value.deserialize_map(|value| {
            Ok(Self {
                message: value.deserialize("message")?,
                block_id: value.deserialize("block_id")?,
            })
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct MsgFromL1 {
    pub from_address: EthereumAddress,
    pub to_address: ContractAddress,
    pub entry_point_selector: EntryPoint,
    pub payload: Vec<CallParam>,
}

impl crate::dto::DeserializeForVersion for MsgFromL1 {
    fn deserialize(value: crate::dto::Value) -> Result<Self, serde_json::Error> {
        value.deserialize_map(|value| {
            Ok(Self {
                from_address: value.deserialize("from_address")?,
                to_address: value.deserialize("to_address").map(ContractAddress)?,
                entry_point_selector: value.deserialize("entry_point_selector").map(EntryPoint)?,
                payload: value
                    .deserialize_array("payload", |value| value.deserialize().map(CallParam))?,
            })
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Output(pathfinder_executor::types::FeeEstimate);

pub async fn estimate_message_fee(
    context: RpcContext,
    input: EstimateMessageFeeInput,
    rpc_version: RpcVersion,
) -> Result<Output, EstimateMessageFeeError> {
    let span = tracing::Span::current();
    if input.message.payload.len() > CALLDATA_LIMIT {
        return Err(EstimateMessageFeeError::Custom(anyhow::anyhow!(
            "Calldata limit ({CALLDATA_LIMIT}) exceeded"
        )));
    }
    let mut result = util::task::spawn_blocking(move |_| {
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
                let pending = context.pending_data.get(&db_tx)?;

                (
                    pending.pre_confirmed_header(),
                    Some(pending.aggregated_state_update()),
                )
            }
            other => {
                let block_id = other
                    .to_common_or_panic(&db_tx)
                    .map_err(|_| EstimateMessageFeeError::BlockNotFound)?;

                let header = db_tx
                    .block_header(block_id)
                    .context("Querying block header")?
                    .ok_or(EstimateMessageFeeError::BlockNotFound)?;

                (header, None)
            }
        };

        // the error isn't in spec for earlier API versions
        if (rpc_version >= RpcVersion::V10)
            && !db_tx.contract_exists(input.message.to_address, header.number.into())?
        {
            return Err(EstimateMessageFeeError::ContractNotFound);
        }

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

        let transaction = create_executor_transaction(input, context.chain_id)?;

        let result = pathfinder_executor::estimate(
            db_tx,
            state,
            vec![transaction],
            context.config.fee_estimation_epsilon,
        )?;

        Ok::<_, EstimateMessageFeeError>(result)
    })
    .await
    .context("Estimating message fee")??;

    if result.len() != 1 {
        return Err(
            anyhow::anyhow!("Internal error: expected exactly one fee estimation result").into(),
        );
    }

    let result = result.pop().unwrap();
    Ok(Output(result))
}

fn create_executor_transaction(
    input: EstimateMessageFeeInput,
    chain_id: ChainId,
) -> anyhow::Result<pathfinder_executor::Transaction> {
    let from_address =
        Felt::from_be_slice(input.message.from_address.0.as_bytes()).expect("This cannot overflow");
    let calldata = std::iter::once(CallParam(from_address))
        .chain(input.message.payload)
        .collect();
    let transaction = pathfinder_common::transaction::L1HandlerTransaction {
        contract_address: input.message.to_address,
        entry_point_selector: input.message.entry_point_selector,
        nonce: TransactionNonce::ZERO,
        calldata,
    };

    let transaction_hash = transaction.calculate_hash(chain_id);

    let tx = starknet_api::transaction::L1HandlerTransaction {
        version: starknet_api::transaction::TransactionVersion::ZERO,
        nonce: starknet_api::core::Nonce(starknet_types_core::felt::Felt::ZERO),
        contract_address: starknet_api::core::ContractAddress(
            PatriciaKey::try_from(transaction.contract_address.0.into_starkfelt())
                .expect("A ContractAddress should be the right size"),
        ),
        entry_point_selector: starknet_api::core::EntryPointSelector(
            input.message.entry_point_selector.0.into_starkfelt(),
        ),
        calldata: Calldata(Arc::new(
            transaction
                .calldata
                .into_iter()
                .map(|x| x.0.into_starkfelt())
                .collect(),
        )),
    };

    let transaction = pathfinder_executor::Transaction::from_api(
        starknet_api::transaction::Transaction::L1Handler(tx),
        starknet_api::transaction::TransactionHash(transaction_hash.0.into_starkfelt()),
        None,
        Some(Fee(1)),
        None,
        pathfinder_executor::AccountTransactionExecutionFlags::default(),
    )?;
    Ok(transaction)
}

impl crate::dto::SerializeForVersion for Output {
    fn serialize(
        &self,
        serializer: crate::dto::Serializer,
    ) -> Result<crate::dto::Ok, crate::dto::Error> {
        self.0.serialize(serializer)
    }
}

#[derive(Debug)]
pub enum EstimateMessageFeeError {
    Internal(anyhow::Error),
    BlockNotFound,
    ContractNotFound,
    ContractError {
        revert_error: String,
        revert_error_stack: pathfinder_executor::ErrorStack,
    },
    Custom(anyhow::Error),
}

impl From<anyhow::Error> for EstimateMessageFeeError {
    fn from(e: anyhow::Error) -> Self {
        Self::Internal(e)
    }
}

crate::error::impl_from_pending_read_error!(EstimateMessageFeeError);

impl From<pathfinder_executor::TransactionExecutionError> for EstimateMessageFeeError {
    fn from(c: pathfinder_executor::TransactionExecutionError) -> Self {
        use pathfinder_executor::TransactionExecutionError::*;
        match c {
            ExecutionError {
                error, error_stack, ..
            } => Self::ContractError {
                revert_error: format!("Execution error: {error}"),
                revert_error_stack: error_stack,
            },
            Internal(e) => Self::Internal(e),
            Custom(e) => Self::Custom(e),
        }
    }
}

impl From<crate::executor::ExecutionStateError> for EstimateMessageFeeError {
    fn from(error: crate::executor::ExecutionStateError) -> Self {
        use crate::executor::ExecutionStateError::*;
        match error {
            BlockNotFound => Self::BlockNotFound,
            Internal(e) => Self::Internal(e),
        }
    }
}

impl From<EstimateMessageFeeError> for ApplicationError {
    fn from(value: EstimateMessageFeeError) -> Self {
        match value {
            EstimateMessageFeeError::BlockNotFound => ApplicationError::BlockNotFound,
            EstimateMessageFeeError::ContractNotFound => ApplicationError::ContractNotFound,
            EstimateMessageFeeError::ContractError {
                revert_error,
                revert_error_stack,
            } => ApplicationError::ContractError {
                revert_error: Some(revert_error),
                revert_error_stack,
            },
            EstimateMessageFeeError::Internal(e) => ApplicationError::Internal(e),
            EstimateMessageFeeError::Custom(e) => ApplicationError::Custom(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use pathfinder_common::class_definition::{
        SerializedCasmDefinition,
        SerializedSierraDefinition,
    };
    use pathfinder_common::macro_prelude::*;
    use pathfinder_common::prelude::*;
    use pathfinder_common::L1DataAvailabilityMode;
    use pathfinder_storage::StorageBuilder;
    use primitive_types::H160;

    use super::*;
    use crate::context::RpcContext;
    use crate::dto::{SerializeForVersion, Serializer};
    use crate::types::BlockId;
    use crate::RpcVersion;

    enum Setup {
        Full,
        _SkipBlock,
        _SkipContract,
    }

    async fn setup(mode: Setup) -> anyhow::Result<RpcContext> {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut db_path = dir.path().to_path_buf();
        db_path.push("db.sqlite");

        let storage = StorageBuilder::file(db_path)
            .migrate()
            .expect("storage")
            .create_pool(std::num::NonZeroU32::new(1).expect("one"))
            .expect("storage");

        {
            let mut db = storage.connection().expect("db connection");
            let tx = db.transaction().expect("tx");

            let sierra_json = include_bytes!("../../fixtures/contracts/l1_handler.json");
            let casm_json = include_bytes!("../../fixtures/contracts/l1_handler.casm");

            let class_hash =
                class_hash!("0x032908a85d43275f8509ba5f2acae88811b293463a3521dc05ab06d534b40848");
            tx.insert_sierra_class_definition(
                &SierraHash(class_hash.0),
                &SerializedSierraDefinition::from_slice(sierra_json),
                &SerializedCasmDefinition::from_slice(casm_json),
                &casm_hash_bytes!(b"casm hash blake"),
            )
            .expect("insert class");

            let block1_number = BlockNumber::GENESIS + 1;
            let block1_hash = BlockHash(felt!("0xb01"));

            if !matches!(mode, Setup::_SkipBlock) {
                let header = BlockHeader::builder()
                    .number(BlockNumber::GENESIS)
                    .timestamp(BlockTimestamp::new_or_panic(0))
                    .l1_da_mode(pathfinder_common::L1DataAvailabilityMode::Blob)
                    .strk_l1_data_gas_price(GasPrice(0x10))
                    .eth_l1_data_gas_price(GasPrice(0x12))
                    .finalize_with_hash(BlockHash(felt!("0xb00")));
                tx.insert_block_header(&header).unwrap();

                let header = BlockHeader::builder()
                    .number(block1_number)
                    .timestamp(BlockTimestamp::new_or_panic(1))
                    .eth_l1_gas_price(GasPrice(2))
                    .eth_l1_data_gas_price(GasPrice(1))
                    .starknet_version(StarknetVersion::new(0, 13, 1, 0))
                    .l1_da_mode(L1DataAvailabilityMode::Blob)
                    .finalize_with_hash(block1_hash);
                tx.insert_block_header(&header).unwrap();
            }

            if !matches!(mode, Setup::_SkipBlock | Setup::_SkipContract) {
                let contract_address = contract_address!(
                    "0x57dde83c18c0efe7123c36a52d704cf27d5c38cdf0b1e1edc3b0dae3ee4e374"
                );
                let state_update =
                    StateUpdate::default().with_deployed_contract(contract_address, class_hash);
                tx.insert_state_update(block1_number, &state_update)
                    .unwrap();
            }

            tx.commit().unwrap();
        }

        let rpc = RpcContext::for_tests().with_storage(storage);

        Ok(rpc)
    }

    fn input() -> EstimateMessageFeeInput {
        EstimateMessageFeeInput {
            message: MsgFromL1 {
                to_address: contract_address!(
                    "0x57dde83c18c0efe7123c36a52d704cf27d5c38cdf0b1e1edc3b0dae3ee4e374"
                ),
                entry_point_selector: EntryPoint::hashed(b"my_l1_handler"),
                payload: vec![call_param!("0xa")],
                from_address: EthereumAddress(H160::zero()),
            },
            block_id: BlockId::Number(BlockNumber::new_or_panic(1)),
        }
    }

    const RPC_VERSION: RpcVersion = RpcVersion::V09;

    #[rstest::rstest]
    #[case::v09(RpcVersion::V09)]
    #[case::v10(RpcVersion::V10)]
    #[tokio::test]
    async fn test_estimate_message_fee(#[case] version: RpcVersion) {
        let rpc = setup(Setup::Full).await.expect("RPC context");
        let result = super::estimate_message_fee(rpc, input(), version)
            .await
            .expect("result");

        let output_json = result.serialize(Serializer { version }).unwrap();
        crate::assert_json_matches_fixture!(output_json, version, "fee_estimates/full.json");
    }

    #[test_log::test(tokio::test)]
    async fn calldata_limit_exceeded() {
        let rpc = setup(Setup::Full).await.expect("RPC context");
        let input = EstimateMessageFeeInput {
            message: MsgFromL1 {
                // Calldata length over the limit, the rest of the fields should not matter.
                payload: vec![call_param!("0x123"); CALLDATA_LIMIT + 5],

                to_address: contract_address!("0xdeadbeef"),
                entry_point_selector: EntryPoint::ZERO,
                from_address: EthereumAddress(H160::zero()),
            },
            block_id: BlockId::Number(BlockNumber::new_or_panic(1)),
        };

        let err = super::estimate_message_fee(rpc, input, RPC_VERSION)
            .await
            .unwrap_err();

        let error_cause = "Calldata limit (10000) exceeded";
        assert_matches!(err, EstimateMessageFeeError::Custom(e) if e.root_cause().to_string() == error_cause);
    }

    #[tokio::test]
    async fn contract_not_found_early() {
        let rpc = setup(Setup::Full).await.expect("RPC context");
        let input = EstimateMessageFeeInput {
            message: MsgFromL1 {
                to_address: contract_address!(
                    // incorrect
                    "0x57dde83c18c0efe7123c36a52d704cf27d5c38cdf0b1e1edc3b0dae3ee4e370"
                ),
                entry_point_selector: EntryPoint::hashed(b"my_l1_handler"),
                payload: vec![call_param!("0xa")],
                from_address: EthereumAddress(H160::zero()),
            },
            block_id: BlockId::Number(BlockNumber::new_or_panic(1)),
        };

        let err = super::estimate_message_fee(rpc, input, RpcVersion::V10)
            .await
            .unwrap_err();
        assert_matches!(err, EstimateMessageFeeError::ContractNotFound);
    }

    #[rstest::rstest]
    #[case::v09(RpcVersion::V09)]
    #[tokio::test]
    async fn contract_not_found_late(#[case] version: RpcVersion) {
        let rpc = setup(Setup::Full).await.expect("RPC context");
        let input = EstimateMessageFeeInput {
            message: MsgFromL1 {
                to_address: contract_address!(
                    // incorrect
                    "0x57dde83c18c0efe7123c36a52d704cf27d5c38cdf0b1e1edc3b0dae3ee4e370"
                ),
                entry_point_selector: EntryPoint::hashed(b"my_l1_handler"),
                payload: vec![call_param!("0xa")],
                from_address: EthereumAddress(H160::zero()),
            },
            block_id: BlockId::Number(BlockNumber::new_or_panic(1)),
        };

        let err = super::estimate_message_fee(rpc, input, version)
            .await
            .unwrap_err();
        let expected_revert_error =
            "Execution error: Transaction execution has failed:\n0: Error in the called contract \
             (contract address: \
             0x057dde83c18c0efe7123c36a52d704cf27d5c38cdf0b1e1edc3b0dae3ee4e370, class hash: \
             0x0000000000000000000000000000000000000000000000000000000000000000, selector: \
             0x031ee153a27e249dc4bade6b861b37ef1e1ea0a4c0bf73b7405a02e9e72f7be3):\nRequested \
             contract address 0x057dde83c18c0efe7123c36a52d704cf27d5c38cdf0b1e1edc3b0dae3ee4e370 \
             is not deployed.\n";
        assert_matches!(err, EstimateMessageFeeError::ContractError { revert_error, revert_error_stack: _ } if revert_error == expected_revert_error);
    }
}
