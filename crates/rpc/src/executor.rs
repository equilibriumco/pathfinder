use anyhow::Context;
use pathfinder_common::class_definition::{
    SerializedOpaqueClassDefinition,
    SerializedSierraDefinition,
};
use pathfinder_common::transaction::TransactionVariant;
use pathfinder_common::{BlockNumber, ChainId, StarknetVersion};
use pathfinder_executor::types::to_starknet_api_transaction;
use pathfinder_executor::{ClassInfo, IntoStarkFelt};
use starknet_api::contract_class::SierraVersion;
use starknet_api::core::PatriciaKey;
use starknet_api::transaction::fields::Fee;

use crate::types::class::sierra::SierraContractClass;
use crate::types::request::{
    BroadcastedDeployAccountTransaction,
    BroadcastedInvokeTransaction,
    BroadcastedTransaction,
};
use crate::PathfinderCompiler;

pub enum ExecutionStateError {
    BlockNotFound,
    Internal(anyhow::Error),
}

impl From<anyhow::Error> for ExecutionStateError {
    fn from(error: anyhow::Error) -> Self {
        Self::Internal(error)
    }
}

pub const CALLDATA_LIMIT: usize = 10_000;
pub const SIGNATURE_ELEMENT_LIMIT: usize = 10_000;
pub const VERSIONS_LOWER_THAN_THIS_SHOULD_FALL_BACK_TO_FETCHING_TRACE_FROM_GATEWAY:
    StarknetVersion = StarknetVersion::new(0, 13, 1, 1);

pub const MAINNET_RANGE_WHERE_RE_EXECUTION_IS_IMPOSSIBLE_START: BlockNumber =
    BlockNumber::new_or_panic(1943704);
pub const MAINNET_RANGE_WHERE_RE_EXECUTION_IS_IMPOSSIBLE_END: BlockNumber =
    BlockNumber::new_or_panic(1952704);

pub(crate) fn calldata_limit_exceeded(tx: &BroadcastedTransaction) -> bool {
    match tx {
        BroadcastedTransaction::Declare(_) => false,
        BroadcastedTransaction::Invoke(broadcasted_invoke_tx) => match broadcasted_invoke_tx {
            BroadcastedInvokeTransaction::V0(tx) => tx.calldata.len() > CALLDATA_LIMIT,
            BroadcastedInvokeTransaction::V1(tx) => tx.calldata.len() > CALLDATA_LIMIT,
            BroadcastedInvokeTransaction::V3(tx) => tx.calldata.len() > CALLDATA_LIMIT,
        },
        BroadcastedTransaction::DeployAccount(broadcasted_deploy_tx) => match broadcasted_deploy_tx
        {
            BroadcastedDeployAccountTransaction::V1(tx) => {
                tx.constructor_calldata.len() > CALLDATA_LIMIT
            }
            BroadcastedDeployAccountTransaction::V3(tx) => {
                tx.constructor_calldata.len() > CALLDATA_LIMIT
            }
        },
    }
}

pub(crate) fn signature_elem_limit_exceeded(tx: &BroadcastedTransaction) -> bool {
    match tx {
        BroadcastedTransaction::Declare(_) => false,
        BroadcastedTransaction::Invoke(broadcasted_invoke_tx) => match broadcasted_invoke_tx {
            BroadcastedInvokeTransaction::V0(tx) => tx.signature.len() > SIGNATURE_ELEMENT_LIMIT,
            BroadcastedInvokeTransaction::V1(tx) => tx.signature.len() > SIGNATURE_ELEMENT_LIMIT,
            BroadcastedInvokeTransaction::V3(tx) => tx.signature.len() > SIGNATURE_ELEMENT_LIMIT,
        },
        BroadcastedTransaction::DeployAccount(broadcasted_deploy_tx) => match broadcasted_deploy_tx
        {
            BroadcastedDeployAccountTransaction::V1(tx) => {
                tx.signature.len() > SIGNATURE_ELEMENT_LIMIT
            }
            BroadcastedDeployAccountTransaction::V3(tx) => {
                tx.signature.len() > SIGNATURE_ELEMENT_LIMIT
            }
        },
    }
}

/// Maps an [RPC layer transaction](BroadcastedTransaction) into a transaction
/// that can be fed into the [executor](pathfinder_executor).
///
/// This is a blocking function. When used inside an async runtime, it should be
/// called on a blocking thread.
pub(crate) fn map_broadcasted_transaction(
    transaction: &BroadcastedTransaction,
    chain_id: ChainId,
    compiler: &PathfinderCompiler,
    skip_validate: bool,
    skip_fee_charge: bool,
) -> anyhow::Result<pathfinder_executor::Transaction> {
    use crate::types::request::BroadcastedDeclareTransaction;

    let class_info = match &transaction {
        BroadcastedTransaction::Declare(BroadcastedDeclareTransaction::V0(tx)) => {
            let contract_class_json = tx
                .contract_class
                .serialize_to_json()
                .context("Serializing Cairo class to JSON")?;

            let contract_class = pathfinder_executor::parse_deprecated_class_definition(
                SerializedOpaqueClassDefinition::from_vec(contract_class_json),
            )?;

            Some(ClassInfo::new(
                &contract_class,
                0,
                0,
                SierraVersion::DEPRECATED,
            )?)
        }
        BroadcastedTransaction::Declare(BroadcastedDeclareTransaction::V1(tx)) => {
            let contract_class_json = tx
                .contract_class
                .serialize_to_json()
                .context("Serializing Cairo class to JSON")?;

            let contract_class = pathfinder_executor::parse_deprecated_class_definition(
                SerializedOpaqueClassDefinition::from_vec(contract_class_json),
            )?;

            Some(ClassInfo::new(
                &contract_class,
                0,
                0,
                SierraVersion::DEPRECATED,
            )?)
        }
        BroadcastedTransaction::Declare(BroadcastedDeclareTransaction::V2(tx)) => {
            let sierra_version =
                SierraVersion::extract_from_program(&tx.contract_class.sierra_program)?;
            let sierra_definition = SerializedSierraDefinition::from_vec(
                serde_json::to_vec(&tx.contract_class)
                    .context("Serializing Sierra class definition")?,
            );
            let casm_contract_definition = compiler
                .compile_sierra_to_casm(&sierra_definition)
                .context("Compiling Sierra class definition to CASM")?;

            let casm_contract_definition = pathfinder_executor::parse_casm_definition(
                casm_contract_definition,
                sierra_version.clone(),
            )
            .context("Parsing CASM contract definition")?;
            Some(ClassInfo::new(
                &casm_contract_definition,
                tx.contract_class.sierra_program.len(),
                tx.contract_class.abi.len(),
                sierra_version,
            )?)
        }
        BroadcastedTransaction::Declare(BroadcastedDeclareTransaction::V3(tx)) => {
            let sierra_version =
                SierraVersion::extract_from_program(&tx.contract_class.sierra_program)?;
            let sierra_definition = SerializedSierraDefinition::from_vec(
                serde_json::to_vec(&tx.contract_class)
                    .context("Serializing Sierra class definition")?,
            );
            let casm_contract_definition = compiler
                .compile_sierra_to_casm(&sierra_definition)
                .context("Compiling Sierra class definition to CASM")?;

            let casm_contract_definition = pathfinder_executor::parse_casm_definition(
                casm_contract_definition,
                sierra_version.clone(),
            )
            .context("Parsing CASM contract definition")?;
            Some(ClassInfo::new(
                &casm_contract_definition,
                tx.contract_class.sierra_program.len(),
                tx.contract_class.abi.len(),
                sierra_version,
            )?)
        }
        BroadcastedTransaction::Invoke(_) | BroadcastedTransaction::DeployAccount(_) => None,
    };

    let deployed_address = match &transaction {
        BroadcastedTransaction::DeployAccount(BroadcastedDeployAccountTransaction::V1(tx)) => {
            Some(starknet_api::core::ContractAddress(
                PatriciaKey::try_from(tx.deployed_contract_address().0.into_starkfelt())
                    .expect("No sender address overflow expected"),
            ))
        }
        BroadcastedTransaction::DeployAccount(BroadcastedDeployAccountTransaction::V3(tx)) => {
            Some(starknet_api::core::ContractAddress(
                PatriciaKey::try_from(tx.deployed_contract_address().0.into_starkfelt())
                    .expect("No sender address overflow expected"),
            ))
        }
        BroadcastedTransaction::Declare(_) | BroadcastedTransaction::Invoke(_) => None,
    };

    let has_query_version = match &transaction {
        BroadcastedTransaction::Declare(BroadcastedDeclareTransaction::V0(tx)) => {
            tx.version.has_query_version()
        }
        BroadcastedTransaction::Declare(BroadcastedDeclareTransaction::V1(tx)) => {
            tx.version.has_query_version()
        }
        BroadcastedTransaction::Declare(BroadcastedDeclareTransaction::V2(tx)) => {
            tx.version.has_query_version()
        }
        BroadcastedTransaction::Declare(BroadcastedDeclareTransaction::V3(tx)) => {
            tx.version.has_query_version()
        }
        BroadcastedTransaction::Invoke(BroadcastedInvokeTransaction::V0(tx)) => {
            tx.version.has_query_version()
        }
        BroadcastedTransaction::Invoke(BroadcastedInvokeTransaction::V1(tx)) => {
            tx.version.has_query_version()
        }
        BroadcastedTransaction::Invoke(BroadcastedInvokeTransaction::V3(tx)) => {
            tx.version.has_query_version()
        }
        BroadcastedTransaction::DeployAccount(BroadcastedDeployAccountTransaction::V1(tx)) => {
            tx.version.has_query_version()
        }
        BroadcastedTransaction::DeployAccount(BroadcastedDeployAccountTransaction::V3(tx)) => {
            tx.version.has_query_version()
        }
    };

    let execution_flags = pathfinder_executor::AccountTransactionExecutionFlags {
        only_query: has_query_version,
        validate: !skip_validate,
        charge_fee: !skip_fee_charge,
        strict_nonce_check: !skip_validate,
    };

    let transaction = transaction.clone().try_into_common(chain_id)?;
    let transaction_hash = transaction.hash;
    let transaction = to_starknet_api_transaction(transaction.variant)?;

    let tx = pathfinder_executor::Transaction::from_api(
        transaction,
        starknet_api::transaction::TransactionHash(transaction_hash.0.into_starkfelt()),
        class_info,
        None,
        deployed_address,
        execution_flags,
    )?;

    Ok(tx)
}

/// Build the executor transaction out of the gateway one
/// while pulling necessary data from the DB along the way.
pub fn compose_executor_transaction(
    transaction: &pathfinder_common::transaction::Transaction,
    db_transaction: &pathfinder_storage::Transaction<'_>,
) -> anyhow::Result<pathfinder_executor::Transaction> {
    let tx_hash = starknet_api::transaction::TransactionHash(transaction.hash.0.into_starkfelt());

    let class_info = match &transaction.variant {
        TransactionVariant::DeclareV0(tx) => {
            let class_definition = db_transaction
                .class_definition(tx.class_hash)?
                .context("Fetching class definition")?;

            let contract_class =
                pathfinder_executor::parse_deprecated_class_definition(class_definition)?;
            Some(ClassInfo::new(
                &contract_class,
                0,
                0,
                SierraVersion::DEPRECATED,
            )?)
        }
        TransactionVariant::DeclareV1(tx) => {
            let class_definition = db_transaction
                .class_definition(tx.class_hash)?
                .context("Fetching class definition")?;

            let contract_class =
                pathfinder_executor::parse_deprecated_class_definition(class_definition)?;
            Some(ClassInfo::new(
                &contract_class,
                0,
                0,
                SierraVersion::DEPRECATED,
            )?)
        }
        TransactionVariant::DeclareV2(tx) => {
            let casm_definition = db_transaction
                .casm_definition(tx.class_hash)?
                .context("Fetching class CASM definition")?;
            let class_definition = db_transaction
                .class_definition(tx.class_hash)?
                .context("Fetching class definition")?;
            let class_definition: SierraContractClass =
                serde_json::from_slice(class_definition.as_slice())
                    .context("Deserializing class definition")?;
            let sierra_version =
                SierraVersion::extract_from_program(&class_definition.sierra_program)?;

            let contract_class =
                pathfinder_executor::parse_casm_definition(casm_definition, sierra_version)?;
            Some(ClassInfo::new(
                &contract_class,
                class_definition.sierra_program.len(),
                class_definition.abi.len(),
                SierraVersion::extract_from_program(&class_definition.sierra_program)?,
            )?)
        }
        TransactionVariant::DeclareV3(tx) => {
            let casm_definition = db_transaction
                .casm_definition(tx.class_hash)?
                .context("Fetching class CASM definition")?;
            let class_definition = db_transaction
                .class_definition(tx.class_hash)?
                .context("Fetching class definition")?;
            let class_definition: SierraContractClass =
                serde_json::from_slice(class_definition.as_slice())
                    .context("Deserializing class definition")?;
            let sierra_version =
                SierraVersion::extract_from_program(&class_definition.sierra_program)?;

            let contract_class =
                pathfinder_executor::parse_casm_definition(casm_definition, sierra_version)?;
            Some(ClassInfo::new(
                &contract_class,
                class_definition.sierra_program.len(),
                class_definition.abi.len(),
                SierraVersion::extract_from_program(&class_definition.sierra_program)?,
            )?)
        }
        TransactionVariant::DeployV0(_)
        | TransactionVariant::DeployV1(_)
        | TransactionVariant::DeployAccountV1(_)
        | TransactionVariant::DeployAccountV3(_)
        | TransactionVariant::InvokeV0(_)
        | TransactionVariant::InvokeV1(_)
        | TransactionVariant::InvokeV3(_)
        | TransactionVariant::L1Handler(_) => None,
    };

    let deployed_address = match &transaction.variant {
        TransactionVariant::DeployAccountV1(tx) => {
            let contract_address = starknet_api::core::ContractAddress(
                PatriciaKey::try_from(tx.contract_address.get().into_starkfelt())
                    .expect("No contract address overflow expected"),
            );

            Some(contract_address)
        }
        TransactionVariant::DeployAccountV3(tx) => {
            let contract_address = starknet_api::core::ContractAddress(
                PatriciaKey::try_from(tx.contract_address.get().into_starkfelt())
                    .expect("No contract address overflow expected"),
            );

            Some(contract_address)
        }
        TransactionVariant::DeclareV0(_)
        | TransactionVariant::DeclareV1(_)
        | TransactionVariant::DeclareV2(_)
        | TransactionVariant::DeclareV3(_)
        | TransactionVariant::DeployV0(_)
        | TransactionVariant::DeployV1(_)
        | TransactionVariant::InvokeV0(_)
        | TransactionVariant::InvokeV1(_)
        | TransactionVariant::InvokeV3(_)
        | TransactionVariant::L1Handler(_) => None,
    };

    let paid_fee_on_l1 = match &transaction.variant {
        TransactionVariant::L1Handler(_) => Some(Fee(1_000_000_000_000)),
        _ => None,
    };

    tracing::trace!(%tx_hash, "Converting transaction");

    let transaction = to_starknet_api_transaction(transaction.variant.clone())?;

    let tx = pathfinder_executor::Transaction::from_api(
        transaction,
        tx_hash,
        class_info,
        paid_fee_on_l1,
        deployed_address,
        pathfinder_executor::AccountTransactionExecutionFlags::default(),
    )?;

    Ok(tx)
}
