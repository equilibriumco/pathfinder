use pathfinder_common::transaction::{
    DeclareTransactionV0V1,
    DeclareTransactionV2,
    DeclareTransactionV3,
    TransactionVariant,
};
use pathfinder_common::{ClassHash, TransactionHash};
use starknet_gateway_client::GatewayApi;
use starknet_gateway_types::error::SequencerError;
use starknet_gateway_types::request::add_transaction::{
    CairoContractDefinition,
    ContractDefinition,
    SierraContractDefinition,
};

use crate::context::RpcContext;
use crate::types::request::BroadcastedDeclareTransaction;

#[derive(Debug)]
pub enum AddDeclareTransactionError {
    ClassAlreadyDeclared,
    InvalidTransactionNonce(String),
    InsufficientResourcesForValidate,
    InsufficientAccountBalance,
    ValidationFailure(String),
    CompilationFailed(String),
    ContractClassSizeIsTooLarge,
    DuplicateTransaction,
    CompiledClassHashMismatch,
    NonAccount,
    UnsupportedTransactionVersion,
    UnsupportedContractClassVersion,
    UnexpectedError(String),
    ForwardedError(reqwest::Error),
}

impl From<AddDeclareTransactionError> for crate::error::ApplicationError {
    fn from(value: AddDeclareTransactionError) -> Self {
        match value {
            AddDeclareTransactionError::ClassAlreadyDeclared => Self::ClassAlreadyDeclared,
            AddDeclareTransactionError::InvalidTransactionNonce(data) => {
                Self::InvalidTransactionNonce { data }
            }
            AddDeclareTransactionError::InsufficientResourcesForValidate => {
                Self::InsufficientResourcesForValidate
            }
            AddDeclareTransactionError::InsufficientAccountBalance => {
                Self::InsufficientAccountBalance
            }
            AddDeclareTransactionError::ValidationFailure(message) => {
                Self::ValidationFailure(message)
            }
            AddDeclareTransactionError::CompilationFailed(data) => Self::CompilationFailed { data },
            AddDeclareTransactionError::ContractClassSizeIsTooLarge => {
                Self::ContractClassSizeIsTooLarge
            }
            AddDeclareTransactionError::DuplicateTransaction => Self::DuplicateTransaction,
            AddDeclareTransactionError::CompiledClassHashMismatch => {
                Self::CompiledClassHashMismatch
            }
            AddDeclareTransactionError::NonAccount => Self::NonAccount,
            AddDeclareTransactionError::UnsupportedTransactionVersion => Self::UnsupportedTxVersion,
            AddDeclareTransactionError::UnsupportedContractClassVersion => {
                Self::UnsupportedContractClassVersion
            }
            AddDeclareTransactionError::UnexpectedError(data) => Self::UnexpectedError { data },
            AddDeclareTransactionError::ForwardedError(error) => Self::ForwardedError(error),
        }
    }
}

impl From<anyhow::Error> for AddDeclareTransactionError {
    fn from(value: anyhow::Error) -> Self {
        AddDeclareTransactionError::UnexpectedError(value.to_string())
    }
}

impl From<SequencerError> for AddDeclareTransactionError {
    fn from(e: SequencerError) -> Self {
        use starknet_gateway_types::error::KnownStarknetErrorCode::{
            ClassAlreadyDeclared,
            CompilationFailed,
            ContractBytecodeSizeTooLarge,
            ContractClassObjectSizeTooLarge,
            DuplicatedTransaction,
            EntryPointNotFound,
            InsufficientAccountBalance,
            InsufficientMaxFee,
            InvalidCompiledClassHash,
            InvalidContractClassVersion,
            InvalidTransactionNonce,
            InvalidTransactionVersion,
            ValidateFailure,
        };
        match e {
            SequencerError::StarknetError(e) if e.code == ClassAlreadyDeclared.into() => {
                AddDeclareTransactionError::ClassAlreadyDeclared
            }
            SequencerError::StarknetError(e) if e.code == CompilationFailed.into() => {
                AddDeclareTransactionError::CompilationFailed(e.message)
            }
            SequencerError::StarknetError(e)
                if e.code == ContractBytecodeSizeTooLarge.into()
                    || e.code == ContractClassObjectSizeTooLarge.into() =>
            {
                AddDeclareTransactionError::ContractClassSizeIsTooLarge
            }
            SequencerError::StarknetError(e) if e.code == DuplicatedTransaction.into() => {
                AddDeclareTransactionError::DuplicateTransaction
            }
            SequencerError::StarknetError(e) if e.code == InsufficientAccountBalance.into() => {
                AddDeclareTransactionError::InsufficientAccountBalance
            }
            SequencerError::StarknetError(e) if e.code == InsufficientMaxFee.into() => {
                AddDeclareTransactionError::InsufficientResourcesForValidate
            }
            SequencerError::StarknetError(e) if e.code == InvalidTransactionNonce.into() => {
                AddDeclareTransactionError::InvalidTransactionNonce(e.message)
            }
            SequencerError::StarknetError(e) if e.code == ValidateFailure.into() => {
                if e.message.contains("Invalid transaction nonce") {
                    AddDeclareTransactionError::InvalidTransactionNonce(e.message)
                } else {
                    AddDeclareTransactionError::ValidationFailure(e.message)
                }
            }
            SequencerError::StarknetError(e) if e.code == InvalidCompiledClassHash.into() => {
                AddDeclareTransactionError::CompiledClassHashMismatch
            }
            SequencerError::StarknetError(e) if e.code == InvalidTransactionVersion.into() => {
                AddDeclareTransactionError::UnsupportedTransactionVersion
            }
            SequencerError::StarknetError(e) if e.code == InvalidContractClassVersion.into() => {
                AddDeclareTransactionError::UnsupportedContractClassVersion
            }
            SequencerError::StarknetError(e) if e.code == EntryPointNotFound.into() => {
                AddDeclareTransactionError::NonAccount
            }
            SequencerError::ReqwestError(e)
                if e.status() == Some(reqwest::StatusCode::PAYLOAD_TOO_LARGE) =>
            {
                AddDeclareTransactionError::ForwardedError(e)
            }
            _ => AddDeclareTransactionError::UnexpectedError(e.to_string()),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Transaction {
    Declare(BroadcastedDeclareTransaction),
}

impl crate::dto::DeserializeForVersion for Transaction {
    fn deserialize(value: crate::dto::Value) -> Result<Self, serde_json::Error> {
        value.deserialize_map(|value| {
            let tag: String = value.deserialize("type")?;
            if tag != "DECLARE" {
                return Err(serde::de::Error::custom("Invalid transaction type"));
            }
            Ok(Self::Declare(BroadcastedDeclareTransaction::deserialize(
                value,
            )?))
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Input {
    declare_transaction: Transaction,
    // An undocumented parameter that we forward to the sequencer API
    // A deploy token is required to deploy contracts on Starknet mainnet only.
    token: Option<String>,
}

impl Input {
    pub fn is_v3_transaction(&self) -> bool {
        matches!(
            self.declare_transaction,
            Transaction::Declare(BroadcastedDeclareTransaction::V3(_))
        )
    }
}

#[cfg(test)]
impl Input {
    pub(crate) fn for_test_with_v0_transaction() -> Self {
        Self {
            declare_transaction: Transaction::Declare(BroadcastedDeclareTransaction::V0(
                crate::types::request::BroadcastedDeclareTransactionV0 {
                    max_fee: Default::default(),
                    version: pathfinder_common::TransactionVersion::ZERO,
                    signature: Default::default(),
                    contract_class: crate::types::class::cairo::CairoContractClass {
                        program: Default::default(),
                        entry_points_by_type:
                            crate::types::class::cairo::entry_point::ContractEntryPoints {
                                constructor: Default::default(),
                                external: Default::default(),
                                l1_handler: Default::default(),
                            },
                        abi: Default::default(),
                    },
                    sender_address: Default::default(),
                },
            )),
            token: None,
        }
    }

    pub(crate) fn for_test_with_v1_transaction() -> Self {
        Self {
            declare_transaction: Transaction::Declare(BroadcastedDeclareTransaction::V1(
                crate::types::request::BroadcastedDeclareTransactionV1 {
                    max_fee: Default::default(),
                    version: pathfinder_common::TransactionVersion::ONE,
                    signature: Default::default(),
                    nonce: Default::default(),
                    contract_class: crate::types::class::cairo::CairoContractClass {
                        program: Default::default(),
                        entry_points_by_type:
                            crate::types::class::cairo::entry_point::ContractEntryPoints {
                                constructor: Default::default(),
                                external: Default::default(),
                                l1_handler: Default::default(),
                            },
                        abi: Default::default(),
                    },
                    sender_address: Default::default(),
                },
            )),
            token: None,
        }
    }

    pub(crate) fn for_test_with_v2_transaction() -> Self {
        Self {
            declare_transaction: Transaction::Declare(BroadcastedDeclareTransaction::V2(
                crate::types::request::BroadcastedDeclareTransactionV2 {
                    max_fee: Default::default(),
                    version: pathfinder_common::TransactionVersion::TWO,
                    signature: Default::default(),
                    nonce: Default::default(),
                    compiled_class_hash: Default::default(),
                    contract_class: crate::types::class::sierra::SierraContractClass {
                        sierra_program: Default::default(),
                        contract_class_version: Default::default(),
                        entry_points_by_type: crate::types::class::sierra::SierraEntryPoints {
                            constructor: Default::default(),
                            external: Default::default(),
                            l1_handler: Default::default(),
                        },
                        abi: Default::default(),
                    },
                    sender_address: Default::default(),
                },
            )),
            token: None,
        }
    }

    pub(crate) fn for_test_with_v3_transaction() -> Self {
        Self {
            declare_transaction: Transaction::Declare(BroadcastedDeclareTransaction::V3(
                crate::types::request::BroadcastedDeclareTransactionV3 {
                    version: pathfinder_common::TransactionVersion::THREE,
                    signature: Default::default(),
                    nonce: Default::default(),
                    resource_bounds: Default::default(),
                    tip: Default::default(),
                    paymaster_data: Default::default(),
                    account_deployment_data: Default::default(),
                    nonce_data_availability_mode: Default::default(),
                    fee_data_availability_mode: Default::default(),
                    compiled_class_hash: Default::default(),
                    contract_class: crate::types::class::sierra::SierraContractClass {
                        sierra_program: Default::default(),
                        contract_class_version: Default::default(),
                        entry_points_by_type: crate::types::class::sierra::SierraEntryPoints {
                            constructor: Default::default(),
                            external: Default::default(),
                            l1_handler: Default::default(),
                        },
                        abi: Default::default(),
                    },
                    sender_address: Default::default(),
                },
            )),
            token: None,
        }
    }
}

impl crate::dto::DeserializeForVersion for Input {
    fn deserialize(value: crate::dto::Value) -> Result<Self, serde_json::Error> {
        value.deserialize_map(|value| {
            let declare_transaction = value.deserialize("declare_transaction")?;
            let token = value.deserialize_optional_serde("token")?;
            Ok(Self {
                declare_transaction,
                token,
            })
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Output {
    transaction_hash: TransactionHash,
    class_hash: ClassHash,
}

pub async fn add_declare_transaction(
    context: RpcContext,
    input: Input,
) -> Result<Output, AddDeclareTransactionError> {
    use starknet_gateway_types::request::add_transaction;

    match input.declare_transaction {
        Transaction::Declare(BroadcastedDeclareTransaction::V0(_)) => {
            Err(AddDeclareTransactionError::UnsupportedTransactionVersion)
        }
        Transaction::Declare(BroadcastedDeclareTransaction::V1(tx)) => {
            let contract_definition: CairoContractDefinition = tx
                .contract_class
                .try_into()
                .map_err(|e| anyhow::anyhow!("Failed to convert contract definition: {e}"))?;

            let response = context
                .sequencer
                .add_declare_transaction(
                    add_transaction::Declare::V1(add_transaction::DeclareV0V1V2 {
                        version: tx.version,
                        max_fee: tx.max_fee,
                        signature: &tx.signature,
                        contract_class: ContractDefinition::Cairo(contract_definition),
                        sender_address: tx.sender_address,
                        nonce: tx.nonce,
                        compiled_class_hash: None,
                    }),
                    input.token,
                )
                .await?;
            let new_tx = DeclareTransactionV0V1 {
                class_hash: response.class_hash,
                max_fee: tx.max_fee,
                nonce: tx.nonce,
                signature: tx.signature,
                sender_address: tx.sender_address,
            };
            context.submission_tracker.insert(
                response.transaction_hash,
                super::get_latest_block_or_genesis(&context.storage)?,
                TransactionVariant::DeclareV1(new_tx),
            );
            Ok(Output {
                transaction_hash: response.transaction_hash,
                class_hash: response.class_hash,
            })
        }
        Transaction::Declare(BroadcastedDeclareTransaction::V2(tx)) => {
            let contract_definition: SierraContractDefinition = tx
                .contract_class
                .try_into()
                .map_err(|e| anyhow::anyhow!("Failed to convert contract definition: {e}"))?;

            let response = context
                .sequencer
                .add_declare_transaction(
                    add_transaction::Declare::V2(add_transaction::DeclareV0V1V2 {
                        version: tx.version,
                        max_fee: tx.max_fee,
                        signature: &tx.signature,
                        contract_class: ContractDefinition::Sierra(contract_definition),
                        sender_address: tx.sender_address,
                        nonce: tx.nonce,
                        compiled_class_hash: Some(tx.compiled_class_hash),
                    }),
                    input.token,
                )
                .await?;
            let new_tx = DeclareTransactionV2 {
                class_hash: response.class_hash,
                max_fee: tx.max_fee,
                nonce: tx.nonce,
                signature: tx.signature,
                sender_address: tx.sender_address,
                compiled_class_hash: tx.compiled_class_hash,
            };
            context.submission_tracker.insert(
                response.transaction_hash,
                super::get_latest_block_or_genesis(&context.storage)?,
                TransactionVariant::DeclareV2(new_tx),
            );
            Ok(Output {
                transaction_hash: response.transaction_hash,
                class_hash: response.class_hash,
            })
        }
        Transaction::Declare(BroadcastedDeclareTransaction::V3(tx)) => {
            let contract_definition: SierraContractDefinition = tx
                .contract_class
                .try_into()
                .map_err(|e| anyhow::anyhow!("Failed to convert contract definition: {e}"))?;

            let response = context
                .sequencer
                .add_declare_transaction(
                    add_transaction::Declare::V3(add_transaction::DeclareV3 {
                        signature: &tx.signature,
                        nonce: tx.nonce,
                        nonce_data_availability_mode: tx.nonce_data_availability_mode.into(),
                        fee_data_availability_mode: tx.fee_data_availability_mode.into(),
                        resource_bounds: tx.resource_bounds.into(),
                        tip: tx.tip,
                        paymaster_data: &tx.paymaster_data,
                        contract_class: contract_definition,
                        compiled_class_hash: tx.compiled_class_hash,
                        sender_address: tx.sender_address,
                        account_deployment_data: &tx.account_deployment_data,
                    }),
                    input.token,
                )
                .await?;
            let new_tx = DeclareTransactionV3 {
                class_hash: response.class_hash,
                nonce: tx.nonce,
                nonce_data_availability_mode: tx.nonce_data_availability_mode,
                fee_data_availability_mode: tx.fee_data_availability_mode,
                resource_bounds: tx.resource_bounds,
                tip: tx.tip,
                paymaster_data: tx.paymaster_data,
                signature: tx.signature,
                account_deployment_data: tx.account_deployment_data,
                sender_address: tx.sender_address,
                compiled_class_hash: tx.compiled_class_hash,
            };
            context.submission_tracker.insert(
                response.transaction_hash,
                super::get_latest_block_or_genesis(&context.storage)?,
                TransactionVariant::DeclareV3(new_tx),
            );
            Ok(Output {
                transaction_hash: response.transaction_hash,
                class_hash: response.class_hash,
            })
        }
    }
}

impl crate::dto::SerializeForVersion for Output {
    fn serialize(
        &self,
        serializer: crate::dto::Serializer,
    ) -> Result<crate::dto::Ok, crate::dto::Error> {
        let mut serializer = serializer.serialize_struct()?;
        serializer.serialize_field("transaction_hash", &self.transaction_hash)?;
        serializer.serialize_field("class_hash", &self.class_hash)?;
        serializer.end()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::LazyLock;

    use pathfinder_common::class_definition::SerializedOpaqueClassDefinition;
    use pathfinder_common::macro_prelude::*;
    use pathfinder_common::prelude::*;
    use pathfinder_common::transaction::{DataAvailabilityMode, ResourceBound, ResourceBounds};
    use pathfinder_crypto::Felt;
    use starknet_gateway_test_fixtures::class_definitions::{
        CAIRO_2_0_0_STACK_OVERFLOW,
        CONTRACT_DEFINITION,
    };
    use starknet_gateway_types::error::{test_response_from, KnownStarknetErrorCode};
    use wiremock::{matchers, Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::types::class::cairo::CairoContractClass;
    use crate::types::class::sierra::SierraContractClass;
    use crate::types::request::{
        BroadcastedDeclareTransaction,
        BroadcastedDeclareTransactionV1,
        BroadcastedDeclareTransactionV2,
        BroadcastedDeclareTransactionV3,
    };
    use crate::types::ContractClass;

    pub static CONTRACT_CLASS: LazyLock<CairoContractClass> = LazyLock::new(|| {
        ContractClass::from_serialized_def(&SerializedOpaqueClassDefinition::from_slice(
            CONTRACT_DEFINITION,
        ))
        .unwrap()
        .as_cairo()
        .unwrap()
    });

    pub static CONTRACT_CLASS_WITH_INVALID_PRIME: LazyLock<CairoContractClass> =
        LazyLock::new(|| {
            let mut definition: serde_json::Value =
                serde_json::from_slice(CONTRACT_DEFINITION).unwrap();
            // change program.prime to an invalid one
            *definition
                .get_mut("program")
                .unwrap()
                .get_mut("prime")
                .unwrap() = serde_json::json!("0x1");
            let definition = serde_json::to_vec(&definition).unwrap();
            ContractClass::from_serialized_def(&SerializedOpaqueClassDefinition::from_slice(
                &definition,
            ))
            .unwrap()
            .as_cairo()
            .unwrap()
        });

    pub static SIERRA_CLASS: LazyLock<SierraContractClass> = LazyLock::new(|| {
        ContractClass::from_serialized_def(&SerializedOpaqueClassDefinition::from_slice(
            CAIRO_2_0_0_STACK_OVERFLOW,
        ))
        .unwrap()
        .as_sierra()
        .unwrap()
    });

    pub static INTEGRATION_SIERRA_CLASS: LazyLock<SierraContractClass> = LazyLock::new(|| {
        ContractClass::from_serialized_def(&SerializedOpaqueClassDefinition::from_slice(
            include_bytes!("../../fixtures/contracts/integration_class_0x5ae9d09292a50ed48c5930904c880dab56e85b825022a7d689cfc9e65e01ee7.json")
        ))
        .unwrap()
        .as_sierra()
        .unwrap()
    });

    mod parsing {
        mod v1 {
            use serde_json::json;

            use super::super::*;
            use crate::dto::{DeserializeForVersion, SerializeForVersion, Serializer};
            use crate::types::request::BroadcastedDeclareTransactionV1;
            use crate::RpcVersion;

            fn test_declare_txn() -> Transaction {
                Transaction::Declare(BroadcastedDeclareTransaction::V1(
                    BroadcastedDeclareTransactionV1 {
                        max_fee: fee!("0x1"),
                        version: TransactionVersion::ONE,
                        signature: vec![],
                        nonce: TransactionNonce(Felt::ZERO),
                        contract_class: CONTRACT_CLASS.clone(),
                        sender_address: ContractAddress::new_or_panic(Felt::from_u64(1)),
                    },
                ))
            }

            #[test]
            fn positional_args() {
                let positional = json!([{
                    "type": "DECLARE",
                    "version": "0x1",
                    "max_fee": "0x1",
                    "signature": [],
                    "nonce": "0x0",
                    "contract_class": CONTRACT_CLASS.clone(),
                    "sender_address": "0x1"
                }]);
                let input = Input::deserialize(crate::dto::Value::new(positional, RpcVersion::V09))
                    .unwrap();
                let expected = Input {
                    declare_transaction: test_declare_txn(),
                    token: None,
                };
                assert_eq!(input, expected);
            }

            #[test]
            fn named_args() {
                let named = json!({
                    "declare_transaction": {
                        "type": "DECLARE",
                        "version": "0x1",
                        "max_fee": "0x1",
                        "signature": [],
                        "nonce": "0x0",
                        "contract_class": CONTRACT_CLASS.clone(),
                        "sender_address": "0x1"
                    },
                    "token": "token"
                });
                let input =
                    Input::deserialize(crate::dto::Value::new(named, RpcVersion::V09)).unwrap();
                let expected = Input {
                    declare_transaction: test_declare_txn(),
                    token: Some("token".to_owned()),
                };
                assert_eq!(input, expected);
            }

            #[test]
            fn unexpected_error_message() {
                use starknet_gateway_types::error::{
                    KnownStarknetErrorCode,
                    StarknetError,
                    StarknetErrorCode,
                };
                let starknet_error = SequencerError::StarknetError(StarknetError {
                    code: StarknetErrorCode::Known(
                        KnownStarknetErrorCode::TransactionLimitExceeded,
                    ),
                    message: "StarkNet Alpha throughput limit reached, please wait a few minutes \
                              and try again."
                        .to_string(),
                });

                let error = AddDeclareTransactionError::from(starknet_error);
                let error = crate::error::ApplicationError::from(error);
                let error = crate::jsonrpc::RpcError::from(error);
                let error = error.serialize(Serializer::new(RpcVersion::V09)).unwrap();

                let expected = json!({
                    "code": 63,
                    "message": "An unexpected error occurred",
                    "data": "StarkNet Alpha throughput limit reached, please wait a few minutes and try again."
                });

                assert_eq!(error, expected);
            }
        }

        mod v2 {
            use serde_json::json;

            use super::super::*;
            use crate::dto::DeserializeForVersion;
            use crate::types::request::BroadcastedDeclareTransactionV2;
            use crate::RpcVersion;

            fn test_declare_txn() -> Transaction {
                Transaction::Declare(BroadcastedDeclareTransaction::V2(
                    BroadcastedDeclareTransactionV2 {
                        max_fee: fee!("0x1"),
                        version: TransactionVersion::TWO,
                        signature: vec![],
                        nonce: TransactionNonce(Felt::ZERO),
                        contract_class: SIERRA_CLASS.clone(),
                        sender_address: ContractAddress::new_or_panic(Felt::from_u64(1)),
                        compiled_class_hash: CasmHash(Felt::from_u64(1)),
                    },
                ))
            }

            #[test]
            fn positional_args() {
                let positional = json!([{
                    "type": "DECLARE",
                    "version": "0x2",
                    "max_fee": "0x1",
                    "signature": [],
                    "nonce": "0x0",
                    "contract_class": SIERRA_CLASS.clone(),
                    "sender_address": "0x1",
                    "compiled_class_hash": "0x1"
                }]);

                let input = Input::deserialize(crate::dto::Value::new(positional, RpcVersion::V09))
                    .unwrap();
                let expected = Input {
                    declare_transaction: test_declare_txn(),
                    token: None,
                };
                pretty_assertions_sorted::assert_eq!(input, expected);
            }

            #[test]
            fn named_args() {
                let named = json!({
                    "declare_transaction": {
                        "type": "DECLARE",
                        "version": "0x2",
                        "max_fee": "0x1",
                        "signature": [],
                        "nonce": "0x0",
                        "contract_class": SIERRA_CLASS.clone(),
                        "sender_address": "0x1",
                        "compiled_class_hash": "0x1"
                    },
                    "token": "token"
                });

                let input =
                    Input::deserialize(crate::dto::Value::new(named, RpcVersion::V09)).unwrap();
                let expected = Input {
                    declare_transaction: test_declare_txn(),
                    token: Some("token".to_owned()),
                };
                pretty_assertions_sorted::assert_eq!(input, expected);
            }
        }
    }

    #[test_log::test(tokio::test)]
    async fn invalid_contract_definition_v1() {
        let (body, code) = test_response_from(KnownStarknetErrorCode::InvalidContractDefinition);
        let server = MockServer::start().await;
        Mock::given(matchers::method("POST"))
            .and(matchers::path("/gateway/add_transaction"))
            .respond_with(ResponseTemplate::new(code).set_body_string(body))
            .mount(&server)
            .await;
        let mut context = RpcContext::for_tests();
        context.sequencer =
            starknet_gateway_client::Client::for_test(server.uri().parse().unwrap())
                .unwrap()
                .disable_retry_for_tests();

        let input = Input {
            declare_transaction: Transaction::Declare(BroadcastedDeclareTransaction::V1(
                BroadcastedDeclareTransactionV1 {
                    version: TransactionVersion::ONE,
                    max_fee: Fee(Default::default()),
                    signature: vec![],
                    nonce: TransactionNonce(Default::default()),
                    contract_class: CONTRACT_CLASS.clone(),
                    sender_address: ContractAddress::new_or_panic(Felt::from_u64(1)),
                },
            )),
            token: None,
        };
        let error = add_declare_transaction(context, input).await.unwrap_err();
        assert_matches::assert_matches!(error, AddDeclareTransactionError::UnexpectedError(_));
    }

    #[test_log::test(tokio::test)]
    async fn invalid_contract_definition_v2() {
        let (body, code) = test_response_from(KnownStarknetErrorCode::InvalidContractDefinition);
        let server = MockServer::start().await;
        Mock::given(matchers::method("POST"))
            .and(matchers::path("/gateway/add_transaction"))
            .respond_with(ResponseTemplate::new(code).set_body_string(body))
            .mount(&server)
            .await;
        let mut context = RpcContext::for_tests_on(pathfinder_common::Chain::SepoliaIntegration);
        context.sequencer =
            starknet_gateway_client::Client::for_test(server.uri().parse().unwrap())
                .unwrap()
                .disable_retry_for_tests();

        let input = Input {
            declare_transaction: Transaction::Declare(BroadcastedDeclareTransaction::V2(
                BroadcastedDeclareTransactionV2 {
                    version: TransactionVersion::TWO,
                    max_fee: Fee(Felt::from_be_slice(&u64::MAX.to_be_bytes()).unwrap()),
                    signature: vec![],
                    nonce: TransactionNonce(Default::default()),
                    contract_class: SIERRA_CLASS.clone(),
                    sender_address: ContractAddress::new_or_panic(Felt::from_u64(1)),
                    compiled_class_hash: casm_hash!(
                        "0x711c0c3e56863e29d3158804aac47f424241eda64db33e2cc2999d60ee5105"
                    ),
                },
            )),
            token: None,
        };
        let error = add_declare_transaction(context, input).await.unwrap_err();
        assert_matches::assert_matches!(error, AddDeclareTransactionError::UnexpectedError(_));
    }

    #[test_log::test(tokio::test)]
    async fn invalid_contract_class() {
        let (body, code) = test_response_from(KnownStarknetErrorCode::InvalidProgram);
        let server = MockServer::start().await;
        Mock::given(matchers::method("POST"))
            .and(matchers::path("/gateway/add_transaction"))
            .respond_with(ResponseTemplate::new(code).set_body_string(body))
            .mount(&server)
            .await;
        let mut context = RpcContext::for_tests();
        context.sequencer =
            starknet_gateway_client::Client::for_test(server.uri().parse().unwrap())
                .unwrap()
                .disable_retry_for_tests();

        let input = Input {
            declare_transaction: Transaction::Declare(BroadcastedDeclareTransaction::V1(
                BroadcastedDeclareTransactionV1 {
                    version: TransactionVersion::ONE,
                    max_fee: fee!("0xfffffffffff"),
                    signature: vec![],
                    nonce: TransactionNonce(Default::default()),
                    contract_class: CONTRACT_CLASS_WITH_INVALID_PRIME.clone(),
                    sender_address: ContractAddress::new_or_panic(Felt::from_u64(1)),
                },
            )),
            token: None,
        };
        let error = add_declare_transaction(context, input).await.unwrap_err();
        assert_matches::assert_matches!(error, AddDeclareTransactionError::UnexpectedError(_));
    }

    #[test_log::test(tokio::test)]
    async fn duplicate_transaction() {
        let (body, code) = test_response_from(KnownStarknetErrorCode::DuplicatedTransaction);
        let server = MockServer::start().await;
        Mock::given(matchers::method("POST"))
            .and(matchers::path("/gateway/add_transaction"))
            .respond_with(ResponseTemplate::new(code).set_body_string(body))
            .mount(&server)
            .await;
        let mut context = RpcContext::for_tests();
        context.sequencer =
            starknet_gateway_client::Client::for_test(server.uri().parse().unwrap())
                .unwrap()
                .disable_retry_for_tests();

        let input = Input {
            declare_transaction: Transaction::Declare(BroadcastedDeclareTransaction::V1(
                BroadcastedDeclareTransactionV1 {
                    version: TransactionVersion::ONE,
                    max_fee: Fee(Felt::from_be_slice(&u64::MAX.to_be_bytes()).unwrap()),
                    signature: vec![],
                    nonce: TransactionNonce(Default::default()),
                    contract_class: CONTRACT_CLASS.clone(),
                    sender_address: ContractAddress::new_or_panic(Felt::from_u64(1)),
                },
            )),
            token: None,
        };
        let error = add_declare_transaction(context, input).await.unwrap_err();
        assert_matches::assert_matches!(error, AddDeclareTransactionError::DuplicateTransaction);
    }

    #[test_log::test(tokio::test)]
    async fn insufficient_max_fee() {
        let (body, code) = test_response_from(KnownStarknetErrorCode::InsufficientAccountBalance);
        let server = MockServer::start().await;
        Mock::given(matchers::method("POST"))
            .and(matchers::path("/gateway/add_transaction"))
            .respond_with(ResponseTemplate::new(code).set_body_string(body))
            .mount(&server)
            .await;
        let mut context = RpcContext::for_tests_on(pathfinder_common::Chain::SepoliaIntegration);
        context.sequencer =
            starknet_gateway_client::Client::for_test(server.uri().parse().unwrap())
                .unwrap()
                .disable_retry_for_tests();

        let input = Input {
            declare_transaction: Transaction::Declare(BroadcastedDeclareTransaction::V2(
                BroadcastedDeclareTransactionV2 {
                    version: TransactionVersion::TWO,
                    max_fee: Fee(felt!("0x01")),
                    signature: vec![],
                    nonce: TransactionNonce(Default::default()),
                    contract_class: SIERRA_CLASS.clone(),
                    sender_address: ContractAddress::new_or_panic(Felt::from_u64(1)),
                    compiled_class_hash: casm_hash!(
                        "0x688e44b1d8612222a25cf742c8e1493af4640fa74b1a7707bde2002df51ea8c"
                    ),
                },
            )),
            token: None,
        };
        let err = add_declare_transaction(context, input).await.unwrap_err();
        assert_matches::assert_matches!(
            err,
            AddDeclareTransactionError::InsufficientAccountBalance
        );
    }

    #[test_log::test(tokio::test)]
    async fn insufficient_account_balance() {
        let (body, code) = test_response_from(KnownStarknetErrorCode::InsufficientAccountBalance);
        let server = MockServer::start().await;
        Mock::given(matchers::method("POST"))
            .and(matchers::path("/gateway/add_transaction"))
            .respond_with(ResponseTemplate::new(code).set_body_string(body))
            .mount(&server)
            .await;
        let mut context = RpcContext::for_tests_on(pathfinder_common::Chain::SepoliaIntegration);
        context.sequencer =
            starknet_gateway_client::Client::for_test(server.uri().parse().unwrap())
                .unwrap()
                .disable_retry_for_tests();

        let input = Input {
            declare_transaction: Transaction::Declare(BroadcastedDeclareTransaction::V2(
                BroadcastedDeclareTransactionV2 {
                    version: TransactionVersion::TWO,
                    max_fee: Fee(Felt::from_be_slice(&u64::MAX.to_be_bytes()).unwrap()),
                    signature: vec![],
                    nonce: TransactionNonce(Default::default()),
                    contract_class: SIERRA_CLASS.clone(),
                    sender_address: ContractAddress::new_or_panic(Felt::from_u64(1)),
                    compiled_class_hash: casm_hash!(
                        "0x688e44b1d8612222a25cf742c8e1493af4640fa74b1a7707bde2002df51ea8c"
                    ),
                },
            )),
            token: None,
        };
        let err = add_declare_transaction(context, input).await.unwrap_err();
        assert_matches::assert_matches!(
            err,
            AddDeclareTransactionError::InsufficientAccountBalance
        );
    }

    #[tokio::test]
    // https://external.integration.starknet.io/feeder_gateway/get_transaction?transactionHash=0x41d1f5206ef58a443e7d3d1ca073171ec25fa75313394318fc83a074a6631c3
    async fn duplicate_v3_transaction() {
        let (body, code) = test_response_from(KnownStarknetErrorCode::InsufficientAccountBalance);
        let server = MockServer::start().await;
        Mock::given(matchers::method("POST"))
            .and(matchers::path("/gateway/add_transaction"))
            .respond_with(ResponseTemplate::new(code).set_body_string(body))
            .mount(&server)
            .await;
        let mut context = RpcContext::for_tests_on(pathfinder_common::Chain::SepoliaIntegration);
        context.sequencer =
            starknet_gateway_client::Client::for_test(server.uri().parse().unwrap())
                .unwrap()
                .disable_retry_for_tests();

        let input = Input {
            declare_transaction: Transaction::Declare(BroadcastedDeclareTransaction::V3(
                BroadcastedDeclareTransactionV3 {
                    version: TransactionVersion::THREE,
                    signature: vec![],
                    nonce: transaction_nonce!("0x1"),
                    resource_bounds: ResourceBounds {
                        l1_gas: ResourceBound {
                            max_amount: ResourceAmount(0x186a0),
                            max_price_per_unit: ResourcePricePerUnit(0x5af3107a4000),
                        },
                        l2_gas: ResourceBound {
                            max_amount: ResourceAmount(0),
                            max_price_per_unit: ResourcePricePerUnit(0),
                        },
                        l1_data_gas: None,
                    },
                    tip: Tip(0),
                    paymaster_data: vec![],
                    account_deployment_data: vec![],
                    nonce_data_availability_mode: DataAvailabilityMode::L1,
                    fee_data_availability_mode: DataAvailabilityMode::L1,
                    compiled_class_hash: casm_hash!(
                        "0x1add56d64bebf8140f3b8a38bdf102b7874437f0c861ab4ca7526ec33b4d0f8"
                    ),
                    contract_class: INTEGRATION_SIERRA_CLASS.clone(),
                    sender_address: contract_address!(
                        "0x2fab82e4aef1d8664874e1f194951856d48463c3e6bf9a8c68e234a629a6f50"
                    ),
                },
            )),
            token: None,
        };

        let err = add_declare_transaction(context, input).await.unwrap_err();
        assert_matches::assert_matches!(
            err,
            AddDeclareTransactionError::InsufficientAccountBalance
        );
    }
}
