use pathfinder_common::transaction::{
    DeployAccountTransactionV1,
    DeployAccountTransactionV3,
    TransactionVariant,
};
use pathfinder_common::{ContractAddress, TransactionHash};
use serde::de::Error;
use starknet_gateway_client::GatewayApi;
use starknet_gateway_types::error::{KnownStarknetErrorCode, SequencerError};

use crate::context::RpcContext;
use crate::types::request::{
    BroadcastedDeployAccountTransaction,
    BroadcastedDeployAccountTransactionV1,
};

#[derive(Debug, PartialEq, Eq)]
pub enum Transaction {
    DeployAccount(BroadcastedDeployAccountTransaction),
}

impl crate::dto::DeserializeForVersion for Transaction {
    fn deserialize(value: crate::dto::Value) -> Result<Self, serde_json::Error> {
        value.deserialize_map(|value| {
            let tag: String = value.deserialize("type")?;
            if tag != "DEPLOY_ACCOUNT" {
                return Err(serde_json::Error::custom("Invalid transaction type"));
            }
            BroadcastedDeployAccountTransaction::deserialize(value).map(Self::DeployAccount)
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Input {
    deploy_account_transaction: Transaction,
}

impl Input {
    pub fn is_v3_transaction(&self) -> bool {
        matches!(
            self.deploy_account_transaction,
            Transaction::DeployAccount(BroadcastedDeployAccountTransaction::V3(_))
        )
    }
}
#[cfg(test)]
impl Input {
    pub(crate) fn for_test_with_v1_transaction() -> Self {
        Self {
            deploy_account_transaction: Transaction::DeployAccount(
                BroadcastedDeployAccountTransaction::V1(BroadcastedDeployAccountTransactionV1 {
                    version: pathfinder_common::TransactionVersion::ONE,
                    max_fee: Default::default(),
                    signature: Default::default(),
                    nonce: Default::default(),
                    class_hash: Default::default(),
                    contract_address_salt: Default::default(),
                    constructor_calldata: Default::default(),
                }),
            ),
        }
    }

    pub(crate) fn for_test_with_v3_transaction() -> Self {
        Self {
            deploy_account_transaction: Transaction::DeployAccount(
                BroadcastedDeployAccountTransaction::V3(
                    crate::types::request::BroadcastedDeployAccountTransactionV3 {
                        version: pathfinder_common::TransactionVersion::THREE,
                        signature: Default::default(),
                        nonce: Default::default(),
                        resource_bounds: Default::default(),
                        tip: Default::default(),
                        paymaster_data: Default::default(),
                        nonce_data_availability_mode: Default::default(),
                        fee_data_availability_mode: Default::default(),
                        contract_address_salt: Default::default(),
                        constructor_calldata: Default::default(),
                        class_hash: Default::default(),
                    },
                ),
            ),
        }
    }
}

impl crate::dto::DeserializeForVersion for Input {
    fn deserialize(value: crate::dto::Value) -> Result<Self, serde_json::Error> {
        value.deserialize_map(|value| {
            Ok(Self {
                deploy_account_transaction: value.deserialize("deploy_account_transaction")?,
            })
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Output {
    transaction_hash: TransactionHash,
    contract_address: ContractAddress,
}

#[derive(Debug)]
pub enum AddDeployAccountTransactionError {
    ClassHashNotFound,
    InvalidTransactionNonce(String),
    InsufficientResourcesForValidate,
    InsufficientAccountBalance,
    ValidationFailure(String),
    DuplicateTransaction,
    NonAccount,
    UnsupportedTransactionVersion,
    UnexpectedError(String),
    ForwardedError(reqwest::Error),
}

impl From<anyhow::Error> for AddDeployAccountTransactionError {
    fn from(value: anyhow::Error) -> Self {
        AddDeployAccountTransactionError::UnexpectedError(value.to_string())
    }
}

impl From<AddDeployAccountTransactionError> for crate::error::ApplicationError {
    fn from(value: AddDeployAccountTransactionError) -> Self {
        use AddDeployAccountTransactionError::*;
        match value {
            ClassHashNotFound => Self::ClassHashNotFound,
            InvalidTransactionNonce(data) => Self::InvalidTransactionNonce { data },
            InsufficientResourcesForValidate => Self::InsufficientResourcesForValidate,
            InsufficientAccountBalance => Self::InsufficientAccountBalance,
            ValidationFailure(message) => Self::ValidationFailureV06(message),
            DuplicateTransaction => Self::DuplicateTransaction,
            NonAccount => Self::NonAccount,
            UnsupportedTransactionVersion => Self::UnsupportedTxVersion,
            UnexpectedError(data) => Self::UnexpectedError { data },
            ForwardedError(error) => Self::ForwardedError(error),
        }
    }
}

impl From<SequencerError> for AddDeployAccountTransactionError {
    fn from(e: SequencerError) -> Self {
        use starknet_gateway_types::error::KnownStarknetErrorCode::{
            DuplicatedTransaction,
            EntryPointNotFound,
            InsufficientAccountBalance,
            InsufficientMaxFee,
            InvalidTransactionNonce,
            InvalidTransactionVersion,
            UndeclaredClass,
            ValidateFailure,
        };
        match e {
            SequencerError::StarknetError(e) if e.code == UndeclaredClass.into() => {
                AddDeployAccountTransactionError::ClassHashNotFound
            }
            SequencerError::StarknetError(e) if e.code == DuplicatedTransaction.into() => {
                AddDeployAccountTransactionError::DuplicateTransaction
            }
            SequencerError::StarknetError(e) if e.code == InsufficientAccountBalance.into() => {
                AddDeployAccountTransactionError::InsufficientAccountBalance
            }
            SequencerError::StarknetError(e) if e.code == InsufficientMaxFee.into() => {
                AddDeployAccountTransactionError::InsufficientResourcesForValidate
            }
            SequencerError::StarknetError(e) if e.code == InvalidTransactionNonce.into() => {
                AddDeployAccountTransactionError::InvalidTransactionNonce(e.message)
            }
            SequencerError::StarknetError(e) if e.code == ValidateFailure.into() => {
                if e.message.contains("Invalid transaction nonce") {
                    AddDeployAccountTransactionError::InvalidTransactionNonce(e.message)
                } else {
                    AddDeployAccountTransactionError::ValidationFailure(e.message)
                }
            }
            SequencerError::StarknetError(e) if e.code == InvalidTransactionVersion.into() => {
                AddDeployAccountTransactionError::UnsupportedTransactionVersion
            }
            SequencerError::StarknetError(e) if e.code == EntryPointNotFound.into() => {
                AddDeployAccountTransactionError::NonAccount
            }
            SequencerError::ReqwestError(e)
                if e.status() == Some(reqwest::StatusCode::PAYLOAD_TOO_LARGE) =>
            {
                AddDeployAccountTransactionError::ForwardedError(e)
            }
            _ => AddDeployAccountTransactionError::UnexpectedError(e.to_string()),
        }
    }
}

pub async fn add_deploy_account_transaction(
    context: RpcContext,
    input: Input,
) -> Result<Output, AddDeployAccountTransactionError> {
    let contract_address = match &input.deploy_account_transaction {
        Transaction::DeployAccount(tx) => tx.deployed_contract_address(),
    };
    let Transaction::DeployAccount(tx) = input.deploy_account_transaction;
    let (transaction_hash, variant) = add_deploy_account_transaction_impl(&context, tx).await?;
    context.submission_tracker.insert(
        transaction_hash,
        super::get_latest_block_or_genesis(&context.storage)?,
        variant,
    );
    Ok(Output {
        transaction_hash,
        contract_address,
    })
}

pub(crate) async fn add_deploy_account_transaction_impl(
    context: &RpcContext,
    tx: BroadcastedDeployAccountTransaction,
) -> Result<(TransactionHash, TransactionVariant), SequencerError> {
    use starknet_gateway_types::request::add_transaction;

    let success = match tx {
        BroadcastedDeployAccountTransaction::V1(
            tx @ BroadcastedDeployAccountTransactionV1 { version, .. },
        ) if version.without_query_version() == 0 => {
            let response = context
                .sequencer
                .add_deploy_account(add_transaction::DeployAccount::V0(
                    add_transaction::DeployAccountV0V1 {
                        max_fee: tx.max_fee,
                        signature: &tx.signature,
                        nonce: tx.nonce,
                        class_hash: tx.class_hash,
                        contract_address_salt: tx.contract_address_salt,
                        constructor_calldata: &tx.constructor_calldata,
                    },
                ))
                .await?;
            let new_tx = DeployAccountTransactionV1 {
                contract_address: tx.deployed_contract_address(),
                max_fee: tx.max_fee,
                signature: tx.signature,
                nonce: tx.nonce,
                contract_address_salt: tx.contract_address_salt,
                constructor_calldata: tx.constructor_calldata,
                class_hash: tx.class_hash,
            };
            (
                response.transaction_hash,
                TransactionVariant::DeployAccountV1(new_tx),
            )
        }
        BroadcastedDeployAccountTransaction::V1(
            tx @ BroadcastedDeployAccountTransactionV1 { version, .. },
        ) if version.without_query_version() == 1 => {
            let response = context
                .sequencer
                .add_deploy_account(add_transaction::DeployAccount::V1(
                    add_transaction::DeployAccountV0V1 {
                        max_fee: tx.max_fee,
                        signature: &tx.signature,
                        nonce: tx.nonce,
                        class_hash: tx.class_hash,
                        contract_address_salt: tx.contract_address_salt,
                        constructor_calldata: &tx.constructor_calldata,
                    },
                ))
                .await?;
            let new_tx = DeployAccountTransactionV1 {
                contract_address: tx.deployed_contract_address(),
                max_fee: tx.max_fee,
                signature: tx.signature,
                nonce: tx.nonce,
                contract_address_salt: tx.contract_address_salt,
                constructor_calldata: tx.constructor_calldata,
                class_hash: tx.class_hash,
            };
            (
                response.transaction_hash,
                TransactionVariant::DeployAccountV1(new_tx),
            )
        }
        BroadcastedDeployAccountTransaction::V1(_) => {
            return Err(SequencerError::StarknetError(
                starknet_gateway_types::error::StarknetError {
                    code: KnownStarknetErrorCode::InvalidTransactionVersion.into(),
                    message: "".to_string(),
                },
            ));
        }
        BroadcastedDeployAccountTransaction::V3(tx) => {
            let response = context
                .sequencer
                .add_deploy_account(add_transaction::DeployAccount::V3(
                    add_transaction::DeployAccountV3 {
                        signature: &tx.signature,
                        nonce: tx.nonce,
                        nonce_data_availability_mode: tx.nonce_data_availability_mode.into(),
                        fee_data_availability_mode: tx.fee_data_availability_mode.into(),
                        resource_bounds: tx.resource_bounds.into(),
                        tip: tx.tip,
                        paymaster_data: &tx.paymaster_data,
                        class_hash: tx.class_hash,
                        contract_address_salt: tx.contract_address_salt,
                        constructor_calldata: &tx.constructor_calldata,
                    },
                ))
                .await?;
            let new_tx = DeployAccountTransactionV3 {
                contract_address: tx.deployed_contract_address(),
                signature: tx.signature,
                nonce: tx.nonce,
                nonce_data_availability_mode: tx.nonce_data_availability_mode,
                fee_data_availability_mode: tx.fee_data_availability_mode,
                resource_bounds: tx.resource_bounds,
                tip: tx.tip,
                paymaster_data: tx.paymaster_data,
                contract_address_salt: tx.contract_address_salt,
                constructor_calldata: tx.constructor_calldata,
                class_hash: tx.class_hash,
            };
            (
                response.transaction_hash,
                TransactionVariant::DeployAccountV3(new_tx),
            )
        }
    };
    Ok(success)
}

impl crate::dto::SerializeForVersion for Output {
    fn serialize(
        &self,
        serializer: crate::dto::Serializer,
    ) -> Result<crate::dto::Ok, crate::dto::Error> {
        let mut serializer = serializer.serialize_struct()?;
        serializer.serialize_field("transaction_hash", &self.transaction_hash)?;
        serializer.serialize_field("contract_address", &self.contract_address)?;
        serializer.end()
    }
}

#[cfg(test)]
mod tests {
    use pathfinder_common::macro_prelude::*;
    use pathfinder_common::prelude::*;
    use pathfinder_common::transaction::{DataAvailabilityMode, ResourceBound, ResourceBounds};
    use starknet_gateway_types::error::{test_response_from, KnownStarknetErrorCode};
    use wiremock::{matchers, Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::dto::{SerializeForVersion, Serializer};
    use crate::types::request::BroadcastedDeployAccountTransactionV3;

    const INPUT_JSON: &str = r#"{
        "max_fee": "0xbf391377813",
        "version": "0x1",
        "constructor_calldata": [
            "0677bb1cdc050e8d63855e8743ab6e09179138def390676cc03c484daf112ba1"
        ],
        "signature": [
            "07dd3a55d94a0de6f3d6c104d7e6c88ec719a82f4e2bbc12587c8c187584d3d5",
            "071456dded17015d1234779889d78f3e7c763ddcfd2662b19e7843c7542614f8"
        ],
        "nonce": "0x0",
        "class_hash": "01fac3074c9d5282f0acc5c69a4781a1c711efea5e73c550c5d9fb253cf7fd3d",
        "contract_address_salt": "06d44a6aecb4339e23a9619355f101cf3cb9baec289fcd9fd51486655c1bb8a8",
        "type": "DEPLOY_ACCOUNT"
    }"#;

    #[tokio::test]
    async fn test_parse_input_named() {
        let json: serde_json::Value =
            serde_json::from_str(&format!("{{\"deploy_account_transaction\":{INPUT_JSON}}}"))
                .unwrap();
        let input: Input = crate::dto::Value::new(json, crate::RpcVersion::V07)
            .deserialize()
            .unwrap();

        assert_eq!(input, get_input());
    }

    #[tokio::test]
    async fn test_parse_input_positional() {
        let json: serde_json::Value = serde_json::from_str(&format!("[{INPUT_JSON}]")).unwrap();
        let input: Input = crate::dto::Value::new(json, crate::RpcVersion::V07)
            .deserialize()
            .unwrap();

        assert_eq!(input, get_input());
    }

    #[test]
    fn unexpected_error_message() {
        use starknet_gateway_types::error::{StarknetError, StarknetErrorCode};
        let starknet_error = SequencerError::StarknetError(StarknetError {
            code: StarknetErrorCode::Known(KnownStarknetErrorCode::TransactionLimitExceeded),
            message: "StarkNet Alpha throughput limit reached, please wait a few minutes and try \
                      again."
                .to_string(),
        });

        let error = AddDeployAccountTransactionError::from(starknet_error);
        let error = crate::error::ApplicationError::from(error);
        let error = crate::jsonrpc::RpcError::from(error);
        let error = error
            .serialize(Serializer::new(crate::RpcVersion::V07))
            .unwrap();

        let expected = serde_json::json!({
            "code": 63,
            "message": "An unexpected error occurred",
            "data": "StarkNet Alpha throughput limit reached, please wait a few minutes and try again."
        });

        assert_eq!(error, expected);
    }

    fn get_input() -> Input {
        Input {
            deploy_account_transaction: Transaction::DeployAccount(
                BroadcastedDeployAccountTransaction::V1(BroadcastedDeployAccountTransactionV1 {
                    version: TransactionVersion::ONE,
                    max_fee: fee!("0xbf391377813"),
                    signature: vec![
                        transaction_signature_elem!(
                            "0x07dd3a55d94a0de6f3d6c104d7e6c88ec719a82f4e2bbc12587c8c187584d3d5"
                        ),
                        transaction_signature_elem!(
                            "0x071456dded17015d1234779889d78f3e7c763ddcfd2662b19e7843c7542614f8"
                        ),
                    ],
                    nonce: TransactionNonce::ZERO,

                    contract_address_salt: contract_address_salt!(
                        "0x06d44a6aecb4339e23a9619355f101cf3cb9baec289fcd9fd51486655c1bb8a8"
                    ),
                    constructor_calldata: vec![call_param!(
                        "0x0677bb1cdc050e8d63855e8743ab6e09179138def390676cc03c484daf112ba1"
                    )],
                    class_hash: class_hash!(
                        "0x01fac3074c9d5282f0acc5c69a4781a1c711efea5e73c550c5d9fb253cf7fd3d"
                    ),
                }),
            ),
        }
    }

    #[tokio::test]
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

        let error = add_deploy_account_transaction(context, get_input())
            .await
            .expect_err("add_deploy_account_transaction");
        assert_matches::assert_matches!(
            error,
            AddDeployAccountTransactionError::DuplicateTransaction
        );
    }

    #[tokio::test]
    // https://external.integration.starknet.io/feeder_gateway/get_transaction?transactionHash=0x29fd7881f14380842414cdfdd8d6c0b1f2174f8916edcfeb1ede1eb26ac3ef0
    async fn duplicate_v3_transaction() {
        let (body, code) = test_response_from(KnownStarknetErrorCode::DuplicatedTransaction);
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
            deploy_account_transaction: Transaction::DeployAccount(
                BroadcastedDeployAccountTransaction::V3(BroadcastedDeployAccountTransactionV3 {
                    version: TransactionVersion::THREE,
                    signature: vec![],
                    nonce: transaction_nonce!("0x0"),
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
                    nonce_data_availability_mode: DataAvailabilityMode::L1,
                    fee_data_availability_mode: DataAvailabilityMode::L1,
                    contract_address_salt: contract_address_salt!("0x0"),
                    constructor_calldata: vec![call_param!(
                        "0x5cd65f3d7daea6c63939d659b8473ea0c5cd81576035a4d34e52fb06840196c"
                    )],
                    class_hash: class_hash!(
                        "0x2338634f11772ea342365abd5be9d9dc8a6f44f159ad782fdebd3db5d969738"
                    ),
                }),
            ),
        };

        let error = add_deploy_account_transaction(context, input)
            .await
            .expect_err("add_deploy_account_transaction");
        assert_matches::assert_matches!(
            error,
            AddDeployAccountTransactionError::DuplicateTransaction
        );
    }
}
