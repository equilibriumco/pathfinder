use pathfinder_common::transaction::{
    InvokeTransactionV0,
    InvokeTransactionV1,
    InvokeTransactionV3,
    TransactionVariant,
};
use pathfinder_common::TransactionHash;
use serde::de::Error;
use starknet_gateway_client::GatewayApi;
use starknet_gateway_types::error::SequencerError;

use crate::context::RpcContext;
use crate::types::request::BroadcastedInvokeTransaction;

#[derive(Debug, PartialEq, Eq)]
pub enum Transaction {
    Invoke(BroadcastedInvokeTransaction),
}

impl crate::dto::DeserializeForVersion for Transaction {
    fn deserialize(value: crate::dto::Value) -> Result<Self, serde_json::Error> {
        value.deserialize_map(|value| {
            let tag: String = value.deserialize("type")?;
            if tag != "INVOKE" {
                return Err(serde_json::Error::custom("Invalid transaction type"));
            }
            BroadcastedInvokeTransaction::deserialize(value).map(Self::Invoke)
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Input {
    invoke_transaction: Transaction,
}

impl crate::dto::DeserializeForVersion for Input {
    fn deserialize(value: crate::dto::Value) -> Result<Self, serde_json::Error> {
        value.deserialize_map(|value| {
            Ok(Self {
                invoke_transaction: value.deserialize("invoke_transaction")?,
            })
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Output {
    transaction_hash: TransactionHash,
}

#[derive(Debug)]
pub enum AddInvokeTransactionError {
    InvalidTransactionNonce(String),
    InsufficientResourcesForValidate,
    InsufficientAccountBalance,
    ValidationFailure(String),
    DuplicateTransaction,
    NonAccount,
    UnsupportedTransactionVersion,
    InvalidProof,
    UnexpectedError(String),
    ForwardedError(reqwest::Error),
}

impl PartialEq for AddInvokeTransactionError {
    fn eq(&self, other: &Self) -> bool {
        use AddInvokeTransactionError::*;
        match (self, other) {
            (InvalidTransactionNonce(a), InvalidTransactionNonce(b)) => a == b,
            (InsufficientResourcesForValidate, InsufficientResourcesForValidate) => true,
            (InsufficientAccountBalance, InsufficientAccountBalance) => true,
            (ValidationFailure(a), ValidationFailure(b)) => a == b,
            (DuplicateTransaction, DuplicateTransaction) => true,
            (NonAccount, NonAccount) => true,
            (UnsupportedTransactionVersion, UnsupportedTransactionVersion) => true,
            (InvalidProof, InvalidProof) => true,
            (UnexpectedError(a), UnexpectedError(b)) => a == b,
            (ForwardedError(a), ForwardedError(b)) => a.to_string() == b.to_string(),
            _ => false,
        }
    }
}

impl From<anyhow::Error> for AddInvokeTransactionError {
    fn from(e: anyhow::Error) -> Self {
        AddInvokeTransactionError::UnexpectedError(e.to_string())
    }
}

impl From<AddInvokeTransactionError> for crate::error::ApplicationError {
    fn from(value: AddInvokeTransactionError) -> Self {
        match value {
            AddInvokeTransactionError::InvalidTransactionNonce(data) => {
                Self::InvalidTransactionNonce { data }
            }
            AddInvokeTransactionError::InsufficientResourcesForValidate => {
                Self::InsufficientResourcesForValidate
            }
            AddInvokeTransactionError::InsufficientAccountBalance => {
                Self::InsufficientAccountBalance
            }
            AddInvokeTransactionError::ValidationFailure(error) => Self::ValidationFailure(error),
            AddInvokeTransactionError::DuplicateTransaction => Self::DuplicateTransaction,
            AddInvokeTransactionError::NonAccount => Self::NonAccount,
            AddInvokeTransactionError::UnsupportedTransactionVersion => Self::UnsupportedTxVersion,
            AddInvokeTransactionError::InvalidProof => Self::InvalidProof,
            AddInvokeTransactionError::UnexpectedError(data) => Self::UnexpectedError { data },
            AddInvokeTransactionError::ForwardedError(error) => Self::ForwardedError(error),
        }
    }
}

impl From<SequencerError> for AddInvokeTransactionError {
    fn from(e: SequencerError) -> Self {
        use starknet_gateway_types::error::KnownStarknetErrorCode::{
            DuplicatedTransaction,
            EntryPointNotFound,
            InsufficientAccountBalance,
            InsufficientMaxFee,
            InvalidProof,
            InvalidTransactionNonce,
            InvalidTransactionVersion,
            ValidateFailure,
        };
        match e {
            SequencerError::StarknetError(e) if e.code == DuplicatedTransaction.into() => {
                AddInvokeTransactionError::DuplicateTransaction
            }
            SequencerError::StarknetError(e) if e.code == InsufficientAccountBalance.into() => {
                AddInvokeTransactionError::InsufficientAccountBalance
            }
            SequencerError::StarknetError(e) if e.code == InsufficientMaxFee.into() => {
                AddInvokeTransactionError::InsufficientResourcesForValidate
            }
            SequencerError::StarknetError(e) if e.code == InvalidTransactionNonce.into() => {
                AddInvokeTransactionError::InvalidTransactionNonce(e.message)
            }
            SequencerError::StarknetError(e) if e.code == ValidateFailure.into() => {
                if e.message.contains("Invalid transaction nonce") {
                    AddInvokeTransactionError::InvalidTransactionNonce(e.message)
                } else {
                    AddInvokeTransactionError::ValidationFailure(e.message)
                }
            }
            SequencerError::StarknetError(e) if e.code == InvalidTransactionVersion.into() => {
                AddInvokeTransactionError::UnsupportedTransactionVersion
            }
            SequencerError::StarknetError(e) if e.code == EntryPointNotFound.into() => {
                AddInvokeTransactionError::NonAccount
            }
            SequencerError::StarknetError(e) if e.code == InvalidProof.into() => {
                // Technically specific to JSON-RPC version >= 0.10,
                // but for the earlier versions this error shouldn't
                // occur, since there is no proof in the first place.
                AddInvokeTransactionError::InvalidProof
            }
            SequencerError::ReqwestError(e)
                if e.status() == Some(reqwest::StatusCode::PAYLOAD_TOO_LARGE) =>
            {
                AddInvokeTransactionError::ForwardedError(e)
            }
            _ => AddInvokeTransactionError::UnexpectedError(e.to_string()),
        }
    }
}

pub async fn add_invoke_transaction(
    context: RpcContext,
    input: Input,
) -> Result<Output, AddInvokeTransactionError> {
    let Transaction::Invoke(tx) = input.invoke_transaction;
    let (transaction_hash, variant) = add_invoke_transaction_impl(&context, tx).await?;
    context.submission_tracker.insert(
        transaction_hash,
        super::get_latest_block_or_genesis(&context.storage)?,
        variant,
    );
    Ok(Output { transaction_hash })
}

pub(crate) async fn add_invoke_transaction_impl(
    context: &RpcContext,
    tx: BroadcastedInvokeTransaction,
) -> Result<(TransactionHash, TransactionVariant), SequencerError> {
    use starknet_gateway_types::request::add_transaction;

    let success = match tx {
        BroadcastedInvokeTransaction::V0(tx) => {
            let response = context
                .sequencer
                .add_invoke_transaction(add_transaction::InvokeFunction::V0(
                    add_transaction::InvokeFunctionV0V1 {
                        max_fee: tx.max_fee,
                        signature: &tx.signature,
                        nonce: None,
                        sender_address: tx.contract_address,
                        entry_point_selector: Some(tx.entry_point_selector),
                        calldata: &tx.calldata,
                    },
                ))
                .await?;
            let new_tx = InvokeTransactionV0 {
                calldata: tx.calldata,
                sender_address: tx.contract_address,
                entry_point_selector: tx.entry_point_selector,
                entry_point_type: None,
                max_fee: tx.max_fee,
                signature: tx.signature,
            };
            (
                response.transaction_hash,
                TransactionVariant::InvokeV0(new_tx),
            )
        }
        BroadcastedInvokeTransaction::V1(tx) => {
            let response = context
                .sequencer
                .add_invoke_transaction(add_transaction::InvokeFunction::V1(
                    add_transaction::InvokeFunctionV0V1 {
                        max_fee: tx.max_fee,
                        signature: &tx.signature,
                        nonce: Some(tx.nonce),
                        sender_address: tx.sender_address,
                        entry_point_selector: None,
                        calldata: &tx.calldata,
                    },
                ))
                .await?;
            let new_tx = InvokeTransactionV1 {
                calldata: tx.calldata,
                sender_address: tx.sender_address,
                max_fee: tx.max_fee,
                signature: tx.signature,
                nonce: tx.nonce,
            };
            (
                response.transaction_hash,
                TransactionVariant::InvokeV1(new_tx),
            )
        }
        BroadcastedInvokeTransaction::V3(tx) => {
            let response = context
                .sequencer
                .add_invoke_transaction(add_transaction::InvokeFunction::V3(
                    add_transaction::InvokeFunctionV3 {
                        signature: &tx.signature,
                        nonce: tx.nonce,
                        nonce_data_availability_mode: tx.nonce_data_availability_mode.into(),
                        fee_data_availability_mode: tx.fee_data_availability_mode.into(),
                        resource_bounds: tx.resource_bounds.into(),
                        tip: tx.tip,
                        paymaster_data: &tx.paymaster_data,
                        sender_address: tx.sender_address,
                        calldata: &tx.calldata,
                        account_deployment_data: &tx.account_deployment_data,
                        proof_facts: &tx.proof_facts,
                        proof: &tx.proof,
                    },
                ))
                .await?;
            let new_tx = InvokeTransactionV3 {
                signature: tx.signature,
                nonce: tx.nonce,
                nonce_data_availability_mode: tx.nonce_data_availability_mode,
                fee_data_availability_mode: tx.fee_data_availability_mode,
                resource_bounds: tx.resource_bounds,
                tip: tx.tip,
                paymaster_data: tx.paymaster_data,
                account_deployment_data: tx.account_deployment_data,
                calldata: tx.calldata,
                sender_address: tx.sender_address,
                proof_facts: tx.proof_facts,
            };
            (
                response.transaction_hash,
                TransactionVariant::InvokeV3(new_tx),
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
        serializer.end()
    }
}

#[cfg(test)]
mod tests {
    use pathfinder_common::macro_prelude::*;
    use pathfinder_common::transaction::{DataAvailabilityMode, ResourceBound, ResourceBounds};
    use pathfinder_common::{Proof, ResourceAmount, ResourcePricePerUnit, Tip, TransactionVersion};
    use starknet_gateway_types::error::{test_response_from, KnownStarknetErrorCode};
    use wiremock::{matchers, Mock, MockServer, ResponseTemplate};

    use super::*;

    fn v3_input() -> Input {
        Input {
            invoke_transaction: Transaction::Invoke(BroadcastedInvokeTransaction::V3(
                crate::types::request::BroadcastedInvokeTransactionV3 {
                    version: TransactionVersion::THREE,
                    signature: vec![],
                    nonce: transaction_nonce!("0x8a9"),
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
                    sender_address: contract_address!(
                        "0x3f6f3bc663aedc5285d6013cc3ffcbc4341d86ab488b8b68d297f8258793c41"
                    ),
                    calldata: vec![],
                    proof_facts: vec![],
                    proof: Proof::default(),
                },
            )),
        }
    }

    mod parsing {
        mod v3 {
            use serde_json::json;

            use super::super::*;
            use crate::dto::DeserializeForVersion;

            fn test_txn() -> Transaction {
                Transaction::Invoke(BroadcastedInvokeTransaction::V3(
                    crate::types::request::BroadcastedInvokeTransactionV3 {
                        version: TransactionVersion::THREE,
                        signature: vec![],
                        nonce: transaction_nonce!("0x8a9"),
                        resource_bounds: ResourceBounds {
                            l1_gas: ResourceBound {
                                max_amount: ResourceAmount(0x186a0),
                                max_price_per_unit: ResourcePricePerUnit(0x5af3107a4000),
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
                        account_deployment_data: vec![],
                        nonce_data_availability_mode: DataAvailabilityMode::L1,
                        fee_data_availability_mode: DataAvailabilityMode::L1,
                        sender_address: contract_address!(
                            "0x3f6f3bc663aedc5285d6013cc3ffcbc4341d86ab488b8b68d297f8258793c41"
                        ),
                        calldata: vec![],
                        proof_facts: vec![],
                        proof: Proof::default(),
                    },
                ))
            }

            fn test_txn_json() -> serde_json::Value {
                json!({
                    "type": "INVOKE",
                    "version": "0x3",
                    "signature": [],
                    "nonce": "0x8a9",
                    "resource_bounds": {
                        "l1_gas": {
                            "max_amount": "0x186a0",
                            "max_price_per_unit": "0x5af3107a4000"
                        },
                        "l2_gas": {
                            "max_amount": "0x0",
                            "max_price_per_unit": "0x0"
                        },
                        "l1_data_gas": {
                            "max_amount": "0x0",
                            "max_price_per_unit": "0x0"
                        }
                    },
                    "tip": "0x0",
                    "paymaster_data": [],
                    "account_deployment_data": [],
                    "nonce_data_availability_mode": "L1",
                    "fee_data_availability_mode": "L1",
                    "sender_address": "0x3f6f3bc663aedc5285d6013cc3ffcbc4341d86ab488b8b68d297f8258793c41",
                    "calldata": []
                })
            }

            #[test]
            fn positional_args() {
                let positional = json!([test_txn_json()]);
                let input =
                    Input::deserialize(crate::dto::Value::new(positional, crate::RpcVersion::V09))
                        .unwrap();
                let expected = Input {
                    invoke_transaction: test_txn(),
                };
                pretty_assertions_sorted::assert_eq!(input, expected);
            }

            #[test]
            fn named_args() {
                let named = json!({ "invoke_transaction": test_txn_json() });
                let input =
                    Input::deserialize(crate::dto::Value::new(named, crate::RpcVersion::V09))
                        .unwrap();
                let expected = Input {
                    invoke_transaction: test_txn(),
                };
                pretty_assertions_sorted::assert_eq!(input, expected);
            }
        }
    }

    #[rstest::rstest]
    #[case(
        KnownStarknetErrorCode::DuplicatedTransaction,
        "",
        AddInvokeTransactionError::DuplicateTransaction
    )]
    #[case(
        KnownStarknetErrorCode::InsufficientAccountBalance,
        "",
        AddInvokeTransactionError::InsufficientAccountBalance
    )]
    #[case(
        KnownStarknetErrorCode::InsufficientMaxFee,
        "",
        AddInvokeTransactionError::InsufficientResourcesForValidate
    )]
    #[case(
        KnownStarknetErrorCode::InvalidTransactionNonce,
        "invalid nonce",
        AddInvokeTransactionError::InvalidTransactionNonce("invalid nonce".to_owned())
    )]
    #[case(
        KnownStarknetErrorCode::ValidateFailure,
        "validation failed",
        AddInvokeTransactionError::ValidationFailure("validation failed".to_owned())
    )]
    #[case(
        KnownStarknetErrorCode::ValidateFailure,
        "Invalid transaction nonce. Expected: 1, got: 2",
        AddInvokeTransactionError::InvalidTransactionNonce(
            "Invalid transaction nonce. Expected: 1, got: 2".to_owned()
        )
    )]
    #[case(
        KnownStarknetErrorCode::InvalidTransactionVersion,
        "",
        AddInvokeTransactionError::UnsupportedTransactionVersion
    )]
    #[case(
        KnownStarknetErrorCode::EntryPointNotFound,
        "",
        AddInvokeTransactionError::NonAccount
    )]
    #[case(
        KnownStarknetErrorCode::InvalidProof,
        "",
        AddInvokeTransactionError::InvalidProof
    )]
    #[case(
        KnownStarknetErrorCode::InvalidProgram,
        "invalid program",
        AddInvokeTransactionError::UnexpectedError("invalid program".to_owned())
    )]
    #[test_log::test(tokio::test)]
    async fn e2e_error_mapping(
        #[case] mock_error_code: KnownStarknetErrorCode,
        #[case] message: &str,
        #[case] expected_error: AddInvokeTransactionError,
    ) {
        let (body, code) = test_response_from(mock_error_code, message);
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

        let actual_error = add_invoke_transaction(context, v3_input())
            .await
            .unwrap_err();
        assert_eq!(actual_error, expected_error);
    }
}
