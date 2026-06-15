use anyhow::Context;
use pathfinder_common::{L1TransactionHash, TransactionHash};

use crate::context::RpcContext;
use crate::dto::TxnExecutionStatus;
use crate::method::get_transaction_status;
use crate::RpcVersion;

#[derive(Debug, PartialEq, Eq)]
pub struct Input {
    transaction_hash: L1TransactionHash,
}

impl crate::dto::DeserializeForVersion for Input {
    fn deserialize(value: crate::dto::Value) -> Result<Self, serde_json::Error> {
        value.deserialize_map(|value| {
            Ok(Self {
                transaction_hash: value
                    .deserialize("transaction_hash")
                    .map(L1TransactionHash::new)?,
            })
        })
    }
}

#[derive(Clone, Debug)]
enum FinalityStatus {
    Received,
    PreConfirmed,
    AcceptedOnL2,
    AcceptedOnL1,
}

impl crate::dto::SerializeForVersion for FinalityStatus {
    fn serialize(
        &self,
        serializer: crate::dto::Serializer,
    ) -> Result<crate::dto::Ok, crate::dto::Error> {
        let status_str = match self {
            FinalityStatus::Received => "RECEIVED",
            FinalityStatus::PreConfirmed => "PRE_CONFIRMED",
            FinalityStatus::AcceptedOnL2 => "ACCEPTED_ON_L2",
            FinalityStatus::AcceptedOnL1 => "ACCEPTED_ON_L1",
        };
        serializer.serialize_str(status_str)
    }
}

#[derive(Clone, Debug)]
pub struct L1HandlerTransactionStatus {
    transaction_hash: TransactionHash,
    finality_status: FinalityStatus,
    execution_status: Option<TxnExecutionStatus>,
    failure_reason: Option<String>,
}

#[derive(Debug)]
pub struct Output(Vec<L1HandlerTransactionStatus>);

crate::error::generate_rpc_error_subset!(Error: TxnHashNotFound);

pub async fn get_messages_status(
    context: RpcContext,
    input: Input,
    rpc_version: RpcVersion,
) -> Result<Output, Error> {
    let span = tracing::Span::current();

    let _g = span.enter();

    // Fetch the L1 handler transactions for the given transaction hash
    let ethereum = context.ethereum.clone();

    let l1_handler_txs = ethereum
        .get_l1_handler_txs(
            &context.contract_addresses.l1_contract_address,
            &input.transaction_hash,
        )
        .await
        .context("Fetching L1 handler tx hashes")
        .map_err(|_| Error::TxnHashNotFound)?;

    get_messages_status_impl(context, rpc_version, l1_handler_txs).await
}

async fn get_messages_status_impl(
    context: RpcContext,
    rpc_version: RpcVersion,
    l1_handler_txs: Vec<pathfinder_common::transaction::L1HandlerTransaction>,
) -> Result<Output, Error> {
    let mut res = vec![];
    for tx in l1_handler_txs {
        let tx_hash = tx.calculate_hash(context.chain_id);

        let input = get_transaction_status::Input::new(tx_hash);
        let status = get_transaction_status(context.clone(), input, rpc_version)
            .await
            .map_err(|_| Error::TxnHashNotFound)?;

        use get_transaction_status::Output as TxStatus;
        let (finality_status, execution_status) = match status {
            // Since Starknet 0.14, get_transaction_status isn't supposed to return Received or
            // Rejected for L1 handler transactions; the cases are kept for backwards compatibility,
            // more explicit error handling can be added if/when they actually happen. Moreover
            // Transaction finality status Rejected has been removed in RPC v0.9.
            TxStatus::Received => (FinalityStatus::Received, None),
            TxStatus::PreConfirmed(ref exec_status) => {
                (FinalityStatus::PreConfirmed, Some(exec_status.clone()))
            }
            TxStatus::AcceptedOnL1(ref exec_status) => {
                (FinalityStatus::AcceptedOnL1, Some(exec_status.clone()))
            }
            TxStatus::AcceptedOnL2(ref exec_status) => {
                (FinalityStatus::AcceptedOnL2, Some(exec_status.clone()))
            }
        };

        let failure_reason = match &execution_status {
            Some(TxnExecutionStatus::Reverted { reason }) => reason.clone(),
            _ => None,
        };

        if execution_status.is_none() {
            continue; // Skip if execution status is not available, since it's
                      // required for V09+
        }

        res.push(L1HandlerTransactionStatus {
            transaction_hash: tx.calculate_hash(context.chain_id),
            finality_status,
            execution_status,
            failure_reason,
        });
    }

    Ok(Output(res))
}

impl crate::dto::SerializeForVersion for Output {
    fn serialize(
        &self,
        serializer: crate::dto::Serializer,
    ) -> Result<crate::dto::Ok, crate::dto::Error> {
        serializer.serialize_iter(self.0.len(), &mut self.0.clone().into_iter())
    }
}

impl crate::dto::SerializeForVersion for L1HandlerTransactionStatus {
    fn serialize(
        &self,
        serializer: crate::dto::Serializer,
    ) -> Result<crate::dto::Ok, crate::dto::Error> {
        let mut serializer = serializer.serialize_struct()?;
        serializer.serialize_field("transaction_hash", &self.transaction_hash)?;
        serializer.serialize_field("finality_status", &self.finality_status)?;
        serializer.serialize_optional("execution_status", self.execution_status.clone())?;
        serializer.serialize_optional("failure_reason", self.failure_reason.clone())?;
        serializer.end()
    }
}

#[cfg(test)]
mod tests {
    use pathfinder_common::macro_prelude::*;
    use pathfinder_common::receipt::{ExecutionStatus, Receipt};
    use pathfinder_common::transaction::{L1HandlerTransaction, Transaction, TransactionVariant};
    use pathfinder_common::{BlockId, BlockTimestamp, L1TransactionHash, TransactionNonce};
    use pathfinder_crypto::Felt;
    use primitive_types::H256;
    use serde_json::json;

    use super::*;
    use crate::dto::{DeserializeForVersion, SerializeForVersion, Serializer};

    mod parsing {
        use super::*;

        #[test]
        fn positional_args() {
            let positional_json = json!(["0x1234"]);
            let positional = crate::dto::Value::new(positional_json, RpcVersion::V09);

            let input = Input::deserialize(positional).unwrap();

            assert_eq!(
                input,
                Input {
                    transaction_hash: L1TransactionHash::new(H256::from_low_u64_be(0x1234)),
                }
            );
        }

        #[test]
        fn named_args() {
            let named_args_json = json!({
                "transaction_hash": "0x1234",
            });
            let named = crate::dto::Value::new(named_args_json, RpcVersion::V09);

            let input = Input::deserialize(named).unwrap();

            assert_eq!(
                input,
                Input {
                    transaction_hash: L1TransactionHash::new(H256::from_low_u64_be(0x1234)),
                }
            );
        }
    }

    #[rstest::rstest]
    #[case::received(FinalityStatus::Received, "RECEIVED")]
    #[case::pre_confirmed(FinalityStatus::PreConfirmed, "PRE_CONFIRMED")]
    #[case::accepted_on_l2(FinalityStatus::AcceptedOnL2, "ACCEPTED_ON_L2")]
    #[case::accepted_on_l1(FinalityStatus::AcceptedOnL1, "ACCEPTED_ON_L1")]
    fn finality_status_serialization(#[case] status: FinalityStatus, #[case] expected: &str) {
        let encoded = status.serialize(Default::default()).unwrap();

        assert_eq!(encoded, json!(expected));
    }

    #[test]
    fn output_serialization_v09_includes_execution_status_and_revert_reason() {
        let output = Output(vec![L1HandlerTransactionStatus {
            transaction_hash: transaction_hash!("0x1234"),
            finality_status: FinalityStatus::AcceptedOnL2,
            execution_status: Some(TxnExecutionStatus::Reverted {
                reason: Some("l1 handler reverted".to_owned()),
            }),
            failure_reason: Some("l1 handler reverted".to_owned()),
        }]);

        let encoded = output.serialize(Serializer::new(RpcVersion::V09)).unwrap();

        assert_eq!(
            encoded,
            json!([
                {
                    "transaction_hash": "0x1234",
                    "finality_status": "ACCEPTED_ON_L2",
                    "execution_status": "REVERTED",
                    "failure_reason": "l1 handler reverted",
                }
            ])
        );
    }

    #[test]
    fn output_serialization_v09_omits_absent_failure_reason() {
        let output = Output(vec![L1HandlerTransactionStatus {
            transaction_hash: transaction_hash!("0x1234"),
            finality_status: FinalityStatus::AcceptedOnL1,
            execution_status: Some(TxnExecutionStatus::Succeeded),
            failure_reason: None,
        }]);

        let encoded = output.serialize(Serializer::new(RpcVersion::V09)).unwrap();

        assert_eq!(
            encoded,
            json!([
                {
                    "transaction_hash": "0x1234",
                    "finality_status": "ACCEPTED_ON_L1",
                    "execution_status": "SUCCEEDED",
                }
            ])
        );
    }

    #[rstest::rstest]
    #[case::v09(
        RpcVersion::V09,
        json!([
            {
                "transaction_hash": "0x5fbdcde319efbd314396a673e2883c9e32b4e5240af54e4433e1bdcd53edf8",
                "finality_status": "ACCEPTED_ON_L1",
                "execution_status": "SUCCEEDED",
            },
            {
                "transaction_hash": "0x4dc69e578dfe0792b2f87c060a973bfca28d2a4d76f68dc4a76fbaf031fd3e7",
                "finality_status": "ACCEPTED_ON_L2",
                "execution_status": "REVERTED",
                "failure_reason": "l1 handler reverted",
            }
        ])
    )]
    #[tokio::test]
    async fn get_messages_status_happy_path_differs_by_rpc_version(
        #[case] version: RpcVersion,
        #[case] expected: serde_json::Value,
    ) {
        let accepted_l1_handler = l1_handler_transaction(1);
        let reverted_l1_handler = l1_handler_transaction(2);
        let l1_handler_txs = vec![accepted_l1_handler.clone(), reverted_l1_handler.clone()];
        let context = RpcContext::for_tests();

        seed_l1_handler_transactions(&context, accepted_l1_handler, reverted_l1_handler);

        let output = get_messages_status_impl(context, version, l1_handler_txs)
            .await
            .unwrap();

        let output_json = output.serialize(Serializer::new(version)).unwrap();
        assert_eq!(output_json, expected);
    }

    fn l1_handler_transaction(nonce: u64) -> L1HandlerTransaction {
        L1HandlerTransaction {
            contract_address: contract_address!("0x1234"),
            entry_point_selector: entry_point!("0x5678"),
            nonce: TransactionNonce(Felt::from(nonce)),
            calldata: vec![call_param!("0xdeadbeef"), call_param!("0x1")],
        }
    }

    fn seed_l1_handler_transactions(
        context: &RpcContext,
        accepted_l1_handler: L1HandlerTransaction,
        reverted_l1_handler: L1HandlerTransaction,
    ) {
        let accepted_hash = accepted_l1_handler.calculate_hash(context.chain_id);
        let reverted_hash = reverted_l1_handler.calculate_hash(context.chain_id);

        let accepted_transaction = Transaction {
            hash: accepted_hash,
            variant: TransactionVariant::L1Handler(accepted_l1_handler),
        };
        let reverted_transaction = Transaction {
            hash: reverted_hash,
            variant: TransactionVariant::L1Handler(reverted_l1_handler),
        };

        let accepted_receipt = Receipt {
            transaction_hash: accepted_hash,
            ..Default::default()
        };
        let reverted_receipt = Receipt {
            transaction_hash: reverted_hash,
            execution_status: ExecutionStatus::Reverted {
                reason: "l1 handler reverted".to_owned(),
            },
            ..Default::default()
        };

        let mut db = context.storage.connection().unwrap();
        let db_tx = db.transaction().unwrap();
        let latest_header = db_tx
            .block_header(BlockId::Latest)
            .unwrap()
            .expect("test storage should contain a latest block");

        let accepted_header = latest_header
            .child_builder()
            .timestamp(BlockTimestamp::new_or_panic(3))
            .finalize_with_hash(block_hash_bytes!(b"message status accepted"));
        db_tx.insert_block_header(&accepted_header).unwrap();
        db_tx
            .insert_transaction_data(
                accepted_header.number,
                &[(accepted_transaction, accepted_receipt)],
                Some(&[vec![]]),
            )
            .unwrap();

        let reverted_header = accepted_header
            .child_builder()
            .timestamp(BlockTimestamp::new_or_panic(4))
            .finalize_with_hash(block_hash_bytes!(b"message status reverted"));
        db_tx.insert_block_header(&reverted_header).unwrap();
        db_tx
            .insert_transaction_data(
                reverted_header.number,
                &[(reverted_transaction, reverted_receipt)],
                Some(&[vec![]]),
            )
            .unwrap();

        db_tx
            .update_l1_l2_pointer(Some(accepted_header.number))
            .unwrap();
        db_tx.commit().unwrap();
    }
}
