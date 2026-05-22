use anyhow::Context;
use pathfinder_common::event::Event;
use pathfinder_common::receipt::Receipt;
use pathfinder_common::transaction::Transaction;
use pathfinder_common::{BlockHash, BlockNumber, TransactionHash};

use crate::context::RpcContext;
use crate::{dto, RpcVersion};

pub struct Input {
    pub transaction_hash: TransactionHash,
}

impl crate::dto::DeserializeForVersion for Input {
    fn deserialize(value: crate::dto::Value) -> Result<Self, serde_json::Error> {
        value.deserialize_map(|value| {
            Ok(Self {
                transaction_hash: value.deserialize("transaction_hash").map(TransactionHash)?,
            })
        })
    }
}

#[derive(Debug)]
pub enum Output {
    Full {
        block_hash: BlockHash,
        block_number: BlockNumber,
        receipt: Receipt,
        transaction: Transaction,
        events: Vec<Event>,
        finality: dto::TxnFinalityStatus,
    },
    Pending {
        receipt: Receipt,
        block_number: BlockNumber,
        transaction: Transaction,
        events: Vec<Event>,
        finality: dto::TxnFinalityStatus,
    },
}

impl crate::dto::SerializeForVersion for Output {
    fn serialize(
        &self,
        serializer: crate::dto::Serializer,
    ) -> Result<crate::dto::Ok, crate::dto::Error> {
        match self {
            Output::Full {
                block_hash,
                block_number,
                receipt,
                transaction,
                events,
                finality,
            } => dto::TxnReceiptWithBlockInfo {
                block_hash: Some(block_hash),
                block_number: *block_number,
                receipt,
                transaction,
                events,
                finality: *finality,
            },
            Output::Pending {
                receipt,
                block_number,
                transaction,
                events,
                finality,
            } => dto::TxnReceiptWithBlockInfo {
                block_hash: None,
                block_number: *block_number,
                receipt,
                transaction,
                events,
                finality: *finality,
            },
        }
        .serialize(serializer)
    }
}

crate::error::generate_rpc_error_subset!(Error: TxnHashNotFound);

pub async fn get_transaction_receipt(
    context: RpcContext,
    input: Input,
    rpc_version: RpcVersion,
) -> Result<Output, Error> {
    let span = tracing::Span::current();
    util::task::spawn_blocking(move |_| {
        let _g = span.enter();
        let mut db = context
            .storage
            .connection()
            .context("Opening database connection")?;

        let db_tx = db.transaction().context("Creating database transaction")?;

        // Check pending transactions.
        let pending = context
            .pending_data
            .get(&db_tx, rpc_version)
            .context("Querying pending data")?;

        if let Some(finalized_tx_data) =
            crate::pending::find_finalized_tx_data(&pending, input.transaction_hash)
        {
            return Ok(Output::Pending {
                receipt: finalized_tx_data.receipt,
                block_number: finalized_tx_data.block_number,
                transaction: finalized_tx_data.transaction,
                events: finalized_tx_data.events,
                finality: finalized_tx_data.finality_status,
            });
        }

        let (transaction, receipt, events, block_number) = db_tx
            .transaction_with_receipt(input.transaction_hash)
            .context("Reading transaction receipt from database")?
            .ok_or(Error::TxnHashNotFound)?;

        let block_hash = db_tx
            .block_hash(block_number.into())
            .context("Querying block hash")?
            .context("Block hash info missing")?;

        let l1_accepted = db_tx
            .block_is_l1_accepted(block_number.into())
            .context("Querying block status")?;

        let finality = if l1_accepted {
            dto::TxnFinalityStatus::AcceptedOnL1
        } else {
            dto::TxnFinalityStatus::AcceptedOnL2
        };

        Ok(Output::Full {
            transaction,
            receipt,
            events,
            block_hash,
            block_number,
            finality,
        })
    })
    .await
    .context("Joining blocking task")?
}

#[cfg(test)]
mod tests {
    use pathfinder_common::transaction_hash_bytes;

    use super::*;
    use crate::dto::{SerializeForVersion, Serializer};
    use crate::RpcVersion;

    #[rstest::rstest]
    #[case::v06(RpcVersion::V06)]
    #[case::v07(RpcVersion::V07)]
    #[case::v08(RpcVersion::V08)]
    #[case::v09(RpcVersion::V09)]
    #[case::v10(RpcVersion::V10)]
    #[tokio::test]
    async fn l1_accepted(#[case] version: RpcVersion) {
        let context = RpcContext::for_tests();
        // This transaction is in block 1 which is L1 accepted.
        let tx_hash = transaction_hash_bytes!(b"txn 1");
        let input = Input {
            transaction_hash: tx_hash,
        };
        let output = get_transaction_receipt(context, input, version)
            .await
            .unwrap();

        let output_json = output.serialize(Serializer { version }).unwrap();

        crate::assert_json_matches_fixture!(
            output_json,
            version,
            "transactions/receipt_l1_accepted.json"
        );
    }

    #[rstest::rstest]
    #[case::v06(RpcVersion::V06)]
    #[case::v07(RpcVersion::V07)]
    #[case::v08(RpcVersion::V08)]
    #[case::v09(RpcVersion::V09)]
    #[case::v10(RpcVersion::V10)]
    #[tokio::test]
    async fn l2_accepted(#[case] version: RpcVersion) {
        let context = RpcContext::for_tests();
        // This transaction is in block 2 which is L2 accepted.
        let tx_hash = transaction_hash_bytes!(b"txn 3");
        let input = Input {
            transaction_hash: tx_hash,
        };
        let output = get_transaction_receipt(context, input, version)
            .await
            .unwrap();

        let output_json = output.serialize(Serializer { version }).unwrap();

        crate::assert_json_matches_fixture!(
            output_json,
            version,
            "transactions/receipt_l2_accepted.json"
        );
    }

    #[rstest::rstest]
    #[case::v06(RpcVersion::V06)]
    #[case::v07(RpcVersion::V07)]
    #[case::v08(RpcVersion::V08)]
    #[case::v09(RpcVersion::V09)]
    #[case::v10(RpcVersion::V10)]
    #[tokio::test]
    async fn pre_confirmed(#[case] version: RpcVersion) {
        let context = RpcContext::for_tests_with_pre_confirmed().await;
        let tx_hash = transaction_hash_bytes!(b"preconfirmed tx hash 0");
        let input = Input {
            transaction_hash: tx_hash,
        };
        let result = get_transaction_receipt(context, input, version).await;

        match version {
            RpcVersion::PathfinderV01 => unreachable!(),
            RpcVersion::V06 | RpcVersion::V07 | RpcVersion::V08 => {
                assert_matches::assert_matches!(result, Err(Error::TxnHashNotFound));
            }
            RpcVersion::V09 => {
                let output_json = result.unwrap().serialize(Serializer { version }).unwrap();
                let expected_json: serde_json::Value = serde_json::from_str(include_str!(
                    "../../fixtures/0.9.0/transactions/receipt_pre_confirmed.json"
                ))
                .unwrap();
                assert_eq!(output_json, expected_json);
            }
            RpcVersion::V10 => {
                let output_json = result.unwrap().serialize(Serializer { version }).unwrap();
                let expected_json: serde_json::Value = serde_json::from_str(include_str!(
                    "../../fixtures/0.10.0/transactions/receipt_pre_confirmed.json"
                ))
                .unwrap();
                assert_eq!(output_json, expected_json);
            }
        }
    }

    #[rstest::rstest]
    #[case::v06(RpcVersion::V06)]
    #[case::v07(RpcVersion::V07)]
    #[case::v08(RpcVersion::V08)]
    #[case::v09(RpcVersion::V09)]
    #[case::v10(RpcVersion::V10)]
    #[tokio::test]
    async fn pre_latest(#[case] version: RpcVersion) {
        let context = RpcContext::for_tests_with_pre_latest_and_pre_confirmed().await;
        let tx_hash = transaction_hash_bytes!(b"prelatest tx hash 0");
        let input = Input {
            transaction_hash: tx_hash,
        };
        let result = get_transaction_receipt(context, input, version).await;

        match version {
            RpcVersion::PathfinderV01 => unreachable!(),
            RpcVersion::V06 | RpcVersion::V07 | RpcVersion::V08 => {
                assert_matches::assert_matches!(result, Err(Error::TxnHashNotFound));
            }
            RpcVersion::V09 => {
                let output_json = result.unwrap().serialize(Serializer { version }).unwrap();
                let expected_json: serde_json::Value = serde_json::from_str(include_str!(
                    "../../fixtures/0.9.0/transactions/receipt_pre_latest.json"
                ))
                .unwrap();
                assert_eq!(output_json, expected_json);
            }
            RpcVersion::V10 => {
                let output_json = result.unwrap().serialize(Serializer { version }).unwrap();
                let expected_json: serde_json::Value = serde_json::from_str(include_str!(
                    "../../fixtures/0.10.0/transactions/receipt_pre_latest.json"
                ))
                .unwrap();
                assert_eq!(output_json, expected_json);
            }
        }
    }

    #[rstest::rstest]
    #[case::v06(RpcVersion::V06)]
    #[case::v07(RpcVersion::V07)]
    #[case::v08(RpcVersion::V08)]
    #[case::v09(RpcVersion::V09)]
    #[case::v10(RpcVersion::V10)]
    #[tokio::test]
    async fn reverted(#[case] version: RpcVersion) {
        let context = RpcContext::for_tests_with_pre_confirmed().await;
        let input = Input {
            transaction_hash: transaction_hash_bytes!(b"txn reverted"),
        };
        let output = get_transaction_receipt(context.clone(), input, version)
            .await
            .unwrap();

        let output_json = output.serialize(Serializer { version }).unwrap();

        crate::assert_json_matches_fixture!(
            output_json,
            version,
            "transactions/receipt_reverted.json"
        );

        let input = Input {
            transaction_hash: transaction_hash_bytes!(b"preconfirmed reverted"),
        };
        let output = get_transaction_receipt(context, input, version).await;

        if version < RpcVersion::V09 {
            assert_matches::assert_matches!(output, Err(Error::TxnHashNotFound));
            return;
        }

        let output_json = output.unwrap().serialize(Serializer { version }).unwrap();

        crate::assert_json_matches_fixture!(
            output_json,
            version,
            "transactions/receipt_reverted_preconfirmed.json"
        );
    }
}
