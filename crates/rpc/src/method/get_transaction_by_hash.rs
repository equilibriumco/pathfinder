crate::error::generate_rpc_error_subset!(GetTransactionByHashError: TxnHashNotFound);

use anyhow::Context;
use pathfinder_common::transaction::Transaction;
use pathfinder_common::TransactionHash;

use crate::context::RpcContext;
use crate::dto::TransactionResponseFlags;
use crate::RpcVersion;

#[derive(Debug, PartialEq, Eq)]
pub struct Input {
    transaction_hash: TransactionHash,
    response_flags: TransactionResponseFlags,
}

impl crate::dto::DeserializeForVersion for Input {
    fn deserialize(value: crate::dto::Value) -> Result<Self, serde_json::Error> {
        let rpc_version = value.version;

        value.deserialize_map(|value| {
            let transaction_hash = value.deserialize("transaction_hash").map(TransactionHash)?;
            let response_flags = if rpc_version >= RpcVersion::V10 {
                value
                    .deserialize_optional("response_flags")?
                    .unwrap_or_default()
            } else {
                TransactionResponseFlags::default()
            };

            Ok(Self {
                transaction_hash,
                response_flags,
            })
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Output {
    transaction: Transaction,
    include_proof_facts: bool,
}

pub async fn get_transaction_by_hash(
    context: RpcContext,
    input: Input,
    rpc_version: RpcVersion,
) -> Result<Output, GetTransactionByHashError> {
    let include_proof_facts = input
        .response_flags
        .0
        .iter()
        .any(|flag| flag == &crate::dto::TransactionResponseFlag::IncludeProofFacts);

    let storage = context.storage.clone();
    let span = tracing::Span::current();
    let jh = util::task::spawn_blocking(move |_| {
        let _g = span.enter();
        let mut db = storage
            .connection()
            .context("Opening database connection")?;

        let db_tx = db.transaction().context("Creating database transaction")?;

        // Pending is an optional first look; a finalized tx lives in the DB regardless.
        let pending = context.pending_data.get_optional(&db_tx, rpc_version)?;
        if let Some(transaction) = pending
            .as_ref()
            .and_then(|p| p.find_transaction(input.transaction_hash))
        {
            return Ok(Output {
                transaction,
                include_proof_facts,
            });
        }

        // Get the transaction from storage.
        db_tx
            .transaction(input.transaction_hash)
            .context("Reading transaction from database")?
            .ok_or(GetTransactionByHashError::TxnHashNotFound)
            .map(|transaction| Output {
                transaction,
                include_proof_facts,
            })
    });

    jh.await.context("Database read panic or shutting down")?
}

impl crate::dto::SerializeForVersion for Output {
    fn serialize(
        &self,
        serializer: crate::dto::Serializer,
    ) -> Result<crate::dto::Ok, crate::dto::Error> {
        serializer.serialize(&crate::dto::TransactionWithHash {
            transaction: &self.transaction,
            include_proof_facts: self.include_proof_facts,
        })
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
            let positional_json = json!(["0xdeadbeef"]);

            let positional = crate::dto::Value::new(positional_json, crate::RpcVersion::V09);

            let input = Input::deserialize(positional).unwrap();
            assert_eq!(
                input,
                Input {
                    transaction_hash: transaction_hash!("0xdeadbeef"),
                    response_flags: TransactionResponseFlags::default(),
                }
            )
        }

        #[test]
        fn named_args() {
            let named_args_json = json!({
                "transaction_hash": "0xdeadbeef"
            });

            let named = crate::dto::Value::new(named_args_json, crate::RpcVersion::V09);

            let input = Input::deserialize(named).unwrap();
            assert_eq!(
                input,
                Input {
                    transaction_hash: transaction_hash!("0xdeadbeef"),
                    response_flags: TransactionResponseFlags::default(),
                }
            )
        }
    }

    use pathfinder_common::transaction_hash_bytes;

    use crate::dto::{SerializeForVersion, Serializer};
    use crate::RpcVersion;

    #[rstest::rstest]
    #[case::v09(RpcVersion::V09)]
    #[case::v10(RpcVersion::V10)]
    #[tokio::test]
    async fn l2_accepted(#[case] version: RpcVersion) {
        let context = RpcContext::for_tests();
        // This transaction is in block 1 which is not L1 accepted.
        let tx_hash = transaction_hash_bytes!(b"txn 1");
        let input = Input {
            transaction_hash: tx_hash,
            response_flags: TransactionResponseFlags::default(),
        };
        let output = get_transaction_by_hash(context, input, version)
            .await
            .unwrap();

        let output_json = output.serialize(Serializer { version }).unwrap();

        crate::assert_json_matches_fixture!(output_json, version, "transactions/txn_1.json");
    }

    #[tokio::test]
    async fn finalized_tx_resolves_when_pending_unavailable() {
        let cache = std::sync::Arc::new(pathfinder_pending_data::PendingDataCache::new());
        cache.mark_unavailable("syncing");
        let context = RpcContext::for_tests().with_pending_data_cache(cache);

        let input = Input {
            transaction_hash: transaction_hash_bytes!(b"txn 1"),
            response_flags: TransactionResponseFlags::default(),
        };

        // A finalized tx lives in the DB, so an unavailable pending cache must not
        // error.
        let result = get_transaction_by_hash(context, input, RpcVersion::V09).await;
        assert!(result.is_ok());
    }

    #[rstest::rstest]
    #[case::v09(RpcVersion::V09)]
    #[case::v10(RpcVersion::V10)]
    #[tokio::test]
    async fn pre_confirmed(#[case] version: RpcVersion) {
        let context = RpcContext::for_tests_with_pre_confirmed().await;
        let tx_hash = transaction_hash_bytes!(b"preconfirmed tx hash 0");
        let input = Input {
            transaction_hash: tx_hash,
            response_flags: TransactionResponseFlags::default(),
        };
        let result = get_transaction_by_hash(context, input, version).await;

        match version {
            RpcVersion::PathfinderV01 => unreachable!(),
            RpcVersion::V09 => {
                let output_json = result.unwrap().serialize(Serializer { version }).unwrap();
                let expected_json: serde_json::Value = serde_json::from_str(include_str!(
                    "../../fixtures/0.9.0/transactions/txn_pre_confirmed_hash_0.json"
                ))
                .unwrap();
                assert_eq!(output_json, expected_json);
            }
            RpcVersion::V10 => {
                let output_json = result.unwrap().serialize(Serializer { version }).unwrap();
                let expected_json: serde_json::Value = serde_json::from_str(include_str!(
                    "../../fixtures/0.10.0/transactions/txn_pre_confirmed_hash_0.json"
                ))
                .unwrap();
                assert_eq!(output_json, expected_json);
            }
        }
    }

    #[rstest::rstest]
    #[case::v09(RpcVersion::V09)]
    #[case::v10(RpcVersion::V10)]
    #[tokio::test]
    async fn pre_latest(#[case] version: RpcVersion) {
        let context = RpcContext::for_tests_with_pre_latest_and_pre_confirmed().await;
        let tx_hash = transaction_hash_bytes!(b"prelatest tx hash 0");
        let input = Input {
            transaction_hash: tx_hash,
            response_flags: TransactionResponseFlags::default(),
        };
        let result = get_transaction_by_hash(context, input, version).await;

        match version {
            RpcVersion::PathfinderV01 => unreachable!(),
            RpcVersion::V09 => {
                let output_json = result.unwrap().serialize(Serializer { version }).unwrap();
                let expected_json: serde_json::Value = serde_json::from_str(include_str!(
                    "../../fixtures/0.9.0/transactions/txn_pre_latest_hash_0.json"
                ))
                .unwrap();
                assert_eq!(output_json, expected_json);
            }
            RpcVersion::V10 => {
                let output_json = result.unwrap().serialize(Serializer { version }).unwrap();
                let expected_json: serde_json::Value = serde_json::from_str(include_str!(
                    "../../fixtures/0.10.0/transactions/txn_pre_latest_hash_0.json"
                ))
                .unwrap();
                assert_eq!(output_json, expected_json);
            }
        }
    }

    #[rstest::rstest]
    #[case::v09(RpcVersion::V09)]
    #[case::v10(RpcVersion::V10)]
    #[tokio::test]
    async fn reverted(#[case] version: RpcVersion) {
        let context = RpcContext::for_tests_with_pre_confirmed().await;
        let input = Input {
            transaction_hash: transaction_hash_bytes!(b"txn reverted"),
            response_flags: TransactionResponseFlags::default(),
        };
        let output = get_transaction_by_hash(context.clone(), input, version)
            .await
            .unwrap();

        let output_json = output.serialize(Serializer { version }).unwrap();

        crate::assert_json_matches_fixture!(output_json, version, "transactions/txn_reverted.json");

        let input = Input {
            transaction_hash: transaction_hash_bytes!(b"preconfirmed reverted"),
            response_flags: TransactionResponseFlags::default(),
        };
        let output = get_transaction_by_hash(context, input, version).await;

        let output_json = output.unwrap().serialize(Serializer { version }).unwrap();

        crate::assert_json_matches_fixture!(
            output_json,
            version,
            "transactions/txn_preconfirmed_reverted.json"
        );
    }
}
