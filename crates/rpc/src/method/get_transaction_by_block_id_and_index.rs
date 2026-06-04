use anyhow::Context;
use pathfinder_common::transaction::Transaction;
use pathfinder_common::TransactionIndex;

use crate::context::RpcContext;
use crate::dto::TransactionResponseFlags;
use crate::types::BlockId;
use crate::RpcVersion;

#[derive(Debug, PartialEq, Eq)]
pub struct Input {
    block_id: BlockId,
    index: TransactionIndex,
    response_flags: TransactionResponseFlags,
}

impl crate::dto::DeserializeForVersion for Input {
    fn deserialize(value: crate::dto::Value) -> Result<Self, serde_json::Error> {
        let rpc_version = value.version;

        value.deserialize_map(|value| {
            let block_id = value.deserialize("block_id")?;
            let index = value.deserialize("index")?;
            let response_flags = if rpc_version >= RpcVersion::V10 {
                value
                    .deserialize_optional("response_flags")?
                    .unwrap_or_default()
            } else {
                TransactionResponseFlags::default()
            };

            Ok(Self {
                block_id,
                index,
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

crate::error::generate_rpc_error_subset!(
    GetTransactionByBlockIdAndIndexError: BlockNotFound,
    InvalidTxnIndex
);

pub async fn get_transaction_by_block_id_and_index(
    context: RpcContext,
    input: Input,
    rpc_version: RpcVersion,
) -> Result<Output, GetTransactionByBlockIdAndIndexError> {
    let index: usize = input
        .index
        .get()
        .try_into()
        .map_err(|_| GetTransactionByBlockIdAndIndexError::InvalidTxnIndex)?;

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

        let block_id = match input.block_id {
            BlockId::PreConfirmed => {
                let result = context
                    .pending_data
                    .get(&db_tx, rpc_version)?
                    .pre_confirmed_transactions()
                    .get(index)
                    .cloned()
                    .ok_or(GetTransactionByBlockIdAndIndexError::InvalidTxnIndex);
                return result.map(|transaction| Output {
                    transaction,
                    include_proof_facts,
                });
            }
            other => other
                .to_common_or_panic(&db_tx)
                .map_err(|_| GetTransactionByBlockIdAndIndexError::BlockNotFound)?,
        };

        // Get the transaction from storage.
        match db_tx
            .transaction_at_block(block_id, index)
            .context("Reading transaction from database")?
        {
            Some(transaction) => Ok(Output {
                transaction,
                include_proof_facts,
            }),
            None => {
                // We now need to check whether it was the block hash or transaction index which
                // were invalid. We do this by checking if the block exists
                // at all. If no, then the block hash is invalid. If yes, then the index is
                // invalid.
                let block_exists = db_tx
                    .block_exists(block_id)
                    .context("Querying block existence")?;
                if block_exists {
                    Err(GetTransactionByBlockIdAndIndexError::InvalidTxnIndex)
                } else {
                    Err(GetTransactionByBlockIdAndIndexError::BlockNotFound)
                }
            }
        }
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
            let positional_json = json!([
                {"block_hash": "0xdeadbeef"},
                1
            ]);

            let positional = crate::dto::Value::new(positional_json, crate::RpcVersion::V10);

            let input = Input::deserialize(positional).unwrap();
            assert_eq!(
                input,
                Input {
                    block_id: BlockId::Hash(block_hash!("0xdeadbeef")),
                    index: TransactionIndex::new_or_panic(1),
                    response_flags: TransactionResponseFlags::default(),
                }
            )
        }

        #[test]
        fn named_args() {
            let named_args_json = json!({
                "block_id": {"block_hash": "0xdeadbeef"},
                "index": 1
            });

            let named = crate::dto::Value::new(named_args_json, crate::RpcVersion::V10);

            let input = Input::deserialize(named).unwrap();
            assert_eq!(
                input,
                Input {
                    block_id: BlockId::Hash(block_hash!("0xdeadbeef")),
                    index: TransactionIndex::new_or_panic(1),
                    response_flags: TransactionResponseFlags::default(),
                }
            )
        }

        #[test]
        fn named_args_with_response_flags() {
            let named_args_json = json!({
                "block_id": {"block_hash": "0xdeadbeef"},
                "index": 1,
                "response_flags": ["INCLUDE_PROOF_FACTS"]
            });

            let named = crate::dto::Value::new(named_args_json, crate::RpcVersion::V10);

            let input = Input::deserialize(named).unwrap();
            assert_eq!(
                input,
                Input {
                    block_id: BlockId::Hash(block_hash!("0xdeadbeef")),
                    index: TransactionIndex::new_or_panic(1),
                    response_flags: TransactionResponseFlags(vec![
                        crate::dto::TransactionResponseFlag::IncludeProofFacts
                    ]),
                }
            )
        }
    }
}
