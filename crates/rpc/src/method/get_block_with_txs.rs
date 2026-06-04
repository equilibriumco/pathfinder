use std::sync::Arc;

use anyhow::Context;
use pathfinder_common::transaction::Transaction;
use pathfinder_common::BlockHeader;

use crate::context::RpcContext;
use crate::dto::TransactionResponseFlags;
use crate::pending::PendingBlocks;
use crate::types::BlockId;
use crate::RpcVersion;

crate::error::generate_rpc_error_subset!(Error: BlockNotFound);

#[derive(Debug, PartialEq)]
pub struct Input {
    block_id: BlockId,
    response_flags: TransactionResponseFlags,
}

impl crate::dto::DeserializeForVersion for Input {
    fn deserialize(value: crate::dto::Value) -> Result<Self, serde_json::Error> {
        let rpc_version = value.version;

        value.deserialize_map(|value| {
            let block_id = value.deserialize("block_id")?;
            let response_flags = if rpc_version >= RpcVersion::V10 {
                value
                    .deserialize_optional("response_flags")?
                    .unwrap_or_default()
            } else {
                TransactionResponseFlags::default()
            };

            Ok(Self {
                block_id,
                response_flags,
            })
        })
    }
}

#[derive(Debug)]
pub enum Output {
    Pending {
        block: Arc<PendingBlocks>,
        // for backward compatibility with pre 0.9 versions we need to
        // mimic the structure of the "pending" block, which included parent block hash
        parent_hash: Option<pathfinder_common::BlockHash>,
        transactions: Vec<Transaction>,
        include_proof_facts: bool,
    },
    Full {
        header: Box<BlockHeader>,
        transactions: Vec<Transaction>,
        l1_accepted: bool,
        include_proof_facts: bool,
    },
}

/// Get block information with full transactions given the block id
pub async fn get_block_with_txs(
    context: RpcContext,
    input: Input,
    rpc_version: RpcVersion,
) -> Result<Output, Error> {
    let span = tracing::Span::current();
    util::task::spawn_blocking(move |_| {
        let _g = span.enter();

        let include_proof_facts = input
            .response_flags
            .0
            .iter()
            .any(|flag| flag == &crate::dto::TransactionResponseFlag::IncludeProofFacts);

        let mut connection = context
            .storage
            .connection()
            .context("Opening database connection")?;

        let transaction = connection
            .transaction()
            .context("Creating database transaction")?;

        let block_id = match input.block_id {
            BlockId::PreConfirmed => {
                let pending = context.pending_data.get(&transaction, rpc_version)?;

                let transactions = pending.pre_confirmed_transactions().to_vec();

                let parent_hash = (rpc_version < RpcVersion::V09)
                    .then(|| {
                        // versions before 0.9 don't have access to pre-confirmed data
                        // so we never need to worry about parent hash coming from pre-latest
                        Ok::<_, anyhow::Error>(
                            transaction
                                .block_header(pathfinder_common::BlockId::Latest)
                                .context("Querying latest block header")?
                                .unwrap_or_default()
                                .hash,
                        )
                    })
                    .transpose()?;

                return Ok(Output::Pending {
                    block: pending.pending_block(),
                    parent_hash,
                    transactions,
                    include_proof_facts,
                });
            }
            other => other
                .to_common_or_panic(&transaction)
                .map_err(|_| Error::BlockNotFound)?,
        };

        let header = transaction
            .block_header(block_id)
            .context("Reading block from database")?
            .ok_or(Error::BlockNotFound)?;

        let l1_accepted = transaction.block_is_l1_accepted(header.number.into())?;

        let transactions = transaction
            .transactions_for_block(header.number.into())
            .context("Reading transactions from database")?
            .context("Transaction data missing")?
            .into_iter()
            .collect();

        Ok(Output::Full {
            header: Box::new(header),
            l1_accepted,
            transactions,
            include_proof_facts,
        })
    })
    .await
    .context("Joining blocking task")?
}

impl crate::dto::SerializeForVersion for Output {
    fn serialize(
        &self,
        serializer: crate::dto::Serializer,
    ) -> Result<crate::dto::Ok, crate::dto::Error> {
        match self {
            Output::Pending {
                block,
                parent_hash,
                transactions,
                include_proof_facts,
            } => {
                let mut serializer = serializer.serialize_struct()?;
                serializer.flatten(&(parent_hash, &block.pre_confirmed))?;
                serializer.serialize_iter(
                    "transactions",
                    transactions.len(),
                    &mut transactions
                        .iter()
                        .map(|transaction| crate::dto::TransactionWithHash {
                            transaction,
                            include_proof_facts: *include_proof_facts,
                        }),
                )?;
                serializer.end()
            }
            Output::Full {
                header,
                transactions,
                l1_accepted,
                include_proof_facts,
            } => {
                let mut serializer = serializer.serialize_struct()?;
                serializer.flatten(header.as_ref())?;
                serializer.serialize_iter(
                    "transactions",
                    transactions.len(),
                    &mut transactions
                        .iter()
                        .map(|transaction| crate::dto::TransactionWithHash {
                            transaction,
                            include_proof_facts: *include_proof_facts,
                        }),
                )?;
                serializer.serialize_field(
                    "status",
                    &if *l1_accepted {
                        "ACCEPTED_ON_L1"
                    } else {
                        "ACCEPTED_ON_L2"
                    },
                )?;
                serializer.end()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dto::{SerializeForVersion, Serializer, TransactionResponseFlag};
    use crate::RpcVersion;

    mod input {
        use super::*;

        #[test]
        fn deserialize_v10_with_response_flags() {
            use crate::dto::DeserializeForVersion;

            let json = r#"{
                "block_id": "latest",
                "response_flags": ["INCLUDE_PROOF_FACTS"]
            }"#;
            let value = crate::dto::Value::new(
                serde_json::from_str::<serde_json::Value>(json).unwrap(),
                RpcVersion::V10,
            );
            let input = Input::deserialize(value).unwrap();

            assert_eq!(
                input,
                Input {
                    block_id: BlockId::Latest,
                    response_flags: TransactionResponseFlags(vec![
                        crate::dto::TransactionResponseFlag::IncludeProofFacts
                    ]),
                }
            );
        }

        #[test]
        fn deserialize_v10_without_response_flags() {
            use crate::dto::DeserializeForVersion;

            let json = r#"{
                "block_id": "latest"
            }"#;
            let value = crate::dto::Value::new(
                serde_json::from_str::<serde_json::Value>(json).unwrap(),
                RpcVersion::V10,
            );
            let input = Input::deserialize(value).unwrap();

            assert_eq!(
                input,
                Input {
                    block_id: BlockId::Latest,
                    response_flags: TransactionResponseFlags::default(),
                }
            );
        }
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

        let input = Input {
            block_id: BlockId::PreConfirmed,
            response_flags: Default::default(),
        };

        let output = get_block_with_txs(context, input, version).await.unwrap();
        let output_json = output.serialize(Serializer { version }).unwrap();

        crate::assert_json_matches_fixture!(
            output_json,
            version,
            "blocks/pre_confirmed_with_txs.json"
        );
    }

    #[rstest::rstest]
    #[case::v06(RpcVersion::V06)]
    #[case::v07(RpcVersion::V07)]
    #[case::v08(RpcVersion::V08)]
    #[case::v09(RpcVersion::V09)]
    #[case::v10(RpcVersion::V10)]
    #[tokio::test]
    async fn latest(#[case] version: RpcVersion) {
        let context = RpcContext::for_tests_with_pre_confirmed().await;

        let input = Input {
            block_id: BlockId::Latest,
            response_flags: TransactionResponseFlags::default(),
        };

        let output = get_block_with_txs(context, input, version).await.unwrap();
        let output_json = output.serialize(Serializer { version }).unwrap();

        crate::assert_json_matches_fixture!(output_json, version, "blocks/latest_with_txs.json");
    }

    #[tokio::test]
    async fn latest_with_proof_facts() {
        let context = RpcContext::for_tests_with_pre_confirmed().await;
        let version = RpcVersion::V10;

        let input = Input {
            block_id: BlockId::Latest,
            response_flags: TransactionResponseFlags(vec![
                TransactionResponseFlag::IncludeProofFacts,
            ]),
        };

        let output = get_block_with_txs(context, input, version).await.unwrap();
        let output_json = output.serialize(Serializer { version }).unwrap();

        let expected_json: serde_json::Value = serde_json::from_str(include_str!(
            "../../fixtures/0.10.0/blocks/latest_with_txs_including_proof_facts.json"
        ))
        .unwrap();

        pretty_assertions_sorted::assert_eq!(output_json, expected_json);
    }
}
