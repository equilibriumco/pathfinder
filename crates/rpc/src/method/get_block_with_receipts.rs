use std::sync::Arc;

use anyhow::Context;

use crate::context::RpcContext;
use crate::dto::{TransactionResponseFlags, TxnFinalityStatus};
use crate::pending::PendingBlocks;
use crate::types::BlockId;
use crate::RpcVersion;

pub enum Output {
    Full {
        header: Box<pathfinder_common::BlockHeader>,
        body: Vec<(
            pathfinder_common::transaction::Transaction,
            pathfinder_common::receipt::Receipt,
            Vec<pathfinder_common::event::Event>,
        )>,
        is_l1_accepted: bool,
        include_proof_facts: bool,
    },
    Pending {
        block: Arc<PendingBlocks>,
        include_proof_facts: bool,
    },
}

#[derive(Debug, PartialEq)]
pub struct Input {
    pub block_id: BlockId,
    pub response_flags: TransactionResponseFlags,
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

crate::error::generate_rpc_error_subset!(Error: BlockNotFound);

pub async fn get_block_with_receipts(
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

        let mut db = context
            .storage
            .connection()
            .context("Creating database connection")?;

        let db = db.transaction().context("Creating database transaction")?;

        let block_id = match input.block_id {
            BlockId::PreConfirmed => {
                let pending = context.pending_data.get(&db, rpc_version)?;

                return Ok(Output::Pending {
                    block: pending.pending_block(),
                    include_proof_facts,
                });
            }
            other => other
                .to_common_or_panic(&db)
                .map_err(|_| Error::BlockNotFound)?,
        };

        let header = db
            .block_header(block_id)
            .context("Fetching block header")?
            .ok_or(Error::BlockNotFound)?;

        let body = db
            .transaction_data_for_block(block_id)
            .context("Fetching transaction data")?
            .context("Transaction data missing")?;

        let is_l1_accepted = db
            .block_is_l1_accepted(block_id)
            .context("Fetching block finality")?;

        Ok(Output::Full {
            header: header.into(),
            body,
            is_l1_accepted,
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
        let mut serializer = serializer.serialize_struct()?;
        match self {
            Output::Full {
                header,
                body,
                is_l1_accepted,
                include_proof_facts,
            } => {
                let finality = if *is_l1_accepted {
                    crate::dto::TxnFinalityStatus::AcceptedOnL1
                } else {
                    crate::dto::TxnFinalityStatus::AcceptedOnL2
                };
                serializer.serialize_field(
                    "status",
                    &if *is_l1_accepted {
                        "ACCEPTED_ON_L1"
                    } else {
                        "ACCEPTED_ON_L2"
                    },
                )?;
                serializer.flatten(header.as_ref())?;
                serializer.serialize_iter(
                    "transactions",
                    body.len(),
                    &mut body
                        .iter()
                        .map(|(transaction, receipt, events)| TransactionWithReceipt {
                            transaction,
                            receipt,
                            events,
                            finality,
                            include_proof_facts: *include_proof_facts,
                        }),
                )?;
            }
            Output::Pending {
                block,
                include_proof_facts,
            } => {
                serializer.flatten(&block.pre_confirmed)?;
                let transactions = block.transactions();
                serializer.serialize_iter(
                    "transactions",
                    transactions.len(),
                    &mut block
                        .transactions()
                        .iter()
                        .zip(block.tx_receipts_and_events().iter())
                        .map(|(transaction, (receipt, events))| TransactionWithReceipt {
                            transaction,
                            receipt,
                            events,
                            finality: TxnFinalityStatus::PreConfirmed,
                            include_proof_facts: *include_proof_facts,
                        }),
                )?;
            }
        }
        serializer.end()
    }
}

struct TransactionWithReceipt<'a> {
    pub transaction: &'a pathfinder_common::transaction::Transaction,
    pub receipt: &'a pathfinder_common::receipt::Receipt,
    pub events: &'a [pathfinder_common::event::Event],
    pub finality: crate::dto::TxnFinalityStatus,
    pub include_proof_facts: bool,
}

impl crate::dto::SerializeForVersion for TransactionWithReceipt<'_> {
    fn serialize(
        &self,
        serializer: crate::dto::Serializer,
    ) -> Result<crate::dto::Ok, crate::dto::Error> {
        let mut serializer = serializer.serialize_struct()?;
        serializer.serialize_field("transaction", &(self.transaction, self.include_proof_facts))?;
        serializer.serialize_field(
            "receipt",
            &crate::dto::TxnReceipt {
                receipt: self.receipt,
                transaction: self.transaction,
                events: self.events,
                finality: self.finality,
            },
        )?;
        serializer.end()
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::dto::{SerializeForVersion, Serializer};
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
    #[case::v09(RpcVersion::V09)]
    #[case::v10(RpcVersion::V10)]
    #[tokio::test]
    async fn pre_confirmed(#[case] version: RpcVersion) {
        let context = RpcContext::for_tests_with_pre_confirmed().await;
        let input = Input {
            block_id: BlockId::PreConfirmed,
            response_flags: TransactionResponseFlags::default(),
        };

        let output = get_block_with_receipts(context.clone(), input, version)
            .await
            .unwrap()
            .serialize(Serializer { version })
            .unwrap();

        crate::assert_json_matches_fixture!(output, version, "blocks/pre_confirmed.json");
    }

    #[rstest::rstest]
    #[case::v09(RpcVersion::V09)]
    #[case::v10(RpcVersion::V10)]
    #[tokio::test]
    async fn latest(#[case] version: RpcVersion) {
        let context = RpcContext::for_tests_with_pre_confirmed().await;
        let input = Input {
            block_id: BlockId::Latest,
            response_flags: TransactionResponseFlags::default(),
        };

        let output = get_block_with_receipts(context.clone(), input, version)
            .await
            .unwrap()
            .serialize(Serializer { version })
            .unwrap();

        crate::assert_json_matches_fixture!(output, version, "blocks/latest.json");
    }
}
