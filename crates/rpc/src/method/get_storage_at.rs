use anyhow::Context;
use pathfinder_common::{
    BlockNumber,
    ContractAddress,
    FoundStorageValue,
    StorageAddress,
    StorageValue,
};

use crate::context::RpcContext;
use crate::dto::{StorageResponseFlag, StorageResponseFlags};
use crate::types::BlockId;
use crate::RpcVersion;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Input {
    pub contract_address: ContractAddress,
    pub key: StorageAddress,
    pub block_id: BlockId,
    pub response_flags: StorageResponseFlags,
}

impl crate::dto::DeserializeForVersion for Input {
    fn deserialize(value: crate::dto::Value) -> Result<Self, serde_json::Error> {
        let rpc_version = value.version;
        value.deserialize_map(|value| {
            let contract_address = value.deserialize("contract_address").map(ContractAddress)?;
            let key = value.deserialize("key").map(StorageAddress)?;
            let block_id = value.deserialize("block_id")?;
            let response_flags = if rpc_version >= RpcVersion::V10 {
                value
                    .deserialize_optional("response_flags")?
                    .unwrap_or_default()
            } else {
                StorageResponseFlags::default()
            };
            Ok(Self {
                contract_address,
                key,
                block_id,
                response_flags,
            })
        })
    }
}

#[derive(Debug)]
pub struct Output {
    value: StorageValue,
    last_update_block: BlockNumber,
    include_last_update_block: bool,
}

crate::error::generate_rpc_error_subset!(Error: ContractNotFound, BlockNotFound);

/// Get the value of the storage at the given address and key.
pub async fn get_storage_at(
    context: RpcContext,
    input: Input,
    rpc_version: RpcVersion,
) -> Result<Output, Error> {
    let span = tracing::Span::current();
    let jh = util::task::spawn_blocking(move |_| {
        let _g = span.enter();

        let include_last_update_block = input
            .response_flags
            .0
            .iter()
            .any(|flag| flag == &StorageResponseFlag::IncludeLastUpdateBlock);

        let mut db = context
            .storage
            .connection()
            .context("Opening database connection")?;

        let tx = db.transaction().context("Creating database transaction")?;

        if input.block_id.is_pending() {
            let pending_data = context.pending_data.get(&tx, rpc_version)?;
            let opt_found = pending_data.find_storage_value(input.contract_address, input.key);
            if let Some(found) = opt_found {
                let (value, last_update_block) = match found {
                    FoundStorageValue::Zero => (StorageValue::ZERO, BlockNumber::new_or_panic(0)),
                    FoundStorageValue::Set(v) => (v, pending_data.pre_confirmed_block_number()),
                };
                return Ok(Output {
                    value,
                    last_update_block,
                    include_last_update_block,
                });
            }
        }

        let block_id = input
            .block_id
            .to_common_coerced(&tx)
            .map_err(|_| Error::BlockNotFound)?;
        if !tx.block_exists(block_id)? {
            return Err(Error::BlockNotFound);
        }

        let opt_pair = tx
            .storage_value_with_block(block_id, input.contract_address, input.key)
            .context("Querying storage value")?;
        match opt_pair {
            Some(pair) => Ok(Output {
                value: pair.0,
                last_update_block: pair.1,
                include_last_update_block,
            }),
            None => {
                if tx.contract_exists(input.contract_address, block_id)? {
                    Ok(Output {
                        value: StorageValue::ZERO,
                        last_update_block: BlockNumber::new_or_panic(0),
                        include_last_update_block,
                    })
                } else {
                    Err(Error::ContractNotFound)
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
        if self.include_last_update_block {
            let mut serializer = serializer.serialize_struct()?;
            serializer.serialize_field("value", &self.value)?;
            serializer.serialize_field("last_update_block", &self.last_update_block)?;
            serializer.end()
        } else {
            serializer.serialize(&self.value)
        }
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use pathfinder_common::macro_prelude::*;
    use pathfinder_common::BlockNumber;
    use serde_json::json;

    use super::*;
    use crate::dto::{DeserializeForVersion, SerializeForVersion, Serializer};
    use crate::RpcVersion;

    /// # Important
    ///
    /// `BlockId` parsing is tested in
    /// [`get_block`][crate::rpc::method::get_block::tests::parsing]
    /// and is not repeated here.
    #[rstest::rstest]
    #[case::positional(json!(["1", "2", "latest"]))]
    #[case::named(json!({"contract_address": "0x1", "key": "0x2", "block_id": "latest"}))]
    fn parsing(#[case] input: serde_json::Value) {
        let expected = Input {
            contract_address: contract_address!("0x1"),
            key: storage_address!("0x2"),
            block_id: BlockId::Latest,
            response_flags: StorageResponseFlags::default(),
        };

        let input = Input::deserialize(crate::dto::Value::new(input, RpcVersion::V09)).unwrap();

        assert_eq!(input, expected);
    }

    #[rstest::rstest]
    #[case::positional(json!(["1", "2", "latest"]))]
    #[case::named(json!({"contract_address": "0x1", "key": "0x2", "block_id": "latest"}))]
    fn deserialize_v10_with_default(#[case] input: serde_json::Value) {
        let expected = Input {
            contract_address: contract_address!("0x1"),
            key: storage_address!("0x2"),
            block_id: BlockId::Latest,
            response_flags: StorageResponseFlags::default(),
        };

        let input = Input::deserialize(crate::dto::Value::new(input, RpcVersion::V10)).unwrap();

        assert_eq!(input, expected);
    }

    #[rstest::rstest]
    #[case::positional(json!(["1", "2", "latest", ["INCLUDE_LAST_UPDATE_BLOCK"]]))]
    #[case::named(json!({"contract_address": "0x1", "key": "0x2", "block_id": "latest", "response_flags": ["INCLUDE_LAST_UPDATE_BLOCK"]}))]
    fn deserialize_v10_with_flag(#[case] json: serde_json::Value) {
        use crate::dto::DeserializeForVersion;

        let value = crate::dto::Value::new(json, RpcVersion::V10);
        let input = Input::deserialize(value).unwrap();

        assert_eq!(
            input,
            Input {
                contract_address: contract_address!("0x1"),
                key: storage_address!("0x2"),
                block_id: BlockId::Latest,
                response_flags: StorageResponseFlags(vec![
                    StorageResponseFlag::IncludeLastUpdateBlock
                ]),
            }
        );
    }

    #[test]
    fn deserialize_v10_without_response_flags() {
        use crate::dto::DeserializeForVersion;

        let json = r#"{
            "contract_address": "0x1",
            "key": "0x2",
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
                contract_address: contract_address!("0x1"),
                key: storage_address!("0x2"),
                block_id: BlockId::Latest,
                response_flags: StorageResponseFlags::default(),
            }
        );
    }

    const RPC_VERSION: RpcVersion = RpcVersion::V09;

    #[tokio::test]
    async fn pre_confirmed() {
        let ctx = RpcContext::for_tests_with_pre_confirmed().await;

        // This contract is created during storage setup and has a storage value set in
        // the pre-confirmed block.
        let input = Input {
            contract_address: contract_address_bytes!(b"preconfirmed contract 1 address"),
            key: storage_address_bytes!(b"preconfirmed storage key 0"),
            block_id: BlockId::PreConfirmed,
            response_flags: StorageResponseFlags::default(),
        };

        let result = get_storage_at(ctx.clone(), input.clone(), RpcVersion::V09)
            .await
            .unwrap();
        assert_eq!(
            result.value,
            storage_value_bytes!(b"preconfirmed storage value 0")
        );
    }

    #[tokio::test]
    async fn pre_latest() {
        let ctx = RpcContext::for_tests_with_pre_latest_and_pre_confirmed().await;

        // This contract is created during storage setup and has a storage value set in
        // the pre-latest block.
        let input = Input {
            contract_address: contract_address_bytes!(b"prelatest contract 1 address"),
            key: storage_address_bytes!(b"prelatest storage key 0"),
            block_id: BlockId::PreConfirmed,
            response_flags: StorageResponseFlags::default(),
        };

        let result = get_storage_at(ctx.clone(), input.clone(), RpcVersion::V09)
            .await
            .unwrap();
        assert_eq!(
            result.value,
            storage_value_bytes!(b"prelatest storage value 0")
        );
    }

    #[tokio::test]
    async fn pre_confirmed_falls_back_to_latest() {
        let ctx = RpcContext::for_tests_with_pre_confirmed().await;
        let contract_address = contract_address_bytes!(b"contract 1");
        let key = storage_address_bytes!(b"storage addr 0");
        let block_id = BlockId::PreConfirmed;

        let result = get_storage_at(
            ctx,
            Input {
                contract_address,
                key,
                block_id,
                response_flags: StorageResponseFlags::default(),
            },
            RPC_VERSION,
        )
        .await
        .unwrap();

        assert_eq!(result.value, storage_value_bytes!(b"storage value 2"));
    }

    #[tokio::test]
    async fn pre_confirmed_deployed_defaults_to_zero() {
        let ctx = RpcContext::for_tests_with_pre_confirmed().await;
        // Contract is deployed in pending block, but has no storage values set.
        let contract_address = contract_address_bytes!(b"preconfirmed contract 0 address");
        let key = storage_address_bytes!(b"non-existent");
        let block_id = BlockId::PreConfirmed;
        let response_flags = StorageResponseFlags::default();

        let result = get_storage_at(
            ctx,
            Input {
                contract_address,
                key,
                block_id,
                response_flags,
            },
            RPC_VERSION,
        )
        .await
        .unwrap();

        assert_eq!(result.value, StorageValue::ZERO);
        assert_eq!(result.last_update_block, BlockNumber::new_or_panic(0));
    }

    #[tokio::test]
    async fn latest() {
        let ctx = RpcContext::for_tests_with_pre_confirmed().await;
        let contract_address = contract_address_bytes!(b"contract 1");
        let key = storage_address_bytes!(b"storage addr 0");
        let block_id = BlockId::Latest;
        let response_flags = StorageResponseFlags::default();

        let result = get_storage_at(
            ctx,
            Input {
                contract_address,
                key,
                block_id,
                response_flags,
            },
            RPC_VERSION,
        )
        .await
        .unwrap();

        assert_eq!(result.value, storage_value_bytes!(b"storage value 2"));
    }

    #[tokio::test]
    async fn latest_with_update_block() {
        let ctx = RpcContext::for_tests_with_pre_confirmed().await;
        let version = RpcVersion::V10;
        let contract_address = contract_address_bytes!(b"contract 1");
        let key = storage_address_bytes!(b"storage addr 0");
        let block_id = BlockId::Latest;
        let response_flags =
            StorageResponseFlags(vec![StorageResponseFlag::IncludeLastUpdateBlock]);

        let output = get_storage_at(
            ctx,
            Input {
                contract_address,
                key,
                block_id,
                response_flags,
            },
            version,
        )
        .await
        .unwrap();
        let output_json = output.serialize(Serializer { version }).unwrap();

        let expected_json: serde_json::Value = serde_json::from_str(include_str!(
            "../../fixtures/0.10.0/storage_at/latest_with_update_block.json"
        ))
        .unwrap();
        pretty_assertions_sorted::assert_eq!(output_json, expected_json);
    }

    #[tokio::test]
    async fn latest_without_update_block() {
        let ctx = RpcContext::for_tests_with_pre_confirmed().await;
        let version = RpcVersion::V10;
        let contract_address = contract_address_bytes!(b"contract 1");
        let key = storage_address_bytes!(b"storage addr 0");
        let block_id = BlockId::Latest;
        let response_flags = StorageResponseFlags::default();

        let output = get_storage_at(
            ctx,
            Input {
                contract_address,
                key,
                block_id,
                response_flags,
            },
            version,
        )
        .await
        .unwrap();
        let output_json = output.serialize(Serializer { version }).unwrap();

        let expected_json: serde_json::Value =
            serde_json::from_str(include_str!("../../fixtures/0.10.0/storage_at/latest.json"))
                .unwrap();
        pretty_assertions_sorted::assert_eq!(output_json, expected_json);
    }

    #[tokio::test]
    async fn l1_accepted() {
        let ctx = RpcContext::for_tests_with_pre_confirmed().await;
        let contract_address = contract_address_bytes!(b"contract 1");
        let key = storage_address_bytes!(b"storage addr 0");
        let block_id = BlockId::L1Accepted;
        let response_flags = StorageResponseFlags::default();

        let result = get_storage_at(
            ctx,
            Input {
                contract_address,
                key,
                block_id,
                response_flags,
            },
            RPC_VERSION,
        )
        .await
        .unwrap();

        assert_eq!(result.value, storage_value_bytes!(b"storage value 1"));
    }

    #[tokio::test]
    async fn defaults_to_zero() {
        let ctx = RpcContext::for_tests_with_pre_confirmed().await;
        let contract_address = contract_address_bytes!(b"contract 1");
        let key = storage_address_bytes!(b"non-existent");
        let block_id = BlockId::Latest;
        let response_flags = StorageResponseFlags::default();

        let result = get_storage_at(
            ctx,
            Input {
                contract_address,
                key,
                block_id,
                response_flags,
            },
            RPC_VERSION,
        )
        .await
        .unwrap();

        assert_eq!(result.value, StorageValue::ZERO);
        assert_eq!(result.last_update_block, BlockNumber::new_or_panic(0));
    }

    #[tokio::test]
    async fn by_hash() {
        let ctx = RpcContext::for_tests_with_pre_confirmed().await;
        let contract_address = contract_address_bytes!(b"contract 1");
        let key = storage_address_bytes!(b"storage addr 0");
        let block_id = BlockId::Hash(block_hash_bytes!(b"block 1"));
        let response_flags = StorageResponseFlags::default();

        let result = get_storage_at(
            ctx,
            Input {
                contract_address,
                key,
                block_id,
                response_flags,
            },
            RPC_VERSION,
        )
        .await
        .unwrap();

        assert_eq!(result.value, storage_value_bytes!(b"storage value 1"));
    }

    #[tokio::test]
    async fn by_number() {
        let ctx = RpcContext::for_tests_with_pre_confirmed().await;
        let contract_address = contract_address_bytes!(b"contract 1");
        let key = storage_address_bytes!(b"storage addr 0");
        let block_id = BlockId::Number(BlockNumber::GENESIS + 1);
        let response_flags = StorageResponseFlags::default();

        let result = get_storage_at(
            ctx,
            Input {
                contract_address,
                key,
                block_id,
                response_flags,
            },
            RPC_VERSION,
        )
        .await
        .unwrap();

        assert_eq!(result.value, storage_value_bytes!(b"storage value 1"));
    }

    #[tokio::test]
    async fn unknown_contract() {
        let ctx = RpcContext::for_tests_with_pre_confirmed().await;
        let contract_address = contract_address_bytes!(b"non-existent");
        let key = storage_address_bytes!(b"storage addr 0");
        let block_id = BlockId::Latest;
        let response_flags = StorageResponseFlags::default();

        let result = get_storage_at(
            ctx,
            Input {
                contract_address,
                key,
                block_id,
                response_flags,
            },
            RPC_VERSION,
        )
        .await;

        assert_matches!(result, Err(Error::ContractNotFound));
    }

    #[tokio::test]
    async fn contract_is_unknown_before_deployment() {
        let ctx = RpcContext::for_tests_with_pre_confirmed().await;
        let contract_address = contract_address_bytes!(b"contract 1");
        let key = storage_address_bytes!(b"storage addr 0");
        let block_id = BlockId::Hash(block_hash_bytes!(b"genesis"));
        let response_flags = StorageResponseFlags::default();

        let result = get_storage_at(
            ctx,
            Input {
                contract_address,
                key,
                block_id,
                response_flags,
            },
            RPC_VERSION,
        )
        .await;

        assert_matches!(result, Err(Error::ContractNotFound));
    }

    #[tokio::test]
    async fn block_not_found_by_number() {
        let ctx = RpcContext::for_tests_with_pre_confirmed().await;
        let contract_address = contract_address_bytes!(b"contract 1");
        let key = storage_address_bytes!(b"storage addr 0");
        let block_id = BlockId::Number(BlockNumber::MAX);
        let response_flags = StorageResponseFlags::default();

        let result = get_storage_at(
            ctx,
            Input {
                contract_address,
                key,
                block_id,
                response_flags,
            },
            RPC_VERSION,
        )
        .await;

        assert_matches!(result, Err(Error::BlockNotFound));
    }

    #[tokio::test]
    async fn block_not_found_by_hash() {
        let ctx = RpcContext::for_tests_with_pre_confirmed().await;
        let contract_address = contract_address_bytes!(b"contract 1");
        let key = storage_address_bytes!(b"storage addr 0");
        let block_id = BlockId::Hash(block_hash_bytes!(b"unknown"));
        let response_flags = StorageResponseFlags::default();

        let result = get_storage_at(
            ctx,
            Input {
                contract_address,
                key,
                block_id,
                response_flags,
            },
            RPC_VERSION,
        )
        .await;

        assert_matches!(result, Err(Error::BlockNotFound));
    }
}
