use anyhow::Context;
use pathfinder_common::BlockId;

use crate::context::RpcContext;

#[derive(Debug, PartialEq, Eq)]
pub struct Output {
    number: pathfinder_common::BlockNumber,
    hash: pathfinder_common::BlockHash,
}

crate::error::generate_rpc_error_subset!(Error: NoBlocks);

pub async fn last_l1_accepted_block_hash_and_number(context: RpcContext) -> Result<Output, Error> {
    let span = tracing::Span::current();
    let jh = util::task::spawn_blocking(move |_| {
        let _g = span.enter();
        let mut db_conn = context
            .storage
            .connection()
            .context("Opening database connection")?;
        let db_tx = db_conn
            .transaction()
            .context("Opening database transaction")?;
        let opt_block_number = db_tx.l1_l2_pointer().context("Querying L1-L2 pointer")?;
        let last_block_number = opt_block_number.ok_or(Error::NoBlocks)?;
        db_tx
            .block_id(BlockId::Number(last_block_number))
            .context("Reading latest accepted block number and hash from database")?
            .map(|(number, hash)| Output { number, hash })
            .ok_or(Error::NoBlocks)
    });

    jh.await.context("Database read panic or shutting down")?
}

impl crate::dto::SerializeForVersion for Output {
    fn serialize(
        &self,
        serializer: crate::dto::Serializer,
    ) -> Result<crate::dto::Ok, crate::dto::Error> {
        let mut serializer = serializer.serialize_struct()?;
        serializer.serialize_field("block_hash", &self.hash)?;
        serializer.serialize_field("block_number", &self.number)?;
        serializer.end()
    }
}

#[cfg(test)]
mod tests {
    use pathfinder_common::{block_hash_bytes, BlockNumber};
    use pathfinder_storage::{StorageBuilder, TriePruneMode};
    use pretty_assertions_sorted::assert_eq;
    use serde_json::json;

    use super::*;
    use crate::dto::{SerializeForVersion, Serializer};
    use crate::RpcVersion;

    #[tokio::test]
    async fn last_l1_accepted() {
        let context = RpcContext::for_tests();
        let actual = last_l1_accepted_block_hash_and_number(context)
            .await
            .unwrap();

        let expected = Output {
            number: BlockNumber::new_or_panic(1),
            hash: block_hash_bytes!(b"block 1"),
        };
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn no_last_l1_accepted() {
        let empty_storage =
            StorageBuilder::in_memory_with_trie_pruning(TriePruneMode::Archive).unwrap();
        let context = RpcContext::for_tests().with_storage(empty_storage);
        let error = last_l1_accepted_block_hash_and_number(context)
            .await
            .err()
            .unwrap();
        let rpc_error = crate::jsonrpc::RpcError::from(error);
        let error_json = rpc_error
            .serialize(Serializer::new(RpcVersion::V09))
            .unwrap();

        let expected = json!({
            "code": 32,
            "message": "There are no blocks",
        });
        assert_eq!(error_json, expected);
    }
}
