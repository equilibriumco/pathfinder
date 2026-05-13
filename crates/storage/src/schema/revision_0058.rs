use anyhow::Context;

pub(crate) fn migrate(
    tx: &rusqlite::Transaction<'_>,
    _rocksdb: &crate::RocksDBInner,
) -> anyhow::Result<()> {
    tracing::info!(
        "Removing count columns, and adding state diff commitment and length columns to \
         block_headers table"
    );

    tx.execute_batch(
        r"ALTER TABLE block_headers ADD COLUMN state_diff_length INTEGER DEFAULT 0;
        ALTER TABLE block_headers ADD COLUMN state_diff_commitment BLOB;
        ALTER TABLE block_headers DROP COLUMN storage_diffs_count;
        ALTER TABLE block_headers DROP COLUMN nonce_updates_count;
        ALTER TABLE block_headers DROP COLUMN declared_classes_count;
        ALTER TABLE block_headers DROP COLUMN deployed_contracts_count;",
    )
    .context(
        "Removing count columns, and adding state diff commitment and length columns to \
         block_headers table",
    )
}
