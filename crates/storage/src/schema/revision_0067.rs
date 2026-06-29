use anyhow::Context;

pub(crate) fn migrate(
    tx: &rusqlite::Transaction<'_>,
    _rocksdb: &crate::RocksDBInner,
) -> anyhow::Result<()> {
    tracing::info!(
        "Removing storage_commitment and class_commitment columns from block_headers table"
    );

    tx.execute_batch(
        r"
        ALTER TABLE block_headers DROP COLUMN storage_commitment;
        ALTER TABLE block_headers DROP COLUMN class_commitment;
        ",
    )
    .context("Removing storage_commitment and class_commitment columns from block_headers table")
}
