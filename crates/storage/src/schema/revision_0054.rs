use anyhow::Context;

pub(crate) fn migrate(
    tx: &rusqlite::Transaction<'_>,
    _rocksdb: &crate::RocksDBInner,
) -> anyhow::Result<()> {
    tx.execute_batch(
        r"
        CREATE TABLE storage_flags (
            flag TEXT NOT NULL PRIMARY KEY
        );",
    )
    .context("Creating storage_flags table")?;

    Ok(())
}
