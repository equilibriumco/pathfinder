use anyhow::Context;

pub(crate) fn migrate(
    tx: &rusqlite::Transaction<'_>,
    _rocksdb: &crate::RocksDBInner,
) -> anyhow::Result<()> {
    tx.execute_batch(
        r"
CREATE TABLE block_signatures (
    block_number INTEGER REFERENCES canonical_blocks(number) ON DELETE CASCADE,
    signature_r BLOB NOT NULL,
    signature_s BLOB NOT NULL
);",
    )
    .context("Creating block_signatures table")?;

    Ok(())
}
