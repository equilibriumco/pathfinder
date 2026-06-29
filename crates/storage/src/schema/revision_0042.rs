use anyhow::Context;

pub(crate) fn migrate(
    tx: &rusqlite::Transaction<'_>,
    _rocksdb: &crate::RocksDBInner,
) -> anyhow::Result<()> {
    tx.execute(
        "CREATE INDEX contract_roots_block_number ON contract_roots(block_number)",
        [],
    )
    .context("Creating index on contract_roots(block_number)")?;

    Ok(())
}
