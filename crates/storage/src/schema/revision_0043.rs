use anyhow::Context;

pub(crate) fn migrate(
    tx: &rusqlite::Transaction<'_>,
    _rocksdb: &crate::RocksDBInner,
) -> anyhow::Result<()> {
    tx.execute(
        "CREATE INDEX class_commitment_leaves_block_number ON \
         class_commitment_leaves(block_number)",
        [],
    )
    .context("Creating index on class_commitment_leaves(block_number)")?;

    tx.execute(
        "CREATE INDEX contract_state_hashes_block_number ON contract_state_hashes(block_number)",
        [],
    )
    .context("Creating index on contract_state_hashes(block_number)")?;

    Ok(())
}
