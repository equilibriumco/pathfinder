use anyhow::Context;

pub(crate) fn migrate(
    tx: &rusqlite::Transaction<'_>,
    _rocksdb: &crate::RocksDBInner,
) -> anyhow::Result<()> {
    tracing::info!("Creating index for Merkle trie leaves");

    tx.execute_batch(
        r"
DROP INDEX contract_state_hashes_address;
DROP INDEX class_commitment_leaves_casm;
CREATE INDEX contract_state_hashes_address_block_number ON contract_state_hashes(contract_address, block_number);
CREATE INDEX class_commitment_leaves_casm_block_number ON class_commitment_leaves(casm, block_number);
",
    )
    .context("Dropping and re-creating trie leaf index")?;

    Ok(())
}
