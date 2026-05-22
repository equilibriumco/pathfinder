use anyhow::Context;

pub(crate) fn migrate(
    tx: &rusqlite::Transaction<'_>,
    _rocksdb: &crate::RocksDBInner,
) -> anyhow::Result<()> {
    tracing::info!("Dropping SQLite trie and contract state hash tables (now in RocksDB)");
    tx.execute_batch(
        "
        DROP TABLE IF EXISTS trie_class;
        DROP TABLE IF EXISTS trie_contracts;
        DROP TABLE IF EXISTS trie_storage;
        DROP TABLE IF EXISTS contract_state_hashes;
        ",
    )
    .context("Dropping trie and contract_state_hashes tables")?;
    Ok(())
}
