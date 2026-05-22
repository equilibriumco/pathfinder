use anyhow::Context;

pub(crate) fn migrate(
    tx: &rusqlite::Transaction<'_>,
    _rocksdb: &crate::RocksDBInner,
) -> anyhow::Result<()> {
    tracing::info!("Dropping SQLite trie and contract state hash tables (now in RocksDB)");
    tx.execute_batch(
        "
        DROP TABLE trie_class;
        DROP TABLE trie_contracts;
        DROP TABLE trie_storage;
        DROP TABLE contract_state_hashes;
        ",
    )
    .context("Dropping trie and contract_state_hashes tables")?;
    Ok(())
}
