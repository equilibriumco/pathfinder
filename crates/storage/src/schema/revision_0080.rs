use anyhow::Context;

pub(crate) fn migrate(
    tx: &rusqlite::Transaction<'_>,
    _rocksdb: &crate::RocksDBInner,
) -> anyhow::Result<()> {
    tracing::info!("Dropping SQLite storage/nonce update tables (now in RocksDB)");
    tx.execute_batch(
        "
        DROP TABLE storage_updates;
        DROP TABLE nonce_updates;
        DROP TABLE storage_addresses;
        DROP TABLE contract_addresses;
        ",
    )
    .context("Dropping nonce/storage update tables")?;
    Ok(())
}
