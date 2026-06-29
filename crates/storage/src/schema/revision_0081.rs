use anyhow::Context;

pub(crate) fn migrate(
    tx: &rusqlite::Transaction<'_>,
    _rocksdb: &crate::RocksDBInner,
) -> anyhow::Result<()> {
    tracing::info!("Dropping SQLite transactions and transaction_hashes tables (now in RocksDB)");
    tx.execute_batch(
        "
        DROP TABLE transactions;
        DROP TABLE transaction_hashes;
        ",
    )
    .context("Dropping transactions and transaction_hashes")?;
    Ok(())
}
