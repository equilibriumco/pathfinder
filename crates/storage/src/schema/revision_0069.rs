use anyhow::Context;
use rusqlite::Transaction;

pub(crate) fn migrate(tx: &Transaction<'_>, _rocksdb: &crate::RocksDBInner) -> anyhow::Result<()> {
    tracing::info!("Dropping the reorg_counter table");

    tx.execute("DROP TABLE reorg_counter", [])
        .context("Dropping reorg_counter table")?;

    Ok(())
}
