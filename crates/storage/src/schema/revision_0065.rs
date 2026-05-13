use anyhow::Context;

pub(crate) fn migrate(
    tx: &rusqlite::Transaction<'_>,
    _rocksdb: &crate::RocksDBInner,
) -> anyhow::Result<()> {
    tracing::info!("Adding l2_gas_price columns to block_headers");

    tx.execute_batch("ALTER TABLE block_headers ADD COLUMN eth_l2_gas_price BLOB DEFAULT NULL;")
        .context("Adding eth_l2_gas_price column to block_headers")?;
    tx.execute_batch("ALTER TABLE block_headers ADD COLUMN strk_l2_gas_price BLOB DEFAULT NULL;")
        .context("Adding strk_l2_gas_price column to block_headers")?;

    Ok(())
}
