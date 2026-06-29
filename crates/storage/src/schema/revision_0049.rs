use anyhow::Context;

pub(crate) fn migrate(
    tx: &rusqlite::Transaction<'_>,
    _rocksdb: &crate::RocksDBInner,
) -> anyhow::Result<()> {
    tx.execute_batch(
        r"
ALTER TABLE block_headers ADD COLUMN eth_l1_data_gas_price BLOB DEFAULT NULL;
ALTER TABLE block_headers ADD COLUMN strk_l1_data_gas_price BLOB DEFAULT NULL;
ALTER TABLE block_headers ADD COLUMN l1_da_mode INTEGER DEFAULT 0;
",
    )
    .context("Adding new columns to block_headers")?;

    Ok(())
}
