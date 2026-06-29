use anyhow::Context;

pub(crate) fn migrate(
    tx: &rusqlite::Transaction<'_>,
    _rocksdb: &crate::RocksDBInner,
) -> anyhow::Result<()> {
    tx.execute_batch(
        r"
ALTER TABLE block_headers RENAME COLUMN gas_price TO eth_l1_gas_price;
ALTER TABLE block_headers ADD COLUMN strk_l1_gas_price BLOB DEFAULT NULL;
",
    )
    .context("Creating block_signatures table")?;

    Ok(())
}
