use anyhow::Context;

pub(crate) fn migrate(
    tx: &rusqlite::Transaction<'_>,
    _rocksdb: &crate::RocksDBInner,
) -> anyhow::Result<()> {
    tracing::info!("Changing l2_gas_price default value");

    tx.execute(
        "UPDATE block_headers SET eth_l2_gas_price = X'00000000000000000000000000000001' WHERE \
         eth_l2_gas_price = X'00000000000000000000000000000000'",
        [],
    )
    .context("Updating block_headers.eth_l2_gas_price")?;
    tx.execute(
        "UPDATE block_headers SET strk_l2_gas_price = X'00000000000000000000000000000001' WHERE \
         strk_l2_gas_price = X'00000000000000000000000000000000'",
        [],
    )
    .context("Updating block_headers.strk_l2_gas_price")?;

    Ok(())
}
