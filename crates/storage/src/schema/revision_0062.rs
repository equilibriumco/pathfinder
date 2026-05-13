use anyhow::Context;

pub(crate) fn migrate(
    tx: &rusqlite::Transaction<'_>,
    _rocksdb: &crate::RocksDBInner,
) -> anyhow::Result<()> {
    tracing::info!("Adding receipt_commitment to block_headers");

    // This field is required for p2p, so the default value makes this migration
    // compatible with the current prod build.
    tx.execute(
        "ALTER TABLE block_headers ADD COLUMN receipt_commitment BLOB NOT NULL DEFAULT x'00'",
        [],
    )
    .context("Adding block_headers.receipt_commitment column")?;

    Ok(())
}
