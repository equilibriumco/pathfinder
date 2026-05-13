use anyhow::Context;

pub(crate) fn migrate(
    tx: &rusqlite::Transaction<'_>,
    _rocksdb: &crate::RocksDBInner,
) -> anyhow::Result<()> {
    tracing::info!("Renaming storage_flags table to storage_options and adding value column");

    tx.execute("ALTER TABLE storage_flags RENAME TO storage_options", [])
        .context("Renaming storage_flags table to storage_options")?;
    tx.execute(
        "ALTER TABLE storage_options RENAME COLUMN flag TO option",
        [],
    )
    .context("Renaming flag column to option")?;
    tx.execute("ALTER TABLE storage_options ADD COLUMN value INTEGER", [])
        .context("Adding value column to storage_options table")?;

    Ok(())
}
