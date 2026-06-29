use anyhow::Context;

pub(crate) fn migrate(
    tx: &rusqlite::Transaction<'_>,
    _rocksdb: &crate::RocksDBInner,
) -> anyhow::Result<()> {
    tracing::info!("Storing parent_hash inline");

    // Default of 0x0 so that the genesis block's parent hash is correct.
    tx.execute(
        "ALTER TABLE block_headers ADD COLUMN parent_hash BLOB NOT NULL DEFAULT x'00'",
        [],
    )
    .context("Adding block_headers.parent_hash column")?;

    // Select the parent hash from the previous row. Skip the genesis block since
    // its parent hash is 0x0 and sqlite does not allow for default values
    // within the update (select cannot return data for rows that do not exist).
    tx.execute(
        r"UPDATE block_headers SET parent_hash = ( 
            SELECT hash FROM block_headers AS parent WHERE parent.number = block_headers.number - 1 
         ) WHERE number > 0",
        [],
    )
    .context("Updating block_headers.parent_hash")?;

    Ok(())
}
