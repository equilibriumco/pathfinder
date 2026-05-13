use anyhow::Context;

/// Add a new table for storing data about re-declared classes.
///
/// The Starknet feeder gateway returns inconsistent state diffs for some
/// blocks: `old_declared_contracts` _may_ contain classes that have already
/// been declared in previous blocks.
///
/// For example state update for block 6356 has class
/// [0x0699053487675242dc0958e192c17fe4dd57d22238ad78e2e1807fa7919ffde0](https://sepolia.starkscan.co/class/0x0699053487675242dc0958e192c17fe4dd57d22238ad78e2e1807fa7919ffde0)
///  in `old_declared_contracts`. However, that class was in fact first declared
/// in block 6355 and then re-declared in 6356. This is an inconsistency in the
/// state diff: even though a second DECLARE transaction was included in that
/// block for the class it is not a diff so it should not have been included in
/// the state diff at all.
///
/// Pathfinder stores only the very first block a contract was declared at, so
/// we cannot reproduce this (inconsistent) state diff at all -- which poses a
/// problem that the state diff commitment calculated by Pathfinder using our
/// representation of the state diff won't match the one from the feeder gateway
/// (and Juno).
///
/// This migration adds a new `redeclared_classes` table storing class
/// re-declarations data. When inserting a state update we take care of adding
/// new rows here if the state update contains a re-declaration -- and then when
/// retrieving the state update from storage we add re-declared classes to the
/// set of Cairo classes declared at the block.
///
/// This allows us to reproduce the exact same state diff as the one we've
/// received from the feeder gateway (or from other nodes via P2P).
pub(crate) fn migrate(
    tx: &rusqlite::Transaction<'_>,
    _rocksdb: &crate::RocksDBInner,
) -> anyhow::Result<()> {
    tracing::info!("Adding redeclared_classes table");

    tx.execute_batch(
        r"CREATE TABLE redeclared_classes (
            class_hash BLOB NOT NULL,
            block_number INTEGER NOT NULL REFERENCES block_headers(number) ON DELETE CASCADE
        );
        CREATE INDEX redeclared_classes_block_number ON redeclared_classes(block_number);",
    )
    .context("Adding redeclared_classes table")?;

    Ok(())
}
