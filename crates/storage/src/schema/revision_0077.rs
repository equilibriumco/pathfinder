/// This is a no-op migration added to bump the revision number to 77.
///
/// No schema changes are needed in this revision, but a new serialization
/// format for transactions has been added that makes newly added transactions
/// incompatible with older versions.
pub(crate) fn migrate(
    _tx: &rusqlite::Transaction<'_>,
    _rocksdb: &crate::RocksDBInner,
) -> anyhow::Result<()> {
    Ok(())
}
