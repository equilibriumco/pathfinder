use anyhow::Context;

pub(crate) fn migrate(
    tx: &rusqlite::Transaction<'_>,
    _rocksdb: &crate::RocksDBInner,
) -> anyhow::Result<()> {
    tx.execute_batch(
        "
    DROP INDEX contract_roots_address_block_number;
    CREATE UNIQUE INDEX contract_roots_address_block_number ON contract_roots(contract_address, \
         block_number);",
    )
    .context("Re-creating contract_roots_address_block_number index as unique")
}
