use anyhow::Context;

pub(crate) fn migrate(
    tx: &rusqlite::Transaction<'_>,
    _rocksdb: &crate::RocksDBInner,
) -> anyhow::Result<()> {
    tracing::info!("Adding new columns to block_headers table");

    tx.execute_batch(
        r"ALTER TABLE block_headers ADD COLUMN storage_diffs_count INTEGER DEFAULT 0;
        ALTER TABLE block_headers ADD COLUMN nonce_updates_count INTEGER DEFAULT 0;
        ALTER TABLE block_headers ADD COLUMN declared_classes_count INTEGER DEFAULT 0;
        ALTER TABLE block_headers ADD COLUMN deployed_contracts_count INTEGER DEFAULT 0;

        UPDATE block_headers SET
            storage_diffs_count=(SELECT COUNT(1) FROM storage_updates WHERE block_number=block_headers.number),
            nonce_updates_count=(SELECT COUNT(1) FROM nonce_updates WHERE block_number=block_headers.number),
            declared_classes_count=(SELECT COUNT(1) FROM class_definitions WHERE block_number=block_headers.number),
            deployed_contracts_count=(SELECT COUNT(1) FROM contract_updates WHERE block_number=block_headers.number);",
    )
    .context("Adding new columns to block_headers")
}
