use std::time::Instant;

use anyhow::Context;
use rusqlite::params;

pub(crate) fn migrate(
    tx: &rusqlite::Transaction<'_>,
    _rocksdb: &crate::RocksDBInner,
) -> anyhow::Result<()> {
    tx.execute_batch(
        r"
        CREATE UNIQUE INDEX block_headers_number ON block_headers(number);
        DROP INDEX starknet_blocks_block_number;
        CREATE TABLE contract_addresses (
            id INTEGER PRIMARY KEY,
            contract_address BLOB
        );
        CREATE UNIQUE INDEX contract_addresses_contract_address ON contract_addresses (contract_address);
        CREATE TABLE storage_addresses (
            id INTEGER PRIMARY KEY,
            storage_address BLOB
        );
        CREATE UNIQUE INDEX storage_addresses_storage_address ON storage_addresses (storage_address);
        CREATE TABLE storage_updates_normalized (
            block_number INTEGER REFERENCES block_headers(number) ON DELETE CASCADE,
            contract_address_id INTEGER REFERENCES contract_addresses(id),
            storage_address_id INTEGER REFERENCES storage_addresses(id),
            storage_value BLOB NOT NULL
        );
        ",
    )
    .context("Creating contract_addresses table")?;

    let mut storage_updates_query = tx
        .prepare_cached(
            "SELECT block_number, contract_address, storage_address, storage_value FROM \
             storage_updates",
        )
        .context("Querying storage_updates")?;
    let mut storage_updates = storage_updates_query
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, Vec<u8>>(2)?,
                row.get::<_, Vec<u8>>(3)?,
            ))
        })
        .context("Iterating over storage_updates")?;
    let mut contract_addresses_query = tx
        .prepare_cached("SELECT id FROM contract_addresses WHERE contract_address = ?")
        .context("Querying contract_addresses")?;
    let mut contract_addresses_insert = tx
        .prepare_cached("INSERT INTO contract_addresses (contract_address) VALUES (?) RETURNING id")
        .context("Inserting into contract_addresses")?;
    let mut storage_addresses_query = tx
        .prepare_cached("SELECT id FROM storage_addresses WHERE storage_address = ?")
        .context("Querying storage_addresses")?;
    let mut storage_addresses_insert = tx
        .prepare_cached("INSERT INTO storage_addresses (storage_address) VALUES (?) RETURNING id")
        .context("Inserting into storage_addresses")?;
    let mut storage_updates_insert = tx
        .prepare_cached(
            "INSERT INTO storage_updates_normalized (block_number, contract_address_id, \
             storage_address_id, storage_value) VALUES (?, ?, ?, ?)",
        )
        .context("Inserting into storage_updates_normalized")?;
    let storage_updates_count = tx
        .query_row("SELECT COUNT(*) FROM storage_updates", [], |row| {
            row.get::<_, i64>(0)
        })
        .context("Counting storage_updates")?;
    let mut last_progress = Instant::now();
    let mut i = 0;
    loop {
        let Some((block_number, contract_address, storage_address, storage_value)) =
            storage_updates.next().transpose()?
        else {
            break;
        };
        if last_progress.elapsed().as_secs() >= 10 {
            tracing::info!(
                "Migrating storage_updates: {:.2}% ({}/{})",
                i as f64 / storage_updates_count as f64 * 100.0,
                i,
                storage_updates_count
            );
            last_progress = Instant::now();
        }
        let contract_address_id = contract_addresses_query
            .query_map(params![&contract_address], |row| row.get::<_, i64>(0))
            .context("Querying contract_addresses")?
            .next()
            .unwrap_or_else(|| {
                contract_addresses_insert
                    .query_row(params![&contract_address], |row| row.get::<_, i64>(0))
            })
            .context("Inserting into contract_addresses")?;
        let storage_address_id = storage_addresses_query
            .query_map(params![&storage_address], |row| row.get::<_, i64>(0))
            .context("Querying storage_addresses")?
            .next()
            .unwrap_or_else(|| {
                storage_addresses_insert
                    .query_row(params![&storage_address], |row| row.get::<_, i64>(0))
            })
            .context("Inserting into storage_addresses")?;
        storage_updates_insert
            .execute(params![
                block_number,
                contract_address_id,
                storage_address_id,
                storage_value
            ])
            .context("Inserting into storage_updates_normalized")?;
        i += 1;
    }

    tracing::info!(
        "Migrating storage_updates: {:.2}% ({}/{})",
        i as f64 / storage_updates_count as f64 * 100.0,
        i,
        storage_updates_count
    );

    tracing::info!("Dropping storage_updates and renaming temporary table, creating indices");

    tx.execute_batch(
        r"
        DROP TABLE storage_updates;
        ALTER TABLE storage_updates_normalized RENAME TO storage_updates;
        CREATE INDEX storage_updates_contract_address_id_storage_address_id_block_number ON storage_updates(contract_address_id, storage_address_id, block_number);
        CREATE INDEX storage_updates_block_number ON storage_updates(block_number);
        ",
    )
    .context("Dropping storage_updates and creating indices")?;

    Ok(())
}
