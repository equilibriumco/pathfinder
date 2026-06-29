use std::time::Instant;

use anyhow::Context;
use rusqlite::params;

pub(crate) fn migrate(
    tx: &rusqlite::Transaction<'_>,
    _rocksdb: &crate::RocksDBInner,
) -> anyhow::Result<()> {
    tx.execute_batch(
        r"
        CREATE TABLE nonce_updates_normalized (
            block_number INTEGER REFERENCES canonical_blocks(number) ON DELETE CASCADE,
            contract_address_id INTEGER NOT NULL REFERENCES contract_addresses(id),
            nonce BLOB NOT NULL
        );
        ",
    )
    .context("Creating nonce_updates_normalized table")?;

    let mut nonce_updates_query = tx
        .prepare_cached("SELECT block_number, contract_address, nonce FROM nonce_updates")
        .context("Querying nonce_updates")?;
    let mut nonce_updates = nonce_updates_query
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, Vec<u8>>(2)?,
            ))
        })
        .context("Iterating over nonce_updates")?;
    let mut contract_addresses_query = tx
        .prepare_cached("SELECT id FROM contract_addresses WHERE contract_address = ?")
        .context("Querying contract_addresses")?;
    let mut contract_addresses_insert = tx
        .prepare_cached("INSERT INTO contract_addresses (contract_address) VALUES (?) RETURNING id")
        .context("Inserting into contract_addresses")?;
    let mut nonce_updates_insert = tx
        .prepare_cached(
            "INSERT INTO nonce_updates_normalized (block_number, contract_address_id, nonce) \
             VALUES (?, ?, ?)",
        )
        .context("Inserting into nonce_updates_normalized")?;
    let nonce_updates_count = tx
        .query_row("SELECT COUNT(*) FROM nonce_updates", [], |row| {
            row.get::<_, i64>(0)
        })
        .context("Counting nonce_updates")?;
    let mut last_progress = Instant::now();
    let mut i = 0;
    loop {
        let Some((block_number, contract_address, nonce)) = nonce_updates.next().transpose()?
        else {
            break;
        };
        if last_progress.elapsed().as_secs() >= 10 {
            tracing::info!(
                "Migrating nonce_updates: {:.2}% ({}/{})",
                i as f64 / nonce_updates_count as f64 * 100.0,
                i,
                nonce_updates_count
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
        nonce_updates_insert
            .execute(params![block_number, contract_address_id, nonce])
            .context("Inserting into nonce_updates_normalized")?;
        i += 1;
    }

    tracing::info!(
        "Migrating nonce_updates: {:.2}% ({}/{})",
        i as f64 / nonce_updates_count as f64 * 100.0,
        i,
        nonce_updates_count
    );

    tracing::info!("Dropping nonce_updates and renaming temporary table, creating indices");

    tx.execute_batch(
        r"
        DROP TABLE nonce_updates;
        ALTER TABLE nonce_updates_normalized RENAME TO nonce_updates;
        CREATE INDEX nonce_updates_block_number ON nonce_updates(block_number);
        CREATE INDEX nonce_updates_contract_address_id_block_number ON nonce_updates(contract_address_id, block_number);
        ",
    )
    .context("Dropping nonce_updates and creating indices")?;

    Ok(())
}
