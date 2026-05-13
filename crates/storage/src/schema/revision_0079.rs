use anyhow::Context;

use crate::{NONCE_UPDATES_COLUMN, STORAGE_UPDATES_COLUMN, TRANSACTIONS_AND_RECEIPTS_COLUMN};

pub(crate) fn migrate(
    tx: &rusqlite::Transaction<'_>,
    rocksdb: &crate::RocksDBInner,
) -> anyhow::Result<()> {
    tracing::info!("Migrating nonce updates to RocksDB");
    migrate_nonce_updates(tx, rocksdb)?;

    tracing::info!("Migrating storage updates to RocksDB");
    migrate_storage_updates(tx, rocksdb)?;

    tracing::info!("Migrating transactions and receipts to RocksDB");
    migrate_transactions_and_receipts(tx, rocksdb)?;

    tracing::info!("Migrating transaction hashes to RocksDB");
    migrate_transaction_hashes(tx, rocksdb)?;

    tracing::info!("Migrating events to RocksDB");
    migrate_events(tx, rocksdb)?;

    // tx.execute_batch(
    //     "
    //     DROP TABLE nonce_updates;
    //     DROP TABLE storage_updates;
    //     ",
    // )?;

    Ok(())
}

const BATCH_SIZE: usize = 1_000_000;

fn nonce_update_key(block_number: u64, contract_address: &[u8; 32]) -> [u8; 36] {
    let block_number: u32 = block_number.try_into().expect("block number fits into u32");
    let block_number = u32::MAX - block_number;

    let mut key = [0u8; 36];
    key[..32].copy_from_slice(contract_address);
    key[32..].copy_from_slice(&block_number.to_be_bytes());
    key
}

fn storage_update_key(
    block_number: u64,
    contract_address: &[u8; 32],
    storage_address: &[u8; 32],
) -> [u8; 68] {
    let block_number: u32 = block_number.try_into().expect("block number fits into u32");
    let block_number = u32::MAX - block_number;

    let mut key = [0u8; 68];
    key[..32].copy_from_slice(contract_address);
    key[32..64].copy_from_slice(storage_address);
    key[64..].copy_from_slice(&block_number.to_be_bytes());
    key
}

fn migrate_nonce_updates(
    tx: &rusqlite::Transaction<'_>,
    rocksdb: &crate::RocksDBInner,
) -> anyhow::Result<()> {
    let mut stmt = tx
        .prepare(
            "SELECT nonce_updates.block_number, contract_addresses.contract_address, \
             nonce_updates.nonce FROM nonce_updates JOIN contract_addresses ON \
             contract_addresses.id = nonce_updates.contract_address_id",
        )
        .context("Preparing nonce updates query")?;

    let rows = stmt
        .query_map([], |row| {
            let block_number: u64 = row.get(0)?;
            let contract_address: [u8; 32] = row.get(1)?;
            let nonce: Vec<u8> = row.get(2)?;
            Ok((block_number, contract_address, nonce))
        })
        .context("Querying nonce updates")?;

    let column = rocksdb.get_column(&NONCE_UPDATES_COLUMN);
    let mut batch = crate::RocksDBBatch::default();

    for (i, row) in rows.enumerate() {
        let (block_number, contract_address, nonce) = row.context("Reading nonce update row")?;

        let key = nonce_update_key(block_number, &contract_address);
        batch.put_cf(&column, key, &nonce);

        if i % BATCH_SIZE == BATCH_SIZE - 1 {
            rocksdb
                .rocksdb
                .write_without_wal(&batch)
                .context("Writing nonce updates batch to RocksDB")?;
            batch = crate::RocksDBBatch::default();
            tracing::info!("Migrated {} nonce update entries", i + 1);
        }
    }

    rocksdb
        .rocksdb
        .write_without_wal(&batch)
        .context("Writing final nonce updates batch to RocksDB")?;
    tracing::info!("Nonce updates migration complete");

    Ok(())
}

fn migrate_storage_updates(
    tx: &rusqlite::Transaction<'_>,
    rocksdb: &crate::RocksDBInner,
) -> anyhow::Result<()> {
    let mut stmt = tx
        .prepare(
            "SELECT storage_updates.block_number, contract_addresses.contract_address, \
             storage_addresses.storage_address, storage_updates.storage_value FROM \
             storage_updates JOIN contract_addresses ON contract_addresses.id = \
             storage_updates.contract_address_id JOIN storage_addresses ON storage_addresses.id = \
             storage_updates.storage_address_id",
        )
        .context("Preparing storage updates query")?;

    let rows = stmt
        .query_map([], |row| {
            let block_number: u64 = row.get(0)?;
            let contract_address: [u8; 32] = row.get(1)?;
            let storage_address: [u8; 32] = row.get(2)?;
            let storage_value: Vec<u8> = row.get(3)?;
            Ok((
                block_number,
                contract_address,
                storage_address,
                storage_value,
            ))
        })
        .context("Querying storage updates")?;

    let column = rocksdb.get_column(&STORAGE_UPDATES_COLUMN);
    let mut batch = crate::RocksDBBatch::default();

    for (i, row) in rows.enumerate() {
        let (block_number, contract_address, storage_address, storage_value) =
            row.context("Reading storage update row")?;

        let key = storage_update_key(block_number, &contract_address, &storage_address);
        batch.put_cf(&column, key, &storage_value);

        if i % BATCH_SIZE == BATCH_SIZE - 1 {
            rocksdb
                .rocksdb
                .write_without_wal(&batch)
                .context("Writing storage updates batch to RocksDB")?;
            batch = crate::RocksDBBatch::default();
            tracing::info!("Migrated {} storage update entries", i + 1);
        }
    }

    tracing::info!("Last batch of storage updates with {} entries", batch.len());

    rocksdb
        .rocksdb
        .write_without_wal(&batch)
        .context("Writing final storage updates batch to RocksDB")?;
    tracing::info!("Storage updates migration complete");

    Ok(())
}

fn migrate_transactions_and_receipts(
    tx: &rusqlite::Transaction<'_>,
    rocksdb: &crate::RocksDBInner,
) -> anyhow::Result<()> {
    let mut stmt = tx
        .prepare("SELECT transactions.block_number, transactions.transactions FROM transactions")
        .context("Preparing transactions and receipts query")?;

    let rows = stmt
        .query_map([], |row| {
            let block_number: u64 = row.get(0)?;
            let data: Vec<u8> = row.get(1)?;
            Ok((block_number, data))
        })
        .context("Querying transactions and receipts")?;

    let column = rocksdb.get_column(&TRANSACTIONS_AND_RECEIPTS_COLUMN);
    let mut batch = crate::RocksDBBatch::default();

    const BATCH_SIZE: usize = 10_000;

    for (i, row) in rows.enumerate() {
        let (block_number, data) = row.context("Reading transactions and receipts row")?;

        let key = block_number.to_be_bytes();
        batch.put_cf(&column, key, &data);

        if i % BATCH_SIZE == BATCH_SIZE - 1 {
            rocksdb
                .rocksdb
                .write_without_wal(&batch)
                .context("Writing transactions and receipts batch to RocksDB")?;
            batch = crate::RocksDBBatch::default();
            tracing::info!("Migrated {} transactions and receipts entries", i + 1);
        }
    }

    rocksdb
        .rocksdb
        .write_without_wal(&batch)
        .context("Writing final transactions and receipts batch to RocksDB")?;
    tracing::info!("Transactions and receipts migration complete");

    Ok(())
}

fn migrate_transaction_hashes(
    tx: &rusqlite::Transaction<'_>,
    rocksdb: &crate::RocksDBInner,
) -> anyhow::Result<()> {
    let mut stmt = tx
        .prepare("SELECT hash, block_number, idx FROM transaction_hashes")
        .context("Preparing transaction hashes query")?;

    let rows = stmt
        .query_map([], |row| {
            let hash: Vec<u8> = row.get(0)?;
            let block_number: u64 = row.get(1)?;
            let idx: u16 = row.get(2)?;
            Ok((hash, block_number, idx))
        })
        .context("Querying transaction hashes")?;

    let column = rocksdb.get_column(&TRANSACTIONS_AND_RECEIPTS_COLUMN);
    let mut batch = crate::RocksDBBatch::default();

    for (i, row) in rows.enumerate() {
        let (hash, block_number, idx) = row.context("Reading transaction hashes row")?;

        let mut buffer = [0u8; 10];
        buffer[..8].copy_from_slice(&block_number.to_be_bytes());
        buffer[8..].copy_from_slice(&idx.to_be_bytes());

        batch.put_cf(&column, hash.as_slice(), &buffer);

        if i % BATCH_SIZE == BATCH_SIZE - 1 {
            rocksdb
                .rocksdb
                .write_without_wal(&batch)
                .context("Writing transaction hashes batch to RocksDB")?;
            batch = crate::RocksDBBatch::default();
            tracing::info!("Migrated {} transaction hashes entries", i + 1);
        }
    }

    rocksdb
        .rocksdb
        .write_without_wal(&batch)
        .context("Writing final transaction hashes batch to RocksDB")?;
    tracing::info!("Transaction hashes migration complete");

    Ok(())
}

fn migrate_events(
    tx: &rusqlite::Transaction<'_>,
    rocksdb: &crate::RocksDBInner,
) -> anyhow::Result<()> {
    let mut stmt = tx
        .prepare("SELECT transactions.block_number, transactions.events FROM transactions")
        .context("Preparing events query")?;

    let rows = stmt
        .query_map([], |row| {
            let block_number: u64 = row.get(0)?;
            let data: Vec<u8> = row.get(1)?;
            Ok((block_number, data))
        })
        .context("Querying events")?;

    let column = rocksdb.get_column(&crate::connection::EVENTS_COLUMN);
    let mut batch = crate::RocksDBBatch::default();

    const BATCH_SIZE: usize = 10_000;

    for (i, row) in rows.enumerate() {
        let (block_number, data) = row.context("Reading events row")?;

        let key = block_number.to_be_bytes();
        batch.put_cf(&column, key, &data);

        if i % BATCH_SIZE == BATCH_SIZE - 1 {
            rocksdb
                .rocksdb
                .write_without_wal(&batch)
                .context("Writing events batch to RocksDB")?;
            batch = crate::RocksDBBatch::default();
            tracing::info!("Migrated {} events entries", i + 1);
        }
    }

    rocksdb
        .rocksdb
        .write_without_wal(&batch)
        .context("Writing final events batch to RocksDB")?;
    tracing::info!("Events migration complete");

    Ok(())
}
