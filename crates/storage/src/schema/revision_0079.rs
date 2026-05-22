use std::collections::{HashMap, HashSet};

use anyhow::Context;
use pathfinder_common::state_update::{
    ContractClassUpdate,
    ContractUpdate,
    StateUpdateData,
    SystemContractUpdate,
};
use pathfinder_common::{
    CasmHash,
    ClassHash,
    ContractAddress,
    ContractNonce,
    SierraHash,
    StorageAddress,
    StorageValue,
};
use pathfinder_crypto::Felt;

use crate::connection::state_update::{dto, STATE_UPDATES_COLUMN};
use crate::{
    NONCE_UPDATES_COLUMN,
    STORAGE_UPDATES_COLUMN,
    TRANSACTIONS_AND_RECEIPTS_COLUMN,
    TRANSACTION_HASHES_COLUMN,
};

pub(crate) fn migrate(
    tx: &rusqlite::Transaction<'_>,
    rocksdb: &crate::RocksDBInner,
) -> anyhow::Result<()> {
    tracing::info!("Migrating state updates to RocksDB");
    migrate_state_updates(tx, rocksdb)?;

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

    Ok(())
}

fn migrate_state_updates(
    tx: &rusqlite::Transaction<'_>,
    rocksdb: &crate::RocksDBInner,
) -> anyhow::Result<()> {
    let mut block_stmt = tx
        .prepare("SELECT number FROM block_headers ORDER BY number")
        .context("Preparing block numbers query")?;

    let mut nonce_stmt = tx
        .prepare(
            "SELECT contract_address, nonce FROM nonce_updates JOIN contract_addresses ON \
             contract_addresses.id = nonce_updates.contract_address_id WHERE block_number = ?",
        )
        .context("Preparing nonce updates query")?;

    let mut storage_stmt = tx
        .prepare(
            "SELECT contract_address, storage_address, storage_value FROM storage_updates JOIN \
             contract_addresses ON contract_addresses.id = storage_updates.contract_address_id \
             JOIN storage_addresses ON storage_addresses.id = storage_updates.storage_address_id \
             WHERE block_number = ?",
        )
        .context("Preparing storage updates query")?;

    let mut declared_stmt = tx
        .prepare(
            "SELECT class_definitions.hash AS class_hash, casm_class_hashes.compiled_class_hash \
             AS compiled_class_hash FROM class_definitions LEFT OUTER JOIN casm_class_hashes ON \
             casm_class_hashes.hash = class_definitions.hash AND casm_class_hashes.block_number = \
             class_definitions.block_number WHERE class_definitions.block_number = ?",
        )
        .context("Preparing declared classes query")?;

    let mut redeclared_stmt = tx
        .prepare("SELECT class_hash FROM redeclared_classes WHERE block_number = ?")
        .context("Preparing redeclared classes query")?;

    let mut migrated_stmt = tx
        .prepare(
            "SELECT ch1.hash AS class_hash, ch1.compiled_class_hash AS casm_hash FROM \
             casm_class_hashes ch1 LEFT OUTER JOIN casm_class_hashes ch2 ON ch1.hash = ch2.hash \
             AND ch2.block_number < ch1.block_number WHERE ch1.block_number = ? AND \
             ch2.block_number IS NOT NULL",
        )
        .context("Preparing migrated compiled classes query")?;

    let mut contract_update_stmt = tx
        .prepare(
            "SELECT cu1.contract_address AS contract_address, cu1.class_hash AS class_hash, \
             EXISTS(SELECT 1 FROM contract_updates cu2 WHERE cu2.contract_address = \
             cu1.contract_address AND cu2.block_number < cu1.block_number) AS is_replaced FROM \
             contract_updates cu1 WHERE cu1.block_number = ?",
        )
        .context("Preparing contract updates query")?;

    let block_numbers: Vec<u64> = block_stmt
        .query_map([], |row| row.get(0))
        .context("Querying block numbers")?
        .collect::<Result<_, _>>()
        .context("Reading block numbers")?;

    let column = rocksdb.get_column(&STATE_UPDATES_COLUMN);
    let mut batch = crate::RocksDBBatch::default();

    const BATCH_SIZE: usize = 10_000;

    for (i, &block_number) in block_numbers.iter().enumerate() {
        // 1. Nonce updates
        let mut contract_updates: HashMap<ContractAddress, ContractUpdate> = HashMap::new();
        let nonce_rows = nonce_stmt
            .query_map([block_number], |row| {
                let contract_address: [u8; 32] = row.get(0)?;
                let nonce: Vec<u8> = row.get(1)?;
                Ok((contract_address, nonce))
            })
            .context("Querying nonce updates for block")?;

        for row in nonce_rows {
            let (addr_bytes, nonce_bytes) = row.context("Reading nonce row")?;
            let felt = Felt::from_be_slice(&addr_bytes).context("Parsing contract address felt")?;
            let address = ContractAddress::new(felt).context("Creating contract address")?;
            let nonce =
                ContractNonce(Felt::from_be_slice(&nonce_bytes).context("Parsing nonce felt")?);
            contract_updates.entry(address).or_default().nonce = Some(nonce);
        }

        // 2. Storage updates
        let mut system_contract_updates: HashMap<ContractAddress, SystemContractUpdate> =
            HashMap::new();
        let storage_rows = storage_stmt
            .query_map([block_number], |row| {
                let contract_address: [u8; 32] = row.get(0)?;
                let storage_address: [u8; 32] = row.get(1)?;
                let storage_value: Vec<u8> = row.get(2)?;
                Ok((contract_address, storage_address, storage_value))
            })
            .context("Querying storage updates for block")?;

        for row in storage_rows {
            let (addr_bytes, key_bytes, val_bytes) = row.context("Reading storage row")?;
            let felt = Felt::from_be_slice(&addr_bytes).context("Parsing contract address felt")?;
            let address = ContractAddress::new(felt).context("Creating contract address")?;
            let key = StorageAddress(
                Felt::from_be_slice(&key_bytes).context("Parsing storage address felt")?,
            );
            let value = StorageValue(
                Felt::from_be_slice(&val_bytes).context("Parsing storage value felt")?,
            );

            if address.is_system_contract() {
                system_contract_updates
                    .entry(address)
                    .or_default()
                    .storage
                    .insert(key, value);
            } else {
                contract_updates
                    .entry(address)
                    .or_default()
                    .storage
                    .insert(key, value);
            }
        }

        // 3. Declared classes (sierra + cairo)
        let mut declared_cairo_classes: HashSet<ClassHash> = HashSet::new();
        let mut declared_sierra_classes: HashMap<SierraHash, CasmHash> = HashMap::new();
        let declared_rows = declared_stmt
            .query_map([block_number], |row| {
                let class_hash: [u8; 32] = row.get(0)?;
                let casm_hash: Option<[u8; 32]> = row.get(1)?;
                Ok((class_hash, casm_hash))
            })
            .context("Querying declared classes for block")?;

        for row in declared_rows {
            let (class_bytes, casm_bytes) = row.context("Reading declared class row")?;
            let class_felt =
                Felt::from_be_slice(&class_bytes).context("Parsing class hash felt")?;
            match casm_bytes {
                Some(casm_bytes) => {
                    let casm_felt =
                        Felt::from_be_slice(&casm_bytes).context("Parsing casm hash felt")?;
                    declared_sierra_classes.insert(SierraHash(class_felt), CasmHash(casm_felt));
                }
                None => {
                    declared_cairo_classes.insert(ClassHash(class_felt));
                }
            }
        }

        // 4. Re-declared classes
        let redeclared_rows = redeclared_stmt
            .query_map([block_number], |row| {
                let class_hash: [u8; 32] = row.get(0)?;
                Ok(class_hash)
            })
            .context("Querying redeclared classes for block")?;

        for row in redeclared_rows {
            let class_bytes = row.context("Reading redeclared class row")?;
            let class_felt =
                Felt::from_be_slice(&class_bytes).context("Parsing class hash felt")?;
            declared_cairo_classes.insert(ClassHash(class_felt));
        }

        // 5. Migrated compiled classes
        let mut migrated_compiled_classes: HashMap<SierraHash, CasmHash> = HashMap::new();
        let migrated_rows = migrated_stmt
            .query_map([block_number], |row| {
                let class_hash: [u8; 32] = row.get(0)?;
                let casm_hash: [u8; 32] = row.get(1)?;
                Ok((class_hash, casm_hash))
            })
            .context("Querying migrated compiled classes for block")?;

        for row in migrated_rows {
            let (class_bytes, casm_bytes) = row.context("Reading migrated class row")?;
            let class_felt =
                Felt::from_be_slice(&class_bytes).context("Parsing class hash felt")?;
            let casm_felt = Felt::from_be_slice(&casm_bytes).context("Parsing casm hash felt")?;
            migrated_compiled_classes.insert(SierraHash(class_felt), CasmHash(casm_felt));
        }

        // 6. Contract updates (deploy/replace)
        let contract_rows = contract_update_stmt
            .query_map([block_number], |row| {
                let contract_address: [u8; 32] = row.get(0)?;
                let class_hash: [u8; 32] = row.get(1)?;
                let is_replaced: bool = row.get(2)?;
                Ok((contract_address, class_hash, is_replaced))
            })
            .context("Querying contract updates for block")?;

        for row in contract_rows {
            let (addr_bytes, class_bytes, is_replaced) =
                row.context("Reading contract update row")?;
            let felt = Felt::from_be_slice(&addr_bytes).context("Parsing contract address felt")?;
            let address = ContractAddress::new(felt).context("Creating contract address")?;
            let class_hash =
                ClassHash(Felt::from_be_slice(&class_bytes).context("Parsing class hash felt")?);

            let class_update = if is_replaced {
                ContractClassUpdate::Replace(class_hash)
            } else {
                ContractClassUpdate::Deploy(class_hash)
            };
            contract_updates.entry(address).or_default().class = Some(class_update);
        }

        // Build and serialize the StateUpdateData
        let state_update_data = StateUpdateData {
            contract_updates,
            system_contract_updates,
            declared_cairo_classes,
            declared_sierra_classes,
            migrated_compiled_classes,
        };
        let dto_data = dto::StateUpdateData::from(state_update_data);
        let data = bincode::serde::encode_to_vec(dto_data, bincode::config::standard())
            .context("Encoding state update data")?;

        let key = block_number.to_be_bytes();
        batch.put_cf(&column, key, &data);

        if i % BATCH_SIZE == BATCH_SIZE - 1 {
            rocksdb
                .rocksdb
                .write_without_wal(&batch)
                .context("Writing state updates batch to RocksDB")?;
            batch = crate::RocksDBBatch::default();
            tracing::info!("Migrated {} state update entries", i + 1);
        }
    }

    rocksdb
        .rocksdb
        .write_without_wal(&batch)
        .context("Writing final state updates batch to RocksDB")?;
    tracing::info!(
        "State updates migration complete ({} blocks)",
        block_numbers.len()
    );

    Ok(())
}

const BATCH_SIZE: usize = 1_000_000;

fn nonce_update_key(block_number: u64, contract_address: &[u8; 32]) -> [u8; 40] {
    let block_number = u64::MAX - block_number;

    let mut key = [0u8; 40];
    key[..32].copy_from_slice(contract_address);
    key[32..].copy_from_slice(&block_number.to_be_bytes());
    key
}

fn storage_update_key(
    block_number: u64,
    contract_address: &[u8; 32],
    storage_address: &[u8; 32],
) -> [u8; 72] {
    let block_number = u64::MAX - block_number;

    let mut key = [0u8; 72];
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

    let column = rocksdb.get_column(&TRANSACTION_HASHES_COLUMN);
    let mut batch = crate::RocksDBBatch::default();

    for (i, row) in rows.enumerate() {
        let (hash, block_number, idx) = row.context("Reading transaction hashes row")?;

        let mut buffer = [0u8; 10];
        buffer[..8].copy_from_slice(&block_number.to_be_bytes());
        buffer[8..].copy_from_slice(&idx.to_be_bytes());

        batch.put_cf(&column, hash.as_slice(), buffer);

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
            let data: Option<Vec<u8>> = row.get(1)?;
            Ok((block_number, data))
        })
        .context("Querying events")?;

    let column = rocksdb.get_column(&crate::connection::EVENTS_COLUMN);
    let mut batch = crate::RocksDBBatch::default();

    const BATCH_SIZE: usize = 10_000;

    for (i, row) in rows.enumerate() {
        let (block_number, data) = row.context("Reading events row")?;
        let Some(data) = data else { continue };

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

#[cfg(test)]
mod tests {
    use pathfinder_common::macro_prelude::*;
    use rusqlite::params;

    use crate::connection::TRANSACTION_HASHES_COLUMN;

    /// Verifies that `migrate_transaction_hashes` writes hashes to
    /// `TRANSACTION_HASHES_COLUMN` (the column the runtime read path uses),
    /// not `TRANSACTIONS_AND_RECEIPTS_COLUMN`.
    #[test]
    fn migrate_transaction_hashes_targets_transaction_hashes_column() {
        let rocksdb_dir = tempfile::tempdir().unwrap();
        let rocksdb = crate::StorageBuilder::open_rocksdb(rocksdb_dir.path()).unwrap();

        let mut conn = rusqlite::Connection::open_in_memory().unwrap();
        // Bring SQLite to the pre-0079 state by applying base + all prior migrations.
        let tx = conn.transaction().unwrap();
        crate::schema::base_schema(&tx).unwrap();
        tx.commit().unwrap();
        // Run migrations through revision_0078 only (index 37). Later revisions
        // drop SQLite tables that this test's INSERT relies on.
        let prior = &crate::schema::migrations()[..38];
        for migration in prior {
            let tx = conn.transaction().unwrap();
            migration(&tx, &rocksdb).unwrap();
            tx.commit().unwrap();
        }

        let hash = transaction_hash!("0xdeadbeef");
        // Disable FK enforcement so we can insert a hash row without a
        // matching block_headers row. We only care about the RocksDB write
        // path, not SQLite integrity here.
        conn.execute_batch("PRAGMA foreign_keys = OFF").unwrap();
        let tx = conn.transaction().unwrap();
        tx.execute(
            "INSERT INTO transaction_hashes (hash, block_number, idx) VALUES (?, ?, ?)",
            params![&hash.0.as_be_bytes().to_vec(), 7u64, 3i64],
        )
        .unwrap();

        // Run the migration under test.
        super::migrate_transaction_hashes(&tx, &rocksdb).unwrap();

        // Hash must be readable from TRANSACTION_HASHES_COLUMN.
        let column = rocksdb.get_column(&TRANSACTION_HASHES_COLUMN);
        let value = rocksdb
            .rocksdb
            .get_pinned_cf(&column, hash.0.as_be_bytes())
            .unwrap()
            .expect("hash present in TRANSACTION_HASHES_COLUMN");
        assert_eq!(value.len(), 10);
        let block_number = u64::from_be_bytes(value[..8].try_into().unwrap());
        let idx = u16::from_be_bytes(value[8..].try_into().unwrap());
        assert_eq!(block_number, 7);
        assert_eq!(idx, 3);
    }
}
