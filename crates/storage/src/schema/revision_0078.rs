use anyhow::Context;

use crate::columns::Column;
use crate::{
    CONTRACT_STATE_HASHES_COLUMN,
    TRIE_CLASS_COLUMN,
    TRIE_CONTRACT_COLUMN,
    TRIE_NEXT_INDEX_COLUMN,
    TRIE_STORAGE_COLUMN,
};

pub(crate) fn migrate(
    tx: &rusqlite::Transaction<'_>,
    rocksdb: &crate::RocksDBInner,
) -> anyhow::Result<()> {
    tracing::info!("Migrating class trie to RocksDB");
    migrate_trie(tx, "trie_class", rocksdb, &TRIE_CLASS_COLUMN)?;

    tracing::info!("Migrating storage trie to RocksDB");
    migrate_trie(tx, "trie_storage", rocksdb, &TRIE_STORAGE_COLUMN)?;

    tracing::info!("Migrating contract trie to RocksDB");
    migrate_trie(tx, "trie_contracts", rocksdb, &TRIE_CONTRACT_COLUMN)?;

    tracing::info!("Migrating contract state hashes to RocksDB");
    migrate_contract_state_hashes(tx, rocksdb)?;

    create_next_index(tx, rocksdb, "trie_class", &TRIE_CLASS_COLUMN)?;
    create_next_index(tx, rocksdb, "trie_contracts", &TRIE_CONTRACT_COLUMN)?;
    create_next_index(tx, rocksdb, "trie_storage", &TRIE_STORAGE_COLUMN)?;

    Ok(())
}

const BATCH_SIZE: usize = 1_000_000;

fn migrate_trie(
    sqlite_txn: &rusqlite::Transaction<'_>,
    sqlite_table_name: &str,
    rocksdb: &crate::RocksDBInner,
    column: &Column,
) -> anyhow::Result<()> {
    let mut stmt = sqlite_txn.prepare(&format!(
        "SELECT idx, hash, data FROM {}",
        sqlite_table_name
    ))?;

    let trie_iter = stmt.query_map([], |row| {
        let idx: u64 = row.get(0)?;
        let hash: [u8; 32] = row.get(1)?;
        let data: Vec<u8> = row.get(2)?;
        Ok((idx, hash, data))
    })?;

    let column: std::sync::Arc<rust_rocksdb::BoundColumnFamily<'_>> = rocksdb.get_column(column);

    let mut buf = [0u8; 256];
    let mut batch = crate::RocksDBBatch::default();

    for (i, trie_result) in trie_iter.enumerate() {
        let (idx, hash, data) = trie_result?;
        anyhow::ensure!(
            data.len() <= buf.len() - 32,
            "Trie node data too large ({} bytes) for table {sqlite_table_name} at index {idx}",
            data.len()
        );
        let idx = idx.to_be_bytes();
        buf[..32].copy_from_slice(&hash);
        buf[32..32 + data.len()].copy_from_slice(&data);
        batch.put_cf(&column, idx, &buf[..32 + data.len()]);

        if i % BATCH_SIZE == BATCH_SIZE - 1 {
            rocksdb.rocksdb.write_without_wal(&batch)?;
            batch = crate::RocksDBBatch::default();
            tracing::info!(
                "Migrated {} entries from table {}",
                i + 1,
                sqlite_table_name
            );
        }
    }

    rocksdb.rocksdb.write_without_wal(&batch)?;

    tracing::info!(%sqlite_table_name, "Migrated trie from table");

    Ok(())
}

fn contract_state_hashes_key(block_number: u64, contract_address: &[u8; 32]) -> [u8; 40] {
    let mut key = [0u8; 40];
    let block_number = u64::MAX - block_number;

    key[..32].copy_from_slice(contract_address);
    key[32..].copy_from_slice(&block_number.to_be_bytes());
    key
}

fn migrate_contract_state_hashes(
    tx: &rusqlite::Transaction<'_>,
    rocksdb: &crate::RocksDBInner,
) -> anyhow::Result<()> {
    let mut stmt = tx
        .prepare("SELECT block_number, contract_address, state_hash FROM contract_state_hashes")
        .context("Preparing contract state hashes query")?;

    let rows = stmt
        .query_map([], |row| {
            let block_number: u64 = row.get(0)?;
            let contract_address: [u8; 32] = row.get(1)?;
            let state_hash: [u8; 32] = row.get(2)?;
            Ok((block_number, contract_address, state_hash))
        })
        .context("Querying contract state hashes")?;

    let column = rocksdb.get_column(&CONTRACT_STATE_HASHES_COLUMN);
    let mut batch = crate::RocksDBBatch::default();

    for (i, row) in rows.enumerate() {
        let (block_number, contract_address, state_hash) =
            row.context("Reading contract state hash row")?;

        let key = contract_state_hashes_key(block_number, &contract_address);
        batch.put_cf(&column, key, state_hash);

        if i % BATCH_SIZE == BATCH_SIZE - 1 {
            rocksdb
                .rocksdb
                .write_without_wal(&batch)
                .context("Writing contract state hashes batch to RocksDB")?;
            batch = crate::RocksDBBatch::default();
            tracing::info!("Migrated {} contract state hash entries", i + 1);
        }
    }

    rocksdb
        .rocksdb
        .write_without_wal(&batch)
        .context("Writing final contract state hashes batch to RocksDB")?;
    tracing::info!("Contract state hashes migration complete");

    Ok(())
}

fn create_next_index(
    tx: &rusqlite::Transaction<'_>,
    rocksdb: &crate::RocksDBInner,
    sqlite_table_name: &str,
    column: &Column,
) -> anyhow::Result<()> {
    let mut stmt = tx.prepare(&format!("SELECT MAX(idx) FROM {}", sqlite_table_name))?;
    let next_index: u64 = stmt.query_row([], |row| {
        let max_idx: Option<u64> = row.get(0)?;
        Ok(max_idx.map(|v| v + 1).unwrap_or(0))
    })?;

    let next_index_column = rocksdb.get_column(&TRIE_NEXT_INDEX_COLUMN);
    rocksdb.rocksdb.put_cf(
        &next_index_column,
        column.name.as_bytes(),
        next_index.to_be_bytes(),
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use rusqlite::params;

    use crate::TRIE_CONTRACT_COLUMN;

    #[test]
    fn contract_trie_migration_copies_rows_one_to_one() {
        let rocksdb_dir = tempfile::tempdir().unwrap();
        let rocksdb = crate::StorageBuilder::open_rocksdb(rocksdb_dir.path()).unwrap();

        let mut conn = rusqlite::Connection::open_in_memory().unwrap();
        let tx = conn.transaction().unwrap();
        crate::schema::base_schema(&tx).unwrap();
        tx.commit().unwrap();

        let migrations = crate::schema::migrations();
        let pos = migrations
            .iter()
            .position(|m| std::ptr::fn_addr_eq(*m, super::migrate as crate::schema::MigrationFn))
            .expect("revision_0078::migrate not in migrations()");
        for migration in &migrations[..pos] {
            let tx = conn.transaction().unwrap();
            migration(&tx, &rocksdb).unwrap();
            tx.commit().unwrap();
        }

        conn.execute_batch("PRAGMA foreign_keys = OFF").unwrap();

        let data_a: &[u8] = b"node-A";
        let data_b: &[u8] = b"node-B";
        let data_c: &[u8] = b"node-C";
        let hash_a = [0xAAu8; 32];
        let hash_b = [0xBBu8; 32];
        let hash_c = [0xCCu8; 32];

        let tx = conn.transaction().unwrap();
        for (idx, hash, data) in [
            (0u64, hash_a, data_a),
            (1, hash_b, data_b),
            (2, hash_c, data_c),
        ] {
            tx.execute(
                "INSERT INTO trie_contracts (idx, hash, data) VALUES (?, ?, ?)",
                params![idx, hash.as_slice(), data],
            )
            .unwrap();
        }

        super::migrate(&tx, &rocksdb).unwrap();
        tx.commit().unwrap();

        let column = rocksdb.get_column(&TRIE_CONTRACT_COLUMN);
        for (idx, hash, data) in [
            (0u64, hash_a, data_a),
            (1, hash_b, data_b),
            (2, hash_c, data_c),
        ] {
            let key = idx.to_be_bytes();
            let value = rocksdb
                .rocksdb
                .get_pinned_cf(&column, key)
                .unwrap()
                .unwrap_or_else(|| panic!("missing row for idx {idx}"));
            assert_eq!(&value[..32], &hash, "hash mismatch for idx {idx}");
            assert_eq!(&value[32..], data, "data mismatch for idx {idx}");
        }

        let next_index_column = rocksdb.get_column(&crate::TRIE_NEXT_INDEX_COLUMN);
        let next_index_bytes = rocksdb
            .rocksdb
            .get_pinned_cf(&next_index_column, TRIE_CONTRACT_COLUMN.name.as_bytes())
            .unwrap()
            .expect("next index for TRIE_CONTRACT_COLUMN");
        let next_index =
            u64::from_be_bytes(<[u8; 8]>::try_from(next_index_bytes.as_ref()).unwrap());
        assert_eq!(next_index, 3, "next_index should be MAX(idx) + 1 = 3");
    }
}
