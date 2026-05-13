use std::sync::atomic::AtomicUsize;

use anyhow::Context;

use crate::columns::Column;
use crate::params::RowExt;
use crate::{
    RocksDBBatch,
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
    migrate_contract_trie(tx, rocksdb)?;

    tracing::info!("Migrating contract state hashes to RocksDB");
    migrate_contract_state_hashes(tx, rocksdb)?;

    create_next_index(tx, rocksdb, "trie_class", &TRIE_CLASS_COLUMN)?;
    create_next_index(tx, rocksdb, "trie_contracts", &TRIE_CONTRACT_COLUMN)?;
    create_next_index(tx, rocksdb, "trie_storage", &TRIE_STORAGE_COLUMN)?;

    // HACK: drop removal markers because they are not compatible with the new
    // serialization format: we'd need to migrate these too.
    tx.execute_batch(
        "
        DELETE FROM trie_class_removals;
        DELETE FROM trie_contracts_removals;
        DELETE FROM trie_storage_removals;",
    )?;

    // tx.execute_batch(
    //     "
    //     DROP TABLE trie_class;
    //     DROP TABLE trie_contracts;
    //     DROP TABLE trie_storage;
    //     DROP TABLE contract_state_hashes;
    //     ",
    // )?;
    Ok(())
}

const BATCH_SIZE: usize = 1000000;

fn migrate_trie(
    sqlite_txn: &rusqlite::Transaction,
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

fn migrate_contract_trie(
    tx: &rusqlite::Transaction<'_>,
    rocksdb: &crate::RocksDBInner,
) -> anyhow::Result<()> {
    let number_of_contract_roots =
        tx.query_one("SELECT COUNT(*) FROM contract_roots", [], |row| {
            let count: usize = row.get(0)?;
            Ok(count)
        })?;

    let mut stmt = tx.prepare("SELECT idx, hash, data FROM trie_contracts ORDER BY idx")?;
    let trie_iter = stmt.query_map([], |row| {
        let idx: u64 = row.get(0)?;
        let hash: [u8; 32] = row.get(1)?;
        let data: Vec<u8> = row.get(2)?;
        Ok((idx, hash, data))
    })?;

    let mut packed_arrays = SparsePackedArrays::new();

    for (i, trie_result) in trie_iter.enumerate() {
        let (idx, hash, data) = trie_result?;
        packed_arrays.push(idx, &hash, &data);

        if i % BATCH_SIZE == BATCH_SIZE - 1 {
            tracing::info!("Loaded {} contract trie entries into memory", i + 1);
        }
    }

    tracing::info!(
        "Loaded {} contract trie entries into memory",
        packed_arrays.len()
    );
    packed_arrays.clear_migrated();

    let mut stmt = tx.prepare("SELECT contract_address, root_index FROM contract_roots")?;
    let roots_iter = stmt.query_map([], |row| {
        let contract_address: [u8; 32] = row.get(0)?;
        let root_index = row.get_optional_i64(1)?;
        Ok((contract_address, root_index))
    })?;

    let column: std::sync::Arc<rust_rocksdb::BoundColumnFamily<'_>> =
        rocksdb.get_column(&TRIE_CONTRACT_COLUMN);

    let mut batch = crate::RocksDBBatch::default();

    for (i, root_result) in roots_iter.enumerate() {
        let (contract_address, root_index) = root_result?;

        if let Some(root_index) = root_index {
            walk_tree(
                &packed_arrays,
                &contract_address,
                root_index.try_into().expect("root index fits into u64"),
                rocksdb,
                &mut batch,
                &column,
            )?;
        }

        if i % 10000 == 9999 {
            tracing::info!(
                "Migrated {}/{} contract tries",
                i + 1,
                number_of_contract_roots
            );
        }
    }

    rocksdb.rocksdb.write_without_wal(&batch)?;

    Ok(())
}

fn walk_tree(
    packed_arrays: &SparsePackedArrays,
    contract_address: &[u8; 32],
    node_idx: u64,
    rocksdb: &crate::RocksDBInner,
    batch: &mut RocksDBBatch,
    column: &std::sync::Arc<rust_rocksdb::BoundColumnFamily<'_>>,
) -> anyhow::Result<()> {
    const CODEC_CFG: bincode::config::Configuration = bincode::config::standard();

    let Some((node_data, array_idx)) = packed_arrays.get(node_idx) else {
        tracing::warn!(
            node_idx,
            "Node index not found in packed arrays, skipping node and subtree"
        );
        return Ok(());
    };

    if packed_arrays.is_migrated(array_idx) {
        // already migrated this node (and subtree!), skip to avoid redundant work
        return Ok(());
    }

    // parse node_data to determine if it's a leaf, extension, or branch node
    // and recursively walk child nodes if necessary
    let (node, _): (StoredSerde, usize) =
        bincode::borrow_decode_from_slice(&node_data[32..], CODEC_CFG)
            .expect("decoding node data should succeed");

    match node {
        StoredSerde::Binary { left, right } => {
            walk_tree(
                packed_arrays,
                contract_address,
                left,
                rocksdb,
                batch,
                column,
            )?;
            walk_tree(
                packed_arrays,
                contract_address,
                right,
                rocksdb,
                batch,
                column,
            )?;
        }
        StoredSerde::Edge { child, .. } => {
            walk_tree(
                packed_arrays,
                contract_address,
                child,
                rocksdb,
                batch,
                column,
            )?;
        }
        StoredSerde::LeafBinary | StoredSerde::LeafEdge { .. } => {
            // leaf node, no children to walk
        }
    }

    let mut key_buf = [0u8; 40];
    contract_trie_key(contract_address, node_idx, &mut key_buf);
    batch.put_cf(column, key_buf, node_data);

    if batch.len() >= BATCH_SIZE {
        rocksdb.rocksdb.write_without_wal(&batch)?;
        batch.clear();
    }

    // mark as migrated in memory to avoid re-walking this node if it's shared
    packed_arrays.set_migrated(array_idx);

    Ok(())
}

#[derive(Clone, Debug, bincode::Encode, bincode::BorrowDecode)]
enum StoredSerde {
    Binary { left: u64, right: u64 },
    Edge { child: u64, path: Vec<u8> },
    LeafBinary,
    LeafEdge { path: Vec<u8> },
}

fn contract_trie_key(prefix: &[u8; 32], storage_idx: u64, buf: &mut [u8; 40]) {
    buf[..32].copy_from_slice(prefix);
    let storage_idx_be_bytes = storage_idx.to_be_bytes();
    buf[32..].copy_from_slice(&storage_idx_be_bytes);
}

pub struct SparsePackedArrays {
    cursor: usize,
    keys: Vec<u64>,      // sorted
    offsets: Vec<usize>, // parallel to keys, + 1 sentinel
    data: Vec<u8>,
    migrated: Vec<AtomicUsize>, // parallel to keys, tracks migration status, bit-indexed
}

impl SparsePackedArrays {
    pub fn new() -> Self {
        Self {
            cursor: 0,
            keys: Vec::new(),
            offsets: vec![0],
            data: Vec::new(),
            migrated: Vec::new(),
        }
    }

    pub fn push(&mut self, key: u64, hash: &[u8], blob: &[u8]) {
        self.keys.push(key);
        *self.offsets.last_mut().unwrap() = self.cursor;
        self.data.extend_from_slice(hash);
        self.data.extend_from_slice(blob);
        self.cursor += hash.len() + blob.len();
        self.offsets.push(self.cursor); // sentinel
    }

    pub fn get(&self, key: u64) -> Option<(&[u8], usize)> {
        let idx = self.keys.binary_search(&key).ok()?;
        let start = self.offsets[idx] as usize;
        let end = self.offsets[idx + 1] as usize;
        Some((&self.data[start..end], idx))
    }

    pub fn len(&self) -> usize {
        self.keys.len()
    }

    pub fn clear_migrated(&mut self) {
        let number_of_entries = self.keys.len();
        let number_of_atomics =
            (number_of_entries + usize::BITS as usize - 1) / usize::BITS as usize;
        self.migrated
            .resize_with(number_of_atomics, Default::default);
    }

    pub fn is_migrated(&self, idx: usize) -> bool {
        let bit_idx = idx % usize::BITS as usize;
        let atomic_idx = idx / usize::BITS as usize;
        if atomic_idx >= self.migrated.len() {
            return false;
        }
        let mask = 1 << bit_idx;
        (self.migrated[atomic_idx].load(std::sync::atomic::Ordering::Acquire) & mask) != 0
    }

    pub fn set_migrated(&self, idx: usize) {
        let bit_idx = idx % usize::BITS as usize;
        let atomic_idx = idx / usize::BITS as usize;
        if atomic_idx >= self.migrated.len() {
            panic!("Index out of bounds for migrated tracking");
        }
        let mask = 1 << bit_idx;
        self.migrated[atomic_idx].fetch_or(mask, std::sync::atomic::Ordering::AcqRel);
    }
}

fn contract_state_hashes_key(block_number: u64, contract_address: &[u8; 32]) -> [u8; 36] {
    let mut key = [0u8; 36];
    let block_number: u32 = block_number.try_into().expect("block number fits into u32");
    let block_number: u32 = u32::MAX - block_number;

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
        batch.put_cf(&column, key, &state_hash);

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
        Ok(max_idx.unwrap_or(0) + 1)
    })?;

    let next_index_column = rocksdb.get_column(&TRIE_NEXT_INDEX_COLUMN);
    rocksdb.rocksdb.put_cf(
        &next_index_column,
        column.name.as_bytes(),
        &next_index.to_be_bytes(),
    )?;

    Ok(())
}
