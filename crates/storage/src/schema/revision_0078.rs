use std::collections::HashMap;
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
    let idx_to_contract = migrate_contract_trie(tx, rocksdb)?;

    tracing::info!("Migrating contract state hashes to RocksDB");
    migrate_contract_state_hashes(tx, rocksdb)?;

    create_next_index(tx, rocksdb, "trie_class", &TRIE_CLASS_COLUMN)?;
    create_next_index(tx, rocksdb, "trie_contracts", &TRIE_CONTRACT_COLUMN)?;
    create_next_index(tx, rocksdb, "trie_storage", &TRIE_STORAGE_COLUMN)?;

    tracing::info!("Migrating trie removal markers");
    migrate_removal_markers(tx, &idx_to_contract)?;

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
) -> anyhow::Result<HashMap<u64, [u8; 32]>> {
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
    let mut idx_to_contract: HashMap<u64, [u8; 32]> = HashMap::new();

    for (i, root_result) in roots_iter.enumerate() {
        let (contract_address, root_index) = root_result?;

        if let Some(root_index) = root_index {
            let root_index: u64 = root_index
                .try_into()
                .map_err(|_| anyhow::anyhow!("root index overflow"))?;
            walk_tree(
                &packed_arrays,
                &contract_address,
                root_index,
                rocksdb,
                &mut batch,
                &column,
                &mut idx_to_contract,
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

    Ok(idx_to_contract)
}

fn walk_tree(
    packed_arrays: &SparsePackedArrays,
    contract_address: &[u8; 32],
    node_idx: u64,
    rocksdb: &crate::RocksDBInner,
    batch: &mut RocksDBBatch,
    column: &std::sync::Arc<rust_rocksdb::BoundColumnFamily<'_>>,
    idx_to_contract: &mut HashMap<u64, [u8; 32]>,
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
            .context("decoding node data")?;

    match node {
        StoredSerde::Binary { left, right } => {
            walk_tree(
                packed_arrays,
                contract_address,
                left,
                rocksdb,
                batch,
                column,
                idx_to_contract,
            )?;
            walk_tree(
                packed_arrays,
                contract_address,
                right,
                rocksdb,
                batch,
                column,
                idx_to_contract,
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
                idx_to_contract,
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
        rocksdb.rocksdb.write_without_wal(batch)?;
        batch.clear();
    }

    // Record which contract address this node was stored under in RocksDB.
    idx_to_contract.insert(node_idx, *contract_address);

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

/// Bincode-compatible representation of `TrieRemovalMarker` for the new format.
///
/// `Felt` encodes as `[u8; 32]` (big-endian bytes) and `TrieStorageIndex`
/// encodes as `u64`, so this struct produces identical wire format to the
/// `TrieRemovalMarker` in `crate::connection::trie`.
#[derive(bincode::Encode)]
struct NewTrieRemovalMarker {
    key_prefix: Option<[u8; 32]>,
    indices: Vec<u64>,
}

/// Migrate trie removal markers from old format (`Vec<u64>`) to the new
/// `TrieRemovalMarker` format which includes a `key_prefix` field.
fn migrate_removal_markers(
    tx: &rusqlite::Transaction<'_>,
    idx_to_contract: &HashMap<u64, [u8; 32]>,
) -> anyhow::Result<()> {
    const CODEC_CFG: bincode::config::Configuration = bincode::config::standard();

    // Class removals: key_prefix is None.
    migrate_simple_removal_table(tx, "trie_class_removals", CODEC_CFG)?;

    // Storage removals: key_prefix is None.
    migrate_simple_removal_table(tx, "trie_storage_removals", CODEC_CFG)?;

    // Contract removals: key_prefix is Some(contract_address), looked up from
    // the idx_to_contract mapping built during contract trie migration.
    migrate_contract_removal_table(tx, idx_to_contract, CODEC_CFG)?;

    Ok(())
}

/// Migrate class or storage removal markers (key_prefix = None).
fn migrate_simple_removal_table(
    tx: &rusqlite::Transaction<'_>,
    table: &str,
    codec_cfg: bincode::config::Configuration,
) -> anyhow::Result<()> {
    let mut select_stmt = tx
        .prepare(&format!(
            "SELECT rowid, indices FROM {} ORDER BY rowid",
            table
        ))
        .with_context(|| format!("preparing select for {table}"))?;

    let rows: Vec<(i64, Vec<u8>)> = select_stmt
        .query_map([], |row| {
            let rowid: i64 = row.get(0)?;
            let blob: Vec<u8> = row.get(1)?;
            Ok((rowid, blob))
        })?
        .collect::<Result<Vec<_>, _>>()
        .with_context(|| format!("reading rows from {table}"))?;

    let mut update_stmt = tx
        .prepare(&format!("UPDATE {} SET indices = ? WHERE rowid = ?", table))
        .with_context(|| format!("preparing update for {table}"))?;

    for (rowid, blob) in &rows {
        let (old_indices, _) = bincode::decode_from_slice::<Vec<u64>, _>(blob, codec_cfg)
            .with_context(|| format!("decoding old removal marker in {table} rowid {rowid}"))?;

        let new_marker = NewTrieRemovalMarker {
            key_prefix: None,
            indices: old_indices,
        };
        let encoded = bincode::encode_to_vec(&new_marker, codec_cfg)
            .with_context(|| format!("encoding new removal marker for {table}"))?;

        update_stmt
            .execute(rusqlite::params![encoded, rowid])
            .with_context(|| format!("updating {table} rowid {rowid}"))?;
    }

    tracing::info!(%table, rows = rows.len(), "Migrated removal markers");
    Ok(())
}

/// Migrate contract removal markers. Each old row contains indices that may
/// belong to different contracts. We group them by contract address, delete the
/// old row, and insert new rows with the proper `key_prefix`.
fn migrate_contract_removal_table(
    tx: &rusqlite::Transaction<'_>,
    idx_to_contract: &HashMap<u64, [u8; 32]>,
    codec_cfg: bincode::config::Configuration,
) -> anyhow::Result<()> {
    let table = "trie_contracts_removals";

    let mut select_stmt = tx
        .prepare(&format!(
            "SELECT rowid, block_number, indices FROM {} ORDER BY rowid",
            table
        ))
        .with_context(|| format!("preparing select for {table}"))?;

    let rows: Vec<(i64, i64, Vec<u8>)> = select_stmt
        .query_map([], |row| {
            let rowid: i64 = row.get(0)?;
            let block_number: i64 = row.get(1)?;
            let blob: Vec<u8> = row.get(2)?;
            Ok((rowid, block_number, blob))
        })?
        .collect::<Result<Vec<_>, _>>()
        .with_context(|| format!("reading rows from {table}"))?;

    let mut delete_stmt = tx
        .prepare(&format!("DELETE FROM {} WHERE rowid = ?", table))
        .with_context(|| format!("preparing delete for {table}"))?;

    let mut insert_stmt = tx
        .prepare(&format!(
            "INSERT INTO {} (block_number, indices) VALUES (?, ?)",
            table
        ))
        .with_context(|| format!("preparing insert for {table}"))?;

    let mut total_migrated = 0usize;
    let mut total_skipped = 0usize;

    for (rowid, block_number, blob) in &rows {
        let (old_indices, _) = bincode::decode_from_slice::<Vec<u64>, _>(blob, codec_cfg)
            .with_context(|| format!("decoding old removal marker in {table} rowid {rowid}"))?;

        // Group indices by contract address.
        let mut grouped: HashMap<[u8; 32], Vec<u64>> = HashMap::new();
        for idx in old_indices {
            if let Some(contract_address) = idx_to_contract.get(&idx) {
                grouped.entry(*contract_address).or_default().push(idx);
            } else {
                // Node not found in RocksDB mapping -- it was never migrated,
                // so there's nothing to delete later. Skip it.
                total_skipped += 1;
            }
        }

        // Delete the old row.
        delete_stmt
            .execute(rusqlite::params![rowid])
            .with_context(|| format!("deleting {table} rowid {rowid}"))?;

        // Insert new rows grouped by contract address.
        for (contract_address, indices) in &grouped {
            let new_marker = NewTrieRemovalMarker {
                key_prefix: Some(*contract_address),
                indices: indices.clone(),
            };
            let encoded = bincode::encode_to_vec(&new_marker, codec_cfg)
                .context("encoding new contract removal marker")?;

            insert_stmt
                .execute(rusqlite::params![block_number, encoded])
                .context("inserting new contract removal marker")?;

            total_migrated += indices.len();
        }
    }

    tracing::info!(
        %table,
        rows = rows.len(),
        migrated_indices = total_migrated,
        skipped_indices = total_skipped,
        "Migrated contract removal markers"
    );
    Ok(())
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
        let start = self.offsets[idx];
        let end = self.offsets[idx + 1];
        Some((&self.data[start..end], idx))
    }

    pub fn len(&self) -> usize {
        self.keys.len()
    }

    pub fn clear_migrated(&mut self) {
        let number_of_entries = self.keys.len();
        let number_of_atomics = number_of_entries.div_ceil(usize::BITS as usize);
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
