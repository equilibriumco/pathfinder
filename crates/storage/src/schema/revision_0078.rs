use std::collections::HashMap;

use anyhow::Context;
use rusqlite::OptionalExtension;

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

    tracing::info!("Migrating trie removal markers");
    migrate_removal_markers(tx)?;

    tx.execute_batch("DROP TABLE IF EXISTS idx_to_contract_map")?;

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

fn migrate_contract_trie(
    tx: &rusqlite::Transaction<'_>,
    rocksdb: &crate::RocksDBInner,
) -> anyhow::Result<()> {
    tx.execute_batch(
        "CREATE TABLE IF NOT EXISTS idx_to_contract_map (idx INTEGER PRIMARY KEY, contract BLOB \
         NOT NULL)",
    )?;
    tx.execute_batch("DELETE FROM idx_to_contract_map")?;

    let mut migrated = roaring::RoaringTreemap::new();

    let number_of_contract_roots: usize = tx
        .prepare("SELECT COUNT(*) FROM contract_roots")?
        .query_row([], |row| row.get(0))?;

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
            let root_index: u64 = root_index
                .try_into()
                .map_err(|_| anyhow::anyhow!("root index overflow"))?;
            walk_tree(
                tx,
                &mut migrated,
                &contract_address,
                root_index,
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
    tx: &rusqlite::Transaction<'_>,
    migrated: &mut roaring::RoaringTreemap,
    contract_address: &[u8; 32],
    node_idx: u64,
    rocksdb: &crate::RocksDBInner,
    batch: &mut RocksDBBatch,
    column: &std::sync::Arc<rust_rocksdb::BoundColumnFamily<'_>>,
) -> anyhow::Result<()> {
    const CODEC_CFG: bincode::config::Configuration = bincode::config::standard();

    if migrated.contains(node_idx) {
        return Ok(());
    }

    let node_data: Option<([u8; 32], Vec<u8>)> = {
        let mut stmt = tx.prepare_cached("SELECT hash, data FROM trie_contracts WHERE idx = ?")?;
        stmt.query_row(rusqlite::params![node_idx], |row| {
            let hash: [u8; 32] = row.get(0)?;
            let data: Vec<u8> = row.get(1)?;
            Ok((hash, data))
        })
        .optional()?
    };

    let Some((hash, data)) = node_data else {
        tracing::warn!(
            node_idx,
            "Node index not found in trie_contracts, skipping node and subtree"
        );
        return Ok(());
    };

    let (node, _): (StoredSerde, usize) =
        bincode::borrow_decode_from_slice(&data, CODEC_CFG).context("decoding node data")?;

    match node {
        StoredSerde::Binary { left, right } => {
            walk_tree(tx, migrated, contract_address, left, rocksdb, batch, column)?;
            walk_tree(
                tx,
                migrated,
                contract_address,
                right,
                rocksdb,
                batch,
                column,
            )?;
        }
        StoredSerde::Edge { child, .. } => {
            walk_tree(
                tx,
                migrated,
                contract_address,
                child,
                rocksdb,
                batch,
                column,
            )?;
        }
        StoredSerde::LeafBinary | StoredSerde::LeafEdge { .. } => {}
    }

    let mut key_buf = [0u8; 40];
    contract_trie_key(contract_address, node_idx, &mut key_buf);

    // Build the RocksDB value (hash || data) only at the write point so the
    // allocation happens after the recursive walk rather than before.
    let mut value_buf = Vec::with_capacity(32 + data.len());
    value_buf.extend_from_slice(&hash);
    value_buf.extend_from_slice(&data);
    batch.put_cf(column, key_buf, &value_buf);

    if batch.len() >= BATCH_SIZE {
        rocksdb.rocksdb.write_without_wal(batch)?;
        batch.clear();
    }

    {
        let mut insert =
            tx.prepare_cached("INSERT INTO idx_to_contract_map (idx, contract) VALUES (?, ?)")?;
        insert.execute(rusqlite::params![node_idx, contract_address.as_slice()])?;
    }

    migrated.insert(node_idx);

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
#[derive(bincode::Encode, bincode::Decode)]
struct NewTrieRemovalMarker {
    key_prefix: Option<[u8; 32]>,
    indices: Vec<u64>,
}

/// Migrate trie removal markers from old format (`Vec<u64>`) to the new
/// `TrieRemovalMarker` format which includes a `key_prefix` field.
fn migrate_removal_markers(tx: &rusqlite::Transaction<'_>) -> anyhow::Result<()> {
    const CODEC_CFG: bincode::config::Configuration = bincode::config::standard();

    // Class removals: key_prefix is None.
    migrate_simple_removal_table(tx, "trie_class_removals", CODEC_CFG)?;

    // Storage removals: key_prefix is None.
    migrate_simple_removal_table(tx, "trie_storage_removals", CODEC_CFG)?;

    // Contract removals: key_prefix is Some(contract_address), looked up from
    // the idx_to_contract_map table built during contract trie migration.
    migrate_contract_removal_table(tx, CODEC_CFG)?;

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

        let mut grouped: HashMap<[u8; 32], Vec<u64>> = HashMap::new();
        for idx in old_indices {
            let contract: Option<[u8; 32]> = {
                let mut lookup =
                    tx.prepare_cached("SELECT contract FROM idx_to_contract_map WHERE idx = ?")?;
                lookup
                    .query_row(rusqlite::params![idx], |row| row.get(0))
                    .optional()?
            };

            if let Some(contract_address) = contract {
                grouped.entry(contract_address).or_default().push(idx);
            } else {
                total_skipped += 1;
            }
        }

        delete_stmt
            .execute(rusqlite::params![rowid])
            .with_context(|| format!("deleting {table} rowid {rowid}"))?;

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

    if total_skipped > 0 {
        tracing::warn!(
            %table,
            rows = rows.len(),
            migrated_indices = total_migrated,
            skipped_indices = total_skipped,
            "Migrated contract removal markers (some indices skipped — unknown contract)"
        );
    } else {
        tracing::info!(
            %table,
            rows = rows.len(),
            migrated_indices = total_migrated,
            "Migrated contract removal markers"
        );
    }
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
    fn contract_trie_migration_shared_nodes_and_removal_markers() {
        const CODEC_CFG: bincode::config::Configuration = bincode::config::standard();

        let rocksdb_dir = tempfile::tempdir().unwrap();
        let rocksdb = crate::StorageBuilder::open_rocksdb(rocksdb_dir.path()).unwrap();

        let mut conn = rusqlite::Connection::open_in_memory().unwrap();
        let tx = conn.transaction().unwrap();
        crate::schema::base_schema(&tx).unwrap();
        tx.commit().unwrap();

        // Run migrations through revision_0077 (index 37 is 0078, so [..37]).
        let prior = &crate::schema::migrations()[..37];
        for migration in prior {
            let tx = conn.transaction().unwrap();
            migration(&tx, &rocksdb).unwrap();
            tx.commit().unwrap();
        }

        conn.execute_batch("PRAGMA foreign_keys = OFF").unwrap();

        // Build a small trie:
        //   node 0 (leaf) — shared by both contracts
        //   node 1 (edge → 0) — root for contract A
        //   node 2 (edge → 0) — root for contract B
        let leaf = super::StoredSerde::LeafBinary;
        let edge_to_0 = super::StoredSerde::Edge {
            child: 0,
            path: vec![0x01],
        };

        let leaf_data = bincode::encode_to_vec(&leaf, CODEC_CFG).unwrap();
        let edge_data = bincode::encode_to_vec(&edge_to_0, CODEC_CFG).unwrap();
        let hash = [0xAAu8; 32];

        let tx = conn.transaction().unwrap();
        for (idx, data) in [(0u64, &leaf_data), (1, &edge_data), (2, &edge_data)] {
            tx.execute(
                "INSERT INTO trie_contracts (idx, hash, data) VALUES (?, ?, ?)",
                params![idx, hash.as_slice(), data.as_slice()],
            )
            .unwrap();
        }

        let contract_a = [0x01u8; 32];
        let contract_b = [0x02u8; 32];
        tx.execute(
            "INSERT INTO contract_roots (block_number, contract_address, root_index) VALUES (?, \
             ?, ?)",
            params![1i64, contract_a.as_slice(), 1i64],
        )
        .unwrap();
        tx.execute(
            "INSERT INTO contract_roots (block_number, contract_address, root_index) VALUES (?, \
             ?, ?)",
            params![2i64, contract_b.as_slice(), 2i64],
        )
        .unwrap();

        let old_removal_indices: Vec<u64> = vec![0, 1];
        let encoded_old = bincode::encode_to_vec(&old_removal_indices, CODEC_CFG).unwrap();
        tx.execute(
            "INSERT INTO trie_contracts_removals (block_number, indices) VALUES (?, ?)",
            params![10i64, encoded_old.as_slice()],
        )
        .unwrap();

        // Run revision_0078 migration.
        super::migrate(&tx, &rocksdb).unwrap();
        tx.commit().unwrap();

        // Verify RocksDB has re-keyed entries for contract A.
        let column = rocksdb.get_column(&TRIE_CONTRACT_COLUMN);
        let mut key_buf = [0u8; 40];

        // Node 1 under contract A.
        super::contract_trie_key(&contract_a, 1, &mut key_buf);
        assert!(
            rocksdb
                .rocksdb
                .get_pinned_cf(&column, key_buf)
                .unwrap()
                .is_some(),
            "node 1 should be under contract A"
        );

        // Node 0 (shared) — claimed by contract A (walked first).
        super::contract_trie_key(&contract_a, 0, &mut key_buf);
        assert!(
            rocksdb
                .rocksdb
                .get_pinned_cf(&column, key_buf)
                .unwrap()
                .is_some(),
            "shared node 0 should be under contract A"
        );

        // Node 2 under contract B.
        super::contract_trie_key(&contract_b, 2, &mut key_buf);
        assert!(
            rocksdb
                .rocksdb
                .get_pinned_cf(&column, key_buf)
                .unwrap()
                .is_some(),
            "node 2 should be under contract B"
        );

        // Node 0 should NOT also be under contract B (shared, already migrated).
        super::contract_trie_key(&contract_b, 0, &mut key_buf);
        assert!(
            rocksdb
                .rocksdb
                .get_pinned_cf(&column, key_buf)
                .unwrap()
                .is_none(),
            "shared node 0 should not be duplicated under contract B"
        );

        // Verify removal markers were regrouped.
        let mut stmt = conn
            .prepare("SELECT indices FROM trie_contracts_removals")
            .unwrap();
        let rows: Vec<Vec<u8>> = stmt
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(
            rows.len(),
            1,
            "removal markers should be regrouped into 1 row"
        );

        let (marker, _) =
            bincode::decode_from_slice::<super::NewTrieRemovalMarker, _>(&rows[0], CODEC_CFG)
                .unwrap();
        assert_eq!(marker.key_prefix, Some(contract_a));
        assert_eq!(marker.indices.len(), 2);
    }
}
