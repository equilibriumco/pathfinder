use std::collections::HashMap;

use anyhow::Context;
use bitvec::prelude::Msb0;
use bitvec::vec::BitVec;
use pathfinder_common::prelude::*;
use pathfinder_crypto::Felt;
use rust_rocksdb::ReadOptions;

use crate::columns::Column;
use crate::prelude::*;
use crate::TriePruneMode;

const TRIE_CONTRACT_PREFIX_LEN: usize = size_of::<Felt>();
const TRIE_CONTRACT_KEY_LEN: usize = TRIE_CONTRACT_PREFIX_LEN + size_of::<TrieStorageIndex>();

pub const TRIE_CLASS_COLUMN: Column = Column::new("trie_class")
    .with_point_lookup()
    .with_optimize_for_hits();
pub const TRIE_CONTRACT_COLUMN: Column = Column::new("trie_contract")
    .with_prefix_length(TRIE_CONTRACT_PREFIX_LEN)
    .with_point_lookup()
    .with_optimize_for_hits();
pub const TRIE_STORAGE_COLUMN: Column = Column::new("trie_storage")
    .with_point_lookup()
    .with_optimize_for_hits();
pub const TRIE_NEXT_INDEX_COLUMN: Column = Column::new("trie_next_index");

/// Constructs the key for a contract trie entry.
///
/// Format is the following:
/// [contract_address (32 bytes)][storage_idx (8 bytes)]
fn contract_trie_key(
    prefix: &Felt,
    storage_idx: TrieStorageIndex,
    buf: &mut [u8; TRIE_CONTRACT_KEY_LEN],
) {
    buf[..TRIE_CONTRACT_PREFIX_LEN].copy_from_slice(prefix.as_be_bytes());
    let storage_idx_be_bytes = storage_idx.0.to_be_bytes();
    buf[TRIE_CONTRACT_PREFIX_LEN..].copy_from_slice(&storage_idx_be_bytes);
}

const CONTRACT_STATE_HASHES_PREFIX_LEN: usize = size_of::<Felt>();
const CONTRACT_STATE_HASHES_KEY_LEN: usize = CONTRACT_STATE_HASHES_PREFIX_LEN + size_of::<u64>();

pub const CONTRACT_STATE_HASHES_COLUMN: Column =
    Column::new("contract_state_hashes").with_prefix_length(CONTRACT_STATE_HASHES_PREFIX_LEN);

/// Constructs the key for a contract state hash entry.
///
/// Format is the following:
/// [contract_address (32 bytes)][inverted block number (8 bytes)]
///
/// We're using an inverted block number to allow for efficient retrieval of the
/// latest state hash for a given contract address using forward iteration.
pub(crate) fn contract_state_hashes_key(
    block_number: BlockNumber,
    contract_address: &ContractAddress,
) -> [u8; CONTRACT_STATE_HASHES_KEY_LEN] {
    let mut key = [0u8; CONTRACT_STATE_HASHES_KEY_LEN];
    let block_number = u64::MAX - block_number.get();

    key[..CONTRACT_STATE_HASHES_PREFIX_LEN].copy_from_slice(contract_address.0.as_be_bytes());
    key[CONTRACT_STATE_HASHES_PREFIX_LEN..].copy_from_slice(&block_number.to_be_bytes());
    key
}

#[derive(bincode::Encode, bincode::Decode)]
struct TrieRemovalMarker {
    key_prefix: Option<Felt>,
    indices: Vec<TrieStorageIndex>,
}

impl Transaction<'_> {
    pub fn class_root_index(
        &self,
        block_number: BlockNumber,
    ) -> anyhow::Result<Option<TrieStorageIndex>> {
        self.inner()
            .query_row(
                "SELECT root_index FROM class_roots WHERE block_number <= ? ORDER BY block_number \
                 DESC LIMIT 1",
                params![&block_number],
                |row| row.get::<_, Option<u64>>(0),
            )
            .optional()
            .map(|x| x.flatten().map(TrieStorageIndex))
            .map_err(Into::into)
    }

    pub fn class_root(&self, block_number: BlockNumber) -> anyhow::Result<Option<ClassCommitment>> {
        let root_index = self.class_root_index(block_number)?;

        if let Some(root_index) = root_index {
            let root = self.class_trie_node_hash(root_index)?.map(ClassCommitment);
            Ok(root)
        } else {
            Ok(None)
        }
    }

    pub fn class_root_exists(&self, block_number: BlockNumber) -> anyhow::Result<bool> {
        self.inner()
            .query_row(
                "SELECT EXISTS (SELECT 1 FROM class_roots WHERE block_number=?)",
                params![&block_number],
                |row| row.get::<_, bool>(0),
            )
            .map_err(Into::into)
    }

    pub fn storage_root_index(
        &self,
        block_number: BlockNumber,
    ) -> anyhow::Result<Option<TrieStorageIndex>> {
        self.inner()
            .query_row(
                "SELECT root_index FROM storage_roots WHERE block_number <= ? ORDER BY \
                 block_number DESC LIMIT 1",
                params![&block_number],
                |row| row.get::<_, Option<u64>>(0),
            )
            .optional()
            .map(|x| x.flatten().map(TrieStorageIndex))
            .map_err(Into::into)
    }

    pub fn storage_root_exists(&self, block_number: BlockNumber) -> anyhow::Result<bool> {
        self.inner()
            .query_row(
                "SELECT EXISTS (SELECT 1 FROM storage_roots WHERE block_number=?)",
                params![&block_number],
                |row| row.get::<_, bool>(0),
            )
            .map_err(Into::into)
    }

    pub fn contract_root_index(
        &self,
        block_number: BlockNumber,
        contract: &ContractAddress,
    ) -> anyhow::Result<Option<TrieStorageIndex>> {
        self.inner()
            .query_row(
                "SELECT root_index FROM contract_roots WHERE contract_address = ? AND \
                 block_number <= ? ORDER BY block_number DESC LIMIT 1",
                params![contract, &block_number],
                |row| row.get::<_, Option<u64>>(0),
            )
            .optional()
            .map(|x| x.flatten().map(TrieStorageIndex))
            .map_err(Into::into)
    }

    pub fn contract_root(
        &self,
        block_number: BlockNumber,
        contract: &ContractAddress,
    ) -> anyhow::Result<Option<ContractRoot>> {
        let root_index = self
            .inner()
            .query_row(
                "SELECT root_index FROM contract_roots WHERE block_number <= ? AND \
                 contract_address = ? ORDER BY block_number DESC LIMIT 1",
                params![&block_number, contract],
                |row| row.get_optional_i64(0),
            )
            .optional()?
            .flatten();

        if let Some(root_index) = root_index {
            let root_index = TrieStorageIndex(root_index.try_into()?);
            let root = self
                .contract_trie_node_hash(root_index, contract)?
                .map(ContractRoot);
            Ok(root)
        } else {
            Ok(None)
        }
    }

    pub fn insert_class_root(
        &self,
        block_number: BlockNumber,
        update: RootIndexUpdate,
    ) -> anyhow::Result<()> {
        let new_root_index = match update {
            RootIndexUpdate::Unchanged => return Ok(()),
            RootIndexUpdate::Updated(idx) => Some(idx),
            RootIndexUpdate::TrieEmpty => None,
        };

        self.inner().execute(
            "INSERT OR REPLACE INTO class_roots (block_number, root_index) VALUES(?, ?)",
            params![&block_number, &new_root_index],
        )?;

        if let TriePruneMode::Prune { num_blocks_kept } = self.trie_prune_mode {
            if let Some(block_number) = block_number.checked_sub(num_blocks_kept) {
                self.delete_class_roots(block_number)?;
            }
        }

        Ok(())
    }

    fn delete_class_roots(&self, before_block: BlockNumber) -> anyhow::Result<()> {
        let mut stmt = self.inner().prepare_cached(
            "SELECT block_number
            FROM class_roots
            WHERE block_number <= ?
            ORDER BY block_number DESC
            LIMIT 1",
        )?;
        let last_block_with_root_index = stmt
            .query_row(params![&before_block], |row| row.get_block_number(0))
            .optional()?;

        if let Some(last_block_with_root_index) = last_block_with_root_index {
            tracing::trace!(%last_block_with_root_index, "Removing class roots");
            let mut stmt = self
                .inner()
                .prepare_cached("DELETE FROM class_roots WHERE block_number < ?")?;
            stmt.execute(params![&last_block_with_root_index])?;
        }

        Ok(())
    }

    pub fn insert_contract_state_hash(
        &self,
        block_number: BlockNumber,
        contract: ContractAddress,
        state_hash: ContractStateHash,
    ) -> anyhow::Result<()> {
        let column = self.rocksdb_get_column(&CONTRACT_STATE_HASHES_COLUMN);
        self.batch.lock().expect("Batch lock poisoned").put_cf(
            &column,
            contract_state_hashes_key(block_number, &contract),
            state_hash.0.as_be_bytes(),
        );

        if let TriePruneMode::Prune { num_blocks_kept } = self.trie_prune_mode {
            if let Some(block_number) = block_number.checked_sub(num_blocks_kept) {
                self.delete_contract_state_hashes(contract, block_number, num_blocks_kept > 0)?;
            }
        }

        Ok(())
    }

    fn delete_contract_state_hashes(
        &self,
        contract: ContractAddress,
        before_block: BlockNumber,
        keep_latest: bool,
    ) -> anyhow::Result<()> {
        let column = self.rocksdb_get_column(&CONTRACT_STATE_HASHES_COLUMN);
        let key = contract_state_hashes_key(before_block, &contract);

        let mut read_options = ReadOptions::default();
        read_options.set_prefix_same_as_start(true);
        let mut iter = self.rocksdb().raw_iterator_cf_opt(&column, read_options);
        iter.seek(key);
        if !iter.valid() {
            iter.status()
                .context("Seeking contract state hashes for deletion")?;
            return Ok(());
        }

        if keep_latest {
            iter.next();
        }
        let mut batch = self.batch.lock().expect("Batch lock poisoned");
        while iter.valid() {
            let key = iter.key().expect("Iterator is valid");
            batch.delete_cf(&column, key);
            iter.next();
        }
        iter.status()
            .context("Iterating contract state hashes for deletion")?;

        Ok(())
    }

    pub fn contract_state_hash(
        &self,
        block_number: BlockNumber,
        contract: ContractAddress,
    ) -> anyhow::Result<Option<ContractStateHash>> {
        let column = self.rocksdb_get_column(&CONTRACT_STATE_HASHES_COLUMN);
        let key = contract_state_hashes_key(block_number, &contract);

        let mut read_options = ReadOptions::default();
        read_options.set_prefix_same_as_start(true);
        let mut iter = self.rocksdb().raw_iterator_cf_opt(&column, read_options);
        iter.seek(key);
        if !iter.valid() {
            iter.status().context("Seeking contract state hash")?;
            return Ok(None);
        }

        let value = iter
            .value()
            .context("Reading contract state hash value from RocksDB")?;
        let value = Felt::from_be_slice(value).context("Parsing contract state hash value")?;
        Ok(Some(ContractStateHash(value)))
    }

    pub fn insert_storage_root(
        &self,
        block_number: BlockNumber,
        update: RootIndexUpdate,
    ) -> anyhow::Result<()> {
        let new_root_index = match update {
            RootIndexUpdate::Unchanged => return Ok(()),
            RootIndexUpdate::Updated(idx) => Some(idx),
            RootIndexUpdate::TrieEmpty => None,
        };
        self.inner().execute(
            "INSERT OR REPLACE INTO storage_roots (block_number, root_index) VALUES(?, ?)",
            params![&block_number, &new_root_index],
        )?;

        if let TriePruneMode::Prune { num_blocks_kept } = self.trie_prune_mode {
            if let Some(block_number) = block_number.checked_sub(num_blocks_kept) {
                self.delete_storage_roots(block_number)?;
            }
        }

        Ok(())
    }

    fn delete_storage_roots(&self, before_block: BlockNumber) -> anyhow::Result<()> {
        let mut stmt = self.inner().prepare_cached(
            "SELECT block_number
            FROM storage_roots
            WHERE block_number <= ?
            ORDER BY block_number DESC
            LIMIT 1",
        )?;
        let last_block_with_root_index = stmt
            .query_row(params![&before_block], |row| row.get_block_number(0))
            .optional()?;

        if let Some(last_block_with_root_index) = last_block_with_root_index {
            let mut stmt = self
                .inner()
                .prepare_cached("DELETE FROM storage_roots WHERE block_number < ?")?;
            stmt.execute(params![&last_block_with_root_index])?;
        }

        Ok(())
    }

    pub fn insert_contract_root(
        &self,
        block_number: BlockNumber,
        contract: ContractAddress,
        update: RootIndexUpdate,
    ) -> anyhow::Result<()> {
        let new_root_index = match update {
            RootIndexUpdate::Unchanged => return Ok(()),
            RootIndexUpdate::Updated(idx) => Some(idx),
            RootIndexUpdate::TrieEmpty => None,
        };
        self.inner().execute(
            "INSERT OR REPLACE INTO contract_roots (block_number, contract_address, root_index) \
             VALUES(?, ?, ?)",
            params![&block_number, &contract, &new_root_index],
        )?;

        if let TriePruneMode::Prune { num_blocks_kept } = self.trie_prune_mode {
            if let Some(block_number) = block_number.checked_sub(num_blocks_kept) {
                self.delete_contract_roots(contract, block_number)?;
            }
        }

        Ok(())
    }

    fn delete_contract_roots(
        &self,
        contract: ContractAddress,
        before_block: BlockNumber,
    ) -> anyhow::Result<()> {
        let mut stmt = self.inner().prepare_cached(
            "SELECT block_number
            FROM contract_roots
            WHERE contract_address = ? AND block_number <= ?
            ORDER BY block_number DESC
            LIMIT 1",
        )?;
        let last_block_with_root_index = stmt
            .query_row(params![&contract, &before_block], |row| {
                row.get_block_number(0)
            })
            .optional()?;

        if let Some(last_block_with_root_index) = last_block_with_root_index {
            let mut stmt = self.inner().prepare_cached(
                "DELETE FROM contract_roots WHERE contract_address = ? AND block_number < ?",
            )?;
            stmt.execute(params![&contract, &last_block_with_root_index])?;
        }

        Ok(())
    }

    pub fn insert_contract_trie(
        &self,
        update: &TrieUpdate,
        block_number: BlockNumber,
        contract_address: &ContractAddress,
    ) -> anyhow::Result<RootIndexUpdate> {
        self.insert_trie(
            update,
            block_number,
            "trie_contracts",
            &TRIE_CONTRACT_COLUMN,
            Some(&contract_address.0),
        )
    }

    pub fn contract_trie_node(
        &self,
        index: TrieStorageIndex,
        contract_address: &ContractAddress,
    ) -> anyhow::Result<Option<StoredNode>> {
        self.trie_node(index, &TRIE_CONTRACT_COLUMN, Some(&contract_address.0))
    }

    pub fn contract_trie_node_hash(
        &self,
        index: TrieStorageIndex,
        contract_address: &ContractAddress,
    ) -> anyhow::Result<Option<Felt>> {
        self.trie_node_hash(index, &TRIE_CONTRACT_COLUMN, Some(&contract_address.0))
    }

    pub fn insert_class_trie(
        &self,
        update: &TrieUpdate,
        block_number: BlockNumber,
    ) -> anyhow::Result<RootIndexUpdate> {
        self.insert_trie(update, block_number, "trie_class", &TRIE_CLASS_COLUMN, None)
    }

    pub fn class_trie_node(&self, index: TrieStorageIndex) -> anyhow::Result<Option<StoredNode>> {
        self.trie_node(index, &TRIE_CLASS_COLUMN, None)
    }

    pub fn class_trie_node_hash(&self, index: TrieStorageIndex) -> anyhow::Result<Option<Felt>> {
        self.trie_node_hash(index, &TRIE_CLASS_COLUMN, None)
    }

    pub fn insert_storage_trie(
        &self,
        update: &TrieUpdate,
        block_number: BlockNumber,
    ) -> anyhow::Result<RootIndexUpdate> {
        self.insert_trie(
            update,
            block_number,
            "trie_storage",
            &TRIE_STORAGE_COLUMN,
            None,
        )
    }

    pub fn storage_trie_node(&self, index: TrieStorageIndex) -> anyhow::Result<Option<StoredNode>> {
        self.trie_node(index, &TRIE_STORAGE_COLUMN, None)
    }

    pub fn storage_trie_node_hash(&self, index: TrieStorageIndex) -> anyhow::Result<Option<Felt>> {
        self.trie_node_hash(index, &TRIE_STORAGE_COLUMN, None)
    }

    /// Prune tries by removing nodes that are no longer needed at the given
    /// block.
    pub fn prune_tries(&self) -> anyhow::Result<()> {
        let Some(block_number) = self.block_number(pathfinder_common::BlockId::Latest)? else {
            return Ok(());
        };
        let TriePruneMode::Prune { num_blocks_kept } = self.trie_prune_mode else {
            return Ok(());
        };
        tracing::info!("Cleaning up state trie");
        self.prune_trie(
            block_number,
            num_blocks_kept,
            "trie_contracts",
            &TRIE_CONTRACT_COLUMN,
        )?;
        self.prune_trie(
            block_number,
            num_blocks_kept,
            "trie_class",
            &TRIE_CLASS_COLUMN,
        )?;
        self.prune_trie(
            block_number,
            num_blocks_kept,
            "trie_storage",
            &TRIE_STORAGE_COLUMN,
        )?;
        Ok(())
    }

    pub fn coalesce_trie_removals(&self, target_block: BlockNumber) -> anyhow::Result<()> {
        self.coalesce_removed_trie_nodes(target_block, "trie_contracts")?;
        self.coalesce_removed_trie_nodes(target_block, "trie_storage")?;
        self.coalesce_removed_trie_nodes(target_block, "trie_class")
    }

    /// Mark the input nodes as ready for removal.
    fn remove_trie(
        &self,
        removed: &[TrieStorageIndex],
        block_number: BlockNumber,
        key_prefix: Option<&Felt>,
        table: &'static str,
    ) -> anyhow::Result<()> {
        if !removed.is_empty() {
            let marker = TrieRemovalMarker {
                key_prefix: key_prefix.cloned(),
                indices: removed.to_vec(),
            };
            let marker = bincode::encode_to_vec(marker, bincode::config::standard())
                .context("Serializing removal marker")?;

            let mut stmt = self
                .inner()
                .prepare_cached(&format!(
                    r"INSERT INTO {table}_removals (block_number, indices) VALUES (?, ?)"
                ))
                .context("Creating statement to insert removal marker")?;
            stmt.execute(params![&block_number, &marker,])
                .context("Inserting removal marker")?;
        }

        Ok(())
    }

    /// Coalesce removed trie nodes to the target block.
    ///
    /// "Moves" all removed nodes from blocks _after_ the target block into
    /// the target block.
    ///
    /// Used during a reorg to move deleted node data of all reorged-away blocks
    /// to our reorg target.
    fn coalesce_removed_trie_nodes(
        &self,
        target_block: BlockNumber,
        table: &'static str,
    ) -> anyhow::Result<()> {
        let mut stmt = self
            .inner()
            .prepare_cached(&format!(
                "UPDATE {table}_removals
                SET block_number = ?1
                WHERE block_number > ?1"
            ))
            .context("Creating update statement")?;
        stmt.execute(params![&target_block])
            .context("Moving removed trie node data to target block")?;

        Ok(())
    }

    /// Prune tries by removing nodes that are no longer needed.
    fn prune_trie(
        &self,
        block_number: BlockNumber,
        num_blocks_kept: u64,
        table: &'static str,
        rocksdb_column: &Column,
    ) -> anyhow::Result<()> {
        if let Some(before_block) = block_number.checked_sub(num_blocks_kept) {
            // Delete nodes that have already been marked as ready for deletion.
            let mut select_stmt = self
                .inner()
                .prepare_cached(&format!(
                    r"SELECT indices FROM {table}_removals WHERE block_number < ?"
                ))
                .context("Creating removal statement")?;
            let mut rows = select_stmt
                .query(params![&before_block])
                .context("Fetching nodes to delete")?;

            let hash_column = self.rocksdb_get_column(rocksdb_column);

            let mut batch = self.batch.lock().expect("Batch lock poisoned");
            let mut buf = [0u8; TRIE_CONTRACT_KEY_LEN];

            while let Some(row) = rows.next().context("Iterating over rows")? {
                let (marker, _) = bincode::decode_from_slice::<TrieRemovalMarker, _>(
                    row.get_blob(0)?,
                    bincode::config::standard(),
                )
                .context("Decoding removal marker")?;
                for idx in marker.indices.iter() {
                    let key = match marker.key_prefix {
                        Some(prefix) => {
                            contract_trie_key(&prefix, *idx, &mut buf);
                            &buf[..TRIE_CONTRACT_KEY_LEN]
                        }
                        None => {
                            buf[..8].copy_from_slice(&idx.0.to_be_bytes());
                            &buf[..8]
                        }
                    };
                    batch.delete_cf(&hash_column, key);
                }
                metrics::counter!(METRIC_TRIE_NODES_REMOVED, "table" => table)
                    .increment(marker.indices.len() as u64);
            }

            // Delete the removal markers.
            let mut delete_stmt = self
                .inner()
                .prepare_cached(&format!(
                    r"DELETE FROM {table}_removals WHERE block_number < ?"
                ))
                .context("Creating statement to delete removal markers")?;
            delete_stmt
                .execute(params![&before_block])
                .context("Deleting removal markers")?;
        }

        Ok(())
    }

    /// Stores the node data for a trie and returns the root index change.
    fn insert_trie(
        &self,
        update: &TrieUpdate,
        block_number: BlockNumber,
        table: &'static str,
        rocksdb_hash_column: &Column,
        key_prefix: Option<&Felt>,
    ) -> anyhow::Result<RootIndexUpdate> {
        if let TriePruneMode::Prune { num_blocks_kept } = self.trie_prune_mode {
            self.prune_trie(block_number, num_blocks_kept, table, rocksdb_hash_column)?;
            self.remove_trie(&update.nodes_removed, block_number, key_prefix, table)?;
        }

        if update.nodes_added.is_empty() {
            if !update.nodes_removed.is_empty() && update.root_commitment.is_zero() {
                return Ok(RootIndexUpdate::TrieEmpty);
            } else {
                return Ok(RootIndexUpdate::Unchanged);
            }
        }

        let mut to_insert = Vec::new();
        let mut to_process = vec![NodeRef::Index(update.nodes_added.len() - 1)];

        while let Some(node) = to_process.pop() {
            // Only index variants need to be stored.
            //
            // Leaf nodes never get stored and a node having an
            // ID indicates it has already been stored as part of a
            // previous tree - and its children as well.
            let NodeRef::Index(idx) = node else {
                continue;
            };

            let (_, node) = &update.nodes_added.get(idx).context("Node index missing")?;
            to_insert.push(idx);

            match node {
                Node::Binary { left, right } => {
                    to_process.push(*left);
                    to_process.push(*right);
                }
                Node::Edge { child, .. } => {
                    to_process.push(*child);
                }
                // Leaves are not stored as separate nodes but are instead serialized in-line in
                // their parents.
                Node::LeafEdge { .. } | Node::LeafBinary => {}
            }
        }

        let column = self.rocksdb_get_column(rocksdb_hash_column);

        let mut storage_idx_base = self
            .rocksdb
            .next_trie_storage_index(rocksdb_hash_column, to_insert.len());

        // Pre-allocate storage indices for all nodes to insert. This allows us to store
        // nodes in any order and still be able to reference their children.
        let indices: HashMap<usize, TrieStorageIndex> = to_insert
            .iter()
            .enumerate()
            .map(|(i, node_idx)| (*node_idx, storage_idx_base + i as u64))
            .collect();

        // Reusable (and oversized) buffer for encoding.
        let mut buffer = [0u8; 256];
        let mut key = [0u8; TRIE_CONTRACT_KEY_LEN];

        let mut batch = self.batch.lock().expect("Batch lock poisoned");

        for idx in to_insert.iter() {
            let (hash, node) = &update.nodes_added.get(*idx).context("Node index missing")?;

            let node = node.as_stored(&indices)?;

            buffer[0..32].copy_from_slice(hash.as_be_bytes());
            let length = node.encode(&mut buffer[32..]).context("Encoding node")?;

            let storage_idx = indices.get(idx).context("Storage index missing")?;

            let key_length = if let Some(key_prefix) = key_prefix {
                contract_trie_key(key_prefix, *storage_idx, &mut key);
                TRIE_CONTRACT_KEY_LEN
            } else {
                key[..8].copy_from_slice(&storage_idx.0.to_be_bytes());
                8
            };

            batch.put_cf(&column, &key[..key_length], &buffer[..length + 32]);

            metrics::counter!(METRIC_TRIE_NODES_ADDED, "table" => table).increment(1);
        }

        // Store next index for future use. This is read after startup to determine the
        // next index to use for new nodes.
        storage_idx_base += to_insert.len() as u64;
        let next_index_column = self.rocksdb_get_column(&TRIE_NEXT_INDEX_COLUMN);
        batch.put_cf(
            &next_index_column,
            rocksdb_hash_column.name.as_bytes(),
            storage_idx_base.0.to_be_bytes(),
        );

        if table == "trie_storage" && block_number.get() % 10000 == 9999 {
            self.rocksdb.log_stats();
        }

        Ok(RootIndexUpdate::Updated(
            *indices
                .get(&(update.nodes_added.len() - 1))
                .expect("Root index must exist as we just inserted it"),
        ))
    }

    /// Returns the node with the given index.
    fn trie_node(
        &self,
        index: TrieStorageIndex,
        rocksdb_column: &Column,
        key_prefix: Option<&Felt>,
    ) -> anyhow::Result<Option<StoredNode>> {
        let mut key = [0u8; TRIE_CONTRACT_KEY_LEN];
        let key_length = if let Some(key_prefix) = key_prefix {
            contract_trie_key(key_prefix, index, &mut key);
            TRIE_CONTRACT_KEY_LEN
        } else {
            key[..8].copy_from_slice(&index.0.to_be_bytes());
            8
        };
        let node = self
            .rocksdb()
            .get_pinned_cf(&self.rocksdb_get_column(rocksdb_column), &key[..key_length])?
            .map(|v| StoredNode::decode(&v.as_ref()[32..]).context("Decoding node from RocksDB"))
            .transpose()?;

        Ok(node)
    }

    /// Returns the hash of the node with the given index.
    fn trie_node_hash(
        &self,
        index: TrieStorageIndex,
        rocksdb_hash_column: &Column,
        key_prefix: Option<&Felt>,
    ) -> anyhow::Result<Option<Felt>> {
        let mut key = [0u8; TRIE_CONTRACT_KEY_LEN];
        let key_length = if let Some(key_prefix) = key_prefix {
            contract_trie_key(key_prefix, index, &mut key);
            TRIE_CONTRACT_KEY_LEN
        } else {
            key[..8].copy_from_slice(&index.0.to_be_bytes());
            8
        };
        let hash = self
            .rocksdb()
            .get_pinned_cf(
                &self.rocksdb_get_column(rocksdb_hash_column),
                &key[..key_length],
            )?
            .map(|v| {
                Felt::from_be_slice(&v.as_ref()[..32]).context("Decoding node hash from RocksDB")
            })
            .transpose()?;

        Ok(hash)
    }
}

const METRIC_TRIE_NODES_REMOVED: &str = "pathfinder_storage_trie_nodes_deleted_total";
const METRIC_TRIE_NODES_ADDED: &str = "pathfinder_storage_trie_nodes_added_total";

/// The result of committing a Merkle tree.
#[derive(Default, Debug)]
pub struct TrieUpdate {
    /// New nodes added. Note that these may contain false positives if the
    /// mutations resulted in removing and then re-adding the same nodes within
    /// the tree.
    ///
    /// The last node is the root of the trie.
    pub nodes_added: Vec<(Felt, Node)>,
    /// Nodes committed to storage that have been removed.
    pub nodes_removed: Vec<TrieStorageIndex>,
    /// New root commitment of the trie.
    pub root_commitment: Felt,
}

/// The storage index of a trie node.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct TrieStorageIndex(pub u64);

impl std::fmt::Display for TrieStorageIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for TrieStorageIndex {
    fn from(index: u64) -> Self {
        Self(index)
    }
}

impl From<TrieStorageIndex> for u64 {
    fn from(index: TrieStorageIndex) -> Self {
        index.0
    }
}

impl bincode::Encode for TrieStorageIndex {
    fn encode<E: bincode::enc::Encoder>(
        &self,
        encoder: &mut E,
    ) -> Result<(), bincode::error::EncodeError> {
        self.0.encode(encoder)
    }
}

impl<Context> bincode::Decode<Context> for TrieStorageIndex {
    fn decode<D: bincode::de::Decoder<Context = Context>>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        Ok(Self(u64::decode(decoder)?))
    }
}

impl<'de, Context> bincode::BorrowDecode<'de, Context> for TrieStorageIndex {
    fn borrow_decode<D: bincode::de::BorrowDecoder<'de, Context = Context>>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        Ok(Self(u64::borrow_decode(decoder)?))
    }
}

impl std::ops::Add for TrieStorageIndex {
    type Output = Self;

    fn add(self, rhs: Self) -> Self {
        Self(
            self.0
                .checked_add(rhs.0)
                .expect("TrieStorageIndex addition overflow"),
        )
    }
}

impl std::ops::Add<u64> for TrieStorageIndex {
    type Output = Self;

    fn add(self, rhs: u64) -> Self {
        Self(
            self.0
                .checked_add(rhs)
                .expect("TrieStorageIndex addition overflow"),
        )
    }
}

impl std::ops::AddAssign<u64> for TrieStorageIndex {
    fn add_assign(&mut self, rhs: u64) {
        self.0 = self
            .0
            .checked_add(rhs)
            .expect("TrieStorageIndex addition overflow");
    }
}

impl std::ops::Sub for TrieStorageIndex {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self {
        Self(
            self.0
                .checked_sub(rhs.0)
                .expect("TrieStorageIndex subtraction underflow"),
        )
    }
}

impl std::ops::Sub<u64> for TrieStorageIndex {
    type Output = Self;

    fn sub(self, rhs: u64) -> Self {
        Self(
            self.0
                .checked_sub(rhs)
                .expect("TrieStorageIndex subtraction underflow"),
        )
    }
}

impl std::ops::SubAssign<u64> for TrieStorageIndex {
    fn sub_assign(&mut self, rhs: u64) {
        self.0 = self
            .0
            .checked_sub(rhs)
            .expect("TrieStorageIndex subtraction underflow");
    }
}

/// The result of inserting a `TrieUpdate`.
#[derive(Debug, PartialEq)]
pub enum RootIndexUpdate {
    Unchanged,
    Updated(TrieStorageIndex),
    TrieEmpty,
}

#[derive(Clone, Debug)]
pub enum Node {
    Binary {
        left: NodeRef,
        right: NodeRef,
    },
    Edge {
        child: NodeRef,
        path: BitVec<u8, Msb0>,
    },
    LeafBinary,
    LeafEdge {
        path: BitVec<u8, Msb0>,
    },
}

#[derive(Copy, Clone, Debug)]
pub enum NodeRef {
    // A reference to a node that has already been committed to storage.
    StorageIndex(TrieStorageIndex),
    // A reference to a node that has not yet been committed to storage.
    // The index within the `nodes_added` vector is used as a reference.
    Index(usize),
}

#[derive(Clone, Debug, PartialEq)]
pub enum StoredNode {
    Binary {
        left: TrieStorageIndex,
        right: TrieStorageIndex,
    },
    Edge {
        child: TrieStorageIndex,
        path: BitVec<u8, Msb0>,
    },
    LeafBinary,
    LeafEdge {
        path: BitVec<u8, Msb0>,
    },
}

#[derive(Clone, Debug, bincode::Encode, bincode::BorrowDecode)]
enum StoredSerde {
    Binary {
        left: TrieStorageIndex,
        right: TrieStorageIndex,
    },
    Edge {
        child: TrieStorageIndex,
        path: Vec<u8>,
    },
    LeafBinary,
    LeafEdge {
        path: Vec<u8>,
    },
}

impl StoredNode {
    const CODEC_CFG: bincode::config::Configuration = bincode::config::standard();

    /// Writes the [StoredNode] into `buffer` and returns the number of bytes
    /// written.
    fn encode(&self, buffer: &mut [u8]) -> Result<usize, bincode::error::EncodeError> {
        let helper = match self {
            Self::Binary { left, right } => StoredSerde::Binary {
                left: *left,
                right: *right,
            },
            Self::Edge { child, path } => {
                let path_length = path.len() as u8;

                let mut path = path.to_owned();
                path.force_align();
                let mut path = path.into_vec();
                path.push(path_length);

                StoredSerde::Edge {
                    child: *child,
                    path,
                }
            }
            Self::LeafBinary => StoredSerde::LeafBinary,
            Self::LeafEdge { path } => {
                let path_length = path.len() as u8;

                let mut path = path.to_owned();
                path.force_align();
                let mut path = path.into_vec();
                path.push(path_length);

                StoredSerde::LeafEdge { path }
            }
        };
        // Do not use serialize() as this will invoke serialization twice.
        // https://github.com/bincode-org/bincode/issues/401
        bincode::encode_into_slice(helper, buffer, Self::CODEC_CFG)
    }

    fn decode(data: &[u8]) -> Result<Self, bincode::error::DecodeError> {
        let helper = bincode::borrow_decode_from_slice(data, Self::CODEC_CFG)?;

        let node = match helper.0 {
            StoredSerde::Binary { left, right } => Self::Binary { left, right },
            StoredSerde::Edge { child, mut path } => {
                let path_length = path.pop().ok_or(bincode::error::DecodeError::Other(
                    "Edge node's path length is missing",
                ))?;
                let mut path = bitvec::vec::BitVec::from_vec(path);
                path.resize(path_length as usize, false);
                Self::Edge { child, path }
            }
            StoredSerde::LeafBinary => Self::LeafBinary,
            StoredSerde::LeafEdge { mut path } => {
                let path_length = path.pop().ok_or(bincode::error::DecodeError::Other(
                    "Edge node's path length is missing",
                ))?;
                let mut path = bitvec::vec::BitVec::from_vec(path);
                path.resize(path_length as usize, false);
                Self::LeafEdge { path }
            }
        };

        Ok(node)
    }
}

impl Node {
    fn as_stored(&self, indices: &HashMap<usize, TrieStorageIndex>) -> anyhow::Result<StoredNode> {
        let node = match self {
            Node::Binary { left, right } => {
                let left = match left {
                    NodeRef::StorageIndex(id) => *id,
                    NodeRef::Index(idx) => *indices.get(idx).context("Node index missing")?,
                };

                let right = match right {
                    NodeRef::StorageIndex(id) => *id,
                    NodeRef::Index(idx) => *indices.get(idx).context("Node index missing")?,
                };

                StoredNode::Binary { left, right }
            }
            Node::Edge { child, path } => {
                let child = match child {
                    NodeRef::StorageIndex(id) => *id,
                    NodeRef::Index(idx) => *indices.get(idx).context("Node index missing")?,
                };

                StoredNode::Edge {
                    child,
                    path: path.clone(),
                }
            }
            Node::LeafEdge { path } => StoredNode::LeafEdge { path: path.clone() },
            Node::LeafBinary => StoredNode::LeafBinary,
        };

        Ok(node)
    }
}

#[cfg(test)]
mod tests {
    use pathfinder_common::macro_prelude::*;

    use super::*;

    #[test]
    fn class_roots() {
        let mut db = crate::StorageBuilder::in_memory()
            .unwrap()
            .connection()
            .unwrap();
        let tx = db.transaction().unwrap();

        let result = tx.class_root_index(BlockNumber::GENESIS).unwrap();
        assert_eq!(result, None);

        tx.insert_class_root(BlockNumber::GENESIS, RootIndexUpdate::Updated(123.into()))
            .unwrap();
        let result = tx.class_root_index(BlockNumber::GENESIS).unwrap();
        assert_eq!(result, Some(123.into()));

        tx.insert_class_root(
            BlockNumber::GENESIS + 1,
            RootIndexUpdate::Updated(456.into()),
        )
        .unwrap();
        let result = tx.class_root_index(BlockNumber::GENESIS).unwrap();
        assert_eq!(result, Some(123.into()));
        let result = tx.class_root_index(BlockNumber::GENESIS + 1).unwrap();
        assert_eq!(result, Some(456.into()));
        let result = tx.class_root_index(BlockNumber::GENESIS + 2).unwrap();
        assert_eq!(result, Some(456.into()));

        tx.insert_class_root(
            BlockNumber::GENESIS + 10,
            RootIndexUpdate::Updated(789.into()),
        )
        .unwrap();
        let result = tx.class_root_index(BlockNumber::GENESIS + 9).unwrap();
        assert_eq!(result, Some(456.into()));
        let result = tx.class_root_index(BlockNumber::GENESIS + 10).unwrap();
        assert_eq!(result, Some(789.into()));
        let result = tx.class_root_index(BlockNumber::GENESIS + 11).unwrap();
        assert_eq!(result, Some(789.into()));

        tx.insert_class_root(BlockNumber::GENESIS + 12, RootIndexUpdate::TrieEmpty)
            .unwrap();
        let result = tx.class_root_index(BlockNumber::GENESIS + 12).unwrap();
        assert_eq!(result, None);
        let result = tx.class_root_index(BlockNumber::GENESIS + 13).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn storage_roots() {
        let mut db = crate::StorageBuilder::in_memory()
            .unwrap()
            .connection()
            .unwrap();
        let tx = db.transaction().unwrap();

        let result = tx.storage_root_index(BlockNumber::GENESIS).unwrap();
        assert_eq!(result, None);

        tx.insert_storage_root(BlockNumber::GENESIS, RootIndexUpdate::Updated(123.into()))
            .unwrap();
        let result = tx.storage_root_index(BlockNumber::GENESIS).unwrap();
        assert_eq!(result, Some(123.into()));

        tx.insert_storage_root(
            BlockNumber::GENESIS + 1,
            RootIndexUpdate::Updated(456.into()),
        )
        .unwrap();
        let result = tx.storage_root_index(BlockNumber::GENESIS).unwrap();
        assert_eq!(result, Some(123.into()));
        let result = tx.storage_root_index(BlockNumber::GENESIS + 1).unwrap();
        assert_eq!(result, Some(456.into()));
        let result = tx.storage_root_index(BlockNumber::GENESIS + 2).unwrap();
        assert_eq!(result, Some(456.into()));

        tx.insert_storage_root(
            BlockNumber::GENESIS + 10,
            RootIndexUpdate::Updated(789.into()),
        )
        .unwrap();
        let result = tx.storage_root_index(BlockNumber::GENESIS + 9).unwrap();
        assert_eq!(result, Some(456.into()));
        let result = tx.storage_root_index(BlockNumber::GENESIS + 10).unwrap();
        assert_eq!(result, Some(789.into()));
        let result = tx.storage_root_index(BlockNumber::GENESIS + 11).unwrap();
        assert_eq!(result, Some(789.into()));

        tx.insert_storage_root(BlockNumber::GENESIS + 12, RootIndexUpdate::TrieEmpty)
            .unwrap();
        let result = tx.storage_root_index(BlockNumber::GENESIS + 12).unwrap();
        assert_eq!(result, None);
        let result = tx.storage_root_index(BlockNumber::GENESIS + 13).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn contract_roots() {
        let mut db = crate::StorageBuilder::in_memory()
            .unwrap()
            .connection()
            .unwrap();
        let tx = db.transaction().unwrap();

        let c1 = contract_address_bytes!(b"first");
        let c2 = contract_address_bytes!(b"second");

        // Simplest trie node setup so we can test the fetching of contract root hashes.
        let root0 = contract_root_bytes!(b"root 0");
        let root_node = Node::LeafBinary;
        let nodes = vec![(root0.0, root_node.clone())];
        let update = TrieUpdate {
            nodes_added: nodes,
            ..Default::default()
        };

        let idx0_update = tx
            .insert_contract_trie(&update, BlockNumber::GENESIS, &c1)
            .unwrap();
        tx.flush_rocksdb_batch().unwrap();
        let RootIndexUpdate::Updated(idx0) = idx0_update else {
            panic!("Expected the root index to be updated");
        };

        let result1 = tx.contract_root_index(BlockNumber::GENESIS, &c1).unwrap();
        assert_eq!(result1, None);

        tx.insert_contract_root(BlockNumber::GENESIS, c1, idx0_update)
            .unwrap();
        let result1 = tx.contract_root_index(BlockNumber::GENESIS, &c1).unwrap();
        let result2 = tx.contract_root_index(BlockNumber::GENESIS, &c2).unwrap();
        let hash1 = tx.contract_root(BlockNumber::GENESIS, &c1).unwrap();
        let hash2 = tx.contract_root(BlockNumber::GENESIS, &c2).unwrap();
        assert_eq!(result1, Some(idx0));
        assert_eq!(result2, None);
        assert_eq!(hash1, Some(root0));
        assert_eq!(hash2, None);

        let root1 = contract_root_bytes!(b"root 1");
        let nodes = vec![(root1.0, root_node.clone())];
        let update = TrieUpdate {
            nodes_added: nodes,
            ..Default::default()
        };

        // Insert trie data for c1 at block 1 (contract trie keys are
        // contract-prefixed in RocksDB, so each contract needs its own data).
        let idx1_c1_update = tx
            .insert_contract_trie(&update, BlockNumber::GENESIS + 1, &c1)
            .unwrap();
        let _idx1_c2_update = tx
            .insert_contract_trie(&update, BlockNumber::GENESIS + 1, &c2)
            .unwrap();
        tx.flush_rocksdb_batch().unwrap();
        let RootIndexUpdate::Updated(idx1) = idx1_c1_update else {
            panic!("Expected the root index to be updated");
        };

        tx.insert_contract_root(BlockNumber::GENESIS + 1, c1, idx1_c1_update)
            .unwrap();
        tx.insert_contract_root(
            BlockNumber::GENESIS + 1,
            c2,
            RootIndexUpdate::Updated(888.into()),
        )
        .unwrap();
        let result1 = tx.contract_root_index(BlockNumber::GENESIS, &c1).unwrap();
        let result2 = tx.contract_root_index(BlockNumber::GENESIS, &c2).unwrap();
        let hash1 = tx.contract_root(BlockNumber::GENESIS, &c1).unwrap();
        assert_eq!(result1, Some(idx0));
        assert_eq!(result2, None);
        assert_eq!(hash1, Some(root0));
        let result1 = tx
            .contract_root_index(BlockNumber::GENESIS + 1, &c1)
            .unwrap();
        let result2 = tx
            .contract_root_index(BlockNumber::GENESIS + 1, &c2)
            .unwrap();
        let hash1 = tx.contract_root(BlockNumber::GENESIS + 1, &c1).unwrap();
        assert_eq!(result1, Some(idx1));
        assert_eq!(result2, Some(888.into()));
        assert_eq!(hash1, Some(root1));
        let result1 = tx
            .contract_root_index(BlockNumber::GENESIS + 2, &c1)
            .unwrap();
        let result2 = tx
            .contract_root_index(BlockNumber::GENESIS + 2, &c2)
            .unwrap();
        let hash1 = tx.contract_root(BlockNumber::GENESIS + 2, &c1).unwrap();
        assert_eq!(result1, Some(idx1));
        assert_eq!(result2, Some(888.into()));
        assert_eq!(hash1, Some(root1));

        let root2 = contract_root_bytes!(b"root 2");
        let nodes = vec![(root2.0, root_node.clone())];
        let update = TrieUpdate {
            nodes_added: nodes,
            ..Default::default()
        };
        let idx2_update = tx
            .insert_contract_trie(&update, BlockNumber::GENESIS + 10, &c1)
            .unwrap();
        tx.flush_rocksdb_batch().unwrap();
        let RootIndexUpdate::Updated(idx2) = idx2_update else {
            panic!("Expected the root index to be updated");
        };

        tx.insert_contract_root(BlockNumber::GENESIS + 10, c1, idx2_update)
            .unwrap();
        tx.insert_contract_root(
            BlockNumber::GENESIS + 11,
            c2,
            RootIndexUpdate::Updated(999.into()),
        )
        .unwrap();
        let result1 = tx
            .contract_root_index(BlockNumber::GENESIS + 9, &c1)
            .unwrap();
        let result2 = tx
            .contract_root_index(BlockNumber::GENESIS + 9, &c2)
            .unwrap();
        let hash1 = tx.contract_root(BlockNumber::GENESIS + 9, &c1).unwrap();
        assert_eq!(result1, Some(idx1));
        assert_eq!(result2, Some(888.into()));
        assert_eq!(hash1, Some(root1));
        let result1 = tx
            .contract_root_index(BlockNumber::GENESIS + 10, &c1)
            .unwrap();
        let result2 = tx
            .contract_root_index(BlockNumber::GENESIS + 10, &c2)
            .unwrap();
        let hash1 = tx.contract_root(BlockNumber::GENESIS + 10, &c1).unwrap();
        assert_eq!(result1, Some(idx2));
        assert_eq!(result2, Some(888.into()));
        assert_eq!(hash1, Some(root2));
        let result2 = tx
            .contract_root_index(BlockNumber::GENESIS + 11, &c2)
            .unwrap();
        let hash1 = tx.contract_root(BlockNumber::GENESIS + 11, &c1).unwrap();
        assert_eq!(result2, Some(999.into()));
        assert_eq!(hash1, Some(root2));

        tx.insert_contract_root(BlockNumber::GENESIS + 12, c1, RootIndexUpdate::TrieEmpty)
            .unwrap();
        let result1 = tx
            .contract_root_index(BlockNumber::GENESIS + 10, &c1)
            .unwrap();
        let hash1 = tx.contract_root(BlockNumber::GENESIS + 10, &c1).unwrap();
        assert_eq!(result1, Some(idx2));
        assert_eq!(hash1, Some(root2));
        let result1 = tx
            .contract_root_index(BlockNumber::GENESIS + 12, &c1)
            .unwrap();
        let hash1 = tx.contract_root(BlockNumber::GENESIS + 12, &c1).unwrap();
        assert_eq!(result1, None);
        assert_eq!(hash1, None);
    }

    #[rstest::rstest]
    #[case::binary(StoredNode::Binary {
        left: TrieStorageIndex(12), right: TrieStorageIndex(34)
    })]
    #[case::edge(StoredNode::Edge {
        child: TrieStorageIndex(123),
        path: bitvec::bitvec![u8, Msb0; 1,0,0,1,0,1,0,0,0,0,0,1,1,1,1]
    })]
    #[case::binary(StoredNode::LeafBinary)]
    #[case::binary(StoredNode::LeafEdge {
        path: bitvec::bitvec![u8, Msb0; 1,0,0,1,0,1,0,0,0,0,0,1,1,1,1]
    })]
    #[case::edge_max_path(StoredNode::Edge {
        child: TrieStorageIndex(123),
        path: bitvec::bitvec![u8, Msb0; 1; 251]
    })]
    #[case::edge_min_path(StoredNode::Edge {
        child: TrieStorageIndex(123),
        path: bitvec::bitvec![u8, Msb0; 0]
    })]
    fn serde(#[case] node: StoredNode) {
        let mut buffer = vec![0; 256];
        let length = node.encode(&mut buffer).unwrap();
        let result = StoredNode::decode(&buffer[..length]).unwrap();

        assert_eq!(result, node);
    }

    #[test]
    fn contract_state_hash() {
        let mut db = crate::StorageBuilder::in_memory()
            .unwrap()
            .connection()
            .unwrap();
        let tx = db.transaction().unwrap();

        let contract = contract_address_bytes!(b"address");
        let state_hash = contract_state_hash_bytes!(b"state hash");

        tx.insert_contract_state_hash(BlockNumber::GENESIS + 2, contract, state_hash)
            .unwrap();
        tx.flush_rocksdb_batch().unwrap();

        let result = tx
            .contract_state_hash(BlockNumber::GENESIS, contract)
            .unwrap();
        assert!(result.is_none());

        let result = tx
            .contract_state_hash(BlockNumber::GENESIS + 2, contract)
            .unwrap();
        assert_eq!(result, Some(state_hash));

        let result = tx
            .contract_state_hash(BlockNumber::GENESIS + 10, contract)
            .unwrap();
        assert_eq!(result, Some(state_hash));

        let result = tx
            .contract_state_hash(
                BlockNumber::GENESIS + 2,
                contract_address_bytes!(b"missing"),
            )
            .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn class_trie_pruning() {
        let mut db = crate::StorageBuilder::in_memory_with_trie_pruning(TriePruneMode::Prune {
            num_blocks_kept: 2,
        })
        .unwrap()
        .connection()
        .unwrap();
        let tx = db.transaction().unwrap();

        tx.insert_class_trie(
            &TrieUpdate {
                nodes_added: vec![
                    (
                        felt!("0x0"),
                        Node::Binary {
                            left: NodeRef::Index(1),
                            right: NodeRef::Index(2),
                        },
                    ),
                    (felt!("0x1"), Node::LeafBinary),
                    (felt!("0x2"), Node::LeafBinary),
                ],
                nodes_removed: vec![],
                root_commitment: Felt::ZERO,
            },
            BlockNumber::GENESIS,
        )
        .unwrap();
        tx.insert_class_trie(
            &TrieUpdate {
                nodes_added: vec![
                    (
                        felt!("0x3"),
                        Node::Binary {
                            left: NodeRef::Index(1),
                            right: NodeRef::Index(2),
                        },
                    ),
                    (felt!("0x4"), Node::LeafBinary),
                    (felt!("0x5"), Node::LeafBinary),
                ],
                nodes_removed: vec![TrieStorageIndex(1)],
                root_commitment: Felt::ZERO,
            },
            BlockNumber::GENESIS + 1,
        )
        .unwrap();
        tx.insert_class_trie(
            &TrieUpdate {
                nodes_added: vec![
                    (
                        felt!("0x6"),
                        Node::Binary {
                            left: NodeRef::Index(1),
                            right: NodeRef::Index(2),
                        },
                    ),
                    (felt!("0x7"), Node::LeafBinary),
                    (felt!("0x8"), Node::LeafBinary),
                ],
                nodes_removed: vec![],
                root_commitment: Felt::ZERO,
            },
            BlockNumber::GENESIS + 2,
        )
        .unwrap();
        tx.insert_class_trie(
            &TrieUpdate {
                nodes_added: vec![
                    (
                        felt!("0x9"),
                        Node::Binary {
                            left: NodeRef::Index(1),
                            right: NodeRef::Index(2),
                        },
                    ),
                    (felt!("0x10"), Node::LeafBinary),
                    (felt!("0x11"), Node::LeafBinary),
                ],
                nodes_removed: vec![],
                root_commitment: Felt::ZERO,
            },
            BlockNumber::GENESIS + 3,
        )
        .unwrap();
        tx.flush_rocksdb_batch().unwrap();

        // At this point, index 1 should still be in the table.
        assert!(tx.class_trie_node(TrieStorageIndex(1)).unwrap().is_some());

        tx.insert_class_trie(
            &TrieUpdate {
                nodes_added: vec![
                    (
                        felt!("0x12"),
                        Node::Binary {
                            left: NodeRef::Index(1),
                            right: NodeRef::Index(2),
                        },
                    ),
                    (felt!("0x13"), Node::LeafBinary),
                    (felt!("0x14"), Node::LeafBinary),
                ],
                nodes_removed: vec![],
                root_commitment: Felt::ZERO,
            },
            BlockNumber::GENESIS + 4,
        )
        .unwrap();
        tx.flush_rocksdb_batch().unwrap();

        // At this point, index 1 should no longer be in the table.
        assert!(tx.class_trie_node(TrieStorageIndex(1)).unwrap().is_none());
    }

    #[test]
    fn class_trie_pruning_change_config() {
        let mut db = crate::StorageBuilder::in_memory_with_trie_pruning(TriePruneMode::Prune {
            num_blocks_kept: 100,
        })
        .unwrap()
        .connection()
        .unwrap();
        let mut tx = db.transaction().unwrap();

        tx.insert_class_trie(
            &TrieUpdate {
                nodes_added: vec![
                    (
                        felt!("0x0"),
                        Node::Binary {
                            left: NodeRef::Index(1),
                            right: NodeRef::Index(2),
                        },
                    ),
                    (felt!("0x1"), Node::LeafBinary),
                    (felt!("0x2"), Node::LeafBinary),
                ],
                nodes_removed: vec![],
                root_commitment: Felt::ZERO,
            },
            BlockNumber::GENESIS,
        )
        .unwrap();
        tx.insert_class_trie(
            &TrieUpdate {
                nodes_added: vec![
                    (
                        felt!("0x3"),
                        Node::Binary {
                            left: NodeRef::Index(1),
                            right: NodeRef::Index(2),
                        },
                    ),
                    (felt!("0x4"), Node::LeafBinary),
                    (felt!("0x5"), Node::LeafBinary),
                ],
                nodes_removed: vec![TrieStorageIndex(1)],
                root_commitment: Felt::ZERO,
            },
            BlockNumber::GENESIS + 1,
        )
        .unwrap();
        tx.insert_class_trie(
            &TrieUpdate {
                nodes_added: vec![
                    (
                        felt!("0x6"),
                        Node::Binary {
                            left: NodeRef::Index(1),
                            right: NodeRef::Index(2),
                        },
                    ),
                    (felt!("0x7"), Node::LeafBinary),
                    (felt!("0x8"), Node::LeafBinary),
                ],
                nodes_removed: vec![],
                root_commitment: Felt::ZERO,
            },
            BlockNumber::GENESIS + 2,
        )
        .unwrap();
        tx.insert_class_trie(
            &TrieUpdate {
                nodes_added: vec![
                    (
                        felt!("0x9"),
                        Node::Binary {
                            left: NodeRef::Index(1),
                            right: NodeRef::Index(2),
                        },
                    ),
                    (felt!("0x10"), Node::LeafBinary),
                    (felt!("0x11"), Node::LeafBinary),
                ],
                nodes_removed: vec![],
                root_commitment: Felt::ZERO,
            },
            BlockNumber::GENESIS + 3,
        )
        .unwrap();

        tx.insert_class_trie(
            &TrieUpdate {
                nodes_added: vec![
                    (
                        felt!("0x12"),
                        Node::Binary {
                            left: NodeRef::Index(1),
                            right: NodeRef::Index(2),
                        },
                    ),
                    (felt!("0x13"), Node::LeafBinary),
                    (felt!("0x14"), Node::LeafBinary),
                ],
                nodes_removed: vec![],
                root_commitment: Felt::ZERO,
            },
            BlockNumber::GENESIS + 4,
        )
        .unwrap();
        tx.flush_rocksdb_batch().unwrap();

        // Nothing was pruned.
        assert!(tx.class_trie_node(TrieStorageIndex(1)).unwrap().is_some());

        // Simulate a configuration change.
        tx.trie_prune_mode = TriePruneMode::Prune { num_blocks_kept: 2 };
        tx.insert_block_header(&BlockHeader {
            number: BlockNumber::GENESIS + 4,
            ..Default::default()
        })
        .unwrap();
        tx.prune_tries().unwrap();
        tx.flush_rocksdb_batch().unwrap();

        // The class trie was pruned.
        assert!(tx.class_trie_node(TrieStorageIndex(1)).unwrap().is_none());
    }

    #[test]
    fn class_trie_pruning_keep_zero_blocks() {
        let mut db = crate::StorageBuilder::in_memory_with_trie_pruning(TriePruneMode::Prune {
            num_blocks_kept: 0,
        })
        .unwrap()
        .connection()
        .unwrap();
        let tx = db.transaction().unwrap();

        tx.insert_class_trie(
            &TrieUpdate {
                nodes_added: vec![
                    (
                        felt!("0x0"),
                        Node::Binary {
                            left: NodeRef::Index(1),
                            right: NodeRef::Index(2),
                        },
                    ),
                    (felt!("0x1"), Node::LeafBinary),
                    (felt!("0x2"), Node::LeafBinary),
                ],
                nodes_removed: vec![],
                root_commitment: Felt::ZERO,
            },
            BlockNumber::GENESIS,
        )
        .unwrap();
        tx.insert_class_trie(
            &TrieUpdate {
                nodes_added: vec![
                    (
                        felt!("0x3"),
                        Node::Binary {
                            left: NodeRef::Index(1),
                            right: NodeRef::Index(2),
                        },
                    ),
                    (felt!("0x4"), Node::LeafBinary),
                    (felt!("0x5"), Node::LeafBinary),
                ],
                nodes_removed: vec![],
                root_commitment: Felt::ZERO,
            },
            BlockNumber::GENESIS,
        )
        .unwrap();
        tx.insert_class_trie(
            &TrieUpdate {
                nodes_added: vec![
                    (
                        felt!("0x6"),
                        Node::Binary {
                            left: NodeRef::Index(1),
                            right: NodeRef::Index(2),
                        },
                    ),
                    (felt!("0x7"), Node::LeafBinary),
                    (felt!("0x8"), Node::LeafBinary),
                ],
                nodes_removed: vec![],
                root_commitment: Felt::ZERO,
            },
            BlockNumber::GENESIS,
        )
        .unwrap();
        tx.flush_rocksdb_batch().unwrap();

        // Each insert_class_trie only stores tree-reachable nodes. With
        // (Binary, LeafBinary, LeafBinary), the traversal starts from the
        // last node (a leaf), so only 1 node is stored per insert.
        // 3 inserts → storage indices 0, 1, 2.
        assert!(tx.class_trie_node(TrieStorageIndex(0)).unwrap().is_some());
        assert!(tx.class_trie_node(TrieStorageIndex(1)).unwrap().is_some());
        assert!(tx.class_trie_node(TrieStorageIndex(2)).unwrap().is_some());

        tx.insert_class_trie(
            &TrieUpdate {
                nodes_added: vec![
                    (
                        felt!("0x3"),
                        Node::Binary {
                            left: NodeRef::Index(1),
                            right: NodeRef::Index(2),
                        },
                    ),
                    (felt!("0x4"), Node::LeafBinary),
                    (felt!("0x5"), Node::LeafBinary),
                ],
                nodes_removed: vec![0, 1, 2].into_iter().map(TrieStorageIndex).collect(),
                root_commitment: Felt::ZERO,
            },
            BlockNumber::GENESIS + 1,
        )
        .unwrap();
        tx.insert_class_trie(
            &TrieUpdate {
                nodes_added: vec![
                    (
                        felt!("0x6"),
                        Node::Binary {
                            left: NodeRef::Index(1),
                            right: NodeRef::Index(2),
                        },
                    ),
                    (felt!("0x7"), Node::LeafBinary),
                    (felt!("0x8"), Node::LeafBinary),
                ],
                nodes_removed: vec![],
                root_commitment: Felt::ZERO,
            },
            BlockNumber::GENESIS + 2,
        )
        .unwrap();
        tx.flush_rocksdb_batch().unwrap();

        assert!(tx.class_trie_node(TrieStorageIndex(0)).unwrap().is_none());
        assert!(tx.class_trie_node(TrieStorageIndex(1)).unwrap().is_none());
        assert!(tx.class_trie_node(TrieStorageIndex(2)).unwrap().is_none());
    }

    #[test]
    fn class_trie_root_updates() {
        let mut db = crate::StorageBuilder::in_memory_with_trie_pruning(TriePruneMode::Prune {
            num_blocks_kept: 0,
        })
        .unwrap()
        .connection()
        .unwrap();
        let tx = db.transaction().unwrap();

        let root_update = tx
            .insert_class_trie(
                &TrieUpdate {
                    nodes_added: vec![
                        (
                            felt!("0x0"),
                            Node::Binary {
                                left: NodeRef::Index(1),
                                right: NodeRef::Index(2),
                            },
                        ),
                        (felt!("0x1"), Node::LeafBinary),
                        (felt!("0x2"), Node::LeafBinary),
                    ],
                    nodes_removed: vec![],
                    root_commitment: Felt::ZERO,
                },
                BlockNumber::GENESIS,
            )
            .unwrap();
        assert_eq!(root_update, RootIndexUpdate::Updated(0.into()));
    }

    #[test]
    fn class_root_insert_should_prune_old_roots() {
        let mut db = crate::StorageBuilder::in_memory_with_trie_pruning(TriePruneMode::Prune {
            num_blocks_kept: 1,
        })
        .unwrap()
        .connection()
        .unwrap();
        let tx = db.transaction().unwrap();

        tx.insert_class_root(BlockNumber::GENESIS, RootIndexUpdate::Updated(1.into()))
            .unwrap();
        tx.insert_class_root(
            BlockNumber::new_or_panic(1),
            RootIndexUpdate::Updated(2.into()),
        )
        .unwrap();
        // no root inserted for block 2
        tx.insert_class_root(
            BlockNumber::new_or_panic(3),
            RootIndexUpdate::Updated(3.into()),
        )
        .unwrap();

        assert!(!tx.class_root_exists(BlockNumber::GENESIS).unwrap());
        // root at block 1 cannot be deleted because it is still required for
        // reconstructing state at block 2
        assert!(tx.class_root_exists(BlockNumber::new_or_panic(1)).unwrap());
        assert!(tx.class_root_exists(BlockNumber::new_or_panic(3)).unwrap());
    }

    #[test]
    fn class_root_insert_should_prune_old_roots_in_no_history_mode() {
        let mut db = crate::StorageBuilder::in_memory_with_trie_pruning(TriePruneMode::Prune {
            num_blocks_kept: 0,
        })
        .unwrap()
        .connection()
        .unwrap();
        let tx = db.transaction().unwrap();

        tx.insert_class_root(BlockNumber::GENESIS, RootIndexUpdate::Updated(1.into()))
            .unwrap();
        tx.insert_class_root(
            BlockNumber::new_or_panic(1),
            RootIndexUpdate::Updated(2.into()),
        )
        .unwrap();

        assert!(!tx.class_root_exists(BlockNumber::GENESIS).unwrap());
        assert!(tx.class_root_exists(BlockNumber::new_or_panic(1)).unwrap());
    }

    #[test]
    fn contract_state_hash_insert_should_prune_old_state_hashes() {
        let mut db = crate::StorageBuilder::in_memory_with_trie_pruning(TriePruneMode::Prune {
            num_blocks_kept: 1,
        })
        .unwrap()
        .connection()
        .unwrap();
        let tx = db.transaction().unwrap();

        let contract = contract_address!("0xdeadbeef");
        tx.insert_contract_state_hash(BlockNumber::GENESIS, contract, contract_state_hash!("0x01"))
            .unwrap();
        tx.flush_rocksdb_batch().unwrap();
        tx.insert_contract_state_hash(
            BlockNumber::new_or_panic(1),
            contract,
            contract_state_hash!("0x02"),
        )
        .unwrap();
        tx.flush_rocksdb_batch().unwrap();
        // no new state hash for block 2
        tx.insert_contract_state_hash(
            BlockNumber::new_or_panic(3),
            contract,
            contract_state_hash!("0x03"),
        )
        .unwrap();
        tx.flush_rocksdb_batch().unwrap();

        assert_eq!(
            tx.contract_state_hash(BlockNumber::GENESIS, contract)
                .unwrap(),
            None
        );
        assert_eq!(
            tx.contract_state_hash(BlockNumber::new_or_panic(2), contract)
                .unwrap(),
            Some(contract_state_hash!("0x02"))
        );
        assert_eq!(
            tx.contract_state_hash(BlockNumber::new_or_panic(3), contract)
                .unwrap(),
            Some(contract_state_hash!("0x03"))
        );
    }

    #[test]
    fn contract_state_hash_insert_should_prune_all_old_state_in_no_history_mode() {
        let mut db = crate::StorageBuilder::in_memory_with_trie_pruning(TriePruneMode::Prune {
            num_blocks_kept: 0,
        })
        .unwrap()
        .connection()
        .unwrap();
        let tx = db.transaction().unwrap();

        let contract = contract_address!("0xdeadbeef");
        tx.insert_contract_state_hash(BlockNumber::GENESIS, contract, contract_state_hash!("0x01"))
            .unwrap();
        tx.flush_rocksdb_batch().unwrap();
        tx.insert_contract_state_hash(
            BlockNumber::new_or_panic(1),
            contract,
            contract_state_hash!("0x02"),
        )
        .unwrap();
        tx.flush_rocksdb_batch().unwrap();

        assert_eq!(
            tx.contract_state_hash(BlockNumber::GENESIS, contract)
                .unwrap(),
            None
        );
        assert_eq!(
            tx.contract_state_hash(BlockNumber::new_or_panic(1), contract)
                .unwrap(),
            Some(contract_state_hash!("0x02"))
        );
    }

    #[test]
    fn storage_root_insert_should_prune_old_roots() {
        let mut db = crate::StorageBuilder::in_memory_with_trie_pruning(TriePruneMode::Prune {
            num_blocks_kept: 1,
        })
        .unwrap()
        .connection()
        .unwrap();
        let tx = db.transaction().unwrap();

        tx.insert_storage_root(BlockNumber::GENESIS, RootIndexUpdate::Updated(1.into()))
            .unwrap();
        tx.insert_storage_root(
            BlockNumber::new_or_panic(1),
            RootIndexUpdate::Updated(2.into()),
        )
        .unwrap();
        // no new root index for block 2
        tx.insert_storage_root(
            BlockNumber::new_or_panic(3),
            RootIndexUpdate::Updated(3.into()),
        )
        .unwrap();

        assert!(!tx.storage_root_exists(BlockNumber::GENESIS).unwrap());
        assert!(tx
            .storage_root_exists(BlockNumber::new_or_panic(1))
            .unwrap());
        assert!(tx
            .storage_root_exists(BlockNumber::new_or_panic(3))
            .unwrap());
    }

    #[test]
    fn storage_root_insert_should_prune_all_old_roots_in_no_history_mode() {
        let mut db = crate::StorageBuilder::in_memory_with_trie_pruning(TriePruneMode::Prune {
            num_blocks_kept: 0,
        })
        .unwrap()
        .connection()
        .unwrap();
        let tx = db.transaction().unwrap();

        tx.insert_storage_root(BlockNumber::GENESIS, RootIndexUpdate::Updated(1.into()))
            .unwrap();
        tx.insert_storage_root(
            BlockNumber::new_or_panic(1),
            RootIndexUpdate::Updated(2.into()),
        )
        .unwrap();

        assert!(!tx.storage_root_exists(BlockNumber::GENESIS).unwrap());
        assert!(tx
            .storage_root_exists(BlockNumber::new_or_panic(1))
            .unwrap());
    }

    #[test]
    fn contract_root_insert_should_prune_old_state_hashes() {
        let mut db = crate::StorageBuilder::in_memory_with_trie_pruning(TriePruneMode::Prune {
            num_blocks_kept: 1,
        })
        .unwrap()
        .connection()
        .unwrap();
        let tx = db.transaction().unwrap();

        let contract = contract_address!("0xdeadbeef");
        tx.insert_contract_root(
            BlockNumber::GENESIS,
            contract,
            RootIndexUpdate::Updated(1.into()),
        )
        .unwrap();
        tx.insert_contract_root(
            BlockNumber::new_or_panic(1),
            contract,
            RootIndexUpdate::Updated(2.into()),
        )
        .unwrap();
        // no new root for block 2
        tx.insert_contract_root(
            BlockNumber::new_or_panic(3),
            contract,
            RootIndexUpdate::Updated(3.into()),
        )
        .unwrap();

        assert_eq!(
            tx.contract_root_index(BlockNumber::GENESIS, &contract)
                .unwrap(),
            None
        );
        assert_eq!(
            tx.contract_root_index(BlockNumber::new_or_panic(2), &contract)
                .unwrap(),
            Some(2.into())
        );
        assert_eq!(
            tx.contract_root_index(BlockNumber::new_or_panic(3), &contract)
                .unwrap(),
            Some(3.into())
        );
    }

    #[test]
    fn contract_root_insert_should_prune_all_old_roots_in_no_history_mode() {
        let mut db = crate::StorageBuilder::in_memory_with_trie_pruning(TriePruneMode::Prune {
            num_blocks_kept: 0,
        })
        .unwrap()
        .connection()
        .unwrap();
        let tx = db.transaction().unwrap();

        let contract = contract_address!("0xdeadbeef");
        tx.insert_contract_root(
            BlockNumber::GENESIS,
            contract,
            RootIndexUpdate::Updated(1.into()),
        )
        .unwrap();
        tx.insert_contract_root(
            BlockNumber::new_or_panic(1),
            contract,
            RootIndexUpdate::Updated(2.into()),
        )
        .unwrap();

        assert_eq!(
            tx.contract_root_index(BlockNumber::GENESIS, &contract)
                .unwrap(),
            None
        );
        assert_eq!(
            tx.contract_root_index(BlockNumber::new_or_panic(1), &contract)
                .unwrap(),
            Some(2.into())
        );
    }
}
