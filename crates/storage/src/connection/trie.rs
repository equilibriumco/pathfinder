use std::collections::HashMap;

use anyhow::Context;
use bitvec::prelude::Msb0;
use bitvec::vec::BitVec;
use pathfinder_common::prelude::*;
use pathfinder_crypto::Felt;

use crate::prelude::*;
use crate::TriePruneMode;

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
                |row| row.get_optional_trie_storage_index(0),
            )
            .optional()
            .map(|x| x.flatten())
            .map_err(Into::into)
    }

    pub fn class_root(&self, block_number: BlockNumber) -> anyhow::Result<Option<ClassCommitment>> {
        self.inner()
        .query_row(
            r"SELECT hash FROM trie_class WHERE idx = (
                SELECT root_index FROM class_roots WHERE block_number <= ? ORDER BY block_number DESC LIMIT 1
            )",
            params![&block_number],
            |row| row.get_optional_class_commitment(0),
        )
        .optional()
        .map(|x| x.flatten())
        .map_err(Into::into)
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
                |row| row.get_optional_trie_storage_index(0),
            )
            .optional()
            .map(|x| x.flatten())
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
        contract: ContractAddress,
    ) -> anyhow::Result<Option<TrieStorageIndex>> {
        self.inner()
            .query_row(
                "SELECT root_index FROM contract_roots WHERE contract_address = ? AND \
                 block_number <= ? ORDER BY block_number DESC LIMIT 1",
                params![&contract, &block_number],
                |row| row.get_optional_trie_storage_index(0),
            )
            .optional()
            .map(|x| x.flatten())
            .map_err(Into::into)
    }

    pub fn contract_root(
        &self,
        block_number: BlockNumber,
        contract: ContractAddress,
    ) -> anyhow::Result<Option<ContractRoot>> {
        self.inner()
        .query_row(
            r"SELECT hash FROM trie_contracts WHERE idx = (
                SELECT root_index FROM contract_roots WHERE block_number <= ? AND contract_address = ? ORDER BY block_number DESC LIMIT 1
            )",
            params![&block_number, &contract],
            |row| row.get_contract_root(0),
        )
        .optional()
        .map_err(Into::into)
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
        self.inner().execute(
            "INSERT OR REPLACE INTO contract_state_hashes(block_number, contract_address, \
             state_hash) VALUES(?,?,?)",
            params![&block_number, &contract, &state_hash],
        )?;

        if let TriePruneMode::Prune { num_blocks_kept } = self.trie_prune_mode {
            if let Some(block_number) = block_number.checked_sub(num_blocks_kept) {
                self.delete_contract_state_hashes(contract, block_number)?;
            }
        }

        Ok(())
    }

    fn delete_contract_state_hashes(
        &self,
        contract: ContractAddress,
        before_block: BlockNumber,
    ) -> anyhow::Result<()> {
        let mut stmt = self.inner().prepare_cached(
            "SELECT block_number
            FROM contract_state_hashes
            WHERE contract_address = ? AND block_number <= ?
            ORDER BY block_number DESC
            LIMIT 1",
        )?;
        let last_block_with_contract_state_hash = stmt
            .query_row(params![&contract, &before_block], |row| {
                row.get_block_number(0)
            })
            .optional()?;

        if let Some(last_block_with_contract_state_hash) = last_block_with_contract_state_hash {
            let mut stmt = self.inner().prepare_cached(
                "DELETE FROM contract_state_hashes WHERE contract_address = ? AND block_number < ?",
            )?;
            stmt.execute(params![&contract, &last_block_with_contract_state_hash])?;
        }
        Ok(())
    }

    pub fn contract_state_hash(
        &self,
        block_number: BlockNumber,
        contract: ContractAddress,
    ) -> anyhow::Result<Option<ContractStateHash>> {
        self.inner()
            .query_row(
                "SELECT state_hash FROM contract_state_hashes WHERE contract_address = ? AND \
                 block_number <= ? ORDER BY block_number DESC LIMIT 1",
                params![&contract, &block_number],
                |row| row.get_contract_state_hash(0),
            )
            .optional()
            .map_err(Into::into)
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
    ) -> anyhow::Result<RootIndexUpdate> {
        self.insert_trie(update, block_number, "trie_contracts")
    }

    pub fn contract_trie_node(
        &self,
        index: TrieStorageIndex,
    ) -> anyhow::Result<Option<StoredNode>> {
        self.trie_node(index, "trie_contracts")
    }

    pub fn contract_trie_node_hash(&self, index: TrieStorageIndex) -> anyhow::Result<Option<Felt>> {
        self.trie_node_hash(index, "trie_contracts")
    }

    pub fn insert_class_trie(
        &self,
        update: &TrieUpdate,
        block_number: BlockNumber,
    ) -> anyhow::Result<RootIndexUpdate> {
        self.insert_trie(update, block_number, "trie_class")
    }

    pub fn class_trie_node(&self, index: TrieStorageIndex) -> anyhow::Result<Option<StoredNode>> {
        self.trie_node(index, "trie_class")
    }

    pub fn class_trie_node_hash(&self, index: TrieStorageIndex) -> anyhow::Result<Option<Felt>> {
        self.trie_node_hash(index, "trie_class")
    }

    pub fn insert_storage_trie(
        &self,
        update: &TrieUpdate,
        block_number: BlockNumber,
    ) -> anyhow::Result<RootIndexUpdate> {
        self.insert_trie(update, block_number, "trie_storage")
    }

    pub fn storage_trie_node(&self, index: TrieStorageIndex) -> anyhow::Result<Option<StoredNode>> {
        self.trie_node(index, "trie_storage")
    }

    pub fn storage_trie_node_hash(&self, index: TrieStorageIndex) -> anyhow::Result<Option<Felt>> {
        self.trie_node_hash(index, "trie_storage")
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
        self.prune_trie(block_number, num_blocks_kept, "trie_contracts")?;
        self.prune_trie(block_number, num_blocks_kept, "trie_class")?;
        self.prune_trie(block_number, num_blocks_kept, "trie_storage")?;
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
        table: &'static str,
    ) -> anyhow::Result<()> {
        if !removed.is_empty() {
            let mut stmt = self
                .inner()
                .prepare_cached(&format!(
                    r"INSERT INTO {table}_removals (block_number, indices) VALUES (?, ?)"
                ))
                .context("Creating statement to insert removal marker")?;
            stmt.execute(params![
                &block_number,
                &bincode::encode_to_vec(removed, bincode::config::standard())
                    .context("Serializing indices")?
            ])
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
        num_blocks_kept: BlockNumber,
        table: &'static str,
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
            let mut delete_stmt = self
                .inner()
                .prepare_cached(&format!(r"DELETE FROM {table} WHERE idx = ?"))
                .context("Creating delete statement")?;
            while let Some(row) = rows.next().context("Iterating over rows")? {
                let (indices, _) = bincode::decode_from_slice::<Vec<u64>, _>(
                    row.get_blob(0)?,
                    bincode::config::standard(),
                )
                .context("Decoding indices")?;
                for idx in indices.iter() {
                    delete_stmt
                        .execute(params![&idx
                            .try_into_sql_int()
                            .context("Trie node index exceeds i64::MAX")?])
                        .context("Deleting node")?;
                }
                metrics::counter!(METRIC_TRIE_NODES_REMOVED, "table" => table)
                    .increment(indices.len() as u64);
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
    ) -> anyhow::Result<RootIndexUpdate> {
        if let TriePruneMode::Prune { num_blocks_kept } = self.trie_prune_mode {
            self.prune_trie(block_number, num_blocks_kept, table)?;
            self.remove_trie(&update.nodes_removed, block_number, table)?;
        }

        if update.nodes_added.is_empty() {
            if !update.nodes_removed.is_empty() && update.root_commitment.is_zero() {
                return Ok(RootIndexUpdate::TrieEmpty);
            } else {
                return Ok(RootIndexUpdate::Unchanged);
            }
        }

        let mut stmt = self
            .inner()
            .prepare_cached(&format!(
                "INSERT INTO {table} (hash, data) VALUES(?, ?) RETURNING idx",
            ))
            .context("Creating insert statement")?;

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

        let mut indices = HashMap::new();

        // Reusable (and oversized) buffer for encoding.
        let mut buffer = [0u8; 256];

        // Insert nodes in reverse to ensure children always have an assigned index for
        // the parent to use.
        for idx in to_insert.into_iter().rev() {
            let (hash, node) = &update.nodes_added.get(idx).context("Node index missing")?;

            let node = node.as_stored(&indices)?;

            let length = node.encode(&mut buffer).context("Encoding node")?;

            let storage_idx = stmt
                .query_row(
                    params![&hash.as_be_bytes().as_slice(), &&buffer[..length]],
                    |row| row.get_trie_storage_index(0),
                )
                .context("Inserting node")?;

            indices.insert(idx, storage_idx);

            metrics::counter!(METRIC_TRIE_NODES_ADDED, "table" => table).increment(1);
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
        table: &'static str,
    ) -> anyhow::Result<Option<StoredNode>> {
        // We rely on sqlite caching the statement here. Storing the statement would be
        // nice, however that leads to &mut requirements or interior mutable
        // work-arounds.
        let mut stmt = self
            .inner()
            .prepare_cached(&format!("SELECT data FROM {table} WHERE idx = ?"))
            .context("Creating get statement")?;

        let Some(data): Option<Vec<u8>> = stmt
            .query_row(params![&index], |row| row.get(0))
            .optional()?
        else {
            return Ok(None);
        };

        let node = StoredNode::decode(&data).context("Decoding node")?;

        Ok(Some(node))
    }

    /// Returns the hash of the node with the given index.
    fn trie_node_hash(
        &self,
        index: TrieStorageIndex,
        table: &'static str,
    ) -> anyhow::Result<Option<Felt>> {
        // We rely on sqlite caching the statement here. Storing the statement would be
        // nice, however that leads to &mut requirements or interior mutable
        // work-arounds.
        let mut stmt = self
            .inner()
            .prepare_cached(&format!("SELECT hash FROM {table} WHERE idx = ?"))
            .context("Creating get statement")?;

        stmt.query_row(params![&index], |row| row.get_felt(0))
            .optional()
            .map_err(Into::into)
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
    fn as_stored(
        &self,
        storage_indices: &HashMap<usize, TrieStorageIndex>,
    ) -> anyhow::Result<StoredNode> {
        let node = match self {
            Node::Binary { left, right } => {
                let left = match left {
                    NodeRef::StorageIndex(id) => *id,
                    NodeRef::Index(idx) => *storage_indices
                        .get(idx)
                        .context("Left child index missing")?,
                };

                let right = match right {
                    NodeRef::StorageIndex(id) => *id,
                    NodeRef::Index(idx) => *storage_indices
                        .get(idx)
                        .context("Right child index missing")?,
                };

                StoredNode::Binary { left, right }
            }
            Node::Edge { child, path } => {
                let child = match child {
                    NodeRef::StorageIndex(id) => id,
                    NodeRef::Index(idx) => {
                        storage_indices.get(idx).context("Child index missing")?
                    }
                };

                StoredNode::Edge {
                    child: *child,
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
            .insert_contract_trie(&update, BlockNumber::GENESIS)
            .unwrap();
        let RootIndexUpdate::Updated(idx0) = idx0_update else {
            panic!("Expected the root index to be updated");
        };

        let result1 = tx.contract_root_index(BlockNumber::GENESIS, c1).unwrap();
        assert_eq!(result1, None);

        tx.insert_contract_root(BlockNumber::GENESIS, c1, idx0_update)
            .unwrap();
        let result1 = tx.contract_root_index(BlockNumber::GENESIS, c1).unwrap();
        let result2 = tx.contract_root_index(BlockNumber::GENESIS, c2).unwrap();
        let hash1 = tx.contract_root(BlockNumber::GENESIS, c1).unwrap();
        let hash2 = tx.contract_root(BlockNumber::GENESIS, c2).unwrap();
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

        let idx1_update = tx
            .insert_contract_trie(&update, BlockNumber::GENESIS + 1)
            .unwrap();
        let RootIndexUpdate::Updated(idx1) = idx1_update else {
            panic!("Expected the root index to be updated");
        };

        tx.insert_contract_root(BlockNumber::GENESIS + 1, c1, idx1_update)
            .unwrap();
        tx.insert_contract_root(
            BlockNumber::GENESIS + 1,
            c2,
            RootIndexUpdate::Updated(888.into()),
        )
        .unwrap();
        let result1 = tx.contract_root_index(BlockNumber::GENESIS, c1).unwrap();
        let result2 = tx.contract_root_index(BlockNumber::GENESIS, c2).unwrap();
        let hash1 = tx.contract_root(BlockNumber::GENESIS, c1).unwrap();
        assert_eq!(result1, Some(idx0));
        assert_eq!(result2, None);
        assert_eq!(hash1, Some(root0));
        let result1 = tx
            .contract_root_index(BlockNumber::GENESIS + 1, c1)
            .unwrap();
        let result2 = tx
            .contract_root_index(BlockNumber::GENESIS + 1, c2)
            .unwrap();
        let hash1 = tx.contract_root(BlockNumber::GENESIS + 1, c1).unwrap();
        assert_eq!(result1, Some(idx1));
        assert_eq!(result2, Some(888.into()));
        assert_eq!(hash1, Some(root1));
        let result1 = tx
            .contract_root_index(BlockNumber::GENESIS + 2, c1)
            .unwrap();
        let result2 = tx
            .contract_root_index(BlockNumber::GENESIS + 2, c2)
            .unwrap();
        let hash1 = tx.contract_root(BlockNumber::GENESIS + 2, c1).unwrap();
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
            .insert_contract_trie(&update, BlockNumber::GENESIS + 10)
            .unwrap();
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
            .contract_root_index(BlockNumber::GENESIS + 9, c1)
            .unwrap();
        let result2 = tx
            .contract_root_index(BlockNumber::GENESIS + 9, c2)
            .unwrap();
        let hash1 = tx.contract_root(BlockNumber::GENESIS + 9, c1).unwrap();
        assert_eq!(result1, Some(idx1));
        assert_eq!(result2, Some(888.into()));
        assert_eq!(hash1, Some(root1));
        let result1 = tx
            .contract_root_index(BlockNumber::GENESIS + 10, c1)
            .unwrap();
        let result2 = tx
            .contract_root_index(BlockNumber::GENESIS + 10, c2)
            .unwrap();
        let hash1 = tx.contract_root(BlockNumber::GENESIS + 10, c1).unwrap();
        assert_eq!(result1, Some(idx2));
        assert_eq!(result2, Some(888.into()));
        assert_eq!(hash1, Some(root2));
        let result2 = tx
            .contract_root_index(BlockNumber::GENESIS + 11, c2)
            .unwrap();
        let hash1 = tx.contract_root(BlockNumber::GENESIS + 11, c1).unwrap();
        assert_eq!(result2, Some(999.into()));
        assert_eq!(hash1, Some(root2));

        tx.insert_contract_root(BlockNumber::GENESIS + 12, c1, RootIndexUpdate::TrieEmpty)
            .unwrap();
        let result1 = tx
            .contract_root_index(BlockNumber::GENESIS + 10, c1)
            .unwrap();
        let hash1 = tx.contract_root(BlockNumber::GENESIS + 10, c1).unwrap();
        assert_eq!(result1, Some(idx2));
        assert_eq!(hash1, Some(root2));
        let result1 = tx
            .contract_root_index(BlockNumber::GENESIS + 12, c1)
            .unwrap();
        let hash1 = tx.contract_root(BlockNumber::GENESIS + 12, c1).unwrap();
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
            num_blocks_kept: BlockNumber::new_or_panic(2),
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

        // At this point, index 1 should no longer be in the table.
        assert!(tx.class_trie_node(TrieStorageIndex(1)).unwrap().is_none());
    }

    #[test]
    fn class_trie_pruning_change_config() {
        let mut db = crate::StorageBuilder::in_memory_with_trie_pruning(TriePruneMode::Prune {
            num_blocks_kept: BlockNumber::new_or_panic(100),
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

        // Nothing was pruned.
        assert!(tx.class_trie_node(TrieStorageIndex(1)).unwrap().is_some());

        // Simulate a configuration change.
        tx.trie_prune_mode = TriePruneMode::Prune {
            num_blocks_kept: BlockNumber::new_or_panic(2),
        };
        tx.insert_block_header(&BlockHeader {
            number: BlockNumber::GENESIS + 4,
            ..Default::default()
        })
        .unwrap();
        tx.prune_tries().unwrap();

        // The class trie was pruned.
        assert!(tx.class_trie_node(TrieStorageIndex(1)).unwrap().is_none());
    }

    #[test]
    fn class_trie_pruning_keep_zero_blocks() {
        let mut db = crate::StorageBuilder::in_memory_with_trie_pruning(TriePruneMode::Prune {
            num_blocks_kept: BlockNumber::new_or_panic(0),
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

        // At this point, indices 1, 2, 3 should be in the table.
        assert!(tx.class_trie_node(TrieStorageIndex(1)).unwrap().is_some());
        assert!(tx.class_trie_node(TrieStorageIndex(2)).unwrap().is_some());
        assert!(tx.class_trie_node(TrieStorageIndex(3)).unwrap().is_some());

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
                nodes_removed: vec![1, 2, 3].into_iter().map(TrieStorageIndex).collect(),
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

        // At this point, 1, 2, 3 should no longer be in the table.
        assert!(tx.class_trie_node(TrieStorageIndex(1)).unwrap().is_none());
        assert!(tx.class_trie_node(TrieStorageIndex(2)).unwrap().is_none());
        assert!(tx.class_trie_node(TrieStorageIndex(3)).unwrap().is_none());
    }

    #[test]
    fn class_trie_root_updates() {
        let mut db = crate::StorageBuilder::in_memory_with_trie_pruning(TriePruneMode::Prune {
            num_blocks_kept: BlockNumber::new_or_panic(0),
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
        assert_eq!(root_update, RootIndexUpdate::Updated(1.into()));
    }

    #[test]
    fn class_root_insert_should_prune_old_roots() {
        let mut db = crate::StorageBuilder::in_memory_with_trie_pruning(TriePruneMode::Prune {
            num_blocks_kept: BlockNumber::new_or_panic(1),
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
            num_blocks_kept: BlockNumber::new_or_panic(0),
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
            num_blocks_kept: BlockNumber::new_or_panic(1),
        })
        .unwrap()
        .connection()
        .unwrap();
        let tx = db.transaction().unwrap();

        let contract = contract_address!("0xdeadbeef");
        tx.insert_contract_state_hash(BlockNumber::GENESIS, contract, contract_state_hash!("0x01"))
            .unwrap();
        tx.insert_contract_state_hash(
            BlockNumber::new_or_panic(1),
            contract,
            contract_state_hash!("0x02"),
        )
        .unwrap();
        // no new state hash for block 2
        tx.insert_contract_state_hash(
            BlockNumber::new_or_panic(3),
            contract,
            contract_state_hash!("0x03"),
        )
        .unwrap();

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
            num_blocks_kept: BlockNumber::new_or_panic(0),
        })
        .unwrap()
        .connection()
        .unwrap();
        let tx = db.transaction().unwrap();

        let contract = contract_address!("0xdeadbeef");
        tx.insert_contract_state_hash(BlockNumber::GENESIS, contract, contract_state_hash!("0x01"))
            .unwrap();
        tx.insert_contract_state_hash(
            BlockNumber::new_or_panic(1),
            contract,
            contract_state_hash!("0x02"),
        )
        .unwrap();

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
            num_blocks_kept: BlockNumber::new_or_panic(1),
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
            num_blocks_kept: BlockNumber::new_or_panic(0),
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
            num_blocks_kept: BlockNumber::new_or_panic(1),
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
            tx.contract_root_index(BlockNumber::GENESIS, contract)
                .unwrap(),
            None
        );
        assert_eq!(
            tx.contract_root_index(BlockNumber::new_or_panic(2), contract)
                .unwrap(),
            Some(2.into())
        );
        assert_eq!(
            tx.contract_root_index(BlockNumber::new_or_panic(3), contract)
                .unwrap(),
            Some(3.into())
        );
    }

    #[test]
    fn contract_root_insert_should_prune_all_old_roots_in_no_history_mode() {
        let mut db = crate::StorageBuilder::in_memory_with_trie_pruning(TriePruneMode::Prune {
            num_blocks_kept: BlockNumber::new_or_panic(0),
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
            tx.contract_root_index(BlockNumber::GENESIS, contract)
                .unwrap(),
            None
        );
        assert_eq!(
            tx.contract_root_index(BlockNumber::new_or_panic(1), contract)
                .unwrap(),
            Some(2.into())
        );
    }
}
