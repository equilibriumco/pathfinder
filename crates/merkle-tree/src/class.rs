use anyhow::Context;
use pathfinder_common::hash::PoseidonHash;
use pathfinder_common::prelude::*;
use pathfinder_crypto::Felt;
use pathfinder_storage::{Transaction, TrieStorageIndex, TrieUpdate};

use crate::tree::{GetProofError, MerkleTree, TrieNodeWithHash};

/// A [Patricia Merkle tree](MerkleTree) used to calculate commitments to
/// Starknet's Sierra classes.
///
/// It maps a class's [SierraHash] to its [ClassCommitmentLeafHash]
///
/// Tree data is persisted by a sqlite table 'trie_class'.
pub struct ClassCommitmentTree<'tx> {
    tree: MerkleTree<PoseidonHash, 251>,
    storage: ClassStorage<'tx>,
}

impl<'tx> ClassCommitmentTree<'tx> {
    pub fn empty(tx: &'tx Transaction<'tx>) -> Self {
        let storage = ClassStorage { tx, block: None };
        let tree = MerkleTree::empty();

        Self { tree, storage }
    }

    pub fn load(tx: &'tx Transaction<'tx>, block: BlockNumber) -> anyhow::Result<Self> {
        let root = tx
            .class_root_index(block)
            .context("Querying class root index")?;
        let Some(root) = root else {
            return Ok(Self::empty(tx));
        };

        let storage = ClassStorage {
            tx,
            block: Some(block),
        };
        let tree = MerkleTree::new(root);

        Ok(Self { tree, storage })
    }

    pub fn with_verify_hashes(mut self, verify_hashes: bool) -> Self {
        self.tree = self.tree.with_verify_hashes(verify_hashes);
        self
    }

    /// Adds a leaf node for a Sierra -> CASM commitment.
    ///
    /// Note that the leaf value is _not_ the Cairo hash, but a hashed value
    /// based on that. See <https://github.com/starkware-libs/cairo-lang/blob/12ca9e91bbdc8a423c63280949c7e34382792067/src/starkware/starknet/core/os/state.cairo#L302>
    /// for details.
    pub fn set(&mut self, class: SierraHash, value: ClassCommitmentLeafHash) -> anyhow::Result<()> {
        let key = class.view_bits().to_owned();
        self.tree.set(&self.storage, key, value.0)
    }

    /// Commits the changes and calculates the new node hashes. Returns the new
    /// commitment and any potentially newly created nodes.
    pub fn commit(self) -> anyhow::Result<(ClassCommitment, TrieUpdate)> {
        let update = self.tree.commit(&self.storage)?;

        let commitment = ClassCommitment(update.root_commitment);
        Ok((commitment, update))
    }

    /// Generates a proof for a given `class_hash`.
    /// See [`MerkleTree::get_proof`].
    pub fn get_proof(
        tx: &'tx Transaction<'tx>,
        block: BlockNumber,
        class_hash: ClassHash,
        root: TrieStorageIndex,
    ) -> Result<Vec<TrieNodeWithHash>, GetProofError> {
        let storage = ClassStorage {
            tx,
            block: Some(block),
        };

        MerkleTree::<PoseidonHash, 251>::get_proof(root, &storage, class_hash.0.view_bits())
    }

    /// Generates proofs for the given list of `class_hashes`. See
    /// [`MerkleTree::get_proofs`]. Returns  [(`TrieNode`,
    /// `Felt`)](TrieNodeWithHash) pairs where the second element is the
    /// node hash.
    pub fn get_proofs(
        tx: &'tx Transaction<'tx>,
        block: BlockNumber,
        class_hashes: &[ClassHash],
        root: TrieStorageIndex,
    ) -> Result<Vec<Vec<TrieNodeWithHash>>, GetProofError> {
        let storage = ClassStorage {
            tx,
            block: Some(block),
        };

        let keys = class_hashes
            .iter()
            .map(|hash| hash.0.view_bits())
            .collect::<Vec<_>>();

        MerkleTree::<PoseidonHash, 251>::get_proofs(root, &storage, &keys)
    }
}

struct ClassStorage<'tx> {
    tx: &'tx Transaction<'tx>,
    block: Option<BlockNumber>,
}

impl crate::storage::Storage for ClassStorage<'_> {
    fn get(
        &self,
        index: TrieStorageIndex,
    ) -> anyhow::Result<Option<pathfinder_storage::StoredNode>> {
        self.tx.class_trie_node(index)
    }

    fn hash(&self, index: TrieStorageIndex) -> anyhow::Result<Option<Felt>> {
        self.tx.class_trie_node_hash(index)
    }

    fn leaf(
        &self,
        path: &bitvec::slice::BitSlice<u8, bitvec::prelude::Msb0>,
    ) -> anyhow::Result<Option<Felt>> {
        assert!(path.len() == 251);

        let Some(block) = self.block else {
            return Ok(None);
        };

        let sierra = ClassHash(Felt::from_bits(path).context("Mapping path to sierra hash")?);

        let casm = self
            .tx
            .casm_hash_at(block.into(), sierra)
            .context("Querying CASM hash")?;
        let Some(casm) = casm else {
            return Ok(None);
        };

        let leaf = self
            .tx
            .class_commitment_leaf(block, &casm)
            .context("Querying class leaf")?
            .map(|x| x.0);

        Ok(leaf)
    }
}
