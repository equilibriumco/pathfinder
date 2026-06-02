use bitvec::view::BitView;
use pathfinder_common::hash::FeltHash;
use pathfinder_crypto::Felt;
use pathfinder_storage::{StoredNode, TrieStorageIndex};

use crate::tree::MerkleTree;

/// A [Patricia Merkle tree](MerkleTree) which can be used to calculate
/// transaction or event commitments.
///
/// The tree has a height of 64 bits and is ephemeral -- it has no persistent
/// storage. This is sensible as each event or transaction tree is confined to a
/// single starknet block i.e. each block a new event / transaction
/// tree is formed from an empty one.
///
/// More information about these commitments can be found in the Starknet [documentation](https://docs.starknet.io/learn/protocol/blocks#block-header-fields).
pub struct TransactionOrEventTree<H: FeltHash> {
    tree: MerkleTree<H, 64>,
}

impl<H: FeltHash> Default for TransactionOrEventTree<H> {
    fn default() -> Self {
        Self {
            tree: MerkleTree::empty(),
        }
    }
}

/// [Storage](crate::storage::Storage) type which always returns [None].
struct NullStorage;

impl crate::storage::Storage for NullStorage {
    fn get(&self, _: TrieStorageIndex) -> anyhow::Result<Option<StoredNode>> {
        Ok(None)
    }

    fn hash(&self, _: TrieStorageIndex) -> anyhow::Result<Option<Felt>> {
        Ok(None)
    }

    fn leaf(
        &self,
        _: &bitvec::slice::BitSlice<u8, bitvec::prelude::Msb0>,
    ) -> anyhow::Result<Option<Felt>> {
        Ok(None)
    }
}

impl<H: FeltHash> TransactionOrEventTree<H> {
    pub fn set(&mut self, index: u64, value: Felt) -> anyhow::Result<()> {
        let key = index.to_be_bytes().view_bits().to_owned();
        self.tree.set(&NullStorage {}, key, value)
    }

    pub fn commit(self) -> anyhow::Result<Felt> {
        self.tree
            .commit(&NullStorage {})
            .map(|update| update.root_commitment)
    }
}

#[cfg(test)]
mod tests {
    use pathfinder_common::felt;
    use pathfinder_common::hash::PedersenHash;

    use super::*;

    #[test]
    fn test_commitment_merkle_tree() {
        let mut tree: TransactionOrEventTree<PedersenHash> = Default::default();

        for (idx, hash) in [1u64, 2, 3, 4].into_iter().enumerate() {
            let hash = Felt::from(hash);
            let idx: u64 = idx.try_into().unwrap();
            tree.set(idx, hash).unwrap();
        }

        // produced by the cairo-lang Python implementation:
        // `hex(asyncio.run(calculate_patricia_root([1, 2, 3, 4], height=64,
        // ffc=ffc))))`
        let expected_root_hash =
            felt!("0x1a0e579b6b444769e4626331230b5ae39bd880f47e703b73fa56bf77e52e461");
        let computed_root_hash = tree.commit().unwrap();

        assert_eq!(expected_root_hash, computed_root_hash);
    }
}
