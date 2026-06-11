use anyhow::Context;
use pathfinder_common::hash::{FeltHash, PedersenHash};
use pathfinder_common::prelude::*;
use pathfinder_common::state_update::{ReverseContractUpdate, StateUpdateError, StorageRef};
use pathfinder_crypto::Felt;
use pathfinder_storage::{Transaction, TrieUpdate};

use crate::ContractsStorageTree;

#[derive(Debug)]
pub struct ContractStateUpdateResult {
    pub state_hash: ContractStateHash,
    pub contract_address: ContractAddress,
    did_storage_updates: bool,
    trie_update: TrieUpdate,
}

impl ContractStateUpdateResult {
    /// Inserts the results of a contract state update into the database.
    ///
    /// The new trie nodes are committed first, then the root node index and the
    /// contract state hash is persisted.
    pub fn insert(self, block: BlockNumber, transaction: &Transaction<'_>) -> anyhow::Result<()> {
        // Insert nodes only if we made storage updates.
        if self.did_storage_updates {
            let root_index = transaction
                .insert_contract_trie(&self.trie_update, block)
                .context("Persisting contract trie")?;

            transaction
                .insert_contract_root(block, self.contract_address, root_index)
                .context("Inserting contract's root index")?;
        }

        transaction
            .insert_contract_state_hash(block, self.contract_address, self.state_hash)
            .context("Inserting contract state hash")
    }
}

/// Updates a contract's state with and returns the resulting
/// [ContractStateHash].
pub fn update_contract_state(
    contract_address: ContractAddress,
    updates: StorageRef<'_>,
    new_nonce: Option<ContractNonce>,
    new_class_hash: Option<ClassHash>,
    transaction: &Transaction<'_>,
    verify_hashes: bool,
    block: BlockNumber,
) -> Result<ContractStateUpdateResult, StateUpdateError> {
    // Load the contract tree and insert the updates.
    let (new_root, trie_update) = if !updates.is_empty() {
        let mut contract_tree = match block.parent() {
            Some(parent) => ContractsStorageTree::load(transaction, contract_address, parent)
                .context("Loading contract storage tree")?
                .with_verify_hashes(verify_hashes),
            None => ContractsStorageTree::empty(transaction, contract_address),
        }
        .with_verify_hashes(verify_hashes);

        for (key, value) in &updates {
            contract_tree
                .set(*key, *value)
                .context("Update contract storage tree")?;
        }
        let (contract_root, trie_update) = contract_tree
            .commit()
            .context("Apply contract storage tree changes")?;

        (contract_root, trie_update)
    } else {
        let current_root = transaction
            .contract_root(block, &contract_address)
            .context("Querying current contract root")?
            .unwrap_or_default();

        (current_root, Default::default())
    };

    let class_hash = if contract_address.is_system_contract() {
        // This is a special system contract at address 0x1 or 0x2, which doesn't have a
        // class hash.
        ClassHash::ZERO
    } else if let Some(class_hash) = new_class_hash {
        class_hash
    } else {
        transaction
            .contract_class_hash(block.into(), contract_address)
            .context("Querying contract's class hash")?
            .ok_or(StateUpdateError::ContractClassHashMissing(contract_address))?
    };

    let nonce = if let Some(nonce) = new_nonce {
        nonce
    } else {
        transaction
            .contract_nonce(contract_address, block.into())
            .context("Querying contract's nonce")?
            //Nonce defaults to ZERO because that is its historical value before being added in
            // 0.10.
            .unwrap_or_default()
    };

    let state_hash = calculate_contract_state_hash(class_hash, new_root, nonce);

    Ok(ContractStateUpdateResult {
        contract_address,
        state_hash,
        did_storage_updates: !updates.is_empty(),
        trie_update,
    })
}

/// Calculates the contract state hash from its preimage.
pub fn calculate_contract_state_hash(
    hash: ClassHash,
    root: ContractRoot,
    nonce: ContractNonce,
) -> ContractStateHash {
    const CONTRACT_STATE_HASH_VERSION: Felt = Felt::ZERO;

    // The contract state hash is defined as H(H(H(hash, root), nonce),
    // CONTRACT_STATE_HASH_VERSION)
    let hash = PedersenHash::hash(hash.0, root.0);
    let hash = PedersenHash::hash(hash, nonce.0);
    let hash = PedersenHash::hash(hash, CONTRACT_STATE_HASH_VERSION);

    // Compare this with the HashChain construction used in the contract_hash: the
    // number of elements is not hashed to this hash, and this is supposed to be
    // different.
    ContractStateHash(hash)
}

/// Reverts Merkle tree state for a contract.
///
/// Takes Merkle tree state at `head` and applies reverse updates.
pub fn revert_contract_state(
    transaction: &Transaction<'_>,
    contract_address: ContractAddress,
    head: BlockNumber,
    target_block: BlockNumber,
    contract_update: ReverseContractUpdate,
) -> anyhow::Result<ContractStateHash> {
    tracing::debug!(%contract_address, "Rolling back");

    match contract_update {
        ReverseContractUpdate::Deleted => {
            tracing::debug!(%contract_address, "Contract has been deleted");
            Ok(ContractStateHash::ZERO)
        }
        ReverseContractUpdate::Updated(update) => {
            let class_hash = match update.class {
                Some(class_hash) => class_hash.class_hash(),
                None => {
                    if contract_address.is_system_contract() {
                        // system contracts have no class hash
                        ClassHash::ZERO
                    } else {
                        transaction
                            .contract_class_hash(target_block.into(), contract_address)?
                            .unwrap()
                    }
                }
            };

            let nonce = match update.nonce {
                Some(nonce) => nonce,
                None => transaction
                    .contract_nonce(contract_address, target_block.into())
                    .context("Getting contract nonce")?
                    .unwrap_or_default(),
            };

            // Apply storage updates
            let root = if !update.storage.is_empty() {
                let mut tree = ContractsStorageTree::load(transaction, contract_address, head)
                    .context("Loading contract state")?;

                for (address, value) in update.storage {
                    tree.set(address, value)
                        .context("Updating contract state")?;
                }

                let (root, trie_update) = tree.commit().context("Committing contract state")?;

                let root_index = transaction
                    .insert_contract_trie(&trie_update, target_block)
                    .context("Persisting contract trie")?;

                transaction
                    .insert_contract_root(target_block, contract_address, root_index)
                    .context("Inserting contract's root index")?;

                root
            } else {
                transaction
                    .contract_root(head, &contract_address)?
                    .unwrap_or(ContractRoot::ZERO)
            };

            let state_hash = if contract_address.is_system_contract() && root == ContractRoot::ZERO
            {
                // special case: if the contract trie is empty the system contract should be
                // deleted
                ContractStateHash::ZERO
            } else {
                calculate_contract_state_hash(class_hash, root, nonce)
            };

            tracing::debug!(%state_hash, %contract_address, "Contract state rolled back");

            Ok(state_hash)
        }
    }
}

#[cfg(test)]
mod tests {
    use pathfinder_common::{felt, ClassHash, ContractNonce, ContractRoot, ContractStateHash};

    use super::calculate_contract_state_hash;

    #[test]
    fn hash() {
        let root = felt!("0x4fb440e8ca9b74fc12a22ebffe0bc0658206337897226117b985434c239c028");
        let root = ContractRoot(root);

        let hash = felt!("0x2ff4903e17f87b298ded00c44bfeb22874c5f73be2ced8f1d9d9556fb509779");
        let hash = ClassHash(hash);

        let nonce = ContractNonce::ZERO;

        let expected = felt!("0x7161b591c893836263a64f2a7e0d829c92f6956148a60ce5e99a3f55c7973f3");
        let expected = ContractStateHash(expected);

        let result = calculate_contract_state_hash(hash, root, nonce);

        assert_eq!(result, expected);
    }
}
