use anyhow::Context;
use pathfinder_common::prelude::*;
use pathfinder_merkle_tree::{ClassCommitmentTree, StorageCommitmentTree};
use pathfinder_storage::Transaction;

/// Revert Starknet state by applying reverse-updates.
///
/// Computes the contract and Sierra class reverse-updates then applies those to
/// the Merkle tries. Returns an error if the commitments calculated after
/// making the changes do not match the commitments in the target block header.
///
/// Handling of delayed removal of trie data is more complicated: we have to
/// account for removed trie nodes separately.
///
/// In general, removing the trie nodes deleted at block N is only safe if we
/// don't ever need to access trie state at block < N. This is not necessarily
/// the case during a reorg: if our reorg/revert target is still in the range of
/// blocks we're keeping trie history for then removing deleted nodes for the
/// reorged-away blocks would break the trie for the blocks _before_
/// the reorg target. Instead, we move all the removed nodes in the reorged-away
/// range to be "owned" by the revert target block.
///
/// For trie roots: if there was a root change in the interval we're reverting
/// that will be taken care by the revert anyway (we're changing the trie during
/// the reverse state update, so there will be a new root inserted). If there
/// were no changes then there was no root index inserted in the revert range so
/// we remove nothing when purging the blocks after the revert.
pub fn revert_starknet_state(
    transaction: &Transaction<'_>,
    head: BlockNumber,
    target_block: BlockNumber,
    target_header: BlockHeader,
) -> Result<(), anyhow::Error> {
    let storage_commitment = revert_contract_updates(transaction, head, target_block)?;
    let class_commitment = revert_class_updates(transaction, head, target_block)?;

    let state_commitment = StateCommitment::calculate(
        storage_commitment,
        class_commitment,
        target_header.starknet_version,
    );
    if state_commitment != target_header.state_commitment {
        anyhow::bail!(
            "State commitment mismatch: expected {}, calculated {}",
            target_header.state_commitment,
            state_commitment,
        );
    }

    transaction.coalesce_trie_removals(target_block)
}

/// Revert all contract/global storage trie updates.
///
/// Fetches reverse updates from the database and updates all tries, returning
/// the [`StorageCommitment`].
fn revert_contract_updates(
    transaction: &Transaction<'_>,
    head: BlockNumber,
    target_block: BlockNumber,
) -> anyhow::Result<StorageCommitment> {
    let updates = transaction.reverse_contract_updates(head, target_block)?;

    let mut global_tree =
        StorageCommitmentTree::load(transaction, head).context("Loading global storage tree")?;

    for (contract_address, contract_update) in updates {
        let state_hash = pathfinder_merkle_tree::contract_state::revert_contract_state(
            transaction,
            contract_address,
            head,
            target_block,
            contract_update,
        )?;

        transaction
            .insert_contract_state_hash(target_block, contract_address, state_hash)
            .context("Inserting reverted contract state hash")?;

        global_tree
            .set(contract_address, state_hash)
            .context("Updating contract state hash in global tree")?;
    }

    tracing::debug!("Applied reverse updates, committing global state tree");

    let (storage_commitment, trie_update) = global_tree
        .commit()
        .context("Committing global state tree")?;

    let root_idx = transaction
        .insert_storage_trie(&trie_update, target_block)
        .context("Persisting storage trie")?;

    transaction
        .insert_storage_root(target_block, root_idx)
        .context("Inserting storage root index")?;
    tracing::debug!(%target_block, %storage_commitment, "Committed global state tree");

    Ok(storage_commitment)
}

/// Revert all class trie updates.
///
/// Fetches reverse updates from the database and updates all tries, returning
/// the [`ClassCommitment`].
fn revert_class_updates(
    transaction: &Transaction<'_>,
    head: BlockNumber,
    target_block: BlockNumber,
) -> anyhow::Result<ClassCommitment> {
    let updates = transaction.reverse_sierra_class_updates(head, target_block)?;

    let mut class_tree =
        ClassCommitmentTree::load(transaction, head).context("Loading class commitment trie")?;

    for (class_hash, casm_update) in updates {
        let new_value = match casm_update {
            None => {
                // The class must be removed
                ClassCommitmentLeafHash::ZERO
            }
            Some(casm_hash) => {
                // Class hash has changed. Note that the class commitment leaf must have already
                // been added to storage.
                pathfinder_common::calculate_class_commitment_leaf_hash(casm_hash)
            }
        };

        class_tree
            .set(class_hash, new_value)
            .context("Updating class commitment trie")?;
    }

    let (class_commitment, trie_update) = class_tree.commit().context("Committing class trie")?;

    let root_idx = transaction
        .insert_class_trie(&trie_update, target_block)
        .context("Persisting class trie")?;

    transaction
        .insert_class_root(target_block, root_idx)
        .context("Inserting class root index")?;

    tracing::debug!(%target_block, %class_commitment, "Committed class trie");

    Ok(class_commitment)
}
