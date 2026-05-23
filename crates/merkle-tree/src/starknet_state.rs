use anyhow::Context;
use pathfinder_common::state_update::{StateUpdateError, StateUpdateRef};
use pathfinder_common::{BlockNumber, ClassCommitment, StorageCommitment};
use pathfinder_storage::{Storage, Transaction};

use crate::contract_state::update_contract_state;
use crate::{ClassCommitmentTree, StorageCommitmentTree};

pub fn update_starknet_state(
    transaction: &Transaction<'_>,
    state_update: StateUpdateRef<'_>,
    verify_hashes: bool,
    block: BlockNumber,
    // we need this so that we can create extra read-only transactions for
    // parallel contract state updates
    storage: Storage,
) -> Result<(StorageCommitment, ClassCommitment), StateUpdateError> {
    use rayon::prelude::*;

    let mut storage_commitment_tree = match block.parent() {
        Some(parent) => StorageCommitmentTree::load(transaction, parent)
            .context("Loading storage commitment tree")?,
        None => StorageCommitmentTree::empty(transaction),
    }
    .with_verify_hashes(verify_hashes);

    let (send, recv) = std::sync::mpsc::channel();

    rayon::scope(|s| {
        s.spawn(|_| {
            let result: Result<Vec<_>, _> = state_update
                .contract_updates
                .par_iter()
                .map_init(
                    || storage.clone().connection(),
                    |connection, (contract_address, update)| {
                        let connection = match connection {
                            Ok(connection) => connection,
                            Err(e) => {
                                return Err(anyhow::anyhow!(
                                    "Failed to create database connection in rayon thread: {e}"
                                )
                                .into())
                            }
                        };
                        let transaction = connection
                            .transaction()
                            .map_err(|e| StateUpdateError::StorageError(e.into()))?;
                        update_contract_state(
                            **contract_address,
                            update.storage,
                            *update.nonce,
                            update.class.as_ref().map(|x| x.class_hash()),
                            &transaction,
                            verify_hashes,
                            block,
                        )
                    },
                )
                .collect();
            let _ = send.send(result);
        })
    });

    let contract_update_results = recv.recv().context("Panic on rayon thread")??;

    for contract_update_result in contract_update_results.into_iter() {
        storage_commitment_tree
            .set(
                contract_update_result.contract_address,
                contract_update_result.state_hash,
            )
            .context("Updating storage commitment tree")?;
        contract_update_result
            .insert(block, transaction)
            .context("Inserting contract update result")?;
    }

    for (contract, update) in state_update.system_contract_updates {
        let update_result = update_contract_state(
            *contract,
            update.storage,
            None,
            None,
            transaction,
            verify_hashes,
            block,
        )
        .context("Update system contract state")?;

        storage_commitment_tree
            .set(*contract, update_result.state_hash)
            .context("Updating system contract storage commitment tree")?;

        update_result
            .insert(block, transaction)
            .context("Persisting system contract trie updates")?;
    }

    // Apply storage commitment tree changes.
    let (storage_commitment, trie_update) = storage_commitment_tree
        .commit()
        .context("Apply storage commitment tree updates")?;

    let root_idx = transaction
        .insert_storage_trie(&trie_update, block)
        .context("Persisting storage trie")?;

    transaction
        .insert_storage_root(block, root_idx)
        .context("Inserting storage root index")?;

    // Add new Sierra classes to class commitment tree.
    let mut class_commitment_tree = match block.parent() {
        Some(parent) => ClassCommitmentTree::load(transaction, parent)
            .context("Loading class commitment tree")?,
        None => ClassCommitmentTree::empty(transaction),
    }
    .with_verify_hashes(verify_hashes);

    for (sierra, casm) in state_update
        .declared_sierra_classes
        .iter()
        .chain(state_update.migrated_compiled_classes.iter())
    {
        let leaf_hash = pathfinder_common::calculate_class_commitment_leaf_hash(*casm);

        transaction
            .insert_class_commitment_leaf(block, &leaf_hash, casm)
            .context("Adding class commitment leaf")?;

        class_commitment_tree
            .set(*sierra, leaf_hash)
            .context("Update class commitment tree")?;
    }

    // Apply all class commitment tree changes.
    let (class_commitment, trie_update) = class_commitment_tree
        .commit()
        .context("Apply class commitment tree updates")?;

    let class_root_idx = transaction
        .insert_class_trie(&trie_update, block)
        .context("Persisting class trie")?;

    transaction
        .insert_class_root(block, class_root_idx)
        .context("Inserting class root index")?;

    Ok((storage_commitment, class_commitment))
}

#[cfg(test)]
mod tests {
    use pathfinder_common::prelude::*;
    use pathfinder_common::{
        class_commitment,
        state_commitment,
        storage_address,
        storage_commitment,
        storage_value,
        ClassCommitment,
        StarknetVersion,
        StateCommitment,
    };

    use crate::contract_state::calculate_contract_state_hash;
    use crate::{ContractsStorageTree, StorageCommitmentTree};

    /// Regression test for state commitment calculation in Starknet v0.14+.
    ///
    /// In Starknet v0.14.0+, the state commitment formula changed: it now
    /// always uses the Poseidon hash with STARKNET_STATE_V0 prefix, even
    /// when the class_commitment is zero.
    ///
    /// Before v0.14:
    ///   If class_commitment == 0: state = storage_commitment.
    ///   Else: state = poseidon([STARKNET_STATE_V0, storage, class]).
    ///
    /// v0.14+:
    ///   State = poseidon([STARKNET_STATE_V0, storage, class]) (always).
    ///
    /// Test data from feeder gateway get_state_update for blockNumber=0.
    /// Expected state_root:
    /// 0x68bcf9e9257ab6bffd9425833a208aaab6b85649fd21c787a546cb7cb9abf.
    #[test]
    fn state_commitment_v0_14_with_zero_class_commitment() {
        let storage = pathfinder_storage::StorageBuilder::in_memory().unwrap();
        let mut db = storage.connection().unwrap();
        let tx = db.transaction().unwrap();

        // Contract 0x2 is a system contract.
        let contract_address = ContractAddress::TWO;

        // Create contract storage tree with single entry: key 0x0 -> value 0x80.
        let mut contract_tree = ContractsStorageTree::empty(&tx, contract_address);
        contract_tree
            .set(storage_address!("0x0"), storage_value!("0x80"))
            .unwrap();
        let (contract_root, _) = contract_tree.commit().unwrap();

        // For system contracts: class_hash = 0, nonce = 0.
        let contract_state_hash =
            calculate_contract_state_hash(ClassHash::ZERO, contract_root, ContractNonce::ZERO);

        // Create storage commitment tree with the contract.
        let mut storage_commitment_tree = StorageCommitmentTree::empty(&tx);
        storage_commitment_tree
            .set(contract_address, contract_state_hash)
            .unwrap();
        let (storage_commitment, _) = storage_commitment_tree.commit().unwrap();

        // Class commitment is ZERO (no declared classes).
        let class_commitment = ClassCommitment::ZERO;

        // Expected state commitment from feeder gateway.
        let expected =
            state_commitment!("0x68bcf9e9257ab6bffd9425833a208aaab6b85649fd21c787a546cb7cb9abf");

        // Test v0.14 calculation (should match).
        let state_commitment_v014 = StateCommitment::calculate(
            storage_commitment,
            class_commitment,
            StarknetVersion::V_0_14_0,
        );
        assert_eq!(
            state_commitment_v014, expected,
            "v0.14 state commitment should match expected."
        );

        // Test pre-0.14 calculation (should NOT match for this case).
        let state_commitment_v013 = StateCommitment::calculate(
            storage_commitment,
            class_commitment,
            StarknetVersion::V_0_13_4,
        );
        assert_ne!(
            state_commitment_v013, expected,
            "Pre-0.14 calculation should NOT match for v0.14+ expected value."
        );

        // Verify pre-0.14 returns storage_commitment directly.
        assert_eq!(
            state_commitment_v013.0, storage_commitment.0,
            "Pre-0.14 should return storage_commitment when class_commitment is zero."
        );
    }

    /// Test that pre-v0.14 behavior is preserved for older versions.
    #[test]
    fn state_commitment_pre_v0_14_with_zero_class_commitment() {
        let storage_commitment = storage_commitment!("0x1234");
        let class_commitment = ClassCommitment::ZERO;

        // Pre-v0.14: state_commitment should equal storage_commitment when class is
        // zero.
        let state_v013 = StateCommitment::calculate(
            storage_commitment,
            class_commitment,
            StarknetVersion::V_0_13_4,
        );
        assert_eq!(
            state_v013.0, storage_commitment.0,
            "Pre-v0.14 should return storage_commitment when class_commitment is zero."
        );

        // v0.14+: state_commitment should use Poseidon formula.
        let state_v014 = StateCommitment::calculate(
            storage_commitment,
            class_commitment,
            StarknetVersion::V_0_14_0,
        );
        assert_ne!(
            state_v014.0, storage_commitment.0,
            "v0.14+ should NOT return storage_commitment directly when class_commitment is zero."
        );
    }

    /// Test that non-zero class commitment uses Poseidon formula for all
    /// versions.
    #[test]
    fn state_commitment_with_nonzero_class_commitment() {
        let storage_commitment = storage_commitment!("0x1234");
        let class_commitment = class_commitment!("0x5678");

        // Both versions should use Poseidon formula when class_commitment is non-zero.
        let state_v013 = StateCommitment::calculate(
            storage_commitment,
            class_commitment,
            StarknetVersion::V_0_13_4,
        );
        let state_v014 = StateCommitment::calculate(
            storage_commitment,
            class_commitment,
            StarknetVersion::V_0_14_0,
        );

        // Both should produce the same result (Poseidon hash).
        assert_eq!(
            state_v013, state_v014,
            "Non-zero class: v0.13 and v0.14 should produce the same result."
        );

        // Neither should equal storage_commitment directly.
        assert_ne!(
            state_v013.0, storage_commitment.0,
            "Non-zero class commitment should use Poseidon formula."
        );
    }

    /// Test that both storage and class commitment being zero returns zero
    /// state commitment.
    #[test]
    fn state_commitment_with_both_zero() {
        let storage_commitment = StorageCommitment::ZERO;
        let class_commitment = ClassCommitment::ZERO;

        // When both are zero, state commitment should be zero for any version.
        let state_v013 = StateCommitment::calculate(
            storage_commitment,
            class_commitment,
            StarknetVersion::V_0_13_4,
        );
        let state_v014 = StateCommitment::calculate(
            storage_commitment,
            class_commitment,
            StarknetVersion::V_0_14_0,
        );

        assert_eq!(
            state_v013,
            StateCommitment::ZERO,
            "Both zero should return StateCommitment::ZERO for pre-0.14."
        );
        assert_eq!(
            state_v014,
            StateCommitment::ZERO,
            "Both zero should return StateCommitment::ZERO for v0.14+."
        );
    }
}
