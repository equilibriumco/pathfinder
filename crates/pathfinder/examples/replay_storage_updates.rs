//! Replay storage updates block-by-block from a Pathfinder database.
//!
//! Uses a null hash function to avoid computation overhead of building the
//! Merkle trees. This way storage performance can be measeured without the
//! overhead of hashing.
use std::num::NonZeroU32;

use anyhow::Context;
use pathfinder_common::hash::FeltHash;
use pathfinder_common::prelude::*;
use pathfinder_common::state_update::StateUpdateRef;
use pathfinder_crypto::Felt;
use pathfinder_merkle_tree::starknet_state::update_starknet_state;
use pathfinder_storage::StorageBuilder;

/// Implements [Hash] for the [Starknet Poseidon hash](poseidon_hash).
#[derive(Debug, Clone, Copy)]
pub struct NullHash;
impl FeltHash for NullHash {
    fn hash(a: Felt, _b: Felt) -> Felt {
        a
    }
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let input_database_path = std::env::args()
        .nth(1)
        .context("Please provide the input database path as the first argument")?;

    let output_database_path = std::env::args()
        .nth(2)
        .context("Please provide the output database path as the second argument")?;

    let input_storage = StorageBuilder::file(input_database_path.into())
        .migrate()
        .context("Migrating database")?
        .create_read_only_pool(NonZeroU32::new(1).expect("1>0"))
        .context("Creating connection pool")?;

    let output_storage = StorageBuilder::file(output_database_path.into())
        .migrate()
        .context("Migrating database")?
        .create_pool(NonZeroU32::new(32).expect("1>0"))
        .context("Creating connection pool")?;

    let mut input_db_conn = input_storage
        .connection()
        .context("Create database connection")?;

    let input_txn = input_db_conn
        .transaction()
        .context("Create database transaction")?;

    let mut output_db_conn = output_storage
        .connection()
        .context("Create database connection")?;

    let latest_block_number = input_txn
        .block_number(pathfinder_common::BlockId::Latest)
        .context("Getting latest block number")?
        .context("No blocks found")?;

    let mut parent_hash = pathfinder_common::BlockHash::ZERO;

    let mut aggregate_state_update = StateUpdate::default();

    for i in 0..=latest_block_number.get() {
        let block_number = BlockNumber::new(i).expect("is valid");

        let state_update = input_txn
            .state_update(block_number.into())
            .context("Getting state update")?
            .context("State update not found")?;

        let output_txn = output_db_conn
            .transaction()
            .context("Create database transaction")?;

        aggregate_state_update = aggregate_state_update.apply(&state_update);

        if i % 1000 == 999 {
            tracing::info!(%block_number, "Applying state update");
            let start = std::time::Instant::now();
            let (_storage_commitment, _class_commitment) =
                update_starknet_state::<NullHash, NullHash>(
                    &output_txn,
                    StateUpdateRef::from(&aggregate_state_update),
                    false,
                    block_number,
                    output_storage.clone(),
                )
                .expect("Failed to update state");
            let elapsed = start.elapsed();
            tracing::info!(%block_number, elapsed=%elapsed.as_millis(), "State update applied");
            aggregate_state_update = StateUpdate::default();
        }

        let header = BlockHeader {
            hash: BlockHash(Felt::from_u64(i)),
            parent_hash,
            number: block_number,
            timestamp: BlockTimestamp::new(i).expect("is valid"),
            eth_l1_gas_price: GasPrice::ZERO,
            strk_l1_gas_price: GasPrice::ZERO,
            eth_l1_data_gas_price: GasPrice::ZERO,
            strk_l1_data_gas_price: GasPrice::ZERO,
            eth_l2_gas_price: GasPrice::ZERO,
            strk_l2_gas_price: GasPrice::ZERO,
            sequencer_address: SequencerAddress::ZERO,
            starknet_version: StarknetVersion::V_0_14_0,
            event_commitment: EventCommitment::ZERO,
            state_commitment: StateCommitment::ZERO,
            transaction_commitment: TransactionCommitment::ZERO,
            transaction_count: 0,
            event_count: 0,
            l1_da_mode: L1DataAvailabilityMode::Blob,
            receipt_commitment: ReceiptCommitment::ZERO,
            state_diff_commitment: StateDiffCommitment::ZERO,
            state_diff_length: 0,
        };
        parent_hash = header.hash;

        output_txn
            .insert_block_header(&header)
            .expect("Failed to insert block header");

        for class_hash in &state_update.declared_cairo_classes {
            output_txn
                .insert_cairo_class_definition(*class_hash, b"")
                .context("Insert Cairo class definition")?;
        }

        for (class_hash, casm_hash) in &state_update.declared_sierra_classes {
            output_txn
                .insert_sierra_class_definition(&SierraHash(class_hash.0), b"", b"", casm_hash)
                .context("Insert Sierra class definition")?;
        }

        output_txn
            .insert_state_update_data(block_number, &state_update.into())
            .context("Insert state update into database")?;

        output_txn.commit().context("Commit transaction")?;
    }

    // Apply any remaining state updates.
    if aggregate_state_update.state_diff_length() > 0 {
        let output_txn = output_db_conn
            .transaction()
            .context("Create database transaction")?;
        let (_storage_commitment, _class_commitment) = update_starknet_state::<NullHash, NullHash>(
            &output_txn,
            StateUpdateRef::from(&aggregate_state_update),
            false,
            latest_block_number,
            output_storage.clone(),
        )
        .expect("Failed to update state");
    }

    Ok(())
}
