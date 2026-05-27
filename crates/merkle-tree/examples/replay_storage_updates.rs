//! Replay storage updates block-by-block from a Pathfinder database.
//!
//! Uses the `bench-skip-hashing` feature to replace hash computations with
//! no-ops, so storage performance can be measured without hashing overhead.

#[cfg(not(feature = "bench-skip-hashing"))]
compile_error!(
    "This example requires the `bench-skip-hashing` feature: cargo run --example \
     replay_storage_updates --features bench-skip-hashing"
);

use std::io::Write;
use std::num::NonZeroU32;

use anyhow::Context;
use pathfinder_common::class_definition::{
    SerializedCairoDefinition,
    SerializedCasmDefinition,
    SerializedSierraDefinition,
};
use pathfinder_common::prelude::*;
use pathfinder_common::state_update::StateUpdateRef;
use pathfinder_crypto::Felt;
use pathfinder_merkle_tree::starknet_state::update_starknet_state;
use pathfinder_storage::{StorageBuilder, TriePruneMode};

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();

    let mut args = std::env::args().skip(1);
    let input_database_path: String = args.next().context("arg 1: input database path")?;
    let output_database_path: String = args.next().context("arg 2: output database path")?;
    let csv_output_path: String = args.next().context("arg 3: CSV output path")?;
    let host_label: String = args
        .next()
        .context("arg 4: host label (free-form identifier)")?;
    let disk_type: String = args.next().context("arg 5: disk type (`ssd` or `hdd`)")?;
    let measure_from: u64 = args
        .next()
        .map(|s| {
            s.parse()
                .context("arg 6: measure_from must be a block number")
        })
        .transpose()?
        .unwrap_or(0);

    anyhow::ensure!(
        matches!(disk_type.as_str(), "ssd" | "hdd"),
        "disk_type must be `ssd` or `hdd`",
    );

    let input_storage = StorageBuilder::file(input_database_path.into())
        .migrate()
        .context("Migrating database")?
        .create_read_only_pool(NonZeroU32::new(1).expect("1>0"))
        .context("Creating connection pool")?;

    let output_storage = StorageBuilder::file(output_database_path.into())
        .trie_prune_mode(Some(TriePruneMode::Archive))
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

    let pragmas = output_storage.sqlite_pragma_snapshot()?;

    let config_path = format!("{csv_output_path}.config.json");
    let branch_sha = option_env!("BENCH_BRANCH_SHA").unwrap_or("unknown");

    let synchronous_str = match pragmas.synchronous {
        0 => "off",
        1 => "normal",
        2 => "full",
        3 => "extra",
        _ => "unknown",
    };

    let trie_prune_mode_str = match output_storage.trie_prune_mode() {
        TriePruneMode::Archive => "archive".to_string(),
        TriePruneMode::Prune { num_blocks_kept } => format!("prune({num_blocks_kept})"),
    };

    let config = serde_json::json!({
        "branch_sha": branch_sha,
        "host": host_label,
        "disk_type": disk_type,
        "engine": "sqlite",
        "trie_prune_mode": trie_prune_mode_str,
        "sqlite_journal_mode": pragmas.journal_mode,
        "sqlite_synchronous": synchronous_str,
        "sqlite_cache_size_pages": pragmas.cache_size_pages,
        "sqlite_mmap_size_bytes": pragmas.mmap_size_bytes,
        "preload_chunk_blocks": 10_000,
        "measure_from": measure_from,
        "input_load_ms_per_chunk": Vec::<u128>::new(),
    });

    std::fs::write(
        &config_path,
        serde_json::to_vec_pretty(&config).context("Serialize config sidecar")?,
    )
    .with_context(|| format!("Write config sidecar to {config_path}"))?;

    let mut parent_hash = pathfinder_common::BlockHash::ZERO;

    let mut aggregate_state_update = StateUpdate::default();

    let mut batch_start: u64 = 0;
    let mut batches_measured: u64 = 0;
    let total_start = std::time::Instant::now();

    let csv_file = std::fs::File::create(&csv_output_path)
        .with_context(|| format!("Create CSV output file at {csv_output_path}"))?;
    let mut csv = std::io::BufWriter::new(csv_file);

    writeln!(
        csv,
        "batch_start,batch_end,trie_ms,commit_ms,contract_updates,system_updates,storage_changes,\
         nonce_updates,class_updates,declared_cairo_classes,declared_sierra_classes,\
         migrated_compiled_classes,state_diff_length"
    )?;

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

        let mut trie_ms = None;
        if i % 1000 == 999 {
            tracing::info!(%block_number, "Applying state update");
            let trie_start = std::time::Instant::now();
            let (_storage_commitment, _class_commitment) = update_starknet_state(
                &output_txn,
                StateUpdateRef::from(&aggregate_state_update),
                false,
                block_number,
                output_storage.clone(),
            )
            .context("Failed to update state")?;
            trie_ms = Some(trie_start.elapsed().as_millis());
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
                .insert_cairo_class_definition(
                    *class_hash,
                    &SerializedCairoDefinition::from_slice(b""),
                )
                .context("Insert Cairo class definition")?;
        }

        for (class_hash, casm_hash) in &state_update.declared_sierra_classes {
            output_txn
                .insert_sierra_class_definition(
                    &SierraHash(class_hash.0),
                    &SerializedSierraDefinition::from_slice(b""),
                    &SerializedCasmDefinition::from_slice(b""),
                    casm_hash,
                )
                .context("Insert Sierra class definition")?;
        }

        output_txn
            .insert_state_update_data(block_number, &state_update.into())
            .context("Insert state update into database")?;

        let commit_start = std::time::Instant::now();
        output_txn.commit().context("Commit transaction")?;
        let commit_ms = commit_start.elapsed().as_millis();

        if let Some(trie_ms) = trie_ms {
            tracing::info!(%block_number, %trie_ms, %commit_ms, "State update applied");

            if batch_start >= measure_from {
                let contract_updates = aggregate_state_update.contract_updates.len();
                let system_updates = aggregate_state_update.system_contract_updates.len();
                let storage_changes: usize = aggregate_state_update
                    .contract_updates
                    .values()
                    .map(|u| u.storage.len())
                    .sum::<usize>()
                    + aggregate_state_update
                        .system_contract_updates
                        .values()
                        .map(|u| u.storage.len())
                        .sum::<usize>();
                let nonce_updates = aggregate_state_update
                    .contract_updates
                    .values()
                    .filter(|u| u.nonce.is_some())
                    .count();
                let class_updates = aggregate_state_update
                    .contract_updates
                    .values()
                    .filter(|u| u.class.is_some())
                    .count();
                let declared_cairo_classes = aggregate_state_update.declared_cairo_classes.len();
                let declared_sierra_classes = aggregate_state_update.declared_sierra_classes.len();
                let migrated_compiled_classes =
                    aggregate_state_update.migrated_compiled_classes.len();
                let state_diff_length = aggregate_state_update.state_diff_length();

                writeln!(
                    csv,
                    "{batch_start},{},{trie_ms},{commit_ms},{contract_updates},{system_updates},\
                     {storage_changes},{nonce_updates},{class_updates},{declared_cairo_classes},\
                     {declared_sierra_classes},{migrated_compiled_classes},{state_diff_length}",
                    i,
                )?;
                batches_measured += 1;
            }

            aggregate_state_update = StateUpdate::default();
            batch_start = i + 1;
        }
    }

    if aggregate_state_update.state_diff_length() > 0 {
        let output_txn = output_db_conn
            .transaction()
            .context("Create database transaction")?;

        let trie_start = std::time::Instant::now();
        let (_storage_commitment, _class_commitment) = update_starknet_state(
            &output_txn,
            StateUpdateRef::from(&aggregate_state_update),
            false,
            latest_block_number,
            output_storage.clone(),
        )
        .context("Failed to update state")?;
        let trie_ms = trie_start.elapsed().as_millis();

        let commit_start = std::time::Instant::now();
        output_txn.commit().context("Commit final transaction")?;
        let commit_ms = commit_start.elapsed().as_millis();

        if batch_start >= measure_from {
            let contract_updates = aggregate_state_update.contract_updates.len();
            let system_updates = aggregate_state_update.system_contract_updates.len();
            let storage_changes: usize = aggregate_state_update
                .contract_updates
                .values()
                .map(|u| u.storage.len())
                .sum::<usize>()
                + aggregate_state_update
                    .system_contract_updates
                    .values()
                    .map(|u| u.storage.len())
                    .sum::<usize>();
            let nonce_updates = aggregate_state_update
                .contract_updates
                .values()
                .filter(|u| u.nonce.is_some())
                .count();
            let class_updates = aggregate_state_update
                .contract_updates
                .values()
                .filter(|u| u.class.is_some())
                .count();
            let declared_cairo_classes = aggregate_state_update.declared_cairo_classes.len();
            let declared_sierra_classes = aggregate_state_update.declared_sierra_classes.len();
            let migrated_compiled_classes = aggregate_state_update.migrated_compiled_classes.len();
            let state_diff_length = aggregate_state_update.state_diff_length();

            writeln!(
                csv,
                "{batch_start},{},{trie_ms},{commit_ms},{contract_updates},{system_updates},\
                 {storage_changes},{nonce_updates},{class_updates},{declared_cairo_classes},\
                 {declared_sierra_classes},{migrated_compiled_classes},{state_diff_length}",
                latest_block_number.get(),
            )?;
            batches_measured += 1;
        }
    }

    csv.flush().context("Flushing CSV")?;

    let total_elapsed = total_start.elapsed();
    eprintln!(
        "Total blocks: {}, Total elapsed: {}ms, Batches measured: {}",
        latest_block_number.get() + 1,
        total_elapsed.as_millis(),
        batches_measured,
    );

    Ok(())
}
