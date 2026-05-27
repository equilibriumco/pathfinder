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
    SerializedOpaqueClassDefinition,
    SerializedSierraDefinition,
};
use pathfinder_common::prelude::*;
use pathfinder_common::state_update::StateUpdateRef;
use pathfinder_crypto::Felt;
use pathfinder_merkle_tree::starknet_state::update_starknet_state;
use pathfinder_storage::{StorageBuilder, TriePruneMode};

struct ClassPayloads {
    cairo: SerializedOpaqueClassDefinition,
    sierra: SerializedOpaqueClassDefinition,
    casm: SerializedCasmDefinition,
}

/// Scans blocks in order and grabs the first non-empty Cairo definition,
/// Sierra definition, and CASM definition. Returns `Some` only once all three
/// have been found; returns `None` if we reach `last_block` short of any
/// payload (the caller falls back to synthetic bytes).
fn fetch_class_payloads(
    tx: &pathfinder_storage::Transaction<'_>,
    last_block: BlockNumber,
) -> anyhow::Result<Option<ClassPayloads>> {
    let mut cairo: Option<SerializedOpaqueClassDefinition> = None;
    let mut sierra: Option<SerializedOpaqueClassDefinition> = None;
    let mut casm: Option<SerializedCasmDefinition> = None;

    for b in 0..=last_block.get() {
        if cairo.is_some() && sierra.is_some() && casm.is_some() {
            break;
        }
        let block = BlockNumber::new(b).expect("valid");
        let su = tx
            .state_update(block.into())?
            .context("missing state update")?;

        if cairo.is_none() {
            for class_hash in &su.declared_cairo_classes {
                if let Some(def) = tx.class_definition(*class_hash)? {
                    if !def.as_slice().is_empty() {
                        cairo = Some(def);
                        break;
                    }
                }
            }
        }

        for (sierra_hash, _casm_hash) in &su.declared_sierra_classes {
            let class_hash = ClassHash(sierra_hash.0);
            if sierra.is_none() {
                if let Some(def) = tx.class_definition(class_hash)? {
                    if !def.as_slice().is_empty() {
                        sierra = Some(def);
                    }
                }
            }
            if casm.is_none() {
                if let Some(ca) = tx.casm_definition(class_hash)? {
                    if !ca.as_slice().is_empty() {
                        casm = Some(ca);
                    }
                }
            }
            if sierra.is_some() && casm.is_some() {
                break;
            }
        }
    }

    match (cairo, sierra, casm) {
        (Some(cairo), Some(sierra), Some(casm)) => Ok(Some(ClassPayloads {
            cairo,
            sierra,
            casm,
        })),
        _ => Ok(None),
    }
}

fn synthetic_class_bytes() -> Vec<u8> {
    // ~4 KB, repeating — non-trivial for zstd to compress, deterministic.
    b"PATHFINDER_BENCH_CLASS_DEF_PAYLOAD_v1\n".repeat(110)
}

fn append_input_load_ms(config_path: &str, value: u128) -> anyhow::Result<()> {
    let bytes = std::fs::read(config_path)?;
    let mut config: serde_json::Value = serde_json::from_slice(&bytes)?;
    config["input_load_ms_per_chunk"]
        .as_array_mut()
        .context("input_load_ms_per_chunk is not an array")?
        .push(serde_json::json!(value));
    std::fs::write(config_path, serde_json::to_vec_pretty(&config)?)?;
    Ok(())
}

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

    const PRELOAD_CHUNK_BLOCKS: u64 = 10_000;

    let config = serde_json::json!({
        "branch_sha": branch_sha,
        "host": host_label,
        "disk_type": disk_type,
        "engine": "rocksdb+sqlite",
        "trie_prune_mode": trie_prune_mode_str,
        "rocksdb_block_cache_bytes": output_storage.rocksdb_block_cache_bytes(),
        "rocksdb_atomic_flush": true,
        "rocksdb_wal_enabled": true,
        "sqlite_journal_mode": pragmas.journal_mode,
        "sqlite_synchronous": synchronous_str,
        "sqlite_cache_size_pages": pragmas.cache_size_pages,
        "sqlite_mmap_size_bytes": pragmas.mmap_size_bytes,
        "preload_chunk_blocks": PRELOAD_CHUNK_BLOCKS,
        "measure_from": measure_from,
        "input_load_ms_per_chunk": Vec::<u128>::new(),
    });

    std::fs::write(
        &config_path,
        serde_json::to_vec_pretty(&config).context("Serialize config sidecar")?,
    )
    .with_context(|| format!("Write config sidecar to {config_path}"))?;

    let (cairo_bytes, sierra_bytes, casm_bytes, class_payload_source) =
        match fetch_class_payloads(&input_txn, latest_block_number)? {
            Some(p) => (
                p.cairo.into_vec(),
                p.sierra.into_vec(),
                p.casm.into_vec(),
                "input_db",
            ),
            None => (
                synthetic_class_bytes(),
                synthetic_class_bytes(),
                synthetic_class_bytes(),
                "synthetic",
            ),
        };

    anyhow::ensure!(!cairo_bytes.is_empty(), "cairo_bytes empty");
    anyhow::ensure!(!sierra_bytes.is_empty(), "sierra_bytes empty");
    anyhow::ensure!(!casm_bytes.is_empty(), "casm_bytes empty");

    let bytes = std::fs::read(&config_path)?;
    let mut config: serde_json::Value = serde_json::from_slice(&bytes)?;
    config["class_payload_source"] = serde_json::json!(class_payload_source);
    std::fs::write(&config_path, serde_json::to_vec_pretty(&config)?)?;

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

    let last_block = latest_block_number.get();
    let mut chunk_start: u64 = 0;
    let mut batch_buf: Vec<StateUpdate> = Vec::with_capacity(1000);

    while chunk_start <= last_block {
        let chunk_end = (chunk_start + PRELOAD_CHUNK_BLOCKS - 1).min(last_block);

        // 1. Preload the chunk in one bulk read pass.
        let preload_start = std::time::Instant::now();
        let mut chunk: Vec<(BlockNumber, StateUpdate)> =
            Vec::with_capacity((chunk_end - chunk_start + 1) as usize);
        for b in chunk_start..=chunk_end {
            let block_number = BlockNumber::new(b).expect("is valid");
            let state_update = input_txn
                .state_update(block_number.into())
                .context("Getting state update")?
                .context("State update not found")?;
            chunk.push((block_number, state_update));
        }
        let preload_ms = preload_start.elapsed().as_millis();
        append_input_load_ms(&config_path, preload_ms)?;

        // 2. Per-block processing.
        for (block_number, state_update) in &chunk {
            let i = block_number.get();

            let output_txn = output_db_conn
                .transaction()
                .context("Create database transaction")?;

            batch_buf.push(state_update.clone());

            let mut trie_ms = None;
            if i % 1000 == 999 {
                for su in batch_buf.drain(..) {
                    aggregate_state_update = aggregate_state_update.apply(&su);
                }
                tracing::info!(%block_number, "Applying state update");
                let trie_start = std::time::Instant::now();
                let (_storage_commitment, _class_commitment) = update_starknet_state(
                    &output_txn,
                    StateUpdateRef::from(&aggregate_state_update),
                    false,
                    *block_number,
                    output_storage.clone(),
                )
                .context("Failed to update state")?;
                trie_ms = Some(trie_start.elapsed().as_millis());
            }

            let header = BlockHeader {
                hash: BlockHash(Felt::from_u64(i)),
                parent_hash,
                number: *block_number,
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
                        &SerializedCairoDefinition::from_slice(&cairo_bytes),
                    )
                    .context("Insert Cairo class definition")?;
            }

            for (class_hash, casm_hash) in &state_update.declared_sierra_classes {
                output_txn
                    .insert_sierra_class_definition(
                        &SierraHash(class_hash.0),
                        &SerializedSierraDefinition::from_slice(&sierra_bytes),
                        &SerializedCasmDefinition::from_slice(&casm_bytes),
                        casm_hash,
                    )
                    .context("Insert Sierra class definition")?;
            }

            output_txn
                .insert_state_update_data(*block_number, &state_update.clone().into())
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
                    let declared_cairo_classes =
                        aggregate_state_update.declared_cairo_classes.len();
                    let declared_sierra_classes =
                        aggregate_state_update.declared_sierra_classes.len();
                    let migrated_compiled_classes =
                        aggregate_state_update.migrated_compiled_classes.len();
                    let state_diff_length = aggregate_state_update.state_diff_length();

                    writeln!(
                        csv,
                        "{batch_start},{},{trie_ms},{commit_ms},{contract_updates},\
                         {system_updates},{storage_changes},{nonce_updates},{class_updates},\
                         {declared_cairo_classes},{declared_sierra_classes},\
                         {migrated_compiled_classes},{state_diff_length}",
                        i,
                    )?;
                    batches_measured += 1;
                }

                aggregate_state_update = StateUpdate::default();
                batch_start = i + 1;
            }
        }

        chunk_start = chunk_end + 1;
    }

    if aggregate_state_update.state_diff_length() > 0 || !batch_buf.is_empty() {
        for su in batch_buf.drain(..) {
            aggregate_state_update = aggregate_state_update.apply(&su);
        }

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
