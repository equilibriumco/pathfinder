//! Block-data bench — insert transactions, receipts, events; scan events by
//! filter. Insert path: `Transaction::insert_transaction_data`. Read path:
//! `Transaction::events` with `EventConstraints`.
//!
//! All filter variants use explicit keys — never `vec![]` at a position — to
//! keep the bench off the aggregate-bloom fix's changed branch. The
//! aggregate-bloom fix sits at the base of the branch, before B0..B4, so both
//! the base checkpoint and the tip checkpoint include the fix. Constant across
//! the comparison.

use std::hint::black_box;
use std::num::{NonZeroU32, NonZeroUsize};

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use pathfinder_common::event::Event;
use pathfinder_common::receipt::Receipt;
use pathfinder_common::transaction::Transaction as StarknetTransaction;
use pathfinder_common::{BlockHeader, BlockNumber, ContractAddress, EventKey};
use pathfinder_storage::{EventConstraints, JournalMode};

mod common;

const WRITE_TX_COUNTS: &[usize] = &[10, 100, 1_000];
const EVENTS_PER_TX: usize = 3;
const READ_POPULATE_BLOCKS: usize = 1_000;

fn block_range_limit() -> NonZeroUsize {
    NonZeroUsize::new(READ_POPULATE_BLOCKS).expect("READ_POPULATE_BLOCKS > 0")
}

fn bench_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("block_data/write");
    for &tx_count in WRITE_TX_COUNTS {
        group.throughput(criterion::Throughput::Elements(tx_count as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(tx_count),
            &tx_count,
            |b, &tx_count| {
                // Generate enough blocks to harvest tx_count transactions.
                // Each block produces at least one transaction with a unique hash
                // (the fake generator hashes transaction content, and block
                // randomness diverges per block). We oversample by 4× to ensure
                // we always collect enough even when some blocks are sparse.
                let n_blocks = (tx_count * 4).max(1);
                let blocks = common::generate_blocks(n_blocks);
                // Use the first block's header for the FK — all txs land in
                // block 0 (GENESIS) in the timed path.
                let block_header: BlockHeader = blocks[0].header.header.clone();
                // Drain transactions across all blocks until we have tx_count.
                let mut txs: Vec<(StarknetTransaction, Receipt)> = Vec::with_capacity(tx_count);
                let mut events: Vec<Vec<Event>> = Vec::with_capacity(tx_count);
                'outer: for block in blocks {
                    for (tx, receipt, mut evs) in block.transaction_data {
                        if txs.len() >= tx_count {
                            break 'outer;
                        }
                        evs.truncate(EVENTS_PER_TX);
                        txs.push((tx, receipt));
                        events.push(evs);
                    }
                }
                assert_eq!(
                    txs.len(),
                    tx_count,
                    "could not collect {tx_count} transactions from fake corpus"
                );

                b.iter_batched(
                    || {
                        let (tempdir, storage) = common::tempdir_storage(
                            JournalMode::Rollback,
                            NonZeroU32::new(2).unwrap(),
                        );
                        // Header must exist before insert_transaction_data due
                        // to the FK constraint on block_headers.number.
                        {
                            let mut conn = storage.connection().expect("setup conn");
                            let tx = conn.transaction().expect("setup tx");
                            tx.insert_block_header(&block_header)
                                .expect("insert_block_header");
                            tx.commit().expect("setup commit");
                        }
                        (tempdir, storage)
                    },
                    |(tempdir, storage)| {
                        let mut conn = storage.connection().expect("bench conn");
                        let tx = conn.transaction().expect("bench tx");
                        black_box(
                            tx.insert_transaction_data(BlockNumber::GENESIS, &txs, Some(&events))
                                .expect("insert_transaction_data"),
                        );
                        tx.commit().expect("commit");
                        drop(storage);
                        drop(tempdir);
                    },
                    BatchSize::PerIteration,
                );
            },
        );
    }
    group.finish();
}

/// Filter selectivity levels for the read group.
#[derive(Clone, Copy, Debug)]
enum Selectivity {
    Narrow,
    Medium,
    Broad,
}

impl std::fmt::Display for Selectivity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Selectivity::Narrow => f.write_str("narrow"),
            Selectivity::Medium => f.write_str("medium"),
            Selectivity::Broad => f.write_str("broad"),
        }
    }
}

fn bench_read(c: &mut Criterion) {
    // Populate once.
    let (tempdir, storage) =
        common::tempdir_storage(JournalMode::Rollback, NonZeroU32::new(4).unwrap());
    let blocks = common::generate_blocks(READ_POPULATE_BLOCKS);
    common::fill_blocks(&storage, &blocks);

    // Harvest one contract address and three event keys from block 0 so the
    // filters actually match. Explicit keys avoid the aggregate-bloom fix's
    // changed branch (F6).
    let (contract, keys) = harvest_filter_targets(&blocks);

    let filters: [(Selectivity, EventConstraints); 3] = [
        (
            Selectivity::Narrow,
            EventConstraints {
                from_block: Some(BlockNumber::new_or_panic(0)),
                to_block: Some(BlockNumber::new_or_panic((READ_POPULATE_BLOCKS - 1) as u64)),
                contract_addresses: vec![contract],
                keys: vec![vec![keys[0]]],
                page_size: 128,
                offset: 0,
            },
        ),
        (
            Selectivity::Medium,
            EventConstraints {
                from_block: Some(BlockNumber::new_or_panic(0)),
                to_block: Some(BlockNumber::new_or_panic((READ_POPULATE_BLOCKS - 1) as u64)),
                contract_addresses: vec![contract],
                keys: vec![vec![keys[0], keys[1], keys[2]]],
                page_size: 128,
                offset: 0,
            },
        ),
        (
            Selectivity::Broad,
            EventConstraints {
                from_block: Some(BlockNumber::new_or_panic(0)),
                to_block: Some(BlockNumber::new_or_panic((READ_POPULATE_BLOCKS - 1) as u64)),
                contract_addresses: Vec::new(),
                keys: vec![vec![keys[0], keys[1], keys[2]]],
                page_size: 128,
                offset: 0,
            },
        ),
    ];

    let mut group = c.benchmark_group("block_data/read");
    for (selectivity, constraints) in filters.iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(selectivity),
            constraints,
            |b, constraints| {
                b.iter(|| {
                    let mut conn = storage.connection().expect("bench conn");
                    let tx = conn.transaction().expect("bench tx");
                    black_box(tx.events(constraints, block_range_limit()).expect("events"));
                });
            },
        );
    }
    group.finish();

    drop(storage);
    drop(tempdir);
}

fn harvest_filter_targets(
    blocks: &[pathfinder_storage::fake::Block],
) -> (ContractAddress, [EventKey; 3]) {
    // Pick the first block that has at least one event with >= 3 keys.
    for block in blocks {
        for (_, _, events) in &block.transaction_data {
            for event in events {
                if event.keys.len() >= 3 {
                    let keys = [event.keys[0], event.keys[1], event.keys[2]];
                    return (event.from_address, keys);
                }
            }
        }
    }
    // Fall back: pad the first single-key event by repeating its one key.
    for block in blocks {
        for (_, _, events) in &block.transaction_data {
            if let Some(event) = events.first() {
                if let Some(&k) = event.keys.first() {
                    return (event.from_address, [k, k, k]);
                }
            }
        }
    }
    panic!("no keyed events in the fake corpus — regenerate with more blocks");
}

criterion_group!(block_data, bench_write, bench_read);
criterion_main!(block_data);
