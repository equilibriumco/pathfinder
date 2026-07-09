//! State-update bench — measures the RocksDB port on the state-update code
//! path preserved across the port commits: `Transaction::insert_state_update`
//! (write) and `Transaction::state_update` (read).

use std::hint::black_box;
use std::num::NonZeroU32;

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use pathfinder_common::{BlockId, BlockNumber};
use pathfinder_storage::JournalMode;

mod common;

const WRITE_BLOCK_COUNTS: &[usize] = &[100, 1_000, 10_000];
const READ_POPULATE_BLOCKS: usize = 1_000;
const READ_SIZES: &[usize] = &[10, 100, 1_000];

fn bench_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("state_update/write");
    for &n_blocks in WRITE_BLOCK_COUNTS {
        group.throughput(criterion::Throughput::Elements(n_blocks as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(n_blocks),
            &n_blocks,
            |b, &n_blocks| {
                let blocks = common::generate_blocks(n_blocks);
                b.iter_batched(
                    || {
                        let (tempdir, storage) = common::tempdir_storage(
                            JournalMode::Rollback,
                            NonZeroU32::new(2).unwrap(),
                        );
                        // Insert block headers outside the timed path; state
                        // updates have a FK on block_number in block_headers.
                        {
                            let mut conn = storage.connection().expect("setup conn");
                            let tx = conn.transaction().expect("setup tx");
                            for block in &blocks {
                                tx.insert_block_header(&block.header.header)
                                    .expect("insert_block_header");
                            }
                            tx.commit().expect("setup commit");
                        }
                        (tempdir, storage)
                    },
                    |(tempdir, storage)| {
                        let mut conn = storage.connection().expect("bench conn");
                        let tx = conn.transaction().expect("bench tx");
                        for block in &blocks {
                            let Some(state_update) = block.state_update.as_ref() else {
                                continue;
                            };
                            black_box(
                                tx.insert_state_update(block.header.header.number, state_update)
                                    .expect("insert_state_update"),
                            );
                        }
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

fn bench_read(c: &mut Criterion) {
    // Populate once for the whole read group.
    let (tempdir, storage) =
        common::tempdir_storage(JournalMode::Rollback, NonZeroU32::new(4).unwrap());
    let blocks = common::generate_blocks(READ_POPULATE_BLOCKS);
    common::fill_blocks(&storage, &blocks);
    let block_numbers: Vec<BlockNumber> = (0..READ_POPULATE_BLOCKS as u64)
        .map(BlockNumber::new_or_panic)
        .collect();

    let mut group = c.benchmark_group("state_update/read");
    for &size in READ_SIZES {
        group.throughput(criterion::Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let mut cursor = 0usize;
            b.iter_batched_ref(
                || {
                    let start = cursor % block_numbers.len();
                    cursor = (cursor + size) % block_numbers.len().max(1);
                    (0..size)
                        .map(|i| block_numbers[(start + i) % block_numbers.len()])
                        .collect::<Vec<_>>()
                },
                |window| {
                    let mut conn = storage.connection().expect("bench conn");
                    let tx = conn.transaction().expect("bench tx");
                    for &block_number in window.iter() {
                        black_box(
                            tx.state_update(BlockId::Number(block_number))
                                .expect("state_update"),
                        );
                    }
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();

    drop(storage);
    drop(tempdir);
}

criterion_group!(state_update, bench_write, bench_read);
criterion_main!(state_update);
