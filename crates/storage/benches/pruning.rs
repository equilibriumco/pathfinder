//! Pruning bench — measures the RocksDB port on the pruning code path
//! preserved across the port commits: `Transaction::prune_block`.
//!
//! Setup pattern:
//! 1. Build a template directory once per DB-size. Drop the `Storage`
//!    completely so RocksDB releases its LOCK file.
//! 2. For each criterion iteration, walk-copy the template into a fresh tempdir
//!    and open a new `Storage`. This is `iter_batched` setup, so it stays off
//!    the timed path.
//! 3. Timed section: open a write transaction, `prune_block` from earliest to
//!    `latest - RETENTION`, commit.
//!
//! The loop shape mirrors the sync-side loop in
//! `crates/pathfinder/src/state/sync.rs:1170-1180` but does not depend on it.

use std::hint::black_box;
use std::num::NonZeroU32;

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use pathfinder_common::BlockNumber;
use pathfinder_storage::JournalMode;
use tempfile::TempDir;

mod common;

// RETENTION < smallest SIZE by design — every size exercises actual pruning.
const SIZES: &[usize] = &[1_000, 10_000];
const RETENTION: u64 = 100;

/// Build a template directory of `n_blocks` synthetic blocks. Drops the
/// `Storage` and its pool before returning so the tempdir is copy-safe.
fn build_template(n_blocks: usize) -> TempDir {
    let (tempdir, storage) =
        common::tempdir_storage(JournalMode::Rollback, NonZeroU32::new(2).unwrap());
    let blocks = common::generate_blocks(n_blocks);
    common::fill_blocks(&storage, &blocks);
    drop(storage);
    tempdir
}

/// Open a copy of `template` under a fresh tempdir. Returns the fresh tempdir
/// (owning both SQLite and RocksDB directories) and the opened `Storage`.
fn open_copy(template: &TempDir) -> (TempDir, pathfinder_storage::Storage) {
    let fresh = tempfile::tempdir().expect("create fresh tempdir for prune iteration");
    common::copy_dir_all(template.path(), fresh.path()).expect("copy template");
    let db_path = fresh.path().join("db.sqlite");
    let storage = pathfinder_storage::StorageBuilder::file(db_path)
        .journal_mode(JournalMode::Rollback)
        .migrate()
        .expect("migrate copied storage")
        .create_pool(NonZeroU32::new(2).unwrap())
        .expect("open copied storage");
    (fresh, storage)
}

fn bench_sweep(c: &mut Criterion) {
    let mut group = c.benchmark_group("pruning/sweep");
    for &n_blocks in SIZES {
        group.throughput(criterion::Throughput::Elements(n_blocks as u64));
        // Larger sizes: copy time is nontrivial; use PerIteration so criterion
        // does not batch iterations without re-running the copy.
        let batch = if n_blocks >= 10_000 {
            BatchSize::PerIteration
        } else {
            BatchSize::SmallInput
        };
        // Template built once per size — expensive, deliberately outside the
        // per-iteration path.
        let template = build_template(n_blocks);
        group.bench_with_input(
            BenchmarkId::from_parameter(n_blocks),
            &n_blocks,
            |b, &n_blocks| {
                b.iter_batched(
                    || {
                        // Setup — off the timed path. Acquire the connection
                        // here so first-connection pragmas / RocksDB handle
                        // setup do not leak into the measured section.
                        let (fresh, storage) = open_copy(&template);
                        let conn = storage.connection().expect("prune conn");
                        (fresh, storage, conn)
                    },
                    |(fresh, storage, mut conn)| {
                        // Timed section: begin transaction, prune loop, commit.
                        let tx = conn.transaction().expect("prune tx");
                        let latest = (n_blocks as u64).saturating_sub(1);
                        if latest >= RETENTION {
                            let last_kept = latest - RETENTION;
                            for block in 0..=last_kept {
                                let block = BlockNumber::new_or_panic(block);
                                black_box(tx.prune_block(block).expect("prune_block"));
                            }
                        }
                        tx.commit().expect("prune commit");
                        drop(conn);
                        drop(storage);
                        drop(fresh);
                    },
                    batch,
                );
            },
        );
        // Template drops when the loop iteration ends.
        drop(template);
    }
    group.finish();
}

criterion_group!(pruning, bench_sweep);
criterion_main!(pruning);
