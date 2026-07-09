//! Shared helpers for the four `pathfinder-storage` benchmarks.
//!
//! The benches share a fixed RNG seed and a small set of Storage constructors
//! so the same synthetic corpus is produced on branch base and on branch tip.
//! The only thing that changes between the two checkpoints is the storage
//! backend.

#![allow(dead_code)]

use std::num::NonZeroU32;
use std::path::Path;

use pathfinder_storage::fake::{self, Block};
use pathfinder_storage::{JournalMode, Storage, StorageBuilder};
use rand::rngs::StdRng;
use rand::SeedableRng;
use tempfile::TempDir;

/// Fixed seed shared by every bench. The corpus is shape-deterministic (block
/// count, row counts, sizes), but a handful of commitment/hash fields flow
/// through `thread_rng` inside `fake::Config::default`, so exact byte contents
/// drift across invocations. Backend timing deltas measured by criterion are
/// still attributable to the storage backend — shape dominates timing.
pub const BENCH_SEED: u64 = 0xB00B_BEEF;

/// Fresh seeded RNG. Every bench builds its corpus from this.
pub fn rng() -> StdRng {
    StdRng::seed_from_u64(BENCH_SEED)
}

/// Fresh tempdir-backed Storage with `pool_size` connections.
///
/// The returned `TempDir` owns the SQLite file and, once this code is rebased
/// onto the branch tip, the co-located RocksDB directory as well. Drop the
/// `Storage` before the `TempDir` so RocksDB releases its LOCK file first.
pub fn tempdir_storage(journal_mode: JournalMode, pool_size: NonZeroU32) -> (TempDir, Storage) {
    let tempdir = tempfile::tempdir().expect("create bench tempdir");
    let db_path = tempdir.path().join("db.sqlite");
    let mut manager = StorageBuilder::file(db_path)
        .journal_mode(journal_mode)
        .migrate()
        .expect("migrate bench storage");
    let storage = manager
        .create_pool(pool_size)
        .expect("create bench connection pool");
    (tempdir, storage)
}

/// Generate `n_blocks` fake blocks from the fixed seed. Delegates to
/// `pathfinder_storage::fake::generate::with_rng_and_config` so the bench uses
/// the same generator the rest of the crate does.
pub fn generate_blocks(n_blocks: usize) -> Vec<Block> {
    let mut rng = rng();
    fake::generate::with_rng_and_config(n_blocks, &mut rng, fake::Config::default())
}

/// Populate `storage` with `blocks` and no trie update — the bench code
/// controls trie insertion itself where relevant. Passes `None` for the
/// `UpdateTriesFn` because we do not want hashing on the timed path.
pub fn fill_blocks(storage: &Storage, blocks: &[Block]) {
    fake::fill(storage, blocks, None);
}

/// Recursively copy `src` into `dst`, creating `dst` if needed. Used by the
/// pruning bench's template pattern (T5). Does not preserve symlinks — the
/// pathfinder storage layout uses only regular files.
pub fn copy_dir_all(src: &Path, dst: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in walkdir::WalkDir::new(src).min_depth(1) {
        let entry = entry?;
        let rel = entry
            .path()
            .strip_prefix(src)
            .expect("walkdir returns paths under root");
        let target = dst.join(rel);
        if entry.file_type().is_dir() {
            std::fs::create_dir_all(&target)?;
        } else if entry.file_type().is_file() {
            if let Some(parent) = target.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::copy(entry.path(), &target)?;
        }
    }
    Ok(())
}
