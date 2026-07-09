//! Trie storage bench — measures the RocksDB port's throughput on the two
//! public storage-trie code paths preserved across the four port commits:
//! `Transaction::insert_storage_trie` (write) and
//! `Transaction::storage_trie_node` (read). Hashing sits above this layer in
//! `pathfinder-merkle-tree` and is therefore off the timed path.

use std::hint::black_box;
use std::num::NonZeroU32;

use bitvec::prelude::Msb0;
use bitvec::vec::BitVec;
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use pathfinder_common::BlockNumber;
use pathfinder_storage::{JournalMode, Node, NodeRef, TrieStorageIndex, TrieUpdate};
use rand::seq::SliceRandom;
use rand::Rng;

mod common;

const WRITE_SIZES: &[usize] = &[100, 1_000, 10_000];
const READ_POPULATE: usize = 10_000;
const READ_SIZES: &[usize] = &[10, 100, 1_000];

/// Build a `TrieUpdate` whose serialization stores exactly `n` nodes.
///
/// `insert_storage_trie` traverses from `nodes_added.last()` (the root) and
/// only stores nodes reachable via child `NodeRef`s. To force all `n` slots to
/// be written we construct a chain: index 0 is a `LeafEdge` and each higher
/// index is an `Edge` whose child points at the previous slot. The last slot
/// is the root. Distinct per-slot paths and per-slot random hashes prevent
/// backend-side dedup from favoring either storage engine.
fn make_update(rng: &mut impl Rng, n: usize) -> TrieUpdate {
    assert!(n >= 1, "make_update needs at least one node");
    let mut nodes_added = Vec::with_capacity(n);
    let random_felt = |rng: &mut _| -> pathfinder_crypto::Felt {
        let mut bytes = [0u8; 32];
        Rng::fill(rng, &mut bytes[..]);
        pathfinder_crypto::Felt::from_be_bytes(bytes).unwrap_or_default()
    };
    let path0 = BitVec::<u8, Msb0>::from_slice(&0u64.to_be_bytes());
    nodes_added.push((random_felt(rng), Node::LeafEdge { path: path0 }));
    for i in 1..n {
        let path = BitVec::<u8, Msb0>::from_slice(&(i as u64).to_be_bytes());
        nodes_added.push((
            random_felt(rng),
            Node::Edge {
                child: NodeRef::Index(i - 1),
                path,
            },
        ));
    }
    let root_commitment = nodes_added.last().map(|(f, _)| *f).expect("n >= 1");
    TrieUpdate {
        nodes_added,
        nodes_removed: Vec::new(),
        root_commitment,
    }
}

fn bench_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("trie/write");
    for &size in WRITE_SIZES {
        group.throughput(criterion::Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter_batched(
                || {
                    // Setup, excluded from timing.
                    let (tempdir, storage) =
                        common::tempdir_storage(JournalMode::Rollback, NonZeroU32::new(2).unwrap());
                    let mut rng = common::rng();
                    let update = make_update(&mut rng, size);
                    (tempdir, storage, update)
                },
                |(tempdir, storage, update)| {
                    let mut conn = storage.connection().expect("bench conn");
                    let tx = conn.transaction().expect("bench tx");
                    black_box(
                        tx.insert_storage_trie(&update, BlockNumber::GENESIS)
                            .expect("insert_storage_trie"),
                    );
                    tx.commit().expect("commit");
                    drop(storage);
                    drop(tempdir);
                },
                BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

fn bench_read(c: &mut Criterion) {
    // Populate once for the whole read group.
    let (tempdir, storage) =
        common::tempdir_storage(JournalMode::Rollback, NonZeroU32::new(4).unwrap());
    let indices: Vec<TrieStorageIndex> = {
        let mut conn = storage.connection().expect("populate conn");
        let tx = conn.transaction().expect("populate tx");
        let mut rng = common::rng();
        let update = make_update(&mut rng, READ_POPULATE);
        let root = tx
            .insert_storage_trie(&update, BlockNumber::GENESIS)
            .expect("populate insert_storage_trie");
        tx.commit().expect("populate commit");
        // Verify populate succeeded, then probe for valid indices: different
        // backends (SQLite vs RocksDB) assign indices in different orders, so
        // we cannot derive the full range from root_index alone. Probe a
        // window wide enough to contain all READ_POPULATE inserted nodes.
        match root {
            pathfinder_storage::RootIndexUpdate::Updated(_) => {}
            other => panic!("expected updated root, got {other:?}"),
        }
        let mut conn2 = storage.connection().expect("probe conn");
        let tx2 = conn2.transaction().expect("probe tx");
        let indices: Vec<TrieStorageIndex> = (0..(READ_POPULATE * 2) as u64)
            .filter_map(TrieStorageIndex::new)
            .filter(|&idx| {
                tx2.storage_trie_node(idx)
                    .expect("probe storage_trie_node")
                    .is_some()
            })
            .take(READ_POPULATE)
            .collect();
        indices
    };
    assert_eq!(indices.len(), READ_POPULATE);
    let mut shuffled = indices.clone();
    shuffled.shuffle(&mut common::rng());

    let mut group = c.benchmark_group("trie/read");
    for &size in READ_SIZES {
        assert!(size <= shuffled.len(), "read size exceeds populated corpus");
        group.throughput(criterion::Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let mut cursor = 0usize;
            b.iter_batched_ref(
                || {
                    let start = cursor % shuffled.len();
                    cursor = (cursor + size) % shuffled.len();
                    (0..size)
                        .map(|i| shuffled[(start + i) % shuffled.len()])
                        .collect::<Vec<_>>()
                },
                |window| {
                    let mut conn = storage.connection().expect("bench conn");
                    let tx = conn.transaction().expect("bench tx");
                    for &idx in window.iter() {
                        black_box(tx.storage_trie_node(idx).expect("storage_trie_node"));
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

criterion_group!(trie, bench_write, bench_read);
criterion_main!(trie);
