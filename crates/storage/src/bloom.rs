//! A Bloom filter is a space-efficient probabilistic data structure that
//! is used to test whether an element is a member of a set. An empty Bloom
//! filter is a bit array of m bits, all set to 0. It is equipped with k
//! different hash functions, which map set elements to one of the m possible
//! array positions. To check whether an element is in the set, the element is
//! hashed k times and the bits at the resulting positions are checked. If all k
//! bits are set to 1, the element is considered to be in the set (false
//! positives are possible). In our case, each block is a set with its own
//! [`BloomFilter`] and the elements are block events' keys and contract
//! addresses.
//!
//! When considering scenarios where keys need to be checked for large ranges
//! of blocks, it can certainly take a long time if [`BloomFilter`]s for each
//! block are loaded and checked against one by one. A possible optimization is
//! to store aggregates of [`BloomFilter`]s for ranges of blocks and, once keys
//! need to be checked for that range, this [`AggregateBloom`] filter can
//! be loaded and checked against in a single shot.
//!
//! Example: A key K1 that is mapped to three (out of *eight) indices of the
//! bloom filter, needs to be added to an aggregate bloom filter for a range
//! of *ten blocks. (*these are illustratory numbers, in practice much larger
//! values are used).
//!
//! We start with an empty aggregate filter, an 8x10 bitmap full of zeroes. Rows
//! of this matrix represent bloom filter indices that keys can be mapped to
//! whereas the columns represent the blocks within the range for which the
//! aggregate bloom filter is used.
//!
//! HashFn(K1) = [0, 1, 0, 1, 1, 0, 0, 0]
//!
//! We are inserting K1 as a part of the first block. In order to insert the key
//! into the aggregate, we first rotate it clockwise by 90 degrees (turning it
//! from a row vector into a column vector). Then, we set the first bit (since
//! we are adding to the first block) of rows 1, 3 and 4 (zero based) because
//! bloom filter hash functions map K1 to these indices. After this, we are left
//! with the following bitmap:
//!
//! [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
//! [1, 0, 0, 0, 0, 0, 0, 0, 0, 0]
//! [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
//! [1, 0, 0, 0, 0, 0, 0, 0, 0, 0]
//! [1, 0, 0, 0, 0, 0, 0, 0, 0, 0]
//! [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
//! [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
//! [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
//!
//! which we can now store. Next, to check if K1 has been added to any of the
//! blocks, we perform the following steps:
//! 1) Load an [`AggregateBloom`] filter with the previously stored bitmap.
//! 2) Obtain the indices to which K1 maps to (1, 3 and 4 in this example),
//!    pluck out the corresponding rows and bitwise AND them together, leaving
//!    us with a 1x10 bit vector.
//! 3) Indices of bits that are set in the bit vector obtained through step 2)
//!    are block numbers to which K1 could have been added (or are false
//!    positives due to how Bloom filters work). In this example, the first bit
//!    will be set meaning block 0 could contain K1 (and it does since this is a
//!    very simplified example).
//!
//! This way, it's possible to quickly figure out which blocks correspond to a
//! specific set of keys without having to load and check each individual bloom
//! filter.

use std::hash::{Hash, Hasher};
use std::sync::{Arc, LazyLock, Mutex};

use bitvec::prelude::*;
use cached::{Cached, SizedCache};
use pathfinder_common::BlockNumber;
use pathfinder_crypto::Felt;
use siphasher::sip::SipHasher13;

/// Maximum number of blocks to aggregate in a single `AggregateBloom`.
pub const AGGREGATE_BLOOM_BLOCK_RANGE_LEN: u64 =
    // if cfg!(any(test, feature = "small_aggregate_filters")) {
    //     // Make testing faster and easier by using a smaller range.
    //     24
    // } else {
    8192;
// };

/// An aggregate of all Bloom filters for a given range of blocks.
/// Before being added to `AggregateBloom`, each [`BloomFilter`] is
/// rotated by 90 degrees (transposed).
#[derive(Clone)]
pub struct AggregateBloom {
    /// A [AGGREGATE_BLOOM_BLOCK_RANGE_LEN] by [BloomFilter::BITVEC_BITS] matrix
    /// stored in a single array.
    bitmap: Vec<u8>,

    /// Starting (inclusive) block number for the range of blocks that this
    /// aggregate covers.
    pub from_block: BlockNumber,

    /// Ending (inclusive) block number for the range of blocks that this
    /// aggregate covers.
    pub to_block: BlockNumber,
}

impl AggregateBloom {
    /// Number of bytes that an `AggregateBloom` block range is represented by.
    const BLOCK_RANGE_BYTES: usize = AGGREGATE_BLOOM_BLOCK_RANGE_LEN as usize / 8;

    /// Create a new `AggregateBloom` for the following range:
    ///
    /// \[`from_block`, `from_block + (AGGREGATE_BLOOM_BLOCK_RANGE_LEN) - 1`\]
    pub fn new(from_block: BlockNumber) -> Self {
        let to_block = from_block + AGGREGATE_BLOOM_BLOCK_RANGE_LEN - 1;
        let bitmap = vec![0; Self::BLOCK_RANGE_BYTES * BloomFilter::BITVEC_BITS];
        Self::from_parts(from_block, to_block, bitmap)
    }

    /// Create an `AggregateBloom` from a compressed bitmap.
    pub fn from_existing_compressed(
        from_block: BlockNumber,
        to_block: BlockNumber,
        compressed_bitmap: Vec<u8>,
    ) -> Self {
        let bitmap = zstd::bulk::decompress(
            &compressed_bitmap,
            Self::BLOCK_RANGE_BYTES * BloomFilter::BITVEC_BITS,
        )
        .expect("Decompressing aggregate Bloom filter");

        Self::from_parts(from_block, to_block, bitmap)
    }

    fn from_parts(from_block: BlockNumber, to_block: BlockNumber, bitmap: Vec<u8>) -> Self {
        assert_eq!(from_block + AGGREGATE_BLOOM_BLOCK_RANGE_LEN - 1, to_block);
        assert_eq!(
            bitmap.len(),
            Self::BLOCK_RANGE_BYTES * BloomFilter::BITVEC_BITS
        );

        Self {
            bitmap,
            from_block,
            to_block,
        }
    }

    /// Compress the bitmap of the aggregate Bloom filter.
    pub fn compress_bitmap(&self) -> Vec<u8> {
        zstd::bulk::compress(&self.bitmap, 10).expect("Compressing aggregate Bloom filter")
    }

    /// Rotate the [`BloomFilter`] by 90 degrees (transpose) and add it to the
    /// aggregate. It is up to the user to keep track of when the aggregate
    /// filter's block range has been exhausted and respond accordingly.
    ///
    /// # Panics
    ///
    /// Panics if the block number is not in the range of blocks that this
    /// aggregate covers.
    pub fn insert(&mut self, bloom: BloomFilter, block_number: BlockNumber) {
        assert!(
            (self.from_block..=self.to_block).contains(&block_number),
            "Block number {} is not in the range {}..={}",
            block_number,
            self.from_block,
            self.to_block
        );

        let bloom_bytes = bloom.into_bytes();

        let relative_block_number = usize::try_from(block_number.get() - self.from_block.get())
            .expect("usize can fit a u64");

        // Column in the bitmap.
        let byte_idx = relative_block_number / 8;
        // Block number offset within a bitmap byte.
        let bit_idx = relative_block_number % 8;

        bloom_bytes
            .into_iter()
            .enumerate()
            .filter(|(_, b)| *b != 0)
            .for_each(|(i, bloom_byte)| {
                let row_idx_base = 8 * i;

                // Each bit (possible key index) in the Bloom filter has its own row.
                for offset in 0..8 {
                    let row_idx = (row_idx_base + offset) * Self::BLOCK_RANGE_BYTES;
                    let bitmap_idx = row_idx + byte_idx;
                    // Reverse the offsets so that the most significant bit is considered as the
                    // first.
                    let bit = (bloom_byte >> (7 - offset)) & 1;
                    self.bitmap[bitmap_idx] |= bit << (7 - bit_idx);
                }
            });
    }

    /// Returns a [bit array](BlockRange) where each bit position represents an
    /// offset from the [starting block][Self::from_block] of the aggregate
    /// filter. If the bit is set, this block contains one of the gives keys.
    /// False positives are possible.
    ///
    /// See [BlockRange::iter_ones].
    pub fn blocks_for_keys(&self, keys: &[Felt]) -> BlockRange {
        if keys.is_empty() {
            return BlockRange::FULL;
        }

        let mut block_matches = BlockRange::EMPTY;

        for k in keys {
            let mut matches_for_key = BlockRange::FULL;

            let indices = BloomFilter::indices_for_key(k);
            for row_idx in indices {
                let row_start = row_idx * Self::BLOCK_RANGE_BYTES;
                let row_end = row_start + Self::BLOCK_RANGE_BYTES;

                let block_range = BlockRange::copy_from_slice(&self.bitmap[row_start..row_end]);

                matches_for_key &= block_range;
            }

            block_matches |= matches_for_key;
        }

        block_matches
    }
}

impl std::fmt::Debug for AggregateBloom {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AggregateBloom")
            .field("from_block", &self.from_block)
            .field("to_block", &self.to_block)
            .field("bitmap_hash", &"...")
            .finish()
    }
}

/// A [`AGGREGATE_BLOOM_BLOCK_RANGE_LEN`] sized bit array. Each bit represents
/// an offset from the starting block of an [`AggregateBloom`].
///
/// Intended use is for return values of functions that check presence of keys
/// inside an [`AggregateBloom`] filter. If a bit at position N is set, then the
/// `aggregate_blom.from_block + N` [block number](BlockNumber) contains the
/// given key. False positives are possible.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct BlockRange([u8; AggregateBloom::BLOCK_RANGE_BYTES]);

#[allow(dead_code)]
impl BlockRange {
    /// An empty `BlockRange`.
    pub(crate) const EMPTY: BlockRange = BlockRange([u8::MIN; AggregateBloom::BLOCK_RANGE_BYTES]);

    /// A full `BlockRange`.
    pub(crate) const FULL: BlockRange = BlockRange([u8::MAX; AggregateBloom::BLOCK_RANGE_BYTES]);

    /// Create a `BlockRange` from a byte slice.
    ///
    /// # Panics
    ///
    /// Panics if the slice is not of length
    /// [`AggregateBloom::BLOCK_RANGE_BYTES`].
    fn copy_from_slice(s: &[u8]) -> Self {
        assert_eq!(s.len(), AggregateBloom::BLOCK_RANGE_BYTES);
        let mut bytes = [0; AggregateBloom::BLOCK_RANGE_BYTES];
        bytes.copy_from_slice(s);
        Self(bytes)
    }

    /// Set the value of a bit at the given index.
    ///
    /// # Panics
    ///
    /// Panics if the index is out of bounds of the block range.
    fn set(&mut self, idx: usize, value: bool) {
        assert!(idx < AGGREGATE_BLOOM_BLOCK_RANGE_LEN as usize);

        let byte_idx = idx / 8;
        let bit_idx = idx % 8;
        if value {
            self.0[byte_idx] |= 1 << (7 - bit_idx);
        } else {
            self.0[byte_idx] &= !(1 << (7 - bit_idx));
        }
    }

    /// Create an iterator over the indices of bits that are set.
    pub(crate) fn iter_ones(&self) -> impl Iterator<Item = usize> + '_ {
        self.iter_val(true)
    }

    /// Create an iterator over the indices of bits that are not set.
    pub(crate) fn iter_zeros(&self) -> impl Iterator<Item = usize> + '_ {
        self.iter_val(false)
    }

    fn iter_val(&self, val: bool) -> impl Iterator<Item = usize> + '_ {
        self.0
            .iter()
            .enumerate()
            .flat_map(move |(byte_idx, &byte)| {
                (0..8).filter_map(move |bit_idx| {
                    if (byte >> (7 - bit_idx)) & 1 == val as u8 {
                        Some(byte_idx * 8 + bit_idx)
                    } else {
                        None
                    }
                })
            })
    }
}

impl Default for BlockRange {
    fn default() -> Self {
        Self::EMPTY
    }
}

impl std::ops::BitAndAssign for BlockRange {
    fn bitand_assign(&mut self, rhs: Self) {
        for (a, b) in self.0.iter_mut().zip(rhs.0.iter()) {
            *a &= b;
        }
    }
}

impl std::ops::BitAnd for BlockRange {
    type Output = Self;

    fn bitand(mut self, rhs: Self) -> Self::Output {
        self &= rhs;
        self
    }
}

impl std::ops::BitOrAssign for BlockRange {
    fn bitor_assign(&mut self, rhs: Self) {
        for (a, b) in self.0.iter_mut().zip(rhs.0.iter()) {
            *a |= b;
        }
    }
}

impl std::ops::BitOr for BlockRange {
    type Output = Self;

    fn bitor(mut self, rhs: Self) -> Self::Output {
        self |= rhs;
        self
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct CacheKey {
    from_block: BlockNumber,
    to_block: BlockNumber,
}

/// A cache for [`AggregateBloom`] filters. It is very expensive to clone these
/// filters, so we store them in an [`Arc`] and clone it instead.
pub(crate) struct AggregateBloomCache(Mutex<SizedCache<CacheKey, Arc<AggregateBloom>>>);

impl AggregateBloomCache {
    /// Create a new cache with the given size.
    pub fn with_size(size: usize) -> Self {
        Self(Mutex::new(SizedCache::with_size(size)))
    }

    /// Reset the cache. Removes all entries and frees the memory.
    pub fn reset(&self) {
        self.0.lock().unwrap().cache_reset();
    }

    /// Retrieve all [AggregateBloom] filters whose range of blocks overlaps
    /// with the given range.
    pub fn get_many(
        &self,
        from_block: BlockNumber,
        to_block: BlockNumber,
    ) -> Vec<Arc<AggregateBloom>> {
        let from_block = from_block.get();
        let to_block = to_block.get();

        // Align to the nearest lower multiple of BLOCK_RANGE_LEN.
        let from_block_aligned = from_block - from_block % AGGREGATE_BLOOM_BLOCK_RANGE_LEN;
        // Align to the nearest higher multiple of BLOCK_RANGE_LEN, then subtract 1
        // (zero based indexing).
        let to_block_aligned = to_block + AGGREGATE_BLOOM_BLOCK_RANGE_LEN
            - (to_block % AGGREGATE_BLOOM_BLOCK_RANGE_LEN)
            - 1;

        let mut cache = self.0.lock().unwrap();

        (from_block_aligned..=to_block_aligned)
            .step_by(AGGREGATE_BLOOM_BLOCK_RANGE_LEN as usize)
            .map(|from| {
                let to = from + AGGREGATE_BLOOM_BLOCK_RANGE_LEN - 1;
                (
                    BlockNumber::new_or_panic(from),
                    BlockNumber::new_or_panic(to),
                )
            })
            .filter_map(|(from_block, to_block)| {
                let k = CacheKey {
                    from_block,
                    to_block,
                };
                cache.cache_get(&k).map(Arc::clone)
            })
            .collect()
    }

    /// Store the given filters in the cache.
    pub fn set_many(&self, filters: &[Arc<AggregateBloom>]) {
        let mut cache = self.0.lock().unwrap();

        filters.iter().for_each(|filter| {
            let k = CacheKey {
                from_block: filter.from_block,
                to_block: filter.to_block,
            };
            cache.cache_set(k, Arc::clone(filter));
        });
    }
}

// The seed used by the hash functions of the filter.
const SEED: [u8; 32] = [
    0xef, 0x51, 0x88, 0x74, 0xef, 0x08, 0x3d, 0xf6, 0x7d, 0x7a, 0x93, 0xb7, 0xb3, 0x13, 0x1f, 0x87,
    0xd3, 0x26, 0xbd, 0x49, 0xc7, 0x18, 0xcc, 0xe5, 0xd7, 0xe8, 0xa0, 0xdb, 0xea, 0x80, 0x67, 0x52,
];

// Base hash functions used by the Bloom filter.
//
// Computed once from the seed and reused for all Bloom filters.
static SIPS: LazyLock<[siphasher::sip::SipHasher13; 2]> = LazyLock::new(|| {
    let k1 = u64::from_le_bytes(SEED[0..8].try_into().unwrap());
    let k2 = u64::from_le_bytes(SEED[8..16].try_into().unwrap());
    let k3 = u64::from_le_bytes(SEED[16..24].try_into().unwrap());
    let k4 = u64::from_le_bytes(SEED[24..32].try_into().unwrap());
    [
        SipHasher13::new_with_keys(k1, k2),
        SipHasher13::new_with_keys(k3, k4),
    ]
});

/// The number of hash functions used by the Bloom filter.
const K_NUM: usize = 12;

/// A Bloom filter implementation for StarkNet events.
///
/// Based on the Bloom filter implementation from the [`bloomfilter`](https://crates.io/crates/bloomfilter/1.0.15) crate.
#[derive(Clone)]
pub(crate) struct BloomFilter {
    bit_vec: bitvec::vec::BitVec<u8, bitvec::order::Msb0>,
}

impl BloomFilter {
    /// The size of the bitmap used by the Bloom filter.
    const BITVEC_BITS: usize = 16_384;
    /// The size of the bitmap used by the Bloom filter (in bytes).
    const BITVEC_BYTES: usize = Self::BITVEC_BITS / 8;

    /// Crate a new empty bloom filter.
    pub fn new() -> Self {
        let bit_vec = bitvec::bitvec![u8, bitvec::order::Msb0; 0; Self::BITVEC_BITS];
        Self { bit_vec }
    }

    /// Create a bloom filter from a compressed byte array of the bitmap.
    pub fn from_compressed_bytes(bytes: &[u8]) -> Self {
        let bytes = zstd::bulk::decompress(bytes, Self::BITVEC_BYTES * 2)
            .expect("Decompressing Bloom filter");
        let bit_vec = BitVec::from_vec(bytes);

        Self { bit_vec }
    }

    /// Convert the bloom filter to a compressed byte array.
    pub fn into_compressed_bytes(self) -> Vec<u8> {
        let bytes = self.into_bytes();
        zstd::bulk::compress(&bytes, 0).expect("Compressing Bloom filter")
    }

    /// Convert the bloom filter to a byte array.
    fn into_bytes(self) -> Vec<u8> {
        self.bit_vec.into_vec()
    }

    /// Record the presence of an item.
    pub fn set(&mut self, item: &Felt) {
        let mut hashes = [0u64, 0u64];
        for k_i in 0..K_NUM {
            let bit_offset = Self::bloom_hash(&mut hashes, item, k_i) as usize % Self::BITVEC_BITS;
            self.bit_vec.set(bit_offset, true);
        }
    }

    /// Compute the bit indices for the given key.
    fn indices_for_key(key: &Felt) -> [usize; K_NUM] {
        let mut indices = [0usize; K_NUM];
        let mut hashes = [0u64, 0u64];
        let iter = (0..K_NUM)
            .map(|k_i| Self::bloom_hash(&mut hashes, key, k_i) as usize % Self::BITVEC_BITS);
        for (i, idx) in iter.enumerate() {
            indices[i] = idx;
        }
        indices
    }

    fn bloom_hash(hashes: &mut [u64; 2], item: &Felt, k_i: usize) -> u64 {
        if k_i < 2 {
            let sip = &mut SIPS[k_i].clone();
            item.hash(sip);
            let hash = sip.finish();
            hashes[k_i] = hash;
            hash
        } else {
            (hashes[0]).wrapping_add((k_i as u64).wrapping_mul(hashes[1]))
                % 0xFFFF_FFFF_FFFF_FFC5u64 //largest u64 prime
        }
    }
}

#[cfg(test)]
mod tests {
    use pathfinder_common::felt;

    use super::*;

    const KEY: Felt = felt!("0x0218b538681900fad5a0b2ffe1d6781c0c3f14df5d32071ace0bdc9d46cb69ea");
    const KEY1: Felt = felt!("0x0218b538681900fad5a0b2ffe1d6781c0c3f14df5d32071ace0bdc9d46cb69eb");
    const KEY_NOT_IN_FILTER: Felt =
        felt!("0x0218b538681900fad5a0b2ffe1d6781c0c3f14df5d32071ace0bdc9d46cb69ec");

    macro_rules! blockrange {
        ($($block:expr),* $(,)?) => {{
            let mut bits = BlockRange::EMPTY;
            $(
                let idx = $block.get() - BlockNumber::GENESIS.get();
                bits.set(idx as usize, true);
            )*
            bits
        }};
    }

    mod filters {
        use super::*;

        #[test]
        fn add_bloom_and_check_single_block_found() {
            let from_block = BlockNumber::new_or_panic(0);
            let mut aggregate_bloom_filter = AggregateBloom::new(from_block);

            let mut bloom = BloomFilter::new();
            bloom.set(&KEY);
            bloom.set(&KEY1);

            aggregate_bloom_filter.insert(bloom, from_block);

            let block_matches = aggregate_bloom_filter.blocks_for_keys(&[KEY]);
            let expected = blockrange![from_block];
            assert_eq!(block_matches, expected);
        }

        #[test]
        fn add_blooms_and_check_multiple_blocks_found() {
            let from_block = BlockNumber::new_or_panic(0);
            let mut aggregate_bloom_filter = AggregateBloom::new(from_block);

            let mut bloom = BloomFilter::new();
            bloom.set(&KEY);

            aggregate_bloom_filter.insert(bloom.clone(), from_block);
            aggregate_bloom_filter.insert(bloom, from_block + 1);

            let block_matches = aggregate_bloom_filter.blocks_for_keys(&[KEY]);
            let expected = blockrange![from_block, from_block + 1];
            assert_eq!(block_matches, expected);
        }

        #[test]
        fn key_not_in_filter_returns_empty_vec() {
            let from_block = BlockNumber::new_or_panic(0);
            let mut aggregate_bloom_filter = AggregateBloom::new(from_block);

            let mut bloom = BloomFilter::new();
            bloom.set(&KEY);
            bloom.set(&KEY1);

            aggregate_bloom_filter.insert(bloom.clone(), from_block);
            aggregate_bloom_filter.insert(bloom, from_block + 1);

            let block_matches_empty = aggregate_bloom_filter.blocks_for_keys(&[KEY_NOT_IN_FILTER]);
            assert_eq!(block_matches_empty, BlockRange::EMPTY);
        }

        #[test]
        fn serialize_aggregate_roundtrip() {
            let from_block = BlockNumber::new_or_panic(0);
            let mut aggregate_bloom_filter = AggregateBloom::new(from_block);

            let mut bloom = BloomFilter::new();
            bloom.set(&KEY);

            aggregate_bloom_filter.insert(bloom.clone(), from_block);
            aggregate_bloom_filter.insert(bloom.clone(), from_block + 1);

            let compressed_bitmap = aggregate_bloom_filter.compress_bitmap();
            let mut decompressed = AggregateBloom::from_existing_compressed(
                aggregate_bloom_filter.from_block,
                aggregate_bloom_filter.to_block,
                compressed_bitmap,
            );
            decompressed.insert(bloom, from_block + 2);

            let block_matches = decompressed.blocks_for_keys(&[KEY]);
            let expected = blockrange![from_block, from_block + 1, from_block + 2];
            assert_eq!(block_matches, expected);

            let block_matches_empty = decompressed.blocks_for_keys(&[KEY_NOT_IN_FILTER]);
            assert_eq!(block_matches_empty, BlockRange::EMPTY);
        }

        #[test]
        #[should_panic]
        fn invalid_insert_pos() {
            let from_block = BlockNumber::new_or_panic(0);
            let mut aggregate_bloom_filter = AggregateBloom::new(from_block);

            let mut bloom = BloomFilter::new();
            bloom.set(&KEY);

            aggregate_bloom_filter.insert(bloom.clone(), from_block);

            let invalid_insert_pos = from_block + AGGREGATE_BLOOM_BLOCK_RANGE_LEN;
            aggregate_bloom_filter.insert(bloom, invalid_insert_pos);
        }
    }

    mod cache {
        use super::*;

        // Tests only use ranges so no need to compare bitmap.
        impl PartialEq for AggregateBloom {
            fn eq(&self, other: &Self) -> bool {
                self.from_block == other.from_block && self.to_block == other.to_block
            }
        }

        #[test]
        fn set_then_get_many_aligned() {
            let cache = AggregateBloomCache::with_size(3);

            let range_start1 = BlockNumber::GENESIS;
            let range_start2 = BlockNumber::GENESIS + AGGREGATE_BLOOM_BLOCK_RANGE_LEN;
            let range_start3 = BlockNumber::GENESIS + 2 * AGGREGATE_BLOOM_BLOCK_RANGE_LEN;

            let range_end2 = range_start2 + AGGREGATE_BLOOM_BLOCK_RANGE_LEN - 1;

            let filters = vec![
                Arc::new(AggregateBloom::new(range_start1)),
                Arc::new(AggregateBloom::new(range_start2)),
                Arc::new(AggregateBloom::new(range_start3)),
            ];

            cache.set_many(&filters);

            let retrieved = cache.get_many(range_start1, range_end2);

            assert_eq!(retrieved, filters[0..2]);
        }

        #[test]
        fn set_then_get_many_unaligned() {
            let cache = AggregateBloomCache::with_size(3);

            let range_start1 = BlockNumber::GENESIS;
            let range_start2 = BlockNumber::GENESIS + AGGREGATE_BLOOM_BLOCK_RANGE_LEN;
            let range_start3 = BlockNumber::GENESIS + 2 * AGGREGATE_BLOOM_BLOCK_RANGE_LEN;

            let filters = vec![
                Arc::new(AggregateBloom::new(range_start1)),
                Arc::new(AggregateBloom::new(range_start2)),
                Arc::new(AggregateBloom::new(range_start3)),
            ];

            let start = range_start2 + 15;
            let end = range_start3 + 15;

            cache.set_many(&filters);

            let retrieved = cache.get_many(start, end);

            assert_eq!(retrieved, &filters[1..3]);
        }

        #[test]
        fn filters_outside_of_range_not_returned() {
            let cache = AggregateBloomCache::with_size(4);

            let filters = vec![
                Arc::new(AggregateBloom::new(BlockNumber::GENESIS)),
                Arc::new(AggregateBloom::new(
                    BlockNumber::GENESIS + AGGREGATE_BLOOM_BLOCK_RANGE_LEN,
                )),
                Arc::new(AggregateBloom::new(
                    BlockNumber::GENESIS + 2 * AGGREGATE_BLOOM_BLOCK_RANGE_LEN,
                )),
                Arc::new(AggregateBloom::new(
                    BlockNumber::GENESIS + 3 * AGGREGATE_BLOOM_BLOCK_RANGE_LEN,
                )),
            ];

            cache.set_many(&filters);

            let range_start1 = BlockNumber::GENESIS;
            let range_end2 = BlockNumber::GENESIS + 2 * AGGREGATE_BLOOM_BLOCK_RANGE_LEN - 1;

            let retrieved = cache.get_many(range_start1, range_end2);

            assert_eq!(retrieved, filters[0..2].to_vec());
        }

        #[test]
        fn cache_edge_cases() {
            let cache = AggregateBloomCache::with_size(2);

            let first_range_start = BlockNumber::GENESIS;
            let first_range_end = first_range_start + AGGREGATE_BLOOM_BLOCK_RANGE_LEN - 1;
            let second_range_start = first_range_end + 1;
            let second_range_end = second_range_start + AGGREGATE_BLOOM_BLOCK_RANGE_LEN - 1;

            let filters = vec![
                Arc::new(AggregateBloom::new(first_range_start)),
                Arc::new(AggregateBloom::new(second_range_start)),
            ];

            cache.set_many(&filters);

            // Edge cases around the lower bound.
            let retrieved = cache.get_many(first_range_end - 1, second_range_end);
            assert_eq!(retrieved, filters[..]);
            let retrieved = cache.get_many(first_range_end, second_range_end);
            assert_eq!(retrieved, filters[..]);
            let retrieved = cache.get_many(first_range_end + 1, second_range_end);
            assert_eq!(retrieved, filters[1..]);

            // Edge cases around the upper bound.
            let retrieved = cache.get_many(first_range_start, first_range_end - 1);
            assert_eq!(retrieved, filters[0..1]);
            let retrieved = cache.get_many(first_range_start, first_range_end);
            assert_eq!(retrieved, filters[0..1]);
            let retrieved = cache.get_many(first_range_start, first_range_end + 1);
            assert_eq!(retrieved, filters[0..=1]);
        }
    }
}
