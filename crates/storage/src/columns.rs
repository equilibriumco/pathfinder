use rust_rocksdb::DataBlockIndexType;

#[derive(Clone, Debug)]
pub(crate) struct Column {
    pub name: &'static str,
    key_prefix_length: Option<usize>,
    point_lookup: bool,
    optimize_for_hits: bool,
}

impl Column {
    pub const fn new(name: &'static str) -> Self {
        Self {
            name,
            key_prefix_length: None,
            point_lookup: false,
            optimize_for_hits: false,
        }
    }

    pub fn options(&self, cache: &rust_rocksdb::Cache) -> rust_rocksdb::Options {
        let mut options = rust_rocksdb::Options::default();

        let mut block_based_options = rust_rocksdb::BlockBasedOptions::default();
        block_based_options.set_block_cache(&cache);
        block_based_options.set_cache_index_and_filter_blocks(true);
        block_based_options.set_pin_l0_filter_and_index_blocks_in_cache(true);
        block_based_options.set_ribbon_filter(10.0);

        options.optimize_level_style_compaction(512 * 1024 * 1024);
        // options.set_bottommost_compression_type(DBCompressionType::Zstd);

        if self.point_lookup {
            block_based_options.set_block_size(4096);
            block_based_options.set_data_block_index_type(DataBlockIndexType::BinaryAndHash);
            block_based_options.set_data_block_hash_ratio(0.75);
            block_based_options.set_whole_key_filtering(true);
        }

        if let Some(prefix_length) = self.key_prefix_length {
            options.set_prefix_extractor(rust_rocksdb::SliceTransform::create_fixed_prefix(
                prefix_length,
            ));
        }

        if self.optimize_for_hits {
            options.set_optimize_filters_for_hits(true);
        }

        options.set_block_based_table_factory(&block_based_options);

        options
    }

    pub const fn with_prefix_length(self, prefix_length: usize) -> Self {
        Self {
            key_prefix_length: Some(prefix_length),
            ..self
        }
    }

    pub const fn with_point_lookup(self) -> Self {
        Self {
            point_lookup: true,
            ..self
        }
    }

    pub const fn with_optimize_for_hits(self) -> Self {
        Self {
            optimize_for_hits: true,
            ..self
        }
    }
}

pub(crate) const COLUMNS: &[Column] = &[
    crate::connection::TRIE_CLASS_COLUMN,
    crate::connection::TRIE_CONTRACT_COLUMN,
    crate::connection::TRIE_STORAGE_COLUMN,
    crate::connection::TRIE_NEXT_INDEX_COLUMN,
    crate::connection::STATE_UPDATES_COLUMN,
    crate::connection::STORAGE_UPDATES_COLUMN,
    crate::connection::NONCE_UPDATES_COLUMN,
    crate::connection::TRANSACTIONS_AND_RECEIPTS_COLUMN,
    crate::connection::EVENTS_COLUMN,
    crate::connection::TRANSACTION_HASHES_COLUMN,
    crate::connection::CONTRACT_STATE_HASHES_COLUMN,
];
