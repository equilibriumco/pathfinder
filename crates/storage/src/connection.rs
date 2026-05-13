use std::sync::{Arc, Mutex};

mod block;
mod class;
pub mod dto;
mod ethereum;
pub mod event;
pub mod pruning;
mod reference;
mod signature;
pub(crate) mod state_update;
pub(crate) mod transaction;
mod trie;

use event::RunningEventFilter;
pub use event::{
    EmittedEvent,
    EventConstraints,
    EventFilterError,
    PageOfEvents,
    PAGE_SIZE_LIMIT as EVENT_PAGE_SIZE_LIMIT,
};
use pathfinder_common::event::Event;
use pathfinder_common::receipt::Receipt;
use pathfinder_common::transaction::Transaction as StarknetTransaction;
use pathfinder_common::{BlockNumber, TransactionHash, TransactionIndex};
use pruning::BlockchainHistoryMode;
// Re-export this so users don't require rusqlite as a direct dep.
pub use rusqlite::TransactionBehavior;
pub(crate) use state_update::{NONCE_UPDATES_COLUMN, STATE_UPDATES_COLUMN, STORAGE_UPDATES_COLUMN};
pub(crate) use transaction::{
    EVENTS_COLUMN,
    TRANSACTIONS_AND_RECEIPTS_COLUMN,
    TRANSACTION_HASHES_COLUMN,
};
pub use trie::{Node, NodeRef, RootIndexUpdate, StoredNode, TrieStorageIndex, TrieUpdate};
pub(crate) use trie::{
    CONTRACT_STATE_HASHES_COLUMN,
    TRIE_CLASS_COLUMN,
    TRIE_CONTRACT_COLUMN,
    TRIE_NEXT_INDEX_COLUMN,
    TRIE_STORAGE_COLUMN,
};

use crate::bloom::AggregateBloomCache;
use crate::params::RowExt;
use crate::{RocksDB, RocksDBInner, StorageError, VERSION_KEY};

type PooledConnection = r2d2::PooledConnection<r2d2_sqlite::SqliteConnectionManager>;

pub struct Connection {
    connection: PooledConnection,
    rocksdb: Arc<RocksDBInner>,
    event_filter_cache: Arc<AggregateBloomCache>,
    running_event_filter: Arc<Mutex<RunningEventFilter>>,
    trie_prune_mode: TriePruneMode,
    pub blockchain_history_mode: BlockchainHistoryMode,
}

impl Connection {
    pub(crate) fn new(
        connection: PooledConnection,
        rocksdb: Arc<RocksDBInner>,
        event_filter_cache: Arc<AggregateBloomCache>,
        running_event_filter: Arc<Mutex<RunningEventFilter>>,
        trie_prune_mode: TriePruneMode,
        blockchain_history_mode: BlockchainHistoryMode,
    ) -> Self {
        Self {
            connection,
            rocksdb,
            event_filter_cache,
            running_event_filter,
            trie_prune_mode,
            blockchain_history_mode,
        }
    }

    pub fn transaction(&mut self) -> Result<Transaction<'_>, StorageError> {
        let tx = self.connection.transaction()?;
        Ok(Transaction {
            transaction: tx,
            rocksdb: Arc::clone(&self.rocksdb),
            batch: Mutex::new(crate::RocksDBBatch::default()),
            event_filter_cache: self.event_filter_cache.clone(),
            running_event_filter: self.running_event_filter.clone(),
            trie_prune_mode: self.trie_prune_mode,
            blockchain_history_mode: self.blockchain_history_mode,
        })
    }

    pub fn transaction_with_behavior(
        &mut self,
        behavior: TransactionBehavior,
    ) -> Result<Transaction<'_>, StorageError> {
        let tx = self.connection.transaction_with_behavior(behavior)?;
        Ok(Transaction {
            transaction: tx,
            rocksdb: Arc::clone(&self.rocksdb),
            batch: Mutex::new(crate::RocksDBBatch::default()),
            event_filter_cache: self.event_filter_cache.clone(),
            running_event_filter: self.running_event_filter.clone(),
            trie_prune_mode: self.trie_prune_mode,
            blockchain_history_mode: self.blockchain_history_mode,
        })
    }

    pub fn with_retry(self) -> anyhow::Result<Self> {
        self.connection.busy_handler(Some(|_| {
            std::thread::sleep(std::time::Duration::from_millis(10));
            true
        }))?;
        Ok(self)
    }
}

pub struct Transaction<'inner> {
    transaction: rusqlite::Transaction<'inner>,
    rocksdb: Arc<super::RocksDBInner>,
    batch: Mutex<crate::RocksDBBatch>,
    event_filter_cache: Arc<AggregateBloomCache>,
    running_event_filter: Arc<Mutex<RunningEventFilter>>,
    trie_prune_mode: TriePruneMode,
    pub blockchain_history_mode: BlockchainHistoryMode,
}

#[derive(Debug, Clone, Copy)]
pub enum TriePruneMode {
    /// Keep all merkle trie history.
    Archive,
    /// Prune merkle trie history. Only keep the last few blocks, as well as the
    /// latest block.
    Prune { num_blocks_kept: u64 },
}

type TransactionWithReceipt = (StarknetTransaction, Receipt, Vec<Event>, BlockNumber);

type TransactionDataForBlock = (StarknetTransaction, Receipt, Vec<Event>);

type EventsForBlock = ((TransactionHash, TransactionIndex), Vec<Event>);

impl Transaction<'_> {
    // The implementations here are intentionally kept as simple wrappers. This lets
    // the real implementations be kept in separate files with more reasonable
    // LOC counts and easier test oversight.

    pub(crate) fn inner(&self) -> &rusqlite::Transaction<'_> {
        &self.transaction
    }

    pub(crate) fn rocksdb(&self) -> &RocksDB {
        &self.rocksdb.rocksdb
    }

    pub(crate) fn rocksdb_get_column(
        &self,
        column: &crate::columns::Column,
    ) -> Arc<rust_rocksdb::BoundColumnFamily<'_>> {
        self.rocksdb.get_column(column)
    }

    pub fn commit(self) -> anyhow::Result<()> {
        let transaction = self.transaction;
        let rocksdb = self.rocksdb;
        let batch = self.batch.into_inner().expect("Batch lock poisoned");
        rocksdb.rocksdb.write(&batch)?;
        Ok(transaction.commit()?)
    }

    pub fn trie_pruning_enabled(&self) -> bool {
        matches!(self.trie_prune_mode, TriePruneMode::Prune { .. })
    }

    pub fn blockchain_pruning_enabled(&self) -> bool {
        matches!(
            self.blockchain_history_mode,
            BlockchainHistoryMode::Prune { .. }
        )
    }

    /// Store the in-memory [`Storage`](crate::Storage) state in the database.
    /// To be performed on shutdown.
    pub fn store_in_memory_state(self) -> anyhow::Result<()> {
        self.store_running_event_filter()?.commit()
    }

    /// Resets the in-memory [`Storage`](crate::Storage) state. Required after
    /// each reorg.
    pub fn reset_in_memory_state(&self, head: BlockNumber) -> anyhow::Result<()> {
        self.event_filter_cache.reset();
        self.rebuild_running_event_filter(head)
    }

    pub fn user_version(&self) -> anyhow::Result<i64> {
        let user_version = self
            .transaction
            .pragma_query_value(None, VERSION_KEY, |row| row.get_i64(0))?;
        Ok(user_version)
    }
}
