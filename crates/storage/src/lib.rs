//! Local storage.
//!
//! SQLite for relational data and RocksDB for key-value data.

// This is intended for internal use only -- do not make public.
mod prelude;

mod bloom;
use bloom::AggregateBloomCache;
pub use bloom::AGGREGATE_BLOOM_BLOCK_RANGE_LEN;
mod columns;
use connection::pruning::BlockchainHistoryMode;
use connection::TrieColumn;
mod connection;
mod error;
pub mod fake;
mod params;
mod schema;
pub use schema::revision_0073::reorg_regression_checks;
pub mod test_utils;

use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::Context;
pub use connection::*;
pub use dto::MinimalFelt;
pub use error::StorageError;
use event::RunningEventFilter;
pub use event::EVENT_KEY_FILTER_LIMIT;
use pathfinder_common::BlockNumber;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{OpenFlags, OptionalExtension};
use rust_rocksdb::ColumnFamilyDescriptor;
pub use transaction::dto::{
    DataAvailabilityMode,
    DeclareTransactionV4,
    DeployAccountTransactionV4,
    InvokeTransactionV5,
    L1HandlerTransactionV0,
    ResourceBound,
    ResourceBoundsV1,
    TransactionV3,
};

use crate::columns::Column;
use crate::params::{RowExt, TryIntoSqlInt};

/// Sqlite key used for the PRAGMA user version.
const VERSION_KEY: &str = "user_version";

type RocksDB = rust_rocksdb::DBWithThreadMode<rust_rocksdb::MultiThreaded>;
type RocksDBBatch = rust_rocksdb::WriteBatchWithTransaction<false>;

/// Specifies the [journal mode](https://sqlite.org/pragma.html#pragma_journal_mode)
/// of the [Storage].
#[derive(Clone, Copy, Debug)]
pub enum JournalMode {
    Rollback,
    WAL,
}

/// Used to create [Connection's](Connection) to the pathfinder database.
///
/// Intended usage:
/// - Use [StorageBuilder] to create the app's database.
/// - Pass the [Storage] (or clones thereof) to components which require
///   database access.
/// - Use [Storage::connection] to create connection's to the database, which
///   can in turn be used to interact with the various [tables](self).
#[derive(Clone)]
pub struct Storage(Inner);

#[derive(Clone)]
struct Inner {
    /// Uses [`Arc`] to allow _shallow_ [Storage] cloning
    database_path: Arc<PathBuf>,
    pool: Pool<SqliteConnectionManager>,
    rocksdb: Arc<RocksDBInner>,
    event_filter_cache: Arc<AggregateBloomCache>,
    running_event_filter: Arc<Mutex<RunningEventFilter>>,
    trie_prune_mode: TriePruneMode,
    blockchain_history_mode: BlockchainHistoryMode,
}

pub(crate) struct RocksDBInner {
    rocksdb: RocksDB,
    options: rust_rocksdb::Options,
    trie_class_next_index: std::sync::atomic::AtomicU64,
    trie_contract_next_index: std::sync::atomic::AtomicU64,
    trie_storage_next_index: std::sync::atomic::AtomicU64,
    /// Owns the tempdir that holds the RocksDB files for ephemeral storages
    /// (in-memory SQLite or tempdir-hosted); `None` only for durable
    /// databases. Must remain the last field so `rocksdb` (and its
    /// background threads) drop before the directory is unlinked.
    /// `in_memory_storage_cleans_up_rocksdb_tempdir` guards that invariant.
    _tempdir: Option<tempfile::TempDir>,
}

impl RocksDBInner {
    fn next_trie_storage_index(
        &self,
        column: TrieColumn,
        number_of_indices_to_allocate: usize,
    ) -> TrieStorageIndex {
        let counter = self.trie_next_index_atomic(column);
        let next_index = counter.fetch_add(
            number_of_indices_to_allocate as u64,
            std::sync::atomic::Ordering::SeqCst,
        );
        TrieStorageIndex::new(next_index).expect("TrieStorageIndex counter exceeded i64::MAX")
    }

    fn trie_next_index_atomic(&self, column: TrieColumn) -> &std::sync::atomic::AtomicU64 {
        match column {
            TrieColumn::Class => &self.trie_class_next_index,
            TrieColumn::Contract => &self.trie_contract_next_index,
            TrieColumn::Storage => &self.trie_storage_next_index,
        }
    }

    /// Overwrites the in-memory `next_index` atomic for a trie CF. Used by the
    /// startup reconcile to re-seed the counter after rewinding to the
    /// confirmed tail or after a fresh-install migration.
    fn store_next_index(&self, column: TrieColumn, value: u64) {
        self.trie_next_index_atomic(column)
            .store(value, std::sync::atomic::Ordering::SeqCst);
    }

    fn get_column(&self, column: &Column) -> Arc<rust_rocksdb::BoundColumnFamily<'_>> {
        self.rocksdb
            .cf_handle(column.name)
            .expect("RocksDB column family missing")
    }

    fn log_stats(&self) {
        let stats = self.options.get_statistics();
        if let Some(stats) = stats {
            tracing::debug!(%stats, "RocksDB statistics");
        }
    }
}

/// Startup pruning deferred until after the connection pool is ready. Populated
/// when the node restarts with a smaller `num_blocks_kept` than before.
struct PendingPrune {
    oldest: u64,
    num_blocks_to_remove: u64,
}

pub struct StorageManager {
    database_path: PathBuf,
    journal_mode: JournalMode,
    rocksdb: Arc<RocksDBInner>,
    event_filter_cache: Arc<AggregateBloomCache>,
    running_event_filter: Arc<Mutex<RunningEventFilter>>,
    trie_prune_mode: TriePruneMode,
    blockchain_history_mode: BlockchainHistoryMode,
    pending_prune: Option<PendingPrune>,
}

pub struct ReadOnlyStorageManager(StorageManager);

impl std::fmt::Debug for StorageManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageManager")
            .field("database_path", &self.database_path)
            .field("journal_mode", &self.journal_mode)
            .field("trie_prune_mode", &self.trie_prune_mode)
            .finish()
    }
}

impl StorageManager {
    fn build_pool(&self, capacity: NonZeroU32, open_flags: OpenFlags) -> anyhow::Result<Storage> {
        let journal_mode = self.journal_mode;
        let pool_manager = SqliteConnectionManager::file(&self.database_path)
            .with_flags(open_flags)
            .with_init(move |connection| setup_connection(connection, journal_mode));
        let pool = Pool::builder()
            .max_size(capacity.get())
            .build(pool_manager)?;

        Ok(Storage(Inner {
            database_path: Arc::new(self.database_path.clone()),
            pool,
            rocksdb: Arc::clone(&self.rocksdb),
            event_filter_cache: self.event_filter_cache.clone(),
            running_event_filter: self.running_event_filter.clone(),
            trie_prune_mode: self.trie_prune_mode,
            blockchain_history_mode: self.blockchain_history_mode,
        }))
    }

    fn apply_pending_prune(&mut self, storage: &Storage) -> anyhow::Result<()> {
        let Some(pending) = self.pending_prune.as_ref() else {
            return Ok(());
        };
        let mut connection = storage.connection().context("Getting storage connection")?;
        let tx = connection
            .transaction()
            .context("Creating storage transaction")?;
        for block in pending.oldest..(pending.oldest + pending.num_blocks_to_remove) {
            let block = BlockNumber::new_or_panic(block);
            tx.prune_block(block)
                .with_context(|| format!("Pruning block {block}"))?;
        }
        tx.commit().context("Committing prune transaction")?;
        self.pending_prune.take();
        Ok(())
    }

    pub fn create_pool(&mut self, capacity: NonZeroU32) -> anyhow::Result<Storage> {
        let storage = self.build_pool(capacity, OpenFlags::default())?;
        self.apply_pending_prune(&storage)?;
        Ok(storage)
    }

    pub fn create_read_only_pool(&self, capacity: NonZeroU32) -> anyhow::Result<Storage> {
        let flags = OpenFlags::SQLITE_OPEN_READ_ONLY
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_URI;
        self.build_pool(capacity, flags)
    }
}

impl ReadOnlyStorageManager {
    pub fn create_read_only_pool(&self, capacity: NonZeroU32) -> anyhow::Result<Storage> {
        self.0.create_read_only_pool(capacity)
    }
}

pub struct StorageBuilder {
    database_path: PathBuf,
    journal_mode: JournalMode,
    event_filter_cache_size: usize,
    trie_prune_mode: Option<TriePruneMode>,
    blockchain_history_mode: Option<BlockchainHistoryMode>,
    /// Preassigned tempdir to own the RocksDB directory for ephemeral
    /// storages; consumed by `migrate()` / `readonly()`.
    rocksdb_tempdir: Option<tempfile::TempDir>,
}

impl StorageBuilder {
    pub fn file(database_path: PathBuf) -> Self {
        Self {
            database_path,
            journal_mode: JournalMode::WAL,
            event_filter_cache_size: 16,
            trie_prune_mode: None,
            blockchain_history_mode: None,
            rocksdb_tempdir: None,
        }
    }

    pub fn journal_mode(mut self, journal_mode: JournalMode) -> Self {
        self.journal_mode = journal_mode;
        self
    }

    pub fn event_filter_cache_size(mut self, event_filter_cache_size: usize) -> Self {
        self.event_filter_cache_size = event_filter_cache_size;
        self
    }

    pub fn trie_prune_mode(mut self, trie_prune_mode: Option<TriePruneMode>) -> Self {
        self.trie_prune_mode = trie_prune_mode;
        self
    }

    pub fn blockchain_history_mode(
        mut self,
        blockchain_history_mode: Option<BlockchainHistoryMode>,
    ) -> Self {
        self.blockchain_history_mode = blockchain_history_mode;
        self
    }

    /// Preassign the tempdir that will own the RocksDB directory. Only the
    /// crate-local ephemeral constructors call this.
    fn rocksdb_tempdir(mut self, tempdir: tempfile::TempDir) -> Self {
        self.rocksdb_tempdir = Some(tempdir);
        self
    }

    /// Picks the RocksDB directory to open. In-memory SQLite URIs must be
    /// paired with a preassigned tempdir; on-disk paths derive the RocksDB
    /// directory from the SQLite path and pass any preassigned tempdir
    /// through untouched.
    fn resolve_rocksdb_location(
        database_path: &Path,
        tempdir: Option<tempfile::TempDir>,
        missing_tempdir_msg: &'static str,
    ) -> anyhow::Result<(PathBuf, Option<tempfile::TempDir>)> {
        let sqlite_is_in_memory = database_path
            .to_str()
            .is_some_and(|s| s.starts_with("file:memdb"));
        if sqlite_is_in_memory {
            let tempdir = tempdir.context(missing_tempdir_msg)?;
            Ok((tempdir.path().to_path_buf(), Some(tempdir)))
        } else {
            Ok((database_path.with_extension("rocksdb"), tempdir))
        }
    }

    /// Convenience function for tests to create an in-memory database.
    pub fn in_memory() -> anyhow::Result<Storage> {
        Self::in_memory_with_trie_pruning(TriePruneMode::Archive)
    }

    /// Convenience function for tests to create an in-memory database with a
    /// specific trie prune mode.
    ///
    /// Note that most of the time we _do_ want to use a pool size of 1. We're
    /// using shared cache mode with our in-memory DB to allow multiple
    /// connections from within the same process. This means that in
    /// contrast to a file-based DB we immediately get locking errors in
    /// case of concurrent writes -- a pool size of one avoids this.
    pub fn in_memory_with_trie_pruning(trie_prune_mode: TriePruneMode) -> anyhow::Result<Storage> {
        Self::in_memory_with_trie_pruning_and_pool_size(
            trie_prune_mode,
            NonZeroU32::new(1).unwrap(),
        )
    }

    /// Convenience function for tests to create an in-memory database with a
    /// specific trie prune mode.
    pub fn in_memory_with_trie_pruning_and_pool_size(
        trie_prune_mode: TriePruneMode,
        pool_size: NonZeroU32,
    ) -> anyhow::Result<Storage> {
        // Create a unique database name so that they are not shared between
        // concurrent tests. i.e. Make every in-mem Storage unique.
        static COUNT: std::sync::Mutex<u64> = std::sync::Mutex::new(0);
        let unique_mem_db = {
            let mut count = COUNT.lock().unwrap();
            // &cache=shared allows other threads to see and access the inmemory database
            let unique_mem_db = format!("file:memdb{count}?mode=memory&cache=shared");
            *count += 1;
            unique_mem_db
        };

        let database_path = PathBuf::from(unique_mem_db);
        // This connection must be held until a pool has been created, since an
        // in-memory database is dropped once all its connections are. This connection
        // therefore holds the database in-place until the pool is established.
        let conn = rusqlite::Connection::open(&database_path)?;
        let rocksdb_tempdir =
            tempfile::tempdir().context("Creating RocksDB tempdir for in-memory database")?;

        let mut storage = Self::file(database_path)
            .journal_mode(JournalMode::Rollback)
            .rocksdb_tempdir(rocksdb_tempdir)
            .migrate()?;

        if let TriePruneMode::Prune { .. } = trie_prune_mode {
            conn.execute(
                "INSERT INTO storage_options (option) VALUES ('prune_tries')",
                [],
            )?;
        }

        storage.trie_prune_mode = trie_prune_mode;
        storage.create_pool(pool_size)
    }

    pub fn in_memory_with_blockchain_pruning_and_pool_size(
        blockchain_history_mode: BlockchainHistoryMode,
        pool_size: NonZeroU32,
    ) -> anyhow::Result<Storage> {
        // Create a unique database name so that they are not shared between
        // concurrent tests. i.e. Make every in-mem Storage unique.
        static COUNT: std::sync::Mutex<u64> = std::sync::Mutex::new(0);
        let unique_mem_db = {
            let mut count = COUNT.lock().unwrap();
            // &cache=shared allows other threads to see and access the inmemory database
            let unique_mem_db = format!("file:memdb{count}?mode=memory&cache=shared");
            *count += 1;
            unique_mem_db
        };

        let database_path = PathBuf::from(unique_mem_db);
        // This connection must be held until a pool has been created, since an
        // in-memory database is dropped once all its connections are. This connection
        // therefore holds the database in-place until the pool is established.
        let conn = rusqlite::Connection::open(&database_path)?;
        let rocksdb_tempdir =
            tempfile::tempdir().context("Creating RocksDB tempdir for in-memory database")?;

        let mut storage = Self::file(database_path)
            .journal_mode(JournalMode::Rollback)
            .rocksdb_tempdir(rocksdb_tempdir)
            .migrate()?;

        if let BlockchainHistoryMode::Prune { num_blocks_kept } = blockchain_history_mode {
            conn.execute(
                "INSERT INTO storage_options (option, value) VALUES ('prune_blockchain', ?)",
                [num_blocks_kept.try_into_sql_int()?],
            )?;
        }

        storage.blockchain_history_mode = blockchain_history_mode;
        storage.create_pool(pool_size)
    }

    /// A workaround for scenarios where a test requires multiple parallel
    /// connections and shared cache causes locking errors if the connection
    /// pool is larger than 1 and timeouts otherwise.
    pub fn in_tempdir() -> anyhow::Result<Storage> {
        let tempdir = tempfile::tempdir()?;
        tracing::trace!("Creating storage in: {}", tempdir.path().display());
        let db_path = tempdir.path().join("db.sqlite");
        let mut manager = crate::StorageBuilder::file(db_path)
            .rocksdb_tempdir(tempdir)
            .migrate()
            .unwrap();
        manager.create_pool(NonZeroU32::new(32).unwrap())
    }

    /// Convenience function for tests to create an in-tempdir database with a
    /// specific trie prune mode.
    pub fn in_tempdir_with_trie_pruning_and_pool_size(
        trie_prune_mode: TriePruneMode,
        pool_size: NonZeroU32,
    ) -> anyhow::Result<Storage> {
        let tempdir = tempfile::tempdir()?;
        tracing::trace!("Creating storage in: {}", tempdir.path().display());
        let db_path = tempdir.path().join("db.sqlite");
        let mut manager = crate::StorageBuilder::file(db_path)
            .trie_prune_mode(Some(trie_prune_mode))
            .rocksdb_tempdir(tempdir)
            .migrate()
            .unwrap();
        manager.create_pool(pool_size)
    }

    /// Convenience function for tests to create a persisted in-tempdir database
    /// with a specific blockchain pruning mode.
    pub fn in_persisted_tempdir_with_blockchain_pruning_and_pool_size(
        tempdir: &tempfile::TempDir,
        blockchain_history_mode: BlockchainHistoryMode,
        pool_size: NonZeroU32,
    ) -> anyhow::Result<Storage> {
        tracing::trace!("Creating storage in: {}", tempdir.path().display());
        crate::StorageBuilder::file(tempdir.path().join("db.sqlite"))
            .blockchain_history_mode(Some(blockchain_history_mode))
            .migrate()?
            .create_pool(pool_size)
    }

    /// Performs the database schema migration and returns a [storage
    /// manager](StorageManager).
    ///
    /// This should be called __once__ at the start of the application,
    /// and passed to the various components which require access to the
    /// database.
    pub fn migrate(mut self) -> anyhow::Result<StorageManager> {
        let (rocksdb_path, rocksdb_tempdir) = Self::resolve_rocksdb_location(
            &self.database_path,
            self.rocksdb_tempdir.take(),
            "in-memory SQLite URI requires a preassigned RocksDB tempdir; use one of the \
             ephemeral constructors on StorageBuilder",
        )?;
        let rocksdb = Arc::new(Self::open_rocksdb(&rocksdb_path, rocksdb_tempdir)?);

        let mut open_flags = OpenFlags::default();
        open_flags.remove(OpenFlags::SQLITE_OPEN_CREATE);
        let (mut connection, is_new_database) =
            rusqlite::Connection::open_with_flags(&self.database_path, open_flags)
                .map_or_else(
                    |e| {
                        if e.sqlite_error_code() == Some(rusqlite::ErrorCode::CannotOpen) {
                            rusqlite::Connection::open(&self.database_path).map(|c| (c, true))
                        } else {
                            Err(e)
                        }
                    },
                    |c| Ok((c, false)),
                )
                .context("Opening DB for migration")?;

        // Migration is done with rollback journal mode. Otherwise dropped tables
        // get copied into the WAL which is prohibitively expensive for large
        // tables.
        setup_journal_mode(&mut connection, JournalMode::Rollback)
            .context("Setting journal mode to rollback")?;
        setup_connection(&mut connection, JournalMode::Rollback)
            .context("Setting up database connection")?;

        migrate_database(&mut connection, &rocksdb).context("Migrate database")?;

        reconcile_rocksdb_with_sqlite(&mut connection, &rocksdb)
            .context("Reconciling RocksDB with SQLite after migration")?;

        // Set the journal mode to the desired value.
        setup_journal_mode(&mut connection, self.journal_mode).context("Setting journal mode")?;

        // Validate that configuration matches database flags.
        let (blockchain_history_mode, pending_prune) =
            self.determine_blockchain_history_mode(&mut connection, is_new_database)?;
        let trie_prune_mode = self.determine_trie_prune_mode(&mut connection, is_new_database)?;

        if let BlockchainHistoryMode::Prune { num_blocks_kept } = blockchain_history_mode {
            tracing::info!(history_kept=%num_blocks_kept, "Blockchain pruning enabled");
        } else {
            tracing::info!("Blockchain pruning disabled");
        }
        if let TriePruneMode::Prune { num_blocks_kept } = trie_prune_mode {
            tracing::info!(history_kept=%num_blocks_kept, "Merkle trie pruning enabled");
        } else {
            tracing::info!("Merkle trie pruning disabled");
        }

        let running_event_filter = {
            // Build a temporary storage Transaction wrapping the raw
            // rusqlite connection and the RocksDB handle so that
            // RunningEventFilter::load (and ::rebuild, if needed) can
            // access both SQLite and RocksDB.
            let dummy_ref = Arc::new(Mutex::new(event::RunningEventFilter {
                filter: crate::bloom::AggregateBloom::new(BlockNumber::GENESIS),
                next_block: BlockNumber::GENESIS,
            }));
            let raw_tx = connection.transaction()?;
            let storage_tx = crate::connection::Transaction::from_raw_parts(
                raw_tx,
                Arc::new(AggregateBloomCache::with_size(self.event_filter_cache_size)),
                dummy_ref,
                rocksdb.clone(),
            );
            event::RunningEventFilter::load(&storage_tx).context("Loading running event filter")?
        };

        connection
            .close()
            .map_err(|(_connection, error)| error)
            .context("Closing DB after migration")?;

        Ok(StorageManager {
            database_path: self.database_path,
            journal_mode: self.journal_mode,
            rocksdb,
            event_filter_cache: Arc::new(AggregateBloomCache::with_size(
                self.event_filter_cache_size,
            )),
            running_event_filter: Arc::new(Mutex::new(running_event_filter)),
            trie_prune_mode,
            blockchain_history_mode,
            pending_prune,
        })
    }

    /// Does not perform any migrations, just loads the database in read-only
    /// mode. This is useful for tools which only need to read from the
    /// database, especially when a Pathfinder instance is writing to the
    /// database at the same time.
    pub fn readonly(self) -> anyhow::Result<ReadOnlyStorageManager> {
        let Self {
            database_path,
            journal_mode,
            event_filter_cache_size,
            rocksdb_tempdir,
            ..
        } = self;

        let mut open_flags = OpenFlags::default();
        open_flags.remove(OpenFlags::SQLITE_OPEN_CREATE);
        let mut connection = rusqlite::Connection::open_with_flags(&database_path, open_flags)
            .context("Opening DB to load running event filter")?;
        let init_num_blocks_kept = connection
            .query_row(
                "SELECT value FROM storage_options WHERE option = 'prune_blockchain'",
                [],
                |row| row.get_u64(0),
            )
            .optional()?;

        let blockchain_history_mode = {
            if let Some(num_blocks_kept) = init_num_blocks_kept {
                BlockchainHistoryMode::Prune { num_blocks_kept }
            } else {
                BlockchainHistoryMode::Archive
            }
        };

        let prune_flag_is_set = connection
            .query_row(
                "SELECT 1 FROM storage_options WHERE option = 'prune_tries'",
                [],
                |_| Ok(()),
            )
            .optional()
            .map(|x| x.is_some())?;

        let trie_prune_mode = if prune_flag_is_set {
            TriePruneMode::Prune {
                num_blocks_kept: 20,
            }
        } else {
            TriePruneMode::Archive
        };

        // Open RocksDB before loading the event filter so that
        // RunningEventFilter::load/rebuild can read the EVENTS_COLUMN.
        // `resolve_rocksdb_location` rejects in-memory SQLite URIs with the
        // message below; on the happy path the tempdir it returns is always
        // None, so it's discarded here.
        let (rocksdb_path, _) = Self::resolve_rocksdb_location(
            &database_path,
            rocksdb_tempdir,
            "readonly() does not support in-memory SQLite URIs",
        )?;
        let rocksdb = Arc::new(Self::open_rocksdb_readonly(&rocksdb_path)?);

        // Lightweight consistency check: warn if RocksDB is ahead of SQLite.
        // Read-only mode reads from last-flushed SST state; catch-up against a
        // running writer is not supported. If the writer has committed blocks
        // beyond the last SST flush, this handle will not see them until the
        // writer flushes and this handle is reopened.
        {
            use crate::connection::STATE_UPDATES_COLUMN;
            let sqlite_highest: Option<u64> = connection
                .query_row("SELECT MAX(number) FROM block_headers", [], |row| {
                    row.get_optional_u64(0)
                })
                .unwrap_or(None);
            let state_updates_cf = rocksdb.get_column(&STATE_UPDATES_COLUMN);
            let mut read_opts = rust_rocksdb::ReadOptions::default();
            read_opts.set_total_order_seek(true);
            let mut iter = rocksdb
                .rocksdb
                .raw_iterator_cf_opt(&state_updates_cf, read_opts);
            iter.seek_to_last();
            let rocksdb_highest = if iter.valid() {
                iter.key()
                    .and_then(|k| k.try_into().ok())
                    .map(u64::from_be_bytes)
            } else {
                if let Err(e) = iter.status() {
                    tracing::warn!(error = %e, "RocksDB iterator error during readonly consistency check");
                }
                None
            };
            if let Some(rocks_top) = rocksdb_highest {
                let is_ahead = match sqlite_highest {
                    Some(sqlite_top) => rocks_top > sqlite_top,
                    None => true,
                };
                if is_ahead {
                    tracing::warn!(
                        ?sqlite_highest,
                        rocks_top,
                        "RocksDB is ahead of SQLite in readonly mode; data may be inconsistent. \
                         Run the node in normal mode first to reconcile."
                    );
                }
            }
        }

        let running_event_filter = {
            let dummy_ref = Arc::new(Mutex::new(event::RunningEventFilter {
                filter: crate::bloom::AggregateBloom::new(BlockNumber::GENESIS),
                next_block: BlockNumber::GENESIS,
            }));
            let raw_tx = connection.transaction()?;
            let storage_tx = crate::connection::Transaction::from_raw_parts(
                raw_tx,
                Arc::new(AggregateBloomCache::with_size(event_filter_cache_size)),
                dummy_ref,
                rocksdb.clone(),
            );
            event::RunningEventFilter::load(&storage_tx).context("Loading running event filter")?
        };

        connection
            .close()
            .map_err(|(_connection, error)| error)
            .context("Closing DB after loading running event filter")?;

        Ok(ReadOnlyStorageManager(StorageManager {
            database_path,
            journal_mode,
            rocksdb,
            event_filter_cache: Arc::new(AggregateBloomCache::with_size(event_filter_cache_size)),
            running_event_filter: Arc::new(Mutex::new(running_event_filter)),
            trie_prune_mode,
            blockchain_history_mode,
            pending_prune: None,
        }))
    }

    pub(crate) fn open_rocksdb(
        path: &Path,
        tempdir: Option<tempfile::TempDir>,
    ) -> anyhow::Result<RocksDBInner> {
        let available_parallelism = std::thread::available_parallelism()
            .map(|e| (e.get() as i32 / 2).max(1))
            .unwrap_or(1);

        let mut options = rust_rocksdb::Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        options.increase_parallelism(available_parallelism);
        options.set_max_background_jobs(available_parallelism);
        options.set_atomic_flush(true);
        options.set_max_subcompactions(available_parallelism as _);
        options.set_max_write_buffer_number(5);
        options.set_min_write_buffer_number_to_merge(2);
        options.set_bytes_per_sync(1024 * 1024_u64);
        options.set_wal_bytes_per_sync(512 * 1024_u64);
        options.set_max_log_file_size(10 * 1024 * 1024_usize);
        options.set_max_open_files(50000);
        options.set_keep_log_file_num(3);
        options.set_log_level(rust_rocksdb::LogLevel::Warn);

        let mut env = rust_rocksdb::Env::new().context("Creating rocksdb env")?;
        // Low priority threads are used for compaction (can be preempted by flush).
        env.set_low_priority_background_threads(available_parallelism);

        options.set_env(&env);

        // TODO: make this configurable
        let cache = rust_rocksdb::Cache::new_hyper_clock_cache(16 * 1024 * 1024 * 1024, 0);

        let cfs = columns::COLUMNS
            .iter()
            .map(|column| ColumnFamilyDescriptor::new(column.name, column.options(&cache)));

        options.enable_statistics();

        let db = RocksDB::open_cf_descriptors(&options, path, cfs)?;

        let (trie_class_next_index, trie_contract_next_index, trie_storage_next_index) =
            Self::rocksdb_fetch_next_trie_storage_indices(&db)?;

        let db_inner = RocksDBInner {
            rocksdb: db,
            options,
            trie_class_next_index: std::sync::atomic::AtomicU64::new(trie_class_next_index),
            trie_contract_next_index: std::sync::atomic::AtomicU64::new(trie_contract_next_index),
            trie_storage_next_index: std::sync::atomic::AtomicU64::new(trie_storage_next_index),
            _tempdir: tempdir,
        };
        Ok(db_inner)
    }

    pub(crate) fn open_rocksdb_readonly(path: &Path) -> anyhow::Result<RocksDBInner> {
        // The read-only path serves support tools on constrained hosts,
        // so the write-path's 16 GiB block cache is wrong here. The tuned
        // write-buffer, atomic-flush and statistics knobs also don't apply.
        let mut options = rust_rocksdb::Options::default();
        options.set_max_open_files(-1);
        options.set_max_log_file_size(10 * 1024 * 1024_usize);
        options.set_keep_log_file_num(3);
        options.set_log_level(rust_rocksdb::LogLevel::Warn);

        let cache = rust_rocksdb::Cache::new_hyper_clock_cache(256 * 1024 * 1024, 0);
        let cfs = columns::COLUMNS
            .iter()
            .map(|column| ColumnFamilyDescriptor::new(column.name, column.options(&cache)));

        // `error_if_log_file_exist = false` skips any unreplayed WAL and
        // reads only from the last-flushed SST state. The alternative would
        // let the tool silently pick up partial writes a crashed writer left
        // behind; the snapshot workflow flushes WAL before archiving anyway.
        let db = RocksDB::open_cf_descriptors_read_only(&options, path, cfs, false)
            .with_context(|| format!("Opening RocksDB read-only at {}", path.display()))?;

        let (trie_class_next_index, trie_contract_next_index, trie_storage_next_index) =
            Self::rocksdb_fetch_next_trie_storage_indices(&db)?;

        let db_inner = RocksDBInner {
            rocksdb: db,
            options,
            trie_class_next_index: std::sync::atomic::AtomicU64::new(trie_class_next_index),
            trie_contract_next_index: std::sync::atomic::AtomicU64::new(trie_contract_next_index),
            trie_storage_next_index: std::sync::atomic::AtomicU64::new(trie_storage_next_index),
            _tempdir: None,
        };
        Ok(db_inner)
    }

    fn rocksdb_fetch_next_trie_storage_indices(db: &RocksDB) -> anyhow::Result<(u64, u64, u64)> {
        let trie_class_last_index =
            Self::trie_next_index(db, &crate::connection::TRIE_CLASS_COLUMN)?;
        let trie_contract_last_index =
            Self::trie_next_index(db, &crate::connection::TRIE_CONTRACT_COLUMN)?;
        let trie_storage_last_index =
            Self::trie_next_index(db, &crate::connection::TRIE_STORAGE_COLUMN)?;
        Ok((
            trie_class_last_index,
            trie_contract_last_index,
            trie_storage_last_index,
        ))
    }

    fn trie_next_index(db: &RocksDB, column: &Column) -> anyhow::Result<u64> {
        Ok(read_trie_next_index_from_disk(db, column)?.unwrap_or(0))
    }

    /// - If there is no explicitly requested configuration, assumes the user
    ///   wants to archive. If this doesn't match the database setting, errors.
    /// - If there's an explicitly requested setting: uses it if matches DB
    ///   setting, enables pruning and sets flag in the database. Otherwise
    ///   errors.
    fn determine_trie_prune_mode(
        &self,
        connection: &mut rusqlite::Connection,
        is_new_database: bool,
    ) -> anyhow::Result<TriePruneMode> {
        let prune_flag_is_set = connection
            .query_row(
                "SELECT 1 FROM storage_options WHERE option = 'prune_tries'",
                [],
                |_| Ok(()),
            )
            .optional()
            .map(|x| x.is_some())?;

        let trie_prune_mode = self.trie_prune_mode.unwrap_or({
            if is_new_database || prune_flag_is_set {
                TriePruneMode::Prune {
                    num_blocks_kept: 20,
                }
            } else {
                TriePruneMode::Archive
            }
        });

        match trie_prune_mode {
            TriePruneMode::Archive => {
                if prune_flag_is_set {
                    anyhow::bail!(
                        "Cannot disable Merkle trie pruning on a database that was created with \
                         it enabled."
                    )
                }
            }
            TriePruneMode::Prune { num_blocks_kept: _ } => {
                if !is_new_database && !prune_flag_is_set {
                    anyhow::bail!(
                        "Cannot enable Merkle trie pruning on a database that was not created \
                         with it enabled."
                    );
                }

                if is_new_database {
                    connection.execute(
                        "INSERT OR IGNORE INTO storage_options (option) VALUES ('prune_tries')",
                        [],
                    )?;
                    tracing::info!("Created new database with Merkle trie pruning enabled.");
                }
            }
        }

        Ok(trie_prune_mode)
    }

    /// Determines the blockchain history mode based on the database state and
    /// configuration.
    ///
    /// - If there is no explicitly requested configuration, assumes the user
    ///   wants to archive. If this doesn't match the database setting, errors.
    /// - If there's an explicitly requested setting: uses it if it matches the
    ///   DB setting, otherwise errors.
    /// - If the database is new and no configuration is provided, the database
    ///   is created in archive mode.
    /// - Once the history mode is chosen, it cannot be changed (the history
    ///   size can change from run to run in pruning mode).
    fn determine_blockchain_history_mode(
        &self,
        connection: &mut rusqlite::Connection,
        is_new_database: bool,
    ) -> anyhow::Result<(BlockchainHistoryMode, Option<PendingPrune>)> {
        let init_num_blocks_kept = connection
            .query_row(
                "SELECT value FROM storage_options WHERE option = 'prune_blockchain'",
                [],
                |row| row.get_u64(0),
            )
            .optional()?;

        let blockchain_history_mode = self.blockchain_history_mode.unwrap_or({
            // Keep the same history size or default to archive mode.
            if let Some(num_blocks_kept) = init_num_blocks_kept {
                BlockchainHistoryMode::Prune { num_blocks_kept }
            } else {
                BlockchainHistoryMode::Archive
            }
        });

        let (validated_blockchain_history_mode, pending_prune) = validate_mode_and_update_db(
            blockchain_history_mode,
            init_num_blocks_kept,
            is_new_database,
            connection,
        )?;

        Ok((validated_blockchain_history_mode, pending_prune))
    }
}

fn validate_mode_and_update_db(
    blockchain_history_mode: BlockchainHistoryMode,
    init_num_blocks_kept: Option<u64>,
    is_new_database: bool,
    connection: &mut rusqlite::Connection,
) -> anyhow::Result<(BlockchainHistoryMode, Option<PendingPrune>)> {
    match blockchain_history_mode {
        BlockchainHistoryMode::Archive => {
            if init_num_blocks_kept.is_some() {
                anyhow::bail!(
                    "Cannot disable blockchain history pruning on a database that was created \
                     with it enabled."
                );
            }
        }
        BlockchainHistoryMode::Prune { num_blocks_kept } => {
            let init_num_blocks_kept = match init_num_blocks_kept {
                Some(init_num_blocks_kept) => init_num_blocks_kept,
                None => {
                    if is_new_database {
                        num_blocks_kept
                    } else {
                        anyhow::bail!(
                            "Cannot enable blockchain history pruning on a database that was \
                             created with it disabled."
                        );
                    }
                }
            };

            connection.execute(
                r"
                INSERT INTO storage_options (option, value)
                VALUES ('prune_blockchain', ?)
                ON CONFLICT(option) DO UPDATE SET value = excluded.value
                ",
                [num_blocks_kept.try_into_sql_int()?],
            )?;

            if is_new_database {
                tracing::info!("Created new database with blockchain history pruning enabled.");
                return Ok((blockchain_history_mode, None));
            }

            // If the blockchain history size got reduced, prune the now-excess blocks
            // once we have a connection pool. If the size increased, we don't need to do
            // anything since the gap will be filled as new blocks are synced.
            let num_blocks_to_remove = match init_num_blocks_kept.checked_sub(num_blocks_kept) {
                Some(block_diff) if block_diff > 0 => block_diff,
                _ => return Ok((blockchain_history_mode, None)),
            };

            let oldest: Option<u64> = connection
                .query_row(
                    "SELECT number FROM block_headers ORDER BY number ASC LIMIT 1",
                    [],
                    |row| row.get_u64(0),
                )
                .optional()
                .context("Fetching oldest block number")?;

            let pending_prune = oldest.map(|oldest| PendingPrune {
                oldest,
                num_blocks_to_remove,
            });

            return Ok((blockchain_history_mode, pending_prune));
        }
    }

    Ok((blockchain_history_mode, None))
}

impl Storage {
    /// Returns a new Sqlite [Connection] to the database.
    pub fn connection(&self) -> Result<Connection, StorageError> {
        let conn = self.0.pool.get().map_err(StorageError::from)?;
        Ok(Connection::new(
            conn,
            Arc::clone(&self.0.rocksdb),
            self.0.event_filter_cache.clone(),
            self.0.running_event_filter.clone(),
            self.0.trie_prune_mode,
            self.0.blockchain_history_mode,
        ))
    }

    pub fn path(&self) -> &Path {
        &self.0.database_path
    }

    #[cfg(test)]
    pub(crate) fn rocksdb_tempdir_path(&self) -> Option<std::path::PathBuf> {
        self.0
            .rocksdb
            ._tempdir
            .as_ref()
            .map(|d| d.path().to_path_buf())
    }

    #[cfg(test)]
    pub(crate) fn rocksdb_inner(&self) -> &Arc<RocksDBInner> {
        &self.0.rocksdb
    }

    pub fn is_migrated(&self) -> Result<bool, StorageError> {
        let mut connection = self.connection()?;
        let tx = connection.transaction()?;

        let user_version = tx.user_version()?;

        Ok(user_version == schema::LATEST_SCHEMA_REVISION as i64)
    }
}

fn setup_journal_mode(
    connection: &mut rusqlite::Connection,
    journal_mode: JournalMode,
) -> Result<(), rusqlite::Error> {
    // set journal mode related pragmas
    match journal_mode {
        JournalMode::Rollback => connection.pragma_update(None, "journal_mode", "DELETE"),
        JournalMode::WAL => {
            connection.pragma_update(None, "journal_mode", "WAL")?;
            // set journal size limit to 1 GB
            connection.pragma_update(
                None,
                "journal_size_limit",
                (1024usize * 1024 * 1024).to_string(),
            )
        }
    }
}

fn setup_connection(
    connection: &mut rusqlite::Connection,
    journal_mode: JournalMode,
) -> Result<(), rusqlite::Error> {
    // Enable foreign keys.
    connection.set_db_config(
        rusqlite::config::DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY,
        true,
    )?;

    // Use a large cache for prepared statements.
    connection.set_prepared_statement_cache_capacity(1000);

    match journal_mode {
        JournalMode::Rollback => {
            // According to the documentation FULL is the recommended setting for rollback
            // mode.
            connection.pragma_update(None, "synchronous", "full")?;
        }
        JournalMode::WAL => {
            // According to the documentation NORMAL is a good choice for WAL mode.
            connection.pragma_update(None, "synchronous", "normal")?;
        }
    };

    // Register the rarray module on the connection.
    // See: https://docs.rs/rusqlite/0.29.0/rusqlite/vtab/array/index.html
    rusqlite::vtab::array::load_module(connection)?;

    Ok(())
}

/// Migrates the database to the latest version. This __MUST__ be called
/// at the beginning of the application.
fn migrate_database(
    connection: &mut rusqlite::Connection,
    rocksdb: &RocksDBInner,
) -> anyhow::Result<()> {
    let mut current_revision = schema_version(connection)?;
    let migrations = schema::migrations();

    // Apply the base schema if the database is new.
    if current_revision == 0 {
        let tx = connection
            .transaction()
            .context("Create database transaction")?;
        schema::base_schema(&tx).context("Applying base schema")?;
        tx.pragma_update(None, VERSION_KEY, schema::BASE_SCHEMA_REVISION as i64)
            .context("Failed to update the schema version number")?;
        tx.commit().context("Commit migration transaction")?;

        current_revision = schema::BASE_SCHEMA_REVISION;
    }

    // Skip migration if we already at latest.
    if current_revision == schema::LATEST_SCHEMA_REVISION {
        tracing::info!(%current_revision, "No database migrations required");
        return Ok(());
    }

    // Check for database version compatibility.
    if current_revision < schema::BASE_SCHEMA_REVISION {
        tracing::error!(
            version=%current_revision,
            limit=%schema::BASE_SCHEMA_REVISION,
            "Database version is too old to migrate"
        );
        anyhow::bail!("Database version {current_revision} too old to migrate");
    }

    if current_revision > schema::LATEST_SCHEMA_REVISION {
        tracing::error!(
            version=%current_revision,
            limit=%schema::LATEST_SCHEMA_REVISION,
            "Database version is from a newer than this application expected"
        );
        anyhow::bail!(
            "Database version {current_revision} is newer than this application expected {}",
            schema::LATEST_SCHEMA_REVISION
        );
    }

    let amount = schema::LATEST_SCHEMA_REVISION - current_revision;
    tracing::info!(%current_revision, latest_revision=%schema::LATEST_SCHEMA_REVISION, migrations=%amount, "Performing database migrations");

    // Sequentially apply each missing migration.
    migrations
        .iter()
        .rev()
        .take(amount)
        .rev()
        .try_for_each(|migration| {
            let mut do_migration = || -> anyhow::Result<()> {
                current_revision += 1;
                let span = tracing::info_span!("db_migration", revision = current_revision);
                let _enter = span.enter();

                let transaction = connection
                    .transaction()
                    .context("Create database transaction")?;
                migration(&transaction, rocksdb)?;
                transaction
                    .pragma_update(None, VERSION_KEY, current_revision as i64)
                    .context("Failed to update the schema version number")?;
                transaction
                    .commit()
                    .context("Commit migration transaction")?;

                Ok(())
            };

            do_migration().with_context(|| format!("Migrating to {current_revision}"))
        })?;

    Ok(())
}

/// Reads the on-disk `TRIE_NEXT_INDEX_COLUMN` entry for `column`. Missing
/// key returns `Ok(None)` — callers that want "0 when absent" apply
/// `.unwrap_or(0)`. An unexpected byte length errors rather than silently
/// truncating.
fn read_trie_next_index_from_disk(db: &RocksDB, column: &Column) -> anyhow::Result<Option<u64>> {
    let column_handle = db
        .cf_handle(TRIE_NEXT_INDEX_COLUMN.name)
        .context("Getting RocksDB column for fetching next trie storage index")?;
    db.get_cf(&column_handle, column.name.as_bytes())?
        .map(|value| -> anyhow::Result<u64> {
            let bytes: [u8; 8] = value.as_slice().try_into().map_err(|_| {
                anyhow::anyhow!(
                    "TRIE_NEXT_INDEX_COLUMN[{}] value has invalid length: {}",
                    column.name,
                    value.len()
                )
            })?;
            Ok(u64::from_be_bytes(bytes))
        })
        .transpose()
}

/// Walks the trie subtree rooted at `root_idx` and returns the highest
/// storage index reached — the confirmed batch's tail. Returns `Ok(None)`
/// if a node is missing or undecodable; the caller must then leave orphans
/// in place rather than fire a `delete_range_cf` against a corrupt DB.
fn dfs_confirmed_tail(
    rocksdb: &RocksDBInner,
    cf_handle: &Arc<rust_rocksdb::BoundColumnFamily<'_>>,
    cf_name: &str,
    root_idx: TrieStorageIndex,
    counter: u64,
) -> anyhow::Result<Option<TrieStorageIndex>> {
    use std::collections::HashSet;

    let mut stack = vec![root_idx];
    let mut visited = HashSet::new();
    let mut max_reached = root_idx;

    while let Some(idx) = stack.pop() {
        // Skip below-batch subtrees; they belong to earlier commits and are
        // not relevant to the tail calculation for this batch.
        if idx.get() < root_idx.get() {
            continue;
        }
        if !visited.insert(idx) {
            continue;
        }
        if idx.get() > max_reached.get() {
            max_reached = idx;
        }
        // Early exit: the confirmed batch aligns with the counter, so
        // there's nothing to reconcile for this CF.
        if max_reached.get().saturating_add(1) == counter {
            return Ok(Some(max_reached));
        }

        let cf_key = idx.get().to_be_bytes();
        let Some(raw) = rocksdb
            .rocksdb
            .get_pinned_cf(cf_handle, cf_key)
            .context("Reading trie node during DFS")?
        else {
            tracing::warn!(
                cf = cf_name,
                index = idx.get(),
                "DFS aborted: missing trie node in confirmed batch"
            );
            return Ok(None);
        };

        let node = match decode_stored_node_with_hash(raw.as_ref()) {
            Ok(node) => node,
            Err(err) => {
                tracing::warn!(
                    cf = cf_name,
                    index = idx.get(),
                    error = %err,
                    "DFS aborted: undecodable trie node in confirmed batch"
                );
                return Ok(None);
            }
        };

        match node {
            StoredNode::Binary { left, right } => {
                stack.push(left);
                stack.push(right);
            }
            StoredNode::Edge { child, .. } => {
                stack.push(child);
            }
            StoredNode::LeafBinary | StoredNode::LeafEdge { .. } => {}
        }
    }

    Ok(Some(max_reached))
}

/// Per-CF trie-orphan reconcile. Detects orphaned trie indices from a
/// crashed commit, stages the range-delete + counter rewrite into `batch`,
/// and re-seeds the in-memory `next_index` atomic.
///
/// The atomic is always overwritten — either with the rewound value (when
/// reconcile found orphans) or with the on-disk counter (when it didn't).
/// One code path handles both the crash-recovery rewind and the stale
/// atomic left over from a fresh-install migration boot.
fn reconcile_trie_column(
    rocksdb: &RocksDBInner,
    connection: &rusqlite::Connection,
    sqlite_top: u64,
    trie_col: TrieColumn,
    root_index_sql: &'static str,
    batch: &mut crate::RocksDBBatch,
) -> anyhow::Result<()> {
    let trie_cf = trie_col.column();
    let cf_handle = rocksdb.get_column(trie_cf);
    let idx_cf = rocksdb.get_column(&TRIE_NEXT_INDEX_COLUMN);

    // Missing key means the CF has no writes yet; treat as 0 so the
    // early-return below skips the reconcile.
    let counter = read_trie_next_index_from_disk(&rocksdb.rocksdb, trie_cf)
        .context("Reading TRIE_NEXT_INDEX_COLUMN for reconcile")?
        .unwrap_or(0);

    // The value we hand back to the in-memory atomic at the end. Reconcile
    // may rewind it below `counter`; every early-return path keeps this at
    // the on-disk value.
    let mut final_counter = counter;

    if counter == 0 {
        rocksdb.store_next_index(trie_col, final_counter);
        return Ok(());
    }

    let row: Option<(u64, u64)> = connection
        .query_row(
            root_index_sql,
            rusqlite::params![sqlite_top as i64],
            |row| {
                let idx: i64 = row.get(0)?;
                let block: i64 = row.get(1)?;
                Ok((idx as u64, block as u64))
            },
        )
        .optional()
        .context("Querying MAX(root_index) for reconcile")?;

    let Some((root_idx_u64, block_num)) = row else {
        tracing::warn!(
            cf = trie_cf.name,
            counter,
            "Trie reconcile: no confirmed root row found; leaving orphans in place"
        );
        rocksdb.store_next_index(trie_col, final_counter);
        return Ok(());
    };

    // Defensive block-distance guard: if pruning has stripped the last
    // real-index row and the surviving MAX row is far below `sqlite_top`,
    // skip reconcile for this CF. The invariant "no live intermediate
    // batch above MAX row's batch" still holds (see spec), so this is
    // belt-and-braces.
    const MAX_BLOCK_DISTANCE: u64 = 100;
    if sqlite_top.saturating_sub(block_num) > MAX_BLOCK_DISTANCE {
        tracing::warn!(
            cf = trie_cf.name,
            sqlite_top,
            max_row_block = block_num,
            "Trie reconcile: MAX(root_index) row is >100 blocks below sqlite_top; skipping \
             range-delete for this CF"
        );
        rocksdb.store_next_index(trie_col, final_counter);
        return Ok(());
    }

    let Some(root_idx) = TrieStorageIndex::new(root_idx_u64) else {
        tracing::warn!(
            cf = trie_cf.name,
            root_index = root_idx_u64,
            "Trie reconcile: MAX(root_index) value is out of TrieStorageIndex range; leaving \
             orphans in place"
        );
        rocksdb.store_next_index(trie_col, final_counter);
        return Ok(());
    };

    let Some(confirmed_tail) =
        dfs_confirmed_tail(rocksdb, &cf_handle, trie_cf.name, root_idx, counter)?
    else {
        // DFS aborted; leave orphans in place, warning already logged.
        rocksdb.store_next_index(trie_col, final_counter);
        return Ok(());
    };

    let new_counter = confirmed_tail.get() + 1;
    // DFS invariants guarantee `new_counter <= counter`: indices grow
    // monotonically and the DFS early-exits on `max_reached + 1 == counter`,
    // so `new_counter > counter` is unreachable.
    debug_assert!(new_counter <= counter);
    if new_counter < counter {
        batch.delete_range_cf(&cf_handle, new_counter.to_be_bytes(), counter.to_be_bytes());
        batch.put_cf(&idx_cf, trie_cf.name.as_bytes(), new_counter.to_be_bytes());
        final_counter = new_counter;

        tracing::info!(
            cf = trie_cf.name,
            deleted_from = new_counter,
            deleted_to = counter,
            "Reconciled orphan trie indices from crashed commit"
        );
    }

    rocksdb.store_next_index(trie_col, final_counter);
    Ok(())
}

/// Deletes RocksDB rows for any block number that is ahead of the highest
/// SQLite block header. This handles the crash window between `Transaction::
/// commit`'s RocksDB write and its SQLite commit.
pub(crate) fn reconcile_rocksdb_with_sqlite(
    connection: &mut rusqlite::Connection,
    rocksdb: &RocksDBInner,
) -> anyhow::Result<()> {
    use crate::connection::state_update::{nonce_update_key, storage_update_key};
    use crate::connection::{
        contract_state_hashes_key,
        CONTRACT_STATE_HASHES_COLUMN,
        EVENTS_COLUMN,
        NONCE_UPDATES_COLUMN,
        STATE_UPDATES_COLUMN,
        STORAGE_UPDATES_COLUMN,
        TRANSACTIONS_AND_RECEIPTS_COLUMN,
        TRANSACTION_HASHES_COLUMN,
    };

    // 1. Highest SQLite block.
    let sqlite_highest: Option<u64> = connection
        .query_row("SELECT MAX(number) FROM block_headers", [], |row| {
            row.get_optional_u64(0)
        })
        .context("Querying highest SQLite block")?;

    // 2. Highest RocksDB block in STATE_UPDATES_COLUMN.
    let state_updates_cf = rocksdb.get_column(&STATE_UPDATES_COLUMN);
    let rocksdb_highest = {
        let mut read_opts = rust_rocksdb::ReadOptions::default();
        read_opts.set_total_order_seek(true);
        let mut iter = rocksdb
            .rocksdb
            .raw_iterator_cf_opt(&state_updates_cf, read_opts);
        iter.seek_to_last();
        if iter.valid() {
            let key = iter.key().context("RocksDB iterator key missing")?;
            let bytes: [u8; 8] = key
                .try_into()
                .map_err(|_| anyhow::anyhow!("Invalid STATE_UPDATES_COLUMN key length"))?;
            Some(u64::from_be_bytes(bytes))
        } else {
            iter.status()
                .context("RocksDB iterator error in reconcile")?;
            None
        }
    };

    // Shared batch: block-orphan deletes AND trie-orphan deletes both stage
    // into this batch, so one atomic write covers the whole reconcile.
    let mut batch = crate::RocksDBBatch::default();

    // 3. Block-orphan cleanup (unchanged semantics: skip when RocksDB has no
    // rows, or when `rocks_top <= sqlite_top`).
    if let Some(rocks_top) = rocksdb_highest {
        let purge_from = match sqlite_highest {
            Some(sqlite_top) if rocks_top <= sqlite_top => None,
            Some(sqlite_top) => Some(sqlite_top + 1),
            None => Some(0u64),
        };
        if let Some(from) = purge_from {
            tracing::warn!(
                ?sqlite_highest,
                rocks_top,
                "RocksDB is ahead of SQLite -- purging orphaned blocks"
            );

            let txs_cf = rocksdb.get_column(&TRANSACTIONS_AND_RECEIPTS_COLUMN);
            let events_cf = rocksdb.get_column(&EVENTS_COLUMN);
            let hashes_cf = rocksdb.get_column(&TRANSACTION_HASHES_COLUMN);
            let nonce_cf = rocksdb.get_column(&NONCE_UPDATES_COLUMN);
            let storage_cf = rocksdb.get_column(&STORAGE_UPDATES_COLUMN);
            let csh_cf = rocksdb.get_column(&CONTRACT_STATE_HASHES_COLUMN);

            for block_number in from..=rocks_top {
                let key = block_number.to_be_bytes();

                if let Some(blob) = rocksdb
                    .rocksdb
                    .get_pinned_cf(&txs_cf, key)
                    .context("Reading orphaned transactions blob")?
                {
                    if let Err(e) = (|| -> anyhow::Result<()> {
                        let decompressed =
                            crate::connection::transaction::compression::decompress_transactions(
                                &blob,
                            )
                            .context("Decompressing orphaned transactions blob")?;
                        let (txs, _): (
                            crate::connection::transaction::dto::TransactionsWithReceiptsForBlock,
                            _,
                        ) = bincode::serde::decode_from_slice(
                            &decompressed,
                            bincode::config::standard(),
                        )
                        .context("Decoding orphaned transactions blob")?;
                        for tx in txs.transactions_with_receipts() {
                            let common_tx: pathfinder_common::transaction::Transaction =
                                tx.transaction.into();
                            batch.delete_cf(&hashes_cf, common_tx.hash.0.as_be_bytes());
                        }
                        Ok(())
                    })() {
                        tracing::warn!(
                            block_number,
                            error = %e,
                            "Failed to decode orphaned transactions blob; transaction hash entries \
                             for this block will remain as orphans in TRANSACTION_HASHES_COLUMN. \
                             The `transaction_block_hash` reader cross-checks the embedded block \
                             number against `block_headers`, so an orphan returns `Ok(None)` \
                             rather than a phantom block reference."
                        );
                    }
                }

                if let Some(blob) = rocksdb
                    .rocksdb
                    .get_pinned_cf(&state_updates_cf, key)
                    .context("Reading orphaned state update blob")?
                {
                    if let Err(e) = (|| -> anyhow::Result<()> {
                        let (data, _): (crate::connection::state_update::dto::StateUpdateData, _) =
                            bincode::serde::decode_from_slice(&blob, bincode::config::standard())
                                .context("Decoding orphaned state update blob")?;
                        let block_number =
                            pathfinder_common::BlockNumber::new_or_panic(block_number);
                        let data = pathfinder_common::state_update::StateUpdateData::from(data);
                        for (address, update) in &data.contract_updates {
                            if update.nonce.is_some() {
                                batch.delete_cf(&nonce_cf, nonce_update_key(block_number, address));
                            }
                            for storage_key in update.storage.keys() {
                                batch.delete_cf(
                                    &storage_cf,
                                    storage_update_key(block_number, address, storage_key),
                                );
                            }
                        }
                        for (address, update) in &data.system_contract_updates {
                            for storage_key in update.storage.keys() {
                                batch.delete_cf(
                                    &storage_cf,
                                    storage_update_key(block_number, address, storage_key),
                                );
                            }
                        }
                        for address in data.contract_updates.keys() {
                            batch.delete_cf(
                                &csh_cf,
                                contract_state_hashes_key(block_number, address),
                            );
                        }
                        for address in data.system_contract_updates.keys() {
                            batch.delete_cf(
                                &csh_cf,
                                contract_state_hashes_key(block_number, address),
                            );
                        }
                        Ok(())
                    })() {
                        // TODO: defensive range-delete for composite-keyed
                        // orphans (NONCE_UPDATES_COLUMN, STORAGE_UPDATES_COLUMN) when the
                        // decode above fails. The block-keyed CF deletes below still fire, but
                        // composite-keyed rows tied to this block remain leaked until a proper
                        // range-delete lands.
                        tracing::warn!(
                            block_number,
                            error = %e,
                            "Failed to decode orphaned state update blob for targeted \
                             nonce/storage cleanup; falling back to block-keyed CF deletion only"
                        );
                    }
                }

                batch.delete_cf(&state_updates_cf, key);
                batch.delete_cf(&txs_cf, key);
                batch.delete_cf(&events_cf, key);
            }
        }
    }

    // 4. Trie-orphan reconcile. Runs on every startup, not nested under the
    // block-orphan guard: a reorg-crash leaves rocks_top < sqlite_top and
    // would skip the guard, and the atomic re-seed inside each per-CF call
    // also needs to fire after a fresh-install migration boot.
    let sqlite_top_u64 = sqlite_highest.unwrap_or(0);

    reconcile_trie_column(
        rocksdb,
        connection,
        sqlite_top_u64,
        TrieColumn::Class,
        "SELECT root_index, block_number FROM class_roots WHERE root_index IS NOT NULL AND \
         block_number <= ? ORDER BY root_index DESC LIMIT 1",
        &mut batch,
    )?;
    reconcile_trie_column(
        rocksdb,
        connection,
        sqlite_top_u64,
        TrieColumn::Contract,
        "SELECT root_index, block_number FROM contract_roots WHERE root_index IS NOT NULL AND \
         block_number <= ? ORDER BY root_index DESC LIMIT 1",
        &mut batch,
    )?;
    reconcile_trie_column(
        rocksdb,
        connection,
        sqlite_top_u64,
        TrieColumn::Storage,
        "SELECT root_index, block_number FROM storage_roots WHERE root_index IS NOT NULL AND \
         block_number <= ? ORDER BY root_index DESC LIMIT 1",
        &mut batch,
    )?;

    // 5. Single atomic write covering block-orphan and trie-orphan.
    rocksdb
        .rocksdb
        .write(&batch)
        .context("Writing reconcile batch to RocksDB")?;

    Ok(())
}

/// Returns the current schema version of the existing database,
/// or `0` if database does not yet exist.
fn schema_version(connection: &rusqlite::Connection) -> anyhow::Result<usize> {
    // We store the schema version in the Sqlite provided PRAGMA "user_version",
    // which stores an INTEGER and defaults to 0.
    let version = connection.query_row(
        &format!("SELECT {VERSION_KEY} FROM pragma_user_version;"),
        [],
        |row| row.get_usize(0),
    )?;
    Ok(version)
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::LazyLock;

    use rstest::rstest;
    use test_utils::*;

    use super::*;
    static EVENT_FILTERS_BLOCK_RANGE_LIMIT: LazyLock<NonZeroUsize> =
        LazyLock::new(|| NonZeroUsize::new(100).unwrap());

    use crate::connection::{
        encode_stored_node_for_test,
        StoredNode,
        TrieStorageIndex,
        STATE_UPDATES_COLUMN,
        TRIE_CLASS_COLUMN,
        TRIE_CONTRACT_COLUMN,
        TRIE_NEXT_INDEX_COLUMN,
        TRIE_STORAGE_COLUMN,
    };

    /// Builds a RocksDBInner + fresh in-memory SQLite connection with all
    /// migrations applied. Returns the tempdir owner alongside so callers can
    /// keep it alive for the duration of the test.
    fn setup_trie_reconcile_scaffold(
    ) -> (tempfile::TempDir, Arc<RocksDBInner>, rusqlite::Connection) {
        let rocksdb_dir = tempfile::tempdir().unwrap();
        let rocksdb = Arc::new(StorageBuilder::open_rocksdb(rocksdb_dir.path(), None).unwrap());
        let mut conn = rusqlite::Connection::open_in_memory().unwrap();
        setup_connection(&mut conn, JournalMode::Rollback).unwrap();
        migrate_database(&mut conn, &rocksdb).unwrap();
        (rocksdb_dir, rocksdb, conn)
    }

    /// Seeds a minimum `block_headers` row for the given block number.
    fn seed_block_header(conn: &mut rusqlite::Connection, block: u64) {
        // Derive a unique hash per block number so callers that seed
        // multi-block windows do not trip the UNIQUE constraint on
        // `block_headers.hash`.
        let mut hash_bytes = [0u8; 32];
        hash_bytes[24..].copy_from_slice(&block.to_be_bytes());
        let tx = conn.transaction().unwrap();
        tx.execute(
            "INSERT INTO block_headers (number, hash, parent_hash, timestamp, eth_l1_gas_price, \
             strk_l1_gas_price, eth_l1_data_gas_price, strk_l1_data_gas_price, eth_l2_gas_price, \
             strk_l2_gas_price, sequencer_address, version, transaction_commitment, \
             event_commitment, state_commitment, transaction_count, event_count, l1_da_mode, \
             receipt_commitment, state_diff_commitment, state_diff_length) VALUES (?, ?, \
             zeroblob(32), 0, zeroblob(16), NULL, NULL, NULL, NULL, NULL, zeroblob(32), NULL, \
             zeroblob(32), zeroblob(32), zeroblob(32), 0, 0, 0, zeroblob(32), NULL, 0)",
            rusqlite::params![block as i64, hash_bytes.to_vec()],
        )
        .unwrap();
        tx.commit().unwrap();
    }

    /// Inserts (or replaces) one row in `{table}_roots`. `root_idx = None`
    /// seeds a `TrieEmpty` row (root_index IS NULL). `contract_roots` has an
    /// extra `contract_address BLOB NOT NULL` column that the reconcile SQL
    /// never reads, so we plug a zero-filled 32-byte placeholder.
    fn seed_root_row(
        conn: &mut rusqlite::Connection,
        table: &str,
        block: u64,
        root_idx: Option<u64>,
    ) {
        let tx = conn.transaction().unwrap();
        if table == "contract" {
            tx.execute(
                "INSERT INTO contract_roots (block_number, contract_address, root_index) VALUES \
                 (?, zeroblob(32), ?)",
                rusqlite::params![block as i64, root_idx.map(|v| v as i64)],
            )
            .unwrap();
        } else {
            let sql = format!(
                "INSERT OR REPLACE INTO {table}_roots (block_number, root_index) VALUES (?, ?)"
            );
            tx.execute(
                &sql,
                rusqlite::params![block as i64, root_idx.map(|v| v as i64)],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }

    /// Stores a `StoredNode` at the given index in the given trie CF. Layout
    /// matches `write_trie_entry`: 32 bytes of zeroed hash prefix followed
    /// by the encoded node body.
    fn write_stored_node(
        rocksdb: &RocksDBInner,
        trie_cf: &crate::columns::Column,
        idx: u64,
        node: &StoredNode,
    ) {
        let mut buf = vec![0u8; 32 + 4096];
        let written = encode_stored_node_for_test(node, &mut buf[32..])
            .expect("encoding stored node for test");
        buf.truncate(32 + written);
        let cf = rocksdb.get_column(trie_cf);
        let mut batch = crate::RocksDBBatch::default();
        batch.put_cf(&cf, idx.to_be_bytes(), &buf);
        rocksdb.rocksdb.write(&batch).unwrap();
    }

    /// Writes the on-disk `TRIE_NEXT_INDEX_COLUMN` value for a trie CF.
    fn seed_trie_next_index(rocksdb: &RocksDBInner, trie_cf: &crate::columns::Column, value: u64) {
        let idx_cf = rocksdb.get_column(&TRIE_NEXT_INDEX_COLUMN);
        let mut batch = crate::RocksDBBatch::default();
        batch.put_cf(&idx_cf, trie_cf.name.as_bytes(), value.to_be_bytes());
        rocksdb.rocksdb.write(&batch).unwrap();
    }

    /// Reads the on-disk `TRIE_NEXT_INDEX_COLUMN` value for a trie CF.
    fn read_trie_next_index(rocksdb: &RocksDBInner, trie_cf: &crate::columns::Column) -> u64 {
        let idx_cf = rocksdb.get_column(&TRIE_NEXT_INDEX_COLUMN);
        let raw = rocksdb
            .rocksdb
            .get_pinned_cf(&idx_cf, trie_cf.name.as_bytes())
            .unwrap()
            .expect("counter present");
        let bytes: [u8; 8] = raw.as_ref().try_into().unwrap();
        u64::from_be_bytes(bytes)
    }

    /// Reads the in-memory atomic counter for a trie CF.
    fn read_atomic_counter(rocksdb: &RocksDBInner, trie_cf: &crate::columns::Column) -> u64 {
        let trie_col = TrieColumn::from_column(trie_cf)
            .unwrap_or_else(|| panic!("not a trie CF: {}", trie_cf.name));
        let counter = match trie_col {
            TrieColumn::Class => &rocksdb.trie_class_next_index,
            TrieColumn::Contract => &rocksdb.trie_contract_next_index,
            TrieColumn::Storage => &rocksdb.trie_storage_next_index,
        };
        counter.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Assert a raw node is absent (proves `delete_range_cf` fired against it).
    fn assert_trie_index_missing(
        rocksdb: &RocksDBInner,
        trie_cf: &crate::columns::Column,
        idx: u64,
    ) {
        let cf = rocksdb.get_column(trie_cf);
        let raw = rocksdb
            .rocksdb
            .get_pinned_cf(&cf, idx.to_be_bytes())
            .unwrap();
        assert!(
            raw.is_none(),
            "expected index {idx} to be absent from {}",
            trie_cf.name
        );
    }

    /// Assert a raw node is present (proves it survived reconcile).
    fn assert_trie_index_present(
        rocksdb: &RocksDBInner,
        trie_cf: &crate::columns::Column,
        idx: u64,
    ) {
        let cf = rocksdb.get_column(trie_cf);
        let raw = rocksdb
            .rocksdb
            .get_pinned_cf(&cf, idx.to_be_bytes())
            .unwrap();
        assert!(
            raw.is_some(),
            "expected index {idx} to be present in {}",
            trie_cf.name
        );
    }

    #[test]
    fn schema_version_defaults_to_zero() {
        let mut conn = rusqlite::Connection::open_in_memory().unwrap();
        let transaction = conn.transaction().unwrap();

        let version = schema_version(&transaction).unwrap();
        assert_eq!(version, 0);
    }

    #[test]
    fn full_migration() {
        let mut conn = rusqlite::Connection::open_in_memory().unwrap();
        setup_connection(&mut conn, JournalMode::Rollback).unwrap();

        let rocksdb_dir = tempfile::TempDir::new().unwrap();
        let rocksdb = StorageBuilder::open_rocksdb(rocksdb_dir.path(), None).unwrap();

        migrate_database(&mut conn, &rocksdb).unwrap();
        let version = schema_version(&conn).unwrap();
        let expected = schema::migrations().len() + schema::BASE_SCHEMA_REVISION;
        assert_eq!(version, expected);
    }

    #[test]
    fn migration_fails_if_db_is_newer() {
        let mut conn = rusqlite::Connection::open_in_memory().unwrap();
        setup_connection(&mut conn, JournalMode::Rollback).unwrap();

        let rocksdb_dir = tempfile::TempDir::new().unwrap();
        let rocksdb = StorageBuilder::open_rocksdb(rocksdb_dir.path(), None).unwrap();

        // Force the schema to a newer version
        let current_version = schema::migrations().len();
        conn.pragma_update(None, VERSION_KEY, (current_version + 1) as i64)
            .unwrap();

        // Migration should fail.
        migrate_database(&mut conn, &rocksdb).unwrap_err();
    }

    #[test]
    fn foreign_keys_are_enforced() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();

        // We first disable foreign key support. Sqlite currently enables this by
        // default, but this may change in the future. So we disable to check
        // that our enable function works regardless of what Sqlite's default
        // is.
        use rusqlite::config::DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY;
        conn.set_db_config(SQLITE_DBCONFIG_ENABLE_FKEY, false)
            .unwrap();

        // Enable foreign key support.
        conn.set_db_config(SQLITE_DBCONFIG_ENABLE_FKEY, true)
            .unwrap();

        // Create tables with a parent-child foreign key requirement.
        conn.execute_batch(
            r"
                    CREATE TABLE parent(
                        id INTEGER PRIMARY KEY
                    );

                    CREATE TABLE child(
                        id INTEGER PRIMARY KEY,
                        parent_id INTEGER NOT NULL REFERENCES parent(id)
                    );
                ",
        )
        .unwrap();

        // Check that foreign keys are enforced.
        conn.execute("INSERT INTO parent (id) VALUES (2)", [])
            .unwrap();
        conn.execute("INSERT INTO child (id, parent_id) VALUES (0, 2)", [])
            .unwrap();
        conn.execute("INSERT INTO child (id, parent_id) VALUES (1, 1)", [])
            .unwrap_err();
    }

    #[test]
    fn rpc_test_db_is_migrated() {
        let (_db_dir, db_path) = rpc_test_db_fixture();

        let database = rusqlite::Connection::open(db_path).unwrap();
        let version = schema_version(&database).unwrap();
        let expected = schema::migrations().len() + schema::BASE_SCHEMA_REVISION;

        assert_eq!(version, expected, "RPC database fixture needs migrating");
    }

    fn rpc_test_db_fixture() -> (tempfile::TempDir, PathBuf) {
        let mut source_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        source_path.push("../rpc/fixtures/mainnet.sqlite");

        let db_dir = tempfile::TempDir::new().unwrap();
        let mut db_path = PathBuf::from(db_dir.path());
        db_path.push("mainnet.sqlite");

        std::fs::copy(&source_path, &db_path).unwrap();

        (db_dir, db_path)
    }

    #[test]
    fn enabling_merkle_trie_pruning_fails_without_flag() {
        let (_db_dir, db_path) = rpc_test_db_fixture();

        assert_eq!(
            StorageBuilder::file(db_path)
                .trie_prune_mode(Some(TriePruneMode::Prune {
                    num_blocks_kept: 10
                }))
                .migrate()
                .unwrap_err()
                .to_string(),
            "Cannot enable Merkle trie pruning on a database that was not created with it enabled."
        );
    }

    #[test]
    fn running_event_filter_rebuilt_after_shutdown() {
        let n_blocks = 6;
        let transactions_per_block = 2;
        let headers = create_blocks(n_blocks);
        let transactions_and_receipts =
            create_transactions_and_receipts(n_blocks, transactions_per_block);
        let emitted_events =
            extract_events(&headers, &transactions_and_receipts, transactions_per_block);
        let insert_block_data = |tx: &Transaction<'_>, idx: usize| {
            let header = &headers[idx];

            tx.insert_block_header(header).unwrap();
            tx.insert_transaction_data(
                header.number,
                &transactions_and_receipts
                    [idx * transactions_per_block..(idx + 1) * transactions_per_block]
                    .iter()
                    .cloned()
                    .map(|(tx, receipt, ..)| (tx, receipt))
                    .collect::<Vec<_>>(),
                Some(
                    &transactions_and_receipts
                        [idx * transactions_per_block..(idx + 1) * transactions_per_block]
                        .iter()
                        .cloned()
                        .map(|(_, _, events)| events)
                        .collect::<Vec<_>>(),
                ),
            )
            .unwrap();
        };

        // Use a file-based temp directory so that RocksDB data survives
        // the drop-and-reopen cycle that simulates a restart.
        let db_dir = tempfile::tempdir().unwrap();
        let db_path = db_dir.path().join("test.sqlite");

        // First run starts here...
        let db = crate::StorageBuilder::file(db_path.clone())
            .journal_mode(JournalMode::Rollback)
            .migrate()
            .unwrap()
            .create_pool(NonZeroU32::new(5).unwrap())
            .unwrap();

        let mut conn = db.connection().unwrap();
        let tx = conn.transaction().unwrap();

        // ...we add two blocks.
        for i in 0..2 {
            insert_block_data(&tx, i);
        }
        tx.flush_rocksdb_batch().unwrap();

        let constraints = EventConstraints {
            keys: vec![
                vec![],
                // Key present in all events as the 2nd key.
                vec![pathfinder_common::macro_prelude::event_key!("0xdeadbeef")],
            ],
            page_size: emitted_events.len(),
            ..Default::default()
        };

        let events_before = tx
            .events(&constraints, *EVENT_FILTERS_BLOCK_RANGE_LIMIT)
            .unwrap()
            .events;

        // Pretend like we shut down by dropping these.
        tx.commit().unwrap();
        drop(conn);
        drop(db);

        // Second run starts here (same database)...
        let db = crate::StorageBuilder::file(db_path.clone())
            .journal_mode(JournalMode::Rollback)
            .migrate()
            .unwrap()
            .create_pool(NonZeroU32::new(5).unwrap())
            .unwrap();

        let mut conn = db.connection().unwrap();
        let tx = conn.transaction().unwrap();

        // ...we add the rest of the blocks.
        for i in 2..headers.len() {
            insert_block_data(&tx, i);
        }
        tx.flush_rocksdb_batch().unwrap();

        let events_after = tx
            .events(&constraints, *EVENT_FILTERS_BLOCK_RANGE_LIMIT)
            .unwrap()
            .events;

        let raw_conn = rusqlite::Connection::open(&db_path).unwrap();
        let inserted_event_filter_count = raw_conn
            .prepare("SELECT COUNT(*) FROM event_filters")
            .unwrap()
            .query_row([], |row| row.get_u64(0))
            .unwrap();

        // We are using only the running event filter.
        assert!(inserted_event_filter_count == 0);
        assert!(events_after.len() > events_before.len());
        // Events added in the first run are present in the running event filter.
        for e in events_before {
            assert!(events_after.contains(&e));
        }
    }

    #[test]
    fn reconcile_rocksdb_purges_orphaned_blocks() {
        use pathfinder_common::macro_prelude::*;
        use pathfinder_common::{BlockHeader, BlockNumber};

        use crate::connection::{
            EVENTS_COLUMN,
            STATE_UPDATES_COLUMN,
            TRANSACTIONS_AND_RECEIPTS_COLUMN,
            TRANSACTION_HASHES_COLUMN,
        };

        // Construct a raw SQLite connection and a RocksDBInner directly,
        // following the same pattern as the `full_migration` test.
        let rocksdb_dir = tempfile::tempdir().unwrap();
        let rocksdb = StorageBuilder::open_rocksdb(rocksdb_dir.path(), None).unwrap();
        let mut conn = rusqlite::Connection::open_in_memory().unwrap();
        setup_connection(&mut conn, JournalMode::Rollback).unwrap();
        migrate_database(&mut conn, &rocksdb).unwrap();

        // Seed SQLite with block 5 header.
        let header5 = BlockHeader::builder()
            .number(BlockNumber::new_or_panic(5))
            .finalize_with_hash(block_hash!("0x5"));
        {
            let tx = conn.transaction().unwrap();
            tx.execute(
                "INSERT INTO block_headers (number, hash, parent_hash, timestamp, \
                 eth_l1_gas_price, strk_l1_gas_price, eth_l1_data_gas_price, \
                 strk_l1_data_gas_price, eth_l2_gas_price, strk_l2_gas_price, sequencer_address, \
                 version, transaction_commitment, event_commitment, state_commitment, \
                 transaction_count, event_count, l1_da_mode, receipt_commitment, \
                 state_diff_commitment, state_diff_length) VALUES (5, ?, zeroblob(32), 0, \
                 zeroblob(16), NULL, NULL, NULL, NULL, NULL, zeroblob(32), NULL, zeroblob(32), \
                 zeroblob(32), zeroblob(32), 0, 0, 0, zeroblob(32), NULL, 0)",
                [header5.hash.0.as_be_bytes().to_vec()],
            )
            .unwrap();
            // Write block 5 state update to RocksDB so the reconciler sees it.
            let state_cf = rocksdb.get_column(&STATE_UPDATES_COLUMN);
            let mut batch = crate::RocksDBBatch::default();
            batch.put_cf(&state_cf, 5u64.to_be_bytes(), b"dummy5");
            rocksdb.rocksdb.write(&batch).unwrap();
            tx.commit().unwrap();
        }

        // Write block 6 data directly to RocksDB (no SQLite header), simulating
        // the post-RocksDB / pre-SQLite-commit crash state.
        {
            let mut batch = crate::RocksDBBatch::default();
            let key = 6u64.to_be_bytes();
            batch.put_cf(&rocksdb.get_column(&STATE_UPDATES_COLUMN), key, b"dummy");
            batch.put_cf(
                &rocksdb.get_column(&TRANSACTIONS_AND_RECEIPTS_COLUMN),
                key,
                b"dummy",
            );
            batch.put_cf(&rocksdb.get_column(&EVENTS_COLUMN), key, b"dummy");
            let tx_hash = transaction_hash!("0xabc");
            let mut value = [0u8; 10];
            value[..8].copy_from_slice(&key);
            value[8..].copy_from_slice(&0u16.to_be_bytes());
            batch.put_cf(
                &rocksdb.get_column(&TRANSACTION_HASHES_COLUMN),
                tx_hash.0.as_be_bytes(),
                value,
            );
            rocksdb.rocksdb.write(&batch).unwrap();
        }

        crate::reconcile_rocksdb_with_sqlite(&mut conn, &rocksdb).unwrap();

        // Block 6 must be gone from every CF the reconciler covers; block 5 stays.
        assert!(rocksdb
            .rocksdb
            .get_pinned_cf(
                &rocksdb.get_column(&STATE_UPDATES_COLUMN),
                6u64.to_be_bytes()
            )
            .unwrap()
            .is_none());
        assert!(rocksdb
            .rocksdb
            .get_pinned_cf(
                &rocksdb.get_column(&TRANSACTIONS_AND_RECEIPTS_COLUMN),
                6u64.to_be_bytes()
            )
            .unwrap()
            .is_none());
        assert!(rocksdb
            .rocksdb
            .get_pinned_cf(&rocksdb.get_column(&EVENTS_COLUMN), 6u64.to_be_bytes())
            .unwrap()
            .is_none());
        // Block 5 state update must still be present.
        assert!(rocksdb
            .rocksdb
            .get_pinned_cf(
                &rocksdb.get_column(&STATE_UPDATES_COLUMN),
                5u64.to_be_bytes()
            )
            .unwrap()
            .is_some());

        // Tx hash entry survives because the transactions blob (b"dummy") can't
        // be decoded, so the reconciler falls back to block-keyed CF deletion
        // only. This is a known limitation: orphaned tx_hash entries from
        // corrupt crash blobs are harmless since read paths validate against
        // SQLite.
        let tx_hash = transaction_hash!("0xabc");
        assert!(rocksdb
            .rocksdb
            .get_pinned_cf(
                &rocksdb.get_column(&TRANSACTION_HASHES_COLUMN),
                tx_hash.0.as_be_bytes()
            )
            .unwrap()
            .is_some());
    }

    #[test]
    fn reconciles_orphan_trie_indices_across_all_three_cfs() {
        // Confirmed blocks at 5 for each trie CF, pointing at a valid 2-node
        // batch [K, K+1] per CF. Crashed batch would have allocated
        // [K+2..K+5). Counter is bumped past that. Reconcile must range-
        // delete indices in [K+2..K+5) for every CF and rewind the
        // counter to K+2.
        let (_dir, rocksdb, mut conn) = setup_trie_reconcile_scaffold();
        seed_block_header(&mut conn, 5);

        let root_k: u64 = 10;
        let child_k = root_k + 1;
        let counter_after_crash = root_k + 5; // 3 orphan indices [K+2..K+5).

        for (table, trie_cf) in [
            ("class", &TRIE_CLASS_COLUMN),
            ("contract", &TRIE_CONTRACT_COLUMN),
            ("storage", &TRIE_STORAGE_COLUMN),
        ] {
            // Confirmed batch: Binary(root -> child, LeafBinary).
            write_stored_node(
                &rocksdb,
                trie_cf,
                root_k,
                &StoredNode::Binary {
                    left: TrieStorageIndex::new(child_k).unwrap(),
                    right: TrieStorageIndex::new(child_k).unwrap(),
                },
            );
            write_stored_node(&rocksdb, trie_cf, child_k, &StoredNode::LeafBinary);
            // Orphan nodes at [K+2..K+5).
            for i in 0..3u64 {
                write_stored_node(&rocksdb, trie_cf, root_k + 2 + i, &StoredNode::LeafBinary);
            }
            seed_root_row(&mut conn, table, 5, Some(root_k));
            seed_trie_next_index(&rocksdb, trie_cf, counter_after_crash);
        }

        reconcile_rocksdb_with_sqlite(&mut conn, &rocksdb).unwrap();

        for trie_cf in [
            &TRIE_CLASS_COLUMN,
            &TRIE_CONTRACT_COLUMN,
            &TRIE_STORAGE_COLUMN,
        ] {
            assert_trie_index_present(&rocksdb, trie_cf, root_k);
            assert_trie_index_present(&rocksdb, trie_cf, child_k);
            for i in 0..3u64 {
                assert_trie_index_missing(&rocksdb, trie_cf, root_k + 2 + i);
            }
            assert_eq!(read_trie_next_index(&rocksdb, trie_cf), child_k + 1);
            assert_eq!(read_atomic_counter(&rocksdb, trie_cf), child_k + 1);
        }
    }

    #[test]
    fn crash_affects_only_one_cf() {
        // Confirmed rows in all three CFs. Only TRIE_STORAGE has a bumped
        // counter with orphans; TRIE_CLASS and TRIE_CONTRACT are aligned.
        let (_dir, rocksdb, mut conn) = setup_trie_reconcile_scaffold();
        seed_block_header(&mut conn, 5);

        let clean_root = 10u64;
        for (table, trie_cf) in [
            ("class", &TRIE_CLASS_COLUMN),
            ("contract", &TRIE_CONTRACT_COLUMN),
        ] {
            write_stored_node(&rocksdb, trie_cf, clean_root, &StoredNode::LeafBinary);
            seed_root_row(&mut conn, table, 5, Some(clean_root));
            seed_trie_next_index(&rocksdb, trie_cf, clean_root + 1);
        }

        // Storage: confirmed at [10], orphans at [11..14).
        write_stored_node(
            &rocksdb,
            &TRIE_STORAGE_COLUMN,
            clean_root,
            &StoredNode::LeafBinary,
        );
        for i in 0..3u64 {
            write_stored_node(
                &rocksdb,
                &TRIE_STORAGE_COLUMN,
                11 + i,
                &StoredNode::LeafBinary,
            );
        }
        seed_root_row(&mut conn, "storage", 5, Some(clean_root));
        seed_trie_next_index(&rocksdb, &TRIE_STORAGE_COLUMN, 14);

        reconcile_rocksdb_with_sqlite(&mut conn, &rocksdb).unwrap();

        // Class + contract untouched.
        for trie_cf in [&TRIE_CLASS_COLUMN, &TRIE_CONTRACT_COLUMN] {
            assert_trie_index_present(&rocksdb, trie_cf, clean_root);
            assert_eq!(read_trie_next_index(&rocksdb, trie_cf), clean_root + 1);
            assert_eq!(read_atomic_counter(&rocksdb, trie_cf), clean_root + 1);
        }
        // Storage orphans gone; counter rewound.
        assert_trie_index_present(&rocksdb, &TRIE_STORAGE_COLUMN, clean_root);
        for i in 0..3u64 {
            assert_trie_index_missing(&rocksdb, &TRIE_STORAGE_COLUMN, 11 + i);
        }
        assert_eq!(read_trie_next_index(&rocksdb, &TRIE_STORAGE_COLUMN), 11);
        assert_eq!(read_atomic_counter(&rocksdb, &TRIE_STORAGE_COLUMN), 11);
    }

    #[test]
    fn clean_shutdown_dfs_finds_aligned_tail() {
        // Every CF is exactly aligned; counter = tail + 1. Reconcile is a
        // no-op for the trie CFs and the atomics land equal to the disk value.
        let (_dir, rocksdb, mut conn) = setup_trie_reconcile_scaffold();
        seed_block_header(&mut conn, 5);

        let root = 42u64;
        for (table, trie_cf) in [
            ("class", &TRIE_CLASS_COLUMN),
            ("contract", &TRIE_CONTRACT_COLUMN),
            ("storage", &TRIE_STORAGE_COLUMN),
        ] {
            write_stored_node(
                &rocksdb,
                trie_cf,
                root,
                &StoredNode::Edge {
                    child: TrieStorageIndex::new(root + 1).unwrap(),
                    path: bitvec::bitvec![u8, bitvec::order::Msb0; 1, 0, 1],
                },
            );
            write_stored_node(&rocksdb, trie_cf, root + 1, &StoredNode::LeafBinary);
            seed_root_row(&mut conn, table, 5, Some(root));
            seed_trie_next_index(&rocksdb, trie_cf, root + 2);
        }

        // STATE_UPDATES[5] present, matches block_headers top.
        {
            let cf = rocksdb.get_column(&STATE_UPDATES_COLUMN);
            let mut batch = crate::RocksDBBatch::default();
            batch.put_cf(&cf, 5u64.to_be_bytes(), b"dummy5");
            rocksdb.rocksdb.write(&batch).unwrap();
        }

        reconcile_rocksdb_with_sqlite(&mut conn, &rocksdb).unwrap();

        for trie_cf in [
            &TRIE_CLASS_COLUMN,
            &TRIE_CONTRACT_COLUMN,
            &TRIE_STORAGE_COLUMN,
        ] {
            assert_trie_index_present(&rocksdb, trie_cf, root);
            assert_trie_index_present(&rocksdb, trie_cf, root + 1);
            assert_eq!(read_trie_next_index(&rocksdb, trie_cf), root + 2);
            assert_eq!(read_atomic_counter(&rocksdb, trie_cf), root + 2);
        }
    }

    #[test]
    fn reorg_crash_reconciles_orphans() {
        // Reorg-crash aftermath: block_headers retains 0..=10, STATE_UPDATES
        // retains 0..=5 (blocks 6..=10 were staged-deleted by purge_block).
        // Trie CFs contain orphans at [K..K+3). Counter is durably bumped.
        // rocks_top (5) < sqlite_top (10), so the block-orphan guard would
        // skip — trie reconcile must still fire.
        let (_dir, rocksdb, mut conn) = setup_trie_reconcile_scaffold();
        for b in 0..=10u64 {
            seed_block_header(&mut conn, b);
        }
        {
            let cf = rocksdb.get_column(&STATE_UPDATES_COLUMN);
            let mut batch = crate::RocksDBBatch::default();
            for b in 0..=5u64 {
                batch.put_cf(&cf, b.to_be_bytes(), b"dummy");
            }
            rocksdb.rocksdb.write(&batch).unwrap();
        }

        let k = 100u64;
        for (table, trie_cf) in [
            ("class", &TRIE_CLASS_COLUMN),
            ("contract", &TRIE_CONTRACT_COLUMN),
            ("storage", &TRIE_STORAGE_COLUMN),
        ] {
            write_stored_node(&rocksdb, trie_cf, k, &StoredNode::LeafBinary);
            for i in 0..3u64 {
                write_stored_node(&rocksdb, trie_cf, k + 1 + i, &StoredNode::LeafBinary);
            }
            seed_root_row(&mut conn, table, 10, Some(k));
            seed_trie_next_index(&rocksdb, trie_cf, k + 4);
        }

        reconcile_rocksdb_with_sqlite(&mut conn, &rocksdb).unwrap();

        for trie_cf in [
            &TRIE_CLASS_COLUMN,
            &TRIE_CONTRACT_COLUMN,
            &TRIE_STORAGE_COLUMN,
        ] {
            assert_trie_index_present(&rocksdb, trie_cf, k);
            for i in 0..3u64 {
                assert_trie_index_missing(&rocksdb, trie_cf, k + 1 + i);
            }
            assert_eq!(read_trie_next_index(&rocksdb, trie_cf), k + 1);
            assert_eq!(read_atomic_counter(&rocksdb, trie_cf), k + 1);
        }
    }

    #[test]
    fn null_max_root_falls_back_safely() {
        // Confirmed row for class_roots at block 5 has root_index = NULL
        // (TrieEmpty). The SQL query filters `root_index IS NOT NULL`, so
        // no row matches: fallback engaged, orphans persist, atomic reflects
        // the still-bumped disk value.
        let (_dir, rocksdb, mut conn) = setup_trie_reconcile_scaffold();
        seed_block_header(&mut conn, 5);

        seed_root_row(&mut conn, "class", 5, None);
        for i in 1..3u64 {
            write_stored_node(&rocksdb, &TRIE_CLASS_COLUMN, i, &StoredNode::LeafBinary);
        }
        seed_trie_next_index(&rocksdb, &TRIE_CLASS_COLUMN, 3);

        reconcile_rocksdb_with_sqlite(&mut conn, &rocksdb).unwrap();

        for i in 1..3u64 {
            assert_trie_index_present(&rocksdb, &TRIE_CLASS_COLUMN, i);
        }
        assert_eq!(read_trie_next_index(&rocksdb, &TRIE_CLASS_COLUMN), 3);
        assert_eq!(read_atomic_counter(&rocksdb, &TRIE_CLASS_COLUMN), 3);
    }

    #[test]
    fn corrupt_node_aborts_dfs_safely() {
        // Two branches: (a) missing node at root; (b) 20-byte value shorter
        // than the 32-byte hash prefix. Both must abort the DFS and leave
        // orphans in place.
        let (_dir, rocksdb, mut conn) = setup_trie_reconcile_scaffold();
        seed_block_header(&mut conn, 5);

        // (a) MAX(root_index) = 10, but no node at index 10.
        seed_root_row(&mut conn, "class", 5, Some(10));
        seed_trie_next_index(&rocksdb, &TRIE_CLASS_COLUMN, 15);
        // Seed an orphan-shaped node at 12 so we can prove it survives.
        write_stored_node(&rocksdb, &TRIE_CLASS_COLUMN, 12, &StoredNode::LeafBinary);

        reconcile_rocksdb_with_sqlite(&mut conn, &rocksdb).unwrap();

        assert_trie_index_missing(&rocksdb, &TRIE_CLASS_COLUMN, 10); // still absent
        assert_trie_index_present(&rocksdb, &TRIE_CLASS_COLUMN, 12);
        assert_eq!(read_trie_next_index(&rocksdb, &TRIE_CLASS_COLUMN), 15);
        assert_eq!(read_atomic_counter(&rocksdb, &TRIE_CLASS_COLUMN), 15);

        // (b) Now seed a bogus 20-byte value at index 10 to exercise the
        // length-guard branch, using a separate scaffold.
        let (_dir2, rocksdb2, mut conn2) = setup_trie_reconcile_scaffold();
        seed_block_header(&mut conn2, 5);
        seed_root_row(&mut conn2, "class", 5, Some(10));
        seed_trie_next_index(&rocksdb2, &TRIE_CLASS_COLUMN, 15);
        {
            let cf = rocksdb2.get_column(&TRIE_CLASS_COLUMN);
            let mut batch = crate::RocksDBBatch::default();
            batch.put_cf(&cf, 10u64.to_be_bytes(), [0u8; 20]);
            rocksdb2.rocksdb.write(&batch).unwrap();
        }

        reconcile_rocksdb_with_sqlite(&mut conn2, &rocksdb2).unwrap();

        // The 20-byte value at 10 persists — no range-delete fired.
        let cf = rocksdb2.get_column(&TRIE_CLASS_COLUMN);
        assert!(rocksdb2
            .rocksdb
            .get_pinned_cf(&cf, 10u64.to_be_bytes())
            .unwrap()
            .is_some());
        assert_eq!(read_trie_next_index(&rocksdb2, &TRIE_CLASS_COLUMN), 15);
        assert_eq!(read_atomic_counter(&rocksdb2, &TRIE_CLASS_COLUMN), 15);
    }

    #[test]
    fn genesis_window_empty_roots_non_zero_counter() {
        // Roots tables empty; counter = 5. Represents a pre-genesis migration
        // write or a genesis-window crash. Fallback engages, orphans persist.
        let (_dir, rocksdb, mut conn) = setup_trie_reconcile_scaffold();
        // No block_headers row: sqlite_highest = None => sqlite_top_u64 = 0.
        seed_trie_next_index(&rocksdb, &TRIE_CLASS_COLUMN, 5);
        for i in 0..5u64 {
            write_stored_node(&rocksdb, &TRIE_CLASS_COLUMN, i, &StoredNode::LeafBinary);
        }

        reconcile_rocksdb_with_sqlite(&mut conn, &rocksdb).unwrap();

        for i in 0..5u64 {
            assert_trie_index_present(&rocksdb, &TRIE_CLASS_COLUMN, i);
        }
        assert_eq!(read_trie_next_index(&rocksdb, &TRIE_CLASS_COLUMN), 5);
        assert_eq!(read_atomic_counter(&rocksdb, &TRIE_CLASS_COLUMN), 5);
    }

    #[test]
    fn end_to_end_via_real_insert_trie() {
        // Exercise the DFS against real writer bytes: run three real
        // insert_class_trie calls, then simulate a crashed batch by writing
        // three dummy nodes above the counter and bumping it. Reconcile
        // rewinds cleanly.
        use std::num::NonZeroU32;

        let db_dir = tempfile::tempdir().unwrap();
        let db_path = db_dir.path().join("test.sqlite");

        let (post_counter, storage_manager) = {
            let mut mgr = crate::StorageBuilder::file(db_path.clone())
                .journal_mode(JournalMode::Rollback)
                .migrate()
                .unwrap();
            let db = mgr.create_pool(NonZeroU32::new(2).unwrap()).unwrap();
            // Run three real inserts across three blocks — one leaf-root
            // per block suffices to bump the counter.
            for b in 0..3u64 {
                // Unique per-block hash so the `block_headers.hash` UNIQUE
                // constraint doesn't fire across the three iterations.
                let mut hash_bytes = [0u8; 32];
                hash_bytes[24..].copy_from_slice(&b.to_be_bytes());
                let block_hash = pathfinder_common::BlockHash(
                    pathfinder_crypto::Felt::from_be_slice(&hash_bytes).unwrap(),
                );
                let header = pathfinder_common::BlockHeader::builder()
                    .number(pathfinder_common::BlockNumber::new_or_panic(b))
                    .finalize_with_hash(block_hash);
                let mut conn = db.connection().unwrap();
                let tx = conn.transaction().unwrap();
                tx.insert_block_header(&header).unwrap();
                let update = crate::connection::TrieUpdate {
                    nodes_added: vec![(
                        pathfinder_common::macro_prelude::felt!("0x1"),
                        crate::connection::Node::LeafBinary,
                    )],
                    nodes_removed: vec![],
                    root_commitment: pathfinder_common::macro_prelude::felt!("0x1"),
                };
                let root_update = tx.insert_class_trie(&update, header.number).unwrap();
                tx.insert_class_root(header.number, root_update).unwrap();
                tx.commit().unwrap();
            }
            // Cache the counter observed after three real inserts.
            let counter = read_trie_next_index(db.rocksdb_inner(), &TRIE_CLASS_COLUMN);
            (counter, db)
        };

        assert!(
            post_counter >= 3,
            "post-insert counter must be >= 3, got {post_counter}"
        );

        // Simulate a crashed batch: three dummy nodes + counter bump by 3.
        let db = &storage_manager;
        let inner = db.rocksdb_inner();
        for i in 0..3u64 {
            write_stored_node(
                inner,
                &TRIE_CLASS_COLUMN,
                post_counter + i,
                &StoredNode::LeafBinary,
            );
        }
        seed_trie_next_index(inner, &TRIE_CLASS_COLUMN, post_counter + 3);

        // Re-invoke reconcile directly.
        let mut raw_conn = rusqlite::Connection::open(&db_path).unwrap();
        setup_connection(&mut raw_conn, JournalMode::Rollback).unwrap();
        reconcile_rocksdb_with_sqlite(&mut raw_conn, inner).unwrap();

        for i in 0..3u64 {
            assert_trie_index_missing(inner, &TRIE_CLASS_COLUMN, post_counter + i);
        }
        assert_eq!(
            read_trie_next_index(inner, &TRIE_CLASS_COLUMN),
            post_counter
        );
        assert_eq!(read_atomic_counter(inner, &TRIE_CLASS_COLUMN), post_counter);
    }

    #[test]
    fn idempotent_across_double_run() {
        // Same seed as `reconciles_orphan_trie_indices_across_all_three_cfs`;
        // reconcile invoked twice back-to-back. Post-condition matches the
        // canonical test's single-run post-condition.
        let (_dir, rocksdb, mut conn) = setup_trie_reconcile_scaffold();
        seed_block_header(&mut conn, 5);

        let root_k: u64 = 10;
        let child_k = root_k + 1;
        let counter_after_crash = root_k + 5;

        for (table, trie_cf) in [
            ("class", &TRIE_CLASS_COLUMN),
            ("contract", &TRIE_CONTRACT_COLUMN),
            ("storage", &TRIE_STORAGE_COLUMN),
        ] {
            write_stored_node(
                &rocksdb,
                trie_cf,
                root_k,
                &StoredNode::Binary {
                    left: TrieStorageIndex::new(child_k).unwrap(),
                    right: TrieStorageIndex::new(child_k).unwrap(),
                },
            );
            write_stored_node(&rocksdb, trie_cf, child_k, &StoredNode::LeafBinary);
            for i in 0..3u64 {
                write_stored_node(&rocksdb, trie_cf, root_k + 2 + i, &StoredNode::LeafBinary);
            }
            seed_root_row(&mut conn, table, 5, Some(root_k));
            seed_trie_next_index(&rocksdb, trie_cf, counter_after_crash);
        }

        reconcile_rocksdb_with_sqlite(&mut conn, &rocksdb).unwrap();
        reconcile_rocksdb_with_sqlite(&mut conn, &rocksdb).unwrap();

        for trie_cf in [
            &TRIE_CLASS_COLUMN,
            &TRIE_CONTRACT_COLUMN,
            &TRIE_STORAGE_COLUMN,
        ] {
            assert_trie_index_present(&rocksdb, trie_cf, root_k);
            assert_trie_index_present(&rocksdb, trie_cf, child_k);
            for i in 0..3u64 {
                assert_trie_index_missing(&rocksdb, trie_cf, root_k + 2 + i);
            }
            assert_eq!(read_trie_next_index(&rocksdb, trie_cf), child_k + 1);
            assert_eq!(read_atomic_counter(&rocksdb, trie_cf), child_k + 1);
        }
    }

    #[test]
    fn empty_rocksdb_and_empty_sqlite_are_noop() {
        // Fresh temp-dir Storage, nothing written. Reconcile must not
        // panic, must not stage any writes, and atomics stay at 0.
        let (_dir, rocksdb, mut conn) = setup_trie_reconcile_scaffold();

        reconcile_rocksdb_with_sqlite(&mut conn, &rocksdb).unwrap();

        for trie_cf in [
            &TRIE_CLASS_COLUMN,
            &TRIE_CONTRACT_COLUMN,
            &TRIE_STORAGE_COLUMN,
        ] {
            assert_eq!(read_atomic_counter(&rocksdb, trie_cf), 0);
        }
    }

    #[test]
    fn stale_atomic_refreshed_after_migration_write() {
        // Simulates fresh-install migration boot: disk counter =
        // MAX(idx)+1, atomic still 0 (stale). Reconcile refreshes the
        // atomic to match disk. No range-delete fires (DFS finds an
        // aligned tail).
        let (_dir, rocksdb, mut conn) = setup_trie_reconcile_scaffold();
        seed_block_header(&mut conn, 5);

        let batch_base = 1229u64;
        let batch_size = 5u64;

        // Write a 5-node batch: root at 1233 -> LeafBinary at 1229..=1232.
        // Use a Binary root with two children pointing at 1231 and 1230, then
        // an Edge at 1232 pointing at 1229; wire so DFS reaches all five.
        write_stored_node(&rocksdb, &TRIE_CLASS_COLUMN, 1229, &StoredNode::LeafBinary);
        write_stored_node(&rocksdb, &TRIE_CLASS_COLUMN, 1230, &StoredNode::LeafBinary);
        write_stored_node(
            &rocksdb,
            &TRIE_CLASS_COLUMN,
            1231,
            &StoredNode::Edge {
                child: TrieStorageIndex::new(1229).unwrap(),
                path: bitvec::bitvec![u8, bitvec::order::Msb0; 1],
            },
        );
        write_stored_node(
            &rocksdb,
            &TRIE_CLASS_COLUMN,
            1232,
            &StoredNode::Edge {
                child: TrieStorageIndex::new(1230).unwrap(),
                path: bitvec::bitvec![u8, bitvec::order::Msb0; 0],
            },
        );
        write_stored_node(
            &rocksdb,
            &TRIE_CLASS_COLUMN,
            1233,
            &StoredNode::Binary {
                left: TrieStorageIndex::new(1231).unwrap(),
                right: TrieStorageIndex::new(1232).unwrap(),
            },
        );

        seed_root_row(&mut conn, "class", 5, Some(1233));
        seed_trie_next_index(&rocksdb, &TRIE_CLASS_COLUMN, batch_base + batch_size);
        // Force the atomic stale (simulate migration boot before reconcile).
        rocksdb
            .trie_class_next_index
            .store(0, std::sync::atomic::Ordering::SeqCst);

        reconcile_rocksdb_with_sqlite(&mut conn, &rocksdb).unwrap();

        // Counter unchanged on disk, no range-delete.
        for i in 0..batch_size {
            assert_trie_index_present(&rocksdb, &TRIE_CLASS_COLUMN, batch_base + i);
        }
        assert_eq!(
            read_trie_next_index(&rocksdb, &TRIE_CLASS_COLUMN),
            batch_base + batch_size
        );
        // Atomic refreshed to disk value.
        assert_eq!(
            read_atomic_counter(&rocksdb, &TRIE_CLASS_COLUMN),
            batch_base + batch_size
        );
    }

    #[test]
    fn in_memory_storage_cleans_up_rocksdb_tempdir() {
        let rocksdb_dir;
        {
            let storage = crate::StorageBuilder::in_memory().unwrap();
            rocksdb_dir = storage
                .rocksdb_tempdir_path()
                .expect("in-memory storage should have a RocksDB tempdir");
            assert!(
                rocksdb_dir.exists(),
                "RocksDB tempdir should exist while storage is alive"
            );
        }
        assert!(
            !rocksdb_dir.exists(),
            "in-memory storage leaked RocksDB tempdir: {}",
            rocksdb_dir.display()
        );
    }

    #[test]
    fn migrate_rejects_in_memory_uri_without_preassigned_tempdir() {
        let uri = PathBuf::from("file:memdb_no_tempdir?mode=memory&cache=shared");
        let Err(err) = crate::StorageBuilder::file(uri).migrate() else {
            panic!("migrate() must reject an in-memory URI without a preassigned tempdir");
        };
        assert!(
            err.to_string().contains("preassigned RocksDB tempdir"),
            "error should mention the missing preassigned tempdir, got: {err}"
        );
    }

    #[test]
    fn readonly_open_does_not_lock_out_read_write() {
        // RocksDB read-only mode must not take the exclusive LOCK file, or
        // a support tool cannot open a read-only handle against a live
        // pathfinder node's on-disk data.
        use std::num::NonZeroU32;

        use pathfinder_common::BlockNumber;

        use crate::{JournalMode, StorageBuilder};

        let db_dir = tempfile::tempdir().unwrap();
        let db_path = db_dir.path().join("test.sqlite");

        // 1. Open read-write, write one block, keep the handle alive.
        let rw = StorageBuilder::file(db_path.clone())
            .journal_mode(JournalMode::Rollback)
            .migrate()
            .unwrap()
            .create_pool(NonZeroU32::new(2).unwrap())
            .unwrap();
        {
            let mut conn = rw.connection().unwrap();
            let tx = conn.transaction().unwrap();
            let headers = create_blocks(1);
            tx.insert_block_header(&headers[0]).unwrap();
            // Persist any RocksDB writes so the subsequent read-only handle
            // can observe them.
            tx.flush_rocksdb_batch().unwrap();
            tx.commit().unwrap();
        }

        // 2. With the read-write handle still alive, opening read-only must succeed.
        //    Before the fix this fails with a RocksDB LOCK error.
        let ro = StorageBuilder::file(db_path.clone())
            .readonly()
            .expect("readonly open must not be blocked by the live read-write handle");
        let ro_pool = ro
            .create_read_only_pool(NonZeroU32::new(1).unwrap())
            .unwrap();

        // 3. Read the block back through both handles.
        {
            let mut conn = rw.connection().unwrap();
            let tx = conn.transaction().unwrap();
            assert!(tx.block_id(BlockNumber::GENESIS.into()).unwrap().is_some());
        }
        {
            let mut conn = ro_pool.connection().unwrap();
            let tx = conn.transaction().unwrap();
            assert!(tx.block_id(BlockNumber::GENESIS.into()).unwrap().is_some());
        }

        // 4. Reverse the ordering: open the read-only handle first, keep it alive, then
        //    open a read-write handle on top. RW takes the LOCK, so this would fail if
        //    RO were secretly holding it.
        drop(ro_pool);
        drop(rw);

        let ro2 = StorageBuilder::file(db_path.clone()).readonly().unwrap();
        let ro2_pool = ro2
            .create_read_only_pool(NonZeroU32::new(1).unwrap())
            .unwrap();
        let mut rw2 = StorageBuilder::file(db_path.clone())
            .journal_mode(JournalMode::Rollback)
            .migrate()
            .expect("read-write open after read-only must succeed");
        let rw2_pool = rw2.create_pool(NonZeroU32::new(1).unwrap()).unwrap();

        // 5. Drop every handle, reopen read-write once more; no orphan LOCK. Dropping
        //    the pools alone is not enough: the StorageManager keeps an
        //    Arc<RocksDBInner> alive, which would still hold the LOCK.
        drop(rw2_pool);
        drop(rw2);
        drop(ro2_pool);
        drop(ro2);

        let rw3 = StorageBuilder::file(db_path)
            .journal_mode(JournalMode::Rollback)
            .migrate()
            .expect("read-write open after all handles dropped must succeed");
        drop(rw3);
    }

    #[rstest]
    #[case::block_before_full_range(AGGREGATE_BLOOM_BLOCK_RANGE_LEN - 1, 0)]
    #[case::full_block_range(AGGREGATE_BLOOM_BLOCK_RANGE_LEN, 1)]
    #[case::block_after_full_range(AGGREGATE_BLOOM_BLOCK_RANGE_LEN + 1, 1)]
    fn rebuild_running_event_filter_edge_cases(
        #[case] n_blocks: u64,
        #[case] expected_insert_count: u64,
    ) {
        let n_blocks = usize::try_from(n_blocks).unwrap();
        let transactions_per_block = 1;
        let headers = create_blocks(n_blocks);
        let transactions_and_receipts =
            create_transactions_and_receipts(n_blocks, transactions_per_block);
        let emitted_events =
            extract_events(&headers, &transactions_and_receipts, transactions_per_block);
        let events_per_block = emitted_events.len() / n_blocks;

        let insert_block_data = |tx: &Transaction<'_>, idx: usize| {
            let header = &headers[idx];

            tx.insert_block_header(header).unwrap();
            tx.insert_transaction_data(
                header.number,
                &transactions_and_receipts
                    [idx * transactions_per_block..(idx + 1) * transactions_per_block]
                    .iter()
                    .cloned()
                    .map(|(tx, receipt, ..)| (tx, receipt))
                    .collect::<Vec<_>>(),
                Some(
                    &transactions_and_receipts
                        [idx * transactions_per_block..(idx + 1) * transactions_per_block]
                        .iter()
                        .cloned()
                        .map(|(_, _, events)| events)
                        .collect::<Vec<_>>(),
                ),
            )
            .unwrap();
        };

        // Use a file-based temp directory so that RocksDB data survives
        // the drop-and-reopen cycle that simulates a restart.
        let db_dir = tempfile::tempdir().unwrap();
        let db_path = db_dir.path().join("test.sqlite");

        let db = crate::StorageBuilder::file(db_path.clone())
            .journal_mode(JournalMode::Rollback)
            .migrate()
            .unwrap()
            .create_pool(NonZeroU32::new(5).unwrap())
            .unwrap();

        let mut conn = db.connection().unwrap();
        let tx = conn.transaction().unwrap();

        for i in 0..n_blocks {
            insert_block_data(&tx, i);
        }

        // Pretend like we shut down by dropping these.
        tx.commit().unwrap();
        drop(conn);
        drop(db);

        let db = crate::StorageBuilder::file(db_path.clone())
            .journal_mode(JournalMode::Rollback)
            .migrate()
            .unwrap()
            .create_pool(NonZeroU32::new(5).unwrap())
            .unwrap();

        let mut conn = db.connection().unwrap();
        let tx = conn.transaction().unwrap();

        let to_block = BlockNumber::GENESIS + n_blocks as u64;

        let constraints = EventConstraints {
            from_block: None,
            to_block: Some(to_block),
            contract_addresses: vec![],
            keys: vec![],
            page_size: 1024,
            offset: 0,
        };

        let events = tx
            .events(&constraints, *EVENT_FILTERS_BLOCK_RANGE_LIMIT)
            .unwrap()
            .events;

        let raw_conn = rusqlite::Connection::open(&db_path).unwrap();
        let inserted_event_filter_count = raw_conn
            .prepare("SELECT COUNT(*) FROM event_filters")
            .unwrap()
            .query_row([], |row| row.get_u64(0))
            .unwrap();

        assert_eq!(inserted_event_filter_count, expected_insert_count);

        let expected = &emitted_events[..events_per_block * n_blocks];
        assert_eq!(events, expected);
    }
}
