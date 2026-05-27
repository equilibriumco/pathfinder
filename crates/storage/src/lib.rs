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
    /// Keeps the RocksDB tempdir alive for in-memory databases.
    /// `None` for file-based databases (RocksDB lives next to the SQLite file).
    _rocksdb_tempdir: Option<Arc<tempfile::TempDir>>,
}

pub(crate) struct RocksDBInner {
    rocksdb: RocksDB,
    options: rust_rocksdb::Options,
    trie_class_next_index: std::sync::atomic::AtomicU64,
    trie_contract_next_index: std::sync::atomic::AtomicU64,
    trie_storage_next_index: std::sync::atomic::AtomicU64,
}

impl RocksDBInner {
    fn next_trie_storage_index(
        &self,
        column: &Column,
        number_of_indices_to_allocate: usize,
    ) -> TrieStorageIndex {
        let next_index = match column.name {
            name if name == crate::connection::TRIE_CLASS_COLUMN.name => {
                self.trie_class_next_index.fetch_add(
                    number_of_indices_to_allocate as u64,
                    std::sync::atomic::Ordering::SeqCst,
                )
            }
            name if name == crate::connection::TRIE_CONTRACT_COLUMN.name => {
                self.trie_contract_next_index.fetch_add(
                    number_of_indices_to_allocate as u64,
                    std::sync::atomic::Ordering::SeqCst,
                )
            }
            name if name == crate::connection::TRIE_STORAGE_COLUMN.name => {
                self.trie_storage_next_index.fetch_add(
                    number_of_indices_to_allocate as u64,
                    std::sync::atomic::Ordering::SeqCst,
                )
            }
            _ => panic!("Invalid column for trie storage index generation"),
        };
        TrieStorageIndex(next_index)
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
    /// Keeps the RocksDB tempdir alive for in-memory databases.
    rocksdb_tempdir: Option<Arc<tempfile::TempDir>>,
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
            _rocksdb_tempdir: self.rocksdb_tempdir.clone(),
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
}

impl StorageBuilder {
    pub fn file(database_path: PathBuf) -> Self {
        Self {
            database_path,
            journal_mode: JournalMode::WAL,
            event_filter_cache_size: 16,
            trie_prune_mode: None,
            blockchain_history_mode: None,
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

        let mut storage = Self::file(database_path)
            .journal_mode(JournalMode::Rollback)
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

        let mut storage = Self::file(database_path)
            .journal_mode(JournalMode::Rollback)
            .migrate()?;

        if let BlockchainHistoryMode::Prune { num_blocks_kept } = blockchain_history_mode {
            conn.execute(
                "INSERT INTO storage_options (option, value) VALUES ('prune_blockchain', ?)",
                [num_blocks_kept],
            )?;
        }

        storage.blockchain_history_mode = blockchain_history_mode;
        storage.create_pool(pool_size)
    }

    /// A workaround for scenarios where a test requires multiple parallel
    /// connections and shared cache causes locking errors if the connection
    /// pool is larger than 1 and timeouts otherwise.
    pub fn in_tempdir() -> anyhow::Result<Storage> {
        let tempdir = Arc::new(tempfile::tempdir()?);
        tracing::trace!("Creating storage in: {}", tempdir.path().display());
        let mut manager = crate::StorageBuilder::file(tempdir.path().join("db.sqlite"))
            .migrate()
            .unwrap();
        // Keep the tempdir alive for the lifetime of Storage so that the
        // RocksDB directory (which lives inside it) is not deleted.
        manager.rocksdb_tempdir = Some(tempdir);
        manager.create_pool(NonZeroU32::new(32).unwrap())
    }

    /// Convenience function for tests to create an in-tempdir database with a
    /// specific trie prune mode.
    pub fn in_tempdir_with_trie_pruning_and_pool_size(
        trie_prune_mode: TriePruneMode,
        pool_size: NonZeroU32,
    ) -> anyhow::Result<Storage> {
        let tempdir = Arc::new(tempfile::tempdir()?);
        tracing::trace!("Creating storage in: {}", tempdir.path().display());
        let mut manager = crate::StorageBuilder::file(tempdir.path().join("db.sqlite"))
            .trie_prune_mode(Some(trie_prune_mode))
            .migrate()
            .unwrap();
        // Keep the tempdir alive for the lifetime of Storage so that the
        // RocksDB directory (which lives inside it) is not deleted.
        manager.rocksdb_tempdir = Some(tempdir);
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
    pub fn migrate(self) -> anyhow::Result<StorageManager> {
        let (rocksdb_path, rocksdb_tempdir) = if self
            .database_path
            .to_str()
            .is_some_and(|s| s.starts_with("file:memdb"))
        {
            // in-memory SQLite database — RocksDB needs a real filesystem path,
            // so we create a temporary directory and keep the handle alive.
            let tmpdir =
                tempfile::tempdir().context("Creating RocksDB tempdir for in-memory database")?;
            let path = tmpdir.path().to_path_buf();
            (path, Some(Arc::new(tmpdir)))
        } else {
            (self.database_path.with_extension("rocksdb"), None)
        };
        let rocksdb = Arc::new(Self::open_rocksdb(&rocksdb_path)?);

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
            rocksdb_tempdir,
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
                |row| row.get(0),
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
        // TODO: open RocksDB read-only here (DB::open_cf_for_read_only) to
        // avoid taking a write lock that could conflict with a running node.
        let (rocksdb_path, rocksdb_tempdir) = if database_path
            .to_str()
            .is_some_and(|s| s.starts_with("file:memdb"))
        {
            let tmpdir =
                tempfile::tempdir().context("Creating RocksDB tempdir for in-memory database")?;
            let path = tmpdir.path().to_path_buf();
            (path, Some(Arc::new(tmpdir)))
        } else {
            (database_path.with_extension("rocksdb"), None)
        };
        let rocksdb = Arc::new(Self::open_rocksdb(&rocksdb_path)?);

        // Lightweight consistency check: warn if RocksDB is ahead of SQLite.
        // In readonly mode we cannot fix the inconsistency, but we should alert
        // the operator so they can run in normal mode first.
        {
            use crate::connection::STATE_UPDATES_COLUMN;
            let sqlite_highest: Option<u64> = connection
                .query_row("SELECT MAX(number) FROM block_headers", [], |row| {
                    row.get::<_, Option<u64>>(0)
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
            rocksdb_tempdir,
            pending_prune: None,
        }))
    }

    pub(crate) fn open_rocksdb(path: &Path) -> anyhow::Result<RocksDBInner> {
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
        let cache = rust_rocksdb::Cache::new_hyper_clock_cache(2 * 1024 * 1024 * 1024, 0);

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
        let column_handle = db
            .cf_handle(TRIE_NEXT_INDEX_COLUMN.name)
            .context("Getting RocksDB column for fetching next trie storage index")?;
        let next_index = db
            .get_cf(&column_handle, column.name.as_bytes())?
            .map(|value| -> anyhow::Result<u64> {
                let bytes: [u8; 8] = value.as_slice().try_into().map_err(|_| {
                    anyhow::anyhow!(
                        "RocksDB trie storage index value has invalid length: {}",
                        value.len()
                    )
                })?;
                Ok(u64::from_be_bytes(bytes))
            })
            .transpose()?;
        Ok(next_index.unwrap_or(0))
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
                |row| row.get(0),
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
                [num_blocks_kept],
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
                    |row| row.get(0),
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
            ._rocksdb_tempdir
            .as_ref()
            .map(|d| d.path().to_path_buf())
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
        tx.pragma_update(None, VERSION_KEY, schema::BASE_SCHEMA_REVISION)
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
                    .pragma_update(None, VERSION_KEY, current_revision)
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
            row.get::<_, Option<u64>>(0)
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

    let Some(rocks_top) = rocksdb_highest else {
        return Ok(());
    };

    let purge_from = match sqlite_highest {
        Some(sqlite_top) if rocks_top <= sqlite_top => return Ok(()),
        Some(sqlite_top) => sqlite_top + 1,
        None => 0,
    };

    tracing::warn!(
        ?sqlite_highest,
        rocks_top,
        "RocksDB is ahead of SQLite -- purging orphaned blocks"
    );

    // 3. Build a delete batch covering every orphaned block.
    let mut batch = crate::RocksDBBatch::default();
    let txs_cf = rocksdb.get_column(&TRANSACTIONS_AND_RECEIPTS_COLUMN);
    let events_cf = rocksdb.get_column(&EVENTS_COLUMN);
    let hashes_cf = rocksdb.get_column(&TRANSACTION_HASHES_COLUMN);
    let nonce_cf = rocksdb.get_column(&NONCE_UPDATES_COLUMN);
    let storage_cf = rocksdb.get_column(&STORAGE_UPDATES_COLUMN);
    let csh_cf = rocksdb.get_column(&CONTRACT_STATE_HASHES_COLUMN);

    for block_number in purge_from..=rocks_top {
        let key = block_number.to_be_bytes();

        // Tx hashes: try to read the transactions blob to learn which hashes
        // to drop. If decompression or decoding fails (e.g. partial/corrupt
        // blob from a crash), log a warning and fall back to deleting only the
        // block-keyed CFs.
        if let Some(blob) = rocksdb
            .rocksdb
            .get_pinned_cf(&txs_cf, key)
            .context("Reading orphaned transactions blob")?
        {
            if let Err(e) = (|| -> anyhow::Result<()> {
                let decompressed =
                    crate::connection::transaction::compression::decompress_transactions(&blob)
                        .context("Decompressing orphaned transactions blob")?;
                let (txs, _): (
                    crate::connection::transaction::dto::TransactionsWithReceiptsForBlock,
                    _,
                ) = bincode::serde::decode_from_slice(&decompressed, bincode::config::standard())
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
                     These are harmless as read paths validate against SQLite."
                );
            }
        }

        // State / nonce / storage: try to drive deletes from the STATE_UPDATES blob.
        if let Some(blob) = rocksdb
            .rocksdb
            .get_pinned_cf(&state_updates_cf, key)
            .context("Reading orphaned state update blob")?
        {
            if let Err(e) = (|| -> anyhow::Result<()> {
                let (data, _): (crate::connection::state_update::dto::StateUpdateData, _) =
                    bincode::serde::decode_from_slice(&blob, bincode::config::standard())
                        .context("Decoding orphaned state update blob")?;
                let block_number = pathfinder_common::BlockNumber::new_or_panic(block_number);
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
                    batch.delete_cf(&csh_cf, contract_state_hashes_key(block_number, address));
                }
                for address in data.system_contract_updates.keys() {
                    batch.delete_cf(&csh_cf, contract_state_hashes_key(block_number, address));
                }
                Ok(())
            })() {
                tracing::warn!(
                    block_number,
                    error = %e,
                    "Failed to decode orphaned state update blob for targeted nonce/storage \
                     cleanup; falling back to block-keyed CF deletion only"
                );
            }
        }

        batch.delete_cf(&state_updates_cf, key);
        batch.delete_cf(&txs_cf, key);
        batch.delete_cf(&events_cf, key);
    }

    rocksdb
        .rocksdb
        .write(&batch)
        .context("Writing orphan-block delete batch to RocksDB")?;
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
        |row| row.get::<_, usize>(0),
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
        let rocksdb = StorageBuilder::open_rocksdb(rocksdb_dir.path()).unwrap();

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
        let rocksdb = StorageBuilder::open_rocksdb(rocksdb_dir.path()).unwrap();

        // Force the schema to a newer version
        let current_version = schema::migrations().len();
        conn.pragma_update(None, VERSION_KEY, current_version + 1)
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
            .query_row([], |row| row.get::<_, u64>(0))
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
        let rocksdb = StorageBuilder::open_rocksdb(rocksdb_dir.path()).unwrap();
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
            .query_row([], |row| row.get::<_, u64>(0))
            .unwrap();

        assert_eq!(inserted_event_filter_count, expected_insert_count);

        let expected = &emitted_events[..events_per_block * n_blocks];
        assert_eq!(events, expected);
    }
}
