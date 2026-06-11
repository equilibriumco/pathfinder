use std::collections::{HashMap, HashSet};

use anyhow::Context;
use pathfinder_common::state_update::{
    ContractClassUpdate,
    ContractUpdate,
    StateUpdateData,
    SystemContractUpdate,
};
use pathfinder_common::{
    CasmHash,
    ClassHash,
    ContractAddress,
    ContractNonce,
    SierraHash,
    StorageAddress,
    StorageValue,
};
use pathfinder_crypto::Felt;

use crate::connection::state_update::{dto, STATE_UPDATES_COLUMN};
use crate::{
    NONCE_UPDATES_COLUMN,
    STORAGE_UPDATES_COLUMN,
    TRANSACTIONS_AND_RECEIPTS_COLUMN,
    TRANSACTION_HASHES_COLUMN,
};

/// Wraps a `rusqlite::Rows<'stmt>` cursor with a typed decode closure and a
/// one-slot peek buffer. Used by `migrate_state_updates` to k-way-merge six
/// `ORDER BY block_number` cursors without unifying their row types.
struct PeekableRows<'stmt, T, F>
where
    F: FnMut(&rusqlite::Row<'_>) -> anyhow::Result<T>,
{
    rows: rusqlite::Rows<'stmt>,
    decode: F,
    peeked: Peeked<T>,
}

enum Peeked<T> {
    /// Cursor has been fully drained.
    Empty,
    /// `peek_block_number` loaded a row that `next_row` has not yet consumed.
    Buffered(T),
    /// Initial state; need to pull from `rows` on next access.
    Fresh,
}

trait HasBlockNumber {
    fn block_number(&self) -> u64;
}

impl<'stmt, T: HasBlockNumber, F> PeekableRows<'stmt, T, F>
where
    F: FnMut(&rusqlite::Row<'_>) -> anyhow::Result<T>,
{
    fn new(rows: rusqlite::Rows<'stmt>, decode: F) -> Self {
        Self {
            rows,
            decode,
            peeked: Peeked::Fresh,
        }
    }

    fn fill(&mut self) -> anyhow::Result<()> {
        if matches!(self.peeked, Peeked::Fresh) {
            self.peeked = match self.rows.next()? {
                Some(row) => Peeked::Buffered((self.decode)(row)?),
                None => Peeked::Empty,
            };
        }
        Ok(())
    }

    fn peek_block_number(&mut self) -> anyhow::Result<Option<u64>> {
        self.fill()?;
        Ok(match &self.peeked {
            Peeked::Buffered(row) => Some(row.block_number()),
            Peeked::Empty => None,
            Peeked::Fresh => unreachable!("fill() above transitions out of Fresh"),
        })
    }

    fn next_row(&mut self) -> anyhow::Result<Option<T>> {
        self.fill()?;
        Ok(match std::mem::replace(&mut self.peeked, Peeked::Fresh) {
            Peeked::Buffered(row) => Some(row),
            Peeked::Empty => {
                self.peeked = Peeked::Empty;
                None
            }
            Peeked::Fresh => unreachable!("fill() above transitions out of Fresh"),
        })
    }
}

pub(crate) fn migrate(
    tx: &rusqlite::Transaction<'_>,
    rocksdb: &crate::RocksDBInner,
) -> anyhow::Result<()> {
    tracing::info!("Migrating state updates to RocksDB");
    migrate_state_updates(tx, rocksdb)?;

    tracing::info!("Migrating nonce updates to RocksDB");
    migrate_nonce_updates(tx, rocksdb)?;

    tracing::info!("Migrating storage updates to RocksDB");
    migrate_storage_updates(tx, rocksdb)?;

    tracing::info!("Migrating transactions and receipts to RocksDB");
    migrate_transactions_and_receipts(tx, rocksdb)?;

    tracing::info!("Migrating transaction hashes to RocksDB");
    migrate_transaction_hashes(tx, rocksdb)?;

    tracing::info!("Migrating events to RocksDB");
    migrate_events(tx, rocksdb)?;

    Ok(())
}

fn parse_felt(bytes: &[u8], what: &'static str) -> anyhow::Result<Felt> {
    Felt::from_be_slice(bytes).with_context(|| format!("Parsing {what} felt"))
}

struct NonceRow {
    block_number: u64,
    contract_address: [u8; 32],
    nonce: Vec<u8>,
}
impl HasBlockNumber for NonceRow {
    fn block_number(&self) -> u64 {
        self.block_number
    }
}

struct StorageRow {
    block_number: u64,
    contract_address: [u8; 32],
    storage_address: [u8; 32],
    storage_value: Vec<u8>,
}
impl HasBlockNumber for StorageRow {
    fn block_number(&self) -> u64 {
        self.block_number
    }
}

struct DeclaredRow {
    block_number: u64,
    class_hash: [u8; 32],
    casm_hash: Option<[u8; 32]>,
}
impl HasBlockNumber for DeclaredRow {
    fn block_number(&self) -> u64 {
        self.block_number
    }
}

struct RedeclaredRow {
    block_number: u64,
    class_hash: [u8; 32],
}
impl HasBlockNumber for RedeclaredRow {
    fn block_number(&self) -> u64 {
        self.block_number
    }
}

struct MigratedRow {
    block_number: u64,
    class_hash: [u8; 32],
    casm_hash: [u8; 32],
}
impl HasBlockNumber for MigratedRow {
    fn block_number(&self) -> u64 {
        self.block_number
    }
}

struct ContractRow {
    block_number: u64,
    contract_address: [u8; 32],
    class_hash: [u8; 32],
}
impl HasBlockNumber for ContractRow {
    fn block_number(&self) -> u64 {
        self.block_number
    }
}

fn decode_nonce_row(row: &rusqlite::Row<'_>) -> anyhow::Result<NonceRow> {
    let block_number: u64 = row.get(0)?;
    let contract_address: [u8; 32] = row.get(1)?;
    let nonce: Vec<u8> = row.get(2)?;
    Ok(NonceRow {
        block_number,
        contract_address,
        nonce,
    })
}

fn decode_storage_row(row: &rusqlite::Row<'_>) -> anyhow::Result<StorageRow> {
    let block_number: u64 = row.get(0)?;
    let contract_address: [u8; 32] = row.get(1)?;
    let storage_address: [u8; 32] = row.get(2)?;
    let storage_value: Vec<u8> = row.get(3)?;
    Ok(StorageRow {
        block_number,
        contract_address,
        storage_address,
        storage_value,
    })
}

fn decode_declared_row(row: &rusqlite::Row<'_>) -> anyhow::Result<DeclaredRow> {
    let block_number: u64 = row.get(0)?;
    let class_hash: [u8; 32] = row.get(1)?;
    let casm_hash: Option<[u8; 32]> = row.get(2)?;
    Ok(DeclaredRow {
        block_number,
        class_hash,
        casm_hash,
    })
}

fn decode_redeclared_row(row: &rusqlite::Row<'_>) -> anyhow::Result<RedeclaredRow> {
    let block_number: u64 = row.get(0)?;
    let class_hash: [u8; 32] = row.get(1)?;
    Ok(RedeclaredRow {
        block_number,
        class_hash,
    })
}

fn decode_migrated_row(row: &rusqlite::Row<'_>) -> anyhow::Result<MigratedRow> {
    let block_number: u64 = row.get(0)?;
    let class_hash: [u8; 32] = row.get(1)?;
    let casm_hash: [u8; 32] = row.get(2)?;
    Ok(MigratedRow {
        block_number,
        class_hash,
        casm_hash,
    })
}

fn decode_contract_row(row: &rusqlite::Row<'_>) -> anyhow::Result<ContractRow> {
    let block_number: u64 = row.get(0)?;
    let contract_address: [u8; 32] = row.get(1)?;
    let class_hash: [u8; 32] = row.get(2)?;
    Ok(ContractRow {
        block_number,
        contract_address,
        class_hash,
    })
}

struct PeekableBlocks<'stmt> {
    rows: rusqlite::Rows<'stmt>,
}

impl<'stmt> PeekableBlocks<'stmt> {
    fn new(rows: rusqlite::Rows<'stmt>) -> Self {
        Self { rows }
    }

    fn next_block(&mut self) -> anyhow::Result<Option<u64>> {
        match self.rows.next()? {
            Some(row) => Ok(Some(row.get::<_, u64>(0)?)),
            None => Ok(None),
        }
    }
}

fn drain_nonces<F>(
    cursor: &mut PeekableRows<'_, NonceRow, F>,
    block_number: u64,
    contract_updates: &mut HashMap<ContractAddress, ContractUpdate>,
) -> anyhow::Result<()>
where
    F: FnMut(&rusqlite::Row<'_>) -> anyhow::Result<NonceRow>,
{
    while cursor.peek_block_number()? == Some(block_number) {
        let row = cursor.next_row()?.expect("just peeked Some");
        let felt = parse_felt(&row.contract_address, "contract address")?;
        let address = ContractAddress::new(felt).context("Creating contract address")?;
        let nonce = ContractNonce(parse_felt(&row.nonce, "nonce")?);
        contract_updates.entry(address).or_default().nonce = Some(nonce);
    }
    Ok(())
}

fn drain_storages<F>(
    cursor: &mut PeekableRows<'_, StorageRow, F>,
    block_number: u64,
    contract_updates: &mut HashMap<ContractAddress, ContractUpdate>,
    system_contract_updates: &mut HashMap<ContractAddress, SystemContractUpdate>,
) -> anyhow::Result<()>
where
    F: FnMut(&rusqlite::Row<'_>) -> anyhow::Result<StorageRow>,
{
    while cursor.peek_block_number()? == Some(block_number) {
        let row = cursor.next_row()?.expect("just peeked Some");
        let felt = parse_felt(&row.contract_address, "contract address")?;
        let address = ContractAddress::new(felt).context("Creating contract address")?;
        let key = StorageAddress(parse_felt(&row.storage_address, "storage address")?);
        let value = StorageValue(parse_felt(&row.storage_value, "storage value")?);
        if address.is_system_contract() {
            system_contract_updates
                .entry(address)
                .or_default()
                .storage
                .insert(key, value);
        } else {
            contract_updates
                .entry(address)
                .or_default()
                .storage
                .insert(key, value);
        }
    }
    Ok(())
}

fn drain_declareds<F>(
    cursor: &mut PeekableRows<'_, DeclaredRow, F>,
    block_number: u64,
    declared_cairo_classes: &mut HashSet<ClassHash>,
    declared_sierra_classes: &mut HashMap<SierraHash, CasmHash>,
) -> anyhow::Result<()>
where
    F: FnMut(&rusqlite::Row<'_>) -> anyhow::Result<DeclaredRow>,
{
    while cursor.peek_block_number()? == Some(block_number) {
        let row = cursor.next_row()?.expect("just peeked Some");
        let class_felt = parse_felt(&row.class_hash, "class hash")?;
        match row.casm_hash {
            Some(casm_bytes) => {
                let casm_felt = parse_felt(&casm_bytes, "casm hash")?;
                declared_sierra_classes.insert(SierraHash(class_felt), CasmHash(casm_felt));
            }
            None => {
                declared_cairo_classes.insert(ClassHash(class_felt));
            }
        }
    }
    Ok(())
}

fn drain_redeclareds<F>(
    cursor: &mut PeekableRows<'_, RedeclaredRow, F>,
    block_number: u64,
    declared_cairo_classes: &mut HashSet<ClassHash>,
) -> anyhow::Result<()>
where
    F: FnMut(&rusqlite::Row<'_>) -> anyhow::Result<RedeclaredRow>,
{
    while cursor.peek_block_number()? == Some(block_number) {
        let row = cursor.next_row()?.expect("just peeked Some");
        let class_felt = parse_felt(&row.class_hash, "class hash")?;
        declared_cairo_classes.insert(ClassHash(class_felt));
    }
    Ok(())
}

fn drain_migrateds<F>(
    cursor: &mut PeekableRows<'_, MigratedRow, F>,
    block_number: u64,
    seen_class_hashes: &mut HashSet<[u8; 32]>,
    migrated_compiled_classes: &mut HashMap<SierraHash, CasmHash>,
) -> anyhow::Result<()>
where
    F: FnMut(&rusqlite::Row<'_>) -> anyhow::Result<MigratedRow>,
{
    while cursor.peek_block_number()? == Some(block_number) {
        let row = cursor.next_row()?.expect("just peeked Some");
        // `(hash, block_number)` is UNIQUE per revision_0075, so `insert == false`
        // captures the "seen in an earlier block" case directly.
        let already_seen = !seen_class_hashes.insert(row.class_hash);
        if already_seen {
            let class_felt = parse_felt(&row.class_hash, "class hash")?;
            let casm_felt = parse_felt(&row.casm_hash, "casm hash")?;
            migrated_compiled_classes.insert(SierraHash(class_felt), CasmHash(casm_felt));
        }
    }
    Ok(())
}

fn drain_contracts<F>(
    cursor: &mut PeekableRows<'_, ContractRow, F>,
    block_number: u64,
    seen_contracts: &mut HashSet<[u8; 32]>,
    contract_updates: &mut HashMap<ContractAddress, ContractUpdate>,
) -> anyhow::Result<()>
where
    F: FnMut(&rusqlite::Row<'_>) -> anyhow::Result<ContractRow>,
{
    // Collect this block's rows first so within-block duplicates collapse
    // (last write wins, deterministic via the cursor's
    // `ORDER BY block_number, contract_address` tiebreaker).
    let mut block_rows: HashMap<[u8; 32], [u8; 32]> = HashMap::new();
    while cursor.peek_block_number()? == Some(block_number) {
        let row = cursor.next_row()?.expect("just peeked Some");
        block_rows.insert(row.contract_address, row.class_hash);
    }
    // Classify against `seen_contracts` as it was at block start, then bulk-
    // insert the block's addresses afterwards. This preserves the rule that
    // within-block siblings all classify as Deploy.
    for (addr_bytes, class_bytes) in &block_rows {
        let was_seen_before_block = seen_contracts.contains(addr_bytes);
        let class_felt = parse_felt(class_bytes, "class hash")?;
        let class_hash = ClassHash(class_felt);
        let class_update = if was_seen_before_block {
            ContractClassUpdate::Replace(class_hash)
        } else {
            ContractClassUpdate::Deploy(class_hash)
        };
        let addr_felt = parse_felt(addr_bytes, "contract address")?;
        let address = ContractAddress::new(addr_felt).context("Creating contract address")?;
        contract_updates.entry(address).or_default().class = Some(class_update);
    }
    seen_contracts.extend(block_rows.keys().copied());
    Ok(())
}

fn migrate_state_updates(
    tx: &rusqlite::Transaction<'_>,
    rocksdb: &crate::RocksDBInner,
) -> anyhow::Result<()> {
    const STATE_UPDATES_BATCH_SIZE: usize = 10_000;

    let mut block_stmt = tx
        .prepare("SELECT number FROM block_headers ORDER BY number")
        .context("Preparing block numbers query")?;

    let mut nonce_stmt = tx
        .prepare(
            "SELECT nonce_updates.block_number, contract_addresses.contract_address, \
             nonce_updates.nonce FROM nonce_updates JOIN contract_addresses ON \
             contract_addresses.id = nonce_updates.contract_address_id ORDER BY \
             nonce_updates.block_number",
        )
        .context("Preparing nonce updates query")?;

    let mut storage_stmt = tx
        .prepare(
            "SELECT storage_updates.block_number, contract_addresses.contract_address, \
             storage_addresses.storage_address, storage_updates.storage_value FROM \
             storage_updates JOIN contract_addresses ON contract_addresses.id = \
             storage_updates.contract_address_id JOIN storage_addresses ON storage_addresses.id = \
             storage_updates.storage_address_id ORDER BY storage_updates.block_number",
        )
        .context("Preparing storage updates query")?;

    // `class_definitions.block_number` is nullable since revision_0071,
    // and revision_0073 deliberately NULLs out two known-affected classes.
    // The `IS NOT NULL` filter excludes those rows; without it the row
    // decoder would fail on `get::<_, u64>(0)`.
    let mut declared_stmt = tx
        .prepare(
            "SELECT class_definitions.block_number, class_definitions.hash AS class_hash, \
             casm_class_hashes.compiled_class_hash AS compiled_class_hash FROM class_definitions \
             LEFT OUTER JOIN casm_class_hashes ON casm_class_hashes.hash = class_definitions.hash \
             AND casm_class_hashes.block_number = class_definitions.block_number WHERE \
             class_definitions.block_number IS NOT NULL ORDER BY class_definitions.block_number",
        )
        .context("Preparing declared classes query")?;

    let mut redeclared_stmt = tx
        .prepare("SELECT block_number, class_hash FROM redeclared_classes ORDER BY block_number")
        .context("Preparing redeclared classes query")?;

    // `casm_class_hashes.block_number` is nullable (revision_0075 SELECTs
    // from `class_definitions.block_number`, which propagates NULLs). The
    // `(hash, block_number)` UNIQUE INDEX from revision_0075 guarantees no
    // within-block duplicates, so the `seen_class_hashes` rule in
    // `drain_migrateds` covers all cross-block reoccurrences.
    let mut migrated_stmt = tx
        .prepare(
            "SELECT block_number, hash, compiled_class_hash FROM casm_class_hashes WHERE \
             block_number IS NOT NULL ORDER BY block_number",
        )
        .context("Preparing migrated compiled classes query")?;

    // `(block_number, contract_address)` is not unique in `contract_updates`
    // (only an ordinary index from revision_0071). Multiple rows for the same
    // (block, address) classify identically as Deploy; `drain_contracts`
    // dedupes per-block before consulting `seen_contracts` to preserve that.
    // The `contract_address` tiebreaker keeps iteration order deterministic.
    let mut contract_update_stmt = tx
        .prepare(
            "SELECT block_number, contract_address, class_hash FROM contract_updates ORDER BY \
             block_number, contract_address",
        )
        .context("Preparing contract updates query")?;

    let mut blocks = PeekableBlocks::new(block_stmt.query([])?);

    let nonce_rows = nonce_stmt.query([])?;
    let mut nonces = PeekableRows::new(nonce_rows, decode_nonce_row);

    let storage_rows = storage_stmt.query([])?;
    let mut storages = PeekableRows::new(storage_rows, decode_storage_row);

    let declared_rows = declared_stmt.query([])?;
    let mut declareds = PeekableRows::new(declared_rows, decode_declared_row);

    let redeclared_rows = redeclared_stmt.query([])?;
    let mut redeclareds = PeekableRows::new(redeclared_rows, decode_redeclared_row);

    let migrated_rows = migrated_stmt.query([])?;
    let mut migrateds = PeekableRows::new(migrated_rows, decode_migrated_row);

    let contract_rows = contract_update_stmt.query([])?;
    let mut contracts = PeekableRows::new(contract_rows, decode_contract_row);

    let column = rocksdb.get_column(&STATE_UPDATES_COLUMN);
    let mut batch = crate::RocksDBBatch::default();

    let mut seen_class_hashes: HashSet<[u8; 32]> = HashSet::new();
    let mut seen_contracts: HashSet<[u8; 32]> = HashSet::new();

    let mut blocks_processed: u64 = 0;
    while let Some(block_number) = blocks.next_block()? {
        let mut contract_updates: HashMap<ContractAddress, ContractUpdate> = HashMap::new();
        let mut system_contract_updates: HashMap<ContractAddress, SystemContractUpdate> =
            HashMap::new();
        let mut declared_cairo_classes: HashSet<ClassHash> = HashSet::new();
        let mut declared_sierra_classes: HashMap<SierraHash, CasmHash> = HashMap::new();
        let mut migrated_compiled_classes: HashMap<SierraHash, CasmHash> = HashMap::new();

        drain_nonces(&mut nonces, block_number, &mut contract_updates)?;
        drain_storages(
            &mut storages,
            block_number,
            &mut contract_updates,
            &mut system_contract_updates,
        )?;
        drain_declareds(
            &mut declareds,
            block_number,
            &mut declared_cairo_classes,
            &mut declared_sierra_classes,
        )?;
        drain_redeclareds(&mut redeclareds, block_number, &mut declared_cairo_classes)?;
        drain_migrateds(
            &mut migrateds,
            block_number,
            &mut seen_class_hashes,
            &mut migrated_compiled_classes,
        )?;
        drain_contracts(
            &mut contracts,
            block_number,
            &mut seen_contracts,
            &mut contract_updates,
        )?;

        let state_update_data = StateUpdateData {
            contract_updates,
            system_contract_updates,
            declared_cairo_classes,
            declared_sierra_classes,
            migrated_compiled_classes,
        };
        let dto_data = dto::StateUpdateData::from(state_update_data);
        let data = bincode::serde::encode_to_vec(dto_data, bincode::config::standard())
            .context("Encoding state update data")?;

        batch.put_cf(&column, block_number.to_be_bytes(), &data);
        blocks_processed += 1;

        if blocks_processed.is_multiple_of(STATE_UPDATES_BATCH_SIZE as u64) {
            rocksdb
                .rocksdb
                .write_without_wal(&batch)
                .context("Writing state updates batch to RocksDB")?;
            batch = crate::RocksDBBatch::default();
            tracing::info!(blocks_processed, "Migrated state update entries");
        }
    }

    rocksdb
        .rocksdb
        .write_without_wal(&batch)
        .context("Writing final state updates batch to RocksDB")?;

    anyhow::ensure!(
        nonces.peek_block_number()?.is_none(),
        "nonce_updates has rows past the last block_headers row",
    );
    anyhow::ensure!(
        storages.peek_block_number()?.is_none(),
        "storage_updates has rows past the last block_headers row",
    );
    anyhow::ensure!(
        declareds.peek_block_number()?.is_none(),
        "class_definitions has rows past the last block_headers row",
    );
    anyhow::ensure!(
        redeclareds.peek_block_number()?.is_none(),
        "redeclared_classes has rows past the last block_headers row",
    );
    anyhow::ensure!(
        migrateds.peek_block_number()?.is_none(),
        "casm_class_hashes has rows past the last block_headers row",
    );
    anyhow::ensure!(
        contracts.peek_block_number()?.is_none(),
        "contract_updates has rows past the last block_headers row",
    );

    tracing::info!(
        blocks_processed,
        seen_class_hashes = seen_class_hashes.len(),
        seen_contracts = seen_contracts.len(),
        "State updates migration complete",
    );
    Ok(())
}

fn nonce_update_key(block_number: u64, contract_address: &[u8; 32]) -> [u8; 40] {
    let block_number = u64::MAX - block_number;

    let mut key = [0u8; 40];
    key[..32].copy_from_slice(contract_address);
    key[32..].copy_from_slice(&block_number.to_be_bytes());
    key
}

fn storage_update_key(
    block_number: u64,
    contract_address: &[u8; 32],
    storage_address: &[u8; 32],
) -> [u8; 72] {
    let block_number = u64::MAX - block_number;

    let mut key = [0u8; 72];
    key[..32].copy_from_slice(contract_address);
    key[32..64].copy_from_slice(storage_address);
    key[64..].copy_from_slice(&block_number.to_be_bytes());
    key
}

fn migrate_nonce_updates(
    tx: &rusqlite::Transaction<'_>,
    rocksdb: &crate::RocksDBInner,
) -> anyhow::Result<()> {
    const NONCE_UPDATES_BATCH_SIZE: usize = 1_000_000;

    let mut stmt = tx
        .prepare(
            "SELECT nonce_updates.block_number, contract_addresses.contract_address, \
             nonce_updates.nonce FROM nonce_updates JOIN contract_addresses ON \
             contract_addresses.id = nonce_updates.contract_address_id",
        )
        .context("Preparing nonce updates query")?;

    let rows = stmt
        .query_map([], |row| {
            let block_number: u64 = row.get(0)?;
            let contract_address: [u8; 32] = row.get(1)?;
            let nonce: Vec<u8> = row.get(2)?;
            Ok((block_number, contract_address, nonce))
        })
        .context("Querying nonce updates")?;

    let column = rocksdb.get_column(&NONCE_UPDATES_COLUMN);
    let mut batch = crate::RocksDBBatch::default();

    for (i, row) in rows.enumerate() {
        let (block_number, contract_address, nonce) = row.context("Reading nonce update row")?;

        let key = nonce_update_key(block_number, &contract_address);
        batch.put_cf(&column, key, &nonce);

        if i % NONCE_UPDATES_BATCH_SIZE == NONCE_UPDATES_BATCH_SIZE - 1 {
            rocksdb
                .rocksdb
                .write_without_wal(&batch)
                .context("Writing nonce updates batch to RocksDB")?;
            batch = crate::RocksDBBatch::default();
            tracing::info!("Migrated {} nonce update entries", i + 1);
        }
    }

    rocksdb
        .rocksdb
        .write_without_wal(&batch)
        .context("Writing final nonce updates batch to RocksDB")?;
    tracing::info!("Nonce updates migration complete");

    Ok(())
}

fn migrate_storage_updates(
    tx: &rusqlite::Transaction<'_>,
    rocksdb: &crate::RocksDBInner,
) -> anyhow::Result<()> {
    const STORAGE_UPDATES_BATCH_SIZE: usize = 1_000_000;

    let mut stmt = tx
        .prepare(
            "SELECT storage_updates.block_number, contract_addresses.contract_address, \
             storage_addresses.storage_address, storage_updates.storage_value FROM \
             storage_updates JOIN contract_addresses ON contract_addresses.id = \
             storage_updates.contract_address_id JOIN storage_addresses ON storage_addresses.id = \
             storage_updates.storage_address_id",
        )
        .context("Preparing storage updates query")?;

    let rows = stmt
        .query_map([], |row| {
            let block_number: u64 = row.get(0)?;
            let contract_address: [u8; 32] = row.get(1)?;
            let storage_address: [u8; 32] = row.get(2)?;
            let storage_value: Vec<u8> = row.get(3)?;
            Ok((
                block_number,
                contract_address,
                storage_address,
                storage_value,
            ))
        })
        .context("Querying storage updates")?;

    let column = rocksdb.get_column(&STORAGE_UPDATES_COLUMN);
    let mut batch = crate::RocksDBBatch::default();

    for (i, row) in rows.enumerate() {
        let (block_number, contract_address, storage_address, storage_value) =
            row.context("Reading storage update row")?;

        let key = storage_update_key(block_number, &contract_address, &storage_address);
        batch.put_cf(&column, key, &storage_value);

        if i % STORAGE_UPDATES_BATCH_SIZE == STORAGE_UPDATES_BATCH_SIZE - 1 {
            rocksdb
                .rocksdb
                .write_without_wal(&batch)
                .context("Writing storage updates batch to RocksDB")?;
            batch = crate::RocksDBBatch::default();
            tracing::info!("Migrated {} storage update entries", i + 1);
        }
    }

    tracing::info!("Last batch of storage updates with {} entries", batch.len());

    rocksdb
        .rocksdb
        .write_without_wal(&batch)
        .context("Writing final storage updates batch to RocksDB")?;
    tracing::info!("Storage updates migration complete");

    Ok(())
}

fn migrate_transactions_and_receipts(
    tx: &rusqlite::Transaction<'_>,
    rocksdb: &crate::RocksDBInner,
) -> anyhow::Result<()> {
    const TRANSACTIONS_BATCH_SIZE: usize = 10_000;

    let mut stmt = tx
        .prepare("SELECT transactions.block_number, transactions.transactions FROM transactions")
        .context("Preparing transactions and receipts query")?;

    let rows = stmt
        .query_map([], |row| {
            let block_number: u64 = row.get(0)?;
            let data: Vec<u8> = row.get(1)?;
            Ok((block_number, data))
        })
        .context("Querying transactions and receipts")?;

    let column = rocksdb.get_column(&TRANSACTIONS_AND_RECEIPTS_COLUMN);
    let mut batch = crate::RocksDBBatch::default();

    for (i, row) in rows.enumerate() {
        let (block_number, data) = row.context("Reading transactions and receipts row")?;

        let key = block_number.to_be_bytes();
        batch.put_cf(&column, key, &data);

        if i % TRANSACTIONS_BATCH_SIZE == TRANSACTIONS_BATCH_SIZE - 1 {
            rocksdb
                .rocksdb
                .write_without_wal(&batch)
                .context("Writing transactions and receipts batch to RocksDB")?;
            batch = crate::RocksDBBatch::default();
            tracing::info!("Migrated {} transactions and receipts entries", i + 1);
        }
    }

    rocksdb
        .rocksdb
        .write_without_wal(&batch)
        .context("Writing final transactions and receipts batch to RocksDB")?;
    tracing::info!("Transactions and receipts migration complete");

    Ok(())
}

fn migrate_transaction_hashes(
    tx: &rusqlite::Transaction<'_>,
    rocksdb: &crate::RocksDBInner,
) -> anyhow::Result<()> {
    const TRANSACTION_HASHES_BATCH_SIZE: usize = 1_000_000;

    let mut stmt = tx
        .prepare("SELECT hash, block_number, idx FROM transaction_hashes")
        .context("Preparing transaction hashes query")?;

    let rows = stmt
        .query_map([], |row| {
            let hash: Vec<u8> = row.get(0)?;
            let block_number: u64 = row.get(1)?;
            let idx: u16 = row.get(2)?;
            Ok((hash, block_number, idx))
        })
        .context("Querying transaction hashes")?;

    let column = rocksdb.get_column(&TRANSACTION_HASHES_COLUMN);
    let mut batch = crate::RocksDBBatch::default();

    for (i, row) in rows.enumerate() {
        let (hash, block_number, idx) = row.context("Reading transaction hashes row")?;

        let mut buffer = [0u8; 10];
        buffer[..8].copy_from_slice(&block_number.to_be_bytes());
        buffer[8..].copy_from_slice(&idx.to_be_bytes());

        batch.put_cf(&column, hash.as_slice(), buffer);

        if i % TRANSACTION_HASHES_BATCH_SIZE == TRANSACTION_HASHES_BATCH_SIZE - 1 {
            rocksdb
                .rocksdb
                .write_without_wal(&batch)
                .context("Writing transaction hashes batch to RocksDB")?;
            batch = crate::RocksDBBatch::default();
            tracing::info!("Migrated {} transaction hashes entries", i + 1);
        }
    }

    rocksdb
        .rocksdb
        .write_without_wal(&batch)
        .context("Writing final transaction hashes batch to RocksDB")?;
    tracing::info!("Transaction hashes migration complete");

    Ok(())
}

fn migrate_events(
    tx: &rusqlite::Transaction<'_>,
    rocksdb: &crate::RocksDBInner,
) -> anyhow::Result<()> {
    const EVENTS_BATCH_SIZE: usize = 10_000;

    let mut stmt = tx
        .prepare("SELECT transactions.block_number, transactions.events FROM transactions")
        .context("Preparing events query")?;

    let rows = stmt
        .query_map([], |row| {
            let block_number: u64 = row.get(0)?;
            let data: Option<Vec<u8>> = row.get(1)?;
            Ok((block_number, data))
        })
        .context("Querying events")?;

    let column = rocksdb.get_column(&crate::connection::EVENTS_COLUMN);
    let mut batch = crate::RocksDBBatch::default();

    for (i, row) in rows.enumerate() {
        let (block_number, data) = row.context("Reading events row")?;
        let Some(data) = data else { continue };

        let key = block_number.to_be_bytes();
        batch.put_cf(&column, key, &data);

        if i % EVENTS_BATCH_SIZE == EVENTS_BATCH_SIZE - 1 {
            rocksdb
                .rocksdb
                .write_without_wal(&batch)
                .context("Writing events batch to RocksDB")?;
            batch = crate::RocksDBBatch::default();
            tracing::info!("Migrated {} events entries", i + 1);
        }
    }

    rocksdb
        .rocksdb
        .write_without_wal(&batch)
        .context("Writing final events batch to RocksDB")?;
    tracing::info!("Events migration complete");

    Ok(())
}

#[cfg(test)]
mod tests {
    use pathfinder_common::macro_prelude::*;
    use pathfinder_common::state_update::{ContractClassUpdate, ContractUpdate, StateUpdateData};
    use pathfinder_common::{CasmHash, SierraHash};
    use rusqlite::params;

    use crate::connection::state_update::{dto, STATE_UPDATES_COLUMN};
    use crate::connection::TRANSACTION_HASHES_COLUMN;

    /// Bring an in-memory SQLite database to the pre-0079 state and return
    /// `(conn, rocksdb, _tempdir)`. The tempdir is returned so the caller can
    /// keep it alive until the test finishes.
    fn fresh_pre_0079_db() -> (rusqlite::Connection, crate::RocksDBInner, tempfile::TempDir) {
        let rocksdb_dir = tempfile::tempdir().unwrap();
        let rocksdb = crate::StorageBuilder::open_rocksdb(rocksdb_dir.path()).unwrap();

        let mut conn = rusqlite::Connection::open_in_memory().unwrap();
        let tx = conn.transaction().unwrap();
        crate::schema::base_schema(&tx).unwrap();
        tx.commit().unwrap();
        let prior = &crate::schema::migrations()[..38];
        for migration in prior {
            let tx = conn.transaction().unwrap();
            migration(&tx, &rocksdb).unwrap();
            tx.commit().unwrap();
        }
        conn.execute_batch("PRAGMA foreign_keys = OFF").unwrap();
        (conn, rocksdb, rocksdb_dir)
    }

    /// Insert a `block_headers` row whose `number` is `block_number`. Every
    /// NOT NULL column declared in the base schema (plus later non-default
    /// additions) is supplied. PRAGMA foreign_keys is OFF in
    /// `fresh_pre_0079_db`, so FK references on `version_id` and on the
    /// (since-dropped) `canonical_blocks` link are not enforced.
    fn insert_block_header(tx: &rusqlite::Transaction<'_>, block_number: u64) {
        // Hash must be unique (PRIMARY KEY). Derive from `block_number`.
        let mut hash = [0u8; 32];
        hash[24..].copy_from_slice(&block_number.to_be_bytes());
        let zero32 = [0u8; 32].to_vec();
        tx.execute(
            "INSERT INTO block_headers (hash, number, timestamp, eth_l1_gas_price, \
             sequencer_address, transaction_commitment, event_commitment, state_commitment, \
             transaction_count, event_count) VALUES (?, ?, 0, ?, ?, ?, ?, ?, 0, 0)",
            params![
                &hash.to_vec(),
                block_number,
                &zero32,
                &zero32,
                &zero32,
                &zero32,
                &zero32,
            ],
        )
        .unwrap();
    }

    /// Decode the per-block blob written by `migrate_state_updates`.
    fn read_state_update_blob(rocksdb: &crate::RocksDBInner, block_number: u64) -> StateUpdateData {
        let column = rocksdb.get_column(&STATE_UPDATES_COLUMN);
        let bytes = rocksdb
            .rocksdb
            .get_pinned_cf(&column, block_number.to_be_bytes())
            .unwrap()
            .expect("per-block blob present");
        let (dto_data, _): (dto::StateUpdateData, _) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();
        StateUpdateData::from(dto_data)
    }

    /// Verifies that `migrate_transaction_hashes` writes hashes to
    /// `TRANSACTION_HASHES_COLUMN` (the column the runtime read path uses),
    /// not `TRANSACTIONS_AND_RECEIPTS_COLUMN`.
    #[test]
    fn migrate_transaction_hashes_targets_transaction_hashes_column() {
        let rocksdb_dir = tempfile::tempdir().unwrap();
        let rocksdb = crate::StorageBuilder::open_rocksdb(rocksdb_dir.path()).unwrap();

        let mut conn = rusqlite::Connection::open_in_memory().unwrap();
        // Bring SQLite to the pre-0079 state by applying base + all prior migrations.
        let tx = conn.transaction().unwrap();
        crate::schema::base_schema(&tx).unwrap();
        tx.commit().unwrap();
        // Run migrations through revision_0078 only (index 37). Later revisions
        // drop SQLite tables that this test's INSERT relies on.
        let prior = &crate::schema::migrations()[..38];
        for migration in prior {
            let tx = conn.transaction().unwrap();
            migration(&tx, &rocksdb).unwrap();
            tx.commit().unwrap();
        }

        let hash = transaction_hash!("0xdeadbeef");
        // Disable FK enforcement so we can insert a hash row without a
        // matching block_headers row. We only care about the RocksDB write
        // path, not SQLite integrity here.
        conn.execute_batch("PRAGMA foreign_keys = OFF").unwrap();
        let tx = conn.transaction().unwrap();
        tx.execute(
            "INSERT INTO transaction_hashes (hash, block_number, idx) VALUES (?, ?, ?)",
            params![&hash.0.as_be_bytes().to_vec(), 7u64, 3i64],
        )
        .unwrap();

        super::migrate_transaction_hashes(&tx, &rocksdb).unwrap();

        // Hash must be readable from TRANSACTION_HASHES_COLUMN.
        let column = rocksdb.get_column(&TRANSACTION_HASHES_COLUMN);
        let value = rocksdb
            .rocksdb
            .get_pinned_cf(&column, hash.0.as_be_bytes())
            .unwrap()
            .expect("hash present in TRANSACTION_HASHES_COLUMN");
        assert_eq!(value.len(), 10);
        let block_number = u64::from_be_bytes(value[..8].try_into().unwrap());
        let idx = u16::from_be_bytes(value[8..].try_into().unwrap());
        assert_eq!(block_number, 7);
        assert_eq!(idx, 3);
    }

    #[test]
    fn migrate_state_updates_sort_merges_six_sources() {
        let (mut conn, rocksdb, _rocksdb_dir) = fresh_pre_0079_db();
        let tx = conn.transaction().unwrap();

        // Three block headers.
        for block_number in 0..3u64 {
            insert_block_header(&tx, block_number);
        }

        // Populate pool tables required by the JOINs in `migrate_state_updates`.
        let addr_a = contract_address!("0xA").0.as_be_bytes().to_vec();
        let addr_b = contract_address!("0xB").0.as_be_bytes().to_vec();
        let storage_key = storage_address!("0x100").0.as_be_bytes().to_vec();
        tx.execute(
            "INSERT INTO contract_addresses (id, contract_address) VALUES (1, ?), (2, ?)",
            params![&addr_a, &addr_b],
        )
        .unwrap();
        tx.execute(
            "INSERT INTO storage_addresses (id, storage_address) VALUES (1, ?)",
            params![&storage_key],
        )
        .unwrap();

        // Block 0: nonce on A, storage on A, declare cairo class X.
        tx.execute(
            "INSERT INTO nonce_updates (block_number, contract_address_id, nonce) VALUES (0, 1, ?)",
            params![&contract_nonce!("0x7").0.as_be_bytes().to_vec()],
        )
        .unwrap();
        tx.execute(
            "INSERT INTO storage_updates (block_number, contract_address_id, storage_address_id, \
             storage_value) VALUES (0, 1, 1, ?)",
            params![&storage_value!("0xFF").0.as_be_bytes().to_vec()],
        )
        .unwrap();
        let cairo_hash = class_hash!("0xCA1");
        tx.execute(
            "INSERT INTO class_definitions (hash, block_number, definition) VALUES (?, 0, ?)",
            params![&cairo_hash.0.as_be_bytes().to_vec(), &Vec::<u8>::new()],
        )
        .unwrap();

        // Block 1: declare sierra class Y, deploy contract B with class Y's casm.
        let sierra_hash = class_hash!("0x51E");
        let casm_hash = casm_hash!("0xCAFE");
        tx.execute(
            "INSERT INTO class_definitions (hash, block_number, definition) VALUES (?, 1, ?)",
            params![&sierra_hash.0.as_be_bytes().to_vec(), &Vec::<u8>::new()],
        )
        .unwrap();
        tx.execute(
            "INSERT INTO casm_class_hashes (hash, block_number, compiled_class_hash) VALUES (?, \
             1, ?)",
            params![
                &sierra_hash.0.as_be_bytes().to_vec(),
                &casm_hash.0.as_be_bytes().to_vec(),
            ],
        )
        .unwrap();
        tx.execute(
            "INSERT INTO contract_updates (block_number, contract_address, class_hash) VALUES (1, \
             ?, ?)",
            params![&addr_b, &sierra_hash.0.as_be_bytes().to_vec()],
        )
        .unwrap();

        // Block 2: redeclare cairo class X, migrate sierra class Y (second
        // casm row for same hash), replace contract B with X.
        tx.execute(
            "INSERT INTO redeclared_classes (block_number, class_hash) VALUES (2, ?)",
            params![&cairo_hash.0.as_be_bytes().to_vec()],
        )
        .unwrap();
        tx.execute(
            "INSERT INTO casm_class_hashes (hash, block_number, compiled_class_hash) VALUES (?, \
             2, ?)",
            params![
                &sierra_hash.0.as_be_bytes().to_vec(),
                &casm_hash.0.as_be_bytes().to_vec(),
            ],
        )
        .unwrap();
        tx.execute(
            "INSERT INTO contract_updates (block_number, contract_address, class_hash) VALUES (2, \
             ?, ?)",
            params![&addr_b, &cairo_hash.0.as_be_bytes().to_vec()],
        )
        .unwrap();

        super::migrate_state_updates(&tx, &rocksdb).unwrap();
        tx.commit().unwrap();

        // Block 0 expected blob.
        let b0 = read_state_update_blob(&rocksdb, 0);
        let address_a = contract_address!("0xA");
        let mut expected_b0 = StateUpdateData::default();
        let entry_a = expected_b0
            .contract_updates
            .entry(address_a)
            .or_insert_with(ContractUpdate::default);
        entry_a.nonce = Some(contract_nonce!("0x7"));
        entry_a
            .storage
            .insert(storage_address!("0x100"), storage_value!("0xFF"));
        expected_b0.declared_cairo_classes.insert(cairo_hash);
        assert_eq!(b0, expected_b0);

        // Block 1: declare sierra class Y, deploy B.
        let b1 = read_state_update_blob(&rocksdb, 1);
        let address_b = contract_address!("0xB");
        let mut expected_b1 = StateUpdateData::default();
        expected_b1
            .declared_sierra_classes
            .insert(SierraHash(sierra_hash.0), casm_hash);
        expected_b1
            .contract_updates
            .entry(address_b)
            .or_insert_with(ContractUpdate::default)
            .class = Some(ContractClassUpdate::Deploy(sierra_hash));
        assert_eq!(b1, expected_b1);

        // Block 2: redeclare X, migrated_compiled_classes has Y, replace B with X.
        let b2 = read_state_update_blob(&rocksdb, 2);
        let mut expected_b2 = StateUpdateData::default();
        expected_b2.declared_cairo_classes.insert(cairo_hash);
        expected_b2
            .migrated_compiled_classes
            .insert(SierraHash(sierra_hash.0), casm_hash);
        expected_b2
            .contract_updates
            .entry(address_b)
            .or_insert_with(ContractUpdate::default)
            .class = Some(ContractClassUpdate::Replace(cairo_hash));
        assert_eq!(b2, expected_b2);
    }

    #[test]
    fn migrate_state_updates_emits_empty_blob_for_block_with_no_changes() {
        let (mut conn, rocksdb, _rocksdb_dir) = fresh_pre_0079_db();
        let tx = conn.transaction().unwrap();
        insert_block_header(&tx, 0);
        super::migrate_state_updates(&tx, &rocksdb).unwrap();
        let blob = read_state_update_blob(&rocksdb, 0);
        assert_eq!(blob, StateUpdateData::default());
    }

    #[test]
    fn migrate_state_updates_classifies_deploy_then_replace() {
        let (mut conn, rocksdb, _rocksdb_dir) = fresh_pre_0079_db();
        let tx = conn.transaction().unwrap();
        insert_block_header(&tx, 0);
        insert_block_header(&tx, 1);

        let addr = contract_address!("0xA").0.as_be_bytes().to_vec();
        let class_a = class_hash!("0xCA");
        let class_b = class_hash!("0xCB");
        let class_c = class_hash!("0xCC");
        let class_a_bytes = class_a.0.as_be_bytes().to_vec();
        let class_b_bytes = class_b.0.as_be_bytes().to_vec();
        let class_c_bytes = class_c.0.as_be_bytes().to_vec();

        // Block 0: two rows for the same (block, address). Today's behaviour:
        // both classify as `Deploy` because `EXISTS(cu2.block_number <
        // cu1.block_number)` is false for within-block siblings; the per-block
        // HashMap overwrite keeps the last (`class_b`) write.
        tx.execute(
            "INSERT INTO contract_updates (block_number, contract_address, class_hash) VALUES (0, \
             ?, ?), (0, ?, ?)",
            params![&addr, &class_a_bytes, &addr, &class_b_bytes],
        )
        .unwrap();
        // Block 1: same address, different class — classified as `Replace`.
        tx.execute(
            "INSERT INTO contract_updates (block_number, contract_address, class_hash) VALUES (1, \
             ?, ?)",
            params![&addr, &class_c_bytes],
        )
        .unwrap();

        super::migrate_state_updates(&tx, &rocksdb).unwrap();

        let address = contract_address!("0xA");
        let b0 = read_state_update_blob(&rocksdb, 0);
        let b1 = read_state_update_blob(&rocksdb, 1);

        // Pin the winning class: `class_b` (the last write) per the
        // `drain_contracts` comment. SQLite's `ORDER BY block_number,
        // contract_address` returns the two `(0, addr)` rows in rowid (=
        // insertion) order, so the second VALUES tuple (class_b) overwrites
        // the first (class_a) in `drain_contracts`'s per-block HashMap.
        assert_eq!(
            b0.contract_updates
                .get(&address)
                .and_then(|u| u.class.as_ref()),
            Some(&ContractClassUpdate::Deploy(class_b)),
        );
        assert_eq!(
            b1.contract_updates
                .get(&address)
                .and_then(|u| u.class.as_ref()),
            Some(&ContractClassUpdate::Replace(class_c)),
        );
    }

    #[test]
    fn migrate_state_updates_emits_migrated_on_second_casm_row() {
        let (mut conn, rocksdb, _rocksdb_dir) = fresh_pre_0079_db();
        let tx = conn.transaction().unwrap();
        insert_block_header(&tx, 0);
        insert_block_header(&tx, 1);

        let sierra_hash = class_hash!("0x51E");
        let casm_hash = casm_hash!("0xCAFE");
        let sierra_hash_bytes = sierra_hash.0.as_be_bytes().to_vec();
        let casm_hash_bytes = casm_hash.0.as_be_bytes().to_vec();
        // Insert the parent class_definitions row so the fixture mirrors real on-chain
        // state.
        tx.execute(
            "INSERT INTO class_definitions (hash, block_number, definition) VALUES (?, 0, ?)",
            params![&sierra_hash_bytes, &Vec::<u8>::new()],
        )
        .unwrap();
        tx.execute(
            "INSERT INTO casm_class_hashes (hash, block_number, compiled_class_hash) VALUES (?, \
             0, ?)",
            params![&sierra_hash_bytes, &casm_hash_bytes],
        )
        .unwrap();
        tx.execute(
            "INSERT INTO casm_class_hashes (hash, block_number, compiled_class_hash) VALUES (?, \
             1, ?)",
            params![&sierra_hash_bytes, &casm_hash_bytes],
        )
        .unwrap();

        super::migrate_state_updates(&tx, &rocksdb).unwrap();

        let b0 = read_state_update_blob(&rocksdb, 0);
        let b1 = read_state_update_blob(&rocksdb, 1);
        assert!(b0.migrated_compiled_classes.is_empty());
        assert_eq!(b1.migrated_compiled_classes.len(), 1);
        assert_eq!(
            b1.migrated_compiled_classes.get(&SierraHash(sierra_hash.0)),
            Some(&CasmHash(casm_hash.0)),
        );
    }

    #[test]
    fn migrate_state_updates_skips_rows_with_null_block_number() {
        let (mut conn, rocksdb, _rocksdb_dir) = fresh_pre_0079_db();
        let tx = conn.transaction().unwrap();
        insert_block_header(&tx, 0);

        // NULL-block_number rows that must NOT surface anywhere.
        let null_class_hash = class_hash!("0xDEAD");
        let null_casm_hash = casm_hash!("0xBEEF");
        let null_class_hash_bytes = null_class_hash.0.as_be_bytes().to_vec();
        let null_casm_hash_bytes = null_casm_hash.0.as_be_bytes().to_vec();
        tx.execute(
            "INSERT INTO class_definitions (hash, block_number, definition) VALUES (?, NULL, ?)",
            params![&null_class_hash_bytes, &Vec::<u8>::new()],
        )
        .unwrap();
        tx.execute(
            "INSERT INTO casm_class_hashes (hash, block_number, compiled_class_hash) VALUES (?, \
             NULL, ?)",
            params![&null_class_hash_bytes, &null_casm_hash_bytes],
        )
        .unwrap();

        // Non-NULL rows on block 0 with distinct hashes so the assertion can
        // tell them apart from the NULL-block fixture. The
        // `class_definitions` row has no matching `casm_class_hashes` row at
        // the same (hash, block_number), so it lands in
        // `declared_cairo_classes` rather than `declared_sierra_classes`. The
        // standalone `casm_class_hashes` row is a first-occurrence and so
        // does not trigger `migrated_compiled_classes`.
        let live_cairo_hash = class_hash!("0xC0FFEE");
        let live_sierra_hash = class_hash!("0x5111E1");
        let live_casm_hash = casm_hash!("0xCA51");
        let live_cairo_hash_bytes = live_cairo_hash.0.as_be_bytes().to_vec();
        let live_sierra_hash_bytes = live_sierra_hash.0.as_be_bytes().to_vec();
        let live_casm_hash_bytes = live_casm_hash.0.as_be_bytes().to_vec();
        tx.execute(
            "INSERT INTO class_definitions (hash, block_number, definition) VALUES (?, 0, ?)",
            params![&live_cairo_hash_bytes, &Vec::<u8>::new()],
        )
        .unwrap();
        tx.execute(
            "INSERT INTO casm_class_hashes (hash, block_number, compiled_class_hash) VALUES (?, \
             0, ?)",
            params![&live_sierra_hash_bytes, &live_casm_hash_bytes],
        )
        .unwrap();

        super::migrate_state_updates(&tx, &rocksdb).unwrap();

        let b0 = read_state_update_blob(&rocksdb, 0);
        // The non-NULL `class_definitions` row surfaces as cairo (no CASM
        // join match at (hash, block_number=0)).
        assert_eq!(b0.declared_cairo_classes.len(), 1);
        assert!(b0.declared_cairo_classes.contains(&live_cairo_hash));
        // NULL-block row must NOT appear in any classes set.
        assert!(!b0.declared_cairo_classes.contains(&null_class_hash));
        assert!(!b0
            .declared_sierra_classes
            .contains_key(&SierraHash(null_class_hash.0)));
        assert!(b0.declared_sierra_classes.is_empty());
        // First-occurrence CASM row alone does not emit migrated.
        assert!(b0.migrated_compiled_classes.is_empty());
    }
}
