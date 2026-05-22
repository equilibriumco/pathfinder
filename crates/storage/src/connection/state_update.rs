use std::collections::{HashMap, HashSet, VecDeque};
use std::num::NonZeroUsize;

use anyhow::Context;
use pathfinder_common::prelude::*;
use pathfinder_common::state_update::{
    ContractClassUpdate,
    ContractUpdate,
    ReverseContractUpdate,
    StateUpdateData,
    SystemContractUpdate,
};
use pathfinder_common::BlockId;
use pathfinder_crypto::Felt;
use rust_rocksdb::ReadOptions;

use crate::columns::Column;
use crate::prelude::*;

/// Prefix length for storage update keys: contract_address (32) +
/// storage_address (32).
const STORAGE_UPDATES_PREFIX_LEN: usize = 2 * size_of::<Felt>();

pub const STORAGE_UPDATES_COLUMN: Column =
    Column::new("storage_updates").with_prefix_length(STORAGE_UPDATES_PREFIX_LEN);

type StorageUpdates = Vec<(StorageAddress, StorageValue)>;

const NONCE_UPDATE_PREFIX_LEN: usize = size_of::<Felt>();
const NONCE_UPDATE_KEY_LEN: usize = NONCE_UPDATE_PREFIX_LEN + size_of::<u64>();

/// Constructs a key used in our RocksDB column family for nonce updates.
///
/// Format is the following:
/// [contract_address (32 bytes)] [inverted block number (8 bytes)]
///
/// We're using an inverted block number to allow for efficient retrieval of the
/// latest nonce for a given contract address using forward iteration.
pub(crate) fn nonce_update_key(
    block_number: BlockNumber,
    contract_address: &ContractAddress,
) -> [u8; NONCE_UPDATE_KEY_LEN] {
    let block_number = u64::MAX - block_number.get();

    let mut key = [0; NONCE_UPDATE_KEY_LEN];
    key[..32].copy_from_slice(contract_address.0.as_be_bytes());
    key[32..].copy_from_slice(&block_number.to_be_bytes());
    key
}

pub const NONCE_UPDATES_COLUMN: Column =
    Column::new("nonce_updates").with_prefix_length(NONCE_UPDATE_PREFIX_LEN);

const STORAGE_UPDATE_PREFIX_LEN: usize = size_of::<Felt>();
const STORAGE_UPDATE_KEY_LEN: usize =
    STORAGE_UPDATE_PREFIX_LEN + size_of::<Felt>() + size_of::<u64>();

/// Constructs a key used in our RocksDB column family for storage updates.
///
/// Format is the following:
/// [contract_address (32 bytes)] [storage_address (32 bytes)] [inverted block
/// number (8 bytes)]
///
/// We're using an inverted block number to allow for efficient retrieval of the
/// latest storage value for a given contract address and storage address using
/// forward iteration.
pub(crate) fn storage_update_key(
    block_number: BlockNumber,
    contract_address: &ContractAddress,
    storage_address: &StorageAddress,
) -> [u8; STORAGE_UPDATE_KEY_LEN] {
    let block_number = u64::MAX - block_number.get();

    let mut key = [0; STORAGE_UPDATE_KEY_LEN];
    key[..32].copy_from_slice(contract_address.0.as_be_bytes());
    key[32..64].copy_from_slice(storage_address.0.as_be_bytes());
    key[64..].copy_from_slice(&block_number.to_be_bytes());
    key
}

pub const STATE_UPDATES_COLUMN: Column = Column::new("state_updates");

fn encode_compressed_felt(value: &Felt) -> &[u8] {
    let bytes = value.as_be_bytes();
    let first_non_zero = bytes.iter().position(|b| *b != 0).unwrap_or(bytes.len());
    &bytes[first_non_zero..]
}

impl Transaction<'_> {
    /// Inserts a canonical [StateUpdate] into storage.
    pub fn insert_state_update(
        &self,
        block_number: BlockNumber,
        state_update: &StateUpdate,
    ) -> anyhow::Result<()> {
        self.insert_state_update_data0(
            block_number,
            &state_update.contract_updates,
            &state_update.system_contract_updates,
            &state_update.declared_cairo_classes,
            &state_update.declared_sierra_classes,
            &state_update.migrated_compiled_classes,
        )
    }

    pub fn insert_state_update_data(
        &self,
        block_number: BlockNumber,
        state_update: &StateUpdateData,
    ) -> anyhow::Result<()> {
        self.insert_state_update_data0(
            block_number,
            &state_update.contract_updates,
            &state_update.system_contract_updates,
            &state_update.declared_cairo_classes,
            &state_update.declared_sierra_classes,
            &state_update.migrated_compiled_classes,
        )
    }

    fn insert_state_update_data0(
        &self,
        block_number: BlockNumber,
        contract_updates: &HashMap<ContractAddress, ContractUpdate>,
        system_contract_updates: &HashMap<ContractAddress, SystemContractUpdate>,
        declared_cairo_classes: &HashSet<ClassHash>,
        declared_sierra_classes: &HashMap<SierraHash, CasmHash>,
        migrated_compiled_classes: &HashMap<SierraHash, CasmHash>,
    ) -> anyhow::Result<()> {
        // Insert serialized state update
        let state_updates_column = self.rocksdb_get_column(&STATE_UPDATES_COLUMN);
        let key = block_number.get().to_be_bytes();
        let state_update_data = StateUpdateData {
            contract_updates: contract_updates.clone(),
            system_contract_updates: system_contract_updates.clone(),
            declared_cairo_classes: declared_cairo_classes.clone(),
            declared_sierra_classes: declared_sierra_classes.clone(),
            migrated_compiled_classes: migrated_compiled_classes.clone(),
        };
        let state_update_data = dto::StateUpdateData::from(state_update_data);
        let data = bincode::serde::encode_to_vec(state_update_data, bincode::config::standard())?;
        self.batch
            .lock()
            .expect("Batch lock poisoned")
            .put_cf(&state_updates_column, key, data);

        let mut insert_contract = self
            .inner()
            .prepare_cached(
                "INSERT INTO contract_updates (block_number, contract_address, class_hash) VALUES \
                 (?, ?, ?)",
            )
            .context("Preparing contract insert statement")?;

        // ON CONFLICT is required to handle legacy syncing logic, where the definition
        // is inserted before the state update
        let mut upsert_declared_at = self
            .inner()
            .prepare_cached(
                r"INSERT INTO class_definitions (block_number, hash) VALUES (?1, ?2)
                ON CONFLICT(hash)
                    DO UPDATE SET block_number=IFNULL(block_number,excluded.block_number)
                RETURNING block_number",
            )
            .context("Preparing class hash and block number upsert statement")?;

        let mut insert_redeclared_class = self
            .inner()
            .prepare_cached(
                r"INSERT INTO redeclared_classes (class_hash, block_number) VALUES (?, ?)",
            )
            .context("Preparing redeclared class insert statement")?;

        let mut insert_casm_hash = self
            .inner()
            .prepare_cached(
                "INSERT OR IGNORE INTO casm_class_hashes (hash, block_number, \
                 compiled_class_hash) VALUES (?, ?, ?)",
            )
            .context("Preparing casm hash insert statement")?;

        let nonce_updates_column = self.rocksdb_get_column(&NONCE_UPDATES_COLUMN);
        let storage_updates_column = self.rocksdb_get_column(&STORAGE_UPDATES_COLUMN);
        let mut batch = self.batch.lock().expect("Batch lock poisoned");

        for (address, update) in contract_updates {
            if let Some(class_update) = &update.class {
                insert_contract
                    .execute(params![&block_number, address, &class_update.class_hash()])
                    .context("Inserting deployed contract")?;
            }

            if let Some(nonce) = &update.nonce {
                let encoded_nonce = encode_compressed_felt(&nonce.0);
                batch.put_cf(
                    &nonce_updates_column,
                    nonce_update_key(block_number, address),
                    encoded_nonce,
                );
            }

            for (key, value) in &update.storage {
                let encoded_value = encode_compressed_felt(&value.0);
                batch.put_cf(
                    &storage_updates_column,
                    storage_update_key(block_number, address, key),
                    encoded_value,
                );
            }
        }

        for (address, update) in system_contract_updates {
            for (key, value) in &update.storage {
                let encoded_value = encode_compressed_felt(&value.0);
                batch.put_cf(
                    &storage_updates_column,
                    storage_update_key(block_number, address, key),
                    encoded_value,
                );
            }
        }

        // Set all declared classes block numbers. Class definitions are inserted by a
        // separate mechanism, prior to state update inserts. However, since the
        // class insertion does not know with which block number to
        // associate with the class definition, we need to fill it in here.
        let sierra = declared_sierra_classes
            .keys()
            .map(|sierra| ClassHash(sierra.0));
        let cairo = declared_cairo_classes.iter().copied();

        let declared_classes = sierra.chain(cairo);

        for class in declared_classes {
            let declared_at = upsert_declared_at
                .query_row(params![&block_number, &class], |row| {
                    row.get_block_number(0)
                })?;
            if declared_at != block_number {
                tracing::debug!(%declared_at, %block_number, class_hash=%class, "Re-declared class");
                insert_redeclared_class.execute(params![&class, &block_number])?;
            }
        }

        // Older cairo 0 classes were never declared, but instead got implicitly
        // declared on first deployment. Until such classes disappear we need to
        // cater for them here. This works because the sql only updates the row
        // if it is null.
        let deployed = contract_updates
            .values()
            .filter_map(|update| match update.class {
                Some(ContractClassUpdate::Deploy(x)) => Some(x),
                _ => None,
            });

        for class in deployed {
            let _ = upsert_declared_at.query_row(params![&block_number, &class], |row| {
                row.get_block_number(0)
            })?;
        }

        for (sierra_hash, casm_hash) in declared_sierra_classes {
            insert_casm_hash
                .execute(params![sierra_hash, &block_number, casm_hash])
                .context("Inserting CASM hash")?;
        }

        // Starknet 0.14.1 introduced CASM hash migrations: CASM class hashes are
        // gradually migrated to the new hash algorithm (using Blake2).
        for (sierra_hash, casm_hash) in migrated_compiled_classes {
            insert_casm_hash
                .execute(params![sierra_hash, &block_number, casm_hash])
                .context("Inserting migrated CASM hash")?;
        }

        Ok(())
    }

    fn block_details(
        &self,
        block: BlockId,
    ) -> anyhow::Result<Option<(BlockNumber, BlockHash, StateCommitment, StateCommitment)>> {
        use const_format::formatcp;

        const PREFIX: &str = r"
            SELECT b1.number, b1.hash, b1.state_commitment, b2.state_commitment
            FROM block_headers b1
            LEFT OUTER JOIN block_headers b2
            ON b2.number = b1.number - 1
        ";

        const LATEST: &str = formatcp!("{PREFIX} ORDER BY b1.number DESC LIMIT 1");
        const NUMBER: &str = formatcp!("{PREFIX} WHERE b1.number = ?");
        const HASH: &str = formatcp!("{PREFIX} WHERE b1.hash = ?");

        let handle_row = |row: &rusqlite::Row<'_>| {
            let number = row.get_block_number(0)?;
            let hash = row.get_block_hash(1)?;
            let state_commitment = row.get_state_commitment(2)?;
            // The genesis block would not have a value.
            let parent_state_commitment =
                row.get_optional_state_commitment(3)?.unwrap_or_else(|| {
                    // Block at the tip of blockchain history (see `pruning.rs`) would also not have
                    // a parent, but this case should be handled at the RPC
                    // layer.
                    assert_eq!(number, BlockNumber::GENESIS);
                    Default::default()
                });

            Ok((number, hash, state_commitment, parent_state_commitment))
        };

        let tx = self.inner();

        match block {
            BlockId::Latest => tx.query_row(LATEST, [], handle_row),
            BlockId::Number(number) => tx.query_row(NUMBER, params![&number], handle_row),
            BlockId::Hash(hash) => tx.query_row(HASH, params![&hash], handle_row),
        }
        .optional()
        .map_err(Into::into)
    }

    pub fn state_update(&self, block: BlockId) -> anyhow::Result<Option<StateUpdate>> {
        let Some((block_number, block_hash, state_commitment, parent_state_commitment)) =
            self.block_details(block).context("Querying block header")?
        else {
            return Ok(None);
        };

        let state_update_data = self.state_update_data(block_number)?.unwrap_or_default();

        Ok(Some(StateUpdate {
            block_hash,
            parent_state_commitment,
            state_commitment,
            contract_updates: state_update_data.contract_updates,
            system_contract_updates: state_update_data.system_contract_updates,
            declared_cairo_classes: state_update_data.declared_cairo_classes,
            declared_sierra_classes: state_update_data.declared_sierra_classes,
            migrated_compiled_classes: state_update_data.migrated_compiled_classes,
        }))
    }

    /// Deletes nonce and storage update entries from RocksDB for the given
    /// block.
    pub fn purge_state_update_data(&self, block_number: BlockNumber) -> anyhow::Result<()> {
        let Some(data) = self.state_update_data(block_number)? else {
            return Ok(());
        };

        let nonce_updates_column = self.rocksdb_get_column(&NONCE_UPDATES_COLUMN);
        let storage_updates_column = self.rocksdb_get_column(&STORAGE_UPDATES_COLUMN);
        let state_updates_column = self.rocksdb_get_column(&STATE_UPDATES_COLUMN);

        let mut batch = self.batch.lock().expect("Batch lock poisoned");

        for (address, update) in &data.contract_updates {
            if update.nonce.is_some() {
                batch.delete_cf(
                    &nonce_updates_column,
                    nonce_update_key(block_number, address),
                );
            }
            for key in update.storage.keys() {
                batch.delete_cf(
                    &storage_updates_column,
                    storage_update_key(block_number, address, key),
                );
            }
        }

        for (address, update) in &data.system_contract_updates {
            for key in update.storage.keys() {
                batch.delete_cf(
                    &storage_updates_column,
                    storage_update_key(block_number, address, key),
                );
            }
        }

        batch.delete_cf(&state_updates_column, block_number.get().to_be_bytes());

        Ok(())
    }

    pub(crate) fn state_update_data(
        &self,
        block_number: BlockNumber,
    ) -> anyhow::Result<Option<StateUpdateData>> {
        let state_updates_column = self.rocksdb_get_column(&STATE_UPDATES_COLUMN);
        let key = block_number.get().to_be_bytes();
        let Some(data) = self
            .rocksdb()
            .get_pinned_cf(&state_updates_column, key)
            .context("Reading state update from RocksDB")?
        else {
            return Ok(None);
        };
        let (state_update_data, _): (dto::StateUpdateData, _) =
            bincode::serde::decode_from_slice(&data, bincode::config::standard())
                .context("Decoding state update data")?;
        Ok(Some(
            pathfinder_common::state_update::StateUpdateData::from(state_update_data),
        ))
    }

    pub fn highest_block_with_state_update(&self) -> anyhow::Result<Option<BlockNumber>> {
        // Find the highest block in RocksDB STATE_UPDATES_COLUMN.
        let state_updates_column = self.rocksdb_get_column(&STATE_UPDATES_COLUMN);
        let mut read_options = ReadOptions::default();
        read_options.set_total_order_seek(true);
        let mut iter = self
            .rocksdb()
            .raw_iterator_cf_opt(&state_updates_column, read_options);
        iter.seek_to_last();
        let rocksdb_highest = if iter.valid() {
            let key = iter.key().context("RocksDB iterator key is missing")?;
            let block_number = u64::from_be_bytes(
                key.try_into()
                    .map_err(|_| anyhow::anyhow!("Invalid key length in STATE_UPDATES_COLUMN"))?,
            );
            Some(BlockNumber::new_or_panic(block_number))
        } else {
            None
        };

        // Also check class_definitions in SQLite.
        let mut stmt = self
            .inner()
            .prepare_cached("SELECT max(block_number) FROM class_definitions")?;
        let sqlite_highest: Option<BlockNumber> = stmt
            .query_row([], |row| row.get_optional_block_number(0))
            .context("Querying highest class definition block")?;

        Ok(match (rocksdb_highest, sqlite_highest) {
            (Some(a), Some(b)) => Some(Ord::max(a, b)),
            (a, b) => a.or(b),
        })
    }

    pub fn state_diff_lengths(
        &self,
        start: BlockNumber,
        max_num_blocks: NonZeroUsize,
    ) -> anyhow::Result<VecDeque<usize>> {
        let mut stmt = self
            .inner()
            .prepare_cached(
                r"SELECT state_diff_length FROM block_headers WHERE number >= ? ORDER BY number ASC LIMIT ?",
            )
            .context("Preparing statement")?;

        let max_len = u64::try_from(max_num_blocks.get()).expect("ptr size is 64 bits");
        let mut counts = stmt
            .query_map(params![&start, &max_len], |row| row.get(0))
            .context("Querying state diff lengths")?;

        let mut ret = VecDeque::new();

        while let Some(stat) = counts.next().transpose().context("Iterating over rows")? {
            ret.push_back(stat);
        }

        Ok(ret)
    }

    pub fn declared_classes_counts(
        &self,
        start: BlockNumber,
        max_num_blocks: NonZeroUsize,
    ) -> anyhow::Result<VecDeque<usize>> {
        let mut stmt = self
            .inner()
            .prepare_cached(
                r"SELECT
                  (SELECT COUNT(block_number) FROM class_definitions WHERE block_number=block_headers.number) +
                  (SELECT COUNT(block_number) FROM redeclared_classes WHERE block_number=block_headers.number)
                FROM block_headers
                WHERE block_headers.number >= ?
                ORDER BY block_headers.number ASC
                LIMIT ?",
            )
            .context("Preparing get number of declared classes statement")?;

        let max_len = u64::try_from(max_num_blocks.get()).expect("ptr size is 64 bits");
        let mut counts = stmt
            .query_map(params![&start, &max_len], |row| row.get(0))
            .context("Querying declared classes counts")?;

        let mut ret = VecDeque::new();

        while let Some(stat) = counts.next().transpose().context("Iterating over rows")? {
            ret.push_back(stat);
        }

        Ok(ret)
    }

    /// Returns hashes of Cairo and Sierra classes declared at a given block.
    pub fn declared_classes_at(&self, block: BlockId) -> anyhow::Result<Option<Vec<ClassHash>>> {
        let Some((block_number, _)) = self.block_id(block).context("Querying block header")? else {
            return Ok(None);
        };

        let mut stmt = self
            .inner()
            .prepare_cached(r"SELECT hash FROM class_definitions WHERE block_number = ?")
            .context("Preparing class declaration query statement")?;

        let mut declared_classes = stmt
            .query_map(params![&block_number], |row| {
                let class_hash: ClassHash = row.get_class_hash(0)?;
                Ok(class_hash)
            })
            .context("Querying class declarations")?;

        let mut result = Vec::new();

        while let Some(class_hash) = declared_classes
            .next()
            .transpose()
            .context("Iterating over class declaration query rows")?
        {
            result.push(class_hash);
        }

        let mut stmt = self
            .inner()
            .prepare_cached(r"SELECT class_hash FROM redeclared_classes WHERE block_number = ?")
            .context("Preparing re-declared class query")?;

        let mut redeclared_classes = stmt
            .query_map(params![&block_number], |row| row.get_class_hash(0))
            .context("Querying re-declared classes")?;

        while let Some(class_hash) = redeclared_classes
            .next()
            .transpose()
            .context("Iterating over re-declared classes")?
        {
            result.push(class_hash)
        }

        Ok(Some(result))
    }

    pub fn storage_value(
        &self,
        block: BlockId,
        contract_address: ContractAddress,
        key: StorageAddress,
    ) -> anyhow::Result<Option<StorageValue>> {
        let Some(block_number) = self.block_number(block).context("Querying block number")? else {
            return Ok(None);
        };

        let key = storage_update_key(block_number, &contract_address, &key);
        let storage_update_column = self.rocksdb_get_column(&STORAGE_UPDATES_COLUMN);

        let mut read_options = ReadOptions::default();
        read_options.set_prefix_same_as_start(true);
        let mut iter = self
            .rocksdb()
            .raw_iterator_cf_opt(&storage_update_column, read_options);
        iter.seek(key);
        if !iter.valid() {
            return Ok(None);
        }
        let value = iter
            .value()
            .context("Reading storage update value from RocksDB")?;
        let value = Felt::from_be_slice(value).context("Parsing storage update value")?;
        Ok(Some(StorageValue(value)))
    }

    pub fn storage_value_with_block(
        &self,
        block: BlockId,
        contract_address: ContractAddress,
        key: StorageAddress,
    ) -> anyhow::Result<Option<(StorageValue, BlockNumber)>> {
        let Some(block_number) = self.block_number(block).context("Querying block number")? else {
            return Ok(None);
        };

        let lookup_key = storage_update_key(block_number, &contract_address, &key);
        let storage_updates_column = self.rocksdb_get_column(&STORAGE_UPDATES_COLUMN);

        let mut read_options = ReadOptions::default();
        read_options.set_prefix_same_as_start(true);
        let mut iter = self
            .rocksdb()
            .raw_iterator_cf_opt(&storage_updates_column, read_options);
        iter.seek(lookup_key);
        if !iter.valid() {
            return Ok(None);
        }

        let key_bytes = iter
            .key()
            .context("Reading storage update key from RocksDB")?;
        if key_bytes.len() != STORAGE_UPDATE_KEY_LEN {
            anyhow::bail!("Unexpected STORAGE_UPDATES key length: {}", key_bytes.len());
        }
        let inverted: [u8; 8] = key_bytes[STORAGE_UPDATE_KEY_LEN - 8..]
            .try_into()
            .expect("8-byte slice");
        let inverted = u64::from_be_bytes(inverted);
        let found_block = BlockNumber::new_or_panic(u64::MAX - inverted);

        let value = iter
            .value()
            .context("Reading storage update value from RocksDB")?;
        let value = Felt::from_be_slice(value).context("Parsing storage update value")?;
        Ok(Some((StorageValue(value), found_block)))
    }

    pub fn contract_exists(
        &self,
        contract_address: ContractAddress,
        block_id: BlockId,
    ) -> anyhow::Result<bool> {
        match block_id {
            BlockId::Number(number) => {
                let mut stmt = self.inner().prepare_cached(
                    "SELECT EXISTS(SELECT 1 FROM contract_updates WHERE contract_address = ? AND block_number <= ?)",
                )?;
                stmt.query_row(
                    params![&contract_address, &number],
                    |row| row.get(0),
                )
            }
            BlockId::Hash(hash) => {
                let mut stmt = self.inner().prepare_cached(
                    r"SELECT EXISTS(
                        SELECT 1 FROM contract_updates WHERE contract_address = ? AND block_number <= (
                            SELECT number FROM block_headers WHERE hash = ?
                        )
                    )",
                )?;
                stmt.query_row(
                    params![&contract_address, &hash],
                    |row| row.get(0),
                )
            }
            BlockId::Latest => {
                let mut stmt = self.inner().prepare_cached(
                    "SELECT EXISTS(SELECT 1 FROM contract_updates WHERE contract_address = ?)",
                )?;
                stmt.query_row(
                    params![&contract_address],
                    |row| row.get(0),
                )
            }
        }
        .context("Querying that contract exists")
    }

    pub fn contract_nonce(
        &self,
        contract_address: ContractAddress,
        block_id: BlockId,
    ) -> anyhow::Result<Option<ContractNonce>> {
        let block_number = match block_id {
            BlockId::Number(number) => Some(number),
            BlockId::Hash(_) | BlockId::Latest => self
                .block_number(block_id)
                .context("Querying block number")?,
        };
        let Some(block_number) = block_number else {
            return Ok(None);
        };

        let key = nonce_update_key(block_number, &contract_address);
        let nonce_updates_column = self.rocksdb_get_column(&NONCE_UPDATES_COLUMN);

        let mut read_options = ReadOptions::default();
        read_options.set_prefix_same_as_start(true);
        let mut iter = self
            .rocksdb()
            .raw_iterator_cf_opt(&nonce_updates_column, read_options);
        iter.seek(key);
        if !iter.valid() {
            return Ok(None);
        }
        let value = iter
            .value()
            .context("Reading nonce update value from RocksDB")?;
        let value = Felt::from_be_slice(value).context("Parsing nonce update value")?;
        Ok(Some(ContractNonce(value)))
    }

    pub fn contract_class_hash(
        &self,
        block_id: BlockId,
        contract_address: ContractAddress,
    ) -> anyhow::Result<Option<ClassHash>> {
        match block_id {
            BlockId::Latest => {
                let mut stmt = self.inner().prepare_cached(
                    r"SELECT class_hash FROM contract_updates
                WHERE contract_address = ?
                ORDER BY block_number DESC LIMIT 1",
                )?;
                stmt.query_row(params![&contract_address], |row| row.get_class_hash(0))
            }
            BlockId::Number(number) => {
                let mut stmt = self.inner().prepare_cached(
                    r"SELECT class_hash FROM contract_updates
                WHERE contract_address = ? AND block_number <= ?
                ORDER BY block_number DESC LIMIT 1",
                )?;
                stmt.query_row(params![&contract_address, &number], |row| {
                    row.get_class_hash(0)
                })
            }
            BlockId::Hash(hash) => {
                let mut stmt = self.inner().prepare_cached(
                    r"SELECT class_hash FROM contract_updates
                WHERE contract_address = ? AND block_number <= (
                    SELECT number FROM block_headers WHERE hash = ?
                )
                ORDER BY block_number DESC LIMIT 1",
                )?;
                stmt.query_row(params![&contract_address, &hash], |row| {
                    row.get_class_hash(0)
                })
            }
        }
        .optional()
        .map_err(|e| e.into())
    }

    pub fn reverse_contract_updates(
        &self,
        from: BlockNumber,
        to: BlockNumber,
    ) -> anyhow::Result<HashMap<ContractAddress, ReverseContractUpdate>> {
        let storage_updates = self.reverse_storage_updates(from, to)?;
        let nonce_updates: Vec<(ContractAddress, Option<ContractNonce>)> =
            self.reverse_nonce_updates(from, to)?;
        let mut stmt = self.inner().prepare_cached(
            r"WITH
                updated_contracts(contract_address) AS (
                    SELECT DISTINCT
                        contract_address
                    FROM contract_updates
                    WHERE
                        block_number > ?2 AND block_number <= ?1
                )
            SELECT
                contract_address,
                (
                    SELECT class_hash
                    FROM contract_updates
                    WHERE
                        contract_address=updated_contracts.contract_address AND block_number <= ?2
                    ORDER BY block_number DESC
                    LIMIT 1
                ) AS old_class_hash
            FROM updated_contracts",
        )?;

        let rows = stmt
            .query_map(params![&from, &to], |row| {
                let contract_address = row.get_contract_address(0)?;
                let old_class_hash = row.get_optional_class_hash(1)?;

                Ok((contract_address, old_class_hash))
            })
            .context("Querying reverse contract updates")?;

        let contract_updates = rows
            .collect::<Result<Vec<_>, _>>()
            .context("Iterating over reverse contract updates")?;

        let mut updates: HashMap<ContractAddress, ReverseContractUpdate> = Default::default();

        for (contract_address, class_hash_update) in contract_updates {
            updates
                .entry(contract_address)
                .or_insert_with(|| match class_hash_update {
                    None => ReverseContractUpdate::Deleted,
                    Some(_) => ReverseContractUpdate::Updated(ContractUpdate {
                        class: class_hash_update.map(ContractClassUpdate::Replace),
                        ..Default::default()
                    }),
                });
        }

        for (contract_address, nonce_update) in nonce_updates {
            if let Some(update) = updates
                .entry(contract_address)
                .or_insert_with(|| ReverseContractUpdate::Updated(Default::default()))
                .update_mut()
            {
                update.nonce = nonce_update
            };
        }

        for (contract_address, storage_updates) in storage_updates {
            if let Some(update) = updates
                .entry(contract_address)
                .or_insert_with(|| ReverseContractUpdate::Updated(Default::default()))
                .update_mut()
            {
                update.storage = storage_updates.into_iter().collect()
            };
        }

        Ok(updates)
    }

    fn reverse_storage_updates(
        &self,
        from_block: BlockNumber,
        to_block: BlockNumber,
    ) -> anyhow::Result<HashMap<ContractAddress, StorageUpdates>> {
        // Collect all (contract, storage_key) pairs changed in (to_block, from_block]
        // by reading StateUpdateData from RocksDB for each block in the range.
        let mut changed: HashSet<(ContractAddress, StorageAddress)> = HashSet::new();
        for block_num in (to_block.get() + 1)..=from_block.get() {
            let block_number = BlockNumber::new_or_panic(block_num);
            if let Some(data) = self.state_update_data(block_number)? {
                for (addr, update) in &data.contract_updates {
                    for key in update.storage.keys() {
                        changed.insert((*addr, *key));
                    }
                }
                for (addr, update) in &data.system_contract_updates {
                    for key in update.storage.keys() {
                        changed.insert((*addr, *key));
                    }
                }
            }
        }

        // For each changed pair, look up the value at to_block.
        let storage_update_column = self.rocksdb_get_column(&STORAGE_UPDATES_COLUMN);
        let mut storage_updates: HashMap<ContractAddress, Vec<_>> = Default::default();

        for (contract_address, storage_address) in changed {
            let key = storage_update_key(to_block, &contract_address, &storage_address);
            let mut read_options = ReadOptions::default();
            read_options.set_prefix_same_as_start(true);
            let mut iter = self
                .rocksdb()
                .raw_iterator_cf_opt(&storage_update_column, read_options);
            iter.seek(key);

            let old_value = if iter.valid() {
                let value = iter
                    .value()
                    .context("Reading storage update value from RocksDB")?;
                let value = Felt::from_be_slice(value).context("Parsing storage update value")?;
                StorageValue(value)
            } else {
                StorageValue::ZERO
            };

            storage_updates
                .entry(contract_address)
                .or_default()
                .push((storage_address, old_value));
        }

        Ok(storage_updates)
    }

    fn reverse_nonce_updates(
        &self,
        from_block: BlockNumber,
        to_block: BlockNumber,
    ) -> anyhow::Result<Vec<(ContractAddress, Option<ContractNonce>)>> {
        // Collect all contract addresses with nonce changes in (to_block, from_block].
        let mut changed: HashSet<ContractAddress> = HashSet::new();
        for block_num in (to_block.get() + 1)..=from_block.get() {
            let block_number = BlockNumber::new_or_panic(block_num);
            if let Some(data) = self.state_update_data(block_number)? {
                for (addr, update) in &data.contract_updates {
                    if update.nonce.is_some() {
                        changed.insert(*addr);
                    }
                }
            }
        }

        // For each changed address, look up the nonce at to_block.
        let nonce_updates_column = self.rocksdb_get_column(&NONCE_UPDATES_COLUMN);
        let mut result = Vec::with_capacity(changed.len());

        for contract_address in changed {
            let key = nonce_update_key(to_block, &contract_address);
            let mut read_options = ReadOptions::default();
            read_options.set_prefix_same_as_start(true);
            let mut iter = self
                .rocksdb()
                .raw_iterator_cf_opt(&nonce_updates_column, read_options);
            iter.seek(key);

            let old_nonce = if iter.valid() {
                let value = iter
                    .value()
                    .context("Reading nonce update value from RocksDB")?;
                let value = Felt::from_be_slice(value).context("Parsing nonce update value")?;
                Some(ContractNonce(value))
            } else {
                None
            };

            result.push((contract_address, old_nonce));
        }

        Ok(result)
    }

    /// Returns the list of changes to be made to revert Sierra class
    /// declarations.
    ///
    /// None means the class was declared _after_ `to_block` and has to be
    /// deleted. Some(casm_hash) means the CASM hash for the class has been
    /// updated (!).
    pub fn reverse_sierra_class_updates(
        &self,
        from_block: BlockNumber,
        to_block: BlockNumber,
    ) -> anyhow::Result<Vec<(SierraHash, Option<CasmHash>)>> {
        let mut stmt = self.inner().prepare_cached(
            r"WITH sierra_casm_class_hash_changes(class_hash) AS (
                SELECT
                    hash AS class_hash
                FROM
                    casm_class_hashes
                WHERE
                    block_number > ?2
                    AND block_number <= ?1
            )
            SELECT
                class_hash,
                (
                    SELECT
                        compiled_class_hash
                    FROM
                        casm_class_hashes
                    WHERE
                        hash = sierra_casm_class_hash_changes.class_hash
                        AND block_number <= ?2
                ) AS compiled_class_hash
            FROM
                sierra_casm_class_hash_changes
            ",
        )?;

        let rows = stmt
            .query_map(params![&from_block, &to_block], |row| {
                let class_hash = SierraHash(row.get_class_hash(0)?.0);
                let casm_hash = row.get_optional_casm_hash(1)?;

                Ok((class_hash, casm_hash))
            })
            .context("Querying reverse contract updates")?;

        rows.collect::<Result<Vec<_>, _>>()
            .context("Iterating over reverse Sierra declarations")
    }

    pub fn deployed_contracts(
        &self,
        sierra_hash: SierraHash,
        block_number: BlockNumber,
    ) -> anyhow::Result<Vec<ContractAddress>> {
        let mut stmt = self
            .inner()
            .prepare_cached(r"SELECT contract_address FROM contract_updates WHERE class_hash = ? AND block_number = ?")
            .context("Preparing deployed contracts query statement")?;

        let rows = stmt
            .query_map(params![&sierra_hash, &block_number], |row| {
                row.get_contract_address(0)
            })
            .context("Querying deployed contracts")?;

        rows.collect::<Result<Vec<_>, _>>()
            .context("Iterating over deployed contracts")
    }
}

pub(crate) mod dto {
    use std::collections::{HashMap, HashSet};

    use pathfinder_common::prelude::*;

    use crate::connection::dto::MinimalFelt;

    #[derive(serde::Deserialize, serde::Serialize, Debug, PartialEq)]
    pub enum StateUpdateData {
        V0(StateUpdateDataV0),
    }

    impl From<pathfinder_common::state_update::StateUpdateData> for StateUpdateData {
        fn from(value: pathfinder_common::state_update::StateUpdateData) -> Self {
            StateUpdateData::V0(StateUpdateDataV0::from(value))
        }
    }

    impl From<StateUpdateData> for pathfinder_common::state_update::StateUpdateData {
        fn from(value: StateUpdateData) -> Self {
            match value {
                StateUpdateData::V0(v0) => {
                    pathfinder_common::state_update::StateUpdateData::from(v0)
                }
            }
        }
    }

    #[derive(serde::Deserialize, serde::Serialize, Default, Debug, PartialEq)]
    pub struct StateUpdateDataV0 {
        pub contract_updates: HashMap<MinimalFelt, ContractUpdate>,
        pub system_contract_updates: HashMap<MinimalFelt, SystemContractUpdate>,
        pub declared_cairo_classes: HashSet<MinimalFelt>,
        pub declared_sierra_classes: HashMap<MinimalFelt, MinimalFelt>,
        pub migrated_compiled_classes: HashMap<MinimalFelt, MinimalFelt>,
    }

    impl From<pathfinder_common::state_update::StateUpdateData> for StateUpdateDataV0 {
        fn from(value: pathfinder_common::state_update::StateUpdateData) -> Self {
            let contract_updates = value
                .contract_updates
                .into_iter()
                .map(|(k, v)| (MinimalFelt::from(k.0), ContractUpdate::from(v)))
                .collect();

            let system_contract_updates = value
                .system_contract_updates
                .into_iter()
                .map(|(k, v)| (MinimalFelt::from(k.0), SystemContractUpdate::from(v)))
                .collect();

            let declared_cairo_classes = value
                .declared_cairo_classes
                .into_iter()
                .map(|h| MinimalFelt::from(h.0))
                .collect();

            let declared_sierra_classes = value
                .declared_sierra_classes
                .into_iter()
                .map(|(k, v)| (MinimalFelt::from(k.0), MinimalFelt::from(v.0)))
                .collect();

            let migrated_compiled_classes = value
                .migrated_compiled_classes
                .into_iter()
                .map(|(k, v)| (MinimalFelt::from(k.0), MinimalFelt::from(v.0)))
                .collect();

            StateUpdateDataV0 {
                contract_updates,
                system_contract_updates,
                declared_cairo_classes,
                declared_sierra_classes,
                migrated_compiled_classes,
            }
        }
    }

    impl From<StateUpdateDataV0> for pathfinder_common::state_update::StateUpdateData {
        fn from(value: StateUpdateDataV0) -> Self {
            let contract_updates = value
                .contract_updates
                .into_iter()
                .map(|(k, v)| (ContractAddress(k.0), ContractUpdate::into(v)))
                .collect();

            let system_contract_updates = value
                .system_contract_updates
                .into_iter()
                .map(|(k, v)| (ContractAddress(k.0), SystemContractUpdate::into(v)))
                .collect();

            let declared_cairo_classes = value
                .declared_cairo_classes
                .into_iter()
                .map(|h| ClassHash(h.0))
                .collect();

            let declared_sierra_classes = value
                .declared_sierra_classes
                .into_iter()
                .map(|(k, v)| (SierraHash(k.0), CasmHash(v.0)))
                .collect();

            let migrated_compiled_classes = value
                .migrated_compiled_classes
                .into_iter()
                .map(|(k, v)| (SierraHash(k.0), CasmHash(v.0)))
                .collect();

            pathfinder_common::state_update::StateUpdateData {
                contract_updates,
                system_contract_updates,
                declared_cairo_classes,
                declared_sierra_classes,
                migrated_compiled_classes,
            }
        }
    }

    #[derive(serde::Deserialize, serde::Serialize, Default, Debug, PartialEq)]
    pub struct ContractUpdate {
        pub storage: HashMap<MinimalFelt, MinimalFelt>,
        /// The class associated with this update as the result of either a
        /// deploy or class replacement transaction.
        pub class: Option<ContractClassUpdate>,
        pub nonce: Option<MinimalFelt>,
    }

    impl From<pathfinder_common::state_update::ContractUpdate> for ContractUpdate {
        fn from(value: pathfinder_common::state_update::ContractUpdate) -> Self {
            let storage = value
                .storage
                .into_iter()
                .map(|(k, v)| (MinimalFelt::from(k.0), MinimalFelt::from(v.0)))
                .collect();

            let class = value.class.map(ContractClassUpdate::from);

            let nonce = value.nonce.map(|n| MinimalFelt::from(n.0));

            ContractUpdate {
                storage,
                class,
                nonce,
            }
        }
    }

    impl From<ContractUpdate> for pathfinder_common::state_update::ContractUpdate {
        fn from(value: ContractUpdate) -> Self {
            let storage = value
                .storage
                .into_iter()
                .map(|(k, v)| (StorageAddress(k.0), StorageValue(v.0)))
                .collect();

            let class = value.class.map(ContractClassUpdate::into);

            let nonce = value.nonce.map(|n| ContractNonce(n.0));

            pathfinder_common::state_update::ContractUpdate {
                storage,
                class,
                nonce,
            }
        }
    }

    #[derive(serde::Deserialize, serde::Serialize, Default, Debug, PartialEq)]
    pub struct SystemContractUpdate {
        pub storage: HashMap<MinimalFelt, MinimalFelt>,
    }

    impl From<pathfinder_common::state_update::SystemContractUpdate> for SystemContractUpdate {
        fn from(value: pathfinder_common::state_update::SystemContractUpdate) -> Self {
            let storage = value
                .storage
                .into_iter()
                .map(|(k, v)| (MinimalFelt::from(k.0), MinimalFelt::from(v.0)))
                .collect();

            SystemContractUpdate { storage }
        }
    }

    impl From<SystemContractUpdate> for pathfinder_common::state_update::SystemContractUpdate {
        fn from(value: SystemContractUpdate) -> Self {
            let storage = value
                .storage
                .into_iter()
                .map(|(k, v)| (StorageAddress(k.0), StorageValue(v.0)))
                .collect();

            pathfinder_common::state_update::SystemContractUpdate { storage }
        }
    }

    #[derive(serde::Deserialize, serde::Serialize, Debug, PartialEq)]
    pub enum ContractClassUpdate {
        Deploy(MinimalFelt),
        Replace(MinimalFelt),
    }

    impl From<pathfinder_common::state_update::ContractClassUpdate> for ContractClassUpdate {
        fn from(value: pathfinder_common::state_update::ContractClassUpdate) -> Self {
            match value {
                pathfinder_common::state_update::ContractClassUpdate::Deploy(hash) => {
                    ContractClassUpdate::Deploy(MinimalFelt::from(hash.0))
                }
                pathfinder_common::state_update::ContractClassUpdate::Replace(hash) => {
                    ContractClassUpdate::Replace(MinimalFelt::from(hash.0))
                }
            }
        }
    }

    impl From<ContractClassUpdate> for pathfinder_common::state_update::ContractClassUpdate {
        fn from(value: ContractClassUpdate) -> Self {
            match value {
                ContractClassUpdate::Deploy(hash) => {
                    pathfinder_common::state_update::ContractClassUpdate::Deploy(ClassHash(hash.0))
                }
                ContractClassUpdate::Replace(hash) => {
                    pathfinder_common::state_update::ContractClassUpdate::Replace(ClassHash(hash.0))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use pathfinder_common::class_definition::{
        SerializedCairoDefinition,
        SerializedCasmDefinition,
        SerializedSierraDefinition,
    };
    use pathfinder_common::macro_prelude::*;
    use pathfinder_common::BlockHeader;

    use super::*;

    #[test]
    fn class_definition_block_number_is_kept() {
        //! A regression test which ensures that the block number is not
        //! overwritten by subsequent declares. Declare V1s are not checked for
        //! duplicates as these do not form part of the class trie.
        //!
        //! We insert the same class twice in consecutive blocks and ensure the
        //! first sticks.
        let mut db = crate::StorageBuilder::in_memory()
            .unwrap()
            .connection()
            .unwrap();
        let tx = db.transaction().unwrap();

        let target_class = class_hash_bytes!(b"target");

        let header_0 = BlockHeader::builder().finalize_with_hash(block_hash!("0xabc"));
        let header_1 = header_0
            .child_builder()
            .finalize_with_hash(block_hash!("0x123"));

        let state_update = StateUpdate::default().with_declared_cairo_class(target_class);

        tx.insert_cairo_class_definition(target_class, &SerializedCairoDefinition::from_slice(&[]))
            .unwrap();
        tx.insert_block_header(&header_0).unwrap();
        tx.insert_block_header(&header_1).unwrap();
        tx.insert_state_update(header_0.number, &state_update)
            .unwrap();
        tx.insert_state_update(header_1.number, &state_update)
            .unwrap();

        // We expect the first state update to contain the class, and not the second.
        let declared_at = tx
            .class_definition_with_block_number(target_class)
            .unwrap()
            .unwrap()
            .0
            .unwrap();

        assert_eq!(declared_at, header_0.number);
    }

    #[test]
    fn contract_class_hash() {
        let mut db = crate::StorageBuilder::in_memory()
            .unwrap()
            .connection()
            .unwrap();
        let tx = db.transaction().unwrap();

        let original_class = class_hash!("0xdeadbeef");
        let replaced_class = class_hash!("0xdeadbeefabcdef");
        let definition = b"example definition";
        let contract = contract_address!("0x12345");

        let header_0 = BlockHeader::builder().finalize_with_hash(block_hash!("0xabc"));
        let header_1 = header_0
            .child_builder()
            .finalize_with_hash(block_hash!("0xabcdef"));
        let header_2 = header_1
            .child_builder()
            .finalize_with_hash(block_hash!("0xa111123"));
        let header_3 = header_2
            .child_builder()
            .finalize_with_hash(block_hash!("0xa111123aaa"));
        let header_4 = header_3
            .child_builder()
            .finalize_with_hash(block_hash!("0xa111123aaafff"));

        let diff_0 = StateUpdate::default();
        let diff_1 = StateUpdate::default()
            .with_declared_cairo_class(original_class)
            .with_deployed_contract(contract, original_class);
        let diff_2 = StateUpdate::default().with_replaced_class(contract, replaced_class);
        let diff_3 = StateUpdate::default();
        let diff_4 = StateUpdate::default();

        tx.insert_cairo_class_definition(
            original_class,
            &SerializedCairoDefinition::from_slice(definition),
        )
        .unwrap();
        tx.insert_cairo_class_definition(
            replaced_class,
            &SerializedCairoDefinition::from_slice(definition),
        )
        .unwrap();

        tx.insert_block_header(&header_0).unwrap();
        tx.insert_block_header(&header_1).unwrap();
        tx.insert_block_header(&header_2).unwrap();
        tx.insert_block_header(&header_3).unwrap();
        tx.insert_block_header(&header_4).unwrap();

        tx.insert_state_update(header_0.number, &diff_0).unwrap();
        tx.insert_state_update(header_1.number, &diff_1).unwrap();
        tx.insert_state_update(header_2.number, &diff_2).unwrap();
        tx.insert_state_update(header_3.number, &diff_3).unwrap();
        tx.insert_state_update(header_4.number, &diff_4).unwrap();

        let not_deployed_yet = tx
            .contract_class_hash(header_0.number.into(), contract)
            .unwrap();
        assert_eq!(not_deployed_yet, None);

        let not_deployed_yet = tx
            .contract_class_hash(header_0.hash.into(), contract)
            .unwrap();
        assert_eq!(not_deployed_yet, None);

        let is_deployed = tx
            .contract_class_hash(header_1.number.into(), contract)
            .unwrap();
        assert_eq!(is_deployed, Some(original_class));

        let is_deployed = tx
            .contract_class_hash(header_1.hash.into(), contract)
            .unwrap();
        assert_eq!(is_deployed, Some(original_class));

        let is_replaced = tx
            .contract_class_hash(header_2.number.into(), contract)
            .unwrap();
        assert_eq!(is_replaced, Some(replaced_class));

        let is_replaced = tx
            .contract_class_hash(header_2.hash.into(), contract)
            .unwrap();
        assert_eq!(is_replaced, Some(replaced_class));

        let non_existent = contract_address!("0xaaaaa");
        let non_existent = tx
            .contract_class_hash(BlockNumber::GENESIS.into(), non_existent)
            .unwrap();
        assert_eq!(non_existent, None);

        // Query a few blocks after deployment as well. This is a regression case where
        // querying by block hash failed to find the class hash if it wasn't
        // literally the deployed block.
        let is_replaced = tx
            .contract_class_hash(header_4.number.into(), contract)
            .unwrap();
        assert_eq!(is_replaced, Some(replaced_class));
        let is_replaced = tx
            .contract_class_hash(header_4.hash.into(), contract)
            .unwrap();
        assert_eq!(is_replaced, Some(replaced_class));
    }

    mod state_update {
        use super::*;

        const CAIRO_HASH: ClassHash = class_hash_bytes!(b"cairo class hash");
        const SIERRA_HASH: SierraHash = sierra_hash_bytes!(b"sierra hash");
        const CASM_HASH: CasmHash = casm_hash_bytes!(b"casm hash");
        const CASM_HASH_V2: CasmHash = casm_hash_bytes!(b"casm hash blake");
        const CAIRO_HASH2: ClassHash = class_hash_bytes!(b"cairo class hash again");
        const CONTRACT_ADDRESS: ContractAddress = contract_address_bytes!(b"contract addr");

        fn setup() -> (crate::Connection, StateUpdate, BlockHeader) {
            let mut db = crate::StorageBuilder::in_memory()
                .unwrap()
                .connection()
                .unwrap();
            let tx = db.transaction().unwrap();

            // Submit the class definitions since this occurs out of band of the header and
            // state diff.
            tx.insert_cairo_class_definition(
                CAIRO_HASH,
                &SerializedCairoDefinition::from_slice(b"cairo definition"),
            )
            .unwrap();
            tx.insert_cairo_class_definition(
                CAIRO_HASH2,
                &SerializedCairoDefinition::from_slice(b"cairo definition 2"),
            )
            .unwrap();

            tx.insert_sierra_class_definition(
                &SIERRA_HASH,
                &SerializedSierraDefinition::from_slice(b"sierra definition"),
                &SerializedCasmDefinition::from_slice(b"casm definition"),
                &CASM_HASH_V2,
            )
            .unwrap();

            // Create genesis block with a deployed contract so we can replace it in the
            // next block and test against it.
            let genesis_state_update = StateUpdate::default()
                .with_declared_cairo_class(CAIRO_HASH)
                .with_deployed_contract(CONTRACT_ADDRESS, CAIRO_HASH);
            let header = BlockHeader::builder().finalize_with_hash(block_hash!("0xabc"));
            tx.insert_block_header(&header).unwrap();
            tx.insert_state_update(header.number, &genesis_state_update)
                .unwrap();

            // The actual data we want to query.
            let header = header
                .child_builder()
                .finalize_with_hash(block_hash!("0xabcdef"));
            let state_update = StateUpdate::default()
                .with_block_hash(header.hash)
                .with_storage_update(
                    CONTRACT_ADDRESS,
                    storage_address_bytes!(b"storage key"),
                    storage_value_bytes!(b"storage value"),
                )
                .with_system_storage_update(
                    ContractAddress::ONE,
                    storage_address_bytes!(b"key 1"),
                    storage_value_bytes!(b"value 1"),
                )
                .with_system_storage_update(
                    ContractAddress::TWO,
                    storage_address_bytes!(b"key 2"),
                    storage_value_bytes!(b"value 2"),
                )
                .with_deployed_contract(
                    contract_address_bytes!(b"contract addr 2"),
                    ClassHash(SIERRA_HASH.0),
                )
                .with_declared_cairo_class(CAIRO_HASH2)
                .with_declared_sierra_class(SIERRA_HASH, CASM_HASH)
                .with_contract_nonce(CONTRACT_ADDRESS, contract_nonce_bytes!(b"nonce"))
                .with_replaced_class(CONTRACT_ADDRESS, ClassHash(SIERRA_HASH.0));

            tx.insert_block_header(&header).unwrap();
            tx.insert_state_update(header.number, &state_update)
                .unwrap();

            tx.commit().unwrap();

            (db, state_update, header)
        }

        #[test]
        fn state_update() {
            let (mut db, state_update, header) = setup();
            let tx = db.transaction().unwrap();

            let result = tx.state_update(header.number.into()).unwrap().unwrap();
            pretty_assertions_sorted::assert_eq!(result, state_update);

            // check getters for compiled class
            let hash = tx
                .casm_hash_at(BlockId::Latest, ClassHash(SIERRA_HASH.0))
                .unwrap()
                .unwrap();
            assert_eq!(hash, casm_hash_bytes!(b"casm hash"));

            let definition = tx
                .casm_definition_at(BlockId::Latest, ClassHash(SIERRA_HASH.0))
                .unwrap()
                .unwrap();
            assert_eq!(
                definition,
                SerializedCasmDefinition::from_slice(b"casm definition")
            );

            // non-existent state update
            let non_existent = tx.state_update((header.number + 1).into()).unwrap();
            assert_eq!(non_existent, None);
        }

        #[test]
        fn redeclared_classes() {
            let (mut db, _state_update, header) = setup();

            let tx = db.transaction().unwrap();
            let new_header = header
                .child_builder()
                .finalize_with_hash(block_hash!("0xabcdee"));
            let new_state_update = StateUpdate::default()
                .with_block_hash(new_header.hash)
                .with_declared_cairo_class(CAIRO_HASH);
            tx.insert_block_header(&new_header).unwrap();
            tx.insert_state_update(new_header.number, &new_state_update)
                .unwrap();

            tx.commit().unwrap();

            let tx = db.transaction().unwrap();

            let result = tx.state_update(new_header.number.into()).unwrap().unwrap();
            assert_eq!(result, new_state_update);

            let result = tx
                .declared_classes_counts(new_header.number, NonZeroUsize::new(1).unwrap())
                .unwrap();
            assert_eq!(result[0], 1);
        }

        #[test]
        fn migrated_compiled_classes() {
            let (mut db, _state_update, header) = setup();

            let tx = db.transaction().unwrap();
            let new_header = header
                .child_builder()
                .finalize_with_hash(block_hash!("0xabcdee"));
            let new_state_update = StateUpdate::default()
                .with_block_hash(new_header.hash)
                .with_migrated_compiled_class(SIERRA_HASH, casm_hash_bytes!(b"casm hash 2"));
            tx.insert_block_header(&new_header).unwrap();
            tx.insert_state_update(new_header.number, &new_state_update)
                .unwrap();

            tx.commit().unwrap();

            let tx = db.transaction().unwrap();

            let result = tx.state_update(new_header.number.into()).unwrap().unwrap();
            assert_eq!(result, new_state_update);
        }

        #[test]
        fn reverse_state_update() {
            let (mut db, _state_update, header) = setup();
            let tx = db.transaction().unwrap();

            let result = tx
                .reverse_contract_updates(header.number, BlockNumber::GENESIS)
                .unwrap();
            assert_eq!(
                result,
                vec![
                    (
                        contract_address_bytes!(b"contract addr 2"),
                        ReverseContractUpdate::Deleted
                    ),
                    (
                        ContractAddress::ONE,
                        ReverseContractUpdate::Updated(ContractUpdate {
                            storage: HashMap::from([(
                                storage_address_bytes!(b"key 1"),
                                StorageValue::ZERO
                            )]),
                            nonce: None,
                            class: None
                        })
                    ),
                    (
                        ContractAddress::TWO,
                        ReverseContractUpdate::Updated(ContractUpdate {
                            storage: HashMap::from([(
                                storage_address_bytes!(b"key 2"),
                                StorageValue::ZERO
                            )]),
                            nonce: None,
                            class: None
                        })
                    ),
                    (
                        CONTRACT_ADDRESS,
                        ReverseContractUpdate::Updated(ContractUpdate {
                            storage: HashMap::from([(
                                storage_address_bytes!(b"storage key"),
                                StorageValue::ZERO
                            )]),
                            nonce: None,
                            class: Some(ContractClassUpdate::Replace(CAIRO_HASH))
                        })
                    )
                ]
                .into_iter()
                .collect()
            );
        }

        #[test]
        fn reverse_sierra_class_updates() {
            let (mut db, _state_update, header) = setup();
            let tx = db.transaction().unwrap();

            let result = tx
                .reverse_sierra_class_updates(header.number, BlockNumber::GENESIS)
                .unwrap();
            assert_eq!(result, vec![(SIERRA_HASH, None)]);
        }

        #[test]
        fn state_update_writes_go_through_batch() {
            use crate::connection::{
                NONCE_UPDATES_COLUMN,
                STATE_UPDATES_COLUMN,
                STORAGE_UPDATES_COLUMN,
            };

            // Use a file-based storage to get an isolated RocksDB instance, since
            // in_memory() shares a single RocksDB path across sequential test runs in
            // the same process.
            let tmpdir = tempfile::tempdir().unwrap();
            let storage = crate::StorageBuilder::file(tmpdir.path().join("db.sqlite"))
                .migrate()
                .unwrap()
                .create_pool(std::num::NonZeroU32::new(1).unwrap())
                .unwrap();
            let mut conn = storage.connection().unwrap();
            let tx = conn.transaction().unwrap();

            // Build a header at block 5 so the FK in contract_updates is satisfied.
            let header = pathfinder_common::BlockHeader::builder()
                .number(BlockNumber::new_or_panic(5))
                .finalize_with_hash(block_hash!("0xa"));
            tx.insert_block_header(&header).unwrap();

            let contract = contract_address!("0x1");
            let storage_key = storage_address!("0x2");
            let storage_val = storage_value!("0x42");
            let nonce = contract_nonce!("0x1");

            let state_update = pathfinder_common::StateUpdate::default()
                .with_contract_nonce(contract, nonce)
                .with_storage_update(contract, storage_key, storage_val);
            tx.insert_state_update(BlockNumber::new_or_panic(5), &state_update)
                .unwrap();

            // Before commit: nothing is visible via a direct RocksDB read.
            let rocksdb = tx.rocksdb_for_test();
            let state_updates_cf = rocksdb.get_column(&STATE_UPDATES_COLUMN);
            let nonce_cf = rocksdb.get_column(&NONCE_UPDATES_COLUMN);
            let storage_cf = rocksdb.get_column(&STORAGE_UPDATES_COLUMN);
            assert!(
                rocksdb
                    .rocksdb
                    .get_pinned_cf(&state_updates_cf, 5u64.to_be_bytes())
                    .unwrap()
                    .is_none(),
                "state update visible before commit"
            );

            tx.commit().unwrap();

            // After commit: all three column families now hold the data.
            assert!(
                rocksdb
                    .rocksdb
                    .get_pinned_cf(&state_updates_cf, 5u64.to_be_bytes())
                    .unwrap()
                    .is_some(),
                "state update missing after commit"
            );
            assert!(
                rocksdb
                    .rocksdb
                    .get_pinned_cf(
                        &nonce_cf,
                        nonce_update_key(BlockNumber::new_or_panic(5), &contract)
                    )
                    .unwrap()
                    .is_some(),
                "nonce missing after commit"
            );
            assert!(
                rocksdb
                    .rocksdb
                    .get_pinned_cf(
                        &storage_cf,
                        storage_update_key(BlockNumber::new_or_panic(5), &contract, &storage_key)
                    )
                    .unwrap()
                    .is_some(),
                "storage missing after commit"
            );
        }

        #[test]
        fn purge_state_update_writes_go_through_batch() {
            use crate::connection::{
                NONCE_UPDATES_COLUMN,
                STATE_UPDATES_COLUMN,
                STORAGE_UPDATES_COLUMN,
            };

            // Use a file-based storage to get an isolated RocksDB instance, since
            // in_memory() shares a single RocksDB path across sequential test runs in
            // the same process.
            let tmpdir = tempfile::tempdir().unwrap();
            let storage = crate::StorageBuilder::file(tmpdir.path().join("db.sqlite"))
                .migrate()
                .unwrap()
                .create_pool(std::num::NonZeroU32::new(1).unwrap())
                .unwrap();
            let mut conn = storage.connection().unwrap();

            // Seed: insert state update and commit so the data is in RocksDB.
            let header = pathfinder_common::BlockHeader::builder()
                .number(BlockNumber::new_or_panic(6))
                .finalize_with_hash(block_hash!("0xb"));
            let contract = contract_address!("0x3");
            let storage_key = storage_address!("0x4");
            let state_update = pathfinder_common::StateUpdate::default()
                .with_contract_nonce(contract, contract_nonce!("0x2"))
                .with_storage_update(contract, storage_key, storage_value!("0x10"));
            {
                let tx = conn.transaction().unwrap();
                tx.insert_block_header(&header).unwrap();
                tx.insert_state_update(BlockNumber::new_or_panic(6), &state_update)
                    .unwrap();
                tx.commit().unwrap();
            }

            // Now purge in a new transaction. Pre-commit, the RocksDB rows must still
            // be visible; post-commit, they must be gone.
            let tx = conn.transaction().unwrap();
            let rocksdb = tx.rocksdb_for_test();
            let state_cf = rocksdb.get_column(&STATE_UPDATES_COLUMN);
            let nonce_cf = rocksdb.get_column(&NONCE_UPDATES_COLUMN);
            let storage_cf = rocksdb.get_column(&STORAGE_UPDATES_COLUMN);

            assert!(rocksdb
                .rocksdb
                .get_pinned_cf(&state_cf, 6u64.to_be_bytes())
                .unwrap()
                .is_some());
            tx.purge_state_update_data(BlockNumber::new_or_panic(6))
                .unwrap();
            // Pre-commit: deletes are still buffered in the batch.
            assert!(
                rocksdb
                    .rocksdb
                    .get_pinned_cf(&state_cf, 6u64.to_be_bytes())
                    .unwrap()
                    .is_some(),
                "purge applied before commit"
            );

            tx.commit().unwrap();

            assert!(
                rocksdb
                    .rocksdb
                    .get_pinned_cf(&state_cf, 6u64.to_be_bytes())
                    .unwrap()
                    .is_none(),
                "state update still present after purge+commit"
            );
            assert!(
                rocksdb
                    .rocksdb
                    .get_pinned_cf(
                        &nonce_cf,
                        nonce_update_key(BlockNumber::new_or_panic(6), &contract)
                    )
                    .unwrap()
                    .is_none(),
                "nonce still present after purge+commit"
            );
            assert!(
                rocksdb
                    .rocksdb
                    .get_pinned_cf(
                        &storage_cf,
                        storage_update_key(BlockNumber::new_or_panic(6), &contract, &storage_key)
                    )
                    .unwrap()
                    .is_none(),
                "storage still present after purge+commit"
            );
        }
    }

    mod contract_state {
        //! Tests involving contract nonces and storage.
        use super::*;

        /// Create and inserts a basic state diff for testing.
        fn setup() -> (crate::Connection, StateUpdate, BlockHeader) {
            let mut db = crate::StorageBuilder::in_memory()
                .unwrap()
                .connection()
                .unwrap();
            let tx = db.transaction().unwrap();

            let header = BlockHeader::builder().finalize_with_hash(block_hash_bytes!(b"hash"));
            let contract_address = contract_address_bytes!(b"contract address");
            let contract_address2 = contract_address_bytes!(b"contract address 2");
            let state_update = StateUpdate::default()
                .with_contract_nonce(contract_address, contract_nonce_bytes!(b"nonce value"))
                .with_contract_nonce(contract_address2, contract_nonce_bytes!(b"nonce value 2"))
                .with_storage_update(
                    contract_address,
                    storage_address_bytes!(b"storage address"),
                    storage_value_bytes!(b"storage value"),
                );

            tx.insert_block_header(&header).unwrap();
            tx.insert_state_update(header.number, &state_update)
                .unwrap();
            tx.commit().unwrap();

            (db, state_update, header)
        }

        #[test]
        fn get_contract_nonce() {
            let (mut db, state_update, header) = setup();
            let tx = db.transaction().unwrap();

            // Valid 1st contract nonce
            let (contract, expected) = state_update
                .contract_updates
                .iter()
                .filter_map(|(addr, update)| update.nonce.map(|n| (*addr, n)))
                .next()
                .unwrap();

            let latest = tx
                .contract_nonce(contract, BlockId::Latest)
                .unwrap()
                .unwrap();
            assert_eq!(latest, expected);

            let by_number = tx
                .contract_nonce(contract, header.number.into())
                .unwrap()
                .unwrap();
            assert_eq!(by_number, expected);

            let by_hash = tx
                .contract_nonce(contract, header.hash.into())
                .unwrap()
                .unwrap();
            assert_eq!(by_hash, expected);

            // Valid 2nd contract nonce. This exercises a bug where we didn't actually
            // use the contract address when querying by hash. Checking an additional
            // contract guards against only having a single entree to find.
            let (contract, expected) = state_update
                .contract_updates
                .iter()
                .filter_map(|(addr, update)| update.nonce.map(|n| (*addr, n)))
                .nth(1)
                .unwrap();

            let latest = tx
                .contract_nonce(contract, BlockId::Latest)
                .unwrap()
                .unwrap();
            assert_eq!(latest, expected);

            let by_number = tx
                .contract_nonce(contract, header.number.into())
                .unwrap()
                .unwrap();
            assert_eq!(by_number, expected);

            let by_hash = tx
                .contract_nonce(contract, header.hash.into())
                .unwrap()
                .unwrap();
            assert_eq!(by_hash, expected);

            // Invalid i.e. missing contract should be None
            let invalid_contract = contract_address_bytes!(b"invalid");
            let invalid_latest = tx
                .contract_nonce(invalid_contract, BlockId::Latest)
                .unwrap();
            assert_eq!(invalid_latest, None);
            let invalid_by_hash = tx
                .contract_nonce(invalid_contract, block_hash_bytes!(b"invalid").into())
                .unwrap();
            assert_eq!(invalid_by_hash, None);
            let invalid_by_number = tx
                .contract_nonce(invalid_contract, BlockNumber::MAX.into())
                .unwrap();
            assert_eq!(invalid_by_number, None);
        }

        #[test]
        fn get_storage_value() {
            let (mut db, state_update, header) = setup();
            let tx = db.transaction().unwrap();

            let (contract, key, expected) = state_update
                .contract_updates
                .iter()
                .flat_map(|(addr, update)| {
                    update
                        .storage
                        .iter()
                        .map(|(key, value)| (*addr, *key, *value))
                })
                .next()
                .unwrap();

            // Valid key and contract.
            let latest = tx
                .storage_value(BlockId::Latest, contract, key)
                .unwrap()
                .unwrap();
            assert_eq!(latest, expected);
            let by_hash = tx
                .storage_value(header.hash.into(), contract, key)
                .unwrap()
                .unwrap();
            assert_eq!(by_hash, expected);
            let by_number = tx
                .storage_value(header.number.into(), contract, key)
                .unwrap()
                .unwrap();
            assert_eq!(by_number, expected);

            // Invalid key should be none
            let invalid_key = storage_address_bytes!(b"invalid key");
            let latest = tx
                .storage_value(BlockId::Latest, contract, invalid_key)
                .unwrap();
            assert_eq!(latest, None);
            let by_hash = tx
                .storage_value(block_hash_bytes!(b"invalid").into(), contract, invalid_key)
                .unwrap();
            assert_eq!(by_hash, None);
            let by_number = tx
                .storage_value(BlockNumber::MAX.into(), contract, invalid_key)
                .unwrap();
            assert_eq!(by_number, None);

            // Invalid contract should be none
            let invalid_contract = contract_address_bytes!(b"invalid");
            let latest = tx
                .storage_value(BlockId::Latest, invalid_contract, key)
                .unwrap();
            assert_eq!(latest, None);
            let by_hash = tx
                .storage_value(header.hash.into(), invalid_contract, key)
                .unwrap();
            assert_eq!(by_hash, None);
            let by_number = tx
                .storage_value(header.number.into(), invalid_contract, key)
                .unwrap();
            assert_eq!(by_number, None);
        }
    }

    #[test]
    fn storage_value_with_block_returns_latest_le_block() {
        let storage = crate::StorageBuilder::in_memory().unwrap();
        let mut conn = storage.connection().unwrap();
        let tx = conn.transaction().unwrap();

        let contract = contract_address!("0xc");
        let key = storage_address!("0x2");
        let writes = [
            (5u64, storage_value!("0x100")),
            (10u64, storage_value!("0x200")),
            (15u64, storage_value!("0x300")),
        ];
        for (n, v) in &writes {
            let header = BlockHeader::builder()
                .number(BlockNumber::new_or_panic(*n))
                .finalize_with_hash(BlockHash(pathfinder_crypto::Felt::from(*n)));
            tx.insert_block_header(&header).unwrap();
            let state_update = StateUpdate::default().with_storage_update(contract, key, *v);
            tx.insert_state_update(BlockNumber::new_or_panic(*n), &state_update)
                .unwrap();
        }

        // Header for block 12 without any storage update, so the RocksDB
        // prefix-seek path is exercised (block_number returns Some(12)).
        let header12 = BlockHeader::builder()
            .number(BlockNumber::new_or_panic(12))
            .finalize_with_hash(BlockHash(pathfinder_crypto::Felt::from(12u64)));
        tx.insert_block_header(&header12).unwrap();

        tx.commit().unwrap();

        let tx = conn.transaction().unwrap();

        // At block 12: should return value from block 10 (latest write <= 12).
        let (value, block) = tx
            .storage_value_with_block(
                BlockId::Number(BlockNumber::new_or_panic(12)),
                contract,
                key,
            )
            .unwrap()
            .expect("storage value present");
        assert_eq!(value, storage_value!("0x200"));
        assert_eq!(block, BlockNumber::new_or_panic(10));

        // At block 4: no value yet (no header for block 4, so block_number returns None
        // → Ok(None)).
        assert!(tx
            .storage_value_with_block(BlockId::Number(BlockNumber::new_or_panic(4)), contract, key,)
            .unwrap()
            .is_none());

        // Latest: block 15.
        let (value, block) = tx
            .storage_value_with_block(BlockId::Latest, contract, key)
            .unwrap()
            .unwrap();
        assert_eq!(value, storage_value!("0x300"));
        assert_eq!(block, BlockNumber::new_or_panic(15));
    }

    #[test]
    fn contract_nonce_missing_block_is_ok_none() {
        let storage = crate::StorageBuilder::in_memory().unwrap();
        let mut conn = storage.connection().unwrap();
        let tx = conn.transaction().unwrap();

        let result = tx
            .contract_nonce(
                contract_address!("0xa"),
                BlockId::Hash(block_hash!("0xdead")),
            )
            .unwrap();
        assert!(
            result.is_none(),
            "missing block should be Ok(None), got {:?}",
            result
        );
    }
}
