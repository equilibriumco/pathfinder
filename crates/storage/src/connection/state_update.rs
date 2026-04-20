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

use crate::prelude::*;

type StorageUpdates = Vec<(StorageAddress, StorageValue)>;

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
        let mut query_contract_address = self
            .inner()
            .prepare_cached("SELECT id FROM contract_addresses WHERE contract_address = ?")
            .context("Preparing contract address query statement")?;
        let mut insert_contract_address = self
            .inner()
            .prepare_cached(
                "INSERT INTO contract_addresses (contract_address) VALUES (?) RETURNING id",
            )
            .context("Preparing contract address insert statement")?;

        let mut query_storage_address = self
            .inner()
            .prepare_cached("SELECT id FROM storage_addresses WHERE storage_address = ?")
            .context("Preparing storage address query statement")?;
        let mut insert_storage_address = self
            .inner()
            .prepare_cached(
                "INSERT INTO storage_addresses (storage_address) VALUES (?) RETURNING id",
            )
            .context("Preparing storage address insert statement")?;

        let mut insert_nonce = self
            .inner()
            .prepare_cached(
                "INSERT INTO nonce_updates (block_number, contract_address_id, nonce) VALUES (?, \
                 ?, ?)",
            )
            .context("Preparing nonce insert statement")?;

        let mut insert_storage = self
            .inner()
            .prepare_cached(
                "INSERT INTO storage_updates (block_number, contract_address_id, \
                 storage_address_id, storage_value) VALUES (?, ?, ?, ?)",
            )
            .context("Preparing nonce insert statement")?;

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

        for (address, update) in contract_updates {
            if let Some(class_update) = &update.class {
                insert_contract
                    .execute(params![&block_number, address, &class_update.class_hash()])
                    .context("Inserting deployed contract")?;
            }

            let contract_address_id = query_contract_address
                .query_map(params![address], |row| row.get::<_, i64>(0))
                .context("Querying contract address")?
                .next()
                .unwrap_or_else(|| {
                    insert_contract_address.query_row(params![address], |row| row.get::<_, i64>(0))
                })
                .context("Inserting contract address")?;

            if let Some(nonce) = &update.nonce {
                insert_nonce
                    .execute(params![&block_number, &contract_address_id, nonce])
                    .context("Inserting nonce update")?;
            }

            for (key, value) in &update.storage {
                let storage_address_id = query_storage_address
                    .query_map(params![key], |row| row.get::<_, i64>(0))
                    .context("Querying storage address")?
                    .next()
                    .unwrap_or_else(|| {
                        insert_storage_address.query_row(params![key], |row| row.get::<_, i64>(0))
                    })
                    .context("Inserting storage address")?;
                insert_storage
                    .execute(params![
                        &block_number,
                        &contract_address_id,
                        &storage_address_id,
                        value
                    ])
                    .context("Inserting storage update")?;
            }
        }

        for (address, update) in system_contract_updates {
            let contract_address_id = query_contract_address
                .query_map(params![address], |row| row.get::<_, i64>(0))
                .context("Querying contract address")?
                .next()
                .unwrap_or_else(|| {
                    insert_contract_address.query_row(params![address], |row| row.get::<_, i64>(0))
                })
                .context("Inserting contract address")?;
            for (key, value) in &update.storage {
                let storage_address_id = query_storage_address
                    .query_map(params![key], |row| row.get::<_, i64>(0))
                    .context("Querying storage address")?
                    .next()
                    .unwrap_or_else(|| {
                        insert_storage_address.query_row(params![key], |row| row.get::<_, i64>(0))
                    })
                    .context("Inserting storage address")?;
                insert_storage
                    .execute(params![
                        &block_number,
                        &contract_address_id,
                        &storage_address_id,
                        value
                    ])
                    .context("Inserting system storage update")?;
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

        let mut state_update = StateUpdate::default()
            .with_block_hash(block_hash)
            .with_state_commitment(state_commitment)
            .with_parent_state_commitment(parent_state_commitment);

        let mut stmt = self
            .inner()
            .prepare_cached(
                r"
                SELECT contract_address, nonce FROM nonce_updates
                JOIN contract_addresses ON contract_addresses.id = nonce_updates.contract_address_id
                WHERE block_number = ?
                ",
            )
            .context("Preparing nonce update query statement")?;

        let mut nonces = stmt
            .query_map(params![&block_number], |row| {
                let contract_address = row.get_contract_address(0)?;
                let nonce = row.get_contract_nonce(1)?;

                Ok((contract_address, nonce))
            })
            .context("Querying nonce updates")?;

        while let Some((address, nonce)) = nonces
            .next()
            .transpose()
            .context("Iterating over nonce query rows")?
        {
            state_update = state_update.with_contract_nonce(address, nonce);
        }

        let mut stmt = self
            .inner()
            .prepare_cached(
                r"
                SELECT contract_address, storage_address, storage_value
                FROM storage_updates
                JOIN contract_addresses ON contract_addresses.id = storage_updates.contract_address_id
                JOIN storage_addresses ON storage_addresses.id = storage_updates.storage_address_id
                WHERE block_number = ?
                ",
            )
            .context("Preparing storage update query statement")?;
        let mut storage_diffs = stmt
            .query_map(params![&block_number], |row| {
                let address: ContractAddress = row.get_contract_address(0)?;
                let key: StorageAddress = row.get_storage_address(1)?;
                let value: StorageValue = row.get_storage_value(2)?;

                Ok((address, key, value))
            })
            .context("Querying storage updates")?;

        while let Some((address, key, value)) = storage_diffs
            .next()
            .transpose()
            .context("Iterating over storage query rows")?
        {
            state_update = if address.is_system_contract() {
                state_update.with_system_storage_update(address, key, value)
            } else {
                state_update.with_storage_update(address, key, value)
            };
        }

        let mut stmt = self
            .inner()
            .prepare_cached(
                r"SELECT
                class_definitions.hash AS class_hash,
                casm_class_hashes.compiled_class_hash AS compiled_class_hash
            FROM
                class_definitions
            LEFT OUTER JOIN
                casm_class_hashes ON casm_class_hashes.hash = class_definitions.hash AND casm_class_hashes.block_number = class_definitions.block_number
            WHERE
                class_definitions.block_number = ?",
            )
            .context("Preparing class declaration query statement")?;

        let mut declared_classes = stmt
            .query_map(params![&block_number], |row| {
                let class_hash: ClassHash = row.get_class_hash(0)?;
                let casm_hash = row.get_optional_casm_hash(1)?;

                Ok((class_hash, casm_hash))
            })
            .context("Querying class declarations")?;

        while let Some((class_hash, casm)) = declared_classes
            .next()
            .transpose()
            .context("Iterating over class declaration query rows")?
        {
            state_update = match casm {
                Some(casm) => {
                    state_update.with_declared_sierra_class(SierraHash(class_hash.0), casm)
                }
                None => state_update.with_declared_cairo_class(class_hash),
            };
        }

        let mut stmt = self
            .inner()
            .prepare_cached(r"SELECT class_hash FROM redeclared_classes WHERE block_number = ?")
            .context("Preparing re-declared class query statement")?;

        let mut redeclared_classes = stmt
            .query_map(params![&block_number], |row| row.get_class_hash(0))
            .context("Querying re-declared classes")?;
        while let Some(class_hash) = redeclared_classes
            .next()
            .transpose()
            .context("Iterating over re-declared classes")?
        {
            state_update = state_update.with_declared_cairo_class(class_hash);
        }

        let mut stmt = self
            .inner()
            .prepare_cached(
                r"
            SELECT
                ch1.hash AS class_hash,
                ch1.compiled_class_hash AS casm_hash
            FROM
                casm_class_hashes ch1
            LEFT OUTER JOIN
                casm_class_hashes ch2 ON ch1.hash = ch2.hash AND ch2.block_number < ch1.block_number
            WHERE
                ch1.block_number = ? AND ch2.block_number IS NOT NULL
            ",
            )
            .context("Preparing migrated compiled class query statement")?;
        let mut migrated_compiled_classes = stmt
            .query_map(params![&block_number], |row| {
                let class_hash: ClassHash = row.get_class_hash(0)?;
                let casm_hash: CasmHash = row.get_casm_hash(1)?;

                Ok((SierraHash(class_hash.0), casm_hash))
            })
            .context("Querying migrated compiled classes")?;
        while let Some((sierra_hash, casm_hash)) = migrated_compiled_classes
            .next()
            .transpose()
            .context("Iterating over migrated compiled class query rows")?
        {
            state_update = state_update.with_migrated_compiled_class(sierra_hash, casm_hash);
        }

        let mut stmt = self
        .inner().prepare_cached(
            r"SELECT
                cu1.contract_address AS contract_address,
                cu1.class_hash AS class_hash,
                cu2.block_number IS NOT NULL AS is_replaced
            FROM
                contract_updates cu1
            LEFT OUTER JOIN
                contract_updates cu2 ON cu1.contract_address = cu2.contract_address AND cu2.block_number < cu1.block_number
            WHERE
                cu1.block_number = ?",
        )
        .context("Preparing contract update query statement")?;

        let mut deployed_and_replaced_contracts = stmt
            .query_map(params![&block_number], |row| {
                let address: ContractAddress = row.get_contract_address(0)?;
                let class_hash: ClassHash = row.get_class_hash(1)?;
                let is_replaced: bool = row.get(2)?;

                Ok((address, class_hash, is_replaced))
            })
            .context("Querying contract deployments")?;

        while let Some((address, class_hash, is_replaced)) = deployed_and_replaced_contracts
            .next()
            .transpose()
            .context("Iterating over contract deployment query rows")?
        {
            state_update = if is_replaced {
                state_update.with_replaced_class(address, class_hash)
            } else {
                state_update.with_deployed_contract(address, class_hash)
            };
        }

        Ok(Some(state_update))
    }

    pub fn highest_block_with_state_update(&self) -> anyhow::Result<Option<BlockNumber>> {
        let mut stmt = self.inner().prepare_cached(
            r"
            SELECT max(storage_update.last_block, nonce_update.last_block, class_definition.last_block)
            FROM
                (SELECT max(block_number) last_block FROM storage_updates) storage_update,
                (SELECT max(block_number) last_block FROM nonce_updates) nonce_update,
                (SELECT max(block_number) last_block FROM class_definitions) class_definition",
        )?;
        stmt.query_row([], |row| row.get_optional_block_number(0))
            .context("Querying highest storage update")
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
        self.storage_value_with_block(block, contract_address, key)
            .map(|opt_pair| opt_pair.map(|pair| pair.0))
    }

    pub fn storage_value_with_block(
        &self,
        block: BlockId,
        contract_address: ContractAddress,
        key: StorageAddress,
    ) -> anyhow::Result<Option<(StorageValue, BlockNumber)>> {
        let handle_row =
            |row: &rusqlite::Row<'_>| Ok((row.get_storage_value(0)?, row.get_block_number(1)?));
        match block {
            BlockId::Latest => {
                let mut stmt = self.inner().prepare_cached(
                    r"
                    SELECT storage_value, block_number
                    FROM storage_updates
                    JOIN contract_addresses ON contract_addresses.id = storage_updates.contract_address_id
                    JOIN storage_addresses ON storage_addresses.id = storage_updates.storage_address_id
                    WHERE contract_address = ? AND storage_address = ?
                    ORDER BY block_number DESC LIMIT 1
                    ",
                )?;
                stmt.query_row(params![&contract_address, &key], handle_row)
            }
            BlockId::Number(number) => {
                let mut stmt = self.inner().prepare_cached(
                    r"
                    SELECT storage_value, block_number
                    FROM storage_updates
                    JOIN contract_addresses ON contract_addresses.id = storage_updates.contract_address_id
                    JOIN storage_addresses ON storage_addresses.id = storage_updates.storage_address_id
                    WHERE contract_address = ? AND storage_address = ? AND block_number <= ?
                    ORDER BY block_number DESC LIMIT 1
                    ",
                )?;
                stmt.query_row(params![&contract_address, &key, &number], handle_row)
            }
            BlockId::Hash(hash) => {
                let mut stmt = self.inner().prepare_cached(
                    r"
                    SELECT storage_value, block_number
                    FROM storage_updates
                    JOIN contract_addresses ON contract_addresses.id = storage_updates.contract_address_id
                    JOIN storage_addresses ON storage_addresses.id = storage_updates.storage_address_id
                    WHERE contract_address = ? AND storage_address = ? AND block_number <= (
                        SELECT number FROM block_headers WHERE hash = ?
                    )
                    ORDER BY block_number DESC LIMIT 1
                    ",
                )?;
                stmt.query_row(params![&contract_address, &key, &hash], handle_row)
            }
        }
        .optional()
        .map_err(|e| e.into())
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
        match block_id {
            BlockId::Latest => {
                let mut stmt = self.inner().prepare_cached(
                    r"
                    SELECT nonce FROM nonce_updates
                    JOIN contract_addresses ON contract_addresses.id = nonce_updates.contract_address_id
                    WHERE contract_address = ?
                    ORDER BY block_number DESC LIMIT 1
                    ",
                )?;
                stmt.query_row(params![&contract_address], |row| row.get_contract_nonce(0))
            }
            BlockId::Number(number) => {
                let mut stmt = self.inner().prepare_cached(
                    r"
                    SELECT nonce FROM nonce_updates
                    JOIN contract_addresses ON contract_addresses.id = nonce_updates.contract_address_id
                    WHERE contract_address = ? AND block_number <= ?
                    ORDER BY block_number DESC LIMIT 1
                    ",
                )?;
                stmt.query_row(params![&contract_address, &number], |row| {
                    row.get_contract_nonce(0)
                })
            }
            BlockId::Hash(hash) => {
                let mut stmt = self.inner().prepare_cached(
                    r"
                    SELECT nonce FROM nonce_updates
                    JOIN contract_addresses ON contract_addresses.id = nonce_updates.contract_address_id
                    WHERE contract_address = ? AND block_number <= (
                        SELECT number FROM block_headers WHERE hash = ?
                    )
                    ORDER BY block_number DESC LIMIT 1
                    ",
                )?;
                stmt.query_row(params![&contract_address, &hash], |row| {
                    row.get_contract_nonce(0)
                })
            }
        }
        .optional()
        .map_err(|e| e.into())
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
        let mut stmt = self.inner().prepare_cached(
            r"
            WITH
                updated_addresses(contract_address, storage_address, contract_address_id, storage_address_id) AS (
                    SELECT DISTINCT
                    contract_address,
                    storage_address,
                    contract_address_id,
                    storage_address_id
                    FROM storage_updates
                    JOIN contract_addresses ON contract_addresses.id = storage_updates.contract_address_id
                    JOIN storage_addresses ON storage_addresses.id = storage_updates.storage_address_id
                    WHERE
                        block_number > ?2 AND block_number <= ?1
                )
            SELECT
                contract_address,
                storage_address,
                (
                    SELECT storage_value
                    FROM storage_updates
                    WHERE
                    contract_address_id=updated_addresses.contract_address_id AND storage_address_id=updated_addresses.storage_address_id AND block_number <= ?2
                    ORDER BY block_number DESC
                    LIMIT 1
                ) AS old_storage_value
            FROM updated_addresses
            "
        )?;

        let mut rows = stmt
            .query_map(params![&from_block, &to_block], |row| {
                let contract_address = row.get_contract_address(0)?;
                let storage_address = row.get_storage_address(1)?;
                let old_storage_value = row.get_optional_storage_value(2)?;

                Ok((
                    contract_address,
                    (
                        storage_address,
                        old_storage_value.unwrap_or(StorageValue::ZERO),
                    ),
                ))
            })
            .context("Querying reverse storage updates")?;

        let mut storage_updates: HashMap<_, Vec<_>> = Default::default();

        while let Some((contract_address, (storage_address, old_storage_value))) = rows
            .next()
            .transpose()
            .context("Iterating over reverse storage updates")?
        {
            let entry = storage_updates.entry(contract_address).or_default();
            entry.push((storage_address, old_storage_value));
        }

        Ok(storage_updates)
    }

    fn reverse_nonce_updates(
        &self,
        from_block: BlockNumber,
        to_block: BlockNumber,
    ) -> anyhow::Result<Vec<(ContractAddress, Option<ContractNonce>)>> {
        let mut stmt = self.inner().prepare_cached(
            r"WITH
                updated_nonces(contract_address_id, contract_address) AS (
                    SELECT DISTINCT contract_address_id, contract_address
                    FROM nonce_updates
                    JOIN contract_addresses ON contract_addresses.id = nonce_updates.contract_address_id
                    WHERE
                        block_number > ?2 AND block_number <= ?1
                )
            SELECT
                contract_address,
                (
                    SELECT nonce
                    FROM nonce_updates
                    WHERE
                        contract_address_id=updated_nonces.contract_address_id AND block_number <= ?2
                    ORDER BY block_number DESC
                    LIMIT 1
                ) AS old_nonce
            FROM updated_nonces",
        )?;

        let rows = stmt
            .query_map(params![&from_block, &to_block], |row| {
                let contract_address = row.get_contract_address(0)?;
                let old_nonce = row.get_optional_nonce(1)?;

                Ok((contract_address, old_nonce))
            })
            .context("Querying reverse nonce updates")?;

        rows.collect::<Result<Vec<_>, _>>()
            .context("Iterating over reverse nonce updates")
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
}
