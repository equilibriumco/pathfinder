use std::collections::VecDeque;
use std::num::NonZeroUsize;

use anyhow::Context;
use pathfinder_common::prelude::*;
use pathfinder_common::BlockId;

use crate::prelude::*;

impl Transaction<'_> {
    pub fn insert_block_header(&self, header: &BlockHeader) -> anyhow::Result<()> {
        // Insert the header
        self.inner().execute(
        r"INSERT INTO block_headers 
                   ( number,  hash,  parent_hash,  timestamp,  eth_l1_gas_price,  strk_l1_gas_price,  eth_l1_data_gas_price,  strk_l1_data_gas_price,  eth_l2_gas_price,  strk_l2_gas_price,  sequencer_address,  version,  transaction_commitment,  event_commitment,  state_commitment,  transaction_count,  event_count,  l1_da_mode,  receipt_commitment,  state_diff_commitment,  state_diff_length)
            VALUES (:number, :hash, :parent_hash, :timestamp, :eth_l1_gas_price, :strk_l1_gas_price, :eth_l1_data_gas_price, :strk_l1_data_gas_price, :eth_l2_gas_price, :strk_l2_gas_price, :sequencer_address, :version, :transaction_commitment, :event_commitment, :state_commitment, :transaction_count, :event_count, :l1_da_mode, :receipt_commitment, :state_diff_commitment, :state_diff_length)",
        named_params! {
            ":number": &header.number,
            ":hash": &header.hash,
            ":parent_hash": &header.parent_hash,
            ":timestamp": &header.timestamp,
            ":eth_l1_gas_price": &header.eth_l1_gas_price.to_be_bytes().as_slice(),
            ":strk_l1_gas_price": &header.strk_l1_gas_price.to_be_bytes().as_slice(),
            ":eth_l1_data_gas_price": &header.eth_l1_data_gas_price.to_be_bytes().as_slice(),
            ":strk_l1_data_gas_price": &header.strk_l1_data_gas_price.to_be_bytes().as_slice(),
            ":eth_l2_gas_price": &header.eth_l2_gas_price.to_be_bytes().as_slice(),
            ":strk_l2_gas_price": &header.strk_l2_gas_price.to_be_bytes().as_slice(),
            ":sequencer_address": &header.sequencer_address,
            ":version": &header.starknet_version.as_u32(),
            ":transaction_commitment": &header.transaction_commitment,
            ":event_commitment": &header.event_commitment,
            ":transaction_count": &header.transaction_count.try_into_sql_int()?,
            ":event_count": &header.event_count.try_into_sql_int()?,
            ":state_commitment": &header.state_commitment,
            ":l1_da_mode": &header.l1_da_mode,
            ":receipt_commitment": &header.receipt_commitment,
            ":state_diff_commitment": &header.state_diff_commitment,
            ":state_diff_length": &header.state_diff_length.try_into_sql_int()?,
        },
        ).context("Inserting block header")?;

        Ok(())
    }

    /// Returns the closest ancestor header that is in storage.
    ///
    /// i.e. returns the latest header with number < target.
    pub fn next_ancestor(
        &self,
        target: BlockNumber,
    ) -> anyhow::Result<Option<(BlockNumber, BlockHash)>> {
        self.inner()
            .query_row(
                "SELECT number,hash FROM block_headers 
                WHERE number < ? 
                ORDER BY number DESC LIMIT 1",
                params![&target],
                |row| {
                    let number = row.get_block_number(0)?;
                    let hash = row.get_block_hash(1)?;
                    Ok((number, hash))
                },
            )
            .optional()
            .map_err(|x| x.into())
    }

    /// Searches in reverse chronological order for a block that exists in
    /// storage, but whose parent does not.
    ///
    /// Note that target is included in the search.
    pub fn next_ancestor_without_parent(
        &self,
        target: BlockNumber,
    ) -> anyhow::Result<Option<(BlockNumber, BlockHash)>> {
        self.inner()
            .query_row(
                "SELECT number,hash FROM block_headers t1 
                WHERE number <= ? AND number > 0 AND
                NOT EXISTS (SELECT * FROM block_headers t2 WHERE t1.number - 1 = t2.number) 
                ORDER BY number DESC LIMIT 1;",
                params![&target],
                |row| {
                    let number = row.get_block_number(0)?;
                    let hash = row.get_block_hash(1)?;
                    Ok((number, hash))
                },
            )
            .optional()
            .map_err(|x| x.into())
    }

    /// Removes all data related to this block.
    ///
    /// This includes block header, block body and state update information.
    pub fn purge_block(&self, block: BlockNumber) -> anyhow::Result<()> {
        self.inner()
            .execute(
                r"
                DELETE FROM event_filters
                WHERE from_block <= :block AND to_block >= :block
                ",
                named_params![":block": &block],
            )
            .context("Deleting event bloom filter")?;

        self.inner()
            .execute(
                "DELETE FROM block_headers WHERE number = ?",
                params![&block],
            )
            .context("Deleting block from block_headers table")?;

        self.inner()
            .execute(
                "DELETE FROM contract_updates WHERE block_number = ?",
                params![&block],
            )
            .context("Deleting block from contract_updates table")?;

        self.inner()
            .execute(
                "DELETE FROM nonce_updates WHERE block_number = ?",
                params![&block],
            )
            .context("Deleting block from nonce_updates table")?;

        self.inner()
            .execute(
                "DELETE FROM storage_updates WHERE block_number = ?",
                params![&block],
            )
            .context("Deleting block from storage_updates table")?;

        self.inner()
            .execute(
                "DELETE FROM contract_roots WHERE block_number = ?",
                params![&block],
            )
            .context("Deleting block from contract_roots table")?;

        self.inner()
            .execute(
                "DELETE FROM class_commitment_leaves WHERE block_number = ?",
                params![&block],
            )
            .context("Deleting block from class_commitment_leaves table")?;

        self.inner()
            .execute(
                "DELETE FROM contract_state_hashes WHERE block_number = ?",
                params![&block],
            )
            .context("Deleting block from contract_state_hashes table")?;

        self.inner()
            .execute(
                "DELETE FROM class_roots WHERE block_number = ?",
                params![&block],
            )
            .context("Deleting block from class_roots table")?;

        self.inner()
            .execute(
                "DELETE FROM storage_roots WHERE block_number = ?",
                params![&block],
            )
            .context("Deleting block from storage_roots table")?;

        self.inner()
            .execute(
                "DELETE FROM trie_contracts_removals WHERE block_number = ?",
                params![&block],
            )
            .context("Deleting block from trie_contracts_removals table")?;

        self.inner()
            .execute(
                "DELETE FROM trie_storage_removals WHERE block_number = ?",
                params![&block],
            )
            .context("Deleting block from trie_storage_removals table")?;

        self.inner()
            .execute(
                "DELETE FROM trie_class_removals WHERE block_number = ?",
                params![&block],
            )
            .context("Deleting block from trie_class_removals table")?;

        self.inner()
            .execute(
                "UPDATE class_definitions SET block_number = NULL WHERE block_number = ?",
                params![&block],
            )
            .context("Removing class definitions for purged block")?;

        Ok(())
    }

    pub fn block_id(&self, block: BlockId) -> anyhow::Result<Option<(BlockNumber, BlockHash)>> {
        match block {
            BlockId::Latest => self.inner().query_row(
                "SELECT number, hash FROM block_headers ORDER BY number DESC LIMIT 1",
                [],
                |row| {
                    let number = row.get_block_number(0)?;
                    let hash = row.get_block_hash(1)?;

                    Ok((number, hash))
                },
            ),
            BlockId::Number(number) => self.inner().query_row(
                "SELECT hash FROM block_headers WHERE number = ?",
                params![&number],
                |row| {
                    let hash = row.get_block_hash(0)?;
                    Ok((number, hash))
                },
            ),
            BlockId::Hash(hash) => self.inner().query_row(
                "SELECT number FROM block_headers WHERE hash = ?",
                params![&hash],
                |row| {
                    let number = row.get_block_number(0)?;
                    Ok((number, hash))
                },
            ),
        }
        .optional()
        .map_err(|e| e.into())
    }

    pub fn block_hash(&self, block: BlockId) -> anyhow::Result<Option<BlockHash>> {
        match block {
            BlockId::Latest => self
                .inner()
                .query_row(
                    "SELECT hash FROM block_headers ORDER BY number DESC LIMIT 1",
                    [],
                    |row| row.get_block_hash(0),
                )
                .optional()
                .map_err(|e| e.into()),
            BlockId::Number(number) => self
                .inner()
                .query_row(
                    "SELECT hash FROM block_headers WHERE number = ?",
                    params![&number],
                    |row| row.get_block_hash(0),
                )
                .optional()
                .map_err(|e| e.into()),
            BlockId::Hash(hash) => {
                // This query ensures that the block exists.
                self.inner()
                    .query_row(
                        "SELECT hash FROM block_headers WHERE hash = ?",
                        params![&hash],
                        |row| row.get_block_hash(0),
                    )
                    .optional()
                    .map_err(|e| e.into())
            }
        }
    }

    pub fn block_number(&self, block: BlockId) -> anyhow::Result<Option<BlockNumber>> {
        match block {
            BlockId::Latest => self
                .inner()
                .query_row(
                    "SELECT number FROM block_headers ORDER BY number DESC LIMIT 1",
                    [],
                    |row| row.get_block_number(0),
                )
                .optional()
                .map_err(|e| e.into()),
            BlockId::Number(number) => {
                // This query ensures that the block exists.
                self.inner()
                    .query_row(
                        "SELECT number FROM block_headers WHERE number = ?",
                        params![&number],
                        |row| row.get_block_number(0),
                    )
                    .optional()
                    .map_err(|e| e.into())
            }
            BlockId::Hash(hash) => self
                .inner()
                .query_row(
                    "SELECT number FROM block_headers WHERE hash = ?",
                    params![&hash],
                    |row| row.get_block_number(0),
                )
                .optional()
                .map_err(|e| e.into()),
        }
    }

    /// Returns the lowest block number currently in the database. The usage of
    /// this function makes sense only in the context of
    /// [blockchain pruning](crate::pruning).
    pub fn earliest_block_number(&self) -> anyhow::Result<Option<BlockNumber>> {
        self.inner()
            .query_row(
                "SELECT number FROM block_headers ORDER BY number ASC LIMIT 1",
                [],
                |row| row.get_block_number(0),
            )
            .optional()
            .map_err(|e| e.into())
    }

    pub fn block_exists(&self, block: BlockId) -> anyhow::Result<bool> {
        match block {
            BlockId::Latest => {
                let mut stmt = self
                    .inner()
                    .prepare_cached("SELECT EXISTS(SELECT 1 FROM block_headers)")?;
                stmt.query_row([], |row| row.get(0))
            }
            BlockId::Number(number) => {
                let mut stmt = self.inner().prepare_cached(
                    "SELECT EXISTS(SELECT 1 FROM block_headers WHERE number = ?)",
                )?;
                stmt.query_row(params![&number], |row| row.get(0))
            }
            BlockId::Hash(hash) => {
                let mut stmt = self
                    .inner()
                    .prepare_cached("SELECT EXISTS(SELECT 1 FROM block_headers WHERE hash = ?)")?;
                stmt.query_row(params![&hash], |row| row.get(0))
            }
        }
        .map_err(|e| e.into())
    }

    pub fn block_version(&self, block: BlockNumber) -> anyhow::Result<Option<StarknetVersion>> {
        let mut stmt = self
            .inner()
            .prepare_cached("SELECT version FROM block_headers WHERE number = ?")?;
        stmt.query_row(params![&block], |row| row.get_starknet_version(0))
            .optional()
            .map_err(|e| e.into())
    }

    pub fn block_header(&self, block: BlockId) -> anyhow::Result<Option<BlockHeader>> {
        let sql = match block {
            BlockId::Latest => "SELECT * FROM block_headers ORDER BY number DESC LIMIT 1",
            BlockId::Number(_) => "SELECT * FROM block_headers WHERE number = ?",
            BlockId::Hash(_) => "SELECT * FROM block_headers WHERE hash = ?",
        };

        let mut stmt = self
            .inner()
            .prepare_cached(sql)
            .context("Preparing block header query")?;

        let header = match block {
            BlockId::Latest => stmt.query_row([], parse_row_as_header),
            BlockId::Number(number) => stmt.query_row(params![&number], parse_row_as_header),
            BlockId::Hash(hash) => stmt.query_row(params![&hash], parse_row_as_header),
        }
        .optional()
        .context("Querying for block header")?;

        Ok(header)
    }

    /// Return all block headers from a range, inclusive on both ends.
    pub fn block_range(
        &self,
        from: BlockNumber,
        to: BlockNumber,
    ) -> anyhow::Result<Vec<BlockHeader>> {
        let sql =
            "SELECT * FROM block_headers WHERE number >= $1 AND number <= $2 ORDER BY number ASC";
        let mut stmt = self
            .inner()
            .prepare_cached(sql)
            .context("Preparing block header query")?;
        let mut headers = Vec::new();
        let mut rows = stmt.query(params![&from, &to])?;
        while let Some(row) = rows.next()? {
            let header = parse_row_as_header(row)?;
            headers.push(header);
        }
        Ok(headers)
    }

    pub fn state_commitment(&self, block: BlockId) -> anyhow::Result<Option<StateCommitment>> {
        let sql = match block {
            BlockId::Latest => {
                "SELECT state_commitment FROM block_headers ORDER BY number DESC LIMIT 1"
            }
            BlockId::Number(_) => "SELECT state_commitment FROM block_headers WHERE number = ?",
            BlockId::Hash(_) => "SELECT state_commitment FROM block_headers WHERE hash = ?",
        };

        let mut stmt = self
            .inner()
            .prepare_cached(sql)
            .context("Preparing state commitment query")?;

        let state_commitment = match block {
            BlockId::Latest => {
                stmt.query_row([], |row| row.get_state_commitment("state_commitment"))
            }
            BlockId::Number(number) => stmt.query_row(params![&number], |row| {
                row.get_state_commitment("state_commitment")
            }),
            BlockId::Hash(hash) => stmt.query_row(params![&hash], |row| {
                row.get_state_commitment("state_commitment")
            }),
        }
        .optional()
        .context("Querying for state commitment")?;

        Ok(state_commitment)
    }

    pub fn block_is_l1_accepted(&self, block: BlockId) -> anyhow::Result<bool> {
        let Some(l1_l2) = self.l1_l2_pointer().context("Querying L1-L2 pointer")? else {
            return Ok(false);
        };

        let Some((block_number, _)) = self.block_id(block).context("Fetching block number")? else {
            return Ok(false);
        };

        Ok(block_number <= l1_l2)
    }

    pub fn first_block_without_transactions(&self) -> anyhow::Result<Option<BlockNumber>> {
        let mut stmt = self
            .inner()
            .prepare_cached(
                r"
                SELECT number
                FROM block_headers
                LEFT JOIN transactions ON transactions.block_number = block_headers.number
                GROUP BY block_headers.number
                HAVING COUNT(transactions.block_number) = 0
                ORDER BY number ASC
                LIMIT 1;
                ",
            )
            .context("Preparing first_block_without_transactions query")?;

        let mut rows = stmt
            .query(params![])
            .context("Executing first_block_without_transactions")?;

        match rows.next()? {
            Some(row) => Ok(Some(row.get_block_number(0)?)),
            None => Ok(None),
        }
    }

    pub fn first_block_with_missing_class_definitions(
        &self,
    ) -> anyhow::Result<Option<BlockNumber>> {
        let mut stmt = self.inner().prepare_cached(
            r"
            SELECT min(block_number)
            FROM class_definitions
            WHERE definition IS NULL",
        )?;
        stmt.query_row([], |row| row.get_optional_block_number(0))
            .context("Querying first block with missing class definitions")
    }

    pub fn highest_block_with_all_events_downloaded(&self) -> anyhow::Result<Option<BlockNumber>> {
        let mut stmt = self.inner().prepare_cached(
            r"SELECT block_number
        FROM starknet_events_filters
        ORDER BY block_number DESC
        LIMIT 1",
        )?;
        stmt.query_row([], |row| row.get_block_number(0))
            .optional()
            .context("Querying highest block with events")
    }

    pub fn event_counts(
        &self,
        block_number: BlockNumber,
        max_len: NonZeroUsize,
    ) -> anyhow::Result<VecDeque<usize>> {
        let mut stmt = self
            .inner()
            .prepare_cached(
                "SELECT event_count FROM block_headers WHERE number >= ? ORDER BY number ASC \
                 LIMIT ?",
            )
            .context("Preparing get event counts statement")?;

        let mut counts = stmt
            .query_map(
                params![&block_number, &max_len.try_into_sql_int()?],
                |row| row.get_usize(0),
            )
            .context("Querying event counts")?;

        let mut ret = VecDeque::new();

        while let Some(stat) = counts
            .next()
            .transpose()
            .context("Iterating over event counts rows")?
        {
            ret.push_back(stat);
        }

        Ok(ret)
    }

    pub fn transaction_counts(
        &self,
        block_number: BlockNumber,
        max_len: NonZeroUsize,
    ) -> anyhow::Result<VecDeque<usize>> {
        let mut stmt = self
            .inner()
            .prepare_cached(
                "SELECT transaction_count FROM block_headers WHERE number >= ? ORDER BY number \
                 ASC LIMIT ?",
            )
            .context("Preparing get transaction counts statement")?;

        let mut rows = stmt
            .query_map(
                params![&block_number, &max_len.try_into_sql_int()?],
                |row| row.get_usize(0),
            )
            .context("Querying transaction counts")?;

        let mut ret = VecDeque::new();

        while let Some(cc) = rows
            .next()
            .transpose()
            .context("Iterating over rows of transaction counts")?
        {
            ret.push_back(cc);
        }

        Ok(ret)
    }

    pub fn state_diff_commitment(
        &self,
        block_number: BlockNumber,
    ) -> anyhow::Result<Option<StateDiffCommitment>> {
        let mut stmt = self
            .inner()
            .prepare_cached("SELECT state_diff_commitment FROM block_headers WHERE number = ?")
            .context("Preparing state diff commitment query")?;

        let state_diff_commitment = stmt
            .query_row(params![&block_number], |row| {
                row.get_state_diff_commitment("state_diff_commitment")
            })
            .optional()
            .context("Querying for state diff commitment")?;

        Ok(state_diff_commitment)
    }

    pub fn transaction_commitment(
        &self,
        block_number: BlockNumber,
    ) -> anyhow::Result<Option<TransactionCommitment>> {
        let mut stmt = self
            .inner()
            .prepare_cached("SELECT transaction_commitment FROM block_headers WHERE number = ?")
            .context("Preparing transaction commitment query")?;

        let transaction_commitment = stmt
            .query_row(params![&block_number], |row| {
                row.get_transaction_commitment("transaction_commitment")
            })
            .optional()
            .context("Querying for transaction commitment")?;

        Ok(transaction_commitment)
    }
}

fn parse_row_as_header(row: &rusqlite::Row<'_>) -> rusqlite::Result<BlockHeader> {
    let number = row.get_block_number("number")?;
    let hash = row.get_block_hash("hash")?;
    let parent_hash = row.get_block_hash("parent_hash")?;
    let timestamp = row.get_timestamp("timestamp")?;
    let eth_l1_gas_price = row.get_gas_price("eth_l1_gas_price")?;
    let strk_l1_gas_price = row
        .get_optional_gas_price("strk_l1_gas_price")?
        .unwrap_or(GasPrice::ZERO);
    let eth_l1_data_gas_price = row
        .get_optional_gas_price("eth_l1_data_gas_price")?
        .unwrap_or(GasPrice::ZERO);
    let strk_l1_data_gas_price = row
        .get_optional_gas_price("strk_l1_data_gas_price")?
        .unwrap_or(GasPrice::ZERO);
    let eth_l2_gas_price = row
        .get_optional_gas_price("eth_l2_gas_price")?
        .unwrap_or(GasPrice::ZERO);
    let strk_l2_gas_price = row
        .get_optional_gas_price("strk_l2_gas_price")?
        .unwrap_or(GasPrice::ZERO);
    let sequencer_address = row.get_sequencer_address("sequencer_address")?;
    let transaction_commitment = row.get_transaction_commitment("transaction_commitment")?;
    let event_commitment = row.get_event_commitment("event_commitment")?;
    let starknet_version = row.get_starknet_version("version")?;
    let event_count: usize = row.get_usize("event_count")?;
    let transaction_count: usize = row.get_usize("transaction_count")?;
    let state_commitment = row.get_state_commitment("state_commitment")?;
    let l1_da_mode = row.get_l1_da_mode("l1_da_mode")?;
    let receipt_commitment = row.get_receipt_commitment("receipt_commitment")?;
    let state_diff_commitment = row
        .get_optional_felt("state_diff_commitment")?
        .unwrap_or_default();
    let state_diff_length: u64 = row.get_u64("state_diff_length")?;

    let header = BlockHeader {
        hash,
        parent_hash,
        number,
        timestamp,
        eth_l1_gas_price,
        strk_l1_gas_price,
        eth_l1_data_gas_price,
        strk_l1_data_gas_price,
        eth_l2_gas_price,
        strk_l2_gas_price,
        sequencer_address,
        event_commitment,
        state_commitment,
        transaction_commitment,
        starknet_version,
        transaction_count,
        event_count,
        l1_da_mode,
        receipt_commitment,
        state_diff_commitment: StateDiffCommitment(state_diff_commitment),
        state_diff_length,
    };

    Ok(header)
}

#[cfg(test)]
mod tests {
    use pathfinder_common::class_definition::SerializedCairoDefinition;
    use pathfinder_common::macro_prelude::*;
    use pathfinder_common::prelude::*;
    use pathfinder_common::L1DataAvailabilityMode;
    use pretty_assertions_sorted::assert_eq;
    use rstest::rstest;

    use super::*;
    use crate::{Connection, StorageBuilder};

    // Create test database filled with block headers.
    fn setup() -> (Connection, Vec<BlockHeader>) {
        let storage = crate::StorageBuilder::in_memory().unwrap();
        let mut connection = storage.connection().unwrap();
        let tx = connection.transaction().unwrap();

        // This intentionally does not use the builder so that we don't forget to test
        // any new fields that get added.
        //
        // Set unique values so we can be sure we are (de)serializing correctly.
        let storage_commitment = storage_commitment_bytes!(b"storage commitment genesis");
        let class_commitment = class_commitment_bytes!(b"class commitment genesis");

        let genesis = BlockHeader {
            hash: block_hash_bytes!(b"genesis hash"),
            parent_hash: BlockHash::ZERO,
            number: BlockNumber::GENESIS,
            timestamp: BlockTimestamp::new_or_panic(10),
            eth_l1_gas_price: GasPrice(32),
            strk_l1_gas_price: GasPrice(33),
            eth_l1_data_gas_price: GasPrice(34),
            strk_l1_data_gas_price: GasPrice(35),
            eth_l2_gas_price: GasPrice(36),
            strk_l2_gas_price: GasPrice(36),
            sequencer_address: sequencer_address_bytes!(b"sequencer address genesis"),
            starknet_version: StarknetVersion::default(),
            event_commitment: event_commitment_bytes!(b"event commitment genesis"),
            state_commitment: StateCommitment::calculate(
                storage_commitment,
                class_commitment,
                StarknetVersion::default(),
            ),
            transaction_commitment: transaction_commitment_bytes!(b"tx commitment genesis"),
            transaction_count: 37,
            event_count: 40,
            l1_da_mode: L1DataAvailabilityMode::Blob,
            receipt_commitment: receipt_commitment_bytes!(b"receipt commitment genesis"),
            state_diff_commitment: state_diff_commitment!("0x12"),
            state_diff_length: 12,
        };

        let header1 = genesis
            .child_builder()
            .timestamp(BlockTimestamp::new_or_panic(12))
            .eth_l1_gas_price(GasPrice(34))
            .strk_l1_gas_price(GasPrice(35))
            .eth_l2_gas_price(GasPrice(36))
            .strk_l2_gas_price(GasPrice(37))
            .sequencer_address(sequencer_address_bytes!(b"sequencer address 1"))
            .event_commitment(event_commitment_bytes!(b"event commitment 1"))
            .calculated_state_commitment(
                storage_commitment_bytes!(b"storage commitment 1"),
                class_commitment_bytes!(b"class commitment 1"),
            )
            .transaction_commitment(transaction_commitment_bytes!(b"tx commitment 1"))
            .l1_da_mode(L1DataAvailabilityMode::Calldata)
            .receipt_commitment(receipt_commitment_bytes!(b"block 1 receipt commitment"))
            .finalize_with_hash(block_hash_bytes!(b"block 1 hash"));

        let header2 = header1
            .child_builder()
            .eth_l1_gas_price(GasPrice(38))
            .strk_l1_gas_price(GasPrice(39))
            .eth_l2_gas_price(GasPrice(40))
            .strk_l2_gas_price(GasPrice(41))
            .timestamp(BlockTimestamp::new_or_panic(15))
            .sequencer_address(sequencer_address_bytes!(b"sequencer address 2"))
            .event_commitment(event_commitment_bytes!(b"event commitment 2"))
            .calculated_state_commitment(
                storage_commitment_bytes!(b"storage commitment 2"),
                class_commitment_bytes!(b"class commitment 2"),
            )
            .transaction_commitment(transaction_commitment_bytes!(b"tx commitment 2"))
            .l1_da_mode(L1DataAvailabilityMode::Blob)
            .receipt_commitment(receipt_commitment_bytes!(b"block 2 receipt commitment"))
            .finalize_with_hash(block_hash_bytes!(b"block 2 hash"));

        let headers = vec![genesis, header1, header2];
        for header in &headers {
            tx.insert_block_header(header).unwrap();
        }
        tx.commit().unwrap();

        (connection, headers)
    }

    #[test]
    fn get_latest() {
        let (mut connection, headers) = setup();
        let tx = connection.transaction().unwrap();

        let result = tx.block_header(BlockId::Latest).unwrap().unwrap();
        let expected = headers.last().unwrap();

        assert_eq!(&result, expected);
    }

    #[test]
    fn get_by_number() {
        let (mut connection, headers) = setup();
        let tx = connection.transaction().unwrap();

        for header in &headers {
            let result = tx.block_header(header.number.into()).unwrap().unwrap();
            assert_eq!(&result, header);
        }

        let past_head = headers.last().unwrap().number + 1;
        let result = tx.block_header(past_head.into()).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn get_by_hash() {
        let (mut connection, headers) = setup();
        let tx = connection.transaction().unwrap();

        for header in &headers {
            let result = tx.block_header(header.hash.into()).unwrap().unwrap();
            assert_eq!(&result, header);
        }

        let invalid = block_hash_bytes!(b"invalid block hash");
        let result = tx.block_header(invalid.into()).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn purge_block() {
        let (mut connection, headers) = setup();
        let tx = connection.transaction().unwrap();
        let latest = headers.last().unwrap();

        // Add a class to test that purging a block unsets its block number;
        let cairo_hash = class_hash!("0x1234");
        tx.insert_cairo_class_definition(cairo_hash, &SerializedCairoDefinition::from_slice(&[]))
            .unwrap();
        tx.insert_state_update(
            latest.number,
            &StateUpdate::default().with_declared_cairo_class(cairo_hash),
        )
        .unwrap();

        tx.purge_block(latest.number).unwrap();

        let exists = tx.block_exists(latest.number.into()).unwrap();
        assert!(!exists);
    }

    #[test]
    fn block_id() {
        let (mut connection, headers) = setup();
        let tx = connection.transaction().unwrap();

        let target = headers.last().unwrap();
        let expected = Some((target.number, target.hash));

        let by_number = tx.block_id(target.number.into()).unwrap();
        assert_eq!(by_number, expected);

        let by_hash = tx.block_id(target.hash.into()).unwrap();
        assert_eq!(by_hash, expected);
    }

    #[test]
    fn block_is_l1_accepted() {
        let (mut connection, headers) = setup();
        let tx = connection.transaction().unwrap();

        // Mark the genesis header as L1 accepted.
        tx.update_l1_l2_pointer(Some(headers[0].number)).unwrap();

        let l1_by_hash = tx.block_is_l1_accepted(headers[0].hash.into()).unwrap();
        assert!(l1_by_hash);
        let l1_by_number = tx.block_is_l1_accepted(headers[0].number.into()).unwrap();
        assert!(l1_by_number);

        // The second block will therefore be L2 accepted.
        let l2_by_hash = tx.block_is_l1_accepted(headers[1].hash.into()).unwrap();
        assert!(!l2_by_hash);
        let l2_by_number = tx.block_is_l1_accepted(headers[1].number.into()).unwrap();
        assert!(!l2_by_number);
    }

    mod next_ancestor {
        use pretty_assertions_sorted::assert_eq;

        use super::*;

        #[test]
        fn empty_chain_returns_none() {
            let storage = crate::StorageBuilder::in_memory().unwrap();
            let mut db = storage.connection().unwrap();
            let db = db.transaction().unwrap();

            let result = db.next_ancestor(BlockNumber::GENESIS + 10).unwrap();
            assert!(result.is_none());

            let result = db.next_ancestor(BlockNumber::GENESIS).unwrap();
            assert!(result.is_none());
        }

        #[test]
        fn father_exists() {
            let (mut connection, headers) = setup();
            let tx = connection.transaction().unwrap();

            let result = tx.next_ancestor(headers[2].number + 1).unwrap().unwrap();
            let expected = (headers[2].number, headers[2].hash);
            assert_eq!(result, expected);
        }

        #[test]
        fn grandfather_exists() {
            let (mut connection, headers) = setup();
            let tx = connection.transaction().unwrap();

            let result = tx.next_ancestor(headers[2].number + 2).unwrap().unwrap();
            let expected = (headers[2].number, headers[2].hash);
            assert_eq!(result, expected);
        }

        #[test]
        fn gap_in_chain() {
            let storage = crate::StorageBuilder::in_memory().unwrap();
            let mut db = storage.connection().unwrap();
            let db = db.transaction().unwrap();

            let genesis = BlockHeader::default();
            db.insert_block_header(&genesis).unwrap();

            let header_after_gap = genesis
                .child_builder()
                .finalize_with_hash(block_hash_bytes!(b"skipped"))
                .child_builder()
                .finalize_with_hash(block_hash_bytes!(b"expected"));

            db.insert_block_header(&header_after_gap).unwrap();

            let result = db
                .next_ancestor(header_after_gap.number + 1)
                .unwrap()
                .unwrap();
            let expected = (header_after_gap.number, header_after_gap.hash);
            assert_eq!(result, expected);
        }
    }

    mod next_ancestor_without_parent {
        use pretty_assertions_sorted::assert_eq;

        use super::*;

        #[test]
        fn empty_chain_returns_none() {
            let storage = crate::StorageBuilder::in_memory().unwrap();
            let mut db = storage.connection().unwrap();
            let db = db.transaction().unwrap();

            let result = db
                .next_ancestor_without_parent(BlockNumber::GENESIS + 10)
                .unwrap();
            assert!(result.is_none());

            let result = db
                .next_ancestor_without_parent(BlockNumber::GENESIS)
                .unwrap();
            assert!(result.is_none());
        }

        #[test]
        fn target_without_parent_returns_target() {
            let storage = crate::StorageBuilder::in_memory().unwrap();
            let mut db = storage.connection().unwrap();
            let db = db.transaction().unwrap();

            let genesis = BlockHeader::default();

            let header_after_gap = genesis
                .child_builder()
                .finalize_with_hash(block_hash_bytes!(b"skipped"))
                .child_builder()
                .finalize_with_hash(block_hash_bytes!(b"expected"));

            db.insert_block_header(&genesis).unwrap();
            db.insert_block_header(&header_after_gap).unwrap();

            let result = db.next_ancestor_without_parent(genesis.number).unwrap();
            assert_eq!(result, None);

            let expected = (header_after_gap.number, header_after_gap.hash);
            let result = db
                .next_ancestor_without_parent(header_after_gap.number)
                .unwrap()
                .unwrap();
            assert_eq!(result, expected);
        }

        #[test]
        fn missing_target_is_skipped() {
            let (mut connection, headers) = setup();
            let tx = connection.transaction().unwrap();

            let target = headers
                .last()
                .unwrap()
                .child_builder()
                .finalize_with_hash(block_hash_bytes!(b"target"));

            let result = tx.next_ancestor_without_parent(target.number).unwrap();
            assert_eq!(result, None);
        }

        #[test]
        fn complete_chain_returns_none() {
            let (mut connection, _) = setup();
            let tx = connection.transaction().unwrap();

            let result = tx
                .next_ancestor_without_parent(BlockNumber::GENESIS)
                .unwrap();
            assert_eq!(result, None);
        }

        #[test]
        fn incomplete_chain_returns_tail() {
            let (mut connection, headers) = setup();
            let tx = connection.transaction().unwrap();

            let tail = headers
                .last()
                .unwrap()
                .child_builder()
                .finalize_with_hash(block_hash_bytes!(b"skipped"))
                .child_builder()
                .finalize_with_hash(block_hash_bytes!(b"tail"));
            let target = tail
                .child_builder()
                .finalize_with_hash(block_hash_bytes!(b"target"));
            tx.insert_block_header(&tail).unwrap();
            tx.insert_block_header(&target).unwrap();

            let result = tx
                .next_ancestor_without_parent(target.number)
                .unwrap()
                .unwrap();
            let expected = (tail.number, tail.hash);
            assert_eq!(result, expected);
        }
    }

    #[rstest]
    #[case::all_missing("UPDATE block_headers SET event_count = 0", 10)]
    #[case::partially_present("UPDATE block_headers SET event_count = 0 WHERE number > 4", 5)]
    #[case::all_present("", 0)]
    fn event_counts(#[case] sql: &str, #[case] num_of_missing_counts: usize) {
        use crate::fake;

        let faked = fake::generate::n_blocks(10);
        let storage = StorageBuilder::in_memory().unwrap();
        fake::fill(&storage, &faked, None);

        let mut connection = storage.connection().unwrap();
        let tx = connection.transaction().unwrap();
        if !sql.is_empty() {
            tx.inner().execute_batch(sql).unwrap();
        }

        let result = tx
            .event_counts(BlockNumber::GENESIS, NonZeroUsize::new(10).unwrap())
            .unwrap();

        assert_eq!(
            result,
            faked
                .into_iter()
                .take(10 - num_of_missing_counts)
                .map(|block| block.header.header.event_count)
                .chain(std::iter::repeat_n(0, num_of_missing_counts))
                .collect::<Vec<_>>()
        );
    }
}
