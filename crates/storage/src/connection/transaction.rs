//! Contains starknet transaction related code and __not__ database transaction.

use anyhow::Context;
use pathfinder_common::event::Event;
use pathfinder_common::receipt::Receipt;
use pathfinder_common::transaction::Transaction as StarknetTransaction;
use pathfinder_common::{BlockHash, BlockId, BlockNumber, TransactionHash, TransactionIndex};

use super::{EventsForBlock, TransactionDataForBlock, TransactionWithReceipt};
use crate::columns::Column;
use crate::prelude::*;

pub const TRANSACTIONS_AND_RECEIPTS_COLUMN: Column = Column::new("transactions_and_receipts");
pub const EVENTS_COLUMN: Column = Column::new("events");
pub const TRANSACTION_HASHES_COLUMN: Column = Column::new("transaction_hashes");

pub(crate) mod compression {
    use std::sync::LazyLock;

    /// Compression level to use.
    ///
    /// Note that our dictionaries are optimized to be used with level 10.
    const ZSTD_COMPRESSION_LEVEL: i32 = 10;

    /// The maximum allowed uncompressed size of a serialized blob of
    /// transactions.
    const MAX_TRANSACTIONS_UNCOMPRESSED_SIZE: usize = 128usize * 1024 * 1024;
    /// The maximum allowed uncompressed size of a serialized blob of events.
    const MAX_EVENTS_UNCOMPRESSED_SIZE: usize = 128usize * 1024 * 1024;

    static ZSTD_TXS_ENCODER_DICTIONARY: LazyLock<zstd::dict::EncoderDictionary<'static>> =
        LazyLock::new(|| {
            zstd::dict::EncoderDictionary::new(
                include_bytes!("../assets/txs.zdict"),
                ZSTD_COMPRESSION_LEVEL,
            )
        });
    static ZSTD_EVENTS_ENCODER_DICTIONARY: LazyLock<zstd::dict::EncoderDictionary<'static>> =
        LazyLock::new(|| {
            zstd::dict::EncoderDictionary::new(
                include_bytes!("../assets/events.zdict"),
                ZSTD_COMPRESSION_LEVEL,
            )
        });

    static ZSTD_TXS_DECODER_DICTIONARY: LazyLock<zstd::dict::DecoderDictionary<'static>> =
        LazyLock::new(|| zstd::dict::DecoderDictionary::new(include_bytes!("../assets/txs.zdict")));
    static ZSTD_EVENTS_DECODER_DICTIONARY: LazyLock<zstd::dict::DecoderDictionary<'static>> =
        LazyLock::new(|| {
            zstd::dict::DecoderDictionary::new(include_bytes!("../assets/events.zdict"))
        });

    pub(super) fn compress_transactions(input: &[u8]) -> std::io::Result<Vec<u8>> {
        let mut compressor = new_txs_compressor()?;
        compressor.compress(input)
    }

    pub(crate) fn new_txs_compressor() -> std::io::Result<zstd::bulk::Compressor<'static>> {
        zstd::bulk::Compressor::with_prepared_dictionary(&ZSTD_TXS_ENCODER_DICTIONARY)
    }

    pub(super) fn compress_events(input: &[u8]) -> std::io::Result<Vec<u8>> {
        let mut compressor = new_events_compressor()?;
        compressor.compress(input)
    }

    pub(crate) fn new_events_compressor() -> std::io::Result<zstd::bulk::Compressor<'static>> {
        zstd::bulk::Compressor::with_prepared_dictionary(&ZSTD_EVENTS_ENCODER_DICTIONARY)
    }

    pub(crate) fn decompress_transactions(input: &[u8]) -> std::io::Result<Vec<u8>> {
        let mut decompressor = new_txs_decompressor()?;
        decompressor.decompress(input, MAX_TRANSACTIONS_UNCOMPRESSED_SIZE)
    }

    fn new_txs_decompressor() -> std::io::Result<zstd::bulk::Decompressor<'static>> {
        zstd::bulk::Decompressor::with_prepared_dictionary(&ZSTD_TXS_DECODER_DICTIONARY)
    }

    pub(crate) fn decompress_events(input: &[u8]) -> std::io::Result<Vec<u8>> {
        let mut decompressor = new_events_decompressor()?;
        decompressor.decompress(input, MAX_EVENTS_UNCOMPRESSED_SIZE)
    }

    fn new_events_decompressor() -> std::io::Result<zstd::bulk::Decompressor<'static>> {
        zstd::bulk::Decompressor::with_prepared_dictionary(&ZSTD_EVENTS_DECODER_DICTIONARY)
    }
}

type TransactionsAndEventsByBlock = (Vec<(StarknetTransaction, Receipt)>, Vec<Vec<Event>>);
type TransactionAndEventsByHash = (
    BlockNumber,
    StarknetTransaction,
    Receipt,
    Option<Vec<Event>>,
);

impl Transaction<'_> {
    /// Inserts the transaction, receipt and event data.
    pub fn insert_transaction_data(
        &self,
        block_number: BlockNumber,
        transactions: &[(StarknetTransaction, Receipt)],
        events: Option<&[Vec<Event>]>,
    ) -> anyhow::Result<()> {
        if let Some(events) = events {
            self.upsert_block_event_filters(block_number, events.iter().flatten())
                .context("Inserting events into Bloom filter")?;
        }
        if transactions.is_empty() && events.is_none_or(|evts| evts.is_empty()) {
            return Ok(());
        }

        // Encode transactions and receipts.
        let transactions_with_receipts: Vec<_> = transactions
            .iter()
            .map(|(transaction, receipt)| dto::TransactionWithReceiptV4 {
                transaction: dto::TransactionV3::from(transaction),
                receipt: receipt.into(),
            })
            .collect();
        let transactions_with_receipts = dto::TransactionsWithReceiptsForBlock::V4 {
            transactions_with_receipts,
        };
        let transactions_with_receipts =
            bincode::serde::encode_to_vec(transactions_with_receipts, bincode::config::standard())
                .context("Serializing transaction")?;
        let transactions_with_receipts =
            compression::compress_transactions(&transactions_with_receipts)
                .context("Compressing transaction")?;

        // Encode events.
        let encoded_events = events
            .map(|evts| {
                let events = dto::EventsForBlock::V0 {
                    events: evts
                        .iter()
                        .map(|evts| evts.iter().cloned().map(Into::into).collect())
                        .collect(),
                };
                let events = bincode::serde::encode_to_vec(events, bincode::config::standard())
                    .context("Serializing events")?;
                compression::compress_events(&events).context("Compressing events")
            })
            .transpose()?;

        let mut batch = self.batch.lock().expect("Batch lock poisoned");
        let block_number_key = block_number.get().to_be_bytes();

        let transactions_and_receipts_column =
            self.rocksdb_get_column(&TRANSACTIONS_AND_RECEIPTS_COLUMN);
        batch.put_cf(
            &transactions_and_receipts_column,
            &block_number_key,
            &transactions_with_receipts,
        );

        let events_column = self.rocksdb_get_column(&EVENTS_COLUMN);
        if let Some(encoded_events) = encoded_events {
            batch.put_cf(&events_column, &block_number_key, &encoded_events);
        }

        let transaction_hashes_column = self.rocksdb_get_column(&TRANSACTION_HASHES_COLUMN);
        for (idx, (transaction, ..)) in transactions.iter().enumerate() {
            let idx: u16 = idx.try_into()?;
            let mut buffer = [0u8; 10];
            buffer[..8].copy_from_slice(&block_number_key);
            buffer[8..].copy_from_slice(&idx.to_be_bytes());

            batch.put_cf(
                &transaction_hashes_column,
                transaction.hash.0.as_be_bytes(),
                &buffer,
            );
        }

        Ok(())
    }

    pub fn update_events(
        &self,
        block_number: BlockNumber,
        events: Vec<Vec<Event>>,
    ) -> anyhow::Result<()> {
        let mut stmt = self
            .inner()
            .prepare_cached(
                r"
                UPDATE transactions
                SET events = :events
                WHERE block_number = :block_number
                ",
            )
            .context("Preparing update events statement")?;

        let encoded_events = dto::EventsForBlock::V0 {
            events: events
                .iter()
                .cloned()
                .map(|events| events.into_iter().map(Into::into).collect())
                .collect(),
        };
        let encoded_events =
            bincode::serde::encode_to_vec(encoded_events, bincode::config::standard())
                .context("Serializing events")?;
        let encoded_events =
            compression::compress_events(&encoded_events).context("Compressing events")?;

        stmt.execute(named_params![
            ":block_number": &block_number,
            ":events": &encoded_events,
        ])
        .context("Updating events")?;

        let events = events.iter().flatten();
        self.upsert_block_event_filters(block_number, events)
            .context("Inserting events into Bloom filter")?;

        Ok(())
    }

    pub fn transaction(
        &self,
        transaction: TransactionHash,
    ) -> anyhow::Result<Option<StarknetTransaction>> {
        let Some((_, transaction, _)) = self.query_transaction_by_hash(transaction)? else {
            return Ok(None);
        };
        Ok(Some(transaction))
    }

    pub fn transaction_with_receipt(
        &self,
        txn_hash: TransactionHash,
    ) -> anyhow::Result<Option<TransactionWithReceipt>> {
        let Some((block_number, transaction, receipt, events)) =
            self.query_transaction_and_events_by_hash(txn_hash)?
        else {
            return Ok(None);
        };
        let events = events.context("Events missing")?;
        Ok(Some((transaction, receipt, events, block_number)))
    }

    pub fn transaction_at_block(
        &self,
        block: BlockId,
        index: usize,
    ) -> anyhow::Result<Option<StarknetTransaction>> {
        let Some(block_number) = self.block_number(block)? else {
            return Ok(None);
        };
        Ok(self
            .query_transactions_by_block(block_number)?
            .get(index)
            .map(|(transaction, ..)| transaction.clone()))
    }

    pub fn transaction_count(&self, block: BlockId) -> anyhow::Result<usize> {
        let Some(block_number) = self.block_number(block)? else {
            return Ok(0);
        };
        Ok(self.query_transactions_by_block(block_number)?.len())
    }

    pub fn transaction_data_for_block(
        &self,
        block: BlockId,
    ) -> anyhow::Result<Option<Vec<TransactionDataForBlock>>> {
        let Some(block_number) = self.block_number(block)? else {
            return Ok(None);
        };

        let (transactions, events) = self.query_transactions_and_events_by_block(block_number)?;

        Ok(Some(
            transactions
                .into_iter()
                .zip(events)
                .map(|((transaction, receipt), events)| (transaction, receipt, events))
                .collect(),
        ))
    }

    pub fn transactions_for_block(
        &self,
        block: BlockId,
    ) -> anyhow::Result<Option<Vec<StarknetTransaction>>> {
        let Some(block_number) = self.block_number(block)? else {
            return Ok(None);
        };

        Ok(Some(
            self.query_transactions_by_block(block_number)?
                .into_iter()
                .map(|(transaction, ..)| transaction)
                .collect(),
        ))
    }

    pub fn transactions_with_receipts_for_block(
        &self,
        block: BlockId,
    ) -> anyhow::Result<Option<Vec<(StarknetTransaction, Receipt)>>> {
        let Some(block_number) = self.block_number(block)? else {
            return Ok(None);
        };

        Ok(Some(
            self.query_transactions_by_block(block_number)?
                .into_iter()
                .map(|(t, r, ..)| (t, r))
                .collect(),
        ))
    }

    pub fn events_for_block(&self, block: BlockId) -> anyhow::Result<Option<Vec<EventsForBlock>>> {
        let Some(block_number) = self.block_number(block)? else {
            return Ok(None);
        };

        // We explicitly do _not_ stop here on no events: empty blocks are valid and
        // fairly common since Starknet 0.14.0.
        let events = self
            .query_events_by_block(block_number)?
            .unwrap_or_default();

        let transaction_infos = self
            .query_transaction_infos_by_block(block_number)
            .context("Querying transaction infos")?;

        if events.len() != transaction_infos.len() {
            anyhow::bail!("Event list and transaction list mismatch");
        }

        Ok(Some(
            events
                .into_iter()
                .zip(transaction_infos)
                .map(|(events, transaction_info)| (transaction_info, events))
                .collect(),
        ))
    }

    pub fn transaction_hashes_for_block(
        &self,
        block: BlockId,
    ) -> anyhow::Result<Option<Vec<TransactionHash>>> {
        let Some(block_number) = self.block_number(block)? else {
            return Ok(None);
        };

        let infos = self.query_transaction_infos_by_block(block_number)?;
        let transactions = infos.into_iter().map(|tx_info| tx_info.0).collect();
        Ok(Some(transactions))
    }

    pub fn transaction_block_hash(
        &self,
        hash: TransactionHash,
    ) -> anyhow::Result<Option<BlockHash>> {
        self.inner()
            .query_row(
                r"
                SELECT block_headers.hash FROM transaction_hashes
                JOIN block_headers ON transaction_hashes.block_number = block_headers.number
                WHERE transaction_hashes.hash = ?
                ",
                params![&hash],
                |row| row.get_block_hash(0),
            )
            .optional()
            .map_err(|e| e.into())
    }

    pub fn delete_transactions_before(&self, block_number: BlockNumber) -> anyhow::Result<()> {
        let mut stmt = self.inner().prepare_cached(
            r"
            DELETE FROM transactions
            WHERE block_number < ?
            ",
        )?;
        stmt.execute(params![&block_number])
            .context("Deleting old transactions")?;

        Ok(())
    }

    pub fn delete_transaction_hashes_before(
        &self,
        block_number: BlockNumber,
    ) -> anyhow::Result<()> {
        let mut stmt = self.inner().prepare_cached(
            r"
            DELETE FROM transaction_hashes
            WHERE block_number < ?
            ",
        )?;
        stmt.execute(params![&block_number])
            .context("Deleting old transaction hashes")?;

        Ok(())
    }

    fn query_transactions_by_block(
        &self,
        block_number: BlockNumber,
    ) -> anyhow::Result<Vec<(StarknetTransaction, Receipt)>> {
        let mut stmt = self.inner().prepare_cached(
            r"
            SELECT transactions
            FROM transactions
            WHERE block_number = ?
            ",
        )?;
        let mut rows = stmt.query(params![&block_number])?;
        let Some(row) = rows.next()? else {
            return Ok(vec![]);
        };
        let transactions = row.get_blob(0)?;
        let transactions = compression::decompress_transactions(transactions)
            .context("Decompressing transactions")?;
        let transactions: dto::TransactionsWithReceiptsForBlock =
            bincode::serde::decode_from_slice(&transactions, bincode::config::standard())
                .context("Deserializing transactions")?
                .0;
        let transactions = transactions.transactions_with_receipts();

        Ok(transactions
            .into_iter()
            .map(
                |dto::TransactionWithReceiptV4 {
                     transaction,
                     receipt,
                 }| { (transaction.into(), receipt.into()) },
            )
            .collect())
    }

    fn query_transaction_infos_by_block(
        &self,
        block_number: BlockNumber,
    ) -> anyhow::Result<Vec<(TransactionHash, TransactionIndex)>> {
        let mut stmt = self.inner().prepare_cached(
            r"
            SELECT hash, idx
            FROM transaction_hashes
            WHERE block_number = ? ORDER BY idx
            ",
        )?;
        let transaction_hashes: Result<Vec<_>, _> = stmt
            .query_map(params![&block_number], |row| {
                let hash = row.get_transaction_hash(0)?;
                let raw_idx = row.get_i64(1)?;
                Ok((hash, TransactionIndex::new_or_panic(raw_idx as u64)))
            })
            .context("Querying transaction hashes for block")?
            .collect();
        let transaction_infos = transaction_hashes?;

        Ok(transaction_infos)
    }

    fn query_transactions_and_events_by_block(
        &self,
        block_number: BlockNumber,
    ) -> anyhow::Result<TransactionsAndEventsByBlock> {
        let mut stmt = self.inner().prepare_cached(
            r"
            SELECT transactions, events
            FROM transactions
            WHERE block_number = ?
            ",
        )?;
        let mut rows = stmt.query(params![&block_number])?;
        let Some(row) = rows.next()? else {
            return Ok((vec![], vec![]));
        };
        let transactions = row.get_blob(0)?;
        let transactions = compression::decompress_transactions(transactions)
            .context("Decompressing transactions")?;
        let transactions: dto::TransactionsWithReceiptsForBlock =
            bincode::serde::decode_from_slice(&transactions, bincode::config::standard())
                .context("Deserializing transactions")?
                .0;
        let transactions = transactions.transactions_with_receipts();
        let events: Option<dto::EventsForBlock> = match row.get_optional_blob(1)? {
            Some(events) => {
                let events =
                    compression::decompress_events(events).context("Decompressing events")?;
                Some(
                    bincode::serde::decode_from_slice(&events, bincode::config::standard())
                        .context("Deserializing events")?
                        .0,
                )
            }
            None => None,
        };
        let events = events.map(|events| match events {
            dto::EventsForBlock::V0 { events } => events,
        });
        Ok((
            transactions
                .into_iter()
                .map(
                    |dto::TransactionWithReceiptV4 {
                         transaction,
                         receipt,
                     }| { (transaction.into(), receipt.into()) },
                )
                .collect(),
            events
                .map(|events| {
                    events
                        .into_iter()
                        .map(|e| e.into_iter().map(Into::into).collect())
                        .collect()
                })
                .unwrap_or_default(),
        ))
    }

    fn query_events_by_block(
        &self,
        block_number: BlockNumber,
    ) -> anyhow::Result<Option<Vec<Vec<Event>>>> {
        let mut stmt = self.inner().prepare_cached(
            r"
            SELECT events
            FROM transactions
            WHERE block_number = ?
            ",
        )?;
        let mut rows = stmt.query(params![&block_number])?;
        let Some(row) = rows.next()? else {
            return Ok(None);
        };
        let events: Option<dto::EventsForBlock> = match row.get_optional_blob(0)? {
            Some(events) => {
                let events =
                    compression::decompress_events(events).context("Decompressing events")?;
                let events: dto::EventsForBlock =
                    bincode::serde::decode_from_slice(&events, bincode::config::standard())
                        .context("Deserializing events")?
                        .0;

                Some(events)
            }
            None => None,
        };
        let events = events.map(|e| e.events());

        Ok(events.map(|events| {
            events
                .into_iter()
                .map(|e| e.into_iter().map(Into::into).collect())
                .collect()
        }))
    }

    fn query_transaction_by_hash(
        &self,
        hash: TransactionHash,
    ) -> anyhow::Result<Option<(BlockNumber, StarknetTransaction, Receipt)>> {
        let mut stmt = self.inner().prepare_cached(
            r"
            SELECT transactions.block_number, transactions, idx
            FROM transactions
            JOIN transaction_hashes ON transactions.block_number = transaction_hashes.block_number
            WHERE hash = ?
            ",
        )?;
        let mut rows = stmt.query(params![&hash])?;
        let Some(row) = rows.next()? else {
            return Ok(None);
        };
        let block_number = row.get_block_number(0)?;
        let transactions = row.get_blob(1)?;
        let idx: usize = row.get_i64(2)?.try_into()?;

        let transactions = compression::decompress_transactions(transactions)
            .context("Decompressing transactions")?;
        let transactions: dto::TransactionsWithReceiptsForBlock =
            bincode::serde::decode_from_slice(&transactions, bincode::config::standard())
                .context("Deserializing transactions")?
                .0;
        let transactions = transactions.transactions_with_receipts();
        let dto::TransactionWithReceiptV4 {
            transaction,
            receipt,
        } = transactions.get(idx).context("Transaction not found")?;

        Ok(Some((
            block_number,
            transaction.clone().into(),
            receipt.clone().into(),
        )))
    }

    fn query_transaction_and_events_by_hash(
        &self,
        hash: TransactionHash,
    ) -> anyhow::Result<Option<TransactionAndEventsByHash>> {
        let mut stmt = self.inner().prepare_cached(
            r"
            SELECT transactions.block_number, transactions, events, idx
            FROM transactions
            JOIN transaction_hashes ON transactions.block_number = transaction_hashes.block_number
            WHERE hash = ?
            ",
        )?;
        let mut rows = stmt.query(params![&hash])?;
        let Some(row) = rows.next()? else {
            return Ok(None);
        };
        let block_number = row.get_block_number(0)?;
        let idx: usize = row.get_i64(3)?.try_into()?;
        let transactions = row.get_blob(1)?;

        let transactions = compression::decompress_transactions(transactions)
            .context("Decompressing transactions")?;
        let transactions: dto::TransactionsWithReceiptsForBlock =
            bincode::serde::decode_from_slice(&transactions, bincode::config::standard())
                .context("Deserializing transactions")?
                .0;
        let transactions = transactions.transactions_with_receipts();

        let events: Option<Vec<Vec<dto::Event>>> = match row.get_optional_blob(2)? {
            Some(events) => {
                let events =
                    compression::decompress_events(events).context("Decompressing events")?;
                let events: dto::EventsForBlock =
                    bincode::serde::decode_from_slice(&events, bincode::config::standard())
                        .context("Deserializing events")?
                        .0;
                Some(events.events())
            }
            None => None,
        };
        let dto::TransactionWithReceiptV4 {
            transaction,
            receipt,
        } = transactions.get(idx).context("Transaction not found")?;
        let events = match events {
            Some(events) => {
                let events = events.get(idx).context("Events missing")?;
                Some(events.iter().cloned().map(Into::into).collect())
            }
            None => None,
        };
        Ok(Some((
            block_number,
            transaction.clone().into(),
            receipt.clone().into(),
            events,
        )))
    }
}

pub(crate) mod dto {
    use fake::{Dummy, Fake, Faker};
    use pathfinder_common::*;
    use pathfinder_crypto::Felt;
    use serde::{Deserialize, Serialize};

    use crate::connection::dto::MinimalFelt;

    /// Represents deserialized L2 transaction entry point values.
    #[derive(Copy, Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Dummy)]
    #[serde(deny_unknown_fields)]
    pub enum EntryPointType {
        External,
        L1Handler,
    }

    impl From<pathfinder_common::transaction::EntryPointType> for EntryPointType {
        fn from(value: pathfinder_common::transaction::EntryPointType) -> Self {
            use pathfinder_common::transaction::EntryPointType::{External, L1Handler};
            match value {
                External => Self::External,
                L1Handler => Self::L1Handler,
            }
        }
    }

    impl From<EntryPointType> for pathfinder_common::transaction::EntryPointType {
        fn from(value: EntryPointType) -> Self {
            match value {
                EntryPointType::External => Self::External,
                EntryPointType::L1Handler => Self::L1Handler,
            }
        }
    }

    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    pub enum EventsForBlock {
        V0 { events: Vec<Vec<Event>> },
    }

    impl EventsForBlock {
        pub fn events(self) -> Vec<Vec<Event>> {
            match self {
                EventsForBlock::V0 { events } => events,
            }
        }
    }

    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    pub struct Event {
        pub data: Vec<MinimalFelt>,
        pub from_address: MinimalFelt,
        pub keys: Vec<MinimalFelt>,
    }

    impl From<pathfinder_common::event::Event> for Event {
        fn from(value: pathfinder_common::event::Event) -> Self {
            let pathfinder_common::event::Event {
                data,
                from_address,
                keys,
            } = value;
            Self {
                data: data
                    .into_iter()
                    .map(|x| x.as_inner().to_owned().into())
                    .collect(),
                from_address: from_address.as_inner().to_owned().into(),
                keys: keys
                    .into_iter()
                    .map(|x| x.as_inner().to_owned().into())
                    .collect(),
            }
        }
    }

    impl From<Event> for pathfinder_common::event::Event {
        fn from(value: Event) -> Self {
            Self {
                data: value
                    .data
                    .into_iter()
                    .map(|x| EventData(x.into()))
                    .collect(),
                from_address: ContractAddress::new_or_panic(value.from_address.into()),
                keys: value.keys.into_iter().map(|x| EventKey(x.into())).collect(),
            }
        }
    }

    /// Represents execution resources for L2 transaction.
    #[derive(Copy, Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    pub struct ExecutionResourcesV0 {
        pub builtins: BuiltinCountersV0,
        pub n_steps: u64,
        pub n_memory_holes: u64,
        pub data_availability: L1Gas,
    }

    #[derive(Copy, Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    pub struct ExecutionResourcesV1 {
        pub builtins: BuiltinCountersV1,
        pub n_steps: u64,
        pub n_memory_holes: u64,
        pub data_availability: L1Gas,
        pub total_gas_consumed: L1Gas,
    }

    /// Represents execution resources for L2 transaction.
    #[derive(Copy, Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    pub struct ExecutionResourcesV2 {
        pub builtins: BuiltinCountersV1,
        pub n_steps: u64,
        pub n_memory_holes: u64,
        pub data_availability: L1Gas,
        pub total_gas_consumed: L1Gas,
        pub l2_gas_consumed: L2Gas,
    }

    #[derive(Copy, Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    pub struct L1Gas {
        // TODO make these mandatory once some new release makes resyncing necessary
        pub l1_gas: Option<u128>,
        pub l1_data_gas: Option<u128>,
    }

    #[derive(Copy, Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    pub struct L2Gas {
        pub l2_gas: u128,
    }

    impl From<ExecutionResourcesV0> for ExecutionResourcesV2 {
        fn from(value: ExecutionResourcesV0) -> Self {
            Self {
                builtins: value.builtins.into(),
                n_steps: value.n_steps,
                n_memory_holes: value.n_memory_holes,
                data_availability: value.data_availability,
                total_gas_consumed: Default::default(),
                l2_gas_consumed: Default::default(),
            }
        }
    }

    impl From<ExecutionResourcesV1> for ExecutionResourcesV2 {
        fn from(value: ExecutionResourcesV1) -> Self {
            Self {
                builtins: value.builtins,
                n_steps: value.n_steps,
                n_memory_holes: value.n_memory_holes,
                data_availability: value.data_availability,
                total_gas_consumed: Default::default(),
                l2_gas_consumed: Default::default(),
            }
        }
    }

    impl From<ExecutionResourcesV2> for pathfinder_common::receipt::ExecutionResources {
        fn from(value: ExecutionResourcesV2) -> Self {
            Self {
                builtins: value.builtins.into(),
                n_steps: value.n_steps,
                n_memory_holes: value.n_memory_holes,
                data_availability: match (
                    value.data_availability.l1_gas,
                    value.data_availability.l1_data_gas,
                ) {
                    (Some(l1_gas), Some(l1_data_gas)) => pathfinder_common::receipt::L1Gas {
                        l1_gas,
                        l1_data_gas,
                    },
                    _ => Default::default(),
                },
                total_gas_consumed: match (
                    value.total_gas_consumed.l1_gas,
                    value.total_gas_consumed.l1_data_gas,
                ) {
                    (Some(l1_gas), Some(l1_data_gas)) => pathfinder_common::receipt::L1Gas {
                        l1_gas,
                        l1_data_gas,
                    },
                    _ => Default::default(),
                },
                l2_gas: pathfinder_common::receipt::L2Gas(value.l2_gas_consumed.l2_gas),
            }
        }
    }

    impl From<&pathfinder_common::receipt::ExecutionResources> for ExecutionResourcesV2 {
        fn from(value: &pathfinder_common::receipt::ExecutionResources) -> Self {
            Self {
                builtins: (&value.builtins).into(),
                n_steps: value.n_steps,
                n_memory_holes: value.n_memory_holes,
                data_availability: L1Gas {
                    l1_gas: Some(value.data_availability.l1_gas),
                    l1_data_gas: Some(value.data_availability.l1_data_gas),
                },
                total_gas_consumed: L1Gas {
                    l1_gas: Some(value.total_gas_consumed.l1_gas),
                    l1_data_gas: Some(value.total_gas_consumed.l1_data_gas),
                },
                l2_gas_consumed: L2Gas {
                    l2_gas: value.l2_gas.0,
                },
            }
        }
    }

    impl<T> Dummy<T> for ExecutionResourcesV0 {
        fn dummy_with_rng<R: rand::Rng + ?Sized>(_: &T, rng: &mut R) -> Self {
            let (l1_gas, l1_data_gas) = if rng.gen() {
                (Some(rng.next_u32() as u128), Some(rng.next_u32() as u128))
            } else {
                (None, None)
            };

            Self {
                builtins: Faker.fake_with_rng(rng),
                n_steps: rng.next_u32() as u64,
                n_memory_holes: rng.next_u32() as u64,
                data_availability: L1Gas {
                    l1_gas,
                    l1_data_gas,
                },
            }
        }
    }

    impl<T> Dummy<T> for ExecutionResourcesV1 {
        fn dummy_with_rng<R: rand::Rng + ?Sized>(_: &T, rng: &mut R) -> Self {
            let (l1_gas, l1_data_gas) = if rng.gen() {
                (Some(rng.next_u32() as u128), Some(rng.next_u32() as u128))
            } else {
                (None, None)
            };

            Self {
                builtins: Faker.fake_with_rng(rng),
                n_steps: rng.next_u32() as u64,
                n_memory_holes: rng.next_u32() as u64,
                data_availability: L1Gas {
                    l1_gas,
                    l1_data_gas,
                },
                total_gas_consumed: L1Gas {
                    l1_gas: l1_gas.map(|x| x + rng.next_u32() as u128),
                    l1_data_gas: Some(0), // Data point no longer present in p2p spec
                },
            }
        }
    }

    impl<T> Dummy<T> for ExecutionResourcesV2 {
        fn dummy_with_rng<R: rand::Rng + ?Sized>(_: &T, rng: &mut R) -> Self {
            let (l1_gas, l1_data_gas) = if rng.gen() {
                (Some(rng.next_u32() as u128), Some(rng.next_u32() as u128))
            } else {
                (None, None)
            };

            Self {
                builtins: Faker.fake_with_rng(rng),
                n_steps: rng.next_u32() as u64,
                n_memory_holes: rng.next_u32() as u64,
                data_availability: L1Gas {
                    l1_gas,
                    l1_data_gas,
                },
                total_gas_consumed: L1Gas {
                    l1_gas: l1_gas.map(|x| x + rng.next_u32() as u128),
                    l1_data_gas: Some(0), // Data point no longer present in p2p spec
                },
                l2_gas_consumed: L2Gas {
                    l2_gas: rng.next_u32() as u128,
                },
            }
        }
    }

    #[derive(Copy, Clone, Default, Debug, Deserialize, Serialize, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    pub struct BuiltinCountersV0 {
        pub output: u64,
        pub pedersen: u64,
        pub range_check: u64,
        pub ecdsa: u64,
        pub bitwise: u64,
        pub ec_op: u64,
        pub keccak: u64,
        pub poseidon: u64,
        pub segment_arena: u64,
    }

    #[derive(Copy, Clone, Default, Debug, Deserialize, Serialize, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    pub struct BuiltinCountersV1 {
        pub output: u64,
        pub pedersen: u64,
        pub range_check: u64,
        pub ecdsa: u64,
        pub bitwise: u64,
        pub ec_op: u64,
        pub keccak: u64,
        pub poseidon: u64,
        pub segment_arena: u64,
        pub add_mod: u64,
        pub mul_mod: u64,
        pub range_check96: u64,
    }

    impl From<BuiltinCountersV0> for BuiltinCountersV1 {
        fn from(value: BuiltinCountersV0) -> Self {
            // Use deconstruction to ensure these structs remain in-sync.
            let BuiltinCountersV0 {
                output,
                pedersen,
                range_check,
                ecdsa,
                bitwise,
                ec_op,
                keccak,
                poseidon,
                segment_arena,
            } = value;
            Self {
                output,
                pedersen,
                range_check,
                ecdsa,
                bitwise,
                ec_op,
                keccak,
                poseidon,
                segment_arena,
                ..Default::default()
            }
        }
    }

    impl From<BuiltinCountersV1> for pathfinder_common::receipt::BuiltinCounters {
        fn from(value: BuiltinCountersV1) -> Self {
            // Use deconstruction to ensure these structs remain in-sync.
            let BuiltinCountersV1 {
                output,
                pedersen,
                range_check,
                ecdsa,
                bitwise,
                ec_op,
                keccak,
                poseidon,
                segment_arena,
                add_mod,
                mul_mod,
                range_check96,
            } = value;
            Self {
                output,
                pedersen,
                range_check,
                ecdsa,
                bitwise,
                ec_op,
                keccak,
                poseidon,
                segment_arena,
                add_mod,
                mul_mod,
                range_check96,
            }
        }
    }

    impl From<&pathfinder_common::receipt::BuiltinCounters> for BuiltinCountersV1 {
        fn from(value: &pathfinder_common::receipt::BuiltinCounters) -> Self {
            // Use deconstruction to ensure these structs remain in-sync.
            let pathfinder_common::receipt::BuiltinCounters {
                output,
                pedersen,
                range_check,
                ecdsa,
                bitwise,
                ec_op,
                keccak,
                poseidon,
                segment_arena,
                add_mod,
                mul_mod,
                range_check96,
            } = value.clone();
            Self {
                output,
                pedersen,
                range_check,
                ecdsa,
                bitwise,
                ec_op,
                keccak,
                poseidon,
                segment_arena,
                add_mod,
                mul_mod,
                range_check96,
            }
        }
    }

    impl<T> Dummy<T> for BuiltinCountersV0 {
        fn dummy_with_rng<R: rand::Rng + ?Sized>(_: &T, rng: &mut R) -> Self {
            Self {
                output: rng.next_u32() as u64,
                pedersen: rng.next_u32() as u64,
                range_check: rng.next_u32() as u64,
                ecdsa: rng.next_u32() as u64,
                bitwise: rng.next_u32() as u64,
                ec_op: rng.next_u32() as u64,
                keccak: rng.next_u32() as u64,
                poseidon: rng.next_u32() as u64,
                segment_arena: 0, // Not used in p2p
            }
        }
    }

    impl<T> Dummy<T> for BuiltinCountersV1 {
        fn dummy_with_rng<R: rand::Rng + ?Sized>(_: &T, rng: &mut R) -> Self {
            Self {
                output: rng.next_u32() as u64,
                pedersen: rng.next_u32() as u64,
                range_check: rng.next_u32() as u64,
                ecdsa: rng.next_u32() as u64,
                bitwise: rng.next_u32() as u64,
                ec_op: rng.next_u32() as u64,
                keccak: rng.next_u32() as u64,
                poseidon: rng.next_u32() as u64,
                segment_arena: 0, // Not used in p2p
                add_mod: rng.next_u32() as u64,
                mul_mod: rng.next_u32() as u64,
                range_check96: rng.next_u32() as u64,
            }
        }
    }

    /// Represents deserialized L2 to L1 message.
    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Dummy)]
    #[serde(deny_unknown_fields)]
    pub struct L2ToL1MessageV0 {
        pub from_address: MinimalFelt,
        pub payload: Vec<MinimalFelt>,
        pub to_address: EthereumAddress,
    }

    /// Represents deserialized L2 to L1 message.
    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    pub struct L2ToL1MessageV1 {
        pub from_address: MinimalFelt,
        pub payload: Vec<MinimalFelt>,
        pub to_address: MinimalFelt,
    }

    impl<T> Dummy<T> for L2ToL1MessageV1 {
        fn dummy_with_rng<R: rand::Rng + ?Sized>(_: &T, rng: &mut R) -> Self {
            Self {
                from_address: Faker.fake_with_rng(rng),
                payload: fake::vec![MinimalFelt; 1..10],
                // Create a Felt using only 160 bits. This is required because p2p specification
                // uses the wrong type to represent this field.
                to_address: MinimalFelt::from(Felt::from_be_slice(&fake::vec![u8; 20]).unwrap()),
            }
        }
    }

    impl From<L2ToL1MessageV0> for L2ToL1MessageV1 {
        fn from(value: L2ToL1MessageV0) -> Self {
            Self {
                from_address: value.from_address,
                payload: value.payload,
                to_address: Felt::from_be_slice(value.to_address.0.as_bytes())
                    .expect("H160 will always fit into a Felt")
                    .into(),
            }
        }
    }

    impl From<L2ToL1MessageV1> for pathfinder_common::receipt::L2ToL1Message {
        fn from(value: L2ToL1MessageV1) -> Self {
            let L2ToL1MessageV1 {
                from_address,
                payload,
                to_address,
            } = value;
            pathfinder_common::receipt::L2ToL1Message {
                from_address: ContractAddress::new_or_panic(from_address.into()),
                payload: payload
                    .into_iter()
                    .map(|x| L2ToL1MessagePayloadElem(x.into()))
                    .collect(),
                to_address: ContractAddress::new_or_panic(to_address.into()),
            }
        }
    }

    impl From<&pathfinder_common::receipt::L2ToL1Message> for L2ToL1MessageV1 {
        fn from(value: &pathfinder_common::receipt::L2ToL1Message) -> Self {
            let pathfinder_common::receipt::L2ToL1Message {
                from_address,
                payload,
                to_address,
            } = value.clone();
            Self {
                from_address: from_address.as_inner().to_owned().into(),
                payload: payload
                    .into_iter()
                    .map(|x| x.as_inner().to_owned().into())
                    .collect(),
                to_address: to_address.as_inner().to_owned().into(),
            }
        }
    }

    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Dummy)]
    pub enum ExecutionStatus {
        Succeeded,
        Reverted { reason: String },
    }

    /// Represents deserialized L2 transaction receipt data.
    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Dummy)]
    #[serde(deny_unknown_fields)]
    pub struct ReceiptV0 {
        pub actual_fee: MinimalFelt,
        pub execution_resources: Option<ExecutionResourcesV0>,
        pub l2_to_l1_messages: Vec<L2ToL1MessageV0>,
        pub transaction_hash: MinimalFelt,
        pub transaction_index: TransactionIndex,
        pub execution_status: ExecutionStatus,
    }

    /// Represents deserialized L2 transaction receipt data.
    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Dummy)]
    #[serde(deny_unknown_fields)]
    pub struct ReceiptV1 {
        pub actual_fee: MinimalFelt,
        pub execution_resources: Option<ExecutionResourcesV0>,
        pub l2_to_l1_messages: Vec<L2ToL1MessageV1>,
        pub transaction_hash: MinimalFelt,
        pub transaction_index: TransactionIndex,
        pub execution_status: ExecutionStatus,
    }

    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Dummy)]
    #[serde(deny_unknown_fields)]
    pub struct ReceiptV2 {
        pub actual_fee: MinimalFelt,
        pub execution_resources: Option<ExecutionResourcesV1>,
        pub l2_to_l1_messages: Vec<L2ToL1MessageV1>,
        pub transaction_hash: MinimalFelt,
        pub transaction_index: TransactionIndex,
        pub execution_status: ExecutionStatus,
    }

    /// Represents deserialized L2 transaction receipt data.
    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Dummy)]
    #[serde(deny_unknown_fields)]
    pub struct ReceiptV3 {
        pub actual_fee: MinimalFelt,
        pub execution_resources: Option<ExecutionResourcesV2>,
        pub l2_to_l1_messages: Vec<L2ToL1MessageV1>,
        pub transaction_hash: MinimalFelt,
        pub transaction_index: TransactionIndex,
        pub execution_status: ExecutionStatus,
    }

    impl From<ReceiptV0> for ReceiptV3 {
        fn from(value: ReceiptV0) -> Self {
            Self {
                actual_fee: value.actual_fee,
                execution_resources: value.execution_resources.map(Into::into),
                l2_to_l1_messages: value
                    .l2_to_l1_messages
                    .into_iter()
                    .map(Into::into)
                    .collect(),
                transaction_hash: value.transaction_hash,
                transaction_index: value.transaction_index,
                execution_status: value.execution_status,
            }
        }
    }

    impl From<ReceiptV1> for ReceiptV3 {
        fn from(value: ReceiptV1) -> Self {
            Self {
                actual_fee: value.actual_fee,
                execution_resources: value.execution_resources.map(Into::into),
                l2_to_l1_messages: value.l2_to_l1_messages.into_iter().collect(),
                transaction_hash: value.transaction_hash,
                transaction_index: value.transaction_index,
                execution_status: value.execution_status,
            }
        }
    }

    impl From<ReceiptV2> for ReceiptV3 {
        fn from(value: ReceiptV2) -> Self {
            Self {
                actual_fee: value.actual_fee,
                execution_resources: value.execution_resources.map(Into::into),
                l2_to_l1_messages: value.l2_to_l1_messages,
                transaction_hash: value.transaction_hash,
                transaction_index: value.transaction_index,
                execution_status: value.execution_status,
            }
        }
    }

    impl From<ReceiptV3> for pathfinder_common::receipt::Receipt {
        fn from(value: ReceiptV3) -> Self {
            use pathfinder_common::receipt as common;

            let ReceiptV3 {
                actual_fee,
                execution_resources,
                // This information is redundant as it is already in the transaction itself.
                l2_to_l1_messages,
                transaction_hash,
                transaction_index,
                execution_status,
            } = value;

            common::Receipt {
                actual_fee: Fee(actual_fee.into()),
                execution_resources: execution_resources.unwrap_or_default().into(),
                l2_to_l1_messages: l2_to_l1_messages.into_iter().map(Into::into).collect(),
                transaction_hash: TransactionHash(transaction_hash.into()),
                transaction_index,
                execution_status: match execution_status {
                    ExecutionStatus::Succeeded => common::ExecutionStatus::Succeeded,
                    ExecutionStatus::Reverted { reason } => {
                        common::ExecutionStatus::Reverted { reason }
                    }
                },
            }
        }
    }

    impl From<&pathfinder_common::receipt::Receipt> for ReceiptV3 {
        fn from(value: &pathfinder_common::receipt::Receipt) -> Self {
            Self {
                actual_fee: value.actual_fee.as_inner().to_owned().into(),
                execution_resources: Some((&value.execution_resources).into()),
                l2_to_l1_messages: value.l2_to_l1_messages.iter().map(Into::into).collect(),
                transaction_hash: value.transaction_hash.as_inner().to_owned().into(),
                transaction_index: value.transaction_index,
                execution_status: match &value.execution_status {
                    receipt::ExecutionStatus::Succeeded => ExecutionStatus::Succeeded,
                    receipt::ExecutionStatus::Reverted { reason } => ExecutionStatus::Reverted {
                        reason: reason.clone(),
                    },
                },
            }
        }
    }

    #[derive(Copy, Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Dummy)]
    pub enum DataAvailabilityMode {
        L1,
        L2,
    }

    impl From<DataAvailabilityMode> for pathfinder_common::transaction::DataAvailabilityMode {
        fn from(value: DataAvailabilityMode) -> Self {
            match value {
                DataAvailabilityMode::L1 => Self::L1,
                DataAvailabilityMode::L2 => Self::L2,
            }
        }
    }

    impl From<pathfinder_common::transaction::DataAvailabilityMode> for DataAvailabilityMode {
        fn from(value: pathfinder_common::transaction::DataAvailabilityMode) -> Self {
            match value {
                pathfinder_common::transaction::DataAvailabilityMode::L1 => Self::L1,
                pathfinder_common::transaction::DataAvailabilityMode::L2 => Self::L2,
            }
        }
    }

    #[derive(Copy, Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq, Dummy)]
    pub struct ResourceBoundsV0 {
        pub l1_gas: ResourceBound,
        pub l2_gas: ResourceBound,
    }

    #[derive(Copy, Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq, Dummy)]
    pub struct ResourceBoundsV1 {
        pub l1_gas: ResourceBound,
        pub l2_gas: ResourceBound,
        pub l1_data_gas: Option<ResourceBound>,
    }

    impl From<ResourceBoundsV0> for ResourceBoundsV1 {
        fn from(value: ResourceBoundsV0) -> Self {
            Self {
                l1_gas: value.l1_gas,
                l2_gas: value.l2_gas,
                l1_data_gas: None,
            }
        }
    }

    impl From<ResourceBoundsV1> for pathfinder_common::transaction::ResourceBounds {
        fn from(value: ResourceBoundsV1) -> Self {
            Self {
                l1_gas: value.l1_gas.into(),
                l2_gas: value.l2_gas.into(),
                l1_data_gas: value.l1_data_gas.map(|g| g.into()),
            }
        }
    }

    impl From<pathfinder_common::transaction::ResourceBounds> for ResourceBoundsV1 {
        fn from(value: pathfinder_common::transaction::ResourceBounds) -> Self {
            Self {
                l1_gas: value.l1_gas.into(),
                l2_gas: value.l2_gas.into(),
                l1_data_gas: value.l1_data_gas.map(|g| g.into()),
            }
        }
    }

    #[derive(Copy, Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq, Dummy)]
    pub struct ResourceBound {
        pub max_amount: ResourceAmount,
        pub max_price_per_unit: ResourcePricePerUnit,
    }

    impl From<ResourceBound> for pathfinder_common::transaction::ResourceBound {
        fn from(value: ResourceBound) -> Self {
            Self {
                max_amount: value.max_amount,
                max_price_per_unit: value.max_price_per_unit,
            }
        }
    }

    impl From<pathfinder_common::transaction::ResourceBound> for ResourceBound {
        fn from(value: pathfinder_common::transaction::ResourceBound) -> Self {
            Self {
                max_amount: value.max_amount,
                max_price_per_unit: value.max_price_per_unit,
            }
        }
    }

    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    pub enum TransactionsWithReceiptsForBlock {
        V0 {
            transactions_with_receipts: Vec<TransactionWithReceiptV0>,
        },
        V1 {
            transactions_with_receipts: Vec<TransactionWithReceiptV1>,
        },
        V2 {
            transactions_with_receipts: Vec<TransactionWithReceiptV2>,
        },
        V3 {
            transactions_with_receipts: Vec<TransactionWithReceiptV3>,
        },
        V4 {
            transactions_with_receipts: Vec<TransactionWithReceiptV4>,
        },
    }

    impl TransactionsWithReceiptsForBlock {
        pub fn transactions_with_receipts(self) -> Vec<TransactionWithReceiptV4> {
            match self {
                TransactionsWithReceiptsForBlock::V0 {
                    transactions_with_receipts: v0,
                } => v0.into_iter().map(Into::into).collect(),
                TransactionsWithReceiptsForBlock::V1 {
                    transactions_with_receipts: v1,
                } => v1.into_iter().map(Into::into).collect(),
                TransactionsWithReceiptsForBlock::V2 {
                    transactions_with_receipts: v2,
                } => v2.into_iter().map(Into::into).collect(),
                TransactionsWithReceiptsForBlock::V3 {
                    transactions_with_receipts: v3,
                } => v3.into_iter().map(Into::into).collect(),
                TransactionsWithReceiptsForBlock::V4 {
                    transactions_with_receipts: v4,
                } => v4,
            }
        }
    }

    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
    pub struct TransactionWithReceiptV0 {
        pub transaction: TransactionV0,
        pub receipt: ReceiptV0,
    }

    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
    pub struct TransactionWithReceiptV1 {
        pub transaction: TransactionV1,
        pub receipt: ReceiptV1,
    }

    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
    pub struct TransactionWithReceiptV2 {
        pub transaction: TransactionV1,
        pub receipt: ReceiptV2,
    }

    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
    pub struct TransactionWithReceiptV3 {
        pub transaction: TransactionV2,
        pub receipt: ReceiptV3,
    }

    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
    pub struct TransactionWithReceiptV4 {
        pub transaction: TransactionV3,
        pub receipt: ReceiptV3,
    }

    impl From<TransactionWithReceiptV0> for TransactionWithReceiptV4 {
        fn from(v0: TransactionWithReceiptV0) -> Self {
            Self {
                transaction: v0.transaction.into(),
                receipt: v0.receipt.into(),
            }
        }
    }

    impl From<TransactionWithReceiptV1> for TransactionWithReceiptV4 {
        fn from(v1: TransactionWithReceiptV1) -> Self {
            Self {
                transaction: v1.transaction.into(),
                receipt: v1.receipt.into(),
            }
        }
    }

    impl From<TransactionWithReceiptV2> for TransactionWithReceiptV4 {
        fn from(v2: TransactionWithReceiptV2) -> Self {
            Self {
                transaction: v2.transaction.into(),
                receipt: v2.receipt.into(),
            }
        }
    }

    impl From<TransactionWithReceiptV3> for TransactionWithReceiptV4 {
        fn from(v3: TransactionWithReceiptV3) -> Self {
            Self {
                transaction: v3.transaction.into(),
                receipt: v3.receipt,
            }
        }
    }

    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Dummy)]
    #[serde(deny_unknown_fields)]
    pub struct TransactionV0 {
        hash: MinimalFelt,
        variant: TransactionVariantV0,
    }

    impl TransactionV0 {
        /// Returns hash of the transaction
        pub fn hash(&self) -> TransactionHash {
            TransactionHash(self.hash.to_owned().into())
        }
    }

    /// Represents deserialized L2 transaction data.
    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Dummy)]
    #[serde(deny_unknown_fields)]
    pub enum TransactionVariantV0 {
        DeclareV0(DeclareTransactionV0V1),
        DeclareV1(DeclareTransactionV0V1),
        DeclareV2(DeclareTransactionV2),
        DeclareV3(DeclareTransactionV3),
        // FIXME regenesis: remove Deploy txn type after regenesis
        // We are keeping this type of transaction until regenesis
        // only to support older pre-0.11.0 blocks
        Deploy(DeployTransaction),
        DeployAccountV1(DeployAccountTransactionV1),
        DeployAccountV3(DeployAccountTransactionV3),
        InvokeV0(InvokeTransactionV0),
        InvokeV1(InvokeTransactionV1),
        InvokeV3(InvokeTransactionV3),
        L1HandlerV0(L1HandlerTransactionV0),
    }

    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Dummy)]
    #[serde(deny_unknown_fields)]
    pub struct TransactionV1 {
        hash: MinimalFelt,
        variant: TransactionVariantV1,
    }

    impl TransactionV1 {
        /// Returns hash of the transaction
        pub fn hash(&self) -> TransactionHash {
            TransactionHash(self.hash.to_owned().into())
        }
    }

    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Dummy)]
    #[serde(deny_unknown_fields)]
    pub enum TransactionVariantV1 {
        DeclareV0(DeclareTransactionV0V1),
        DeclareV1(DeclareTransactionV0V1),
        DeclareV2(DeclareTransactionV2),
        DeclareV3(DeclareTransactionV3),
        // FIXME regenesis: remove Deploy txn type after regenesis
        // We are keeping this type of transaction until regenesis
        // only to support older pre-0.11.0 blocks
        DeployV0(DeployTransactionV0),
        DeployV1(DeployTransactionV1),
        DeployAccountV1(DeployAccountTransactionV1),
        DeployAccountV3(DeployAccountTransactionV3),
        InvokeV0(InvokeTransactionV0),
        InvokeV1(InvokeTransactionV1),
        InvokeV3(InvokeTransactionV3),
        L1HandlerV0(L1HandlerTransactionV0),
    }

    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Dummy)]
    #[serde(deny_unknown_fields)]
    pub struct TransactionV2 {
        hash: MinimalFelt,
        variant: TransactionVariantV2,
    }

    impl TransactionV2 {
        /// Returns hash of the transaction
        pub fn hash(&self) -> TransactionHash {
            TransactionHash(self.hash.to_owned().into())
        }
    }

    /// Represents deserialized L2 transaction data.
    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Dummy)]
    #[serde(deny_unknown_fields)]
    pub enum TransactionVariantV2 {
        DeclareV0(DeclareTransactionV0V1),
        DeclareV1(DeclareTransactionV0V1),
        DeclareV2(DeclareTransactionV2),
        DeclareV3(DeclareTransactionV3),
        DeclareV4(DeclareTransactionV4),
        // FIXME regenesis: remove Deploy txn type after regenesis
        // We are keeping this type of transaction until regenesis
        // only to support older pre-0.11.0 blocks
        DeployV0(DeployTransactionV0),
        DeployV1(DeployTransactionV1),
        DeployAccountV1(DeployAccountTransactionV1),
        DeployAccountV3(DeployAccountTransactionV3),
        DeployAccountV4(DeployAccountTransactionV4),
        InvokeV0(InvokeTransactionV0),
        InvokeV1(InvokeTransactionV1),
        InvokeV3(InvokeTransactionV3),
        InvokeV4(InvokeTransactionV4),
        L1HandlerV0(L1HandlerTransactionV0),
    }

    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Dummy)]
    #[serde(deny_unknown_fields)]
    pub struct TransactionV3 {
        hash: MinimalFelt,
        variant: TransactionVariantV3,
    }

    impl TransactionV3 {
        /// Returns hash of the transaction
        pub fn hash(&self) -> TransactionHash {
            TransactionHash(self.hash.to_owned().into())
        }
    }

    /// Represents deserialized L2 transaction data.
    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Dummy)]
    #[serde(deny_unknown_fields)]
    pub enum TransactionVariantV3 {
        DeclareV0(DeclareTransactionV0V1),
        DeclareV1(DeclareTransactionV0V1),
        DeclareV2(DeclareTransactionV2),
        DeclareV3(DeclareTransactionV3),
        DeclareV4(DeclareTransactionV4),
        // FIXME regenesis: remove Deploy txn type after regenesis
        // We are keeping this type of transaction until regenesis
        // only to support older pre-0.11.0 blocks
        DeployV0(DeployTransactionV0),
        DeployV1(DeployTransactionV1),
        DeployAccountV1(DeployAccountTransactionV1),
        DeployAccountV3(DeployAccountTransactionV3),
        DeployAccountV4(DeployAccountTransactionV4),
        InvokeV0(InvokeTransactionV0),
        InvokeV1(InvokeTransactionV1),
        InvokeV3(InvokeTransactionV3),
        InvokeV4(InvokeTransactionV4),
        InvokeV5(InvokeTransactionV5),
        L1HandlerV0(L1HandlerTransactionV0),
    }

    impl From<TransactionVariantV0> for TransactionVariantV3 {
        fn from(value: TransactionVariantV0) -> Self {
            match value {
                TransactionVariantV0::DeclareV0(tx) => Self::DeclareV0(tx),
                TransactionVariantV0::DeclareV1(tx) => Self::DeclareV1(tx),
                TransactionVariantV0::DeclareV2(tx) => Self::DeclareV2(tx),
                TransactionVariantV0::DeclareV3(tx) => Self::DeclareV3(tx),
                TransactionVariantV0::Deploy(tx) if tx.version.0 == Felt::ZERO => {
                    Self::DeployV0(DeployTransactionV0 {
                        contract_address: tx.contract_address,
                        contract_address_salt: tx.contract_address_salt,
                        class_hash: tx.class_hash,
                        constructor_calldata: tx.constructor_calldata,
                    })
                }
                TransactionVariantV0::Deploy(tx) if tx.version.0 == Felt::ONE => {
                    Self::DeployV1(DeployTransactionV1 {
                        contract_address: tx.contract_address,
                        contract_address_salt: tx.contract_address_salt,
                        class_hash: tx.class_hash,
                        constructor_calldata: tx.constructor_calldata,
                    })
                }
                TransactionVariantV0::Deploy(tx) => {
                    panic!("Unexpected deploy transaction version {}", tx.version.0)
                }
                TransactionVariantV0::DeployAccountV1(tx) => Self::DeployAccountV1(tx),
                TransactionVariantV0::DeployAccountV3(tx) => Self::DeployAccountV3(tx),
                TransactionVariantV0::InvokeV0(tx) => Self::InvokeV0(tx),
                TransactionVariantV0::InvokeV1(tx) => Self::InvokeV1(tx),
                TransactionVariantV0::InvokeV3(tx) => Self::InvokeV3(tx),
                TransactionVariantV0::L1HandlerV0(tx) => Self::L1HandlerV0(tx),
            }
        }
    }

    impl From<TransactionVariantV1> for TransactionVariantV3 {
        fn from(value: TransactionVariantV1) -> Self {
            match value {
                TransactionVariantV1::DeclareV0(tx) => Self::DeclareV0(tx),
                TransactionVariantV1::DeclareV1(tx) => Self::DeclareV1(tx),
                TransactionVariantV1::DeclareV2(tx) => Self::DeclareV2(tx),
                TransactionVariantV1::DeclareV3(tx) => Self::DeclareV3(tx),
                TransactionVariantV1::DeployV0(tx) => Self::DeployV0(tx),
                TransactionVariantV1::DeployV1(tx) => Self::DeployV1(tx),
                TransactionVariantV1::DeployAccountV1(tx) => Self::DeployAccountV1(tx),
                TransactionVariantV1::DeployAccountV3(tx) => Self::DeployAccountV3(tx),
                TransactionVariantV1::InvokeV0(tx) => Self::InvokeV0(tx),
                TransactionVariantV1::InvokeV1(tx) => Self::InvokeV1(tx),
                TransactionVariantV1::InvokeV3(tx) => Self::InvokeV3(tx),
                TransactionVariantV1::L1HandlerV0(tx) => Self::L1HandlerV0(tx),
            }
        }
    }

    impl From<TransactionVariantV2> for TransactionVariantV3 {
        fn from(value: TransactionVariantV2) -> Self {
            match value {
                TransactionVariantV2::DeclareV0(tx) => Self::DeclareV0(tx),
                TransactionVariantV2::DeclareV1(tx) => Self::DeclareV1(tx),
                TransactionVariantV2::DeclareV2(tx) => Self::DeclareV2(tx),
                TransactionVariantV2::DeclareV3(tx) => Self::DeclareV3(tx),
                TransactionVariantV2::DeclareV4(tx) => Self::DeclareV4(tx),
                TransactionVariantV2::DeployV0(tx) => Self::DeployV0(tx),
                TransactionVariantV2::DeployV1(tx) => Self::DeployV1(tx),
                TransactionVariantV2::DeployAccountV1(tx) => Self::DeployAccountV1(tx),
                TransactionVariantV2::DeployAccountV3(tx) => Self::DeployAccountV3(tx),
                TransactionVariantV2::DeployAccountV4(tx) => Self::DeployAccountV4(tx),
                TransactionVariantV2::InvokeV0(tx) => Self::InvokeV0(tx),
                TransactionVariantV2::InvokeV1(tx) => Self::InvokeV1(tx),
                TransactionVariantV2::InvokeV3(tx) => Self::InvokeV3(tx),
                TransactionVariantV2::InvokeV4(tx) => Self::InvokeV4(tx),
                TransactionVariantV2::L1HandlerV0(tx) => Self::L1HandlerV0(tx),
            }
        }
    }

    impl From<TransactionV0> for TransactionV3 {
        fn from(value: TransactionV0) -> Self {
            Self {
                hash: value.hash,
                variant: value.variant.into(),
            }
        }
    }

    impl From<TransactionV1> for TransactionV3 {
        fn from(value: TransactionV1) -> Self {
            Self {
                hash: value.hash,
                variant: value.variant.into(),
            }
        }
    }

    impl From<TransactionV2> for TransactionV3 {
        fn from(value: TransactionV2) -> Self {
            Self {
                hash: value.hash,
                variant: value.variant.into(),
            }
        }
    }

    impl From<&pathfinder_common::transaction::Transaction> for TransactionV3 {
        fn from(value: &pathfinder_common::transaction::Transaction) -> Self {
            use pathfinder_common::transaction::TransactionVariant::*;
            use pathfinder_common::transaction::*;

            let transaction_hash = value.hash;
            match value.variant.clone() {
                DeclareV0(DeclareTransactionV0V1 {
                    class_hash,
                    max_fee,
                    nonce,
                    sender_address,
                    signature,
                }) => Self {
                    hash: transaction_hash.as_inner().to_owned().into(),
                    variant: TransactionVariantV3::DeclareV0(self::DeclareTransactionV0V1 {
                        class_hash: class_hash.as_inner().to_owned().into(),
                        max_fee: max_fee.as_inner().to_owned().into(),
                        nonce: nonce.as_inner().to_owned().into(),
                        sender_address: sender_address.as_inner().to_owned().into(),
                        signature: signature
                            .into_iter()
                            .map(|x| x.as_inner().to_owned().into())
                            .collect(),
                    }),
                },
                DeclareV1(DeclareTransactionV0V1 {
                    class_hash,
                    max_fee,
                    nonce,
                    sender_address,
                    signature,
                }) => Self {
                    hash: transaction_hash.as_inner().to_owned().into(),
                    variant: TransactionVariantV3::DeclareV1(self::DeclareTransactionV0V1 {
                        class_hash: class_hash.as_inner().to_owned().into(),
                        max_fee: max_fee.as_inner().to_owned().into(),
                        nonce: nonce.as_inner().to_owned().into(),
                        sender_address: sender_address.as_inner().to_owned().into(),
                        signature: signature
                            .into_iter()
                            .map(|x| x.as_inner().to_owned().into())
                            .collect(),
                    }),
                },
                DeclareV2(DeclareTransactionV2 {
                    class_hash,
                    max_fee,
                    nonce,
                    sender_address,
                    signature,
                    compiled_class_hash,
                }) => Self {
                    hash: transaction_hash.as_inner().to_owned().into(),
                    variant: TransactionVariantV3::DeclareV2(self::DeclareTransactionV2 {
                        class_hash: class_hash.as_inner().to_owned().into(),
                        max_fee: max_fee.as_inner().to_owned().into(),
                        nonce: nonce.as_inner().to_owned().into(),
                        sender_address: sender_address.as_inner().to_owned().into(),
                        signature: signature
                            .into_iter()
                            .map(|x| x.as_inner().to_owned().into())
                            .collect(),
                        compiled_class_hash: compiled_class_hash.as_inner().to_owned().into(),
                    }),
                },
                DeclareV3(DeclareTransactionV3 {
                    class_hash,
                    nonce,
                    nonce_data_availability_mode,
                    fee_data_availability_mode,
                    resource_bounds,
                    tip,
                    paymaster_data,
                    signature,
                    account_deployment_data,
                    sender_address,
                    compiled_class_hash,
                }) => Self {
                    hash: transaction_hash.as_inner().to_owned().into(),
                    variant: TransactionVariantV3::DeclareV4(self::DeclareTransactionV4 {
                        class_hash: class_hash.as_inner().to_owned().into(),
                        nonce: nonce.as_inner().to_owned().into(),
                        nonce_data_availability_mode: nonce_data_availability_mode.into(),
                        fee_data_availability_mode: fee_data_availability_mode.into(),
                        resource_bounds: resource_bounds.into(),
                        tip,
                        paymaster_data: paymaster_data
                            .into_iter()
                            .map(|x| x.as_inner().to_owned().into())
                            .collect(),
                        sender_address: sender_address.as_inner().to_owned().into(),
                        signature: signature
                            .into_iter()
                            .map(|x| x.as_inner().to_owned().into())
                            .collect(),
                        compiled_class_hash: compiled_class_hash.as_inner().to_owned().into(),
                        account_deployment_data: account_deployment_data
                            .into_iter()
                            .map(|x| x.as_inner().to_owned().into())
                            .collect(),
                    }),
                },
                DeployV0(DeployTransactionV0 {
                    contract_address,
                    contract_address_salt,
                    class_hash,
                    constructor_calldata,
                }) => Self {
                    hash: transaction_hash.as_inner().to_owned().into(),
                    variant: TransactionVariantV3::DeployV0(self::DeployTransactionV0 {
                        contract_address: contract_address.as_inner().to_owned().into(),
                        contract_address_salt: contract_address_salt.as_inner().to_owned().into(),
                        class_hash: class_hash.as_inner().to_owned().into(),
                        constructor_calldata: constructor_calldata
                            .into_iter()
                            .map(|x| x.as_inner().to_owned().into())
                            .collect(),
                    }),
                },
                DeployV1(DeployTransactionV1 {
                    contract_address,
                    contract_address_salt,
                    class_hash,
                    constructor_calldata,
                }) => Self {
                    hash: transaction_hash.as_inner().to_owned().into(),
                    variant: TransactionVariantV3::DeployV1(self::DeployTransactionV1 {
                        contract_address: contract_address.as_inner().to_owned().into(),
                        contract_address_salt: contract_address_salt.as_inner().to_owned().into(),
                        class_hash: class_hash.as_inner().to_owned().into(),
                        constructor_calldata: constructor_calldata
                            .into_iter()
                            .map(|x| x.as_inner().to_owned().into())
                            .collect(),
                    }),
                },
                DeployAccountV1(DeployAccountTransactionV1 {
                    contract_address,
                    max_fee,
                    signature,
                    nonce,
                    contract_address_salt,
                    constructor_calldata,
                    class_hash,
                }) => Self {
                    hash: transaction_hash.as_inner().to_owned().into(),
                    variant: TransactionVariantV3::DeployAccountV1(
                        self::DeployAccountTransactionV1 {
                            contract_address: contract_address.as_inner().to_owned().into(),
                            max_fee: max_fee.as_inner().to_owned().into(),
                            signature: signature
                                .into_iter()
                                .map(|x| x.as_inner().to_owned().into())
                                .collect(),
                            nonce: nonce.as_inner().to_owned().into(),
                            contract_address_salt: contract_address_salt
                                .as_inner()
                                .to_owned()
                                .into(),
                            constructor_calldata: constructor_calldata
                                .into_iter()
                                .map(|x| x.as_inner().to_owned().into())
                                .collect(),
                            class_hash: class_hash.as_inner().to_owned().into(),
                        },
                    ),
                },
                DeployAccountV3(DeployAccountTransactionV3 {
                    contract_address,
                    signature,
                    nonce,
                    nonce_data_availability_mode,
                    fee_data_availability_mode,
                    resource_bounds,
                    tip,
                    paymaster_data,
                    contract_address_salt,
                    constructor_calldata,
                    class_hash,
                }) => Self {
                    hash: transaction_hash.as_inner().to_owned().into(),
                    variant: TransactionVariantV3::DeployAccountV4(
                        self::DeployAccountTransactionV4 {
                            nonce: nonce.as_inner().to_owned().into(),
                            nonce_data_availability_mode: nonce_data_availability_mode.into(),
                            fee_data_availability_mode: fee_data_availability_mode.into(),
                            resource_bounds: resource_bounds.into(),
                            tip,
                            paymaster_data: paymaster_data
                                .into_iter()
                                .map(|x| x.as_inner().to_owned().into())
                                .collect(),
                            sender_address: contract_address.as_inner().to_owned().into(),
                            signature: signature
                                .into_iter()
                                .map(|x| x.as_inner().to_owned().into())
                                .collect(),
                            contract_address_salt: contract_address_salt
                                .as_inner()
                                .to_owned()
                                .into(),
                            constructor_calldata: constructor_calldata
                                .into_iter()
                                .map(|x| x.as_inner().to_owned().into())
                                .collect(),
                            class_hash: class_hash.as_inner().to_owned().into(),
                        },
                    ),
                },
                InvokeV0(InvokeTransactionV0 {
                    calldata,
                    sender_address,
                    entry_point_selector,
                    entry_point_type,
                    max_fee,
                    signature,
                }) => Self {
                    hash: transaction_hash.as_inner().to_owned().into(),
                    variant: TransactionVariantV3::InvokeV0(self::InvokeTransactionV0 {
                        calldata: calldata
                            .into_iter()
                            .map(|x| x.as_inner().to_owned().into())
                            .collect(),
                        sender_address: sender_address.as_inner().to_owned().into(),
                        entry_point_selector: entry_point_selector.as_inner().to_owned().into(),
                        entry_point_type: entry_point_type.map(Into::into),
                        max_fee: max_fee.as_inner().to_owned().into(),
                        signature: signature
                            .into_iter()
                            .map(|x| x.as_inner().to_owned().into())
                            .collect(),
                    }),
                },
                InvokeV1(InvokeTransactionV1 {
                    calldata,
                    sender_address,
                    max_fee,
                    signature,
                    nonce,
                }) => Self {
                    hash: transaction_hash.as_inner().to_owned().into(),
                    variant: TransactionVariantV3::InvokeV1(self::InvokeTransactionV1 {
                        calldata: calldata
                            .into_iter()
                            .map(|x| x.as_inner().to_owned().into())
                            .collect(),
                        sender_address: sender_address.as_inner().to_owned().into(),
                        max_fee: max_fee.as_inner().to_owned().into(),
                        signature: signature
                            .into_iter()
                            .map(|x| x.as_inner().to_owned().into())
                            .collect(),
                        nonce: nonce.as_inner().to_owned().into(),
                    }),
                },
                InvokeV3(InvokeTransactionV3 {
                    signature,
                    nonce,
                    nonce_data_availability_mode,
                    fee_data_availability_mode,
                    resource_bounds,
                    tip,
                    paymaster_data,
                    account_deployment_data,
                    calldata,
                    sender_address,
                    proof_facts,
                }) => Self {
                    hash: transaction_hash.as_inner().to_owned().into(),
                    variant: TransactionVariantV3::InvokeV5(self::InvokeTransactionV5 {
                        nonce: nonce.as_inner().to_owned().into(),
                        nonce_data_availability_mode: nonce_data_availability_mode.into(),
                        fee_data_availability_mode: fee_data_availability_mode.into(),
                        resource_bounds: resource_bounds.into(),
                        tip,
                        paymaster_data: paymaster_data
                            .into_iter()
                            .map(|x| x.as_inner().to_owned().into())
                            .collect(),
                        sender_address: sender_address.as_inner().to_owned().into(),
                        signature: signature
                            .into_iter()
                            .map(|x| x.as_inner().to_owned().into())
                            .collect(),
                        calldata: calldata
                            .into_iter()
                            .map(|x| x.as_inner().to_owned().into())
                            .collect(),
                        account_deployment_data: account_deployment_data
                            .into_iter()
                            .map(|x| x.as_inner().to_owned().into())
                            .collect(),
                        proof_facts: proof_facts
                            .into_iter()
                            .map(|x| x.as_inner().to_owned().into())
                            .collect(),
                    }),
                },
                L1Handler(L1HandlerTransaction {
                    contract_address,
                    entry_point_selector,
                    nonce,
                    calldata,
                }) => Self {
                    hash: transaction_hash.as_inner().to_owned().into(),
                    variant: TransactionVariantV3::L1HandlerV0(self::L1HandlerTransactionV0 {
                        contract_address: contract_address.as_inner().to_owned().into(),
                        entry_point_selector: entry_point_selector.as_inner().to_owned().into(),
                        nonce: nonce.as_inner().to_owned().into(),
                        calldata: calldata
                            .into_iter()
                            .map(|x| x.as_inner().to_owned().into())
                            .collect(),
                    }),
                },
            }
        }
    }

    impl From<TransactionV3> for pathfinder_common::transaction::Transaction {
        fn from(value: TransactionV3) -> Self {
            use pathfinder_common::transaction::TransactionVariant;

            let hash = value.hash();
            let variant = match value {
                TransactionV3 {
                    hash: _,
                    variant:
                        TransactionVariantV3::DeclareV0(DeclareTransactionV0V1 {
                            class_hash,
                            max_fee,
                            nonce,
                            sender_address,
                            signature,
                        }),
                } => TransactionVariant::DeclareV0(
                    pathfinder_common::transaction::DeclareTransactionV0V1 {
                        class_hash: ClassHash(class_hash.into()),
                        max_fee: Fee(max_fee.into()),
                        nonce: TransactionNonce(nonce.into()),
                        sender_address: ContractAddress::new_or_panic(sender_address.into()),
                        signature: signature
                            .into_iter()
                            .map(|x| TransactionSignatureElem(x.into()))
                            .collect(),
                    },
                ),
                TransactionV3 {
                    hash: _,
                    variant:
                        TransactionVariantV3::DeclareV1(DeclareTransactionV0V1 {
                            class_hash,
                            max_fee,
                            nonce,
                            sender_address,
                            signature,
                        }),
                } => TransactionVariant::DeclareV1(
                    pathfinder_common::transaction::DeclareTransactionV0V1 {
                        class_hash: ClassHash(class_hash.into()),
                        max_fee: Fee(max_fee.into()),
                        nonce: TransactionNonce(nonce.into()),
                        sender_address: ContractAddress(sender_address.into()),
                        signature: signature
                            .into_iter()
                            .map(|x| TransactionSignatureElem(x.into()))
                            .collect(),
                    },
                ),
                TransactionV3 {
                    hash: _,
                    variant:
                        TransactionVariantV3::DeclareV2(DeclareTransactionV2 {
                            class_hash,
                            max_fee,
                            nonce,
                            sender_address,
                            signature,
                            compiled_class_hash,
                        }),
                } => TransactionVariant::DeclareV2(
                    pathfinder_common::transaction::DeclareTransactionV2 {
                        class_hash: ClassHash(class_hash.into()),
                        max_fee: Fee(max_fee.into()),
                        nonce: TransactionNonce(nonce.into()),
                        sender_address: ContractAddress(sender_address.into()),
                        signature: signature
                            .into_iter()
                            .map(|x| TransactionSignatureElem(x.into()))
                            .collect(),
                        compiled_class_hash: CasmHash::new_or_panic(compiled_class_hash.into()),
                    },
                ),
                TransactionV3 {
                    hash: _,
                    variant:
                        TransactionVariantV3::DeclareV3(DeclareTransactionV3 {
                            class_hash,
                            nonce,
                            nonce_data_availability_mode,
                            fee_data_availability_mode,
                            resource_bounds,
                            tip,
                            paymaster_data,
                            sender_address,
                            signature,
                            compiled_class_hash,
                            account_deployment_data,
                        }),
                } => TransactionVariant::DeclareV3(
                    pathfinder_common::transaction::DeclareTransactionV3 {
                        class_hash: ClassHash(class_hash.into()),
                        nonce: TransactionNonce(nonce.into()),
                        nonce_data_availability_mode: nonce_data_availability_mode.into(),
                        fee_data_availability_mode: fee_data_availability_mode.into(),
                        resource_bounds: Into::<ResourceBoundsV1>::into(resource_bounds).into(),
                        tip,
                        paymaster_data: paymaster_data
                            .into_iter()
                            .map(|x| PaymasterDataElem(x.into()))
                            .collect(),
                        sender_address: ContractAddress::new_or_panic(sender_address.into()),
                        signature: signature
                            .into_iter()
                            .map(|x| TransactionSignatureElem(x.into()))
                            .collect(),
                        compiled_class_hash: CasmHash::new_or_panic(compiled_class_hash.into()),
                        account_deployment_data: account_deployment_data
                            .into_iter()
                            .map(|x| AccountDeploymentDataElem(x.into()))
                            .collect(),
                    },
                ),
                TransactionV3 {
                    hash: _,
                    variant:
                        TransactionVariantV3::DeclareV4(DeclareTransactionV4 {
                            class_hash,
                            nonce,
                            nonce_data_availability_mode,
                            fee_data_availability_mode,
                            resource_bounds,
                            tip,
                            paymaster_data,
                            sender_address,
                            signature,
                            compiled_class_hash,
                            account_deployment_data,
                        }),
                } => TransactionVariant::DeclareV3(
                    pathfinder_common::transaction::DeclareTransactionV3 {
                        class_hash: ClassHash(class_hash.into()),
                        nonce: TransactionNonce(nonce.into()),
                        nonce_data_availability_mode: nonce_data_availability_mode.into(),
                        fee_data_availability_mode: fee_data_availability_mode.into(),
                        resource_bounds: resource_bounds.into(),
                        tip,
                        paymaster_data: paymaster_data
                            .into_iter()
                            .map(|x| PaymasterDataElem(x.into()))
                            .collect(),
                        sender_address: ContractAddress::new_or_panic(sender_address.into()),
                        signature: signature
                            .into_iter()
                            .map(|x| TransactionSignatureElem(x.into()))
                            .collect(),
                        compiled_class_hash: CasmHash::new_or_panic(compiled_class_hash.into()),
                        account_deployment_data: account_deployment_data
                            .into_iter()
                            .map(|x| AccountDeploymentDataElem(x.into()))
                            .collect(),
                    },
                ),
                TransactionV3 {
                    hash: _,
                    variant:
                        TransactionVariantV3::DeployV0(DeployTransactionV0 {
                            contract_address,
                            contract_address_salt,
                            class_hash,
                            constructor_calldata,
                        }),
                } => TransactionVariant::DeployV0(
                    pathfinder_common::transaction::DeployTransactionV0 {
                        contract_address: ContractAddress::new_or_panic(contract_address.into()),
                        contract_address_salt: ContractAddressSalt(contract_address_salt.into()),
                        class_hash: ClassHash(class_hash.into()),
                        constructor_calldata: constructor_calldata
                            .into_iter()
                            .map(|x| ConstructorParam(x.into()))
                            .collect(),
                    },
                ),
                TransactionV3 {
                    hash: _,
                    variant:
                        TransactionVariantV3::DeployV1(DeployTransactionV1 {
                            contract_address,
                            contract_address_salt,
                            class_hash,
                            constructor_calldata,
                        }),
                } => TransactionVariant::DeployV1(
                    pathfinder_common::transaction::DeployTransactionV1 {
                        contract_address: ContractAddress::new_or_panic(contract_address.into()),
                        contract_address_salt: ContractAddressSalt(contract_address_salt.into()),
                        class_hash: ClassHash(class_hash.into()),
                        constructor_calldata: constructor_calldata
                            .into_iter()
                            .map(|x| ConstructorParam(x.into()))
                            .collect(),
                    },
                ),
                TransactionV3 {
                    hash: _,
                    variant:
                        TransactionVariantV3::DeployAccountV1(DeployAccountTransactionV1 {
                            contract_address,
                            max_fee,
                            signature,
                            nonce,
                            contract_address_salt,
                            constructor_calldata,
                            class_hash,
                        }),
                } => TransactionVariant::DeployAccountV1(
                    pathfinder_common::transaction::DeployAccountTransactionV1 {
                        contract_address: ContractAddress::new_or_panic(contract_address.into()),
                        max_fee: Fee(max_fee.into()),
                        signature: signature
                            .into_iter()
                            .map(|x| TransactionSignatureElem(x.into()))
                            .collect(),
                        nonce: TransactionNonce(nonce.into()),
                        contract_address_salt: ContractAddressSalt(contract_address_salt.into()),
                        constructor_calldata: constructor_calldata
                            .into_iter()
                            .map(|x| CallParam(x.into()))
                            .collect(),
                        class_hash: ClassHash(class_hash.into()),
                    },
                ),
                TransactionV3 {
                    hash: _,
                    variant:
                        TransactionVariantV3::DeployAccountV3(DeployAccountTransactionV3 {
                            nonce,
                            nonce_data_availability_mode,
                            fee_data_availability_mode,
                            resource_bounds,
                            tip,
                            paymaster_data,
                            sender_address,
                            signature,
                            contract_address_salt,
                            constructor_calldata,
                            class_hash,
                        }),
                } => TransactionVariant::DeployAccountV3(
                    pathfinder_common::transaction::DeployAccountTransactionV3 {
                        contract_address: ContractAddress::new_or_panic(sender_address.into()),
                        signature: signature
                            .into_iter()
                            .map(|x| TransactionSignatureElem(x.into()))
                            .collect(),
                        nonce: TransactionNonce(nonce.into()),
                        nonce_data_availability_mode: nonce_data_availability_mode.into(),
                        fee_data_availability_mode: fee_data_availability_mode.into(),
                        resource_bounds: Into::<ResourceBoundsV1>::into(resource_bounds).into(),
                        tip,
                        paymaster_data: paymaster_data
                            .into_iter()
                            .map(|x| PaymasterDataElem(x.into()))
                            .collect(),
                        contract_address_salt: ContractAddressSalt(contract_address_salt.into()),
                        constructor_calldata: constructor_calldata
                            .into_iter()
                            .map(|x| CallParam(x.into()))
                            .collect(),
                        class_hash: ClassHash(class_hash.into()),
                    },
                ),
                TransactionV3 {
                    hash: _,
                    variant:
                        TransactionVariantV3::DeployAccountV4(DeployAccountTransactionV4 {
                            nonce,
                            nonce_data_availability_mode,
                            fee_data_availability_mode,
                            resource_bounds,
                            tip,
                            paymaster_data,
                            sender_address,
                            signature,
                            contract_address_salt,
                            constructor_calldata,
                            class_hash,
                        }),
                } => TransactionVariant::DeployAccountV3(
                    pathfinder_common::transaction::DeployAccountTransactionV3 {
                        contract_address: ContractAddress::new_or_panic(sender_address.into()),
                        signature: signature
                            .into_iter()
                            .map(|x| TransactionSignatureElem(x.into()))
                            .collect(),
                        nonce: TransactionNonce(nonce.into()),
                        nonce_data_availability_mode: nonce_data_availability_mode.into(),
                        fee_data_availability_mode: fee_data_availability_mode.into(),
                        resource_bounds: resource_bounds.into(),
                        tip,
                        paymaster_data: paymaster_data
                            .into_iter()
                            .map(|x| PaymasterDataElem(x.into()))
                            .collect(),
                        contract_address_salt: ContractAddressSalt(contract_address_salt.into()),
                        constructor_calldata: constructor_calldata
                            .into_iter()
                            .map(|x| CallParam(x.into()))
                            .collect(),
                        class_hash: ClassHash(class_hash.into()),
                    },
                ),
                TransactionV3 {
                    hash: _,
                    variant:
                        TransactionVariantV3::InvokeV0(InvokeTransactionV0 {
                            calldata,
                            sender_address,
                            entry_point_selector,
                            entry_point_type,
                            max_fee,
                            signature,
                        }),
                } => TransactionVariant::InvokeV0(
                    pathfinder_common::transaction::InvokeTransactionV0 {
                        calldata: calldata.into_iter().map(|x| CallParam(x.into())).collect(),
                        sender_address: ContractAddress::new_or_panic(sender_address.into()),
                        entry_point_selector: EntryPoint(entry_point_selector.into()),
                        entry_point_type: entry_point_type.map(Into::into),
                        max_fee: Fee(max_fee.into()),
                        signature: signature
                            .into_iter()
                            .map(|x| TransactionSignatureElem(x.into()))
                            .collect(),
                    },
                ),
                TransactionV3 {
                    hash: _,
                    variant:
                        TransactionVariantV3::InvokeV1(InvokeTransactionV1 {
                            calldata,
                            sender_address,
                            max_fee,
                            signature,
                            nonce,
                        }),
                } => TransactionVariant::InvokeV1(
                    pathfinder_common::transaction::InvokeTransactionV1 {
                        calldata: calldata.into_iter().map(|x| CallParam(x.into())).collect(),
                        sender_address: ContractAddress::new_or_panic(sender_address.into()),
                        max_fee: Fee(max_fee.into()),
                        signature: signature
                            .into_iter()
                            .map(|x| TransactionSignatureElem(x.into()))
                            .collect(),
                        nonce: TransactionNonce(nonce.into()),
                    },
                ),
                TransactionV3 {
                    hash: _,
                    variant:
                        TransactionVariantV3::InvokeV3(InvokeTransactionV3 {
                            nonce,
                            nonce_data_availability_mode,
                            fee_data_availability_mode,
                            resource_bounds,
                            tip,
                            paymaster_data,
                            sender_address,
                            signature,
                            calldata,
                            account_deployment_data,
                        }),
                } => TransactionVariant::InvokeV3(
                    pathfinder_common::transaction::InvokeTransactionV3 {
                        signature: signature
                            .into_iter()
                            .map(|x| TransactionSignatureElem(x.into()))
                            .collect(),
                        nonce: TransactionNonce(nonce.into()),
                        nonce_data_availability_mode: nonce_data_availability_mode.into(),
                        fee_data_availability_mode: fee_data_availability_mode.into(),
                        resource_bounds: Into::<ResourceBoundsV1>::into(resource_bounds).into(),
                        tip,
                        paymaster_data: paymaster_data
                            .into_iter()
                            .map(|x| PaymasterDataElem(x.into()))
                            .collect(),
                        account_deployment_data: account_deployment_data
                            .into_iter()
                            .map(|x| AccountDeploymentDataElem(x.into()))
                            .collect(),
                        calldata: calldata.into_iter().map(|x| CallParam(x.into())).collect(),
                        sender_address: ContractAddress::new_or_panic(sender_address.into()),
                        proof_facts: vec![],
                    },
                ),
                TransactionV3 {
                    hash: _,
                    variant:
                        TransactionVariantV3::InvokeV4(InvokeTransactionV4 {
                            nonce,
                            nonce_data_availability_mode,
                            fee_data_availability_mode,
                            resource_bounds,
                            tip,
                            paymaster_data,
                            sender_address,
                            signature,
                            calldata,
                            account_deployment_data,
                        }),
                } => TransactionVariant::InvokeV3(
                    pathfinder_common::transaction::InvokeTransactionV3 {
                        signature: signature
                            .into_iter()
                            .map(|x| TransactionSignatureElem(x.into()))
                            .collect(),
                        nonce: TransactionNonce(nonce.into()),
                        nonce_data_availability_mode: nonce_data_availability_mode.into(),
                        fee_data_availability_mode: fee_data_availability_mode.into(),
                        resource_bounds: resource_bounds.into(),
                        tip,
                        paymaster_data: paymaster_data
                            .into_iter()
                            .map(|x| PaymasterDataElem(x.into()))
                            .collect(),
                        account_deployment_data: account_deployment_data
                            .into_iter()
                            .map(|x| AccountDeploymentDataElem(x.into()))
                            .collect(),
                        calldata: calldata.into_iter().map(|x| CallParam(x.into())).collect(),
                        sender_address: ContractAddress::new_or_panic(sender_address.into()),
                        proof_facts: vec![],
                    },
                ),
                TransactionV3 {
                    hash: _,
                    variant:
                        TransactionVariantV3::InvokeV5(InvokeTransactionV5 {
                            nonce,
                            nonce_data_availability_mode,
                            fee_data_availability_mode,
                            resource_bounds,
                            tip,
                            paymaster_data,
                            sender_address,
                            signature,
                            calldata,
                            account_deployment_data,
                            proof_facts,
                        }),
                } => TransactionVariant::InvokeV3(
                    pathfinder_common::transaction::InvokeTransactionV3 {
                        signature: signature
                            .into_iter()
                            .map(|x| TransactionSignatureElem(x.into()))
                            .collect(),
                        nonce: TransactionNonce(nonce.into()),
                        nonce_data_availability_mode: nonce_data_availability_mode.into(),
                        fee_data_availability_mode: fee_data_availability_mode.into(),
                        resource_bounds: resource_bounds.into(),
                        tip,
                        paymaster_data: paymaster_data
                            .into_iter()
                            .map(|x| PaymasterDataElem(x.into()))
                            .collect(),
                        account_deployment_data: account_deployment_data
                            .into_iter()
                            .map(|x| AccountDeploymentDataElem(x.into()))
                            .collect(),
                        calldata: calldata.into_iter().map(|x| CallParam(x.into())).collect(),
                        sender_address: ContractAddress::new_or_panic(sender_address.into()),
                        proof_facts: proof_facts
                            .into_iter()
                            .map(|x| ProofFactElem(x.into()))
                            .collect(),
                    },
                ),
                TransactionV3 {
                    hash: _,
                    variant:
                        TransactionVariantV3::L1HandlerV0(L1HandlerTransactionV0 {
                            contract_address,
                            entry_point_selector,
                            nonce,
                            calldata,
                        }),
                } => TransactionVariant::L1Handler(
                    pathfinder_common::transaction::L1HandlerTransaction {
                        contract_address: ContractAddress::new_or_panic(contract_address.into()),
                        entry_point_selector: EntryPoint(entry_point_selector.into()),
                        nonce: TransactionNonce(nonce.into()),
                        calldata: calldata.into_iter().map(|x| CallParam(x.into())).collect(),
                    },
                ),
            };

            pathfinder_common::transaction::Transaction { hash, variant }
        }
    }

    /// A version 0 or 1 declare transaction.
    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    pub struct DeclareTransactionV0V1 {
        pub class_hash: MinimalFelt,
        pub max_fee: MinimalFelt,
        pub nonce: MinimalFelt,
        pub signature: Vec<MinimalFelt>,
        pub sender_address: MinimalFelt,
    }

    impl<T> Dummy<T> for DeclareTransactionV0V1 {
        fn dummy_with_rng<R: rand::Rng + ?Sized>(_: &T, rng: &mut R) -> Self {
            Self {
                class_hash: Faker.fake_with_rng(rng),
                max_fee: Faker.fake_with_rng(rng),
                nonce: TransactionNonce::ZERO.0.into(),
                sender_address: Faker.fake_with_rng(rng),
                signature: Faker.fake_with_rng(rng),
            }
        }
    }

    /// A version 2 declare transaction.
    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Dummy)]
    #[serde(deny_unknown_fields)]
    pub struct DeclareTransactionV2 {
        pub class_hash: MinimalFelt,
        pub max_fee: MinimalFelt,
        pub nonce: MinimalFelt,
        pub signature: Vec<MinimalFelt>,
        pub sender_address: MinimalFelt,
        pub compiled_class_hash: MinimalFelt,
    }

    /// A version 2 declare transaction.
    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    pub struct DeclareTransactionV3 {
        pub class_hash: MinimalFelt,
        pub nonce: MinimalFelt,
        pub nonce_data_availability_mode: DataAvailabilityMode,
        pub fee_data_availability_mode: DataAvailabilityMode,
        pub resource_bounds: ResourceBoundsV0,
        pub tip: Tip,
        pub paymaster_data: Vec<MinimalFelt>,
        pub signature: Vec<MinimalFelt>,
        pub account_deployment_data: Vec<MinimalFelt>,
        pub sender_address: MinimalFelt,
        pub compiled_class_hash: MinimalFelt,
    }

    impl<T> Dummy<T> for DeclareTransactionV3 {
        fn dummy_with_rng<R: rand::Rng + ?Sized>(_: &T, rng: &mut R) -> Self {
            Self {
                class_hash: Faker.fake_with_rng(rng),
                nonce: Faker.fake_with_rng(rng),
                nonce_data_availability_mode: Faker.fake_with_rng(rng),
                fee_data_availability_mode: Faker.fake_with_rng(rng),
                resource_bounds: Faker.fake_with_rng(rng),
                tip: Faker.fake_with_rng(rng),
                paymaster_data: vec![Faker.fake_with_rng(rng)], // TODO p2p allows 1 elem only
                sender_address: Faker.fake_with_rng(rng),
                signature: Faker.fake_with_rng(rng),
                compiled_class_hash: Faker.fake_with_rng(rng),
                account_deployment_data: vec![Faker.fake_with_rng(rng)], /* TODO p2p allows 1
                                                                          * elem only */
            }
        }
    }

    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    pub struct DeclareTransactionV4 {
        pub class_hash: MinimalFelt,
        pub nonce: MinimalFelt,
        pub nonce_data_availability_mode: DataAvailabilityMode,
        pub fee_data_availability_mode: DataAvailabilityMode,
        pub resource_bounds: ResourceBoundsV1,
        pub tip: Tip,
        pub paymaster_data: Vec<MinimalFelt>,
        pub signature: Vec<MinimalFelt>,
        pub account_deployment_data: Vec<MinimalFelt>,
        pub sender_address: MinimalFelt,
        pub compiled_class_hash: MinimalFelt,
    }

    impl<T> Dummy<T> for DeclareTransactionV4 {
        fn dummy_with_rng<R: rand::Rng + ?Sized>(_: &T, rng: &mut R) -> Self {
            Self {
                class_hash: Faker.fake_with_rng(rng),
                nonce: Faker.fake_with_rng(rng),
                nonce_data_availability_mode: Faker.fake_with_rng(rng),
                fee_data_availability_mode: Faker.fake_with_rng(rng),
                resource_bounds: Faker.fake_with_rng(rng),
                tip: Faker.fake_with_rng(rng),
                paymaster_data: vec![Faker.fake_with_rng(rng)], // TODO p2p allows 1 elem only
                sender_address: Faker.fake_with_rng(rng),
                signature: Faker.fake_with_rng(rng),
                compiled_class_hash: Faker.fake_with_rng(rng),
                account_deployment_data: vec![Faker.fake_with_rng(rng)], /* TODO p2p allows 1
                                                                          * elem only */
            }
        }
    }

    /// Represents deserialized L2 deploy transaction data.
    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    pub struct DeployTransaction {
        pub contract_address: MinimalFelt,
        pub version: MinimalFelt,
        pub contract_address_salt: MinimalFelt,
        pub class_hash: MinimalFelt,
        pub constructor_calldata: Vec<MinimalFelt>,
    }

    impl<T> Dummy<T> for DeployTransaction {
        fn dummy_with_rng<R: rand::Rng + ?Sized>(_: &T, rng: &mut R) -> Self {
            let class_hash: MinimalFelt = Faker.fake_with_rng(rng);
            let constructor_calldata: Vec<MinimalFelt> = Faker.fake_with_rng(rng);
            let contract_address_salt: MinimalFelt = Faker.fake_with_rng(rng);

            let contract_address = ContractAddress::deployed_contract_address(
                constructor_calldata.iter().map(|f| CallParam(f.0)),
                &ContractAddressSalt(contract_address_salt.0),
                &ClassHash(class_hash.0),
            )
            .as_inner()
            .to_owned()
            .into();

            Self {
                version: Felt::from_u64(rng.gen_range(0..=1)).into(),
                contract_address,
                contract_address_salt,
                class_hash,
                constructor_calldata,
            }
        }
    }

    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    pub struct DeployTransactionV0 {
        pub contract_address: MinimalFelt,
        pub contract_address_salt: MinimalFelt,
        pub class_hash: MinimalFelt,
        pub constructor_calldata: Vec<MinimalFelt>,
    }

    impl<T> Dummy<T> for DeployTransactionV0 {
        fn dummy_with_rng<R: rand::Rng + ?Sized>(_: &T, rng: &mut R) -> Self {
            let class_hash: MinimalFelt = Faker.fake_with_rng(rng);
            let constructor_calldata: Vec<MinimalFelt> = Faker.fake_with_rng(rng);
            let contract_address_salt: MinimalFelt = Faker.fake_with_rng(rng);

            let contract_address = ContractAddress::deployed_contract_address(
                constructor_calldata.iter().map(|f| CallParam(f.0)),
                &ContractAddressSalt(contract_address_salt.0),
                &ClassHash(class_hash.0),
            )
            .as_inner()
            .to_owned()
            .into();

            Self {
                contract_address,
                contract_address_salt,
                class_hash,
                constructor_calldata,
            }
        }
    }

    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    pub struct DeployTransactionV1 {
        pub contract_address: MinimalFelt,
        pub contract_address_salt: MinimalFelt,
        pub class_hash: MinimalFelt,
        pub constructor_calldata: Vec<MinimalFelt>,
    }

    impl<T> Dummy<T> for DeployTransactionV1 {
        fn dummy_with_rng<R: rand::Rng + ?Sized>(_: &T, rng: &mut R) -> Self {
            let class_hash: MinimalFelt = Faker.fake_with_rng(rng);
            let constructor_calldata: Vec<MinimalFelt> = Faker.fake_with_rng(rng);
            let contract_address_salt: MinimalFelt = Faker.fake_with_rng(rng);

            let contract_address = ContractAddress::deployed_contract_address(
                constructor_calldata.iter().map(|f| CallParam(f.0)),
                &ContractAddressSalt(contract_address_salt.0),
                &ClassHash(class_hash.0),
            )
            .as_inner()
            .to_owned()
            .into();

            Self {
                contract_address,
                contract_address_salt,
                class_hash,
                constructor_calldata,
            }
        }
    }

    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    pub struct DeployAccountTransactionV1 {
        pub contract_address: MinimalFelt,
        pub max_fee: MinimalFelt,
        pub signature: Vec<MinimalFelt>,
        pub nonce: MinimalFelt,
        pub contract_address_salt: MinimalFelt,
        pub constructor_calldata: Vec<MinimalFelt>,
        pub class_hash: MinimalFelt,
    }

    impl<T> Dummy<T> for DeployAccountTransactionV1 {
        fn dummy_with_rng<R: rand::Rng + ?Sized>(_: &T, rng: &mut R) -> Self {
            let contract_address_salt = Faker.fake_with_rng(rng);
            let constructor_calldata: Vec<CallParam> = Faker.fake_with_rng(rng);
            let class_hash = Faker.fake_with_rng(rng);

            Self {
                contract_address: ContractAddress::deployed_contract_address(
                    constructor_calldata.iter().copied(),
                    &contract_address_salt,
                    &class_hash,
                )
                .as_inner()
                .to_owned()
                .into(),
                max_fee: Faker.fake_with_rng(rng),
                signature: Faker.fake_with_rng(rng),
                nonce: Faker.fake_with_rng(rng),
                contract_address_salt: contract_address_salt.as_inner().to_owned().into(),
                constructor_calldata: constructor_calldata
                    .into_iter()
                    .map(|x| x.as_inner().to_owned().into())
                    .collect(),
                class_hash: class_hash.as_inner().to_owned().into(),
            }
        }
    }

    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    pub struct DeployAccountTransactionV3 {
        pub sender_address: MinimalFelt,
        pub signature: Vec<MinimalFelt>,
        pub nonce: MinimalFelt,
        pub nonce_data_availability_mode: DataAvailabilityMode,
        pub fee_data_availability_mode: DataAvailabilityMode,
        pub resource_bounds: ResourceBoundsV0,
        pub tip: Tip,
        pub paymaster_data: Vec<MinimalFelt>,
        pub contract_address_salt: MinimalFelt,
        pub constructor_calldata: Vec<MinimalFelt>,
        pub class_hash: MinimalFelt,
    }

    impl<T> Dummy<T> for DeployAccountTransactionV3 {
        fn dummy_with_rng<R: rand::Rng + ?Sized>(_: &T, rng: &mut R) -> Self {
            let contract_address_salt = Faker.fake_with_rng(rng);
            let constructor_calldata: Vec<CallParam> = Faker.fake_with_rng(rng);
            let class_hash = Faker.fake_with_rng(rng);

            Self {
                nonce: Faker.fake_with_rng(rng),
                nonce_data_availability_mode: Faker.fake_with_rng(rng),
                fee_data_availability_mode: Faker.fake_with_rng(rng),
                resource_bounds: Faker.fake_with_rng(rng),
                tip: Faker.fake_with_rng(rng),
                paymaster_data: vec![Faker.fake_with_rng(rng)], // TODO p2p allows 1 elem only

                sender_address: ContractAddress::deployed_contract_address(
                    constructor_calldata.iter().copied(),
                    &contract_address_salt,
                    &class_hash,
                )
                .as_inner()
                .to_owned()
                .into(),
                signature: Faker.fake_with_rng(rng),
                contract_address_salt: contract_address_salt.as_inner().to_owned().into(),
                constructor_calldata: constructor_calldata
                    .into_iter()
                    .map(|x| x.as_inner().to_owned().into())
                    .collect(),
                class_hash: class_hash.as_inner().to_owned().into(),
            }
        }
    }

    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    pub struct DeployAccountTransactionV4 {
        pub sender_address: MinimalFelt,
        pub signature: Vec<MinimalFelt>,
        pub nonce: MinimalFelt,
        pub nonce_data_availability_mode: DataAvailabilityMode,
        pub fee_data_availability_mode: DataAvailabilityMode,
        pub resource_bounds: ResourceBoundsV1,
        pub tip: Tip,
        pub paymaster_data: Vec<MinimalFelt>,
        pub contract_address_salt: MinimalFelt,
        pub constructor_calldata: Vec<MinimalFelt>,
        pub class_hash: MinimalFelt,
    }

    impl<T> Dummy<T> for DeployAccountTransactionV4 {
        fn dummy_with_rng<R: rand::Rng + ?Sized>(_: &T, rng: &mut R) -> Self {
            let contract_address_salt = Faker.fake_with_rng(rng);
            let constructor_calldata: Vec<CallParam> = Faker.fake_with_rng(rng);
            let class_hash = Faker.fake_with_rng(rng);

            Self {
                nonce: Faker.fake_with_rng(rng),
                nonce_data_availability_mode: Faker.fake_with_rng(rng),
                fee_data_availability_mode: Faker.fake_with_rng(rng),
                resource_bounds: Faker.fake_with_rng(rng),
                tip: Faker.fake_with_rng(rng),
                paymaster_data: vec![Faker.fake_with_rng(rng)], // TODO p2p allows 1 elem only

                sender_address: ContractAddress::deployed_contract_address(
                    constructor_calldata.iter().copied(),
                    &contract_address_salt,
                    &class_hash,
                )
                .as_inner()
                .to_owned()
                .into(),
                signature: Faker.fake_with_rng(rng),
                contract_address_salt: contract_address_salt.as_inner().to_owned().into(),
                constructor_calldata: constructor_calldata
                    .into_iter()
                    .map(|x| x.as_inner().to_owned().into())
                    .collect(),
                class_hash: class_hash.as_inner().to_owned().into(),
            }
        }
    }

    /// Represents deserialized L2 invoke transaction v0 data.
    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    pub struct InvokeTransactionV0 {
        pub calldata: Vec<MinimalFelt>,
        pub sender_address: MinimalFelt,
        pub entry_point_selector: MinimalFelt,
        pub entry_point_type: Option<EntryPointType>,
        pub max_fee: MinimalFelt,
        pub signature: Vec<MinimalFelt>,
    }

    impl<T> Dummy<T> for InvokeTransactionV0 {
        fn dummy_with_rng<R: rand::Rng + ?Sized>(_: &T, rng: &mut R) -> Self {
            Self {
                calldata: Faker.fake_with_rng(rng),
                sender_address: Faker.fake_with_rng(rng),
                entry_point_selector: Faker.fake_with_rng(rng),
                entry_point_type: None,
                max_fee: Faker.fake_with_rng(rng),
                signature: Faker.fake_with_rng(rng),
            }
        }
    }

    /// Represents deserialized L2 invoke transaction v1 data.
    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Dummy)]
    #[serde(deny_unknown_fields)]
    pub struct InvokeTransactionV1 {
        pub calldata: Vec<MinimalFelt>,
        pub sender_address: MinimalFelt,
        pub max_fee: MinimalFelt,
        pub signature: Vec<MinimalFelt>,
        pub nonce: MinimalFelt,
    }

    /// Represents deserialized L2 invoke transaction v3 data.
    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    pub struct InvokeTransactionV3 {
        pub signature: Vec<MinimalFelt>,
        pub nonce: MinimalFelt,
        pub nonce_data_availability_mode: DataAvailabilityMode,
        pub fee_data_availability_mode: DataAvailabilityMode,
        pub resource_bounds: ResourceBoundsV0,
        pub tip: Tip,
        pub paymaster_data: Vec<MinimalFelt>,
        pub account_deployment_data: Vec<MinimalFelt>,
        pub calldata: Vec<MinimalFelt>,
        pub sender_address: MinimalFelt,
    }

    impl<T> Dummy<T> for InvokeTransactionV3 {
        fn dummy_with_rng<R: rand::Rng + ?Sized>(_: &T, rng: &mut R) -> Self {
            Self {
                nonce: Faker.fake_with_rng(rng),
                nonce_data_availability_mode: Faker.fake_with_rng(rng),
                fee_data_availability_mode: Faker.fake_with_rng(rng),
                resource_bounds: Faker.fake_with_rng(rng),
                tip: Faker.fake_with_rng(rng),
                paymaster_data: vec![Faker.fake_with_rng(rng)], // TODO p2p allows 1 elem only

                sender_address: Faker.fake_with_rng(rng),
                signature: Faker.fake_with_rng(rng),
                calldata: Faker.fake_with_rng(rng),
                account_deployment_data: vec![Faker.fake_with_rng(rng)], /* TODO p2p allows 1
                                                                          * elem only */
            }
        }
    }

    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    pub struct InvokeTransactionV4 {
        pub signature: Vec<MinimalFelt>,
        pub nonce: MinimalFelt,
        pub nonce_data_availability_mode: DataAvailabilityMode,
        pub fee_data_availability_mode: DataAvailabilityMode,
        pub resource_bounds: ResourceBoundsV1,
        pub tip: Tip,
        pub paymaster_data: Vec<MinimalFelt>,
        pub account_deployment_data: Vec<MinimalFelt>,
        pub calldata: Vec<MinimalFelt>,
        pub sender_address: MinimalFelt,
    }

    impl<T> Dummy<T> for InvokeTransactionV4 {
        fn dummy_with_rng<R: rand::Rng + ?Sized>(_: &T, rng: &mut R) -> Self {
            Self {
                nonce: Faker.fake_with_rng(rng),
                nonce_data_availability_mode: Faker.fake_with_rng(rng),
                fee_data_availability_mode: Faker.fake_with_rng(rng),
                resource_bounds: Faker.fake_with_rng(rng),
                tip: Faker.fake_with_rng(rng),
                paymaster_data: vec![Faker.fake_with_rng(rng)], // TODO p2p allows 1 elem only

                sender_address: Faker.fake_with_rng(rng),
                signature: Faker.fake_with_rng(rng),
                calldata: Faker.fake_with_rng(rng),
                account_deployment_data: vec![Faker.fake_with_rng(rng)], /* TODO p2p allows 1
                                                                          * elem only */
            }
        }
    }

    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    pub struct InvokeTransactionV5 {
        pub signature: Vec<MinimalFelt>,
        pub nonce: MinimalFelt,
        pub nonce_data_availability_mode: DataAvailabilityMode,
        pub fee_data_availability_mode: DataAvailabilityMode,
        pub resource_bounds: ResourceBoundsV1,
        pub tip: Tip,
        pub paymaster_data: Vec<MinimalFelt>,
        pub account_deployment_data: Vec<MinimalFelt>,
        pub calldata: Vec<MinimalFelt>,
        pub sender_address: MinimalFelt,
        pub proof_facts: Vec<MinimalFelt>,
    }

    impl<T> Dummy<T> for InvokeTransactionV5 {
        fn dummy_with_rng<R: rand::Rng + ?Sized>(_: &T, rng: &mut R) -> Self {
            Self {
                nonce: Faker.fake_with_rng(rng),
                nonce_data_availability_mode: Faker.fake_with_rng(rng),
                fee_data_availability_mode: Faker.fake_with_rng(rng),
                resource_bounds: Faker.fake_with_rng(rng),
                tip: Faker.fake_with_rng(rng),
                paymaster_data: vec![Faker.fake_with_rng(rng)], // TODO p2p allows 1 elem only

                sender_address: Faker.fake_with_rng(rng),
                signature: Faker.fake_with_rng(rng),
                calldata: Faker.fake_with_rng(rng),
                account_deployment_data: vec![Faker.fake_with_rng(rng)], /* TODO p2p allows 1
                                                                          * elem only */
                proof_facts: vec![Faker.fake_with_rng(rng)],
            }
        }
    }

    /// Represents deserialized L2 "L1 handler" transaction data.
    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    pub struct L1HandlerTransactionV0 {
        pub contract_address: MinimalFelt,
        pub entry_point_selector: MinimalFelt,
        pub nonce: MinimalFelt,
        pub calldata: Vec<MinimalFelt>,
    }

    impl<T> Dummy<T> for L1HandlerTransactionV0 {
        fn dummy_with_rng<R: rand::Rng + ?Sized>(_: &T, rng: &mut R) -> Self {
            Self {
                contract_address: Faker.fake_with_rng(rng),
                entry_point_selector: Faker.fake_with_rng(rng),
                nonce: Faker.fake_with_rng(rng),
                calldata: Faker.fake_with_rng(rng),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use pathfinder_common::macro_prelude::*;
    use pathfinder_common::transaction::*;
    use pathfinder_common::{BlockHeader, ResourceAmount, ResourcePricePerUnit, TransactionIndex};

    use super::*;

    #[test]
    fn serialize_deserialize_transaction() {
        let transaction = pathfinder_common::transaction::Transaction {
            hash: transaction_hash_bytes!(b"pending tx hash 1"),
            variant: TransactionVariant::DeployV1(DeployTransactionV1 {
                contract_address: contract_address!("0x1122355"),
                contract_address_salt: contract_address_salt_bytes!(b"salty"),
                class_hash: class_hash_bytes!(b"pending class hash 1"),
                ..Default::default()
            }),
        };
        let dto = dto::TransactionV3::from(&transaction);
        let serialized = bincode::serde::encode_to_vec(&dto, bincode::config::standard()).unwrap();
        let deserialized: (dto::TransactionV3, _) =
            bincode::serde::decode_from_slice(&serialized, bincode::config::standard()).unwrap();
        assert_eq!(deserialized.0, dto);
    }

    fn setup() -> (
        crate::Connection,
        BlockHeader,
        Vec<(StarknetTransaction, Receipt)>,
    ) {
        let header = BlockHeader::builder().finalize_with_hash(block_hash_bytes!(b"block hash"));

        // Create one of each transaction type.
        let transactions = vec![
            StarknetTransaction {
                hash: transaction_hash_bytes!(b"declare v0 tx hash"),
                variant: TransactionVariant::DeclareV0(DeclareTransactionV0V1 {
                    class_hash: class_hash_bytes!(b"declare v0 class hash"),
                    max_fee: fee_bytes!(b"declare v0 max fee"),
                    nonce: transaction_nonce_bytes!(b"declare v0 tx nonce"),
                    sender_address: contract_address_bytes!(b"declare v0 contract address"),
                    signature: vec![
                        transaction_signature_elem_bytes!(b"declare v0 tx sig 0"),
                        transaction_signature_elem_bytes!(b"declare v0 tx sig 1"),
                    ],
                }),
            },
            StarknetTransaction {
                hash: transaction_hash_bytes!(b"declare v1 tx hash"),
                variant: TransactionVariant::DeclareV1(DeclareTransactionV0V1 {
                    class_hash: class_hash_bytes!(b"declare v1 class hash"),
                    max_fee: fee_bytes!(b"declare v1 max fee"),
                    nonce: transaction_nonce_bytes!(b"declare v1 tx nonce"),
                    sender_address: contract_address_bytes!(b"declare v1 contract address"),
                    signature: vec![
                        transaction_signature_elem_bytes!(b"declare v1 tx sig 0"),
                        transaction_signature_elem_bytes!(b"declare v1 tx sig 1"),
                    ],
                }),
            },
            StarknetTransaction {
                hash: transaction_hash_bytes!(b"declare v2 tx hash"),
                variant: TransactionVariant::DeclareV2(DeclareTransactionV2 {
                    class_hash: class_hash_bytes!(b"declare v2 class hash"),
                    max_fee: fee_bytes!(b"declare v2 max fee"),
                    nonce: transaction_nonce_bytes!(b"declare v2 tx nonce"),
                    sender_address: contract_address_bytes!(b"declare v2 contract address"),
                    signature: vec![
                        transaction_signature_elem_bytes!(b"declare v2 tx sig 0"),
                        transaction_signature_elem_bytes!(b"declare v2 tx sig 1"),
                    ],
                    compiled_class_hash: casm_hash_bytes!(b"declare v2 casm hash"),
                }),
            },
            StarknetTransaction {
                hash: transaction_hash_bytes!(b"deploy tx hash"),
                variant: TransactionVariant::DeployV0(DeployTransactionV0 {
                    contract_address: contract_address_bytes!(b"deploy contract address"),
                    contract_address_salt: contract_address_salt_bytes!(
                        b"deploy contract address salt"
                    ),
                    class_hash: class_hash_bytes!(b"deploy class hash"),
                    constructor_calldata: vec![
                        constructor_param_bytes!(b"deploy call data 0"),
                        constructor_param_bytes!(b"deploy call data 1"),
                    ],
                }),
            },
            StarknetTransaction {
                hash: transaction_hash_bytes!(b"deploy account tx hash"),
                variant: TransactionVariant::DeployAccountV1(DeployAccountTransactionV1 {
                    contract_address: contract_address_bytes!(b"deploy account contract address"),
                    max_fee: fee_bytes!(b"deploy account max fee"),
                    signature: vec![
                        transaction_signature_elem_bytes!(b"deploy account tx sig 0"),
                        transaction_signature_elem_bytes!(b"deploy account tx sig 1"),
                    ],
                    nonce: transaction_nonce_bytes!(b"deploy account tx nonce"),
                    contract_address_salt: contract_address_salt_bytes!(
                        b"deploy account address salt"
                    ),
                    constructor_calldata: vec![
                        call_param_bytes!(b"deploy account call data 0"),
                        call_param_bytes!(b"deploy account call data 1"),
                    ],
                    class_hash: class_hash_bytes!(b"deploy account class hash"),
                }),
            },
            StarknetTransaction {
                hash: transaction_hash_bytes!(b"invoke v0 tx hash"),
                variant: TransactionVariant::InvokeV0(InvokeTransactionV0 {
                    calldata: vec![
                        call_param_bytes!(b"invoke v0 call data 0"),
                        call_param_bytes!(b"invoke v0 call data 1"),
                    ],
                    sender_address: contract_address_bytes!(b"invoke v0 contract address"),
                    entry_point_selector: entry_point_bytes!(b"invoke v0 entry point"),
                    entry_point_type: None,
                    max_fee: fee_bytes!(b"invoke v0 max fee"),
                    signature: vec![
                        transaction_signature_elem_bytes!(b"invoke v0 tx sig 0"),
                        transaction_signature_elem_bytes!(b"invoke v0 tx sig 1"),
                    ],
                }),
            },
            StarknetTransaction {
                hash: transaction_hash_bytes!(b"invoke v1 tx hash"),
                variant: TransactionVariant::InvokeV1(InvokeTransactionV1 {
                    calldata: vec![
                        call_param_bytes!(b"invoke v1 call data 0"),
                        call_param_bytes!(b"invoke v1 call data 1"),
                    ],
                    sender_address: contract_address_bytes!(b"invoke v1 contract address"),
                    max_fee: fee_bytes!(b"invoke v1 max fee"),
                    signature: vec![
                        transaction_signature_elem_bytes!(b"invoke v1 tx sig 0"),
                        transaction_signature_elem_bytes!(b"invoke v1 tx sig 1"),
                    ],
                    nonce: transaction_nonce_bytes!(b"invoke v1 tx nonce"),
                }),
            },
            StarknetTransaction {
                hash: transaction_hash_bytes!(b"invoke v3 tx hash"),
                variant: TransactionVariant::InvokeV3(InvokeTransactionV3 {
                    calldata: vec![
                        call_param_bytes!(b"invoke v3 call data 0"),
                        call_param_bytes!(b"invoke v3 call data 1"),
                    ],
                    sender_address: contract_address_bytes!(b"invoke v3 contract address"),
                    signature: vec![
                        transaction_signature_elem_bytes!(b"invoke v3 tx sig 0"),
                        transaction_signature_elem_bytes!(b"invoke v3 tx sig 1"),
                    ],
                    nonce: transaction_nonce_bytes!(b"invoke v3 tx nonce"),
                    nonce_data_availability_mode: DataAvailabilityMode::L1,
                    fee_data_availability_mode: DataAvailabilityMode::L1,
                    resource_bounds: ResourceBounds {
                        l1_gas: ResourceBound {
                            max_amount: ResourceAmount(1),
                            max_price_per_unit: ResourcePricePerUnit(2),
                        },
                        l2_gas: ResourceBound {
                            max_amount: ResourceAmount(0xff),
                            max_price_per_unit: ResourcePricePerUnit(0xeeeee),
                        },
                        l1_data_gas: Some(ResourceBound {
                            max_amount: ResourceAmount(0xee),
                            max_price_per_unit: ResourcePricePerUnit(0xfffff),
                        }),
                    },
                    tip: pathfinder_common::Tip(0xdeadbeefu64),
                    paymaster_data: vec![paymaster_data_elem_bytes!(b"invoke v3 paymaster data 0")],
                    account_deployment_data: vec![account_deployment_data_elem_bytes!(
                        b"invoke v3 account deployment 0"
                    )],
                    proof_facts: vec![
                        proof_fact_elem_bytes!(b"invoke v3 proof fact 0"),
                        proof_fact_elem_bytes!(b"invoke v3 proof fact 1"),
                    ],
                }),
            },
            StarknetTransaction {
                hash: transaction_hash_bytes!(b"L1 handler tx hash"),
                variant: TransactionVariant::L1Handler(L1HandlerTransaction {
                    contract_address: contract_address_bytes!(b"L1 handler contract address"),
                    entry_point_selector: entry_point_bytes!(b"L1 handler entry point"),
                    nonce: transaction_nonce_bytes!(b"L1 handler tx nonce"),
                    calldata: vec![
                        call_param_bytes!(b"L1 handler call data 0"),
                        call_param_bytes!(b"L1 handler call data 1"),
                    ],
                }),
            },
        ];

        // Generate a random receipt for each transaction. Note that these won't make
        // physical sense but its enough for the tests.
        let receipts: Vec<pathfinder_common::receipt::Receipt> = transactions
            .iter()
            .enumerate()
            .map(|(i, t)| Receipt {
                transaction_hash: t.hash,
                transaction_index: TransactionIndex::new_or_panic(i as u64),
                ..Default::default()
            })
            .collect();
        assert_eq!(transactions.len(), receipts.len());

        let body = transactions.into_iter().zip(receipts).collect::<Vec<_>>();

        let mut db = crate::StorageBuilder::in_memory()
            .unwrap()
            .connection()
            .unwrap();
        let db_tx = db.transaction().unwrap();

        db_tx.insert_block_header(&header).unwrap();
        db_tx
            .insert_transaction_data(
                header.number,
                &body,
                Some(&body.iter().map(|(..)| vec![]).collect::<Vec<_>>()),
            )
            .unwrap();

        db_tx.commit().unwrap();

        (db, header, body)
    }

    #[test]
    fn transaction() {
        let (mut db, _, body) = setup();
        let tx = db.transaction().unwrap();

        let (expected, _) = body.first().unwrap().clone();

        let result = tx.transaction(expected.hash).unwrap().unwrap();
        assert_eq!(result, expected);

        let invalid = tx.transaction(transaction_hash_bytes!(b"invalid")).unwrap();
        assert_eq!(invalid, None);
    }

    #[test]
    fn transaction_with_receipt() {
        let (mut db, header, body) = setup();
        let tx = db.transaction().unwrap();

        let (transaction, receipt) = body.first().unwrap().clone();

        let result = tx
            .transaction_with_receipt(transaction.hash)
            .unwrap()
            .unwrap();
        assert_eq!(result.0, transaction);
        assert_eq!(result.1, receipt);
        assert_eq!(result.2, vec![]);
        assert_eq!(result.3, header.number);

        let invalid = tx
            .transaction_with_receipt(transaction_hash_bytes!(b"invalid"))
            .unwrap();
        assert_eq!(invalid, None);
    }

    #[test]
    fn transaction_at_block() {
        let (mut db, header, body) = setup();
        let tx = db.transaction().unwrap();

        let idx = 5;
        let expected = Some(body[idx].0.clone());

        let by_number = tx.transaction_at_block(header.number.into(), idx).unwrap();
        assert_eq!(by_number, expected);
        let by_hash = tx.transaction_at_block(header.hash.into(), idx).unwrap();
        assert_eq!(by_hash, expected);
        let by_latest = tx.transaction_at_block(BlockId::Latest, idx).unwrap();
        assert_eq!(by_latest, expected);

        let invalid_index = tx
            .transaction_at_block(header.number.into(), body.len() + 1)
            .unwrap();
        assert_eq!(invalid_index, None);

        let invalid_index = tx
            .transaction_at_block(BlockNumber::MAX.into(), idx)
            .unwrap();
        assert_eq!(invalid_index, None);
    }

    #[test]
    fn transaction_count() {
        let (mut db, header, body) = setup();
        let tx = db.transaction().unwrap();

        let by_latest = tx.transaction_count(BlockId::Latest).unwrap();
        assert_eq!(by_latest, body.len());
        let by_number = tx.transaction_count(header.number.into()).unwrap();
        assert_eq!(by_number, body.len());
        let by_hash = tx.transaction_count(header.hash.into()).unwrap();
        assert_eq!(by_hash, body.len());
    }

    #[test]
    fn transaction_data_for_block() {
        let (mut db, header, body) = setup();
        let tx = db.transaction().unwrap();

        let expected = Some(
            body.into_iter()
                .map(|(tx, receipt)| (tx, receipt, vec![]))
                .collect(),
        );

        let by_number = tx.transaction_data_for_block(header.number.into()).unwrap();
        assert_eq!(by_number, expected);
        let by_hash = tx.transaction_data_for_block(header.hash.into()).unwrap();
        assert_eq!(by_hash, expected);
        let by_latest = tx.transaction_data_for_block(BlockId::Latest).unwrap();
        assert_eq!(by_latest, expected);

        let invalid_block = tx
            .transaction_data_for_block(BlockNumber::MAX.into())
            .unwrap();
        assert_eq!(invalid_block, None);
    }

    #[test]
    fn transactions_for_block() {
        let (mut db, header, body) = setup();
        let tx = db.transaction().unwrap();

        let expected = Some(body.into_iter().map(|(t, _)| t).collect::<Vec<_>>());

        let by_number = tx.transactions_for_block(header.number.into()).unwrap();
        assert_eq!(by_number, expected);
        let by_hash = tx.transactions_for_block(header.hash.into()).unwrap();
        assert_eq!(by_hash, expected);
        let by_latest = tx.transactions_for_block(BlockId::Latest).unwrap();
        assert_eq!(by_latest, expected);

        let invalid_block = tx
            .transaction_data_for_block(BlockNumber::MAX.into())
            .unwrap();
        assert_eq!(invalid_block, None);

        let invalid_block = tx
            .transaction_data_for_block(block_hash!("0x123").into())
            .unwrap();
        assert_eq!(invalid_block, None);
    }

    #[test]
    fn transaction_hashes_for_block() {
        let (mut db, header, body) = setup();
        let tx = db.transaction().unwrap();

        let expected = Some(
            body.iter()
                .map(|(transaction, _)| transaction.hash)
                .collect(),
        );

        let by_number = tx
            .transaction_hashes_for_block(header.number.into())
            .unwrap();
        assert_eq!(by_number, expected);
        let by_hash = tx.transaction_hashes_for_block(header.hash.into()).unwrap();
        assert_eq!(by_hash, expected);
        let by_latest = tx.transaction_hashes_for_block(BlockId::Latest).unwrap();
        assert_eq!(by_latest, expected);

        let invalid_block = tx
            .transaction_hashes_for_block(BlockNumber::MAX.into())
            .unwrap();
        assert_eq!(invalid_block, None);
    }

    #[test]
    fn transaction_block_hash() {
        let (mut db, header, body) = setup();
        let tx = db.transaction().unwrap();

        let target = body.first().unwrap().0.hash;
        let valid = tx.transaction_block_hash(target).unwrap().unwrap();
        assert_eq!(valid, header.hash);

        let invalid = tx
            .transaction_block_hash(transaction_hash_bytes!(b"invalid hash"))
            .unwrap();
        assert_eq!(invalid, None);
    }

    #[test]
    fn delete_transactions_before() {
        let (mut db, header, body) = setup();
        let tx = db.transaction().unwrap();

        let target = body.first().unwrap().0.hash;
        let result = tx.transaction(target).unwrap().unwrap();
        assert_eq!(result, body.first().unwrap().0);

        // Add 1 because transactions belong to `header`.
        tx.delete_transactions_before(header.number + 1).unwrap();

        let invalid = tx.transaction(target).unwrap();
        assert_eq!(invalid, None);
    }

    #[test]
    fn delete_transaction_hashes_before() {
        let (mut db, header, body) = setup();
        let tx = db.transaction().unwrap();

        let target = body.first().unwrap().0.hash;
        let result = tx.transaction(target).unwrap().unwrap();
        assert_eq!(result, body.first().unwrap().0);

        // Add 1 because transactions belong to `header`.
        tx.delete_transaction_hashes_before(header.number + 1)
            .unwrap();

        let invalid = tx.transaction(target).unwrap();
        assert_eq!(invalid, None);
    }
}
