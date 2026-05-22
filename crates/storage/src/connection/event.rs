use std::num::NonZeroUsize;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use pathfinder_common::event::{Event, EventIndex};
use pathfinder_common::prelude::*;
use pathfinder_common::BlockId;
use rusqlite::types::Value;

use crate::bloom::{AggregateBloom, BlockRange, BloomFilter};
use crate::prelude::*;
use crate::AGGREGATE_BLOOM_BLOCK_RANGE_LEN;

// We're using the upper 4 bits of the 32 byte representation of a felt
// to store the index of the key in the values set in the Bloom filter.
// This allows for the maximum of 16 keys per event to be stored in the
// filter.
pub const EVENT_KEY_FILTER_LIMIT: usize = 16;
pub const PAGE_SIZE_LIMIT: usize = 1_024;

#[derive(Debug, Default)]
pub struct EventConstraints {
    pub from_block: Option<BlockNumber>,
    pub to_block: Option<BlockNumber>,
    pub contract_addresses: Vec<ContractAddress>,
    pub keys: Vec<Vec<EventKey>>,
    pub page_size: usize,
    pub offset: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmittedEvent {
    pub from_address: ContractAddress,
    pub data: Vec<EventData>,
    pub keys: Vec<EventKey>,
    pub block_hash: BlockHash,
    pub block_number: BlockNumber,
    pub transaction_hash: TransactionHash,
    pub transaction_index: TransactionIndex,
    pub event_index: EventIndex,
}

#[derive(Debug, thiserror::Error)]
pub enum EventFilterError {
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
    #[error("requested page size is too small, supported minimum is 1")]
    PageSizeTooSmall,
}

impl From<rusqlite::Error> for EventFilterError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Internal(error.into())
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct ContinuationToken {
    pub block_number: BlockNumber,
    pub offset: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PageOfEvents {
    pub events: Vec<EmittedEvent>,
    pub continuation_token: Option<ContinuationToken>,
}

impl Transaction<'_> {
    pub fn store_running_event_filter(self) -> anyhow::Result<Self> {
        let running_event_filter = self.running_event_filter.lock().unwrap();

        self.inner().execute(
            r"
            UPDATE running_event_filter
            SET from_block = ?, to_block = ?, bitmap = ?, next_block = ?
            WHERE id = 1
            ",
            params![
                &running_event_filter.filter.from_block,
                &running_event_filter.filter.to_block,
                &running_event_filter.filter.compress_bitmap(),
                &running_event_filter.next_block,
            ],
        )?;

        drop(running_event_filter);

        Ok(self)
    }

    pub fn rebuild_running_event_filter(&self, head: BlockNumber) -> anyhow::Result<()> {
        let rebuilt = RunningEventFilter::rebuild(self, head)?;

        let mut running = self.running_event_filter.lock().unwrap();
        *running = rebuilt;

        Ok(())
    }

    /// Upsert the [running event Bloom filter](RunningEventFilter) for the
    /// given block number. This function operates under the assumption that
    /// blocks are _never_ skipped so even if there are no events for a
    /// block, this function should still be called with an empty iterator.
    /// When testing it is fine to skip blocks, as long as the block at the end
    /// of the current range is not skipped.
    pub(super) fn upsert_block_event_filters<'a>(
        &self,
        block_number: BlockNumber,
        events: impl Iterator<Item = &'a Event>,
    ) -> anyhow::Result<()> {
        let mut insert_stmt = self.inner().prepare_cached(
            r"
            INSERT INTO event_filters
            (from_block, to_block, bitmap)
            VALUES (?, ?, ?)
            ON CONFLICT DO UPDATE SET bitmap=excluded.bitmap
            ",
        )?;

        let mut running_event_filter = self.running_event_filter.lock().unwrap();

        let mut bloom = BloomFilter::new();
        for event in events {
            bloom.set_keys(&event.keys);
            bloom.set_address(&event.from_address);
        }

        running_event_filter.filter.insert(bloom, block_number);
        running_event_filter.next_block = block_number + 1;

        // This check is the reason that blocks cannot be skipped, if they were we would
        // risk missing the last block of the running event filter's range.
        if block_number == running_event_filter.filter.to_block {
            insert_stmt.execute(params![
                &running_event_filter.filter.from_block,
                &running_event_filter.filter.to_block,
                &running_event_filter.filter.compress_bitmap()
            ])?;

            *running_event_filter = RunningEventFilter {
                filter: AggregateBloom::new(block_number + 1),
                next_block: block_number + 1,
            };
        }

        Ok(())
    }

    /// Return all of the events in the given block range, filtered by the given
    /// keys and contract address. Along with the events, return the last
    /// block number that was scanned, which may be smaller than `to_block`
    /// if there are no more blocks in the database.
    pub fn events_in_range(
        &self,
        from_block: BlockNumber,
        to_block: BlockNumber,
        contract_addresses: Vec<ContractAddress>,
        mut keys: Vec<Vec<EventKey>>,
    ) -> anyhow::Result<(Vec<EmittedEvent>, Option<BlockNumber>)> {
        let Some(latest_block) = self.block_number(BlockId::Latest)? else {
            // No blocks in the database.
            return Ok((vec![], None));
        };
        if from_block > latest_block {
            return Ok((vec![], None));
        }
        let to_block = std::cmp::min(to_block, latest_block);

        // Truncate empty key lists from the end of the key filter.
        if let Some(last_non_empty) = keys.iter().rposition(|keys| !keys.is_empty()) {
            keys.truncate(last_non_empty + 1);
        }

        let constraints = EventConstraints {
            contract_addresses,
            keys,
            page_size: usize::MAX - 1,
            ..Default::default()
        };

        let event_filters = self.load_event_filter_range(from_block, to_block)?;

        let blocks_to_scan = event_filters
            .iter()
            .flat_map(|filter| filter.check(&constraints))
            .filter(|&block| (from_block..=to_block).contains(&block));

        let no_key_constraints = constraints.keys.iter().flatten().count() == 0;
        let keys: Vec<std::collections::HashSet<_>> = constraints
            .keys
            .iter()
            .map(|keys| keys.iter().collect())
            .collect();

        let mut emitted_events = vec![];

        for block in blocks_to_scan {
            let Some(block_header) = self.block_header(BlockId::Number(block))? else {
                break;
            };

            let events = match self.events_for_block(block.into())? {
                Some(events) => events,
                // Reached the end of P2P (checkpoint) synced events.
                None => break,
            };

            let events = events
                .into_iter()
                .flat_map(|(tx_info, events)| {
                    events
                        .into_iter()
                        .zip(std::iter::repeat(tx_info).enumerate())
                })
                .filter(|(event, _)| {
                    if constraints.contract_addresses.is_empty() {
                        true
                    } else {
                        constraints.contract_addresses.contains(&event.from_address)
                    }
                })
                .filter(|(event, _)| {
                    if no_key_constraints {
                        return true;
                    }

                    if event.keys.len() < keys.len() {
                        return false;
                    }

                    event
                        .keys
                        .iter()
                        .zip(keys.iter())
                        .all(|(key, filter)| filter.is_empty() || filter.contains(key))
                })
                .map(|(event, (idx, tx_info))| EmittedEvent {
                    data: event.data.clone(),
                    keys: event.keys.clone(),
                    from_address: event.from_address,
                    block_hash: block_header.hash,
                    block_number: block_header.number,
                    transaction_hash: tx_info.0,
                    transaction_index: tx_info.1,
                    event_index: EventIndex(idx as u64),
                });

            emitted_events.extend(events);
        }

        Ok((emitted_events, Some(to_block)))
    }

    #[tracing::instrument(skip(self))]
    pub fn events(
        &self,
        constraints: &EventConstraints,
        block_range_limit: NonZeroUsize,
    ) -> Result<PageOfEvents, EventFilterError> {
        if constraints.page_size < 1 {
            return Err(EventFilterError::PageSizeTooSmall);
        }

        let Some(latest_block) = self.block_number(BlockId::Latest)? else {
            // No blocks in the database.
            return Ok(PageOfEvents {
                events: vec![],
                continuation_token: None,
            });
        };
        let block_range_limit: u64 = block_range_limit
            .get()
            .try_into()
            .expect("Conversion error");

        let from_block = constraints
            .from_block
            .map(|from_block| {
                if self.blockchain_pruning_enabled() {
                    self.earliest_block_number()
                        .context("Fetching earliest block in database")
                        .transpose()
                        .expect("There should be blocks in the database")
                } else {
                    Ok(from_block)
                }
            })
            .transpose()?
            .unwrap_or(BlockNumber::GENESIS);
        // The -1 is needed since `from_block` also counts as one block.
        let max_to_block = from_block + block_range_limit - 1;
        let to_block = constraints
            .to_block
            // Can't go beyond latest block.
            .map(|to_block| std::cmp::min(to_block, latest_block))
            .unwrap_or(latest_block);
        // Can't exceed `block_range_limit`.
        let to_block_limited = std::cmp::min(to_block, max_to_block);

        let event_filters = self.load_event_filter_range(from_block, to_block_limited)?;

        let last_covered_block = std::cmp::min(
            to_block_limited,
            event_filters
                .last()
                .expect("At least one filter is present")
                .to_block,
        );
        let add_continuation_token = to_block > last_covered_block;
        let blocks_to_scan = event_filters
            .iter()
            .flat_map(|filter| filter.check(constraints))
            .filter(|&block| (from_block..=last_covered_block).contains(&block));

        let keys: Vec<std::collections::HashSet<_>> = constraints
            .keys
            .iter()
            .map(|keys| keys.iter().collect())
            .collect();

        let no_key_constraints = constraints.keys.iter().flatten().count() == 0;
        let mut offset = constraints.offset;

        let mut emitted_events = vec![];

        for block in blocks_to_scan {
            let events_required = constraints.page_size + 1 - emitted_events.len();
            tracing::trace!(%block, %events_required, "Processing block");

            let block_header = self
                .block_header(BlockId::Number(block))?
                .expect("Only existing blocks should be scanned");

            let events = match self.events_for_block(block.into())? {
                Some(events) => events,
                // Reached the end of P2P (checkpoint) synced events.
                None => {
                    return Ok(PageOfEvents {
                        events: emitted_events,
                        continuation_token: None,
                    })
                }
            };

            let events = events
                .into_iter()
                .flat_map(|(tx_info, events)| {
                    events
                        .into_iter()
                        .zip(std::iter::repeat(tx_info).enumerate())
                })
                .filter(|(event, _)| {
                    if constraints.contract_addresses.is_empty() {
                        true
                    } else {
                        constraints.contract_addresses.contains(&event.from_address)
                    }
                })
                .filter(|(event, _)| {
                    if no_key_constraints {
                        return true;
                    }

                    if event.keys.len() < keys.len() {
                        return false;
                    }

                    event
                        .keys
                        .iter()
                        .zip(keys.iter())
                        .all(|(key, filter)| filter.is_empty() || filter.contains(key))
                })
                .skip_while(|_| {
                    let should_skip = offset > 0;
                    offset = offset.saturating_sub(1);
                    should_skip
                })
                .take(events_required)
                .map(|(event, (idx, tx_info))| EmittedEvent {
                    data: event.data.clone(),
                    keys: event.keys.clone(),
                    from_address: event.from_address,
                    block_hash: block_header.hash,
                    block_number: block_header.number,
                    transaction_hash: tx_info.0,
                    transaction_index: tx_info.1,
                    event_index: EventIndex(idx as u64),
                });

            emitted_events.extend(events);

            // Stop if we have a page of events plus an extra one to decide if we're on
            // the last page.
            if emitted_events.len() > constraints.page_size {
                let continuation_token = continuation_token(
                    &emitted_events,
                    ContinuationToken {
                        block_number: from_block,
                        offset: constraints.offset,
                    },
                )
                .unwrap();

                emitted_events.truncate(constraints.page_size);

                return Ok(PageOfEvents {
                    events: emitted_events,
                    continuation_token: Some(ContinuationToken {
                        block_number: continuation_token.block_number,
                        // Account for the extra event.
                        offset: continuation_token.offset - 1,
                    }),
                });
            }
        }

        if add_continuation_token {
            Ok(PageOfEvents {
                events: emitted_events,
                continuation_token: Some(ContinuationToken {
                    // Event filter block range is inclusive so + 1.
                    block_number: last_covered_block + 1,
                    offset: 0,
                }),
            })
        } else {
            Ok(PageOfEvents {
                events: emitted_events,
                continuation_token: None,
            })
        }
    }

    /// Load the event bloom filters (either from the cache or the database) for
    /// the given block range with an optional database load limit.  
    fn load_event_filter_range(
        &self,
        start_block: BlockNumber,
        end_block: BlockNumber,
    ) -> anyhow::Result<Vec<Arc<AggregateBloom>>> {
        let cached_filters = self.event_filter_cache.get_many(start_block, end_block);
        let cached_filters_rarray = Rc::new(
            cached_filters
                .iter()
                // The `event_filters` table has a unique constraint over (from_block, to_block)
                // pairs but tuples cannot be used here. Technically, both columns individually
                // _should_ also have unique elements.
                .map(|filter| i64::try_from(filter.from_block.get()).unwrap())
                .map(Value::from)
                .collect::<Vec<Value>>(),
        );

        let mut load_stmt = self.inner().prepare_cached(
            r"
            SELECT from_block, to_block, bitmap
            FROM event_filters
            WHERE from_block <= :end_block AND to_block >= :start_block
            AND from_block NOT IN rarray(:cached_filters)
            ORDER BY from_block
            ",
        )?;

        let mut event_filters = load_stmt
            .query_map(
                // Cannot use crate::params::named_params![] here because of the rarray.
                rusqlite::named_params![
                    ":end_block": &end_block.get(),
                    ":start_block": &start_block.get(),
                    ":cached_filters": &cached_filters_rarray,
                ],
                |row| {
                    let from_block = row.get_block_number(0)?;
                    let to_block = row.get_block_number(1)?;
                    let compressed_bitmap: Vec<u8> = row.get(2)?;

                    Ok(Arc::new(AggregateBloom::from_existing_compressed(
                        from_block,
                        to_block,
                        compressed_bitmap,
                    )))
                },
            )
            .context("Querying event filter range")?
            .collect::<Result<Vec<_>, _>>()?;

        self.event_filter_cache.set_many(&event_filters);
        event_filters.extend(cached_filters);
        event_filters.sort_by_key(|filter| filter.from_block);

        let running_event_filter = self.running_event_filter.lock().unwrap();

        // There are no event filters in the database yet or the loaded ones
        // don't cover the requested range.
        let should_include_running = event_filters.last().is_none_or(|last| {
            last.to_block + 1 == running_event_filter.filter.from_block && end_block > last.to_block
        });

        if should_include_running {
            event_filters.push(Arc::new(running_event_filter.filter.clone()));
        }

        Ok(event_filters)
    }

    /// Returns the next block number whose events are missing from storage.
    pub fn next_block_without_events(&self) -> BlockNumber {
        self.running_event_filter.lock().unwrap().next_block
    }

    #[cfg(feature = "small_aggregate_filters")]
    pub fn event_filter_exists(
        &self,
        from_block: BlockNumber,
        to_block: BlockNumber,
    ) -> anyhow::Result<bool> {
        self.inner()
            .query_row(
                r"
                SELECT EXISTS (
                    SELECT 1
                    FROM event_filters
                    WHERE from_block = ? AND to_block = ?
                )
                ",
                params![&from_block, &to_block],
                |row| row.get(0),
            )
            .map_err(|e| e.into())
    }
}

impl AggregateBloom {
    /// Returns the block numbers that match the given constraints.
    pub fn check(&self, constraints: &EventConstraints) -> Vec<BlockNumber> {
        let addr_blocks = self.check_addresses(&constraints.contract_addresses);
        let keys_blocks = self.check_keys(&constraints.keys);

        let block_matches = addr_blocks & keys_blocks;

        block_matches
            .iter_ones()
            .map(|offset| self.from_block + offset as u64)
            .collect()
    }

    fn check_addresses(&self, addresses: &[ContractAddress]) -> BlockRange {
        if addresses.is_empty() {
            BlockRange::FULL
        } else {
            let contracts: Vec<pathfinder_crypto::Felt> =
                addresses.iter().map(|addr| addr.0).collect();
            self.blocks_for_keys(&contracts)
        }
    }

    fn check_keys(&self, keys: &[Vec<EventKey>]) -> BlockRange {
        if keys.is_empty() {
            return BlockRange::FULL;
        }

        let mut result = BlockRange::FULL;

        for (idx, key_group) in keys.iter().enumerate() {
            if key_group.is_empty() {
                continue; // "any key at this position" — no filtering possible
            }

            let indexed_keys: Vec<_> = key_group
                .iter()
                .map(|key| {
                    let mut key_with_idx = key.0;
                    key_with_idx.as_mut_be_bytes()[0] |= (idx as u8) << 4;
                    key_with_idx
                })
                .collect();

            let blocks_for_key = self.blocks_for_keys(&indexed_keys);

            // No point to continue AND operations with an empty range.
            if blocks_for_key == BlockRange::EMPTY {
                return BlockRange::EMPTY;
            }

            result &= blocks_for_key;
        }

        result
    }
}

impl BloomFilter {
    pub fn set_address(&mut self, address: &ContractAddress) {
        self.set(&address.0);
    }

    pub fn set_keys(&mut self, keys: &[EventKey]) {
        for (i, key) in keys.iter().take(EVENT_KEY_FILTER_LIMIT).enumerate() {
            let mut key = key.0;
            key.as_mut_be_bytes()[0] |= (i as u8) << 4;
            self.set(&key);
        }
    }
}

/// An [AggregateBloom] filter that is currently being constructed and will be
/// stored into the DB once the chain head goes past its range.
pub(crate) struct RunningEventFilter {
    pub(crate) filter: AggregateBloom,
    pub(crate) next_block: BlockNumber,
}

impl RunningEventFilter {
    /// Load the [running event filter](RunningEventFilter) from the database if
    /// it was stored during graceful shutdown. Otherwise, rebuild it from
    /// events.
    pub(crate) fn load(tx: &super::Transaction<'_>) -> anyhow::Result<Self> {
        let Some(latest) = tx
            .inner()
            .query_row(
                "SELECT number FROM block_headers ORDER BY number DESC LIMIT 1",
                [],
                |row| row.get_block_number(0),
            )
            .optional()?
        else {
            // No blocks in the database, create an event filter starting from the Genesis
            // block.
            return Ok(Self {
                filter: AggregateBloom::new(BlockNumber::GENESIS),
                next_block: BlockNumber::GENESIS,
            });
        };

        let (filter, next_block) = tx
            .inner()
            .query_row(
                r"
                SELECT from_block, to_block, bitmap, next_block
                FROM running_event_filter
                WHERE id = 1
                ",
                [],
                |row| {
                    let from_block = row.get_block_number(0)?;
                    let to_block = row.get_block_number(1)?;
                    let compressed_bitmap: Vec<u8> = row.get(2)?;
                    let next_block = row.get_block_number(3)?;

                    let filter = AggregateBloom::from_existing_compressed(
                        from_block,
                        to_block,
                        compressed_bitmap,
                    );

                    Ok((filter, next_block))
                },
            )
            .context("Querying running event filter")?;

        // Check whether the running event filter was stored during graceful shutdown.
        let running_event_filter = if next_block == latest + 1 {
            Self { filter, next_block }
        } else {
            tracing::info!("Running event filter was not stored during last shutdown, rebuilding.");
            Self::rebuild(tx, latest)?
        };

        Ok(running_event_filter)
    }

    /// Rebuild from the SQLite `transactions` table. Used only by historical
    /// migrations (revision_0068) that run before the table is dropped.
    pub(crate) fn rebuild_from_sqlite(
        tx: &rusqlite::Transaction<'_>,
        latest: BlockNumber,
    ) -> anyhow::Result<Self> {
        use super::transaction;

        let mut last_to_block_stmt = tx.prepare(
            r"
            SELECT to_block
            FROM event_filters
            ORDER BY from_block DESC LIMIT 1
            ",
        )?;
        let mut load_events_stmt = tx.prepare(
            r"
            SELECT block_number, events
            FROM transactions
            WHERE block_number >= :first_running_event_filter_block
            ",
        )?;

        let last_to_block = last_to_block_stmt
            .query_row([], |row| row.get::<_, u64>(0))
            .optional()
            .context("Querying last stored event filter to_block")?;

        let first_running_event_filter_block = match last_to_block {
            Some(last_to_block) if last_to_block == latest.get() => {
                let next_block = latest + 1;

                return Ok(RunningEventFilter {
                    filter: AggregateBloom::new(next_block),
                    next_block,
                });
            }
            Some(last_to_block) => BlockNumber::new_or_panic(last_to_block + 1),
            None => latest
                .get()
                .checked_sub(latest.get() % AGGREGATE_BLOOM_BLOCK_RANGE_LEN)
                .map(BlockNumber::new_or_panic)
                .unwrap_or(BlockNumber::GENESIS),
        };

        let total_blocks_to_cover = latest.get() - first_running_event_filter_block.get();
        let mut covered_blocks = 0;
        let mut last_progress_report = Instant::now();

        tracing::trace!(
            "Rebuilding running event filter: 0.00% (0/{}) blocks covered",
            total_blocks_to_cover
        );
        let rebuilt_filters: Vec<Option<(BlockNumber, BloomFilter)>> = load_events_stmt
            .query_and_then(
                named_params![
                    ":first_running_event_filter_block": &first_running_event_filter_block
                ],
                |row| {
                    if last_progress_report.elapsed().as_secs() >= 3 {
                        tracing::trace!(
                            "Rebuilding running event filter: {:.2}% ({}/{}) blocks covered",
                            covered_blocks as f64 / total_blocks_to_cover.max(1) as f64 * 100.0,
                            covered_blocks,
                            total_blocks_to_cover
                        );
                        last_progress_report = Instant::now();
                    }

                    covered_blocks += 1;

                    let block_number = row.get_block_number(0)?;
                    let events = row
                        .get_optional_blob(1)?
                        .map(|events_blob| -> anyhow::Result<_> {
                            let events = transaction::compression::decompress_events(events_blob)
                                .context("Decompressing events")?;
                            let events: transaction::dto::EventsForBlock =
                                bincode::serde::decode_from_slice(
                                    &events,
                                    bincode::config::standard(),
                                )
                                .context("Deserializing events")?
                                .0;

                            Ok(events)
                        })
                        .transpose()?
                        .map(|efb| {
                            efb.events()
                                .into_iter()
                                .flatten()
                                .map(Event::from)
                                .collect::<Vec<_>>()
                        });
                    let Some(events) = events else {
                        return Ok(None);
                    };

                    let mut bloom = BloomFilter::new();
                    for event in events {
                        bloom.set_keys(&event.keys);
                        bloom.set_address(&event.from_address);
                    }

                    Ok(Some((block_number, bloom)))
                },
            )
            .context("Querying events to rebuild")?
            .collect::<anyhow::Result<_>>()?;
        tracing::trace!(
            "Rebuilding running event filter: 100.00% ({total}/{total}) blocks covered",
            total = total_blocks_to_cover,
        );

        let mut filter = AggregateBloom::new(first_running_event_filter_block);

        for block_bloom_filter in rebuilt_filters {
            let Some((block_number, bloom)) = block_bloom_filter else {
                break;
            };

            filter.insert(bloom, block_number);
        }

        Ok(Self {
            filter,
            next_block: latest + 1,
        })
    }

    /// Rebuild the [event filter](RunningEventFilter) from the RocksDB
    /// EVENTS_COLUMN. This is the primary path after the SQLite `transactions`
    /// table has been dropped.
    pub(crate) fn rebuild(
        tx: &super::Transaction<'_>,
        latest: BlockNumber,
    ) -> anyhow::Result<Self> {
        let mut last_to_block_stmt = tx.inner().prepare(
            r"
            SELECT to_block
            FROM event_filters
            ORDER BY from_block DESC LIMIT 1
            ",
        )?;

        let last_to_block = last_to_block_stmt
            .query_row([], |row| row.get::<_, u64>(0))
            .optional()
            .context("Querying last stored event filter to_block")?;

        let first_running_event_filter_block = match last_to_block {
            Some(last_to_block) if last_to_block == latest.get() => {
                let next_block = latest + 1;
                return Ok(RunningEventFilter {
                    filter: AggregateBloom::new(next_block),
                    next_block,
                });
            }
            Some(last_to_block) => BlockNumber::new_or_panic(last_to_block + 1),
            None => latest
                .get()
                .checked_sub(latest.get() % AGGREGATE_BLOOM_BLOCK_RANGE_LEN)
                .map(BlockNumber::new_or_panic)
                .unwrap_or(BlockNumber::GENESIS),
        };

        let total_blocks_to_cover = latest.get() - first_running_event_filter_block.get();
        let mut covered_blocks = 0u64;
        let mut last_progress_report = Instant::now();

        tracing::trace!(
            "Rebuilding running event filter: 0.00% (0/{}) blocks covered",
            total_blocks_to_cover
        );

        let events_cf = tx.rocksdb_get_column(&super::transaction::EVENTS_COLUMN);
        let mut iter = tx.rocksdb().raw_iterator_cf(&events_cf);
        iter.seek(first_running_event_filter_block.get().to_be_bytes());

        let mut rebuilt_filters: Vec<(BlockNumber, BloomFilter)> = Vec::new();
        let mut expected_block = first_running_event_filter_block;

        while iter.valid() {
            let key = iter.key().context("Reading events key")?;
            if key.len() != 8 {
                anyhow::bail!("Unexpected EVENTS_COLUMN key length: {}", key.len());
            }
            let block_number = BlockNumber::new_or_panic(u64::from_be_bytes(
                key.try_into().expect("8-byte slice"),
            ));
            if block_number > latest {
                break;
            }

            if block_number > expected_block {
                tracing::warn!(
                    from = %expected_block,
                    to = %(block_number - 1),
                    "Blocks missing from EVENTS_COLUMN during event filter rebuild; \
                     Bloom filter may be incomplete for these blocks"
                );
            }

            if last_progress_report.elapsed().as_secs() >= 3 {
                tracing::trace!(
                    "Rebuilding running event filter: {:.2}% ({}/{}) blocks covered",
                    covered_blocks as f64 / total_blocks_to_cover.max(1) as f64 * 100.0,
                    covered_blocks,
                    total_blocks_to_cover
                );
                last_progress_report = Instant::now();
            }
            covered_blocks += 1;

            let blob = iter.value().context("Reading events blob")?;
            let decompressed = super::transaction::compression::decompress_events(blob)
                .context("Decompressing events")?;
            let (efb, _): (super::transaction::dto::EventsForBlock, _) =
                bincode::serde::decode_from_slice(&decompressed, bincode::config::standard())
                    .context("Deserializing events")?;
            let mut bloom = BloomFilter::new();
            for dto_event in efb.events().into_iter().flatten() {
                let event = Event::from(dto_event);
                bloom.set_keys(&event.keys);
                bloom.set_address(&event.from_address);
            }
            rebuilt_filters.push((block_number, bloom));

            expected_block = block_number + 1;
            iter.next();
        }

        tracing::trace!(
            "Rebuilding running event filter: 100.00% ({total}/{total}) blocks covered",
            total = total_blocks_to_cover,
        );

        let mut filter = AggregateBloom::new(first_running_event_filter_block);

        for (block_number, bloom) in rebuilt_filters {
            filter.insert(bloom, block_number);
        }

        Ok(Self {
            filter,
            next_block: latest + 1,
        })
    }
}

fn continuation_token(
    events: &[EmittedEvent],
    previous_token: ContinuationToken,
) -> Option<ContinuationToken> {
    if events.is_empty() {
        return None;
    }

    let last_block_number = events.last().unwrap().block_number;
    let number_of_events_in_last_block = events
        .iter()
        .rev()
        .take_while(|event| event.block_number == last_block_number)
        .count();

    // Since we're taking the block number of the last block this is at least one.
    assert!(number_of_events_in_last_block >= 1);

    let token = if number_of_events_in_last_block < events.len() {
        // the page contains events from a new block
        ContinuationToken {
            block_number: last_block_number,
            offset: number_of_events_in_last_block,
        }
    } else {
        // the page contains events from the same block
        ContinuationToken {
            block_number: previous_token.block_number,
            offset: previous_token.offset + events.len(),
        }
    };

    Some(token)
}

#[cfg(test)]
mod tests {
    use std::sync::LazyLock;

    use pathfinder_common::macro_prelude::*;
    use pathfinder_common::receipt::Receipt;
    use pathfinder_common::{transaction as common, BlockHeader, BlockTimestamp, EntryPoint, Fee};
    use pathfinder_crypto::Felt;
    use pretty_assertions_sorted::assert_eq;
    use rstest::rstest;

    use super::*;
    use crate::{test_utils, AGGREGATE_BLOOM_BLOCK_RANGE_LEN};

    static EVENT_FILTER_BLOCK_RANGE_LIMIT: LazyLock<NonZeroUsize> =
        LazyLock::new(|| NonZeroUsize::new(100).unwrap());

    mod event_bloom {
        use pretty_assertions_sorted::assert_eq;

        use super::*;

        #[test]
        fn matching_constraints() {
            let mut aggregate = AggregateBloom::new(BlockNumber::GENESIS);

            let mut filter = BloomFilter::new();
            filter.set_keys(&[event_key!("0xdeadbeef")]);
            filter.set_address(&contract_address!("0x1234"));

            aggregate.insert(filter.clone(), BlockNumber::GENESIS);
            aggregate.insert(filter, BlockNumber::GENESIS + 1);
            let constraints = EventConstraints {
                from_block: None,
                to_block: None,
                contract_addresses: vec![contract_address!("0x1234")],
                keys: vec![vec![event_key!("0xdeadbeef")]],
                page_size: 1024,
                offset: 0,
            };

            assert_eq!(
                aggregate.check(&constraints),
                vec![BlockNumber::GENESIS, BlockNumber::GENESIS + 1]
            );
        }

        #[test]
        fn extra_address() {
            let mut aggregate = AggregateBloom::new(BlockNumber::GENESIS);

            let mut filter = BloomFilter::new();
            filter.set_keys(&[event_key!("0xdeadbeef")]);
            filter.set_address(&contract_address!("0x1234"));

            aggregate.insert(filter.clone(), BlockNumber::GENESIS);
            aggregate.insert(filter, BlockNumber::GENESIS + 1);
            let contract_addresses =
                vec![contract_address!("0x123456"), contract_address!("0x1234")];
            let constraints = EventConstraints {
                from_block: None,
                to_block: None,
                contract_addresses,
                keys: vec![vec![event_key!("0xdeadbeef")]],
                page_size: 1024,
                offset: 0,
            };

            assert_eq!(
                aggregate.check(&constraints),
                vec![BlockNumber::GENESIS, BlockNumber::GENESIS + 1]
            );
        }

        #[test]
        fn correct_key_wrong_address() {
            let mut aggregate = AggregateBloom::new(BlockNumber::GENESIS);

            let mut filter = BloomFilter::new();
            filter.set_keys(&[event_key!("0xdeadbeef")]);
            filter.set_address(&contract_address!("0x1234"));

            aggregate.insert(filter.clone(), BlockNumber::GENESIS);
            aggregate.insert(filter, BlockNumber::GENESIS + 1);
            let constraints = EventConstraints {
                from_block: None,
                to_block: None,
                contract_addresses: vec![contract_address!("0x4321")],
                keys: vec![vec![event_key!("0xdeadbeef")]],
                page_size: 1024,
                offset: 0,
            };

            assert_eq!(aggregate.check(&constraints), Vec::<BlockNumber>::new());
        }

        #[test]
        fn correct_address_wrong_key() {
            let mut aggregate = AggregateBloom::new(BlockNumber::GENESIS);

            let mut filter = BloomFilter::new();
            filter.set_keys(&[event_key!("0xdeadbeef")]);
            filter.set_address(&contract_address!("0x1234"));

            aggregate.insert(filter.clone(), BlockNumber::GENESIS);
            aggregate.insert(filter, BlockNumber::GENESIS + 1);
            let constraints = EventConstraints {
                from_block: None,
                to_block: None,
                contract_addresses: vec![contract_address!("0x1234")],
                keys: vec![vec![event_key!("0xfeebdaed"), event_key!("0x4321")]],
                page_size: 1024,
                offset: 0,
            };

            assert_eq!(aggregate.check(&constraints), Vec::<BlockNumber>::new());
        }

        #[test]
        fn wrong_and_correct_key() {
            let mut aggregate = AggregateBloom::new(BlockNumber::GENESIS);

            let mut filter = BloomFilter::new();
            filter.set_address(&contract_address!("0x1234"));
            filter.set_keys(&[event_key!("0xdeadbeef")]);

            aggregate.insert(filter.clone(), BlockNumber::GENESIS);
            aggregate.insert(filter, BlockNumber::GENESIS + 1);
            let constraints = EventConstraints {
                from_block: None,
                to_block: None,
                contract_addresses: vec![],
                keys: vec![
                    // Key present in both blocks as the first key.
                    vec![event_key!("0xdeadbeef")],
                    // Key that does not exist in any block.
                    vec![event_key!("0xbeefdead")],
                ],
                page_size: 1024,
                offset: 0,
            };

            assert_eq!(aggregate.check(&constraints), Vec::<BlockNumber>::new());
        }

        #[test]
        fn no_constraints() {
            fn all_blocks(bloom: &AggregateBloom) -> Vec<BlockNumber> {
                (bloom.from_block.get()..=bloom.to_block.get())
                    .map(BlockNumber::new_or_panic)
                    .collect()
            }

            let mut aggregate = AggregateBloom::new(BlockNumber::GENESIS);

            let mut filter = BloomFilter::new();
            filter.set_keys(&[event_key!("0xdeadbeef")]);
            filter.set_address(&contract_address!("0x1234"));

            aggregate.insert(filter.clone(), BlockNumber::GENESIS);
            aggregate.insert(filter, BlockNumber::GENESIS + 1);
            let constraints = EventConstraints {
                from_block: None,
                to_block: None,
                contract_addresses: vec![],
                keys: vec![],
                page_size: 1024,
                offset: 0,
            };

            assert_eq!(aggregate.check(&constraints), all_blocks(&aggregate));
        }
    }

    #[test_log::test(test)]
    fn get_events_with_fully_specified_filter() {
        let (storage, test_data) = test_utils::setup_test_storage();
        let emitted_events = test_data.events;
        let mut connection = storage.connection().unwrap();
        let tx = connection.transaction().unwrap();

        let expected_event = &emitted_events[1];
        let constraints = EventConstraints {
            from_block: Some(expected_event.block_number),
            to_block: Some(expected_event.block_number),
            contract_addresses: vec![expected_event.from_address],
            // We're using a key which is present in _all_ events as the 2nd key.
            keys: vec![vec![], vec![event_key!("0xdeadbeef")]],
            page_size: test_utils::NUM_EVENTS,
            offset: 0,
        };

        let events = tx
            .events(&constraints, *EVENT_FILTER_BLOCK_RANGE_LIMIT)
            .unwrap();
        assert_eq!(
            events,
            PageOfEvents {
                events: vec![expected_event.clone()],
                continuation_token: None,
            }
        );
    }

    #[test]
    fn events_are_ordered() {
        // This is a regression test where events were incorrectly ordered by
        // transaction hash instead of transaction index.
        //
        // Events should be ordered by block number, transaction index, event index.

        // All events we are storing, arbitrarily use from_address to distinguish them.
        let expected_events = (0u8..5)
            .map(|idx| Event {
                data: Vec::new(),
                keys: Vec::new(),
                from_address: ContractAddress::new_or_panic(
                    Felt::from_be_slice(&idx.to_be_bytes()).unwrap(),
                ),
            })
            .collect::<Vec<_>>();

        let header = BlockHeader::builder()
            .sequencer_address(sequencer_address!("0x1234"))
            .timestamp(BlockTimestamp::new_or_panic(0))
            .state_commitment(state_commitment!("0x1234"))
            .finalize_with_hash(block_hash!("0x1234"));

        // Note: hashes are reverse ordered to trigger the sorting bug.
        let transactions = &[
            common::Transaction {
                hash: transaction_hash!("0xF"),
                variant: common::TransactionVariant::InvokeV0(common::InvokeTransactionV0 {
                    calldata: vec![],
                    // Only required because event insert rejects if this is None
                    sender_address: ContractAddress::new_or_panic(Felt::ZERO),
                    entry_point_type: Some(common::EntryPointType::External),
                    entry_point_selector: EntryPoint(Felt::ZERO),
                    max_fee: Fee::ZERO,
                    signature: vec![],
                }),
            },
            common::Transaction {
                hash: transaction_hash!("0x1"),
                variant: common::TransactionVariant::InvokeV0(common::InvokeTransactionV0 {
                    calldata: vec![],
                    // Only required because event insert rejects if this is None
                    sender_address: ContractAddress::new_or_panic(Felt::ZERO),
                    entry_point_type: Some(common::EntryPointType::External),
                    entry_point_selector: EntryPoint(Felt::ZERO),
                    max_fee: Fee::ZERO,
                    signature: vec![],
                }),
            },
        ];

        let receipts = &[
            Receipt {
                transaction_hash: transactions[0].hash,
                transaction_index: pathfinder_common::TransactionIndex::new_or_panic(0),
                ..Default::default()
            },
            Receipt {
                transaction_hash: transactions[1].hash,
                transaction_index: pathfinder_common::TransactionIndex::new_or_panic(1),
                ..Default::default()
            },
        ];

        let mut connection = crate::StorageBuilder::in_memory()
            .unwrap()
            .connection()
            .unwrap();
        let tx = connection.transaction().unwrap();

        tx.insert_block_header(&header).unwrap();
        tx.insert_transaction_data(
            header.number,
            &[
                (transactions[0].clone(), receipts[0].clone()),
                (transactions[1].clone(), receipts[1].clone()),
            ],
            Some(&[expected_events[..3].to_vec(), expected_events[3..].to_vec()]),
        )
        .unwrap();
        tx.flush_rocksdb_batch().unwrap();

        let addresses = tx
            .events(
                &EventConstraints {
                    from_block: None,
                    to_block: None,
                    contract_addresses: vec![],
                    keys: vec![],
                    page_size: 1024,
                    offset: 0,
                },
                *EVENT_FILTER_BLOCK_RANGE_LIMIT,
            )
            .unwrap()
            .events
            .iter()
            .map(|e| e.from_address)
            .collect::<Vec<_>>();

        let expected = expected_events
            .iter()
            .map(|e| e.from_address)
            .collect::<Vec<_>>();

        assert_eq!(addresses, expected);
    }

    #[test]
    fn get_events_by_block() {
        let (storage, test_data) = test_utils::setup_test_storage();
        let emitted_events = test_data.events;
        let mut connection = storage.connection().unwrap();
        let tx = connection.transaction().unwrap();

        const BLOCK_NUMBER: usize = 2;
        let constraints = EventConstraints {
            from_block: Some(BlockNumber::new_or_panic(BLOCK_NUMBER as u64)),
            to_block: Some(BlockNumber::new_or_panic(BLOCK_NUMBER as u64)),
            contract_addresses: vec![],
            keys: vec![],
            page_size: test_utils::NUM_EVENTS,
            offset: 0,
        };

        let expected_events = &emitted_events[test_utils::EVENTS_PER_BLOCK * BLOCK_NUMBER
            ..test_utils::EVENTS_PER_BLOCK * (BLOCK_NUMBER + 1)];
        let events = tx
            .events(&constraints, *EVENT_FILTER_BLOCK_RANGE_LIMIT)
            .unwrap();
        assert_eq!(
            events,
            PageOfEvents {
                events: expected_events.to_vec(),
                continuation_token: None,
            }
        );
    }

    #[test]
    fn get_events_up_to_block() {
        let (storage, test_data) = test_utils::setup_test_storage();
        let emitted_events = test_data.events;
        let mut connection = storage.connection().unwrap();
        let tx = connection.transaction().unwrap();

        const UNTIL_BLOCK_NUMBER: usize = 2;
        let constraints = EventConstraints {
            from_block: None,
            to_block: Some(BlockNumber::new_or_panic(UNTIL_BLOCK_NUMBER as u64)),
            contract_addresses: vec![],
            keys: vec![],
            page_size: test_utils::NUM_EVENTS,
            offset: 0,
        };

        let expected_events =
            &emitted_events[..test_utils::EVENTS_PER_BLOCK * (UNTIL_BLOCK_NUMBER + 1)];
        let events = tx
            .events(&constraints, *EVENT_FILTER_BLOCK_RANGE_LIMIT)
            .unwrap();
        assert_eq!(
            events,
            PageOfEvents {
                events: expected_events.to_vec(),
                continuation_token: None,
            }
        );
    }

    #[test]
    fn get_events_up_to_block_with_paging() {
        let (storage, test_data) = test_utils::setup_test_storage();
        let emitted_events = test_data.events;
        let mut connection = storage.connection().unwrap();
        let tx = connection.transaction().unwrap();

        let constraints = EventConstraints {
            from_block: None,
            to_block: Some(BlockNumber::new_or_panic(1)),
            contract_addresses: vec![],
            keys: vec![],
            page_size: test_utils::EVENTS_PER_BLOCK + 1,
            offset: 0,
        };

        let expected_events = &emitted_events[..test_utils::EVENTS_PER_BLOCK + 1];
        let events = tx
            .events(&constraints, *EVENT_FILTER_BLOCK_RANGE_LIMIT)
            .unwrap();
        pretty_assertions_sorted::assert_eq!(
            events,
            PageOfEvents {
                events: expected_events.to_vec(),
                continuation_token: Some(ContinuationToken {
                    block_number: BlockNumber::new_or_panic(1),
                    offset: 1
                }),
            }
        );

        // test continuation token
        let constraints = EventConstraints {
            from_block: Some(events.continuation_token.unwrap().block_number),
            to_block: Some(BlockNumber::new_or_panic(1)),
            contract_addresses: vec![],
            keys: vec![],
            page_size: test_utils::EVENTS_PER_BLOCK + 1,
            offset: events.continuation_token.unwrap().offset,
        };

        let expected_events =
            &emitted_events[test_utils::EVENTS_PER_BLOCK + 1..test_utils::EVENTS_PER_BLOCK * 2];
        let events = tx
            .events(&constraints, *EVENT_FILTER_BLOCK_RANGE_LIMIT)
            .unwrap();
        pretty_assertions_sorted::assert_eq!(
            events,
            PageOfEvents {
                events: expected_events.to_vec(),
                continuation_token: None,
            }
        );
    }

    #[test]
    fn get_events_from_block_onwards() {
        let (storage, test_data) = test_utils::setup_test_storage();
        let emitted_events = test_data.events;
        let mut connection = storage.connection().unwrap();
        let tx = connection.transaction().unwrap();

        const FROM_BLOCK_NUMBER: usize = 2;
        let constraints = EventConstraints {
            from_block: Some(BlockNumber::new_or_panic(FROM_BLOCK_NUMBER as u64)),
            to_block: None,
            contract_addresses: vec![],
            keys: vec![],
            page_size: test_utils::NUM_EVENTS,
            offset: 0,
        };

        let expected_events = &emitted_events[test_utils::EVENTS_PER_BLOCK * FROM_BLOCK_NUMBER..];
        let events = tx
            .events(&constraints, *EVENT_FILTER_BLOCK_RANGE_LIMIT)
            .unwrap();
        assert_eq!(
            events,
            PageOfEvents {
                events: expected_events.to_vec(),
                continuation_token: None,
            }
        );
    }

    #[test]
    fn get_events_from_contract() {
        let (storage, test_data) = test_utils::setup_test_storage();
        let emitted_events = test_data.events;
        let mut connection = storage.connection().unwrap();
        let tx = connection.transaction().unwrap();

        let expected_event = &emitted_events[33];

        let constraints = EventConstraints {
            from_block: None,
            to_block: None,
            contract_addresses: vec![expected_event.from_address],
            keys: vec![],
            page_size: test_utils::NUM_EVENTS,
            offset: 0,
        };

        let events = tx
            .events(&constraints, *EVENT_FILTER_BLOCK_RANGE_LIMIT)
            .unwrap();
        assert_eq!(
            events,
            PageOfEvents {
                events: vec![expected_event.clone()],
                continuation_token: None,
            }
        );
    }

    #[test]
    fn get_events_by_key() {
        let (storage, test_data) = test_utils::setup_test_storage();
        let emitted_events = test_data.events;
        let mut connection = storage.connection().unwrap();
        let tx = connection.transaction().unwrap();

        let expected_event = &emitted_events[27];
        let constraints = EventConstraints {
            from_block: None,
            to_block: None,
            contract_addresses: vec![],
            keys: vec![vec![expected_event.keys[0]], vec![expected_event.keys[1]]],
            page_size: test_utils::NUM_EVENTS,
            offset: 0,
        };

        let events = tx
            .events(&constraints, *EVENT_FILTER_BLOCK_RANGE_LIMIT)
            .unwrap();
        assert_eq!(
            events,
            PageOfEvents {
                events: vec![expected_event.clone()],
                continuation_token: None,
            }
        );

        // try event keys in the wrong order, should not match
        let constraints = EventConstraints {
            keys: vec![vec![expected_event.keys[1]], vec![expected_event.keys[0]]],
            ..constraints
        };

        let events = tx
            .events(&constraints, *EVENT_FILTER_BLOCK_RANGE_LIMIT)
            .unwrap();
        assert_eq!(
            events,
            PageOfEvents {
                events: vec![],
                continuation_token: None,
            }
        );
    }

    #[test]
    fn get_events_with_no_filter() {
        let (storage, test_data) = test_utils::setup_test_storage();
        let emitted_events = test_data.events;
        let mut connection = storage.connection().unwrap();
        let tx = connection.transaction().unwrap();

        let constraints = EventConstraints {
            from_block: None,
            to_block: None,
            contract_addresses: vec![],
            keys: vec![],
            page_size: test_utils::NUM_EVENTS,
            offset: 0,
        };

        let events = tx
            .events(&constraints, *EVENT_FILTER_BLOCK_RANGE_LIMIT)
            .unwrap();
        assert_eq!(
            events,
            PageOfEvents {
                events: emitted_events,
                continuation_token: None,
            }
        );
    }

    #[test]
    fn get_events_with_no_filter_and_paging() {
        let (storage, test_data) = test_utils::setup_test_storage();
        let emitted_events = test_data.events;
        let mut connection = storage.connection().unwrap();
        let tx = connection.transaction().unwrap();

        let constraints = EventConstraints {
            from_block: None,
            to_block: None,
            contract_addresses: vec![],
            keys: vec![],
            page_size: 10,
            offset: 0,
        };

        let events = tx
            .events(&constraints, *EVENT_FILTER_BLOCK_RANGE_LIMIT)
            .unwrap();
        assert_eq!(
            events,
            PageOfEvents {
                events: emitted_events[..10].to_vec(),
                continuation_token: Some(ContinuationToken {
                    block_number: BlockNumber::new_or_panic(1),
                    offset: 0
                }),
            }
        );

        let constraints = EventConstraints {
            from_block: None,
            to_block: None,
            contract_addresses: vec![],
            keys: vec![],
            page_size: 10,
            offset: 10,
        };

        let events = tx
            .events(&constraints, *EVENT_FILTER_BLOCK_RANGE_LIMIT)
            .unwrap();
        assert_eq!(
            events,
            PageOfEvents {
                events: emitted_events[10..20].to_vec(),
                continuation_token: Some(ContinuationToken {
                    block_number: BlockNumber::new_or_panic(2),
                    offset: 0
                }),
            }
        );

        let constraints = EventConstraints {
            from_block: None,
            to_block: None,
            contract_addresses: vec![],
            keys: vec![],
            page_size: 10,
            offset: 30,
        };

        let events = tx
            .events(&constraints, *EVENT_FILTER_BLOCK_RANGE_LIMIT)
            .unwrap();
        assert_eq!(
            events,
            PageOfEvents {
                events: emitted_events[30..40].to_vec(),
                continuation_token: None
            }
        );
    }

    #[test]
    fn get_events_with_no_filter_and_nonexistent_page() {
        let (storage, _) = test_utils::setup_test_storage();
        let mut connection = storage.connection().unwrap();
        let tx = connection.transaction().unwrap();

        const PAGE_SIZE: usize = 10;
        let constraints = EventConstraints {
            from_block: None,
            to_block: None,
            contract_addresses: vec![],
            keys: vec![],
            page_size: PAGE_SIZE,
            // _after_ the last one
            offset: test_utils::NUM_BLOCKS * test_utils::EVENTS_PER_BLOCK,
        };

        let events = tx
            .events(&constraints, *EVENT_FILTER_BLOCK_RANGE_LIMIT)
            .unwrap();
        assert_eq!(
            events,
            PageOfEvents {
                events: vec![],
                continuation_token: None,
            }
        );
    }

    #[test]
    fn get_events_by_key_with_paging() {
        let (storage, test_data) = test_utils::setup_test_storage();
        let emitted_events = test_data.events;
        let mut connection = storage.connection().unwrap();
        let tx = connection.transaction().unwrap();

        let expected_events = &emitted_events[27..32];
        let keys_for_expected_events = vec![
            expected_events.iter().map(|e| e.keys[0]).collect(),
            expected_events.iter().map(|e| e.keys[1]).collect(),
        ];

        let constraints = EventConstraints {
            from_block: None,
            to_block: None,
            contract_addresses: vec![],
            keys: keys_for_expected_events.clone(),
            page_size: 2,
            offset: 0,
        };

        let events = tx
            .events(&constraints, *EVENT_FILTER_BLOCK_RANGE_LIMIT)
            .unwrap();
        assert_eq!(
            events,
            PageOfEvents {
                events: expected_events[..2].to_vec(),
                continuation_token: Some(ContinuationToken {
                    block_number: BlockNumber::new_or_panic(0),
                    offset: 2
                }),
            }
        );

        // increase offset
        let constraints: EventConstraints = EventConstraints {
            from_block: None,
            to_block: None,
            contract_addresses: vec![],
            keys: keys_for_expected_events.clone(),
            page_size: 2,
            offset: 2,
        };

        let events = tx
            .events(&constraints, *EVENT_FILTER_BLOCK_RANGE_LIMIT)
            .unwrap();
        assert_eq!(
            events,
            PageOfEvents {
                events: expected_events[2..4].to_vec(),
                continuation_token: Some(ContinuationToken {
                    block_number: BlockNumber::new_or_panic(3),
                    offset: 1
                }),
            }
        );

        // using the continuation token should be equivalent to the previous query
        let constraints: EventConstraints = EventConstraints {
            from_block: Some(BlockNumber::new_or_panic(0)),
            to_block: None,
            contract_addresses: vec![],
            keys: keys_for_expected_events.clone(),
            page_size: 2,
            offset: 2,
        };

        let events = tx
            .events(&constraints, *EVENT_FILTER_BLOCK_RANGE_LIMIT)
            .unwrap();
        assert_eq!(
            events,
            PageOfEvents {
                events: expected_events[2..4].to_vec(),
                continuation_token: Some(ContinuationToken {
                    block_number: BlockNumber::new_or_panic(3),
                    offset: 1
                }),
            }
        );

        // increase offset by two
        let constraints = EventConstraints {
            from_block: None,
            to_block: None,
            contract_addresses: vec![],
            keys: keys_for_expected_events.clone(),
            page_size: 2,
            offset: 4,
        };

        let events = tx
            .events(&constraints, *EVENT_FILTER_BLOCK_RANGE_LIMIT)
            .unwrap();
        assert_eq!(
            events,
            PageOfEvents {
                events: expected_events[4..].to_vec(),
                continuation_token: None,
            }
        );

        // using the continuation token should be equivalent to the previous query
        let constraints = EventConstraints {
            from_block: Some(BlockNumber::new_or_panic(3)),
            to_block: None,
            contract_addresses: vec![],
            keys: keys_for_expected_events,
            page_size: 2,
            offset: 1,
        };

        let events = tx
            .events(&constraints, *EVENT_FILTER_BLOCK_RANGE_LIMIT)
            .unwrap();
        assert_eq!(
            events,
            PageOfEvents {
                events: expected_events[4..].to_vec(),
                continuation_token: None,
            }
        );
    }

    #[test]
    fn crossing_event_filter_range_stores_and_updates_running() {
        // Two and a half ranges.
        let n_blocks = 2 * AGGREGATE_BLOOM_BLOCK_RANGE_LEN + AGGREGATE_BLOOM_BLOCK_RANGE_LEN / 2;
        let n_blocks = usize::try_from(n_blocks).unwrap();

        let (storage, test_data) = test_utils::setup_custom_test_storage(n_blocks, 1);
        let emitted_events = test_data.events;
        let mut connection = storage.connection().unwrap();
        let tx = connection.transaction().unwrap();

        let inserted_event_filter_count = tx
            .inner()
            .prepare("SELECT COUNT(*) FROM event_filters")
            .unwrap()
            .query_row([], |row| row.get::<_, u64>(0))
            .unwrap();
        assert_eq!(inserted_event_filter_count, 2);

        let running_event_filter = tx.running_event_filter.lock().unwrap();
        // Running event filter starts from next block range.
        assert_eq!(
            running_event_filter.filter.from_block,
            2 * AGGREGATE_BLOOM_BLOCK_RANGE_LEN
        );
        // Lock needed in `events()`.
        drop(running_event_filter);

        let constraints = EventConstraints {
            from_block: None,
            to_block: None,
            contract_addresses: vec![],
            // We're using a key which is present in _all_ events as the 2nd key.
            keys: vec![vec![], vec![event_key!("0xdeadbeef")]],
            page_size: emitted_events.len(),
            offset: 0,
        };

        let events = tx
            .events(&constraints, *EVENT_FILTER_BLOCK_RANGE_LIMIT)
            .unwrap();
        assert_eq!(
            events,
            PageOfEvents {
                events: emitted_events,
                continuation_token: None,
            }
        );
    }

    #[test]
    fn event_filter_filter_load_limit() {
        let n_blocks = 2 * AGGREGATE_BLOOM_BLOCK_RANGE_LEN + AGGREGATE_BLOOM_BLOCK_RANGE_LEN / 2;
        let n_blocks = usize::try_from(n_blocks).unwrap();

        let (storage, test_data) = test_utils::setup_custom_test_storage(n_blocks, 1);
        let emitted_events = test_data.events;
        let events_per_block = emitted_events.len() / n_blocks;

        let mut connection = storage.connection().unwrap();
        let tx = connection.transaction().unwrap();

        let constraints = EventConstraints {
            from_block: None,
            to_block: None,
            contract_addresses: vec![],
            // We're using a key which is present in _all_ events as the 2nd key...
            keys: vec![vec![], vec![event_key!("0xdeadbeef")]],
            page_size: emitted_events.len(),
            offset: 0,
        };

        let events = tx.events(&constraints, 10.try_into().unwrap()).unwrap();

        assert_eq!(
            events,
            PageOfEvents {
                // ...but only events from the first bloom filter range are returned...
                events: emitted_events[..events_per_block * 10].to_vec(),
                // ...with a continuation token pointing to the next block range.
                continuation_token: Some(ContinuationToken {
                    block_number: BlockNumber::new_or_panic(10),
                    offset: 0,
                })
            }
        );

        let constraints_with_offset = EventConstraints {
            // Use the provided continuation token.
            from_block: Some(events.continuation_token.unwrap().block_number),
            to_block: None,
            contract_addresses: vec![],
            // We're using a key which is present in _all_ events as the 2nd key...
            keys: vec![vec![], vec![event_key!("0xdeadbeef")]],
            page_size: emitted_events.len(),
            offset: 0,
        };

        let events = tx
            .events(&constraints_with_offset, *EVENT_FILTER_BLOCK_RANGE_LIMIT)
            .unwrap();
        assert_eq!(
            events,
            PageOfEvents {
                // ...but only events from the second (loaded) and third (running) event filter
                // range are returned...
                events: emitted_events[events_per_block * 10..].to_vec(),
                // ...without a continuation token.
                continuation_token: None,
            }
        );
    }

    #[rustfmt::skip]
    #[rstest]
    #[case(0,  0,  0, 0, 0)] //  0     ..=(N    )
    #[case(0,  0, -1, 0, 0)] //  0     ..=(N - 1)
    #[case(0,  0,  1, 0, 2)] //  0     ..=(N + 1)
    #[case(1,  0,  0, 1, 1)] // (N    )..=(2 * N)
    #[case(1, -2,  0, 0, 1)] // (N - 1)..=(2 * N)
    #[case(1,  1,  0, 1, 1)] // (N + 1)..=(2 * N)
    fn event_filter_edge_cases(
        #[case] range_idx: usize,
        #[case] offset_from: i32,
        #[case] offset_to: i32,
        #[case] range_start_idx: usize,
        #[case] range_end_idx: usize,
    ) {
        use std::collections::BTreeSet;

        fn contained_blocks(page: &PageOfEvents) -> BTreeSet<BlockNumber> {
            page.events
                .iter()
                .map(|event| event.block_number)
                .collect::<BTreeSet<_>>()
        }

        let n_blocks = 2 * AGGREGATE_BLOOM_BLOCK_RANGE_LEN + 10;
        let n_blocks = usize::try_from(n_blocks).unwrap();

        let (storage, test_data) = test_utils::setup_custom_test_storage(n_blocks, 1);
        let emitted_events = test_data.events;

        let mut connection = storage.connection().unwrap();
        let tx = connection.transaction().unwrap();

        let ranges = [
            (
                BlockNumber::GENESIS,
                BlockNumber::GENESIS + AGGREGATE_BLOOM_BLOCK_RANGE_LEN - 1,
            ),
            (
                BlockNumber::GENESIS + AGGREGATE_BLOOM_BLOCK_RANGE_LEN,
                BlockNumber::GENESIS + 2 * AGGREGATE_BLOOM_BLOCK_RANGE_LEN - 1,
            ),
            (
                BlockNumber::GENESIS + 2 * AGGREGATE_BLOOM_BLOCK_RANGE_LEN,
                BlockNumber::GENESIS + 3 * AGGREGATE_BLOOM_BLOCK_RANGE_LEN - 1,
            ),
        ];

        let from_block = ranges[range_idx].0.get() as i32 + offset_from;
        let to_block = ranges[range_idx].1.get() as i32 + offset_to;

        let constraints = EventConstraints {
            from_block: Some(BlockNumber::new_or_panic(u64::try_from(from_block).unwrap())),
            to_block: Some(BlockNumber::new_or_panic(u64::try_from(to_block).unwrap())),
            contract_addresses: vec![],
            keys: vec![],
            page_size: emitted_events.len(),
            offset: 0,
        };

        let page = tx
            .events(
                &constraints,
                *EVENT_FILTER_BLOCK_RANGE_LIMIT,
            )
            .unwrap();
        let blocks = contained_blocks(&page);

        let expected = (ranges[range_start_idx].0.get()..=ranges[range_end_idx].1.get())
            .filter(|&block| (from_block..=to_block).contains(&(block as i32)))
            .map(BlockNumber::new_or_panic)
            .collect::<BTreeSet<_>>();

        assert_eq!(blocks, expected);
    }
}
