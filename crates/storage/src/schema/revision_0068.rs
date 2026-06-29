use std::time::Instant;

use anyhow::Context;
use pathfinder_common::event::Event;
use pathfinder_common::BlockNumber;
use rusqlite::Transaction;

use crate::bloom::{AggregateBloom, BloomFilter};
use crate::event::RunningEventFilter;
use crate::prelude::*;
use crate::AGGREGATE_BLOOM_BLOCK_RANGE_LEN;

pub(crate) fn migrate(tx: &Transaction<'_>, _rocksdb: &crate::RocksDBInner) -> anyhow::Result<()> {
    tracing::info!("Creating running_event_filter table");

    tx.execute(
        r"
        CREATE TABLE running_event_filter (
            id          INTEGER PRIMARY KEY,
            from_block  INTEGER NOT NULL,
            to_block    INTEGER NOT NULL,
            bitmap      BLOB NOT NULL,
            next_block  INTEGER NOT NULL
        )
        ",
        [],
    )
    .context("Creating running_event_filter table")?;

    let latest = tx
        .query_row(
            "SELECT number FROM canonical_blocks ORDER BY number DESC LIMIT 1",
            [],
            |row| row.get_block_number(0),
        )
        .optional()
        .context("Fetching latest block number")?;

    let running_event_filter = if let Some(latest) = latest {
        rebuild_running_event_filter_from_sqlite(tx, latest)
            .context("Rebuilding initial running_event_filter")?
    } else {
        // No blocks in the database, create an event filter starting from the Genesis
        // block.
        RunningEventFilter {
            filter: AggregateBloom::new(BlockNumber::GENESIS),
            next_block: BlockNumber::GENESIS,
        }
    };

    tx.execute(
        r"
        INSERT INTO running_event_filter
        (id, from_block, to_block, bitmap, next_block)
        VALUES (?, ?, ?, ?, ?)
        ",
        params![
            &1,
            &running_event_filter.filter.from_block,
            &running_event_filter.filter.to_block,
            &running_event_filter.filter.compress_bitmap(),
            &running_event_filter.next_block,
        ],
    )
    .context("Inserting initial running_event_filter")?;

    Ok(())
}

/// Rebuild the running event filter from the SQLite `transactions` table.
///
/// Used only by this migration, which runs before the RocksDB migration drops
/// the `transactions` table. Lives here (rather than on `RunningEventFilter`)
/// so no production build compiles it after the migration has landed.
fn rebuild_running_event_filter_from_sqlite(
    tx: &rusqlite::Transaction<'_>,
    latest: BlockNumber,
) -> anyhow::Result<RunningEventFilter> {
    use crate::connection::transaction;

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
        .query_row([], |row| row.get_u64(0))
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
                            bincode::serde::decode_from_slice(&events, bincode::config::standard())
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

    Ok(RunningEventFilter {
        filter,
        next_block: latest + 1,
    })
}
