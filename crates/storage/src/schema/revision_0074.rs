//! The purpose if this migration is to repair any database instances that were
//! affected by a bug in the code that rebuilds the
//! [running event filter](crate::connection::event::RunningEventFilter). The
//! code there assumed that there is a row in the `transactions` table for each
//! block, which is not true anymore after Starknet v0.14.0. Sadly, since the
//! running event filter could have been rebuilt for any range of blocks between
//! the first v0.14.0 block and the latest one for each DB instances, we'll
//! have to rebuild all event filters that contain v0.14.0 blocks.
//!
//! This entire migration should be applied only to Sepolia testnet, as Mainnet
//! is not on v0.14.0 yet.
//!
//! More info [here](https://github.com/equilibriumco/pathfinder/issues/2925).

use std::time::Instant;

use anyhow::Context;
use pathfinder_common::event::Event;
use pathfinder_common::BlockNumber;

use crate::bloom::{AggregateBloom, BloomFilter};
use crate::prelude::*;
use crate::{transaction, AGGREGATE_BLOOM_BLOCK_RANGE_LEN};

/// The first event [AggregateBloom] filter on Sepolia testnet that contains
/// Starknet v0.14.0 blocks.
const FIRST_EVENT_FILTER_TO_REBUILD_FROM_BLOCK: BlockNumber = BlockNumber::new_or_panic(917504);

pub(crate) fn migrate(
    tx: &rusqlite::Transaction<'_>,
    _rocksdb: &crate::RocksDBInner,
) -> anyhow::Result<()> {
    tracing::info!("Rebuilding potentially corrupted event filters");

    let Some(genesis_hash) = tx
        .query_row(
            r"
            SELECT hash FROM block_headers WHERE number = 0
            ",
            [],
            |row| row.get_block_hash(0),
        )
        .optional()
        .context("Querying genesis hash")?
    else {
        // Empty database, nothing to repair.
        return Ok(());
    };

    if genesis_hash != pathfinder_common::consts::SEPOLIA_TESTNET_GENESIS_HASH {
        // Only Sepolia testnet is affected by the bug.
        return Ok(());
    }

    let latest = tx
        .query_row(
            "SELECT number FROM block_headers ORDER BY number DESC LIMIT 1",
            [],
            |row| row.get_block_number(0),
        )
        .optional()
        .context("Querying latest block number")?
        .expect("DB is not empty");

    if FIRST_EVENT_FILTER_TO_REBUILD_FROM_BLOCK > latest {
        // This DB instance has not reached the corrupted aggregate filter range.
        // No need to repair it.
        return Ok(());
    }

    let total_event_filters_to_rebuild: u32 = tx
        .query_row(
            r"
            SELECT COUNT(*)
            FROM event_filters
            WHERE from_block >= ?
            ",
            params![&FIRST_EVENT_FILTER_TO_REBUILD_FROM_BLOCK],
            |row| row.get(0),
        )
        .context("Counting total event filters to rebuild")?;

    let mut update_event_filter_stmt = tx
        .prepare_cached(
            r"
            UPDATE event_filters
            SET bitmap = ?
            WHERE from_block = ? AND to_block = ?
            ",
        )
        .context("Preparing update_event_filter_stmt")?;

    tracing::info!("Rebuilt 0/{total_event_filters_to_rebuild} event filters (0.00%)",);

    let mut last_progress_report = Instant::now();
    let mut event_filters_covered = 0;

    let start_block = FIRST_EVENT_FILTER_TO_REBUILD_FROM_BLOCK.get();
    let end_block = latest.get()
        // Last range is covered by the running event filter.
        - AGGREGATE_BLOOM_BLOCK_RANGE_LEN
        // Previous multiple of AGGREGATE_BLOOM_BLOCK_RANGE_LEN.
        - latest.get() % AGGREGATE_BLOOM_BLOCK_RANGE_LEN;

    for from_block in (start_block..=end_block).step_by(AGGREGATE_BLOOM_BLOCK_RANGE_LEN as usize) {
        let from_block = BlockNumber::new_or_panic(from_block);

        let rebuilt_event_filter = rebuild_event_filter(tx, from_block)?;
        update_event_filter_stmt
            .execute(params![
                &rebuilt_event_filter.compress_bitmap(),
                &rebuilt_event_filter.from_block,
                &rebuilt_event_filter.to_block,
            ])
            .context("Updating corrupted stored aggregate filter")?;

        event_filters_covered += 1;
        if last_progress_report.elapsed().as_secs() >= 10 {
            tracing::info!(
                "Rebuilt {event_filters_covered}/{total_event_filters_to_rebuild} event filters \
                 ({:.2}%)",
                event_filters_covered as f64 / total_event_filters_to_rebuild as f64 * 100.0
            );
            last_progress_report = Instant::now();
        }
    }

    tracing::info!(
        "Rebuilt {total_event_filters_to_rebuild}/{total_event_filters_to_rebuild} event filters \
         (100.00%)",
    );
    tracing::info!("Rebuilding running event filter");

    let running_event_filter_from_block = latest - latest.get() % AGGREGATE_BLOOM_BLOCK_RANGE_LEN;
    let rebuilt_running_event_filter = rebuild_event_filter(tx, running_event_filter_from_block)?;

    tx.execute(
        r"
        UPDATE running_event_filter
        SET from_block = ?, to_block = ?, bitmap = ?, next_block = ?
        WHERE id = 1
        ",
        params![
            &rebuilt_running_event_filter.from_block,
            &rebuilt_running_event_filter.to_block,
            &rebuilt_running_event_filter.compress_bitmap(),
            &(latest + 1)
        ],
    )
    .context("Updating corrupted running event filter")?;

    Ok(())
}

fn rebuild_event_filter(
    tx: &rusqlite::Transaction<'_>,
    from_block: BlockNumber,
) -> anyhow::Result<AggregateBloom> {
    let mut load_events_stmt = tx
        .prepare_cached(
            r"
            SELECT block_number, events
            FROM transactions
            WHERE block_number >= :from_block AND block_number <= :to_block
            ",
        )
        .context("Preparing load_events_stmt")?;

    let to_block = from_block + AGGREGATE_BLOOM_BLOCK_RANGE_LEN - 1;

    // Correctly reconstruct a potentially corrupted aggregate filter.
    let bloom_filters_per_block: Vec<Option<(BlockNumber, BloomFilter)>> = load_events_stmt
        .query_and_then(
            named_params![":from_block": &from_block, ":to_block": &to_block],
            |row| {
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

    let mut rebuilt_aggregate_filter = AggregateBloom::new(from_block);
    for block_bloom_filter in bloom_filters_per_block {
        let Some((block_number, bloom)) = block_bloom_filter else {
            // Reached the end of P2P (checkpoint) synced events.
            break;
        };

        rebuilt_aggregate_filter.insert(bloom, block_number);
    }

    Ok(rebuilt_aggregate_filter)
}
