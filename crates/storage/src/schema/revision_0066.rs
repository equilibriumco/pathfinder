use std::time::Instant;

use anyhow::Context;
use pathfinder_common::BlockNumber;
use rusqlite::OptionalExtension;

use crate::bloom::{AggregateBloom, BloomFilter};
use crate::prelude::{params, RowExt};

pub(crate) fn migrate(
    tx: &rusqlite::Transaction<'_>,
    _rocksdb: &crate::RocksDBInner,
) -> anyhow::Result<()> {
    tracing::info!("Creating event_filters table and migrating event Bloom filters");

    tx.execute(
        r"
        CREATE TABLE event_filters (
            from_block  INTEGER NOT NULL,
            to_block    INTEGER NOT NULL,
            bitmap      BLOB NOT NULL,
            UNIQUE(from_block, to_block)
        )
        ",
        [],
    )
    .context("Creating event_filters table")?;

    migrate_event_filters(tx).context("Migrating event Bloom filters")?;

    tx.execute("DROP TABLE starknet_events_filters", [])
        .context("Dropping starknet_events_filters table")?;

    Ok(())
}

/// Migrate individual event bloom filters to the new aggregate table. We only
/// need to migrate all of the [crate::bloom::AGGREGATE_BLOOM_BLOCK_RANGE_LEN]
/// sized chunks. The remainder will be reconstructed by the
/// [crate::StorageManager] as the
/// [RunningEventFilter](crate::connection::event::RunningEventFilter).
fn migrate_event_filters(tx: &rusqlite::Transaction<'_>) -> anyhow::Result<()> {
    let Some(latest_block) = tx
        .query_row(
            "SELECT number FROM canonical_blocks ORDER BY number DESC LIMIT 1",
            [],
            |row| {
                let number = row.get_block_number(0)?.get();

                Ok(number)
            },
        )
        .optional()
        .context("Counting existing blocks")?
    else {
        // No event Bloom filters to migrate.
        return Ok(());
    };

    let mut fetch_bloom_stmt =
        tx.prepare("SELECT bloom FROM starknet_events_filters WHERE block_number = ?")?;

    let mut insert_aggregate_stmt = tx.prepare_cached(
        r"
        INSERT INTO event_filters (from_block, to_block, bitmap)
        VALUES (?, ?, ?)
        ",
    )?;

    let total_blocks = latest_block + 1;
    let mut aggregate = AggregateBloom::new(BlockNumber::GENESIS);
    let mut last_progress_report = Instant::now();

    tracing::info!("Migrating event Bloom filters: 0.00% (0/{})", total_blocks);
    for block in 0..=latest_block {
        let block_number = BlockNumber::new_or_panic(block);
        let bloom_filter = fetch_bloom_stmt
            .query_row(params![&block_number], |row| {
                let bloom: Vec<u8> = row.get(0)?;
                Ok(BloomFilter::from_compressed_bytes(&bloom))
            })
            .optional()
            .context(format!("Querying old Bloom filter for block {block}"))?
            // It might be possible for a block to not have an associated entry in the
            // `starknet_events_filters` table.
            .unwrap_or(BloomFilter::new());

        aggregate.insert(bloom_filter, block_number);

        if block_number == aggregate.to_block {
            insert_aggregate_stmt
                .execute(params![
                    &aggregate.from_block,
                    &aggregate.to_block,
                    &aggregate.compress_bitmap()
                ])
                .context("Inserting aggregate bloom filter")?;

            aggregate = AggregateBloom::new(block_number + 1);
        }

        if last_progress_report.elapsed().as_secs() >= 10 {
            tracing::info!(
                "Migrating event Bloom filters: {:.2}% ({}/{})",
                block as f64 / total_blocks as f64 * 100.0,
                block,
                total_blocks
            );
            last_progress_report = Instant::now();
        }
    }
    tracing::info!(
        "Migrating event Bloom filters: 100.00% ({count}/{count})",
        count = total_blocks,
    );

    Ok(())
}
