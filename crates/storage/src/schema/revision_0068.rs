use anyhow::Context;
use pathfinder_common::BlockNumber;
use rusqlite::Transaction;

use crate::bloom::AggregateBloom;
use crate::event::RunningEventFilter;
use crate::prelude::*;

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
        RunningEventFilter::rebuild(tx, latest)
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
            &running_event_filter.filter.from_block.get(),
            &running_event_filter.filter.to_block.get(),
            &running_event_filter.filter.compress_bitmap(),
            &running_event_filter.next_block,
        ],
    )
    .context("Inserting initial running_event_filter")?;

    Ok(())
}
