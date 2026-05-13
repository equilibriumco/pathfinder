use std::time::{Duration, Instant};

use anyhow::Context;
use base64::prelude::*;
use pathfinder_common::EventKey;
use rusqlite::params;

use crate::bloom::BloomFilter;
use crate::params::RowExt;

pub(crate) fn migrate(
    tx: &rusqlite::Transaction<'_>,
    _rocksdb: &crate::RocksDBInner,
) -> anyhow::Result<()> {
    tx.execute_batch(
        r"
        CREATE TABLE starknet_events_filters (
            block_number INTEGER NOT NULL PRIMARY KEY,
            bloom BLOB NOT NULL
        );
    ",
    )
    .context("Creating event Bloom filter table")?;

    tracing::info!("Creating Bloom filters for events");

    let mut query_statement = tx.prepare(
        r"SELECT
        block_number,
        from_address,
        keys
    FROM starknet_events
    ORDER BY block_number
    ",
    )?;

    let mut insert_statement =
        tx.prepare(r"INSERT INTO starknet_events_filters (block_number, bloom) VALUES (?, ?)")?;

    let mut rows = query_statement.query([])?;

    let mut prev_block_number: u64 = 0;
    let mut bloom = BloomFilter::new();
    let mut events_in_filter: usize = 0;
    let mut progress_logged = Instant::now();
    const LOG_RATE: Duration = Duration::from_secs(10);

    while let Some(row) = rows.next().context("Fetching next receipt")? {
        let block_number = row.get_block_number("block_number")?;

        let current_block_number = block_number.get();
        if current_block_number > prev_block_number {
            if current_block_number % 1024 == 0 && progress_logged.elapsed() > LOG_RATE {
                tracing::debug!(%current_block_number, "Processing events");
                progress_logged = Instant::now();
            }

            insert_statement.execute(params![prev_block_number, bloom.into_compressed_bytes()])?;

            bloom = BloomFilter::new();
            prev_block_number = current_block_number;
            events_in_filter = 0;
        }

        let from_address = row.get_contract_address("from_address")?;
        let keys = row.get_ref_unwrap("keys").as_str()?;
        // no need to allocate a vec for this in loop
        let mut temp = [0u8; 32];
        let keys: Vec<EventKey> = keys
            .split(' ')
            .map(|key| {
                let used = BASE64_STANDARD
                    .decode_slice(key, &mut temp)
                    .map_err(anyhow::Error::from)?;
                let key = pathfinder_crypto::Felt::from_be_slice(&temp[..used])
                    .map_err(anyhow::Error::from)?;
                Ok(EventKey(key))
            })
            .collect::<Result<_, anyhow::Error>>()?;

        bloom.set_keys(&keys);
        bloom.set_address(&from_address);

        events_in_filter += 1;
    }

    if events_in_filter > 0 {
        insert_statement.execute(params![prev_block_number, bloom.into_compressed_bytes()])?;
    }

    tracing::info!("Dropping starknet_events table");
    tx.execute_batch(
        r"
        DROP TABLE starknet_events_keys_03;
        DROP TABLE starknet_events;
    ",
    )
    .context("Dropping starknet_events table")?;

    tracing::debug!("Creating reorg counter table");
    tx.execute_batch(
        "CREATE TABLE reorg_counter (
            id INTEGER NOT NULL PRIMARY KEY,
            counter INTEGER NOT NULL
        );
        INSERT INTO reorg_counter (id, counter) VALUES (1, 0);",
    )
    .context("Creating reorg counter table")?;

    Ok(())
}
