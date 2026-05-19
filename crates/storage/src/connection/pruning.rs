//! Blockchain history pruning is a feature that can be enabled to limit the
//! number of blocks stored in the database, thus keeping the size of the
//! database in check. The latest block is always stored.
//!
//! Database tables that are subject to pruning are:
//! - `transactions`
//! - `transaction_hashes`
//! - `block_headers`
//! - `block_signatures`
//! - `event_filters`
//! - `contract_updates` (a row can be pruned if there is another row with the
//!   same `contract_address` and a higher `block_number`)
//! - `nonce_updates` (a row can be pruned if there is another row with the same
//!   `contract_address_id` and a higher `block_number`)
//! - `storage_updates` (a row can be pruned if there is another row with the
//!   same `contract_address_id`, same `storage_address_id` and a higher
//!   `block_number`)
//!
//! It is forbidden to enable pruning on a database that was created with it
//! disabled (and vice versa). However, it is possible to change the number of
//! blocks that are kept in the database between runs.

use anyhow::Context;
use pathfinder_common::BlockNumber;

use super::Transaction;
use crate::prelude::{named_params, params, RowExt};
use crate::AGGREGATE_BLOOM_BLOCK_RANGE_LEN;

#[derive(Debug, Clone, Copy)]
pub enum BlockchainHistoryMode {
    /// Keep the entire blockchain history.
    Archive,
    /// Prune the blockchain history. Only keep the last `num_blocks_kept`
    /// blocks as well as the latest block.
    Prune { num_blocks_kept: u64 },
}

impl Transaction<'_> {
    pub fn prune_block(&self, block_to_prune: BlockNumber) -> anyhow::Result<()> {
        prune_block(self.inner(), block_to_prune)
    }
}

pub(crate) fn prune_block(
    db: &rusqlite::Transaction<'_>,
    block: BlockNumber,
) -> anyhow::Result<()> {
    // Prune block and transaction (via FOREIGN KEY + ON DELETE CASCADE) data.
    let mut block_headers_delete_stmt = db.prepare_cached(
        r"
        DELETE FROM block_headers
        WHERE number = :block_to_prune
        ",
    )?;
    let mut event_filters_delete_stmt = db.prepare_cached(
        r"
        DELETE FROM event_filters
        WHERE to_block = :block_to_prune
        ",
    )?;

    block_headers_delete_stmt
        .execute(named_params!(
            ":block_to_prune": &block,
        ))
        .context("Deleting block from block_headers")?;

    // Only run event filter pruning if the block to prune is the last block in an
    // event filter range, because now we know that all blocks covered by this
    // filter will be gone.
    let is_to_block = (block.get() + 1).is_multiple_of(AGGREGATE_BLOOM_BLOCK_RANGE_LEN);
    if is_to_block {
        event_filters_delete_stmt
            .execute(named_params!(
                ":block_to_prune": &block,
            ))
            .context("Deleting filter from event_filters")?;
    }

    // Prune state update data (where possible).
    let mut contract_updates_select_stmt = db.prepare_cached(
        r"
        SELECT contract_address
        FROM contract_updates
        WHERE block_number = :last_kept_block
        ",
    )?;
    let mut nonce_updates_select_stmt = db.prepare_cached(
        r"
        SELECT contract_address_id
        FROM nonce_updates
        WHERE block_number = :last_kept_block
        ",
    )?;
    let mut storage_updates_select_stmt = db.prepare_cached(
        r"
        SELECT contract_address_id, storage_address_id
        FROM storage_updates
        WHERE block_number = :last_kept_block
        ",
    )?;
    let mut blocks_with_same_contract_update = db.prepare_cached(
        r"
        SELECT block_number
        FROM contract_updates
        WHERE contract_address = ?
        AND block_number < ?
        ORDER BY block_number ASC
        ",
    )?;
    let mut blocks_with_same_nonce_update = db.prepare_cached(
        r"
        SELECT block_number
        FROM nonce_updates
        WHERE contract_address_id = ?
        AND block_number < ?
        ",
    )?;
    let mut blocks_with_same_storage_update = db.prepare_cached(
        r"
        SELECT block_number
        FROM storage_updates
        WHERE contract_address_id = ?
        AND storage_address_id = ?
        AND block_number < ?
        ",
    )?;
    let mut contract_updates_delete_stmt = db.prepare_cached(
        r"
        DELETE FROM contract_updates
        WHERE contract_address = ?
        AND block_number = ?
        ",
    )?;
    let mut nonce_updates_delete_stmt = db.prepare_cached(
        r"
        DELETE FROM nonce_updates
        WHERE contract_address_id = ?
        AND block_number = ?
        ",
    )?;
    let mut storage_updates_delete_stmt = db.prepare_cached(
        r"
        DELETE FROM storage_updates
        WHERE contract_address_id = ?
        AND storage_address_id = ?
        AND block_number = ?
        ",
    )?;

    let last_kept_block = block + 1;

    // Find and delete state updates that are no longer needed.
    let contract_updates_addresses = contract_updates_select_stmt
        .query_map(
            named_params!(
                ":last_kept_block": &last_kept_block,
            ),
            |row| row.get_contract_address(0),
        )
        .context("Querying contract_updates")?
        .collect::<Result<Vec<_>, _>>()?;
    let nonce_updates_ids = nonce_updates_select_stmt
        .query_map(
            named_params! {
                ":last_kept_block": &last_kept_block,
            },
            |row| row.get(0),
        )
        .context("Querying nonce_updates")?
        .collect::<Result<Vec<i64>, _>>()?;
    let storage_updates_ids = storage_updates_select_stmt
        .query_map(
            named_params! {
                ":last_kept_block": &last_kept_block,
            },
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .context("Querying storage_updates")?
        .collect::<Result<Vec<(i64, i64)>, _>>()?;
    for address in &contract_updates_addresses {
        let blocks_with_same_update = blocks_with_same_contract_update
            .query_map(params![address, &last_kept_block], |row| {
                row.get_block_number(0)
            })
            .context("Querying blocks with same contract update")?
            .collect::<Result<Vec<_>, _>>()?;
        for block in blocks_with_same_update {
            contract_updates_delete_stmt
                .execute(params![address, &block])
                .context("Deleting storage updates")?;
        }
    }
    for id in &nonce_updates_ids {
        let blocks_with_same_update = blocks_with_same_nonce_update
            .query_map(params![id, &last_kept_block], |row| row.get_block_number(0))
            .context("Querying blocks with same nonce update")?
            .collect::<Result<Vec<_>, _>>()?;
        for block in blocks_with_same_update {
            nonce_updates_delete_stmt
                .execute(params![id, &block])
                .context("Deleting nonce updates")?;
        }
    }
    for (contract_address_id, storage_address_id) in &storage_updates_ids {
        let blocks_with_same_update = blocks_with_same_storage_update
            .query_map(
                params![contract_address_id, storage_address_id, &last_kept_block],
                |row| row.get_block_number(0),
            )
            .context("Querying blocks with same storage update")?
            .collect::<Result<Vec<_>, _>>()?;
        for block in blocks_with_same_update {
            storage_updates_delete_stmt
                .execute(params![contract_address_id, storage_address_id, &block])
                .context("Deleting storage updates")?;
        }
    }

    Ok(())
}
