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
//!
//! Nonce and storage updates are stored in RocksDB column families and are
//! pruned by this function as well.
//!
//! It is forbidden to enable pruning on a database that was created with it
//! disabled (and vice versa). However, it is possible to change the number of
//! blocks that are kept in the database between runs.

use anyhow::Context;
use pathfinder_common::{BlockNumber, ContractAddress, StorageAddress};

use super::{Transaction, NONCE_UPDATES_COLUMN, STORAGE_UPDATES_COLUMN};
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
        prune_block(self, block_to_prune)
    }
}

pub(crate) fn prune_block(tx: &super::Transaction<'_>, block: BlockNumber) -> anyhow::Result<()> {
    let db = tx.inner();

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

    // Prune SQLite contract_updates rows that have a newer entry.
    let last_kept_block = block + 1;

    let mut contract_updates_select_stmt = db.prepare_cached(
        r"
        SELECT contract_address
        FROM contract_updates
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
    let mut contract_updates_delete_stmt = db.prepare_cached(
        r"
        DELETE FROM contract_updates
        WHERE contract_address = ?
        AND block_number = ?
        ",
    )?;

    let contract_updates_addresses = contract_updates_select_stmt
        .query_map(
            named_params!(
                ":last_kept_block": &last_kept_block,
            ),
            |row| row.get_contract_address(0),
        )
        .context("Querying contract_updates")?
        .collect::<Result<Vec<_>, _>>()?;

    for address in &contract_updates_addresses {
        let blocks_with_same_update = blocks_with_same_contract_update
            .query_map(params![address, &last_kept_block], |row| {
                row.get_block_number(0)
            })
            .context("Querying blocks with same contract update")?
            .collect::<Result<Vec<_>, _>>()?;
        for old_block in blocks_with_same_update {
            contract_updates_delete_stmt
                .execute(params![address, &old_block])
                .context("Deleting contract updates")?;
        }
    }

    // Prune RocksDB nonce/storage entries for the block being removed.
    let Some(state_update) = tx.state_update_data(last_kept_block)? else {
        return Ok(());
    };
    let mut batch = tx.batch.lock().expect("Batch lock poisoned");
    let nonce_updates_column = tx.rocksdb_get_column(&NONCE_UPDATES_COLUMN);
    let storage_updates_column = tx.rocksdb_get_column(&STORAGE_UPDATES_COLUMN);

    for (address, update) in &state_update.contract_updates {
        if update.nonce.is_some() {
            delete_prior_block_entries(
                &mut batch,
                tx.rocksdb(),
                &nonce_updates_column,
                address.0.as_be_bytes(),
                last_kept_block,
            )?;
        }
        for (storage_key, _) in &update.storage {
            delete_prior_block_entries(
                &mut batch,
                tx.rocksdb(),
                &storage_updates_column,
                &storage_prefix(address, storage_key),
                last_kept_block,
            )?;
        }
    }
    for (address, update) in &state_update.system_contract_updates {
        for (storage_key, _) in &update.storage {
            delete_prior_block_entries(
                &mut batch,
                tx.rocksdb(),
                &storage_updates_column,
                &storage_prefix(address, storage_key),
                last_kept_block,
            )?;
        }
    }

    Ok(())
}

fn storage_prefix(address: &ContractAddress, key: &StorageAddress) -> [u8; 64] {
    let mut prefix = [0u8; 64];
    prefix[..32].copy_from_slice(address.0.as_be_bytes());
    prefix[32..].copy_from_slice(key.0.as_be_bytes());
    prefix
}

fn delete_prior_block_entries<C: rust_rocksdb::AsColumnFamilyRef>(
    batch: &mut crate::RocksDBBatch,
    rocksdb: &crate::RocksDB,
    cf: &C,
    prefix: &[u8],
    last_kept_block: BlockNumber,
) -> anyhow::Result<()> {
    let last_kept_inverted =
        u32::MAX - u32::try_from(last_kept_block.get()).expect("block fits into u32");
    let mut seek_key = Vec::with_capacity(prefix.len() + 4);
    seek_key.extend_from_slice(prefix);
    seek_key.extend_from_slice(&last_kept_inverted.wrapping_add(1).to_be_bytes());

    let mut read_opts = rust_rocksdb::ReadOptions::default();
    read_opts.set_prefix_same_as_start(true);
    let mut iter = rocksdb.raw_iterator_cf_opt(cf, read_opts);
    iter.seek(&seek_key);
    while iter.valid() {
        let key = iter.key().context("Reading prune key")?;
        batch.delete_cf(cf, key);
        iter.next();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use pathfinder_common::macro_prelude::*;
    use pathfinder_common::{BlockHash, BlockHeader, BlockNumber, StateUpdate, StorageValue};

    use crate::connection::{NONCE_UPDATES_COLUMN, STORAGE_UPDATES_COLUMN};

    #[test]
    fn prune_block_removes_rocksdb_nonce_and_storage_entries() {
        let storage = crate::StorageBuilder::in_memory().unwrap();
        let mut conn = storage.connection().unwrap();
        let contract = contract_address!("0xc");
        let storage_key = storage_address!("0x2");

        // Two blocks; both modify the same (contract, key) so the older one
        // becomes prunable.
        for (n, val) in &[(1u64, "0x10"), (2u64, "0x20")] {
            let tx = conn.transaction().unwrap();
            let header = BlockHeader::builder()
                .number(BlockNumber::new_or_panic(*n))
                .finalize_with_hash(BlockHash(pathfinder_crypto::Felt::from(*n)));
            tx.insert_block_header(&header).unwrap();
            let su = StateUpdate::default()
                .with_storage_update(
                    contract,
                    storage_key,
                    StorageValue(pathfinder_crypto::Felt::from_hex_str(val).unwrap()),
                )
                .with_contract_nonce(contract, contract_nonce!("0x1"));
            tx.insert_state_update(BlockNumber::new_or_panic(*n), &su)
                .unwrap();
            tx.commit().unwrap();
        }

        // Pre-state: block-1 and block-2 entries exist in RocksDB.
        let tx = conn.transaction().unwrap();
        let rocksdb = tx.rocksdb_for_test();
        let storage_cf = rocksdb.get_column(&STORAGE_UPDATES_COLUMN);
        let nonce_cf = rocksdb.get_column(&NONCE_UPDATES_COLUMN);
        drop(tx);

        // Count entries with the contract prefix before pruning.
        fn count_prefix(
            db: &crate::RocksDB,
            cf: &impl rust_rocksdb::AsColumnFamilyRef,
            prefix: &[u8],
        ) -> usize {
            let mut iter = db.raw_iterator_cf(cf);
            iter.seek(prefix);
            let mut n = 0;
            while iter.valid() {
                let key = iter.key().unwrap();
                if !key.starts_with(prefix) {
                    break;
                }
                n += 1;
                iter.next();
            }
            n
        }

        let storage_before = count_prefix(&rocksdb.rocksdb, &storage_cf, contract.0.as_be_bytes());
        let nonce_before = count_prefix(&rocksdb.rocksdb, &nonce_cf, contract.0.as_be_bytes());
        assert!(
            storage_before >= 2,
            "storage entries before pruning: {storage_before}"
        );
        assert!(
            nonce_before >= 2,
            "nonce entries before pruning: {nonce_before}"
        );

        // Prune block 1.
        let tx = conn.transaction().unwrap();
        tx.prune_block(BlockNumber::new_or_panic(1)).unwrap();
        tx.commit().unwrap();

        let storage_after = count_prefix(&rocksdb.rocksdb, &storage_cf, contract.0.as_be_bytes());
        let nonce_after = count_prefix(&rocksdb.rocksdb, &nonce_cf, contract.0.as_be_bytes());
        assert_eq!(
            storage_after,
            storage_before - 1,
            "block 1 storage entry not pruned"
        );
        assert_eq!(
            nonce_after,
            nonce_before - 1,
            "block 1 nonce entry not pruned"
        );
    }
}
