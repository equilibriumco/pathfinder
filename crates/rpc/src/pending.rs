use std::sync::Arc;

use anyhow::Context;
use pathfinder_common::{BlockNumber, StateUpdate};
pub use pathfinder_pending_data::{
    PendingBlocks,
    PendingData,
    PreConfirmedBlock,
    PreLatestBlock,
    PreLatestData,
    TxnReceiptAndEvents,
};
use pathfinder_pending_data::{PendingDataCache, ReadError};
use pathfinder_storage::Transaction;
use tokio::sync::watch::Receiver as WatchReceiver;

use crate::RpcVersion;

/// A finalized transaction along with its receipt, events, status and the block
/// number it was included in.
pub struct FinalizedTxData {
    pub block_number: BlockNumber,
    pub transaction: pathfinder_common::transaction::Transaction,
    pub receipt: pathfinder_common::receipt::Receipt,
    pub events: Vec<pathfinder_common::event::Event>,
    pub finality_status: crate::dto::TxnFinalityStatus,
}

/// Validates the cached pre-confirmed data against the latest block in
/// storage and the JSON-RPC version before returning it.
#[derive(Clone)]
pub struct PendingWatcher {
    cache: Arc<PendingDataCache>,
}

impl PendingWatcher {
    pub fn new(cache: Arc<PendingDataCache>) -> Self {
        Self { cache }
    }

    /// A fresh receiver for awaiting changes directly.
    pub fn subscribe(&self) -> WatchReceiver<PendingData> {
        self.cache.subscribe()
    }

    /// Returns [PendingData] which has been validated against the latest block
    /// available in storage and the JSON-RPC version.
    ///
    /// Returns an empty block with gas price and timestamp taken from the
    /// latest block if no valid pending data is available. The block number
    /// is also incremented.
    pub fn get(
        &self,
        tx: &Transaction<'_>,
        rpc_version: RpcVersion,
    ) -> Result<PendingData, ReadError> {
        let latest = tx
            .block_header(pathfinder_common::BlockId::Latest)
            .context("Querying latest block header")?
            .unwrap_or_default();

        // The pre-confirmed block is to be only ever used on JSON-RPC 0.9 and up.
        // Older versions did have the semantics that expected that pending block
        // contents are L2_ACCEPTED, which is not the case for the pre-confirmed
        // block.
        if rpc_version < RpcVersion::V09 {
            return Ok(PendingData::empty(&latest));
        }

        let watched_pending_data = match self.cache.try_read() {
            Some(data) => data,
            None => tokio::runtime::Handle::current().block_on(self.cache.read())?,
        };

        let watched_pending_blocks = watched_pending_data.pending_block();
        let PendingBlocks {
            pre_confirmed,
            pre_latest,
        } = watched_pending_blocks.as_ref();

        let pending_data = {
            // The parent state commitment is only available here. The task polling the
            // pre-confirmed block has no access to the parent block header, thus it
            // cannot properly set the parent state commitment.

            // We can consider the pre-confirmed block valid if:
            //   - the pre-latest block exists and is the child our latest stored block,
            //   - the pre-latest block exists and is the same block as our latest block,
            //   i.e. we received that block as a finalized L2 block but it still lingers
            //   in pending data.
            //   - the pre-latest block does not exist and the pre-confirmed block is the
            //   child of our latest stored block.
            match pre_latest {
                // Is pre-latest the next block?
                Some(pre_latest) if pre_latest.block.number == latest.number + 1 => {
                    assert_eq!(
                        pre_latest.block.number + 1,
                        pre_confirmed.number,
                        "Pre-confirmed block should be child of pre-latest"
                    );
                    // Set pre-latest block parent state commitment, clone rest of the data.
                    let pre_latest = pre_latest.clone();
                    let pre_latest_state_update = pre_latest
                        .state_update
                        .with_parent_state_commitment(latest.state_commitment);
                    let pre_latest = PreLatestData {
                        block: pre_latest.block,
                        state_update: pre_latest_state_update,
                    };
                    PendingData {
                        blocks: PendingBlocks {
                            pre_confirmed: pre_confirmed.clone(),
                            pre_latest: Some(pre_latest),
                        }
                        .into(),
                        state_update: Arc::clone(&watched_pending_data.state_update),
                        aggregated_state_update: Arc::clone(
                            &watched_pending_data.aggregated_state_update,
                        ),
                        number: pre_confirmed.number,
                    }
                }
                // Is pre-latest already in the database?
                Some(pre_latest) if pre_latest.block.number == latest.number => {
                    // We'll ignore pre-latest data here but let's make sure everything is
                    // still as expected.
                    assert_eq!(
                        pre_latest.block.number + 1,
                        pre_confirmed.number,
                        "Pre-confirmed block should be child of pre-latest"
                    );
                    // Set pre-latest data to `None`, pre-confirmed block parent state
                    // commitment and clone rest of the data.
                    let pre_confirmed_block = PendingBlocks {
                        pre_confirmed: pre_confirmed.clone(),
                        pre_latest: None,
                    };
                    let pre_confirmed_state_update = Arc::new(
                        StateUpdate::clone(&watched_pending_data.state_update)
                            .with_parent_state_commitment(latest.state_commitment),
                    );

                    PendingData {
                        blocks: Arc::new(pre_confirmed_block),
                        state_update: Arc::clone(&pre_confirmed_state_update),
                        aggregated_state_update: pre_confirmed_state_update,
                        number: pre_confirmed.number,
                    }
                }
                // Is pre-confirmed the next block?
                None if pre_confirmed.number == latest.number + 1 => {
                    // Set pre-confirmed block parent state commitment, clone rest of the data.
                    let pre_confirmed_state_update =
                        StateUpdate::clone(&watched_pending_data.state_update)
                            .with_parent_state_commitment(latest.state_commitment);
                    let state_update = Arc::new(pre_confirmed_state_update);
                    PendingData {
                        blocks: Arc::clone(&watched_pending_data.blocks),
                        state_update: Arc::clone(&state_update),
                        aggregated_state_update: state_update,
                        number: pre_confirmed.number,
                    }
                }
                _ => PendingData::empty(&latest),
            }
        };

        Ok(pending_data)
    }

    #[cfg(test)]
    pub fn get_unchecked(&self) -> PendingData {
        self.cache.subscribe().borrow().clone()
    }
}

/// Find a transaction-with-receipt in the pre-confirmed or pre-latest block,
/// in that order.
pub fn find_finalized_tx_data(
    pending: &PendingData,
    tx_hash: pathfinder_common::TransactionHash,
) -> Option<FinalizedTxData> {
    if let Some(tx) = pending
        .pre_confirmed_transactions()
        .iter()
        .find(|t| t.hash == tx_hash)
    {
        let (receipt, events) = pending
            .pre_confirmed_tx_receipts_and_events()
            .iter()
            .find(|(r, _)| r.transaction_hash == tx_hash)
            .cloned()
            .expect("Receipt should exist if the transaction exists");
        return Some(FinalizedTxData {
            block_number: pending.pre_confirmed_block_number(),
            transaction: tx.clone(),
            receipt,
            events,
            finality_status: crate::dto::TxnFinalityStatus::PreConfirmed,
        });
    }

    if let Some(pre_latest_block) = pending.pre_latest_block() {
        if let Some(tx) = pre_latest_block
            .transactions
            .iter()
            .find(|t| t.hash == tx_hash)
        {
            let (receipt, events) = pending
                .pre_latest_tx_receipts_and_events()
                .and_then(|rs| {
                    rs.iter()
                        .find(|(r, _)| r.transaction_hash == tx_hash)
                        .cloned()
                })
                .expect("Receipt should exist if the transaction exists");
            return Some(FinalizedTxData {
                block_number: pre_latest_block.number,
                transaction: tx.clone(),
                receipt,
                events,
                finality_status: crate::dto::TxnFinalityStatus::PreConfirmed,
            });
        }
    }

    None
}

#[cfg(test)]
mod tests {

    use pathfinder_common::macro_prelude::*;
    use pathfinder_common::receipt::Receipt;
    use pathfinder_common::transaction::Transaction;
    use pathfinder_common::{
        BlockHeader,
        BlockTimestamp,
        GasPrice,
        L1DataAvailabilityMode,
        StarknetVersion,
        TransactionHash,
    };
    use starknet_gateway_types::reply::{GasPrices, Status};

    use super::*;

    fn latest_block() -> BlockHeader {
        BlockHeader::builder()
            .eth_l1_gas_price(GasPrice(1234))
            .strk_l1_gas_price(GasPrice(3377))
            .timestamp(BlockTimestamp::new_or_panic(6777))
            .finalize_with_hash(block_hash_bytes!(b"latest hash"))
    }

    fn valid_pre_confirmed_block(latest: &BlockHeader) -> PendingData {
        let state_update = Arc::new(StateUpdate::default().with_contract_nonce(
            contract_address_bytes!(b"contract address"),
            contract_nonce_bytes!(b"nonce"),
        ));
        PendingData {
            blocks: PendingBlocks {
                pre_confirmed: PreConfirmedBlock {
                    number: latest.number + 1,
                    l1_gas_price: Default::default(),
                    l1_data_gas_price: Default::default(),
                    l2_gas_price: Default::default(),
                    sequencer_address: sequencer_address!("0x1234"),
                    status: Status::PreConfirmed,
                    timestamp: BlockTimestamp::new_or_panic(112233),
                    starknet_version: StarknetVersion::new(0, 14, 0, 0),
                    l1_da_mode: L1DataAvailabilityMode::Blob,
                    transactions: vec![],
                    transaction_receipts: vec![],
                },
                pre_latest: None,
            }
            .into(),
            state_update: Arc::clone(&state_update),
            aggregated_state_update: state_update,
            number: latest.number + 1,
        }
    }

    fn valid_pre_confirmed_block_with_pre_latest(latest: &BlockHeader) -> PendingData {
        let pre_latest_block = PreLatestBlock {
            number: latest.number + 1,
            parent_hash: latest.hash,
            l1_gas_price: Default::default(),
            l1_data_gas_price: Default::default(),
            l2_gas_price: Default::default(),
            sequencer_address: sequencer_address!("0x1234"),
            status: Status::Pending,
            timestamp: BlockTimestamp::new_or_panic(112233),
            starknet_version: StarknetVersion::new(0, 14, 0, 0),
            l1_da_mode: L1DataAvailabilityMode::Blob,
            transactions: vec![Transaction::default()],
            transaction_receipts: vec![(Receipt::default(), vec![])],
        };
        let pre_latest_state_update = StateUpdate::default().with_contract_nonce(
            contract_address_bytes!(b"pre latest contract address"),
            contract_nonce_bytes!(b"pre latest nonce"),
        );

        let pre_confirmed_state_update = StateUpdate::default().with_contract_nonce(
            contract_address_bytes!(b"contract address"),
            contract_nonce_bytes!(b"nonce"),
        );

        let aggregated_state_update = pre_latest_state_update
            .clone()
            .apply(&pre_confirmed_state_update);

        PendingData {
            blocks: PendingBlocks {
                pre_confirmed: PreConfirmedBlock {
                    number: latest.number + 2,
                    l1_gas_price: Default::default(),
                    l1_data_gas_price: Default::default(),
                    l2_gas_price: Default::default(),
                    sequencer_address: sequencer_address!("0x1234"),
                    status: Status::PreConfirmed,
                    timestamp: BlockTimestamp::new_or_panic(112233),
                    starknet_version: StarknetVersion::new(0, 14, 0, 0),
                    l1_da_mode: L1DataAvailabilityMode::Blob,
                    transactions: vec![Transaction::default()],
                    transaction_receipts: vec![(Receipt::default(), vec![])],
                },
                pre_latest: Some(PreLatestData {
                    block: pre_latest_block,
                    state_update: pre_latest_state_update,
                }),
            }
            .into(),
            state_update: pre_confirmed_state_update.into(),
            aggregated_state_update: aggregated_state_update.into(),
            number: latest.number + 2,
        }
    }

    fn invalid_pre_confirmed_block_with_pre_latest(latest: &BlockHeader) -> PendingData {
        let pre_latest_block = PreLatestBlock {
            // These are okay.
            number: latest.number + 1,
            parent_hash: latest.hash,
            ..Default::default()
        };
        let pre_latest_data = PreLatestData {
            block: pre_latest_block,
            ..Default::default()
        };

        PendingData {
            blocks: PendingBlocks {
                pre_confirmed: PreConfirmedBlock {
                    // This is not okay. Should be latest.number + 2 to be valid.
                    number: latest.number + 3,
                    ..Default::default()
                },
                pre_latest: Some(pre_latest_data),
            }
            .into(),
            state_update: StateUpdate::default().into(),
            aggregated_state_update: StateUpdate::default().into(),
            // Should be latest.number + 2 to be valid.
            number: latest.number + 3,
        }
    }

    #[test]
    fn valid_pre_confirmed() {
        let cache = Arc::new(PendingDataCache::new());
        let uut = PendingWatcher::new(cache.clone());

        let mut storage = pathfinder_storage::StorageBuilder::in_memory()
            .unwrap()
            .connection()
            .unwrap();

        let latest = latest_block();

        let tx = storage.transaction().unwrap();
        tx.insert_block_header(&latest).unwrap();

        let pending = valid_pre_confirmed_block(&latest);
        cache.store(pending.clone());

        let result = uut.get(&tx, RpcVersion::V09).unwrap();
        pretty_assertions_sorted::assert_eq_sorted!(result, pending);
    }

    #[test]
    fn valid_pre_confirmed_with_pre_latest() {
        // There are certain intervals where the pre-latest block is still stored in
        // pending data but that same block has already been finalized and received as
        // the new L2 block. This test makes sure that we still provide pending data
        // from the pre-confirmed block in this case and *we do not provide* the
        // pre-latest block because it is not pending anymore.
        let cache = Arc::new(PendingDataCache::new());
        let uut = PendingWatcher::new(cache.clone());

        let mut storage = pathfinder_storage::StorageBuilder::in_memory()
            .unwrap()
            .connection()
            .unwrap();

        // Required otherwise latest doesn't have a valid parent hash in storage.
        let parent = BlockHeader::builder()
            .number(BlockNumber::GENESIS + 12)
            .finalize_with_hash(block_hash_bytes!(b"parent hash"));

        let latest = parent
            .child_builder()
            .eth_l1_gas_price(GasPrice(1234))
            .strk_l1_gas_price(GasPrice(3377))
            .eth_l1_data_gas_price(GasPrice(9999))
            .strk_l1_data_gas_price(GasPrice(8888))
            .l1_da_mode(L1DataAvailabilityMode::Blob)
            .timestamp(BlockTimestamp::new_or_panic(6777))
            .sequencer_address(sequencer_address!("0xffff"))
            .finalize_with_hash(block_hash_bytes!(b"latest hash"));

        let tx = storage.transaction().unwrap();
        tx.insert_block_header(&parent).unwrap();
        tx.insert_block_header(&latest).unwrap();

        // Pre-latest block will be `latest + 1` which is valid.
        let pending = valid_pre_confirmed_block_with_pre_latest(&latest);
        cache.store(pending.clone());

        let result = uut.get(&tx, RpcVersion::V09).unwrap();
        pretty_assertions_sorted::assert_eq_sorted!(result, pending);

        // Pre-latest block will be same as `latest` which is also valid, but in this
        // case the pre-latest block should be ignored.
        let pending = valid_pre_confirmed_block_with_pre_latest(&parent);
        cache.store(pending);

        let result = uut.get(&tx, RpcVersion::V09).unwrap();
        // We got a non-empty pre-confirmed block..
        assert!(!result.pre_confirmed_transactions().is_empty());
        // ..and we did not receive a pre-latest block.
        assert!(result.pre_latest_block().is_none());
    }

    #[test]
    fn valid_pre_confirmed_is_not_used_for_old_rpc_versions() {
        let cache = Arc::new(PendingDataCache::new());
        let uut = PendingWatcher::new(cache.clone());

        let mut storage = pathfinder_storage::StorageBuilder::in_memory()
            .unwrap()
            .connection()
            .unwrap();

        let latest = latest_block();

        let tx = storage.transaction().unwrap();
        tx.insert_block_header(&latest).unwrap();

        let pending = valid_pre_confirmed_block(&latest);
        cache.store(pending.clone());

        let expected_empty_pending_data = PendingData::empty(&latest);

        let result = uut.get(&tx, RpcVersion::V06).unwrap();
        pretty_assertions_sorted::assert_eq_sorted!(result, expected_empty_pending_data);
        let result = uut.get(&tx, RpcVersion::V07).unwrap();
        pretty_assertions_sorted::assert_eq_sorted!(result, expected_empty_pending_data);
        let result = uut.get(&tx, RpcVersion::V08).unwrap();
        pretty_assertions_sorted::assert_eq_sorted!(result, expected_empty_pending_data);
    }

    #[test]
    fn valid_pre_confirmed_with_pre_latest_is_not_used_for_old_rpc_versions() {
        let cache = Arc::new(PendingDataCache::new());
        let uut = PendingWatcher::new(cache.clone());

        let mut storage = pathfinder_storage::StorageBuilder::in_memory()
            .unwrap()
            .connection()
            .unwrap();

        let latest = latest_block();

        let tx = storage.transaction().unwrap();
        tx.insert_block_header(&latest).unwrap();

        let pending = valid_pre_confirmed_block_with_pre_latest(&latest);
        cache.store(pending.clone());

        let expected_empty_pending_data = PendingData::empty(&latest);

        let result = uut.get(&tx, RpcVersion::V06).unwrap();
        pretty_assertions_sorted::assert_eq_sorted!(result, expected_empty_pending_data);
        let result = uut.get(&tx, RpcVersion::V07).unwrap();
        pretty_assertions_sorted::assert_eq_sorted!(result, expected_empty_pending_data);
        let result = uut.get(&tx, RpcVersion::V08).unwrap();
        pretty_assertions_sorted::assert_eq_sorted!(result, expected_empty_pending_data);
    }

    #[test]
    fn invalid_pending_defaults_to_latest_in_storage() {
        // If the pending data isn't consistent with the latest data in storage,
        // then the result should be an empty block with the gas price, timestamp
        // and hash as parent hash of the latest block in storage.

        let cache = Arc::new(PendingDataCache::new());
        let uut = PendingWatcher::new(cache.clone());

        let mut storage = pathfinder_storage::StorageBuilder::in_memory()
            .unwrap()
            .connection()
            .unwrap();

        // Required otherwise latest doesn't have a valid parent hash in storage.
        let parent = BlockHeader::builder()
            .number(BlockNumber::GENESIS + 12)
            .finalize_with_hash(block_hash_bytes!(b"parent hash"));

        let latest = parent
            .child_builder()
            .eth_l1_gas_price(GasPrice(1234))
            .strk_l1_gas_price(GasPrice(3377))
            .eth_l1_data_gas_price(GasPrice(9999))
            .strk_l1_data_gas_price(GasPrice(8888))
            .l1_da_mode(L1DataAvailabilityMode::Blob)
            .timestamp(BlockTimestamp::new_or_panic(6777))
            .sequencer_address(sequencer_address!("0xffff"))
            .finalize_with_hash(block_hash_bytes!(b"latest hash"));

        let tx = storage.transaction().unwrap();
        tx.insert_block_header(&parent).unwrap();
        tx.insert_block_header(&latest).unwrap();

        let result = uut.get(&tx, RpcVersion::V09).unwrap();

        let expected = PendingData::empty(&latest);

        pretty_assertions_sorted::assert_eq_sorted!(result, expected);
    }

    #[test]
    fn invalid_pre_confirmed_defaults_to_latest_in_storage() {
        // If the pending data isn't consistent with the latest data in storage,
        // then the result should be an empty block with the gas price, timestamp
        // and hash as parent hash of the latest block in storage.

        let cache = Arc::new(PendingDataCache::new());
        let uut = PendingWatcher::new(cache.clone());

        let mut storage = pathfinder_storage::StorageBuilder::in_memory()
            .unwrap()
            .connection()
            .unwrap();

        // Required otherwise latest doesn't have a valid parent hash in storage.
        let parent = BlockHeader::builder()
            .number(BlockNumber::GENESIS + 12)
            .finalize_with_hash(block_hash_bytes!(b"parent hash"));

        let latest = parent
            .child_builder()
            .eth_l1_gas_price(GasPrice(1234))
            .strk_l1_gas_price(GasPrice(3377))
            .eth_l1_data_gas_price(GasPrice(9999))
            .strk_l1_data_gas_price(GasPrice(8888))
            .l1_da_mode(L1DataAvailabilityMode::Blob)
            .timestamp(BlockTimestamp::new_or_panic(6777))
            .sequencer_address(sequencer_address!("0xffff"))
            .finalize_with_hash(block_hash_bytes!(b"latest hash"));

        let tx = storage.transaction().unwrap();
        tx.insert_block_header(&parent).unwrap();
        tx.insert_block_header(&latest).unwrap();

        let pending = valid_pre_confirmed_block(&parent);
        cache.store(pending.clone());

        let result = uut.get(&tx, RpcVersion::V09).unwrap();

        let expected = empty_pre_confirmed_block(&latest);

        pretty_assertions_sorted::assert_eq_sorted!(result, expected);
    }

    #[test]
    fn invalid_pre_confirmed_with_pre_latest_defaults_to_latest_in_storage() {
        // If the pending data isn't consistent with the latest data in storage,
        // then the result should be an empty block with the gas price, timestamp
        // and hash as parent hash of the latest block in storage.

        let cache = Arc::new(PendingDataCache::new());
        let uut = PendingWatcher::new(cache.clone());

        let mut storage = pathfinder_storage::StorageBuilder::in_memory()
            .unwrap()
            .connection()
            .unwrap();

        // Required otherwise latest doesn't have a valid parent hash in storage.
        let parent1 = BlockHeader::builder()
            .number(BlockNumber::GENESIS + 12)
            .finalize_with_hash(block_hash_bytes!(b"parent1 hash"));

        let parent2 = parent1
            .child_builder()
            .eth_l1_gas_price(GasPrice(1234))
            .strk_l1_gas_price(GasPrice(3377))
            .eth_l1_data_gas_price(GasPrice(9999))
            .strk_l1_data_gas_price(GasPrice(8888))
            .l1_da_mode(L1DataAvailabilityMode::Blob)
            .timestamp(BlockTimestamp::new_or_panic(6777))
            .sequencer_address(sequencer_address!("0xffff"))
            .finalize_with_hash(block_hash_bytes!(b"paren2 hash"));

        let latest = parent2
            .child_builder()
            .eth_l1_gas_price(GasPrice(1234))
            .strk_l1_gas_price(GasPrice(3377))
            .eth_l1_data_gas_price(GasPrice(9999))
            .strk_l1_data_gas_price(GasPrice(8888))
            .l1_da_mode(L1DataAvailabilityMode::Blob)
            .timestamp(BlockTimestamp::new_or_panic(6777))
            .sequencer_address(sequencer_address!("0xffff"))
            .finalize_with_hash(block_hash_bytes!(b"latest hash"));

        let tx = storage.transaction().unwrap();
        tx.insert_block_header(&parent1).unwrap();
        tx.insert_block_header(&parent2).unwrap();
        tx.insert_block_header(&latest).unwrap();

        // Pre-latest block exists but is behind `== latest - 1` (because `== latest`
        // is still considered valid).
        let pending = valid_pre_confirmed_block_with_pre_latest(&parent1);
        cache.store(pending.clone());

        let result = uut.get(&tx, RpcVersion::V09).unwrap();

        let expected = empty_pre_confirmed_block(&latest);

        pretty_assertions_sorted::assert_eq_sorted!(result, expected);
    }

    #[test]
    #[should_panic]
    fn pre_confirmed_is_not_child_of_pre_latest_panics() {
        let cache = Arc::new(PendingDataCache::new());
        let uut = PendingWatcher::new(cache.clone());

        let mut storage = pathfinder_storage::StorageBuilder::in_memory()
            .unwrap()
            .connection()
            .unwrap();

        let parent = BlockHeader::builder()
            .number(BlockNumber::GENESIS + 12)
            .finalize_with_hash(block_hash_bytes!(b"parent hash"));

        let latest = parent
            .child_builder()
            .eth_l1_gas_price(GasPrice(1234))
            .strk_l1_gas_price(GasPrice(3377))
            .eth_l1_data_gas_price(GasPrice(9999))
            .strk_l1_data_gas_price(GasPrice(8888))
            .l1_da_mode(L1DataAvailabilityMode::Blob)
            .timestamp(BlockTimestamp::new_or_panic(6777))
            .sequencer_address(sequencer_address!("0xffff"))
            .finalize_with_hash(block_hash_bytes!(b"latest hash"));

        let tx = storage.transaction().unwrap();
        tx.insert_block_header(&parent).unwrap();
        tx.insert_block_header(&latest).unwrap();

        let pending = invalid_pre_confirmed_block_with_pre_latest(&latest);
        cache.store(pending.clone());
        let _ = uut.get(&tx, RpcVersion::V09).unwrap();
    }

    fn empty_pre_confirmed_block(latest: &BlockHeader) -> PendingData {
        let pre_confirmed = PreConfirmedBlock {
            number: latest.number + 1,
            l1_gas_price: GasPrices {
                price_in_wei: latest.eth_l1_gas_price,
                price_in_fri: latest.strk_l1_gas_price,
            },
            l1_data_gas_price: GasPrices {
                price_in_wei: latest.eth_l1_data_gas_price,
                price_in_fri: latest.strk_l1_data_gas_price,
            },
            l2_gas_price: GasPrices {
                price_in_wei: latest.eth_l2_gas_price,
                price_in_fri: latest.strk_l2_gas_price,
            },
            sequencer_address: latest.sequencer_address,
            status: Status::PreConfirmed,
            timestamp: latest.timestamp,
            starknet_version: latest.starknet_version,
            l1_da_mode: latest.l1_da_mode,
            transactions: vec![],
            transaction_receipts: vec![],
        };
        PendingData {
            blocks: Arc::new(PendingBlocks {
                pre_confirmed,
                pre_latest: None,
            }),
            state_update: StateUpdate::default().into(),
            aggregated_state_update: StateUpdate::default().into(),
            number: latest.number + 1,
        }
    }

    #[test]
    fn pre_confirmed_block_state_diff_conversion() {
        let json =
            starknet_gateway_test_fixtures::v0_14_0::preconfirmed_block::SEPOLIA_INTEGRATION_955821;
        let pre_confirmed_block: starknet_gateway_types::reply::PreConfirmedBlock =
            serde_json::from_str(json).unwrap();
        let number_of_pre_confirmed_transactions = pre_confirmed_block.transaction_receipts.len();
        let block_number = BlockNumber::new_or_panic(955821);

        // Convert the pre-confirmed block into pending data.
        let pending_data =
            PendingData::try_from_pre_confirmed_block(pre_confirmed_block.into(), block_number)
                .unwrap();

        assert_eq!(pending_data.pre_confirmed_block_number(), block_number);

        let expected_state_update = StateUpdate::default()
            .with_contract_nonce(
                contract_address!(
                    "0x352057331d5ad77465315d30b98135ddb815b86aa485d659dfeef59a904f88d"
                ),
                contract_nonce!("0x2d10e9"),
            )
            .with_storage_update(
                contract_address!(
                    "0x304d9d15c1c0ddb5824e0bd46cfb665c57a87ca5d5ed85d7f2348c6d29b2235"
                ),
                storage_address!("0x16c"),
                storage_value!("0x1d040cbb8281fe41c0ed888a970ea0747ad85e6740e772eb3c59172a437bbf"),
            )
            .with_storage_update(
                contract_address!(
                    "0x4718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d"
                ),
                storage_address!(
                    "0x3c204dd68b8e800b4f42e438d9ed4ccbba9f8e436518758cd36553715c1d6ab"
                ),
                storage_value!("0x15502e1d8fd6eaa9bb0"),
            )
            .with_storage_update(
                contract_address!(
                    "0x4718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d"
                ),
                storage_address!(
                    "0x5496768776e3db30053404f18067d81a6e06f5a2b0de326e21298fd9d569a9a"
                ),
                storage_value!("0x1cfaea14e6596648f874"),
            )
            .with_storage_update(
                contract_address!(
                    "0x505110514c6cd158678300c7678fdc63421f04dd2c12e1ce392dd0312f185e5"
                ),
                storage_address!("0x18d"),
                storage_value!("0x3db9b7cb22b4a3bd9f9799ea99decfd5e08ca5541f760992e8a503de253270f"),
            )
            .with_storage_update(
                contract_address!(
                    "0x505110514c6cd158678300c7678fdc63421f04dd2c12e1ce392dd0312f185e5"
                ),
                storage_address!("0x57"),
                storage_value!("0x23280cb06bd32f75b7646bf5dfabf4ab73f525ed8c02cab06888935be2f3abd"),
            );
        pretty_assertions_sorted::assert_eq_sorted!(
            &expected_state_update,
            pending_data.pre_confirmed_state_update().as_ref()
        );

        // We expect the transaction list to contain pre-confirmed transactions only.
        assert_eq!(
            number_of_pre_confirmed_transactions,
            pending_data.pre_confirmed_transactions().len()
        );
    }

    fn sample_tx(hash: TransactionHash) -> Transaction {
        Transaction {
            hash,
            ..Default::default()
        }
    }

    fn sample_receipt(hash: TransactionHash) -> Receipt {
        Receipt {
            transaction_hash: hash,
            ..Default::default()
        }
    }

    fn unwrap_pre_confirmed_err(transactions: Vec<Transaction>, receipts: Vec<Receipt>) -> String {
        let len = transactions.len().max(receipts.len());
        let block = starknet_gateway_types::reply::PreConfirmedBlock {
            transactions,
            transaction_receipts: receipts.into_iter().map(|r| Some((r, vec![]))).collect(),
            transaction_state_diffs: vec![None; len],
            ..Default::default()
        };
        PendingData::try_from_pre_confirmed_block(Box::new(block), BlockNumber::new_or_panic(1))
            .unwrap_err()
            .to_string()
    }

    #[test]
    fn pre_confirmed_block_with_tx_missing_receipt() {
        let tx_hash = transaction_hash_bytes!(b"tx a");
        let err = unwrap_pre_confirmed_err(
            vec![sample_tx(tx_hash)],
            vec![sample_receipt(transaction_hash_bytes!(b"tx b"))],
        );
        assert_eq!(
            err,
            format!("Missing transaction receipt for tx ({tx_hash})")
        );
    }

    #[test]
    fn pre_confirmed_block_with_more_txs_than_receipts() {
        let tx_b_hash = transaction_hash_bytes!(b"tx b");
        let err = unwrap_pre_confirmed_err(
            vec![
                sample_tx(transaction_hash_bytes!(b"tx a")),
                sample_tx(tx_b_hash),
            ],
            vec![sample_receipt(transaction_hash_bytes!(b"tx a"))],
        );
        assert_eq!(
            err,
            format!("Missing transaction receipt for tx ({tx_b_hash})")
        );
    }

    #[test]
    fn pre_confirmed_block_with_more_receipts_than_txs() {
        let err = unwrap_pre_confirmed_err(
            vec![sample_tx(transaction_hash_bytes!(b"tx a"))],
            vec![
                sample_receipt(transaction_hash_bytes!(b"tx a")),
                sample_receipt(transaction_hash_bytes!(b"tx b")),
            ],
        );
        assert_eq!(
            err,
            "Mismatched transaction and receipt count in pre-confirmed block"
        );
    }
}
