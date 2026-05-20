use std::collections::HashSet;
use std::sync::Arc;

use anyhow::Context;
use pathfinder_common::{
    BlockHash,
    BlockHeader,
    BlockNumber,
    BlockTimestamp,
    L1DataAvailabilityMode,
    SequencerAddress,
    StarknetVersion,
    StateCommitment,
    StateUpdate,
};
use pathfinder_storage::Transaction;
use starknet_gateway_types::reply::{GasPrices, Status};
use tokio::sync::watch::Receiver as WatchReceiver;
use tokio::sync::Notify;

use crate::RpcVersion;

type TxnReceiptAndEvents = (
    pathfinder_common::receipt::Receipt,
    Vec<pathfinder_common::event::Event>,
);

/// A finalized transaction along with its receipt, events, status and the block
/// number it was included in.
pub struct FinalizedTxData {
    pub block_number: BlockNumber,
    pub transaction: pathfinder_common::transaction::Transaction,
    pub receipt: pathfinder_common::receipt::Receipt,
    pub events: Vec<pathfinder_common::event::Event>,
    pub finality_status: crate::dto::TxnFinalityStatus,
}

/// Provides the latest [PendingData] which is consistent with a given
/// view of storage.
///
/// Reading data fires `on_read`, signalling the upstream polling loop
/// that the cache is in use.
#[derive(Clone)]
pub struct PendingWatcher {
    receiver: WatchReceiver<PendingData>,
    on_read: Arc<Notify>,
}

#[derive(Clone, Default, Debug, PartialEq)]
pub struct PreConfirmedBlock {
    pub number: BlockNumber,

    pub l1_gas_price: GasPrices,
    pub l1_data_gas_price: GasPrices,
    pub l2_gas_price: GasPrices,

    pub sequencer_address: SequencerAddress,
    pub status: Status,
    pub timestamp: BlockTimestamp,
    pub starknet_version: StarknetVersion,
    pub l1_da_mode: L1DataAvailabilityMode,

    pub transactions: Vec<pathfinder_common::transaction::Transaction>,

    pub transaction_receipts: Vec<TxnReceiptAndEvents>,
}

#[derive(Clone, Default, Debug, PartialEq)]
pub struct PreLatestBlock {
    pub number: BlockNumber,

    pub parent_hash: BlockHash,

    pub l1_gas_price: GasPrices,
    pub l1_data_gas_price: GasPrices,
    pub l2_gas_price: GasPrices,

    pub sequencer_address: SequencerAddress,
    pub status: Status,
    pub timestamp: BlockTimestamp,
    pub starknet_version: StarknetVersion,
    pub l1_da_mode: L1DataAvailabilityMode,

    pub transactions: Vec<pathfinder_common::transaction::Transaction>,

    pub transaction_receipts: Vec<TxnReceiptAndEvents>,
}

#[derive(Clone, Default, Debug, PartialEq)]
pub struct PreLatestData {
    pub block: PreLatestBlock,
    pub state_update: StateUpdate,
}

/// Currently known chain data that is yet to be confirmed on L2
#[derive(Clone, Default, Debug, PartialEq)]
pub struct PendingBlocks {
    /// Pre confirmed block, sequencer's nightly state
    pub pre_confirmed: PreConfirmedBlock,
    /// Pre-latest parent of pre-confirmed block, exists if not yet confirmed
    pub pre_latest: Option<PreLatestData>,
    /// Txs submitted but not yet executed
    pub candidate_transactions: Vec<pathfinder_common::transaction::Transaction>,
}

impl PendingBlocks {
    pub fn transactions(&self) -> &[pathfinder_common::transaction::Transaction] {
        &self.pre_confirmed.transactions
    }

    pub fn pre_latest_transactions(
        &self,
    ) -> Option<&[pathfinder_common::transaction::Transaction]> {
        self.pre_latest
            .as_ref()
            .map(|data| data.block.transactions.as_slice())
    }

    pub fn tx_receipts_and_events(&self) -> &[TxnReceiptAndEvents] {
        &self.pre_confirmed.transaction_receipts
    }

    pub fn pre_latest_tx_receipts_and_events(&self) -> Option<&[TxnReceiptAndEvents]> {
        self.pre_latest
            .as_ref()
            .map(|data| data.block.transaction_receipts.as_slice())
    }

    pub fn finality_status(&self) -> crate::dto::TxnFinalityStatus {
        // For more info:
        //  - on why `AcceptedOnL2` is wrong: https://github.com/equilibriumco/pathfinder/issues/3259
        //  - on why `PendingBlockVariant::Pending` case was dead code:
        //  https://github.com/equilibriumco/pathfinder/issues/3272
        crate::dto::TxnFinalityStatus::PreConfirmed
    }
}

#[derive(Clone, Default, Debug, PartialEq)]
pub struct PendingData {
    /// The blocks container, holding pre-confirmed, pre-latest, and candidate
    /// data.
    blocks: Arc<PendingBlocks>,
    /// The state update of the pre-confirmed block.
    ///
    /// Does not include the [pre-latest](PreLatestData) state update.
    state_update: Arc<StateUpdate>,
    /// The aggregated state update. Contains the merged state update from
    /// the pre-latest block (if exists) and the pre-confirmed block.
    aggregated_state_update: Arc<StateUpdate>,
    /// The block number of the pre-confirmed block.
    number: BlockNumber,
}

impl PendingData {
    #[cfg(test)]
    pub fn from_parts(
        blocks: PendingBlocks,
        state_update: StateUpdate,
        aggregated_state_update: StateUpdate,
        number: BlockNumber,
    ) -> Self {
        Self {
            blocks: Arc::new(blocks),
            state_update: Arc::new(state_update),
            aggregated_state_update: Arc::new(aggregated_state_update),
            number,
        }
    }

    /// Converts a pre-confirmed block fetched from the gateway into pending
    /// data.
    ///
    /// Candidate transactions are filtered out and handled separately. State
    /// update is constructed from the per-transaction updates.
    pub fn try_from_pre_confirmed_block(
        block: Box<starknet_gateway_types::reply::PreConfirmedBlock>,
        number: BlockNumber,
    ) -> anyhow::Result<Self> {
        Self::try_from_pre_confirmed_and_pre_latest(block, number, None)
    }

    /// Converts a pre-confirmed block and optional pre-latest block fetched
    /// from the gateway into pending data.
    ///
    /// Candidate transactions are filtered out and handled separately. State
    /// update is constructed from the per-transaction updates.
    ///
    /// If the pre-latest block exists, the pre-confirmed block is expected to
    /// be its child.
    pub fn try_from_pre_confirmed_and_pre_latest(
        pre_confirmed_block: Box<starknet_gateway_types::reply::PreConfirmedBlock>,
        pre_confirmed_block_number: BlockNumber,
        pre_latest_data: Option<
            Box<(
                BlockNumber,
                starknet_gateway_types::reply::PreLatestBlock,
                StateUpdate,
            )>,
        >,
    ) -> anyhow::Result<Self> {
        // Get rid of Nones in transaction receipt
        let transaction_receipts: Vec<_> = pre_confirmed_block
            .transaction_receipts
            .into_iter()
            .flatten()
            .collect();

        let pre_confirmed_transaction_hashes: HashSet<_> = transaction_receipts
            .iter()
            .map(|(receipt, _)| receipt.transaction_hash)
            .collect();
        let (pre_confirmed_transactions, candidate_transactions): (Vec<_>, Vec<_>) =
            pre_confirmed_block
                .transactions
                .into_iter()
                .partition(|tx| pre_confirmed_transaction_hashes.contains(&tx.hash));

        if transaction_receipts.len() != pre_confirmed_transactions.len() {
            anyhow::bail!("Mismatched transaction and receipt count in pre-confirmed block");
        }

        // Compute aggregated state diff for the pre-confirmed block.
        let mut pre_confirmed_state_diff =
            starknet_gateway_types::reply::state_update::StateDiff::default();
        for transaction_diff in pre_confirmed_block
            .transaction_state_diffs
            .into_iter()
            .flatten()
        {
            pre_confirmed_state_diff.extend(transaction_diff);
        }
        pre_confirmed_state_diff.deduplicate();

        let pre_confirmed_state_update = {
            let state_update = starknet_gateway_types::reply::StateUpdate {
                state_diff: pre_confirmed_state_diff.clone(),
                block_hash: Default::default(),
                new_root: StateCommitment::default(),
                old_root: StateCommitment::default(),
            };
            Arc::new(StateUpdate::from(state_update))
        };

        let pre_confirmed_block = PreConfirmedBlock {
            number: pre_confirmed_block_number,
            l1_gas_price: pre_confirmed_block.l1_gas_price,
            l1_data_gas_price: pre_confirmed_block.l1_data_gas_price,
            l2_gas_price: pre_confirmed_block.l2_gas_price,
            sequencer_address: pre_confirmed_block.sequencer_address,
            status: Status::PreConfirmed,
            timestamp: pre_confirmed_block.timestamp,
            starknet_version: pre_confirmed_block.starknet_version,
            l1_da_mode: pre_confirmed_block.l1_da_mode.into(),
            transactions: pre_confirmed_transactions,
            transaction_receipts,
        };

        let pre_latest_data = pre_latest_data.map(|pre_latest| {
            let (pre_latest_block_number, pre_latest_block, pre_latest_state_update) = *pre_latest;
            assert_eq!(
                pre_latest_block_number + 1,
                pre_confirmed_block_number,
                "Pre-confirmed block should be child of pre-latest"
            );
            let pre_latest_block = PreLatestBlock {
                number: pre_latest_block_number,
                parent_hash: pre_latest_block.parent_hash,
                l1_gas_price: pre_latest_block.l1_gas_price,
                l1_data_gas_price: pre_latest_block.l1_data_gas_price,
                l2_gas_price: pre_latest_block.l2_gas_price,
                sequencer_address: pre_latest_block.sequencer_address,
                status: Status::Pending,
                timestamp: pre_latest_block.timestamp,
                starknet_version: pre_latest_block.starknet_version,
                l1_da_mode: pre_latest_block.l1_da_mode.into(),
                transactions: pre_latest_block.transactions,
                transaction_receipts: pre_latest_block.transaction_receipts,
            };
            PreLatestData {
                block: pre_latest_block,
                state_update: pre_latest_state_update,
            }
        });

        let aggregated_state_update = Arc::new(
            pre_latest_data
                .clone()
                .map(|data| data.state_update)
                .unwrap_or_default()
                .apply(pre_confirmed_state_update.as_ref()),
        );

        Ok(Self {
            blocks: Arc::new(PendingBlocks {
                pre_confirmed: pre_confirmed_block,
                pre_latest: pre_latest_data,
                candidate_transactions,
            }),
            state_update: pre_confirmed_state_update,
            aggregated_state_update,
            number: pre_confirmed_block_number,
        })
    }

    fn empty(latest: &BlockHeader) -> Self {
        let block = PreConfirmedBlock {
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
        let state_update =
            Arc::new(StateUpdate::default().with_parent_state_commitment(latest.state_commitment));
        Self {
            blocks: Arc::new(PendingBlocks {
                pre_confirmed: block,
                candidate_transactions: vec![],
                pre_latest: None,
            }),
            state_update: Arc::clone(&state_update),
            aggregated_state_update: state_update,
            number: latest.number + 1,
        }
    }

    pub fn pre_confirmed_block_number(&self) -> BlockNumber {
        self.number
    }

    pub fn pre_latest_block_number(&self) -> Option<BlockNumber> {
        self.blocks
            .pre_latest
            .as_ref()
            .map(|data| data.block.number)
    }

    /// Returns a mutable reference to the block number.
    #[cfg(test)]
    pub fn pre_confirmed_block_number_mut(&mut self) -> &mut BlockNumber {
        &mut self.number
    }

    /// Get the header of the pre-confirmed block.
    pub fn pre_confirmed_header(&self) -> BlockHeader {
        let block = &self.blocks.pre_confirmed;
        BlockHeader {
            // Pre-confirmed blocks do not have a parent hash.
            parent_hash: pathfinder_common::BlockHash::ZERO,
            number: self.number,
            timestamp: block.timestamp,
            eth_l1_gas_price: block.l1_gas_price.price_in_wei,
            strk_l1_gas_price: block.l1_gas_price.price_in_fri,
            eth_l1_data_gas_price: block.l1_data_gas_price.price_in_wei,
            strk_l1_data_gas_price: block.l1_data_gas_price.price_in_fri,
            eth_l2_gas_price: block.l2_gas_price.price_in_wei,
            strk_l2_gas_price: block.l2_gas_price.price_in_fri,
            sequencer_address: block.sequencer_address,
            starknet_version: block.starknet_version,
            // Pre-confirmed block does not know what these are yet.
            hash: Default::default(),
            event_commitment: Default::default(),
            state_commitment: Default::default(),
            transaction_commitment: Default::default(),
            transaction_count: Default::default(),
            event_count: Default::default(),
            l1_da_mode: block.l1_da_mode,
            receipt_commitment: Default::default(),
            state_diff_commitment: Default::default(),
            state_diff_length: Default::default(),
        }
    }

    /// Get the header of the pre-latest block, if it exists.
    pub fn pre_latest_header(&self) -> Option<BlockHeader> {
        self.blocks.pre_latest.as_ref().map(|data| {
            let pre_latest_block = &data.block;
            BlockHeader {
                parent_hash: pre_latest_block.parent_hash,
                number: pre_latest_block.number,
                timestamp: pre_latest_block.timestamp,
                eth_l1_gas_price: pre_latest_block.l1_gas_price.price_in_wei,
                strk_l1_gas_price: pre_latest_block.l1_gas_price.price_in_fri,
                eth_l1_data_gas_price: pre_latest_block.l1_data_gas_price.price_in_wei,
                strk_l1_data_gas_price: pre_latest_block.l1_data_gas_price.price_in_fri,
                eth_l2_gas_price: pre_latest_block.l2_gas_price.price_in_wei,
                strk_l2_gas_price: pre_latest_block.l2_gas_price.price_in_fri,
                sequencer_address: pre_latest_block.sequencer_address,
                starknet_version: pre_latest_block.starknet_version,
                // Pre-latest block does not know what these are yet.
                hash: Default::default(),
                event_commitment: Default::default(),
                state_commitment: Default::default(),
                transaction_commitment: Default::default(),
                transaction_count: Default::default(),
                event_count: Default::default(),
                l1_da_mode: pre_latest_block.l1_da_mode,
                receipt_commitment: Default::default(),
                state_diff_commitment: Default::default(),
                state_diff_length: Default::default(),
            }
        })
    }

    /// Get the pending blocks container.
    pub fn pending_block(&self) -> Arc<PendingBlocks> {
        Arc::clone(&self.blocks)
    }

    /// Get the pre-latest block, if it exists.
    pub fn pre_latest_block(&self) -> Option<Arc<PreLatestBlock>> {
        self.blocks
            .pre_latest
            .as_ref()
            .map(|data| Arc::new(data.block.clone()))
    }

    /// Get the state update of the pre-confirmed block.
    pub fn pre_confirmed_state_update(&self) -> Arc<StateUpdate> {
        Arc::clone(&self.state_update)
    }

    /// Get the aggregated state update from the pre-latest (if exists) and the
    /// pre-confirmed block.
    pub fn aggregated_state_update(&self) -> Arc<StateUpdate> {
        Arc::clone(&self.aggregated_state_update)
    }

    /// Get the transactions in the pre-confirmed block.
    pub fn pre_confirmed_transactions(&self) -> &[pathfinder_common::transaction::Transaction] {
        self.blocks.transactions()
    }

    /// Get the transactions in the pre-latest block, if it exists.
    pub fn pre_latest_transactions(
        &self,
    ) -> Option<&[pathfinder_common::transaction::Transaction]> {
        self.blocks.pre_latest_transactions()
    }

    /// Get the transaction receipts and events in the pre-confirmed block.
    pub fn pre_confirmed_tx_receipts_and_events(&self) -> &[TxnReceiptAndEvents] {
        self.blocks.tx_receipts_and_events()
    }

    /// Get the transaction receipts and events in the pre-latest block, if it
    /// exists.
    pub fn pre_latest_tx_receipts_and_events(&self) -> Option<&[TxnReceiptAndEvents]> {
        self.blocks.pre_latest_tx_receipts_and_events()
    }

    /// Get the candidate transactions in the pre-confirmed block.
    pub fn candidate_transactions(&self) -> &[pathfinder_common::transaction::Transaction] {
        &self.blocks.candidate_transactions
    }

    pub fn finality_status(&self) -> crate::dto::TxnFinalityStatus {
        self.blocks.finality_status()
    }

    /// Find a contract nonce by its contract address in the
    /// pre-confirmed or pre-latest block (in that order).
    pub fn find_nonce(
        &self,
        contract_address: pathfinder_common::ContractAddress,
    ) -> Option<pathfinder_common::ContractNonce> {
        self.aggregated_state_update()
            .contract_nonce(contract_address)
    }

    /// Find a storage value by its contract and storage address in
    /// the pre-confirmed or pre-latest block (in that order).
    pub fn find_storage_value(
        &self,
        contract_address: pathfinder_common::ContractAddress,
        storage_address: pathfinder_common::StorageAddress,
    ) -> Option<pathfinder_common::FoundStorageValue> {
        self.aggregated_state_update()
            .storage_value_with_provenance(contract_address, storage_address)
    }

    /// Find a transaction by its hash in the pre-confirmed block,
    /// candidate transactions, or pre-latest block (in that order).
    pub fn find_transaction(
        &self,
        tx_hash: pathfinder_common::TransactionHash,
    ) -> Option<pathfinder_common::transaction::Transaction> {
        self.pre_confirmed_transactions()
            .iter()
            .find(|tx| tx.hash == tx_hash)
            .cloned()
            .or_else(|| {
                self.candidate_transactions()
                    .iter()
                    .find(|tx| tx.hash == tx_hash)
                    .cloned()
            })
            .or_else(|| {
                self.pre_latest_transactions()
                    .and_then(|pre_latest| pre_latest.iter().find(|tx| tx.hash == tx_hash).cloned())
            })
    }

    /// Find a [FinalizedTxData] by the transaction hash in the
    /// pre-confirmed or pre-latest block (in that order).
    ///
    /// This function does not check candidate transactions, as they are not
    /// finalized.
    pub fn find_finalized_tx_data(
        &self,
        tx_hash: pathfinder_common::TransactionHash,
    ) -> Option<FinalizedTxData> {
        let pending_tx = self
            .pre_confirmed_transactions()
            .iter()
            .find(|tx| tx.hash == tx_hash);
        if let Some(pending_tx) = pending_tx {
            let (receipt, events) = self
                .pre_confirmed_tx_receipts_and_events()
                .iter()
                .find(|(receipt, _)| receipt.transaction_hash == tx_hash)
                .cloned()
                .expect("Should exist if transaction exists");

            return Some(FinalizedTxData {
                block_number: self.pre_confirmed_block_number(),
                transaction: pending_tx.clone(),
                receipt,
                events,
                finality_status: self.finality_status(),
            });
        }

        if let Some(pre_latest_block) = self.pre_latest_block() {
            let pre_latest_tx = pre_latest_block
                .transactions
                .iter()
                .find(|tx| tx.hash == tx_hash);
            if let Some(pre_latest_tx) = pre_latest_tx {
                let (receipt, events) = self
                    .pre_latest_tx_receipts_and_events()
                    .and_then(|receipts| {
                        receipts
                            .iter()
                            .find(|(receipt, _)| receipt.transaction_hash == tx_hash)
                            .cloned()
                    })
                    .expect("Should exist if transaction exists");

                return Some(FinalizedTxData {
                    block_number: pre_latest_block.number,
                    transaction: pre_latest_tx.clone(),
                    receipt,
                    events,
                    finality_status: crate::dto::TxnFinalityStatus::PreConfirmed,
                });
            }
        }

        None
    }

    /// Find a contract class hash by its contract address in the
    /// pre-confirmed or pre-latest block (in that order).
    pub fn find_contract_class(
        &self,
        contract_address: pathfinder_common::ContractAddress,
    ) -> Option<pathfinder_common::ClassHash> {
        self.aggregated_state_update()
            .contract_class(contract_address)
    }

    /// Check if a class hash has been declared in the pre-confirmed
    /// or pre-latest block.
    pub fn class_is_declared(&self, class_hash: pathfinder_common::ClassHash) -> bool {
        self.aggregated_state_update().class_is_declared(class_hash)
    }

    pub fn is_pre_latest_or_pre_confirmed(&self, block: BlockNumber) -> bool {
        self.pre_latest_block_number()
            .is_some_and(|pre_latest| pre_latest == block)
            || self.pre_confirmed_block_number() == block
    }
}

impl PendingWatcher {
    pub fn new(receiver: WatchReceiver<PendingData>) -> Self {
        Self {
            receiver,
            on_read: Arc::new(Notify::new()),
        }
    }

    /// Handle for awaiting cache-read events fired by [`Self::get`].
    pub fn on_read(&self) -> Arc<Notify> {
        self.on_read.clone()
    }

    /// A fresh receiver for awaiting changes directly.
    pub fn subscribe(&self) -> WatchReceiver<PendingData> {
        self.receiver.clone()
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
    ) -> anyhow::Result<PendingData> {
        self.on_read.notify_one();

        let latest = tx
            .block_header(pathfinder_common::BlockId::Latest)
            .context("Querying latest block header")?
            .unwrap_or_default();

        let watched_pending_data = self.receiver.borrow();
        let watched_pending_blocks = watched_pending_data.pending_block();
        let PendingBlocks {
            pre_confirmed,
            pre_latest,
            candidate_transactions,
        } = watched_pending_blocks.as_ref();
        // The pre-confirmed block is to be only ever used on JSON-RPC 0.9 and up.
        // Older versions did have the semantics that expected that pending block
        // contents are L2_ACCEPTED, which is not the case for the pre-confirmed
        // block.
        let pending_data = if rpc_version >= RpcVersion::V09 {
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
                            candidate_transactions: candidate_transactions.clone(),
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
                        candidate_transactions: candidate_transactions.clone(),
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
        } else {
            PendingData::empty(&latest)
        };

        Ok(pending_data)
    }

    #[cfg(test)]
    pub fn get_unchecked(&self) -> PendingData {
        self.receiver.borrow().clone()
    }
}

#[cfg(test)]
mod tests {

    use pathfinder_common::macro_prelude::*;
    use pathfinder_common::{BlockHeader, BlockTimestamp, GasPrice, L1DataAvailabilityMode};

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
                candidate_transactions: vec![],
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
            transactions: vec![pathfinder_common::transaction::Transaction::default()],
            transaction_receipts: vec![(pathfinder_common::receipt::Receipt::default(), vec![])],
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
                    transactions: vec![pathfinder_common::transaction::Transaction::default()],
                    transaction_receipts: vec![(
                        pathfinder_common::receipt::Receipt::default(),
                        vec![],
                    )],
                },
                candidate_transactions: vec![],
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
                candidate_transactions: vec![],
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
        let (sender, receiver) = tokio::sync::watch::channel(Default::default());
        let uut = PendingWatcher::new(receiver);

        let mut storage = pathfinder_storage::StorageBuilder::in_memory()
            .unwrap()
            .connection()
            .unwrap();

        let latest = latest_block();

        let tx = storage.transaction().unwrap();
        tx.insert_block_header(&latest).unwrap();

        let pending = valid_pre_confirmed_block(&latest);
        sender.send(pending.clone()).unwrap();

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
        let (sender, receiver) = tokio::sync::watch::channel(Default::default());
        let uut = PendingWatcher::new(receiver);

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
        sender.send(pending.clone()).unwrap();

        let result = uut.get(&tx, RpcVersion::V09).unwrap();
        pretty_assertions_sorted::assert_eq_sorted!(result, pending);

        // Pre-latest block will be same as `latest` which is also valid, but in this
        // case the pre-latest block should be ignored.
        let pending = valid_pre_confirmed_block_with_pre_latest(&parent);
        sender.send_replace(pending);

        let result = uut.get(&tx, RpcVersion::V09).unwrap();
        // We got a non-empty pre-confirmed block..
        assert!(!result.pre_confirmed_transactions().is_empty());
        // ..and we did not receive a pre-latest block.
        assert!(result.pre_latest_block().is_none());
    }

    #[test]
    fn valid_pre_confirmed_is_not_used_for_old_rpc_versions() {
        let (sender, receiver) = tokio::sync::watch::channel(Default::default());
        let uut = PendingWatcher::new(receiver);

        let mut storage = pathfinder_storage::StorageBuilder::in_memory()
            .unwrap()
            .connection()
            .unwrap();

        let latest = latest_block();

        let tx = storage.transaction().unwrap();
        tx.insert_block_header(&latest).unwrap();

        let pending = valid_pre_confirmed_block(&latest);
        sender.send(pending.clone()).unwrap();

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
        let (sender, receiver) = tokio::sync::watch::channel(Default::default());
        let uut = PendingWatcher::new(receiver);

        let mut storage = pathfinder_storage::StorageBuilder::in_memory()
            .unwrap()
            .connection()
            .unwrap();

        let latest = latest_block();

        let tx = storage.transaction().unwrap();
        tx.insert_block_header(&latest).unwrap();

        let pending = valid_pre_confirmed_block_with_pre_latest(&latest);
        sender.send(pending.clone()).unwrap();

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

        let (_sender, receiver) = tokio::sync::watch::channel(Default::default());
        let uut = PendingWatcher::new(receiver);

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

        let (sender, receiver) = tokio::sync::watch::channel(Default::default());
        let uut = PendingWatcher::new(receiver);

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
        sender.send(pending.clone()).unwrap();

        let result = uut.get(&tx, RpcVersion::V09).unwrap();

        let expected = empty_pre_confirmed_block(&latest);

        pretty_assertions_sorted::assert_eq_sorted!(result, expected);
    }

    #[test]
    fn invalid_pre_confirmed_with_pre_latest_defaults_to_latest_in_storage() {
        // If the pending data isn't consistent with the latest data in storage,
        // then the result should be an empty block with the gas price, timestamp
        // and hash as parent hash of the latest block in storage.

        let (sender, receiver) = tokio::sync::watch::channel(Default::default());
        let uut = PendingWatcher::new(receiver);

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
        sender.send(pending.clone()).unwrap();

        let result = uut.get(&tx, RpcVersion::V09).unwrap();

        let expected = empty_pre_confirmed_block(&latest);

        pretty_assertions_sorted::assert_eq_sorted!(result, expected);
    }

    #[test]
    #[should_panic]
    fn pre_confirmed_is_not_child_of_pre_latest_panics() {
        let (sender, receiver) = tokio::sync::watch::channel(Default::default());
        let uut = PendingWatcher::new(receiver);

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
        sender.send(pending.clone()).unwrap();
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
                candidate_transactions: vec![],
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
        let mut pre_confirmed_block: starknet_gateway_types::reply::PreConfirmedBlock =
            serde_json::from_str(json).unwrap();
        let number_of_pre_confirmed_transactions = pre_confirmed_block.transaction_receipts.len();
        let block_number = BlockNumber::new_or_panic(955821);

        // Unfortunately that fixture does not contain a _candidate_ transaction, so
        // just push one on to the end of the list.
        let candidate_transaction = pathfinder_common::transaction::Transaction {
            hash: transaction_hash!(
                "0x352057331d5ad77465315d30b98135ddb815b86aa485d659dfeef59a904f88d"
            ),
            variant: pathfinder_common::transaction::TransactionVariant::InvokeV3(
                pathfinder_common::transaction::InvokeTransactionV3 {
                    ..Default::default()
                },
            ),
        };
        pre_confirmed_block
            .transactions
            .push(candidate_transaction.clone());
        pre_confirmed_block.transaction_receipts.push(None);
        pre_confirmed_block.transaction_state_diffs.push(None);

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

        // And the single candidate transaction we've added.
        assert_eq!(
            &vec![candidate_transaction],
            pending_data.candidate_transactions()
        );
    }
}
