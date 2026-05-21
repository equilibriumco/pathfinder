use std::collections::HashSet;
use std::sync::Arc;

use pathfinder_common::event::Event;
use pathfinder_common::receipt::Receipt;
use pathfinder_common::transaction::Transaction;
use pathfinder_common::{
    BlockHash,
    BlockHeader,
    BlockNumber,
    BlockTimestamp,
    ClassHash,
    ContractAddress,
    ContractNonce,
    FoundStorageValue,
    L1DataAvailabilityMode,
    SequencerAddress,
    StarknetVersion,
    StateCommitment,
    StateUpdate,
    StorageAddress,
    TransactionHash,
};
use starknet_gateway_types::reply::{GasPrices, Status};

pub type TxnReceiptAndEvents = (Receipt, Vec<Event>);

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

    pub transactions: Vec<Transaction>,

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

    pub transactions: Vec<Transaction>,

    pub transaction_receipts: Vec<TxnReceiptAndEvents>,
}

#[derive(Clone, Default, Debug, PartialEq)]
pub struct PreLatestData {
    pub block: PreLatestBlock,
    pub state_update: StateUpdate,
}

/// Chain data observed in flight: the pre-confirmed block, an optional
/// pre-latest parent, and any candidate transactions.
#[derive(Clone, Default, Debug, PartialEq)]
pub struct PendingBlocks {
    pub pre_confirmed: PreConfirmedBlock,
    pub pre_latest: Option<PreLatestData>,
    pub candidate_transactions: Vec<Transaction>,
}

impl PendingBlocks {
    pub fn transactions(&self) -> &[Transaction] {
        &self.pre_confirmed.transactions
    }

    pub fn pre_latest_transactions(&self) -> Option<&[Transaction]> {
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
}

#[derive(Clone, Default, Debug, PartialEq)]
pub struct PendingData {
    blocks: Arc<PendingBlocks>,
    state_update: Arc<StateUpdate>,
    aggregated_state_update: Arc<StateUpdate>,
    number: BlockNumber,
}

impl PendingData {
    #[doc(hidden)]
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

    /// Converts a pre-confirmed block from the gateway into pending data.
    /// Candidate transactions are filtered out; the state update is composed
    /// from the per-transaction diffs.
    pub fn try_from_pre_confirmed_block(
        block: Box<starknet_gateway_types::reply::PreConfirmedBlock>,
        number: BlockNumber,
    ) -> anyhow::Result<Self> {
        Self::try_from_pre_confirmed_and_pre_latest(block, number, None)
    }

    /// Same as [`Self::try_from_pre_confirmed_block`] but also accepts an
    /// optional pre-latest parent. When present, the pre-confirmed block
    /// must be its child.
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
        // Drop placeholder Nones from receipts.
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

        // Compose aggregated state diff for the pre-confirmed block.
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

    /// An empty pending block synthesised from the latest finalised header.
    pub fn empty(latest: &BlockHeader) -> Self {
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

    #[doc(hidden)]
    pub fn pre_confirmed_block_number_mut(&mut self) -> &mut BlockNumber {
        &mut self.number
    }

    /// Synthesise a `BlockHeader` from the pre-confirmed block. Fields that
    /// the pre-confirmed block does not yet know (hash, commitments) are
    /// left as defaults.
    pub fn pre_confirmed_header(&self) -> BlockHeader {
        let block = &self.blocks.pre_confirmed;
        BlockHeader {
            parent_hash: BlockHash::ZERO,
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

    /// Synthesise a `BlockHeader` from the pre-latest block, if it exists.
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

    pub fn pending_block(&self) -> Arc<PendingBlocks> {
        Arc::clone(&self.blocks)
    }

    pub fn pre_latest_block(&self) -> Option<Arc<PreLatestBlock>> {
        self.blocks
            .pre_latest
            .as_ref()
            .map(|data| Arc::new(data.block.clone()))
    }

    pub fn pre_confirmed_state_update(&self) -> Arc<StateUpdate> {
        Arc::clone(&self.state_update)
    }

    /// Combined state update from pre-latest (if any) and pre-confirmed.
    pub fn aggregated_state_update(&self) -> Arc<StateUpdate> {
        Arc::clone(&self.aggregated_state_update)
    }

    pub fn pre_confirmed_transactions(&self) -> &[Transaction] {
        self.blocks.transactions()
    }

    pub fn pre_latest_transactions(&self) -> Option<&[Transaction]> {
        self.blocks.pre_latest_transactions()
    }

    pub fn pre_confirmed_tx_receipts_and_events(&self) -> &[TxnReceiptAndEvents] {
        self.blocks.tx_receipts_and_events()
    }

    pub fn pre_latest_tx_receipts_and_events(&self) -> Option<&[TxnReceiptAndEvents]> {
        self.blocks.pre_latest_tx_receipts_and_events()
    }

    pub fn candidate_transactions(&self) -> &[Transaction] {
        &self.blocks.candidate_transactions
    }

    /// Look up a contract nonce across the aggregated pending state.
    pub fn find_nonce(&self, contract_address: ContractAddress) -> Option<ContractNonce> {
        self.aggregated_state_update()
            .contract_nonce(contract_address)
    }

    /// Look up a storage value across the aggregated pending state.
    pub fn find_storage_value(
        &self,
        contract_address: ContractAddress,
        storage_address: StorageAddress,
    ) -> Option<FoundStorageValue> {
        self.aggregated_state_update()
            .storage_value_with_provenance(contract_address, storage_address)
    }

    /// Look up a transaction by hash across pre-confirmed, candidate, and
    /// pre-latest, in that order.
    pub fn find_transaction(&self, tx_hash: TransactionHash) -> Option<Transaction> {
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

    /// Look up the declared class hash for a contract across pending state.
    pub fn find_contract_class(&self, contract_address: ContractAddress) -> Option<ClassHash> {
        self.aggregated_state_update()
            .contract_class(contract_address)
    }

    /// True if the given class hash is declared in pending state.
    pub fn class_is_declared(&self, class_hash: ClassHash) -> bool {
        self.aggregated_state_update().class_is_declared(class_hash)
    }

    /// True if `block` matches the pre-latest or pre-confirmed block number.
    pub fn is_pre_latest_or_pre_confirmed(&self, block: BlockNumber) -> bool {
        self.pre_latest_block_number()
            .is_some_and(|pre_latest| pre_latest == block)
            || self.pre_confirmed_block_number() == block
    }
}
