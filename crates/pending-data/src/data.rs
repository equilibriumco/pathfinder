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

// TODO consider removing, because we're gonna be setting the parent hash to
// zero anyway.
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

impl PreLatestData {
    /// Block header for this un-committed parent block.
    pub fn header(&self) -> BlockHeader {
        let block = &self.block;
        BlockHeader {
            parent_hash: block.parent_hash,
            number: block.number,
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
}

/// Chain data observed in flight: the pre-confirmed block and every
/// uncommitted parent block below it.
#[derive(Clone, Default, Debug, PartialEq)]
pub struct PendingBlocks {
    pub pre_confirmed: PreConfirmedBlock,
    /// Blocks between the committed head and the pre-confirmed tip, oldest
    /// first, newest (the immediate parent) last
    pub parents: Vec<PreLatestData>,
}

impl PendingBlocks {
    pub fn transactions(&self) -> &[Transaction] {
        &self.pre_confirmed.transactions
    }

    /// The immediate parent of the pre-confirmed block, ie. the newest
    /// uncommitted parent.
    pub fn immediate_parent(&self) -> Option<&PreLatestData> {
        self.parents.last()
    }

    pub fn pre_latest_transactions(&self) -> Option<&[Transaction]> {
        self.immediate_parent()
            .map(|data| data.block.transactions.as_slice())
    }

    pub fn tx_receipts_and_events(&self) -> &[TxnReceiptAndEvents] {
        &self.pre_confirmed.transaction_receipts
    }

    pub fn pre_latest_tx_receipts_and_events(&self) -> Option<&[TxnReceiptAndEvents]> {
        self.immediate_parent()
            .map(|data| data.block.transaction_receipts.as_slice())
    }

    /// All uncommitted parent blocks below the pre-confirmed tip, oldest first,
    /// newest (the immediate parent) last.
    pub fn parent_blocks(&self) -> impl DoubleEndedIterator<Item = &PreLatestData> + '_ {
        self.parents.iter()
    }
}

#[derive(Clone, Default, Debug, PartialEq)]
pub struct PendingData {
    pub blocks: Arc<PendingBlocks>,
    /// State update for the pre-confirmed tip only, without any parents.
    pub state_update: Arc<StateUpdate>,
    /// State update for the pre-confirmed tip and every uncommitted parent.
    pub aggregated_state_update: Arc<StateUpdate>,
    pub number: BlockNumber,
    /// [`aggregated_state_update`](Self::aggregated_state_update) is composed
    /// on top of this committed height, so the covered block range is
    /// `(aggregated_lower_bound, number]`.
    pub aggregated_lower_bound: BlockNumber,
}

/// Computes the aggregated lower bound from the lowest block covered by the
/// pending data.
fn aggregated_lower_bound_from_lowest_covered(lowest_covered: BlockNumber) -> BlockNumber {
    lowest_covered
        .get()
        .checked_sub(1)
        .map(BlockNumber::new_or_panic)
        .unwrap_or(BlockNumber::GENESIS)
}

impl PendingData {
    #[doc(hidden)]
    pub fn from_parts(
        blocks: PendingBlocks,
        state_update: StateUpdate,
        aggregated_state_update: StateUpdate,
        number: BlockNumber,
    ) -> Self {
        // The lowest covered block is the oldest parent or the preconfirmed tip
        // otherwise.
        let lowest_covered = blocks
            .parents
            .first()
            .map(|d| d.block.number)
            .unwrap_or(number);
        Self {
            blocks: Arc::new(blocks),
            state_update: Arc::new(state_update),
            aggregated_state_update: Arc::new(aggregated_state_update),
            number,
            aggregated_lower_bound: aggregated_lower_bound_from_lowest_covered(lowest_covered),
        }
    }

    /// Converts a pre-confirmed block from the gateway into pending data. State
    /// update is composed from the per-transaction diffs.
    pub fn try_from_pre_confirmed_block(
        block: Box<starknet_gateway_types::reply::PreConfirmedBlock>,
        number: BlockNumber,
    ) -> anyhow::Result<Self> {
        let (pre_confirmed_block, pre_confirmed_state_update) =
            convert_pre_confirmed_block(block, number)?;

        Ok(Self {
            blocks: Arc::new(PendingBlocks {
                pre_confirmed: pre_confirmed_block,
                parents: Vec::new(),
            }),
            state_update: Arc::clone(&pre_confirmed_state_update),
            aggregated_state_update: pre_confirmed_state_update,
            number,
            aggregated_lower_bound: aggregated_lower_bound_from_lowest_covered(number),
        })
    }

    /// Build a pending view whose aggregated overlay spans a multi-block
    /// window.
    ///
    /// `parents` are all uncommitted blocks between the committed head
    /// (`aggregated_lower_bound`) and the preconfirmed tip (`number`). The
    /// aggregated overlay composes every parent's diffs (oldest first,
    /// newest last) and then the pre-confirmed tip's own diffs on top.
    pub fn from_window(
        pre_confirmed_block: Box<starknet_gateway_types::reply::PreConfirmedBlock>,
        number: BlockNumber,
        parents: Vec<PreLatestData>,
        aggregated_lower_bound: BlockNumber,
    ) -> anyhow::Result<Self> {
        let (pre_confirmed_block, pre_confirmed_state_update) =
            convert_pre_confirmed_block(pre_confirmed_block, number)?;

        // Compose the overlay starting from the oldest block so fresher updates
        // overwrite older ones.
        let mut overlay = StateUpdate::default();
        for parent in &parents {
            overlay = overlay.apply(&parent.state_update);
        }
        let aggregated_state_update = Arc::new(overlay.apply(pre_confirmed_state_update.as_ref()));

        Ok(Self {
            blocks: Arc::new(PendingBlocks {
                pre_confirmed: pre_confirmed_block,
                parents,
            }),
            state_update: pre_confirmed_state_update,
            aggregated_state_update,
            number,
            aggregated_lower_bound,
        })
    }

    /// Convert to the form that is used by consumers.
    pub fn to_pre_latest_data(&self) -> PreLatestData {
        let block = &self.blocks.pre_confirmed;
        PreLatestData {
            block: PreLatestBlock {
                number: block.number,
                parent_hash: BlockHash::ZERO,
                l1_gas_price: block.l1_gas_price,
                l1_data_gas_price: block.l1_data_gas_price,
                l2_gas_price: block.l2_gas_price,
                sequencer_address: block.sequencer_address,
                status: block.status,
                timestamp: block.timestamp,
                starknet_version: block.starknet_version,
                l1_da_mode: block.l1_da_mode,
                transactions: block.transactions.clone(),
                transaction_receipts: block.transaction_receipts.clone(),
            },
            state_update: StateUpdate::clone(&self.state_update),
        }
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
                parents: Vec::new(),
            }),
            state_update: Arc::clone(&state_update),
            aggregated_state_update: state_update,
            number: latest.number + 1,
            // No uncommitted blocks, so it sits directly on the latest committed block.
            aggregated_lower_bound: latest.number,
        }
    }

    pub fn pre_confirmed_block_number(&self) -> BlockNumber {
        self.number
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

    /// Synthesise a `BlockHeader` from the immediate parent block, if it
    /// exists.
    pub fn pre_latest_header(&self) -> Option<BlockHeader> {
        self.blocks.immediate_parent().map(|data| data.header())
    }

    pub fn pending_block(&self) -> Arc<PendingBlocks> {
        Arc::clone(&self.blocks)
    }

    pub fn pre_latest_block(&self) -> Option<Arc<PreLatestBlock>> {
        self.blocks
            .immediate_parent()
            .map(|data| Arc::new(data.block.clone()))
    }

    /// State update for the pre-confirmed tip only, without any parents.
    pub fn pre_confirmed_state_update(&self) -> Arc<StateUpdate> {
        Arc::clone(&self.state_update)
    }

    /// Combined state update from preconfirmed and any uncommitted parents.
    pub fn aggregated_state_update(&self) -> Arc<StateUpdate> {
        Arc::clone(&self.aggregated_state_update)
    }

    /// Transactions in the pre-confirmed tip only, without any parents.
    pub fn pre_confirmed_transactions(&self) -> &[Transaction] {
        self.blocks.transactions()
    }

    /// Receipts and events in the pre-confirmed tip only, without any parents.
    pub fn pre_confirmed_tx_receipts_and_events(&self) -> &[TxnReceiptAndEvents] {
        self.blocks.tx_receipts_and_events()
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

    /// Look up a transaction by hash across the whole un-committed window:
    /// the pre-confirmed tip first, then every parent from newest to oldest.
    pub fn find_transaction(&self, tx_hash: TransactionHash) -> Option<Transaction> {
        self.pre_confirmed_transactions()
            .iter()
            .find(|tx| tx.hash == tx_hash)
            .cloned()
            .or_else(|| {
                self.blocks.parent_blocks().rev().find_map(|parent| {
                    parent
                        .block
                        .transactions
                        .iter()
                        .find(|tx| tx.hash == tx_hash)
                        .cloned()
                })
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

    /// True if `block` is the pre-confirmed block or any un-committed parent
    /// (the immediate pre-latest or a deeper ancestor).
    pub fn is_pre_latest_or_pre_confirmed(&self, block: BlockNumber) -> bool {
        self.pre_confirmed_block_number() == block
            || self
                .blocks
                .parent_blocks()
                .any(|parent| parent.block.number == block)
    }

    /// All uncommitted parent blocks below the pre-confirmed tip, oldest first,
    /// newest (the immediate parent) last.
    pub fn parent_blocks(&self) -> impl DoubleEndedIterator<Item = &PreLatestData> + '_ {
        self.blocks.parent_blocks()
    }
}

/// Validate a gateway preconfirmed block and convert it into the display form
/// with its aggregated state update:
/// 1. Drop the receipt placeholders a pre-confirmed response carries, failing
///    if a transaction is still missing one. A missing receipt means a
///    candidate transaction the sequencer hasn't executed yet.
/// 2. Fold the per-transaction state diffs of a pre-confirmed response into one
///    state update. A pending block has no root of its own, so the roots are
///    left empty.
fn convert_pre_confirmed_block(
    block: Box<starknet_gateway_types::reply::PreConfirmedBlock>,
    number: BlockNumber,
) -> anyhow::Result<(PreConfirmedBlock, Arc<StateUpdate>)> {
    // Drop placeholder Nones from receipts.
    let transaction_receipts: Vec<_> = block.transaction_receipts.into_iter().flatten().collect();

    let receipted: HashSet<_> = transaction_receipts
        .iter()
        .map(|(receipt, _)| receipt.transaction_hash)
        .collect();
    for tx in &block.transactions {
        if !receipted.contains(&tx.hash) {
            anyhow::bail!("Missing transaction receipt for tx ({})", tx.hash);
        }
    }
    if transaction_receipts.len() != block.transactions.len() {
        anyhow::bail!("Mismatched transaction and receipt count in pre-confirmed block");
    }

    // Compose the aggregated state diff for the pre-confirmed block.
    let mut state_diff = starknet_gateway_types::reply::state_update::StateDiff::default();
    for transaction_diff in block.transaction_state_diffs.into_iter().flatten() {
        state_diff.extend(transaction_diff);
    }
    state_diff.deduplicate();
    let own_state_update = Arc::new(StateUpdate::from(
        starknet_gateway_types::reply::StateUpdate {
            state_diff,
            block_hash: Default::default(),
            new_root: StateCommitment::default(),
            old_root: StateCommitment::default(),
        },
    ));

    let display = PreConfirmedBlock {
        number,
        l1_gas_price: block.l1_gas_price,
        l1_data_gas_price: block.l1_data_gas_price,
        l2_gas_price: block.l2_gas_price,
        sequencer_address: block.sequencer_address,
        status: Status::PreConfirmed,
        timestamp: block.timestamp,
        starknet_version: block.starknet_version,
        l1_da_mode: block.l1_da_mode.into(),
        transactions: block.transactions,
        transaction_receipts,
    };

    Ok((display, own_state_update))
}

#[cfg(test)]
mod tests {
    use pathfinder_common::{BlockHeader, BlockNumber, StateUpdate};

    use super::{PendingBlocks, PendingData, PreConfirmedBlock, PreLatestBlock, PreLatestData};

    fn bn(n: u64) -> BlockNumber {
        BlockNumber::new_or_panic(n)
    }

    #[test]
    fn lower_bound_without_pre_latest_is_number_minus_one() {
        let blocks = PendingBlocks {
            pre_confirmed: PreConfirmedBlock {
                number: bn(10),
                ..Default::default()
            },
            parents: Vec::new(),
        };
        let data = PendingData::from_parts(
            blocks,
            StateUpdate::default(),
            StateUpdate::default(),
            bn(10),
        );
        assert_eq!(data.aggregated_lower_bound, bn(9));
    }

    #[test]
    fn lower_bound_with_pre_latest_is_number_minus_two() {
        let blocks = PendingBlocks {
            pre_confirmed: PreConfirmedBlock {
                number: bn(10),
                ..Default::default()
            },
            parents: vec![PreLatestData {
                block: PreLatestBlock {
                    number: bn(9),
                    ..Default::default()
                },
                state_update: StateUpdate::default(),
            }],
        };
        let data = PendingData::from_parts(
            blocks,
            StateUpdate::default(),
            StateUpdate::default(),
            bn(10),
        );
        assert_eq!(data.aggregated_lower_bound, bn(8));
    }

    #[test]
    fn empty_lower_bound_is_latest() {
        let latest = BlockHeader {
            number: bn(10),
            ..Default::default()
        };
        let data = PendingData::empty(&latest);
        assert_eq!(data.number, bn(11));
        assert_eq!(data.aggregated_lower_bound, bn(10));
    }

    #[test]
    fn from_window_sets_explicit_lower_bound_and_keeps_parents() {
        // Empty pre-confirmed block at height 10.
        let pre_confirmed = Box::new(starknet_gateway_types::reply::PreConfirmedBlock::default());

        // Un-committed parents 7, 8, 9, composed on top of committed block 6
        let parent = |n: u64| PreLatestData {
            block: PreLatestBlock {
                number: bn(n),
                ..Default::default()
            },
            state_update: StateUpdate::default(),
        };
        let parents = vec![parent(7), parent(8), parent(9)];

        let data = PendingData::from_window(pre_confirmed, bn(10), parents, bn(6)).unwrap();

        assert_eq!(data.number, bn(10));
        assert_eq!(data.aggregated_lower_bound, bn(6));
        let parent_numbers: Vec<_> = data.parent_blocks().map(|p| p.block.number).collect();
        assert_eq!(parent_numbers, vec![bn(7), bn(8), bn(9)]);
    }

    #[test]
    fn from_window_composes_aggregated_state_update_correctly() {
        use pathfinder_common::macro_prelude::*;

        let addr = |b: &[u8]| contract_address_bytes!(b);
        let nonce = |b: &[u8]| contract_nonce_bytes!(b);
        // Three parents each bump a distinct contract's nonce; the pre-confirmed
        // block bumps a fourth.
        let parent = |b: u64, a: &'static [u8], n: &'static [u8]| PreLatestData {
            block: PreLatestBlock {
                number: bn(b),
                ..Default::default()
            },
            state_update: StateUpdate::default().with_contract_nonce(addr(a), nonce(n)),
        };
        // Oldest to youngest
        let parents = vec![
            parent(7, b"c7", b"77"),
            parent(8, b"c8", b"88"),
            parent(9, b"c9", b"99"),
        ];

        let pre_confirmed = Box::new(starknet_gateway_types::reply::PreConfirmedBlock {
            transaction_state_diffs: vec![Some(
                starknet_gateway_types::reply::state_update::StateDiff {
                    nonces: [(addr(b"c10"), nonce(b"1010"))].into_iter().collect(),
                    ..Default::default()
                },
            )],
            ..Default::default()
        });

        let data = PendingData::from_window(pre_confirmed, bn(10), parents, bn(6)).unwrap();

        let overlay = data.aggregated_state_update();
        // Every deep ancestor's and the immediate parent's diffs are present.
        assert_eq!(overlay.contract_nonce(addr(b"c7")), Some(nonce(b"77")));
        assert_eq!(overlay.contract_nonce(addr(b"c8")), Some(nonce(b"88")));
        assert_eq!(overlay.contract_nonce(addr(b"c9")), Some(nonce(b"99")));
        assert_eq!(overlay.contract_nonce(addr(b"c10")), Some(nonce(b"1010")));
    }

    #[test]
    fn lookups_reach_deep_ancestors() {
        use pathfinder_common::macro_prelude::*;
        use pathfinder_common::transaction::Transaction;

        let deep_tx = transaction_hash!("0xdeed");
        let parent = |n: u64, tx: pathfinder_common::TransactionHash| PreLatestData {
            block: PreLatestBlock {
                number: bn(n),
                transactions: vec![Transaction {
                    hash: tx,
                    ..Default::default()
                }],
                ..Default::default()
            },
            state_update: StateUpdate::default(),
        };

        let blocks = PendingBlocks {
            pre_confirmed: PreConfirmedBlock {
                number: bn(10),
                ..Default::default()
            },
            // Deep ancestors 7 and 8, then the immediate parent 9 (oldest →
            // newest).
            parents: vec![
                parent(7, deep_tx),
                parent(8, transaction_hash!("0x8")),
                parent(9, transaction_hash!("0x9")),
            ],
        };
        let data = PendingData::from_parts(
            blocks,
            StateUpdate::default(),
            StateUpdate::default(),
            bn(10),
        );

        // A transaction in the deepest ancestor is found.
        assert_eq!(
            data.find_transaction(deep_tx).map(|t| t.hash),
            Some(deep_tx)
        );

        // Every un-committed block reports as pending; a committed one does not.
        assert!(data.is_pre_latest_or_pre_confirmed(bn(7)));
        assert!(data.is_pre_latest_or_pre_confirmed(bn(8)));
        assert!(data.is_pre_latest_or_pre_confirmed(bn(9)));
        assert!(data.is_pre_latest_or_pre_confirmed(bn(10)));
        assert!(!data.is_pre_latest_or_pre_confirmed(bn(6)));
    }
}
