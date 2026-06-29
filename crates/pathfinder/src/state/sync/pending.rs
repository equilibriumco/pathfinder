use std::sync::Arc;

use anyhow::Context;
use pathfinder_common::{BlockHash, BlockNumber, StateUpdate};
use pathfinder_pending_data::{PendingData, PendingDataCache};
use starknet_gateway_client::{BlockId, GatewayApi};
use starknet_gateway_types::reply::{PreConfirmedBlock, PreConfirmedPollResponse, PreLatestBlock};
use tokio::sync::watch;
use tokio::time::Instant;

#[derive(Debug)]
struct State {
    /// Height we're currently polling.
    block_number: BlockNumber,
    /// Server-given block identifier from the last successful poll. `None`
    /// until we've completed our first poll for this height. The server uses
    /// this to detect when our view is stale and needs a full rebuild.
    block_identifier: Option<String>,
    /// Running merged view of the preconfirmed block at `block_number`.
    /// `None` until we've received our first response for this height.
    accumulated: Option<PreConfirmedBlock>,
    /// Whether the pre-latest block was present at the last poll.
    pre_latest_data_present: bool,
}

impl Default for State {
    fn default() -> Self {
        Self {
            block_number: BlockNumber::GENESIS,
            block_identifier: None,
            accumulated: None,
            pre_latest_data_present: false,
        }
    }
}

impl State {
    fn tx_count(&self) -> u64 {
        self.accumulated
            .as_ref()
            .map(|b| b.transactions.len() as u64)
            .unwrap_or(0)
    }

    /// Apply a fresh poll response, given the pre-latest presence
    /// observed for this poll. Returns `true` if `accumulated` was
    /// updated and the caller should emit the new view.
    fn apply(&mut self, response: PreConfirmedPollResponse, new_pre_latest: bool) -> bool {
        let prev_pre_latest = self.pre_latest_data_present;
        self.pre_latest_data_present = new_pre_latest;

        match response {
            PreConfirmedPollResponse::Unchanged => false,

            PreConfirmedPollResponse::Delta {
                identifier,
                new_transactions,
                new_receipts,
                new_state_diffs,
            } => {
                // Per spec, the server only sends a delta when its identifier matches
                // ours. A mismatch indicates a server bug or local state corruption;
                // skip defensively and wait for the next poll (which our stored identifier
                // will not match server's, triggering a full rebuild).
                if self.block_identifier.as_ref() != Some(&identifier) {
                    tracing::warn!(
                        ours = ?self.block_identifier,
                        theirs = %identifier,
                        "delta response identifier doesn't match ours; skipping"
                    );
                    return false;
                }
                if new_transactions.is_empty() {
                    return false;
                }
                let acc = self
                    .accumulated
                    .as_mut()
                    .expect("accumulated present whenever block_identifier is Some");
                acc.transactions.extend(new_transactions);
                acc.transaction_receipts.extend(new_receipts);
                acc.transaction_state_diffs.extend(new_state_diffs);
                true
            }

            PreConfirmedPollResponse::Full {
                identifier,
                block_number: _,
                block,
            } => {
                // Emit on any of three independent signals:
                //  - the server's identifier changed (round bump, new height, or first poll)
                //  - new transactions arrived
                //  - pre-latest just finalised
                // Otherwise suppress: pre-0.14.3 gateways re-serve the same view across
                // polls and we don't want to bombard downstream with redundant events.
                let identifier_changed = self.block_identifier.as_ref() != Some(&identifier);
                let new_txs_arrived = (block.transactions.len() as u64) > self.tx_count();
                let pre_latest_finalised = prev_pre_latest && !new_pre_latest;

                if identifier_changed || new_txs_arrived || pre_latest_finalised {
                    self.block_identifier = Some(identifier);
                    self.accumulated = Some(block);
                    true
                } else {
                    false
                }
            }
        }
    }
}

/// Emits new pending data events while the current block is close to the latest
/// block.
///
/// Suspends polling once the cache reports itself idle and resumes on the
/// next read.
pub(super) async fn poll_pre_confirmed<S: GatewayApi + Clone + Send + 'static>(
    sequencer: S,
    poll_interval: std::time::Duration,
    cache: Arc<PendingDataCache>,
    latest: watch::Receiver<(BlockNumber, BlockHash)>,
    current: watch::Receiver<(BlockNumber, BlockHash)>,
    in_sync_threshold: u64,
) {
    let mut state = State::default();

    loop {
        // Suspend if idle.
        if cache.is_idle() && cache.subscriber_count() == 0 {
            tracing::debug!("Pre-confirmed polling idle; waiting for cache reads");
            cache.mark_stale();
            cache.wait_for_read().await;
        }

        let t_fetch = Instant::now();

        // Skip while catching up to head.
        let (latest_number, latest_hash) = *latest.borrow();
        let current_number = current.borrow().0.get();

        if latest_number.get().abs_diff(current_number) > in_sync_threshold {
            tracing::debug!(
                latest = %latest_number.get(), current = %current_number,
                "Not in sync yet; skipping pre-confirmed block download"
            );
            cache.mark_unavailable("syncing");
            wait_for_next_poll(t_fetch + poll_interval, &cache).await;
            continue;
        }

        // Fetch pre-confirmed and pre-latest concurrently.
        let (response, pre_latest_result) = tokio::join!(
            sequencer.preconfirmed_block(
                BlockId::Latest,
                state.block_identifier.clone(),
                state.tx_count(),
            ),
            fetch_pre_latest(&sequencer, latest_number, latest_hash),
        );

        let response = match response {
            Ok(r) => r,
            Err(err) => {
                tracing::debug!(%err, "Failed to fetch pre-confirmed block");
                cache.mark_unavailable("gateway error");
                wait_for_next_poll(t_fetch + poll_interval, &cache).await;
                continue;
            }
        };

        // Reconcile state with the resolved height. The gateway only reports
        // a height on `Full` responses to `latest` requests; everything else
        // (Unchanged, Delta, and concrete-number Full) leaves state at its
        // tracked block.
        let resolved = match response {
            PreConfirmedPollResponse::Full { block_number, .. } => block_number,
            _ => None,
        };

        // Set when the chain advances: the previous block to complete once the
        // new height has been published (see below).
        let mut prev_to_complete: Option<State> = None;

        if let Some(height) = resolved {
            // A transient (?) lower-height shouldn't discard accumulated state.
            if height < state.block_number && state.block_number != BlockNumber::GENESIS {
                tracing::debug!(
                    resolved = %height,
                    current = %state.block_number,
                    "Pre-confirmed resolved to a lower height than tracked; skipping poll"
                );
                // We assume this is just a hiccup and trust our tracked block
                cache.mark_fresh();
                wait_for_next_poll(t_fetch + poll_interval, &cache).await;
                continue;
            }

            // The chain advanced. Keep the just superseded block's state so we can attempt
            // best-effort completion after publishing the new height. We are
            // keeping it off the critical path of serving the new pre-confirmed block.
            if height > state.block_number {
                let prev = std::mem::replace(
                    &mut state,
                    State {
                        block_number: height,
                        ..State::default()
                    },
                );
                if prev.block_identifier.is_some() && prev.accumulated.is_some() {
                    prev_to_complete = Some(prev);
                }
            }
        }

        // Keep pre-latest only when it chains to the pre-confirmed height
        // resolved above. A mismatch means the chain advanced between the two
        // fetches, in which case publishing pre-confirmed alone is the safe move.
        let pre_latest_data = match pre_latest_result {
            Ok(Some(pre_latest)) => {
                let pre_latest_number = pre_latest.0;
                if pre_latest_number + 1 == state.block_number {
                    Some(Box::new(pre_latest))
                } else {
                    tracing::debug!(
                        pre_latest = %pre_latest_number,
                        pre_confirmed = %state.block_number,
                        "Pre-latest doesn't chain to pre-confirmed; dropping (chain raced)"
                    );
                    None
                }
            }
            Ok(None) => None,
            Err(e) => {
                tracing::debug!(%e, "Failed to fetch pre-latest block");
                None
            }
        };

        // Publish. On a change we store the new view directly; otherwise we mark
        // the existing contents fresh to unblock cold-start readers.
        if state.apply(response, pre_latest_data.is_some()) {
            let accumulated = state
                .accumulated
                .as_ref()
                .expect("accumulated block present after a successful update");
            store_pre_confirmed(
                &cache,
                current.borrow().0,
                state.block_number,
                accumulated.clone().into(),
                pre_latest_data,
            );
        } else {
            tracing::trace!("No change in pre-confirmed block data");
            cache.mark_fresh();
        }

        // Complete the just-superseded block off the critical path. The new
        // height is already in the cache, the aim is to capture any tail transactions
        // that landed before the block was superseded and deliver them to streaming
        // subscribers, without touching the read view RPC readers see.
        if let Some(prev) = prev_to_complete {
            let sequencer = sequencer.clone();
            let cache = cache.clone();
            tokio::spawn(async move {
                complete_previous_block(&sequencer, prev, &cache).await;
            });
        }

        // Wait for the next tick.
        wait_for_next_poll(t_fetch + poll_interval, &cache).await;
    }
}

/// Sleeps until `deadline`, returning early if the cache is read.
async fn wait_for_next_poll(deadline: Instant, cache: &PendingDataCache) {
    tokio::select! {
        _ = tokio::time::sleep_until(deadline) => {}
        _ = cache.wait_for_read() => {}
    }
}

/// Fetch the previously-tracked block one final time to capture any
/// transactions that landed before it was superseded, and deliver them to
/// streaming subscribers. Best-effort.
///
/// This publishes to the subscriber stream only (via
/// [`PendingDataCache::publish_tail`]), never to the read view: by the time it
/// runs the chain has advanced and a newer block is what RPC readers should
/// see. Subscribers dedupe per block, so they pick up only the previously
/// unsent tail transactions. Because it doesn't touch the read view it needs no
/// committed-head chaining check.
async fn complete_previous_block<S: GatewayApi + Send + 'static>(
    sequencer: &S,
    mut state: State,
    cache: &PendingDataCache,
) {
    let response = match sequencer
        .preconfirmed_block(
            state.block_number.into(),
            state.block_identifier.clone(),
            state.tx_count(),
        )
        .await
    {
        Ok(r) => r,
        Err(err) => {
            tracing::debug!(%err, "Completion query for previous pre-confirmed block failed");
            return;
        }
    };

    if state.apply(response, false) {
        let accumulated = state
            .accumulated
            .as_ref()
            .expect("accumulated present after successful completion apply");
        match PendingData::try_from_pre_confirmed_block(
            accumulated.clone().into(),
            state.block_number,
        ) {
            Ok(pending) => cache.publish_tail(pending),
            Err(e) => tracing::info!(
                block_number = %state.block_number, error = %e,
                "Failed to convert completed pre-confirmed block; skipping tail publish"
            ),
        }
    }
}

/// Validate a fresh pre-confirmed view against the committed head and, when it
/// chains, convert it to [`PendingData`] and store it in the cache.
///
/// `committed_head` is the latest block already in storage, mirrored by the
/// `current` watch. A view that doesn't chain to it is dropped.
///
/// # Why this is safe outside the sync DB write transaction
///
/// Pre-confirmed updates used to flow through the sync event channel and were
/// applied inside the consumer's `Immediate` write transaction. They are now
/// written straight to the cache from the polling task, with no transaction.
/// This is OK because:
/// - `committed_head` is the already storage-committed head,
/// - [`PendingDataCache`] in in-memory and doesn't write anything to storage,
/// - data is consistent: there is a chaining check below and ultimately the RPC
///   read path re-validates the chaining requirement.
fn store_pre_confirmed(
    cache: &PendingDataCache,
    committed_head: BlockNumber,
    number: BlockNumber,
    block: Box<PreConfirmedBlock>,
    pre_latest_data: Option<Box<(BlockNumber, PreLatestBlock, StateUpdate)>>,
) {
    // Reject views that don't chain to the committed head.
    let next_block_number = pre_latest_data
        .as_ref()
        .map(|pre_latest| pre_latest.0)
        .unwrap_or(number);

    if next_block_number != committed_head + 1 {
        tracing::debug!(
            %number, %committed_head,
            "Pre-confirmed doesn't chain to committed head; skipping store"
        );
        return;
    }

    // Convert and store.
    match PendingData::try_from_pre_confirmed_and_pre_latest(block, number, pre_latest_data) {
        Ok(pending) => {
            let pre_latest_tx_count = pending.pre_latest_transactions().map(|txs| txs.len());
            let pre_confirmed_tx_count = pending.pre_confirmed_transactions().len();
            cache.store(pending);
            tracing::debug!(block_number = %number, %pre_confirmed_tx_count, ?pre_latest_tx_count, "Updated pre-confirmed data");
        }
        Err(e) => {
            tracing::info!(block_number=%number, error=%e, "Failed to validate pre-confirmed data, skipping update");
        }
    }
}

/// Fetch the pending block from the sequencer and classify it as
/// [pre-latest](starknet_gateway_types::reply::PreLatestBlock) if its parent
/// hash matches our latest block hash.
///
/// If the pre-latest block (N) exists, the sequencer has already started
/// building the next pre-confirmed block (N + 1).
async fn fetch_pre_latest<S: GatewayApi + Send + 'static>(
    sequencer: &S,
    our_latest_number: BlockNumber,
    our_latest_hash: BlockHash,
) -> anyhow::Result<
    Option<(
        BlockNumber,
        starknet_gateway_types::reply::PreLatestBlock,
        pathfinder_common::StateUpdate,
    )>,
> {
    let (pending_block, state_update) = sequencer
        .pending_block()
        .await
        .context("Fetching pre-latest block from sequencer")?;

    let pre_latest_data = (pending_block.parent_hash == our_latest_hash).then_some((
        our_latest_number + 1,
        pending_block,
        state_update,
    ));
    Ok(pre_latest_data)
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, LazyLock};

    use pathfinder_common::macro_prelude::*;
    use pathfinder_common::prelude::*;
    use pathfinder_common::transaction::{L1HandlerTransaction, Transaction, TransactionVariant};
    use pathfinder_pending_data::PendingDataCache;
    use starknet_gateway_client::MockGatewayApi;
    use starknet_gateway_types::reply::{
        Block,
        GasPrices,
        L1DataAvailabilityMode,
        PreConfirmedBlock,
        PreConfirmedPollResponse,
        PreLatestBlock,
        Status,
    };
    use tokio::sync::watch;

    use super::{complete_previous_block, poll_pre_confirmed, store_pre_confirmed, State};

    const TEST_IN_SYNC_THRESHOLD: u64 = 6;
    const PARENT_HASH: BlockHash = block_hash!("0x1234");
    const PARENT_ROOT: StateCommitment = state_commitment_bytes!(b"parent root");

    pub static NEXT_BLOCK: LazyLock<Block> = LazyLock::new(|| Block {
        block_hash: block_hash!("0xabcd"),
        block_number: BlockNumber::new_or_panic(1),
        l1_gas_price: Default::default(),
        l1_data_gas_price: Default::default(),
        l2_gas_price: Default::default(),
        parent_block_hash: PARENT_HASH,
        sequencer_address: None,
        state_commitment: PARENT_ROOT,
        status: Status::AcceptedOnL2,
        timestamp: BlockTimestamp::new_or_panic(10),
        transaction_receipts: Vec::new(),
        transactions: Vec::new(),
        starknet_version: StarknetVersion::default(),
        l1_da_mode: Default::default(),
        transaction_commitment: Default::default(),
        event_commitment: Default::default(),
        receipt_commitment: Default::default(),
        state_diff_commitment: Default::default(),
        state_diff_length: Default::default(),
    });

    pub static PENDING_UPDATE: LazyLock<StateUpdate> =
        LazyLock::new(|| StateUpdate::default().with_parent_state_commitment(PARENT_ROOT));

    pub static PRE_LATEST_BLOCK: LazyLock<PreLatestBlock> = LazyLock::new(|| PreLatestBlock {
        l1_gas_price: GasPrices {
            price_in_wei: GasPrice(11),
            ..Default::default()
        },
        l1_data_gas_price: Default::default(),
        l2_gas_price: Default::default(),
        parent_hash: NEXT_BLOCK.parent_block_hash,
        sequencer_address: sequencer_address_bytes!(b"seqeunecer address"),
        status: Status::Pending,
        timestamp: BlockTimestamp::new_or_panic(20),
        transaction_receipts: Vec::new(),
        transactions: vec![pathfinder_common::transaction::Transaction {
            hash: transaction_hash!("0x22"),
            variant: pathfinder_common::transaction::TransactionVariant::L1Handler(
                L1HandlerTransaction {
                    contract_address: contract_address!("0x1"),
                    entry_point_selector: entry_point!("0x55"),
                    nonce: transaction_nonce!("0x2"),
                    calldata: Vec::new(),
                },
            ),
        }],
        starknet_version: StarknetVersion::new(0, 14, 0, 0),
        l1_da_mode: L1DataAvailabilityMode::Calldata,
    });

    /// Arbitrary upper bound for awaiting a cache update in tests, so a failing
    /// test fails fast instead of hanging.
    const TEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

    /// A pre-confirmed block whose every transaction carries a receipt, so the
    /// `try_from_pre_confirmed_and_pre_latest` conversion accepts it (it
    /// rejects blocks containing candidate transactions).
    fn all_receipted_block(n: usize) -> PreConfirmedBlock {
        let receipted_tx = |i: usize| {
            let hash = match i {
                0 => transaction_hash!("0x1"),
                1 => transaction_hash!("0x2"),
                2 => transaction_hash!("0x3"),
                _ => transaction_hash!("0x4"),
            };
            Transaction {
                hash,
                variant: TransactionVariant::L1Handler(L1HandlerTransaction {
                    contract_address: contract_address!("0x1"),
                    entry_point_selector: entry_point!("0x55"),
                    nonce: transaction_nonce!("0x0"),
                    calldata: Vec::new(),
                }),
            }
        };
        let transactions: Vec<_> = (0..n).map(receipted_tx).collect();
        let transaction_receipts = transactions
            .iter()
            .enumerate()
            .map(|(i, tx)| {
                Some((
                    pathfinder_common::receipt::Receipt {
                        transaction_hash: tx.hash,
                        transaction_index: TransactionIndex::new_or_panic(i as u64),
                        ..Default::default()
                    },
                    vec![],
                ))
            })
            .collect();
        PreConfirmedBlock {
            status: Status::PreConfirmed,
            transactions,
            transaction_receipts,
            transaction_state_diffs: vec![None; n],
            ..Default::default()
        }
    }

    /// Spawn the producer polling at a 1ms interval against the given cache.
    /// The first poll fires immediately, so single-store tests aren't delayed.
    fn spawn_producer(
        sequencer: MockGatewayApi,
        committed: BlockNumber,
        latest_hash: BlockHash,
        cache: Arc<PendingDataCache>,
    ) {
        let (_latest_tx, latest) = watch::channel((committed, latest_hash));
        let (_current_tx, current) = watch::channel((committed, latest_hash));
        let sequencer = Arc::new(sequencer);
        tokio::spawn(async move {
            poll_pre_confirmed(
                sequencer,
                std::time::Duration::from_millis(1),
                cache,
                latest,
                current,
                TEST_IN_SYNC_THRESHOLD,
            )
            .await
        });
    }

    #[tokio::test]
    async fn store_pre_confirmed_stores_and_converts_a_chaining_view() {
        // Committed head 1; pre-confirmed at 3 chains via a pre-latest at 2.
        let cache = PendingDataCache::new();
        let mut rx = cache.subscribe();
        let _ = rx.borrow_and_update();

        let committed = BlockNumber::new_or_panic(1);
        let pre_latest = Box::new((
            committed + 1,
            PRE_LATEST_BLOCK.clone(),
            PENDING_UPDATE.clone(),
        ));
        store_pre_confirmed(
            &cache,
            committed,
            BlockNumber::new_or_panic(3),
            Box::new(all_receipted_block(2)),
            Some(pre_latest),
        );

        assert!(
            rx.has_changed().unwrap(),
            "a chaining view should be stored"
        );
        let pending = rx.borrow().clone();
        assert_eq!(
            pending.pre_confirmed_block_number(),
            BlockNumber::new_or_panic(3)
        );
        assert_eq!(pending.pre_latest_block_number(), Some(committed + 1));
        assert_eq!(pending.pre_confirmed_transactions().len(), 2);
    }

    #[tokio::test]
    async fn store_pre_confirmed_skips_a_non_chaining_view() {
        // Committed head 1, pre-confirmed at 5: gap > 1, so it doesn't chain.
        let cache = PendingDataCache::new();
        let mut rx = cache.subscribe();
        let _ = rx.borrow_and_update();

        store_pre_confirmed(
            &cache,
            BlockNumber::new_or_panic(1),
            BlockNumber::new_or_panic(5),
            Box::new(all_receipted_block(2)),
            None,
        );

        assert!(
            !rx.has_changed().unwrap(),
            "a non-chaining view must not be stored"
        );
    }

    #[tokio::test]
    async fn fetch_pre_latest_returns_some_when_parent_matches() {
        let mut sequencer = MockGatewayApi::new();
        let our_latest_number = NEXT_BLOCK.block_number - 1;
        let our_latest_hash = NEXT_BLOCK.parent_block_hash;

        sequencer
            .expect_pending_block()
            .returning(move || Ok((PRE_LATEST_BLOCK.clone(), PENDING_UPDATE.clone())));

        let (number, block, state_update) =
            super::fetch_pre_latest(&sequencer, our_latest_number, our_latest_hash)
                .await
                .unwrap()
                .unwrap();

        assert_eq!(number, NEXT_BLOCK.block_number);
        assert_eq!(block.parent_hash, our_latest_hash);
        assert_eq!(state_update, PENDING_UPDATE.clone());
    }

    #[tokio::test]
    async fn fetch_pre_latest_returns_none_when_parent_differs() {
        let mut sequencer = MockGatewayApi::new();
        let our_latest_number = NEXT_BLOCK.block_number - 1;
        let our_latest_hash = NEXT_BLOCK.parent_block_hash;
        let different_hash = block_hash!("0xdeadbeef");

        let pending_block = PreLatestBlock {
            parent_hash: different_hash,
            ..PRE_LATEST_BLOCK.clone()
        };

        sequencer
            .expect_pending_block()
            .returning(move || Ok((pending_block.clone(), PENDING_UPDATE.clone())));

        let result = super::fetch_pre_latest(&sequencer, our_latest_number, our_latest_hash)
            .await
            .unwrap();

        assert!(result.is_none());
    }

    mod apply {
        use pathfinder_common::macro_prelude::*;
        use pathfinder_common::transaction::{
            L1HandlerTransaction,
            Transaction,
            TransactionVariant,
        };
        use starknet_gateway_types::reply::{PreConfirmedBlock, PreConfirmedPollResponse};

        use super::super::{BlockNumber, State};

        fn placeholder_tx(index: usize) -> Transaction {
            let hash = match index {
                0 => transaction_hash!("0x1"),
                1 => transaction_hash!("0x2"),
                _ => transaction_hash!("0x3"),
            };
            Transaction {
                hash,
                variant: TransactionVariant::L1Handler(L1HandlerTransaction {
                    contract_address: contract_address!("0x1"),
                    entry_point_selector: entry_point!("0x55"),
                    nonce: transaction_nonce!("0x0"),
                    calldata: Vec::new(),
                }),
            }
        }

        /// Build a minimal `PreConfirmedBlock` with `n` placeholder
        /// transactions and matching empty receipt/state-diff slots.
        /// Other fields use `Default`.
        fn block_with_txs(n: usize) -> PreConfirmedBlock {
            let txs: Vec<Transaction> = (0..n).map(placeholder_tx).collect();
            let receipts = vec![None; n];
            let state_diffs = vec![None; n];
            PreConfirmedBlock {
                transactions: txs,
                transaction_receipts: receipts,
                transaction_state_diffs: state_diffs,
                ..Default::default()
            }
        }

        fn state(identifier: Option<&str>, txs: usize, pre_latest: bool) -> State {
            State {
                block_number: BlockNumber::new_or_panic(10),
                block_identifier: identifier.map(String::from),
                accumulated: if identifier.is_some() {
                    Some(block_with_txs(txs))
                } else {
                    None
                },
                pre_latest_data_present: pre_latest,
            }
        }

        #[test]
        fn unchanged_response_is_noop() {
            let mut s = state(Some("abc"), 1, false);
            let original_accumulated = s.accumulated.clone();
            let original_identifier = s.block_identifier.clone();
            let original_number = s.block_number;

            let emitted = s.apply(PreConfirmedPollResponse::Unchanged, true);

            assert!(!emitted);
            assert_eq!(s.accumulated, original_accumulated);
            assert_eq!(s.block_identifier, original_identifier);
            assert_eq!(s.block_number, original_number);
            // apply always writes the new pre-latest presence
            assert!(s.pre_latest_data_present);
        }

        #[test]
        fn delta_with_mismatching_identifier_is_skipped() {
            let mut s = state(Some("abc"), 1, false);
            let original_accumulated = s.accumulated.clone();

            let emitted = s.apply(
                PreConfirmedPollResponse::Delta {
                    identifier: "xyz".into(),
                    new_transactions: vec![Transaction {
                        hash: transaction_hash!("0x99"),
                        variant: TransactionVariant::L1Handler(L1HandlerTransaction {
                            contract_address: contract_address!("0x1"),
                            entry_point_selector: entry_point!("0x55"),
                            nonce: transaction_nonce!("0x0"),
                            calldata: Vec::new(),
                        }),
                    }],
                    new_receipts: vec![None],
                    new_state_diffs: vec![None],
                },
                false,
            );

            assert!(!emitted);
            assert_eq!(s.accumulated, original_accumulated);
            assert_eq!(s.block_identifier, Some("abc".into()));
        }

        #[test]
        fn delta_with_matching_identifier_and_empty_transactions_is_noop() {
            let mut s = state(Some("abc"), 1, false);
            let original_accumulated = s.accumulated.clone();

            let emitted = s.apply(
                PreConfirmedPollResponse::Delta {
                    identifier: "abc".into(),
                    new_transactions: vec![],
                    new_receipts: vec![],
                    new_state_diffs: vec![],
                },
                false,
            );

            assert!(!emitted);
            assert_eq!(s.accumulated, original_accumulated);
            assert_eq!(s.block_identifier, Some("abc".into()));
        }

        #[test]
        fn delta_with_matching_identifier_appends() {
            let mut s = state(Some("abc"), 1, false);

            let new_tx = Transaction {
                hash: transaction_hash!("0x99"),
                variant: TransactionVariant::L1Handler(L1HandlerTransaction {
                    contract_address: contract_address!("0x1"),
                    entry_point_selector: entry_point!("0x55"),
                    nonce: transaction_nonce!("0x0"),
                    calldata: Vec::new(),
                }),
            };

            let emitted = s.apply(
                PreConfirmedPollResponse::Delta {
                    identifier: "abc".into(),
                    new_transactions: vec![new_tx],
                    new_receipts: vec![None],
                    new_state_diffs: vec![None],
                },
                false,
            );

            assert!(emitted);
            let acc = s.accumulated.as_ref().unwrap();
            assert_eq!(acc.transactions.len(), 2);
            assert_eq!(acc.transaction_receipts.len(), 2);
            assert_eq!(acc.transaction_state_diffs.len(), 2);
            assert_eq!(s.block_identifier, Some("abc".into()));
        }

        #[test]
        fn full_with_changed_identifier_emits() {
            let mut s = state(Some("abc"), 1, false);
            let new_block = block_with_txs(1);

            let emitted = s.apply(
                PreConfirmedPollResponse::Full {
                    identifier: "xyz".into(),
                    block_number: Some(BlockNumber::new_or_panic(10)),
                    block: new_block.clone(),
                },
                false,
            );

            assert!(emitted);
            assert_eq!(s.block_identifier, Some("xyz".into()));
            assert_eq!(s.accumulated, Some(new_block));
        }

        #[test]
        fn full_with_more_transactions_emits() {
            let mut s = state(Some("abc"), 1, false);
            let new_block = block_with_txs(2);

            let emitted = s.apply(
                PreConfirmedPollResponse::Full {
                    identifier: "abc".into(),
                    block_number: Some(BlockNumber::new_or_panic(10)),
                    block: new_block.clone(),
                },
                false,
            );

            assert!(emitted);
            assert_eq!(s.accumulated.as_ref().unwrap().transactions.len(), 2);
        }

        #[test]
        fn full_with_pre_latest_finalised_emits() {
            let mut s = state(Some("abc"), 2, true);
            let same_block = block_with_txs(2);

            // pre_latest transitions true → false: should force an emit
            let emitted = s.apply(
                PreConfirmedPollResponse::Full {
                    identifier: "abc".into(),
                    block_number: Some(BlockNumber::new_or_panic(10)),
                    block: same_block.clone(),
                },
                false,
            );

            assert!(emitted);
            assert_eq!(s.accumulated, Some(same_block));
        }

        #[test]
        fn full_with_no_signals_is_deduped() {
            let mut s = state(Some("abc"), 2, false);
            let same_block = block_with_txs(2);
            let original_accumulated = s.accumulated.clone();

            // Same identifier, no new txs, no pre-latest transition: nothing to emit
            let emitted = s.apply(
                PreConfirmedPollResponse::Full {
                    identifier: "abc".into(),
                    block_number: Some(BlockNumber::new_or_panic(10)),
                    block: same_block,
                },
                false,
            );

            assert!(!emitted);
            assert_eq!(s.accumulated, original_accumulated);
            assert_eq!(s.block_identifier, Some("abc".into()));
        }
    }

    /// A transient gateway inconsistency could briefly resolve `latest` to a
    /// lower height than we're already tracking. The polling loop should skip
    /// those responses without overwriting the cached view.
    ///
    /// See also <https://github.com/equilibriumco/pathfinder/issues/3081>.
    #[tokio::test]
    async fn lower_height_is_skipped() {
        static COUNT: std::sync::Mutex<usize> = std::sync::Mutex::new(0);

        let mut sequencer = MockGatewayApi::new();
        sequencer
            .expect_pending_block()
            .returning(|| Ok((PRE_LATEST_BLOCK.clone(), PENDING_UPDATE.clone())));
        sequencer.expect_preconfirmed_block().returning(|_, _, _| {
            let mut count = COUNT.lock().unwrap();
            let block_number = match *count {
                0 => {
                    *count += 1;
                    // First poll establishes the tracked height at 12.
                    BlockNumber::new_or_panic(12)
                }
                // Subsequent polls resolve to a lower height (gateway hiccup).
                _ => BlockNumber::new_or_panic(11),
            };
            Ok(PreConfirmedPollResponse::Full {
                identifier: String::new(),
                block_number: Some(block_number),
                block: all_receipted_block(1),
            })
        });

        let committed = BlockNumber::new_or_panic(11);
        let cache = Arc::new(PendingDataCache::new());
        let mut sub = cache.subscribe();
        let _ = sub.borrow_and_update();

        // Unrelated hash so pre-latest is classified absent.
        spawn_producer(sequencer, committed, block_hash!("0xdead"), cache.clone());

        // The first poll stores block 12.
        tokio::time::timeout(TEST_TIMEOUT, sub.changed())
            .await
            .expect("block 12 should be stored")
            .unwrap();
        assert_eq!(
            sub.borrow().pre_confirmed_block_number(),
            BlockNumber::new_or_panic(12)
        );

        // The backwards-resolved poll must not overwrite the cache.
        let changed =
            tokio::time::timeout(std::time::Duration::from_millis(100), sub.changed()).await;
        assert!(
            changed.is_err(),
            "a lower-height poll must not overwrite the cache"
        );

        // The skip keeps the cache serviceable and still holding block 12.
        let retained = cache.try_read().expect("cache should remain serviceable");
        assert_eq!(
            retained.pre_confirmed_block_number(),
            BlockNumber::new_or_panic(12)
        );
    }

    /// While catching up to head, the loop marks the cache unavailable so reads
    /// fail fast with "syncing" instead of blocking on the cold-start
    /// timeout.
    #[tokio::test(start_paused = true)]
    async fn syncing_marks_cache_unavailable() {
        // latest far ahead of current: the catch-up branch runs and never
        // fetches, so an expectation-free mock gateway is fine.
        let sequencer = Arc::new(MockGatewayApi::new());
        let hash = block_hash!("0xabcd");
        let (_tx_latest, latest) = watch::channel((BlockNumber::new_or_panic(100), hash));
        let (_tx_current, current) = watch::channel((BlockNumber::new_or_panic(0), hash));

        let cache = Arc::new(PendingDataCache::new());
        let _jh = tokio::spawn({
            let cache = cache.clone();
            async move {
                super::poll_pre_confirmed(
                    sequencer,
                    std::time::Duration::from_secs(1),
                    cache,
                    latest,
                    current,
                    TEST_IN_SYNC_THRESHOLD,
                )
                .await
            }
        });

        // Hand the loop a turn to reach the catch-up branch (virtual time).
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let err = cache.read().await.unwrap_err();
        assert!(err.to_string().contains("syncing"), "got: {err}");
    }

    /// A failed pre-confirmed fetch marks the cache unavailable with
    /// "gateway error", so reads fail fast rather than blocking.
    #[tokio::test(start_paused = true)]
    async fn gateway_error_marks_cache_unavailable() {
        use starknet_gateway_types::error::SequencerError;

        // In sync (latest == current) so the loop reaches the fetch, which fails.
        let mut sequencer = MockGatewayApi::new();
        sequencer
            .expect_preconfirmed_block()
            .returning(|_, _, _| Err(SequencerError::InvalidResponse("boom".into())));
        sequencer
            .expect_pending_block()
            .returning(|| Err(SequencerError::InvalidResponse("boom".into())));
        let sequencer = Arc::new(sequencer);

        let hash = block_hash!("0xabcd");
        let number = BlockNumber::new_or_panic(10);
        let (_tx_latest, latest) = watch::channel((number, hash));
        let (_tx_current, current) = watch::channel((number, hash));

        let cache = Arc::new(PendingDataCache::new());
        let _jh = tokio::spawn({
            let cache = cache.clone();
            async move {
                super::poll_pre_confirmed(
                    sequencer,
                    std::time::Duration::from_secs(1),
                    cache,
                    latest,
                    current,
                    TEST_IN_SYNC_THRESHOLD,
                )
                .await
            }
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let err = cache.read().await.unwrap_err();
        assert!(err.to_string().contains("gateway error"), "got: {err}");
    }

    /// The completion query for a superseded block carries any tail
    /// transactions that landed just before it advanced; they must reach
    /// streaming subscribers (via the subscriber stream).
    #[tokio::test]
    async fn complete_previous_block_applies_the_tail_delta() {
        const BLOCK_ID: &str = "id-11";

        let tail_hash = transaction_hash!("0x012345");
        let tail_tx = Transaction {
            hash: tail_hash,
            variant: TransactionVariant::L1Handler(L1HandlerTransaction {
                contract_address: contract_address!("0x1"),
                entry_point_selector: entry_point!("0x55"),
                nonce: transaction_nonce!("0x2"),
                calldata: Vec::new(),
            }),
        };
        let tail_receipt = pathfinder_common::receipt::Receipt {
            transaction_hash: tail_hash,
            transaction_index: TransactionIndex::new_or_panic(1),
            ..Default::default()
        };

        let mut sequencer = MockGatewayApi::new();
        sequencer
            .expect_preconfirmed_block()
            .returning(move |_, _, _| {
                Ok(PreConfirmedPollResponse::Delta {
                    identifier: BLOCK_ID.into(),
                    new_transactions: vec![tail_tx.clone()],
                    new_receipts: vec![Some((tail_receipt.clone(), vec![]))],
                    new_state_diffs: vec![None],
                })
            });

        // We're tracking block 11 with one receipted transaction already merged.
        let state = State {
            block_number: BlockNumber::new_or_panic(11),
            block_identifier: Some(BLOCK_ID.to_string()),
            accumulated: Some(all_receipted_block(1)),
            pre_latest_data_present: false,
        };

        let cache = PendingDataCache::new();
        let mut rx = cache.subscribe();
        let _ = rx.borrow_and_update();

        // The completion publishes the superseded block to subscribers only.
        complete_previous_block(&sequencer, state, &cache).await;

        assert!(
            rx.has_changed().unwrap(),
            "the completion should publish the block to subscribers"
        );
        let pending = rx.borrow().clone();
        assert_eq!(
            pending.pre_confirmed_block_number(),
            BlockNumber::new_or_panic(11)
        );
        assert!(
            pending
                .pre_confirmed_transactions()
                .iter()
                .any(|t| t.hash == tail_hash),
            "the tail transaction should be in the completed block"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn idle_pauses_polling_until_cache_read() {
        // Polling suspends once the inactivity window elapses without a read,
        // and a cache read resumes it. Activity is observed via the gateway call
        // counter, since touching the cache would itself reset the window.
        const POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(10);
        const IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(100);

        let calls = Arc::new(std::sync::Mutex::new(0u64));
        let mut sequencer = MockGatewayApi::new();
        sequencer
            .expect_pending_block()
            .returning(|| Ok((PRE_LATEST_BLOCK.clone(), PENDING_UPDATE.clone())));
        {
            let calls = calls.clone();
            sequencer
                .expect_preconfirmed_block()
                .returning(move |_, _, _| {
                    let mut c = calls.lock().unwrap();
                    *c += 1;
                    Ok(PreConfirmedPollResponse::Full {
                        identifier: format!("id-{}", *c),
                        block_number: Some(BlockNumber::new_or_panic(2)),
                        block: all_receipted_block(1),
                    })
                });
        }

        let latest_hash = PRE_LATEST_BLOCK.parent_hash;
        let committed = BlockNumber::new_or_panic(1);
        let (_latest_tx, latest) = watch::channel((committed, latest_hash));
        let (_current_tx, current) = watch::channel((committed, latest_hash));

        let cache = Arc::new(PendingDataCache::new().with_inactivity_timeout(IDLE_TIMEOUT));
        let sequencer = Arc::new(sequencer);
        tokio::spawn({
            let cache = cache.clone();
            async move {
                poll_pre_confirmed(
                    sequencer,
                    POLL_INTERVAL,
                    cache,
                    latest,
                    current,
                    TEST_IN_SYNC_THRESHOLD,
                )
                .await
            }
        });

        // Polls happen for a while, then idle suspends them.
        tokio::time::sleep(IDLE_TIMEOUT * 3).await;
        let count_at_idle = *calls.lock().unwrap();
        assert!(count_at_idle >= 1, "expected at least one poll before idle");

        tokio::time::sleep(IDLE_TIMEOUT * 3).await;
        assert_eq!(
            *calls.lock().unwrap(),
            count_at_idle,
            "polling should be suspended while idle"
        );

        // A cache read wakes the loop.
        let _ = cache.read().await;
        tokio::time::sleep(POLL_INTERVAL * 3).await;
        assert!(
            *calls.lock().unwrap() > count_at_idle,
            "polling should resume after a cache read"
        );
    }
}
