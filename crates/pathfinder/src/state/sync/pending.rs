use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use pathfinder_common::{BlockHash, BlockNumber};
use pathfinder_pending_data::{PendingData, PendingDataCache, PreLatestData};
use starknet_gateway_client::{BlockId, GatewayApi};
use starknet_gateway_types::reply::{PreConfirmedBlock, PreConfirmedPollResponse};
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
}

impl Default for State {
    fn default() -> Self {
        Self {
            block_number: BlockNumber::GENESIS,
            block_identifier: None,
            accumulated: None,
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

    /// Apply a fresh poll response. Returns `true` if `accumulated` was
    /// updated and the caller should emit the new view.
    fn apply(&mut self, response: PreConfirmedPollResponse) -> bool {
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
                // Emit on either independent signal:
                //  - the server's identifier changed (round bump, new height, or first poll)
                //  - new transactions arrived
                // Otherwise suppress: pre-0.14.3 gateways re-serve the same view across
                // polls and we don't want to bombard downstream with redundant events.
                let identifier_changed = self.block_identifier.as_ref() != Some(&identifier);
                let new_txs_arrived = (block.transactions.len() as u64) > self.tx_count();

                if identifier_changed || new_txs_arrived {
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

/// The un-committed blocks below the current pre-confirmed tip, keyed by block
/// number, used both to compose the aggregated execution overlay and as the
/// served `ancestors`.
///
/// There are two sources of entries:
/// - *provisional* (`complete = false`): the recent preconfirmed tip, just
///   superseded by the current new preconfirmed tip. It may be missing some
///   tail data, which will be filled in by [`complete_previous_block`].
/// - *complete* (`complete = true`): full block, filled by
///   [`complete_previous_block`], no tail data missing.
///
/// A provisional entry allows the new tip to be served immediately and the
/// window stays contiguous. `generation` advances on every content change so
/// the producer re-serves a fuller view.
#[derive(Default)]
struct Window {
    blocks: BTreeMap<BlockNumber, WindowEntry>,
    /// Bumped when an entry is added (new tip) or upgraded (tip is extended
    /// with new data or tip - 1 gets its tail data via
    /// [`complete_previous_block`]). The producer serves again when the
    /// generation changes compared to the last served generation, so any update
    /// is visible right away to the readers.
    generation: u64,
}

struct WindowEntry {
    data: Arc<PreLatestData>,
    /// `false` for a provisional accumulated placeholder (which may be missing
    /// tail data), `true` when upgraded with any missing tail by
    /// [`complete_previous_block`].
    complete: bool,
}

impl Window {
    /// Prune the window from the bottom, removing any blocks that have been
    /// committed.
    fn prune(&mut self, committed: BlockNumber) {
        self.blocks.retain(|&n, _| n > committed);
    }

    /// Either a provisional entry only fills an empty slot or a complete entry
    /// upgrades a provisional entry. Generation is then incremented.
    ///
    /// Does nothing otherwise (ie. a complete slot cannot be updated, a
    /// provisional entry cannot replace an existing provisional entry).
    fn record(&mut self, number: BlockNumber, data: Arc<PreLatestData>, complete: bool) {
        match self.blocks.get(&number) {
            // The existing entry if complete, nothing more to do.
            Some(existing) if existing.complete => return,
            // A provisional record can't replace an existing provisional one.
            Some(_) if !complete => return,
            _ => {}
        }
        self.blocks.insert(number, WindowEntry { data, complete });
        self.generation += 1;
    }

    /// Collect the parents in the range `(committed, tip)`, oldest
    /// to newest. `None` if any block inbetween is missing (e.g. cold
    /// start, or a block that failed deserialization/conversion).
    fn collect(&self, committed: BlockNumber, tip: BlockNumber) -> Option<Vec<Arc<PreLatestData>>> {
        let mut parents = Vec::new();
        let mut expected = committed + 1;
        while expected < tip {
            parents.push(self.blocks.get(&expected)?.data.clone());
            expected += 1;
        }
        Some(parents)
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

    // The un-committed window below the current tip. Any missing tail data is
    // filled in by [`complete_previous_block`].
    let window: Arc<Mutex<Window>> = Arc::new(Mutex::new(Window::default()));

    // The `(tip, window generation)` we last served. View is served again when:
    // 1. the tip is filled with more delta,
    // 2. the tip's block number is incremented (a new tip),
    // 3. window generation is incremented (tip-1 was completed by
    //    [`complete_previous_block`]).
    let mut served: Option<(BlockNumber, u64)> = None;

    loop {
        // Suspend if idle.
        if cache.is_idle() && cache.subscriber_count() == 0 {
            tracing::debug!("Pre-confirmed polling idle; waiting for cache reads");
            cache.mark_stale();
            cache.wait_for_read().await;
        }

        let t_fetch = Instant::now();

        // Skip while catching up to head.
        let latest_number = latest.borrow().0;
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

        // Fetch the pre-confirmed tip (block).
        let response = match sequencer
            .preconfirmed_block(
                BlockId::Latest,
                state.block_identifier.clone(),
                state.tx_count(),
            )
            .await
        {
            Ok(r) => r,
            Err(err) => {
                // A transient failure must not invalidate the cache. We serve the last good
                // view as a best effort until a poll succeeds again.
                tracing::debug!(%err, "Failed to fetch pre-confirmed block; retaining last view");
                cache.mark_fresh();
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

        // Set when the preconfirmed tip advances: the previous block to complete once
        // the new height has been published.
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

            // The preconfirmed tip advanced. Keep the previous block's state so we can
            // complete it after publishing the new height. We keep completion of tip-1 off
            // the critical path to avoid delaying serving the new tip to readers.
            if height > state.block_number {
                let prev = std::mem::replace(
                    &mut state,
                    State {
                        block_number: height,
                        ..State::default()
                    },
                );

                // Provisionally fill the window with the previous block from our previous
                // state, so the window stays contiguous and the new tip can be served
                // on this poll instead of waiting for [`complete_previous_block`] to finish.
                if let Some(block) = prev.accumulated.clone() {
                    let prev_number = prev.block_number;
                    if let Ok(pending) = run_cpu_bound(|| {
                        PendingData::try_from_pre_confirmed_block(Box::new(block), prev_number)
                    }) {
                        window.lock().unwrap().record(
                            prev_number,
                            Arc::new(pending.to_pre_latest_data()),
                            false,
                        );
                    }
                }

                if prev.block_identifier.is_some() && prev.accumulated.is_some() {
                    prev_to_complete = Some(prev);
                }
            }
        }

        let committed = current.borrow().0;

        let changed = state.apply(response);
        let number = state.block_number;

        // Prune committed blocks, then collect the parents that fill the gap
        // `(committed, number)`. None means a hole exists in the window (cold start
        // or a block that failed deserialization/conversion), which is a temporary
        // situation that self-heals overtime.
        let (bridge, generation) = {
            let mut window = window.lock().unwrap();
            window.prune(committed);
            let bridge = if number > committed {
                window.collect(committed, number)
            } else {
                None
            };
            (bridge, window.generation)
        };

        match bridge {
            // Serve when the tip's own contents changed, when the tip advanced, or when the window
            // generation increased, i.e. when an updated view is available.
            Some(parents) if changed || served != Some((number, generation)) => {
                let accumulated = state
                    .accumulated
                    .clone()
                    .expect("accumulated block present whenever the tip is bridgeable");
                let built = run_cpu_bound(|| {
                    let parents = parents.iter().map(|p| PreLatestData::clone(p)).collect();
                    PendingData::from_window(Box::new(accumulated), number, parents, committed)
                });
                match built {
                    Ok(pending) => {
                        let pre_confirmed_tx_count = pending.pre_confirmed_transactions().len();
                        cache.store(pending);
                        served = Some((number, generation));
                        tracing::debug!(block_number = %number, %committed, %pre_confirmed_tx_count, %generation, "Updated pre-confirmed data (windowed)");
                    }
                    Err(e) => tracing::info!(
                        block_number = %number, error = %e,
                        "Failed to build windowed pre-confirmed data, skipping update"
                    ),
                }
            }
            // Nothing new to serve for this tip, keep the cache fresh for cold-start readers.
            Some(_) => {
                tracing::trace!("No change in pre-confirmed block data");
                cache.mark_fresh();
            }
            // There is a hole that no provisional entry covered. This is due to a cold start into a
            // larger gap. Defer until the gap is closed by the advancing height committed into
            // storage.
            None => {
                if number > committed {
                    tracing::debug!(
                        block_number = %number, %committed,
                        "Pre-confirmed window has a hole; deferring until finalisation closes the gap"
                    );
                }
                cache.mark_fresh();
            }
        }

        // Complete the previous block off the critical path. Any missing tail data will
        // be filled in the window, incrementing its generation, which is a signal to
        // serve the fresher data on the next poll.
        if let Some(prev) = prev_to_complete {
            let sequencer = sequencer.clone();
            let window = window.clone();
            tokio::spawn(async move {
                complete_previous_block(&sequencer, prev, &window).await;
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

/// Run a CPU-bound closure without starving the async runtime. Falls back to
/// inline execution on a current-thread runtime (e.g. in tests).
fn run_cpu_bound<R>(f: impl FnOnce() -> R) -> R {
    use tokio::runtime::{Handle, RuntimeFlavor};
    match Handle::try_current().map(|h| h.runtime_flavor()) {
        Ok(RuntimeFlavor::MultiThread) => tokio::task::block_in_place(f),
        _ => f(),
    }
}

/// Fetch any missing tail data for the previous pre-confirmed block and upgrade
/// its window entry to a complete block so the next poll serves it in an
/// updated view with by-then likely updated new tip.
async fn complete_previous_block<S: GatewayApi + Send + 'static>(
    sequencer: &S,
    mut state: State,
    window: &Mutex<Window>,
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

    if state.apply(response) {
        let accumulated = state
            .accumulated
            .as_ref()
            .expect("accumulated present after successful completion apply");
        let number = state.block_number;
        let converted = run_cpu_bound(|| {
            PendingData::try_from_pre_confirmed_block(Box::new(accumulated.clone()), number)
        });
        match converted {
            Ok(pending) => {
                // Upgrade the window entry to the complete block (full transactions,
                // receipts/events and composed state diff).
                window
                    .lock()
                    .unwrap()
                    .record(number, Arc::new(pending.to_pre_latest_data()), true);
            }
            Err(e) => tracing::info!(
                block_number = %number, error = %e,
                "Failed to convert completed pre-confirmed block; skipping window upgrade"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use pathfinder_common::macro_prelude::*;
    use pathfinder_common::prelude::*;
    use pathfinder_common::transaction::{L1HandlerTransaction, Transaction, TransactionVariant};
    use pathfinder_pending_data::PendingDataCache;
    use starknet_gateway_client::MockGatewayApi;
    use starknet_gateway_types::reply::{PreConfirmedBlock, PreConfirmedPollResponse, Status};
    use tokio::sync::watch;

    use super::{complete_previous_block, poll_pre_confirmed, State};

    const TEST_IN_SYNC_THRESHOLD: u64 = 6;

    /// Arbitrary upper bound for awaiting a cache update in tests, so a failing
    /// test fails fast instead of hanging.
    const TEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

    /// A pre-confirmed block whose every transaction carries a receipt, so the
    /// `try_from_pre_confirmed_block` conversion accepts it (it rejects blocks
    /// containing candidate transactions).
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

        fn state(identifier: Option<&str>, txs: usize) -> State {
            State {
                block_number: BlockNumber::new_or_panic(10),
                block_identifier: identifier.map(String::from),
                accumulated: if identifier.is_some() {
                    Some(block_with_txs(txs))
                } else {
                    None
                },
            }
        }

        #[test]
        fn unchanged_response_is_noop() {
            let mut s = state(Some("abc"), 1);
            let original_accumulated = s.accumulated.clone();
            let original_identifier = s.block_identifier.clone();
            let original_number = s.block_number;

            let emitted = s.apply(PreConfirmedPollResponse::Unchanged);

            assert!(!emitted);
            assert_eq!(s.accumulated, original_accumulated);
            assert_eq!(s.block_identifier, original_identifier);
            assert_eq!(s.block_number, original_number);
        }

        #[test]
        fn delta_with_mismatching_identifier_is_skipped() {
            let mut s = state(Some("abc"), 1);
            let original_accumulated = s.accumulated.clone();

            let emitted = s.apply(PreConfirmedPollResponse::Delta {
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
            });

            assert!(!emitted);
            assert_eq!(s.accumulated, original_accumulated);
            assert_eq!(s.block_identifier, Some("abc".into()));
        }

        #[test]
        fn delta_with_matching_identifier_and_empty_transactions_is_noop() {
            let mut s = state(Some("abc"), 1);
            let original_accumulated = s.accumulated.clone();

            let emitted = s.apply(PreConfirmedPollResponse::Delta {
                identifier: "abc".into(),
                new_transactions: vec![],
                new_receipts: vec![],
                new_state_diffs: vec![],
            });

            assert!(!emitted);
            assert_eq!(s.accumulated, original_accumulated);
            assert_eq!(s.block_identifier, Some("abc".into()));
        }

        #[test]
        fn delta_with_matching_identifier_appends() {
            let mut s = state(Some("abc"), 1);

            let new_tx = Transaction {
                hash: transaction_hash!("0x99"),
                variant: TransactionVariant::L1Handler(L1HandlerTransaction {
                    contract_address: contract_address!("0x1"),
                    entry_point_selector: entry_point!("0x55"),
                    nonce: transaction_nonce!("0x0"),
                    calldata: Vec::new(),
                }),
            };

            let emitted = s.apply(PreConfirmedPollResponse::Delta {
                identifier: "abc".into(),
                new_transactions: vec![new_tx],
                new_receipts: vec![None],
                new_state_diffs: vec![None],
            });

            assert!(emitted);
            let acc = s.accumulated.as_ref().unwrap();
            assert_eq!(acc.transactions.len(), 2);
            assert_eq!(acc.transaction_receipts.len(), 2);
            assert_eq!(acc.transaction_state_diffs.len(), 2);
            assert_eq!(s.block_identifier, Some("abc".into()));
        }

        #[test]
        fn full_with_changed_identifier_emits() {
            let mut s = state(Some("abc"), 1);
            let new_block = block_with_txs(1);

            let emitted = s.apply(PreConfirmedPollResponse::Full {
                identifier: "xyz".into(),
                block_number: Some(BlockNumber::new_or_panic(10)),
                block: new_block.clone(),
            });

            assert!(emitted);
            assert_eq!(s.block_identifier, Some("xyz".into()));
            assert_eq!(s.accumulated, Some(new_block));
        }

        #[test]
        fn full_with_more_transactions_emits() {
            let mut s = state(Some("abc"), 1);
            let new_block = block_with_txs(2);

            let emitted = s.apply(PreConfirmedPollResponse::Full {
                identifier: "abc".into(),
                block_number: Some(BlockNumber::new_or_panic(10)),
                block: new_block.clone(),
            });

            assert!(emitted);
            assert_eq!(s.accumulated.as_ref().unwrap().transactions.len(), 2);
        }

        #[test]
        fn full_with_no_signals_is_deduped() {
            let mut s = state(Some("abc"), 2);
            let same_block = block_with_txs(2);
            let original_accumulated = s.accumulated.clone();

            // Same identifier and no new txs: nothing to emit.
            let emitted = s.apply(PreConfirmedPollResponse::Full {
                identifier: "abc".into(),
                block_number: Some(BlockNumber::new_or_panic(10)),
                block: same_block,
            });

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

    /// Once a view is published, a failing gateway must not take the cache
    /// down: the last good view keeps being served rather than blanking to an
    /// error.
    #[tokio::test]
    async fn retains_last_good_view_on_fetch_failure() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        use starknet_gateway_types::error::SequencerError;

        let calls = Arc::new(AtomicUsize::new(0));
        let mut sequencer = MockGatewayApi::new();
        // The first poll succeeds and publishes tip 101 (gap of one → no
        // parents to complete); every poll after it fails.
        sequencer
            .expect_preconfirmed_block()
            .returning(move |_, _, _| {
                if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    Ok(PreConfirmedPollResponse::Full {
                        identifier: "id-101".into(),
                        block_number: Some(BlockNumber::new_or_panic(101)),
                        block: all_receipted_block(1),
                    })
                } else {
                    Err(SequencerError::InvalidResponse("boom".into()))
                }
            });

        let committed = BlockNumber::new_or_panic(100);
        let cache = Arc::new(PendingDataCache::new());
        let mut sub = cache.subscribe();
        let _ = sub.borrow_and_update();

        spawn_producer(sequencer, committed, block_hash!("0xc0"), cache.clone());

        // The first poll publishes the good view.
        tokio::time::timeout(TEST_TIMEOUT, sub.changed())
            .await
            .expect("first view should be published")
            .unwrap();
        assert_eq!(
            sub.borrow().pre_confirmed_block_number(),
            BlockNumber::new_or_panic(101)
        );

        // Let the failing polls run.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // The cache still serves the last good view rather than an error.
        let served = cache.read().await.expect("cache must stay serviceable");
        assert_eq!(
            served.pre_confirmed_block_number(),
            BlockNumber::new_or_panic(101)
        );
    }

    /// The completion query for a superseded block carries any tail
    /// transactions that landed just before it advanced; the completion must
    /// record the full block (with the tail) as a *complete* window entry so
    /// the next poll re-serves the tip with it. It publishes nothing
    /// itself.
    #[tokio::test]
    async fn complete_previous_block_records_the_completed_block() {
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
        };

        // The completion records the completed block as a complete window
        // entry, carrying the tail transaction — ready for the next poll to
        // re-serve the tip with it.
        let window = std::sync::Mutex::new(super::Window::default());
        complete_previous_block(&sequencer, state, &window).await;

        let guard = window.lock().unwrap();
        let entry = guard
            .blocks
            .get(&BlockNumber::new_or_panic(11))
            .expect("the completion should record the block in the window");
        assert!(entry.complete, "the recorded entry should be complete");
        assert!(
            entry
                .data
                .block
                .transactions
                .iter()
                .any(|t| t.hash == tail_hash),
            "the tail transaction should be in the recorded block"
        );
    }

    mod window {
        use std::sync::Arc;

        use pathfinder_common::BlockNumber;
        use pathfinder_pending_data::{PreLatestBlock, PreLatestData};

        use super::super::Window;

        fn bn(n: u64) -> BlockNumber {
            BlockNumber::new_or_panic(n)
        }

        // An entry stamped with its block number, as the producer records it.
        fn entry(n: u64) -> Arc<PreLatestData> {
            Arc::new(PreLatestData {
                block: PreLatestBlock {
                    number: bn(n),
                    ..Default::default()
                },
                ..Default::default()
            })
        }

        fn with_blocks(complete: &[u64], provisional: &[u64]) -> Window {
            let mut window = Window::default();
            for &n in complete {
                window.record(bn(n), entry(n), true);
            }
            for &n in provisional {
                window.record(bn(n), entry(n), false);
            }
            window
        }

        fn parent_numbers(parents: &[Arc<PreLatestData>]) -> Vec<u64> {
            parents.iter().map(|p| p.block.number.get()).collect()
        }

        #[test]
        fn collect_gap_of_one_needs_no_parents() {
            let window = Window::default();
            let parents = window.collect(bn(5), bn(6)).unwrap();
            assert!(parents.is_empty());
        }

        #[test]
        fn collect_contiguous_window_collects_every_parent() {
            let window = with_blocks(&[6, 7, 8], &[9, 10]);
            let parents = window.collect(bn(5), bn(11)).unwrap();
            assert_eq!(parent_numbers(&parents), vec![6, 7, 8, 9, 10]);
        }

        #[test]
        fn collect_a_hole_yields_none() {
            let window = with_blocks(&[6, 8], &[]);
            assert!(window.collect(bn(5), bn(9)).is_none());
        }

        #[test]
        fn collect_ignores_blocks_outside_the_open_range() {
            let window = with_blocks(&[4, 5, 6, 7, 8, 9, 10], &[]);
            let parents = window.collect(bn(5), bn(9)).unwrap();
            assert_eq!(parent_numbers(&parents), vec![6, 7, 8]);
        }

        #[test]
        fn record_complete_entry_upgrades_provisional_and_bumps_generation() {
            let mut window = Window::default();
            window.record(bn(7), entry(7), false);
            let after_provisional = window.generation;
            assert!(
                after_provisional > 0,
                "inserting provisional bumps the generation"
            );
            assert!(!window.blocks[&bn(7)].complete);

            window.record(bn(7), entry(7), true);
            assert!(window.blocks[&bn(7)].complete);
            assert!(
                window.generation > after_provisional,
                "an upgrade bumps the generation"
            );
        }

        #[test]
        fn record_does_not_upgrade_a_complete_entry_or_bump() {
            let mut window = Window::default();
            window.record(bn(7), entry(7), true);
            let gen = window.generation;

            // Upgrading with another provisional entry is a no-op.
            window.record(bn(7), entry(7), false);
            // Upgrading with another complete entry is a no-op.
            window.record(bn(7), entry(7), true);
            assert_eq!(window.generation, gen);
            assert!(window.blocks[&bn(7)].complete);
        }

        #[test]
        fn record_provisional_does_not_replace_provisional() {
            let mut window = Window::default();
            window.record(bn(7), entry(7), false);
            let gen = window.generation;
            // A second provisional record for the same block is a no-op.
            window.record(bn(7), entry(7), false);
            assert_eq!(window.generation, gen);
        }

        #[test]
        fn prune_drops_committed_blocks() {
            let mut window = with_blocks(&[6, 7, 8], &[]);
            window.prune(bn(7));
            assert!(!window.blocks.contains_key(&bn(6)));
            assert!(!window.blocks.contains_key(&bn(7)));
            assert!(window.blocks.contains_key(&bn(8)));
        }
    }

    fn block_with_tx(tag: TransactionHash) -> PreConfirmedBlock {
        let tx = Transaction {
            hash: tag,
            variant: TransactionVariant::L1Handler(L1HandlerTransaction {
                contract_address: contract_address!("0x1"),
                entry_point_selector: entry_point!("0x55"),
                nonce: transaction_nonce!("0x0"),
                calldata: Vec::new(),
            }),
        };
        let receipt = pathfinder_common::receipt::Receipt {
            transaction_hash: tag,
            transaction_index: TransactionIndex::new_or_panic(0),
            ..Default::default()
        };
        PreConfirmedBlock {
            status: Status::PreConfirmed,
            transactions: vec![tx],
            transaction_receipts: vec![Some((receipt, vec![]))],
            transaction_state_diffs: vec![None],
            ..Default::default()
        }
    }

    fn tx_for(height: u64) -> TransactionHash {
        match height {
            11 => transaction_hash!("0x11"),
            12 => transaction_hash!("0x12"),
            _ => transaction_hash!("0x13"),
        }
    }

    /// End-to-end: with finalisation lagging (committed head fixed, no gateway
    /// pre-latest), the producer fills its window from block *completions* and
    /// serves a multi-block view whose deep ancestor's transactions are
    /// present. This proves no tail is lost for a window wider than two
    /// blocks, and that the re-serve-on-warm path fires (the tip's contents
    /// don't change between the poll that first sees it and the poll that
    /// finally bridges it).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn windowed_serve_preserves_deep_ancestor_tail() {
        use starknet_gateway_client::BlockId;

        let mut sequencer = MockGatewayApi::new();

        // `Latest` polls advance the tip 11, 12, 13 (then hold at 13, with a stable
        // identifier so the contents stop changing - forcing the re-serve to rely on
        // the window warming up, not on a content change). Concrete-number queries are
        // completions and return that block in full.
        static HEIGHT: Mutex<u64> = Mutex::new(10);
        sequencer
            .expect_preconfirmed_block()
            .returning(move |block_id, _, _| match block_id {
                BlockId::Latest => {
                    let mut height = HEIGHT.lock().unwrap();
                    if *height < 13 {
                        *height += 1;
                    }
                    let height = *height;
                    Ok(PreConfirmedPollResponse::Full {
                        identifier: format!("tip-{height}"),
                        block_number: Some(BlockNumber::new_or_panic(height)),
                        block: block_with_tx(tx_for(height)),
                    })
                }
                BlockId::Number(number) => {
                    let number = number.get();
                    Ok(PreConfirmedPollResponse::Full {
                        identifier: format!("complete-{number}"),
                        block_number: Some(BlockNumber::new_or_panic(number)),
                        block: block_with_tx(tx_for(number)),
                    })
                }
                other => panic!("unexpected block id: {other:?}"),
            });

        let committed = BlockNumber::new_or_panic(10);
        let cache = Arc::new(PendingDataCache::new());
        let mut sub = cache.subscribe();
        let _ = sub.borrow_and_update();

        spawn_producer(sequencer, committed, block_hash!("0xbeef"), cache.clone());

        // Wait until the producer serves block 13 with its full window: deep ancestor
        // 11 plus immediate parent 12.
        let deadline = tokio::time::Instant::now() + TEST_TIMEOUT;
        loop {
            tokio::time::timeout_at(deadline, sub.changed())
                .await
                .expect("producer should serve the windowed view before timeout")
                .unwrap();
            let pending = sub.borrow().clone();
            if pending.pre_confirmed_block_number() != BlockNumber::new_or_panic(13) {
                continue;
            }
            let parents: Vec<_> = pending.parent_blocks().map(|p| p.block.number).collect();
            if parents != vec![BlockNumber::new_or_panic(11), BlockNumber::new_or_panic(12)] {
                continue;
            }

            // The deep ancestor's, the immediate parent's and the tip's transactions are
            // all present - no tail lost across the window.
            assert!(pending.find_transaction(tx_for(11)).is_some());
            assert!(pending.find_transaction(tx_for(12)).is_some());
            assert!(pending.find_transaction(tx_for(13)).is_some());
            break;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn windowed_serve_does_not_wait_for_completions() {
        use std::sync::Mutex as StdMutex;

        use starknet_gateway_client::BlockId;
        use starknet_gateway_types::error::SequencerError;

        let mut sequencer = MockGatewayApi::new();

        static HEIGHT: StdMutex<u64> = StdMutex::new(10);
        sequencer
            .expect_preconfirmed_block()
            .returning(move |block_id, _, _| match block_id {
                BlockId::Latest => {
                    let mut height = HEIGHT.lock().unwrap();
                    if *height < 13 {
                        *height += 1;
                    }
                    let height = *height;
                    Ok(PreConfirmedPollResponse::Full {
                        identifier: format!("tip-{height}"),
                        block_number: Some(BlockNumber::new_or_panic(height)),
                        block: block_with_tx(tx_for(height)),
                    })
                }
                // Completion fetches always fail — serving must not depend on them.
                _ => Err(SequencerError::InvalidResponse("no completions".into())),
            });

        let committed = BlockNumber::new_or_panic(10);
        let cache = Arc::new(PendingDataCache::new());
        let mut sub = cache.subscribe();
        let _ = sub.borrow_and_update();

        spawn_producer(sequencer, committed, block_hash!("0xbeef"), cache.clone());

        let deadline = tokio::time::Instant::now() + TEST_TIMEOUT;
        loop {
            tokio::time::timeout_at(deadline, sub.changed())
                .await
                .expect("producer should serve from provisional entries before timeout")
                .unwrap();
            let pending = sub.borrow().clone();
            if pending.pre_confirmed_block_number() != BlockNumber::new_or_panic(13) {
                continue;
            }
            let parents: Vec<_> = pending.parent_blocks().map(|p| p.block.number).collect();
            if parents != vec![BlockNumber::new_or_panic(11), BlockNumber::new_or_panic(12)] {
                continue;
            }
            // Both un-committed parents are present from provisional data alone.
            assert!(pending.find_transaction(tx_for(11)).is_some());
            assert!(pending.find_transaction(tx_for(12)).is_some());
            assert!(pending.find_transaction(tx_for(13)).is_some());
            break;
        }
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

        let latest_hash = block_hash!("0xabcd");
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
