use std::sync::Arc;

use anyhow::Context;
use pathfinder_common::{BlockHash, BlockNumber, StateUpdate};
use pathfinder_pending_data::{PendingData, PendingDataCache};
use starknet_gateway_client::GatewayApi;
use starknet_gateway_types::reply::{PreConfirmedBlock, PreConfirmedPollResponse, PreLatestBlock};
use tokio::sync::watch;
use tokio::time::Instant;

/// The pre-confirmed block we're building up, kept between polls so the gateway
/// can send just the new transactions each time instead of the whole block.
#[derive(Debug)]
struct Tracked {
    /// The pre-confirmed height this view is for.
    number: BlockNumber,
    /// The gateway's id for the view we're holding.
    identifier: String,
    /// The block we've merged so far, at height `number`.
    block: PreConfirmedBlock,
}

impl Tracked {
    fn tx_count(&self) -> u64 {
        self.block.transactions.len() as u64
    }
}

/// What we already have at `height`: our id and tx count if we're holding it,
/// or empty to fetch the whole block.
fn delta_cursor(tracked: &Option<Tracked>, height: BlockNumber) -> (Option<String>, u64) {
    match tracked {
        Some(t) if t.number == height => (Some(t.identifier.clone()), t.tx_count()),
        _ => (None, 0),
    }
}

/// Merge a poll response into the tracked view at `target`. Returns `true` when
/// the published view should be refreshed.
fn apply(
    tracked: &mut Option<Tracked>,
    target: BlockNumber,
    response: PreConfirmedPollResponse,
) -> bool {
    match response {
        PreConfirmedPollResponse::Unchanged => false,

        PreConfirmedPollResponse::Delta {
            identifier,
            new_transactions,
            new_receipts,
            new_state_diffs,
        } => {
            // A delta only applies on top of the exact view it was computed from,
            // so the height and identifier have to match.
            let matched = tracked
                .as_mut()
                .filter(|t| t.number == target && t.identifier == identifier);
            let Some(tracked) = matched else {
                tracing::warn!(%target, "Delta response with no matching tracked view; skipping");
                return false;
            };
            if new_transactions.is_empty() {
                return false;
            }
            tracked.block.transactions.extend(new_transactions);
            tracked.block.transaction_receipts.extend(new_receipts);
            tracked
                .block
                .transaction_state_diffs
                .extend(new_state_diffs);
            true
        }

        PreConfirmedPollResponse::Full {
            identifier, block, ..
        } => {
            // A Full response can repeat the view we already have. Only publish
            // on a real change: a new identifier or more transactions.
            let changed = match tracked.as_ref() {
                Some(t) if t.number == target => {
                    t.identifier != identifier || block.transactions.len() as u64 > t.tx_count()
                }
                _ => true,
            };
            *tracked = Some(Tracked {
                number: target,
                identifier,
                block,
            });
            changed
        }
    }
}

/// Emits new pending data while the current block is close to the latest block.
///
/// Suspends polling once the cache reports itself idle and resumes on the next
/// read.
pub(super) async fn poll_pre_confirmed<S: GatewayApi + Clone + Send + 'static>(
    sequencer: S,
    poll_interval: std::time::Duration,
    cache: Arc<PendingDataCache>,
    latest: watch::Receiver<(BlockNumber, BlockHash)>,
    current: watch::Receiver<(BlockNumber, BlockHash)>,
    in_sync_threshold: u64,
) {
    let mut tracked: Option<Tracked> = None;

    loop {
        // Suspend if idle.
        if cache.is_idle() && cache.subscriber_count() == 0 {
            tracing::debug!("Pre-confirmed polling idle; waiting for cache reads");
            cache.mark_stale();
            cache.wait_for_read().await;
        }

        let t_fetch = Instant::now();

        let (committed, committed_hash) = *current.borrow();
        let gateway_latest = latest.borrow().0;

        // Skip while catching up to head.
        if gateway_latest.get().abs_diff(committed.get()) > in_sync_threshold {
            tracing::debug!(
                latest = %gateway_latest, %committed,
                "Not in sync yet; skipping pre-confirmed block download"
            );
            cache.mark_unavailable("syncing");
            wait_for_next_poll(t_fetch + poll_interval, &cache).await;
            continue;
        }

        // Assume the common shape: a pre-latest at committed+1 with the
        // pre-confirmed at committed+2. Fetch that height and the pending block
        // concurrently.
        let assumed = committed + 2;
        let (cursor_id, cursor_count) = delta_cursor(&tracked, assumed);
        let (pc_result, pre_latest_result) = tokio::join!(
            sequencer.preconfirmed_block(assumed.into(), cursor_id, cursor_count),
            fetch_pre_latest(&sequencer, committed, committed_hash),
        );

        let pre_latest = match pre_latest_result {
            Ok(pre_latest) => pre_latest,
            Err(e) => {
                tracing::debug!(%e, "Failed to fetch pre-latest block");
                None
            }
        };

        // Now pending() tells us if there's really a pre-latest. With one, committed+2
        // is the pre-confirmed; without one it's committed+1, so we drop what we
        // fetched and get committed+1 instead.
        let (number, response) = if pre_latest.is_some() {
            (assumed, pc_result)
        } else {
            let number = committed + 1;
            let (cursor_id, cursor_count) = delta_cursor(&tracked, number);
            let response = sequencer
                .preconfirmed_block(number.into(), cursor_id, cursor_count)
                .await;
            (number, response)
        };

        // A failed or not-yet-available fetch must not blank the cache.
        let response = match response {
            Ok(response) => response,
            Err(e) => {
                tracing::debug!(%e, %number, "Pre-confirmed fetch failed; retaining last view");
                cache.mark_fresh();
                wait_for_next_poll(t_fetch + poll_interval, &cache).await;
                continue;
            }
        };

        // Merge and publish.
        let changed = apply(&mut tracked, number, response);
        match tracked.as_ref().filter(|_| changed) {
            Some(t) => {
                let block = t.block.clone();
                store_pre_confirmed(&cache, number, block.into(), pre_latest.map(Box::new));
            }
            None => cache.mark_fresh(),
        }

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

/// Convert a fresh pre-confirmed view and store it in the cache.
///
/// A failed conversion (say, candidate transactions) keeps the last good view.
fn store_pre_confirmed(
    cache: &PendingDataCache,
    number: BlockNumber,
    block: Box<PreConfirmedBlock>,
    pre_latest_data: Option<Box<(BlockNumber, PreLatestBlock, StateUpdate)>>,
) {
    match PendingData::try_from_pre_confirmed_and_pre_latest(block, number, pre_latest_data) {
        Ok(pending) => {
            cache.store(pending);
            tracing::debug!(block_number = %number, "Updated pre-confirmed data");
        }
        Err(e) => {
            tracing::info!(block_number=%number, error=%e, "Pre-confirmed failed validation; retaining last view");
            cache.mark_fresh();
        }
    }
}

/// Fetch the pending block from the sequencer and classify it as
/// [pre-latest](starknet_gateway_types::reply::PreLatestBlock) if it builds on
/// our committed head.
///
/// If the pre-latest block (committed+1) exists, the sequencer has already
/// started building the next pre-confirmed block (committed+2).
async fn fetch_pre_latest<S: GatewayApi + Send + 'static>(
    sequencer: &S,
    committed: BlockNumber,
    committed_hash: BlockHash,
) -> anyhow::Result<Option<(BlockNumber, PreLatestBlock, StateUpdate)>> {
    let (pending_block, state_update) = sequencer
        .pending_block()
        .await
        .context("Fetching pre-latest block from sequencer")?;

    let pre_latest_data = (pending_block.parent_hash == committed_hash).then_some((
        committed + 1,
        pending_block,
        state_update,
    ));
    Ok(pre_latest_data)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use pathfinder_common::macro_prelude::*;
    use pathfinder_common::receipt::Receipt;
    use pathfinder_common::transaction::{L1HandlerTransaction, Transaction, TransactionVariant};
    use pathfinder_common::{BlockHash, BlockNumber, StateUpdate, TransactionIndex};
    use pathfinder_pending_data::PendingDataCache;
    use starknet_gateway_client::{BlockId, MockGatewayApi};
    use starknet_gateway_types::error::SequencerError;
    use starknet_gateway_types::reply::{
        PreConfirmedBlock,
        PreConfirmedPollResponse,
        PreLatestBlock,
        Status,
    };
    use tokio::sync::watch;
    use tokio::time::timeout;

    use super::{apply, poll_pre_confirmed, Tracked};

    const IN_SYNC: u64 = 6;
    const TIMEOUT: Duration = Duration::from_secs(5);

    /// An L1-handler transaction with a small, distinct hash.
    fn sample_tx(i: usize) -> Transaction {
        let hash = match i {
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
                calldata: vec![],
            }),
        }
    }

    /// A pre-confirmed block with `n` receipted transactions, so the conversion
    /// into pending data accepts it.
    fn pre_confirmed(n: usize) -> PreConfirmedBlock {
        let transactions: Vec<_> = (0..n).map(sample_tx).collect();
        let transaction_receipts = transactions
            .iter()
            .enumerate()
            .map(|(i, t)| {
                Some((
                    Receipt {
                        transaction_hash: t.hash,
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

    /// A pending block that builds on `parent`.
    fn pending_on(parent: BlockHash) -> PreLatestBlock {
        PreLatestBlock {
            parent_hash: parent,
            status: Status::Pending,
            ..Default::default()
        }
    }

    /// A full poll response carrying `block`.
    fn full(block: PreConfirmedBlock) -> PreConfirmedPollResponse {
        PreConfirmedPollResponse::Full {
            identifier: "id".into(),
            block_number: None,
            block,
        }
    }

    /// Spawn the producer against `sequencer`, polling fast and anchored at
    /// `committed`.
    fn spawn(
        sequencer: MockGatewayApi,
        committed: BlockNumber,
        committed_hash: BlockHash,
        cache: Arc<PendingDataCache>,
    ) {
        let (_latest, latest) = watch::channel((committed, committed_hash));
        let (_current, current) = watch::channel((committed, committed_hash));
        let sequencer = Arc::new(sequencer);
        tokio::spawn(async move {
            poll_pre_confirmed(
                sequencer,
                Duration::from_millis(1),
                cache,
                latest,
                current,
                IN_SYNC,
            )
            .await
        });
    }

    /// The common path: a pending block sits above our head, so it's the
    /// pre-latest at committed+1 and the pre-confirmed chains onto it at
    /// committed+2. The producer stores the two as one view.
    #[tokio::test]
    async fn stores_chained_view_when_pre_latest_present() {
        let committed = BlockNumber::new_or_panic(100);
        let committed_hash = block_hash!("0xc0");

        let mut sequencer = MockGatewayApi::new();
        // The pending block builds on our head: the pre-latest at committed+1.
        sequencer
            .expect_pending_block()
            .returning(move || Ok((pending_on(committed_hash), StateUpdate::default())));
        // The pre-confirmed that chains onto it, at committed+2.
        sequencer
            .expect_preconfirmed_block()
            .returning(|_, _, _| Ok(full(pre_confirmed(2))));

        let cache = Arc::new(PendingDataCache::new());
        let mut changes = cache.subscribe();
        let _ = changes.borrow_and_update();

        spawn(sequencer, committed, committed_hash, cache.clone());

        // Wait for the producer to publish.
        timeout(TIMEOUT, changes.changed())
            .await
            .expect("a view should be published")
            .unwrap();

        let stored = changes.borrow();
        assert_eq!(
            stored.pre_confirmed_block_number(),
            BlockNumber::new_or_panic(102)
        );
        assert_eq!(
            stored.pre_latest_block_number(),
            Some(BlockNumber::new_or_panic(101))
        );
        assert_eq!(stored.pre_confirmed_transactions().len(), 2);
    }

    /// With nothing closing above our head, the pending block doesn't chain
    /// onto us, so there's no pre-latest. The producer corrects inside the
    /// same poll and stores the pre-confirmed at committed+1, alone.
    #[tokio::test]
    async fn corrects_to_committed_plus_one_without_pre_latest() {
        let committed = BlockNumber::new_or_panic(100);
        let committed_hash = block_hash!("0xc0");

        let mut sequencer = MockGatewayApi::new();
        // The pending block builds on someone else, so it isn't our pre-latest.
        sequencer
            .expect_pending_block()
            .returning(|| Ok((pending_on(block_hash!("0xdead")), StateUpdate::default())));
        // committed+2 is fetched on the assumption and dropped; committed+1 is
        // the block we actually serve.
        sequencer
            .expect_preconfirmed_block()
            .returning(|block, _, _| match block {
                BlockId::Number(n) if n == BlockNumber::new_or_panic(101) => {
                    Ok(full(pre_confirmed(1)))
                }
                _ => Ok(full(pre_confirmed(0))),
            });

        let cache = Arc::new(PendingDataCache::new());
        let mut changes = cache.subscribe();
        let _ = changes.borrow_and_update();

        spawn(sequencer, committed, committed_hash, cache.clone());

        timeout(TIMEOUT, changes.changed())
            .await
            .expect("a view should be published")
            .unwrap();

        let stored = changes.borrow();
        assert_eq!(
            stored.pre_confirmed_block_number(),
            BlockNumber::new_or_panic(101)
        );
        assert_eq!(stored.pre_latest_block_number(), None);
    }

    /// Once a view is published, a failing gateway must not take the cache
    /// down. The last good view keeps being served.
    #[tokio::test]
    async fn retains_last_good_view_on_fetch_failure() {
        let committed = BlockNumber::new_or_panic(100);
        let committed_hash = block_hash!("0xc0");

        let calls = Arc::new(AtomicUsize::new(0));
        let mut sequencer = MockGatewayApi::new();
        sequencer
            .expect_pending_block()
            .returning(move || Ok((pending_on(committed_hash), StateUpdate::default())));
        // The first poll succeeds; every poll after it fails.
        sequencer
            .expect_preconfirmed_block()
            .returning(move |_, _, _| {
                if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    Ok(full(pre_confirmed(1)))
                } else {
                    Err(SequencerError::InvalidResponse("boom".into()))
                }
            });

        let cache = Arc::new(PendingDataCache::new());
        let mut changes = cache.subscribe();
        let _ = changes.borrow_and_update();

        spawn(sequencer, committed, committed_hash, cache.clone());

        // The first poll publishes the good view.
        timeout(TIMEOUT, changes.changed())
            .await
            .expect("first view should be published")
            .unwrap();
        assert_eq!(
            changes.borrow().pre_confirmed_block_number(),
            BlockNumber::new_or_panic(102)
        );

        // Let the failing polls run.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // The cache still serves the last good view rather than an error.
        let served = cache.read().await.expect("cache must stay serviceable");
        assert_eq!(
            served.pre_confirmed_block_number(),
            BlockNumber::new_or_panic(102)
        );
    }

    /// While our head is far behind the chain, pre-confirmed reads fail fast as
    /// "syncing" instead of serving nothing.
    #[tokio::test(start_paused = true)]
    async fn marks_syncing_when_far_behind_head() {
        let hash = block_hash!("0xc0");
        // Our head is genesis; the chain head is well past the in-sync threshold.
        let (_current, current) = watch::channel((BlockNumber::GENESIS, hash));
        let (_latest, latest) = watch::channel((BlockNumber::new_or_panic(100), hash));

        // The sync guard returns before any fetch, so the gateway is never called.
        let sequencer = Arc::new(MockGatewayApi::new());
        let cache = Arc::new(PendingDataCache::new());
        tokio::spawn({
            let cache = cache.clone();
            async move {
                poll_pre_confirmed(
                    sequencer,
                    Duration::from_secs(1),
                    cache,
                    latest,
                    current,
                    IN_SYNC,
                )
                .await
            }
        });

        // Give the loop a turn to reach the guard.
        tokio::time::sleep(Duration::from_millis(50)).await;

        let err = cache.read().await.unwrap_err();
        assert!(err.to_string().contains("syncing"), "got: {err}");
    }

    /// A delta carries only the new transactions, which the producer appends
    /// onto the block it's already tracking at that height.
    #[test]
    fn delta_appends_to_the_tracked_block() {
        let height = BlockNumber::new_or_panic(102);

        // We're already tracking a block with one transaction.
        let mut tracked = Some(Tracked {
            number: height,
            identifier: "id".into(),
            block: pre_confirmed(1),
        });

        // A delta with a second transaction, tagged with the same identifier.
        let second = sample_tx(1);
        let republished = apply(
            &mut tracked,
            height,
            PreConfirmedPollResponse::Delta {
                identifier: "id".into(),
                new_transactions: vec![second.clone()],
                new_receipts: vec![Some((
                    Receipt {
                        transaction_hash: second.hash,
                        transaction_index: TransactionIndex::new_or_panic(1),
                        ..Default::default()
                    },
                    vec![],
                ))],
                new_state_diffs: vec![None],
            },
        );

        assert!(
            republished,
            "a delta with new transactions refreshes the view"
        );
        assert_eq!(tracked.unwrap().block.transactions.len(), 2);
    }
}
