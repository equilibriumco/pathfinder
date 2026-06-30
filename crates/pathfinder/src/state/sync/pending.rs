use std::sync::Arc;

use pathfinder_common::{BlockHash, BlockNumber};
use pathfinder_pending_data::{PendingData, PendingDataCache};
use starknet_gateway_client::GatewayApi;
use starknet_gateway_types::reply::{PreConfirmedBlock, PreConfirmedPollResponse};
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

        // The pre-latest sits at committed+1, decided in consensus and only
        // awaiting its block hash; the pre-confirmed at committed+2 is still
        // building. Fetch both by number so they chain onto our head by
        // construction. The pre-confirmed carries deltas between polls; the
        // pre-latest is settled, so we take it whole each time.
        let pre_latest_height = committed + 1;
        let pre_confirmed_height = committed + 2;
        let (cursor_id, cursor_count) = delta_cursor(&tracked, pre_confirmed_height);
        let (pre_latest_result, pre_confirmed_result) = tokio::join!(
            sequencer.preconfirmed_block(pre_latest_height.into(), None, 0),
            sequencer.preconfirmed_block(pre_confirmed_height.into(), cursor_id, cursor_count),
        );

        // The pre-latest is the foundation: without it nothing chains onto our
        // head, so we keep serving the last good view and try again.
        let (pre_latest_identifier, pre_latest_block) = match pre_latest_result {
            Ok(PreConfirmedPollResponse::Full {
                identifier, block, ..
            }) => (identifier, block),
            other => {
                match other {
                    Err(e) => {
                        tracing::debug!(%e, %pre_latest_height, "Pre-latest fetch failed; retaining last view")
                    }
                    Ok(_) => {
                        tracing::debug!(%pre_latest_height, "Pre-latest returned no full block; retaining last view")
                    }
                }
                cache.mark_fresh();
                wait_for_next_poll(t_fetch + poll_interval, &cache).await;
                continue;
            }
        };

        // A pre-confirmed above the pre-latest means we serve the two chained.
        // When it isn't there yet, the pre-latest is itself the newest block, so
        // we serve it alone at committed+1.
        let (number, response, pre_latest) = match pre_confirmed_result {
            Ok(response) => (
                pre_confirmed_height,
                response,
                Some(Box::new((
                    pre_latest_height,
                    pre_latest_block,
                    committed_hash,
                ))),
            ),
            Err(e) => {
                tracing::debug!(%e, %pre_confirmed_height, "No pre-confirmed above pre-latest; serving it alone");
                let response = PreConfirmedPollResponse::Full {
                    identifier: pre_latest_identifier,
                    block_number: None,
                    block: pre_latest_block,
                };
                (pre_latest_height, response, None)
            }
        };

        // Merge and publish.
        let changed = apply(&mut tracked, number, response);
        match tracked.as_ref().filter(|_| changed) {
            Some(t) => {
                let block = t.block.clone();
                store_pre_confirmed(&cache, number, block.into(), pre_latest);
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
    pre_latest: Option<Box<(BlockNumber, PreConfirmedBlock, BlockHash)>>,
) {
    match PendingData::try_from_pre_confirmed_and_pre_latest(block, number, pre_latest) {
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use pathfinder_common::macro_prelude::*;
    use pathfinder_common::receipt::Receipt;
    use pathfinder_common::transaction::{L1HandlerTransaction, Transaction, TransactionVariant};
    use pathfinder_common::{BlockHash, BlockNumber, TransactionIndex};
    use pathfinder_pending_data::PendingDataCache;
    use starknet_gateway_client::{BlockId, MockGatewayApi};
    use starknet_gateway_types::error::{KnownStarknetErrorCode, SequencerError, StarknetError};
    use starknet_gateway_types::reply::{PreConfirmedBlock, PreConfirmedPollResponse, Status};
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

    /// A full poll response carrying `block`.
    fn full(block: PreConfirmedBlock) -> PreConfirmedPollResponse {
        PreConfirmedPollResponse::Full {
            identifier: "id".into(),
            block_number: None,
            block,
        }
    }

    /// The error the gateway returns for a height that hasn't been built yet.
    fn block_not_found() -> SequencerError {
        SequencerError::StarknetError(StarknetError {
            code: KnownStarknetErrorCode::BlockNotFound.into(),
            message: "Block not found".into(),
        })
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

    /// The common path: a decided block sits above our head as the pre-latest
    /// at committed+1, with the pre-confirmed still building on top at
    /// committed+2. The producer fetches both by number and stores them as
    /// one chained view.
    #[tokio::test]
    async fn stores_chained_view_when_pre_latest_present() {
        let committed = BlockNumber::new_or_panic(100);
        let committed_hash = block_hash!("0xc0");

        let mut sequencer = MockGatewayApi::new();
        // The pre-latest at committed+1 and the pre-confirmed that chains onto it
        // at committed+2.
        sequencer
            .expect_preconfirmed_block()
            .returning(|block, _, _| match block {
                BlockId::Number(n) if n == BlockNumber::new_or_panic(101) => {
                    Ok(full(pre_confirmed(1)))
                }
                _ => Ok(full(pre_confirmed(2))),
            });

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

    /// With nothing building above it yet, committed+2 isn't there, so the
    /// pre-latest is itself the newest block. The producer serves it alone at
    /// committed+1.
    #[tokio::test]
    async fn serves_pre_latest_alone_when_nothing_above_it() {
        let committed = BlockNumber::new_or_panic(100);
        let committed_hash = block_hash!("0xc0");

        let mut sequencer = MockGatewayApi::new();
        // committed+1 is there; committed+2 hasn't been built yet.
        sequencer
            .expect_preconfirmed_block()
            .returning(|block, _, _| match block {
                BlockId::Number(n) if n == BlockNumber::new_or_panic(101) => {
                    Ok(full(pre_confirmed(1)))
                }
                _ => Err(block_not_found()),
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

        let pre_latest_calls = Arc::new(AtomicUsize::new(0));
        let mut sequencer = MockGatewayApi::new();
        // committed+2 always answers; committed+1 answers the first poll, then
        // fails. Losing the foundation must not blank the cache.
        sequencer
            .expect_preconfirmed_block()
            .returning(move |block, _, _| match block {
                BlockId::Number(n) if n == BlockNumber::new_or_panic(101) => {
                    if pre_latest_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                        Ok(full(pre_confirmed(1)))
                    } else {
                        Err(SequencerError::InvalidResponse("boom".into()))
                    }
                }
                _ => Ok(full(pre_confirmed(2))),
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
