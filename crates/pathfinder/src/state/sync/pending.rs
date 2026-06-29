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
