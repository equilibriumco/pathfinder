use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cached::{Cached, TimedSizedCache};
use pathfinder_common::transaction::TransactionVariant;
use pathfinder_common::{BlockNumber, TransactionHash};
use tokio::sync::watch;

/// A tracker of submitted transactions and the latest blocks at the moment of
/// their submission.
#[derive(Clone, Debug)]
pub struct SubmittedTransactionTracker(Arc<Mutex<TrackerState>>);

#[derive(Clone, Debug)]
struct TrackerState {
    cache: TimedSizedCache<TransactionHash, TrackerValue>,
    sender: watch::Sender<BTreeSet<TransactionHash>>,
}

#[derive(Clone, Debug)]
struct TrackerValue {
    block_number: BlockNumber,
    transaction: TransactionVariant,
}

impl SubmittedTransactionTracker {
    pub fn new(limit_size: usize, limit_sec: u64) -> Self {
        Self(Arc::new(Mutex::new(TrackerState::new(
            limit_size, limit_sec,
        ))))
    }

    pub fn get_block(&self, hash: &TransactionHash) -> Option<BlockNumber> {
        let mut state = self.0.lock().unwrap();
        state.cache.cache_get(hash).map(|v| v.block_number)
    }

    pub fn get_transaction(&self, hash: &TransactionHash) -> Option<TransactionVariant> {
        let mut state = self.0.lock().unwrap();
        state.cache.cache_get(hash).map(|v| v.transaction.clone())
    }

    pub fn contains_key(&self, hash: &TransactionHash) -> bool {
        let mut state = self.0.lock().unwrap();
        let res = state.cache.cache_get(hash);
        res.is_some()
    }

    /// Inserts a transaction into the tracker.
    ///
    /// # Parameters
    ///
    /// - `hash`: The hash of the transaction to insert.
    /// - `latest_block`: The latest block number at the moment of submitting
    ///   the transaction.
    /// - `tx`: The transaction data.
    pub fn insert(&self, hash: TransactionHash, latest_block: BlockNumber, tx: TransactionVariant) {
        let mut state = self.0.lock().unwrap();
        state.cache.flush();
        let res = state.cache.cache_set(
            hash,
            TrackerValue {
                block_number: latest_block,
                transaction: tx,
            },
        );
        if res.is_some() {
            tracing::warn!("repeated tx hash in mempool: {}", hash);
        }
        state.update_subscribers();
    }

    pub fn subscribe(&self) -> watch::Receiver<BTreeSet<TransactionHash>> {
        let state = self.0.lock().unwrap();
        state.sender.subscribe()
    }

    pub fn clear(&self) {
        let mut state = self.0.lock().unwrap();
        state.cache.cache_clear();
        state.update_subscribers();
    }

    pub fn flush(&self) {
        let mut state = self.0.lock().unwrap();
        state.cache.flush();
        state.update_subscribers();
    }
}

impl TrackerState {
    fn new(limit_size: usize, limit_sec: u64) -> Self {
        let (sender, _receiver) = watch::channel(Default::default());
        let limit = Duration::from_secs(limit_sec);
        Self {
            cache: TimedSizedCache::with_size_and_lifespan(limit_size, limit),
            sender,
        }
    }

    fn update_subscribers(&mut self) {
        let mut out = BTreeSet::new();
        for hash in self.cache.key_order() {
            out.insert(*hash);
        }

        self.sender.send_replace(out);
    }
}

#[cfg(test)]
mod tests {
    use pathfinder_common::{BlockNumber, TransactionHash};
    use pathfinder_crypto::Felt;
    use tokio::time::Duration;

    use super::SubmittedTransactionTracker;

    #[test]
    fn test_full() {
        let tt = SubmittedTransactionTracker::new(2, 10);
        let mut hash = Default::default();
        assert!(!tt.contains_key(&hash));
        for i in 1..=10 {
            hash = TransactionHash(Felt::from_u64(i));
            tt.insert(hash, BlockNumber::new_or_panic(i), Default::default());
        }

        assert!(tt.contains_key(&hash));
        hash = TransactionHash(Felt::from_u64(1));
        assert!(!tt.contains_key(&hash));

        let block = tt.get_block(&TransactionHash(Felt::from_u64(9)));
        assert_eq!(block, Some(BlockNumber::new_or_panic(9)));
    }

    #[tokio::test]
    async fn test_flush() {
        let tt = SubmittedTransactionTracker::new(2, 1);
        let hash = TransactionHash(Felt::from_u64(42));
        tt.insert(hash, BlockNumber::new_or_panic(10), Default::default());
        assert!(tt.contains_key(&hash));
        tokio::time::sleep(Duration::from_millis(3000)).await;
        tt.flush();
        assert!(!tt.contains_key(&hash));
    }
}
