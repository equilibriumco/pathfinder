use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{watch, Notify};
use tokio::time::Instant;

use crate::data::PendingData;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Freshness {
    stale: bool,
    refresh_count: u64,
}

/// Shared pre-confirmed data cache with freshness tracking.
///
/// Holds the latest [`PendingData`] and a small amount of freshness state.
/// - Writes via [`Self::store`] / [`Self::refresh`] / [`Self::mark_stale`].
/// - Reads via [`Self::read`] (blocks if stale, then returns fresh data) and
///   [`Self::subscribe`] for streaming.
pub struct PendingDataCache {
    data_tx: watch::Sender<PendingData>,
    data_rx: watch::Receiver<PendingData>,
    on_read: Arc<Notify>,
    freshness_tx: watch::Sender<Freshness>,
    last_read: Mutex<Instant>,
    cold_start_timeout: Duration,
    inactivity_timeout: Duration,
}

impl PendingDataCache {
    /// Default maximum time a cold-start read will block before erroring.
    pub const DEFAULT_COLD_START_TIMEOUT: Duration = Duration::from_secs(5);
    /// Default window of read inactivity after which [`Self::is_idle`] reports
    /// the cache as idle.
    pub const DEFAULT_INACTIVITY_TIMEOUT: Duration = Duration::from_secs(60);

    pub fn new() -> Self {
        let (data_tx, data_rx) = watch::channel(PendingData::default());
        let (freshness_tx, _) = watch::channel(Freshness::default());
        Self {
            data_tx,
            data_rx,
            on_read: Arc::new(Notify::new()),
            freshness_tx,
            last_read: Mutex::new(Instant::now()),
            cold_start_timeout: Self::DEFAULT_COLD_START_TIMEOUT,
            inactivity_timeout: Self::DEFAULT_INACTIVITY_TIMEOUT,
        }
    }

    pub fn with_cold_start_timeout(mut self, timeout: Duration) -> Self {
        self.cold_start_timeout = timeout;
        self
    }

    pub fn with_inactivity_timeout(mut self, timeout: Duration) -> Self {
        self.inactivity_timeout = timeout;
        self
    }

    /// Writes fresh data into the cache and marks it fresh.
    pub fn store(&self, data: PendingData) {
        self.data_tx.send_replace(data);
        self.bump_freshness();
    }

    /// Marks the cache fresh without changing its contents.
    pub fn refresh(&self) {
        self.bump_freshness();
    }

    /// Marks the cache stale; subsequent [`Self::read`] calls block until
    /// the next [`Self::store`] / [`Self::refresh`] or a timeout elapses.
    pub fn mark_stale(&self) {
        self.freshness_tx.send_if_modified(|f| {
            if !f.stale {
                f.stale = true;
                return true;
            }
            false
        });
    }

    /// Resolves the next time [`Self::read`] is called.
    pub async fn wait_for_read(&self) {
        self.on_read.notified().await;
    }

    /// Returns the latest cached data. Blocks while the cache is stale,
    /// up to the configured cold-start timeout. Always fires the read signal.
    pub async fn read(&self) -> anyhow::Result<PendingData> {
        self.signal_read();
        self.wait_for_fresh_data().await?;
        Ok(self.data_rx.borrow().clone())
    }

    /// Returns the latest cached data if it is fresh, `None` otherwise.
    /// Fast path (non-blocking). Always fires the read signal.
    pub fn try_read(&self) -> Option<PendingData> {
        self.signal_read();
        if self.freshness_tx.borrow().stale {
            return None;
        }
        Some(self.data_rx.borrow().clone())
    }

    /// A fresh `watch::Receiver` for awaiting changes directly.
    pub fn subscribe(&self) -> watch::Receiver<PendingData> {
        self.signal_read();
        self.data_rx.clone()
    }

    pub fn subscriber_count(&self) -> usize {
        self.data_tx
            .receiver_count()
            // Exclude the cache's own internal receiver from the count.
            .saturating_sub(1)
    }

    /// `true` when no reads have happened within the inactivity window.
    pub fn is_idle(&self) -> bool {
        self.last_read.lock().unwrap().elapsed() >= self.inactivity_timeout
    }

    /// Fires the read signal and stamps the last-read time. Both move together
    /// so suspend decisions observe a single, consistent notion of "last read".
    fn signal_read(&self) {
        self.on_read.notify_one();
        *self.last_read.lock().unwrap() = Instant::now();
    }

    fn bump_freshness(&self) {
        self.freshness_tx.send_modify(|f| {
            f.stale = false;
            f.refresh_count = f.refresh_count.wrapping_add(1);
        });
    }

    async fn wait_for_fresh_data(&self) -> anyhow::Result<()> {
        let snapshot = *self.freshness_tx.borrow();
        if !snapshot.stale {
            return Ok(());
        }

        let mut rx = self.freshness_tx.subscribe();
        let _ = rx.borrow_and_update();

        let result = tokio::time::timeout(self.cold_start_timeout, async {
            while rx.borrow().refresh_count == snapshot.refresh_count {
                if rx.changed().await.is_err() {
                    return Err(anyhow::anyhow!("producer disconnected"));
                }
            }
            Ok(())
        })
        .await;

        match result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(anyhow::anyhow!(
                "cold-start read timed out after {:?}",
                self.cold_start_timeout
            )),
        }
    }
}

impl Default for PendingDataCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use pathfinder_common::{BlockHeader, BlockNumber};
    use tokio::time::timeout;

    use super::PendingDataCache;
    use crate::data::PendingData;

    fn sample_data(number: u64) -> PendingData {
        PendingData::empty(&BlockHeader {
            number: BlockNumber::new_or_panic(number),
            ..Default::default()
        })
    }

    #[tokio::test]
    async fn read_returns_default_when_unwritten() {
        let cache = PendingDataCache::new();
        let data = cache.read().await.unwrap();
        assert_eq!(data, PendingData::default());
    }

    #[tokio::test]
    async fn read_returns_stored_data() {
        let cache = PendingDataCache::new();
        let expected = sample_data(42);
        cache.store(expected.clone());
        assert_eq!(cache.read().await.unwrap(), expected);
    }

    #[tokio::test]
    async fn store_notifies_subscribers() {
        let cache = PendingDataCache::new();
        let mut rx = cache.subscribe();
        let _ = rx.borrow_and_update();

        cache.store(sample_data(7));

        assert!(timeout(Duration::from_millis(50), rx.changed())
            .await
            .expect("subscriber should observe the change")
            .is_ok());
        assert_eq!(rx.borrow().pre_confirmed_block_number().get(), 8);
    }

    #[tokio::test]
    async fn refresh_does_not_notify_subscribers() {
        let cache = PendingDataCache::new();
        let mut rx = cache.subscribe();
        let _ = rx.borrow_and_update();

        cache.refresh();

        assert!(timeout(Duration::from_millis(50), rx.changed())
            .await
            .is_err());
    }

    #[tokio::test]
    async fn wait_for_read_completes_when_read() {
        let cache = Arc::new(PendingDataCache::new());

        let waiter_cache = cache.clone();
        let waiter = tokio::spawn(async move { waiter_cache.wait_for_read().await });

        let _ = cache.read().await.unwrap();

        timeout(Duration::from_millis(100), waiter)
            .await
            .expect("wait_for_read should complete")
            .unwrap();
    }

    #[tokio::test]
    async fn idle_cache_blocks_read_until_store() {
        let cache =
            Arc::new(PendingDataCache::new().with_cold_start_timeout(Duration::from_secs(5)));
        cache.mark_stale();

        let reader_cache = cache.clone();
        let reader = tokio::spawn(async move { reader_cache.read().await });

        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(!reader.is_finished(), "reader should still be blocked");

        let expected = sample_data(11);
        cache.store(expected.clone());

        let got = timeout(Duration::from_millis(100), reader)
            .await
            .expect("reader should unblock")
            .unwrap()
            .unwrap();
        assert_eq!(got, expected);
    }

    #[tokio::test]
    async fn idle_cache_blocks_read_until_refresh() {
        let cache =
            Arc::new(PendingDataCache::new().with_cold_start_timeout(Duration::from_secs(5)));
        let initial = sample_data(99);
        cache.store(initial.clone());
        cache.mark_stale();

        let reader_cache = cache.clone();
        let reader = tokio::spawn(async move { reader_cache.read().await });

        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(!reader.is_finished());

        cache.refresh();

        let got = timeout(Duration::from_millis(100), reader)
            .await
            .expect("reader should unblock on refresh")
            .unwrap()
            .unwrap();
        assert_eq!(got, initial);
    }

    #[tokio::test]
    async fn concurrent_cold_reads_share_one_refresh() {
        let cache =
            Arc::new(PendingDataCache::new().with_cold_start_timeout(Duration::from_secs(5)));
        cache.mark_stale();

        let readers: Vec<_> = (0..10)
            .map(|_| {
                let c = cache.clone();
                tokio::spawn(async move { c.read().await })
            })
            .collect();

        tokio::time::sleep(Duration::from_millis(30)).await;
        let expected = sample_data(123);
        cache.store(expected.clone());

        for r in readers {
            let got = timeout(Duration::from_millis(200), r)
                .await
                .expect("reader should unblock")
                .unwrap()
                .unwrap();
            assert_eq!(got, expected);
        }
    }

    #[tokio::test]
    async fn cold_read_times_out_when_no_refresh() {
        let cache = PendingDataCache::new().with_cold_start_timeout(Duration::from_millis(40));
        cache.mark_stale();

        let result = cache.read().await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("timed out"));
    }

    #[tokio::test]
    async fn store_after_idle_clears_stale() {
        let cache = PendingDataCache::new().with_cold_start_timeout(Duration::from_millis(50));
        cache.mark_stale();
        cache.store(sample_data(1));

        timeout(Duration::from_millis(20), cache.read())
            .await
            .expect("read should be immediate once fresh")
            .unwrap();
    }

    #[tokio::test]
    async fn refresh_after_idle_clears_stale() {
        let cache = PendingDataCache::new().with_cold_start_timeout(Duration::from_millis(50));
        cache.mark_stale();
        cache.refresh();

        timeout(Duration::from_millis(20), cache.read())
            .await
            .expect("read should be immediate once fresh")
            .unwrap();
    }

    #[tokio::test]
    async fn repeated_mark_stale_is_idempotent() {
        let cache =
            Arc::new(PendingDataCache::new().with_cold_start_timeout(Duration::from_secs(5)));
        cache.mark_stale();
        cache.mark_stale();
        cache.mark_stale();

        let reader_cache = cache.clone();
        let reader = tokio::spawn(async move { reader_cache.read().await });

        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(!reader.is_finished());

        cache.refresh();
        timeout(Duration::from_millis(100), reader)
            .await
            .expect("reader should unblock")
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn active_reads_return_immediately() {
        let cache = PendingDataCache::new();
        cache.store(sample_data(10));
        let expected = sample_data(20);
        cache.store(expected.clone());

        let got = timeout(Duration::from_millis(10), cache.read())
            .await
            .expect("read should be immediate")
            .unwrap();
        assert_eq!(got, expected);
    }

    #[tokio::test]
    async fn try_read_returns_none_when_stale() {
        let cache = PendingDataCache::new();
        cache.mark_stale();
        assert!(cache.try_read().is_none());
    }

    #[tokio::test]
    async fn try_read_returns_data_when_fresh_and_fires_on_read() {
        let cache = Arc::new(PendingDataCache::new());
        let expected = sample_data(33);
        cache.store(expected.clone());

        let waiter_cache = cache.clone();
        let waiter = tokio::spawn(async move { waiter_cache.wait_for_read().await });

        assert_eq!(cache.try_read(), Some(expected));

        timeout(Duration::from_millis(100), waiter)
            .await
            .expect("wait_for_read should complete after try_read")
            .unwrap();
    }

    #[tokio::test]
    async fn subscriber_count_tracks_live_receivers() {
        let cache = PendingDataCache::new();
        assert_eq!(cache.subscriber_count(), 0);

        let rx1 = cache.subscribe();
        assert_eq!(cache.subscriber_count(), 1);

        let rx2 = cache.subscribe();
        assert_eq!(cache.subscriber_count(), 2);

        drop(rx1);
        assert_eq!(cache.subscriber_count(), 1);

        drop(rx2);
        assert_eq!(cache.subscriber_count(), 0);
    }

    async fn is_idle_after_inactivity_window() {
        let cache = PendingDataCache::new().with_inactivity_timeout(Duration::from_secs(1));

        // Fresh cache is not yet idle.
        assert!(!cache.is_idle());

        // Becomes idle once the inactivity window elapses with no reads.
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(cache.is_idle());

        // `try_read` resets the window.
        assert!(cache.try_read().is_some());
        assert!(!cache.is_idle());

        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(cache.is_idle());

        // `subscribe` resets the window.
        let _rx = cache.subscribe();
        assert!(!cache.is_idle());

        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(cache.is_idle());

        // `read` resets the window.
        let _ = cache.read().await.unwrap();
        assert!(!cache.is_idle());
    }

    #[tokio::test]
    async fn subscribe_wakes_wait_for_read() {
        let cache = Arc::new(PendingDataCache::new());

        let waiter_cache = cache.clone();
        let waiter = tokio::spawn(async move { waiter_cache.wait_for_read().await });

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiter.is_finished(), "producer should still be suspended");

        let _rx = cache.subscribe();

        timeout(Duration::from_millis(100), waiter)
            .await
            .expect("subscribe() should wake wait_for_read()")
            .unwrap();
    }
}
