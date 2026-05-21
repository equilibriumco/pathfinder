use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{watch, Notify};

use crate::data::PendingData;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Freshness {
    stale: bool,
    refresh_count: u64,
}

/// Shared pre-confirmed data cache with freshness tracking.
///
/// Holds the latest [`PendingData`] and a small amount of freshness state.
/// - Writes via [`Self::store`] / [`Self::refresh`] / [`Self::mark_idle`].
/// - Reads via [`Self::read`] (blocks if stale, then returns fresh data) and
///   [`Self::subscribe`] for streaming.
pub struct PreConfirmedCache {
    data_tx: watch::Sender<PendingData>,
    data_rx: watch::Receiver<PendingData>,
    on_read: Arc<Notify>,
    freshness_tx: watch::Sender<Freshness>,
    cold_start_timeout: Duration,
}

impl PreConfirmedCache {
    /// Default maximum time a cold-start read will block before erroring.
    pub const DEFAULT_COLD_START_TIMEOUT: Duration = Duration::from_secs(5);

    pub fn new() -> Self {
        Self::with_cold_start_timeout(Self::DEFAULT_COLD_START_TIMEOUT)
    }

    pub fn with_cold_start_timeout(cold_start_timeout: Duration) -> Self {
        let (data_tx, data_rx) = watch::channel(PendingData::default());
        let (freshness_tx, _) = watch::channel(Freshness::default());
        Self {
            data_tx,
            data_rx,
            on_read: Arc::new(Notify::new()),
            freshness_tx,
            cold_start_timeout,
        }
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
    pub fn mark_idle(&self) {
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
        self.on_read.notify_one();
        self.wait_for_fresh_data().await?;
        Ok(self.data_rx.borrow().clone())
    }

    /// A fresh `watch::Receiver` for awaiting changes directly.
    pub fn subscribe(&self) -> watch::Receiver<PendingData> {
        self.data_rx.clone()
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

impl Default for PreConfirmedCache {
    fn default() -> Self {
        Self::new()
    }
}
