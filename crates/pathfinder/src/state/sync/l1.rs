use std::time::Duration;

#[cfg(feature = "p2p")]
use anyhow::Context;
use pathfinder_common::Chain;
#[cfg(feature = "p2p")]
use pathfinder_common::L1BlockNumber;
use pathfinder_ethereum::EthereumClient;
#[cfg(feature = "p2p")]
use pathfinder_ethereum::L1GasPriceData;
#[cfg(feature = "p2p")]
use pathfinder_gas_price::{AddSampleError, L1GasPriceProvider};
use primitive_types::H160;
use tokio::sync::mpsc;

use crate::state::sync::SyncEvent;

#[derive(Clone)]
pub struct L1SyncContext<EthereumClient> {
    pub ethereum: EthereumClient,
    /// The Ethereum chain to sync from
    pub chain: Chain,
    /// The Starknet core contract address on Ethereum
    pub core_address: H160,
    /// The interval at which to poll for updates on finalized blocks
    pub poll_interval: Duration,
}

/// Syncs L1 state update logs.
///
/// Emits [Ethereum state update](pathfinder_ethereum::EthereumStateUpdate)
/// which should be handled to update storage and respond to queries.
pub async fn sync(
    tx_event: mpsc::Sender<SyncEvent>,
    context: L1SyncContext<EthereumClient>,
) -> anyhow::Result<()> {
    let L1SyncContext {
        mut ethereum,
        chain: _,
        core_address,
        poll_interval,
    } = context;

    let tx_event = std::sync::Arc::new(tx_event);

    // Subscribe to subsequent state updates and message logs
    ethereum
        .sync_and_listen(&core_address, poll_interval, move |state_update| {
            let tx_event = tx_event.clone();
            async move {
                let _ = tx_event.send(SyncEvent::L1Update(state_update)).await;
            }
        })
        .await?;

    Ok(())
}

/// Configuration for L1 gas price synchronization.
#[cfg(feature = "p2p")]
#[derive(Debug, Clone)]
pub struct L1GasPriceSyncConfig {
    /// Number of historical blocks to fetch on startup.
    /// Default: 10
    pub startup_blocks: u64,

    /// Delay before reconnecting after a failure (seconds).
    /// Default: 15
    pub reconnect_delay_secs: u64,

    /// Maximum gap size (in blocks) to attempt inline backfill. Gaps larger
    /// than this trigger a full buffer reset instead.
    /// Default: 100
    pub max_gap_blocks: u64,
}

#[cfg(feature = "p2p")]
impl Default for L1GasPriceSyncConfig {
    fn default() -> Self {
        Self {
            startup_blocks: 10,
            reconnect_delay_secs: 15,
            max_gap_blocks: 100,
        }
    }
}

/// Syncs L1 gas prices from block headers into the provider.
///
/// Uses historical data at startup, and then subscribes to new block headers
/// and adds gas prices as they arrive.
#[cfg(feature = "p2p")]
pub async fn sync_gas_prices(
    ethereum: EthereumClient,
    provider: L1GasPriceProvider,
    config: L1GasPriceSyncConfig,
) -> anyhow::Result<()> {
    let reconnect_delay = Duration::from_secs(config.reconnect_delay_secs);

    loop {
        match sync_gas_prices_inner(&ethereum, &provider, &config).await {
            Ok(()) => {
                tracing::warn!("L1 gas price subscription ended, restarting");
            }
            Err(e) => {
                tracing::warn!(error = %e, "L1 gas price sync failed, restarting");
            }
        }

        provider.clear();
        tokio::time::sleep(reconnect_delay).await;
    }
}

/// A self-contained sync "cycle" for gas price syncing that allows us to
/// recover from gaps and reorgs.
///
/// Bootstraps with historical data, then subscribes and processes blocks until
/// the subscription ends or an unrecoverable error occurs.
#[cfg(feature = "p2p")]
async fn sync_gas_prices_inner(
    ethereum: &EthereumClient,
    provider: &L1GasPriceProvider,
    config: &L1GasPriceSyncConfig,
) -> anyhow::Result<()> {
    // Bootstrap with historical data up to the latest block (not just finalized)
    // to minimize the gap between bootstrap and the first subscription block.
    let latest = ethereum.get_latest_block_number().await?;
    let start_block =
        L1BlockNumber::new_or_panic(latest.get().saturating_sub(config.startup_blocks).max(1));

    tracing::debug!(
        start = %start_block,
        end = %latest,
        "Fetching historical gas prices"
    );

    let historical_data = ethereum
        .get_gas_price_data_range(start_block, latest)
        .await
        .context("Fetching historical gas prices")?;

    provider
        .add_samples(historical_data)
        .context("Adding historical samples")?;

    tracing::debug!(
        samples = provider.sample_count(),
        "Historical gas prices loaded"
    );

    // Subscribe to new block headers
    let (tx, mut rx) = mpsc::channel::<L1GasPriceData>(32);

    let ethereum_for_sub = ethereum.clone();
    util::task::spawn(async move {
        if let Err(e) = ethereum_for_sub.subscribe_block_headers(tx).await {
            tracing::warn!(error = %e, "Block header subscription failed");
        }
    });

    // Process incoming blocks
    while let Some(data) = rx.recv().await {
        process_block(ethereum, provider, config, data).await?;
    }

    // Channel closed (WS died)
    Ok(())
}

/// Processes a single block from the subscription. Handles gaps via inline
/// backfill and signals reorgs as errors for the outer loop to handle.
#[cfg(feature = "p2p")]
async fn process_block(
    ethereum: &EthereumClient,
    provider: &L1GasPriceProvider,
    config: &L1GasPriceSyncConfig,
    data: L1GasPriceData,
) -> anyhow::Result<()> {
    match provider.add_sample(data) {
        Ok(()) => {
            tracing::trace!(
                block = %data.block_number,
                base_fee = data.base_fee_per_gas,
                blob_fee = data.blob_fee,
                "Added gas price sample"
            );
            Ok(())
        }
        Err(AddSampleError::Gap { expected, actual }) => {
            if actual < expected {
                anyhow::bail!("Block number went backward: expected {expected}, got {actual}");
            }

            let gap_size = actual.get() - expected.get();

            if gap_size > config.max_gap_blocks {
                anyhow::bail!(
                    "Gap too large ({gap_size} blocks, max {}), restarting",
                    config.max_gap_blocks
                );
            }

            tracing::info!(
                expected = %expected,
                actual = %actual,
                gap_size,
                "Gap detected, backfilling"
            );

            let gap_end = L1BlockNumber::new_or_panic(actual.get() - 1);
            let gap_data = ethereum
                .get_gas_price_data_range(expected, gap_end)
                .await
                .context("Backfilling gap")?;

            provider
                .add_samples(gap_data)
                .context("Adding gap-fill samples")?;

            provider
                .add_sample(data)
                .context("Adding block after gap-fill")?;

            tracing::info!(
                from = %expected,
                to = %data.block_number,
                "Gap filled successfully"
            );
            Ok(())
        }
        Err(AddSampleError::Reorg { block_number, .. }) => {
            anyhow::bail!("L1 reorg detected at block {block_number}");
        }
    }
}
