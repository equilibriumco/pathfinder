mod class;
pub mod l1;
pub mod l2;
mod pending;
pub mod repair;
pub mod revert;

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use pathfinder_block_commitments as block_hash;
use pathfinder_common::class_definition::{
    SerializedCairoDefinition,
    SerializedCasmDefinition,
    SerializedSierraDefinition,
};
use pathfinder_common::hash::{PedersenHash, PoseidonHash};
use pathfinder_common::prelude::*;
use pathfinder_common::state_update::StateUpdateData;
use pathfinder_common::{
    BlockId,
    Chain,
    ConsensusFinalizedL2Block,
    DeclaredClass,
    L2Block,
    L2BlockToCommit,
};
use pathfinder_crypto::Felt;
use pathfinder_ethereum::{EthereumClient, EthereumStateUpdate};
use pathfinder_merkle_tree::starknet_state::update_starknet_state;
use pathfinder_rpc::types::syncing::{self, NumberedBlock, Syncing};
use pathfinder_rpc::{Notifications, PendingData, Reorg, SyncState};
use pathfinder_storage::pruning::BlockchainHistoryMode;
use pathfinder_storage::{Connection, Storage, Transaction, TransactionBehavior};
use primitive_types::H160;
use starknet_gateway_client::GatewayApi;
use starknet_gateway_types::error::{KnownStarknetErrorCode, SequencerError};
use starknet_gateway_types::reply::{Block, GasPrices, PreConfirmedBlock, PreLatestBlock};
use tokio::sync::mpsc::{self, Receiver};
use tokio::sync::watch::{self, Sender as WatchSender};

use crate::state::l1::L1SyncContext;
use crate::state::l2::{BlockChain, L2SyncContext};
use crate::{ConsensusChannels, SyncMessageToConsensus};

/// Delay before restarting L1 or L2 tasks if they fail. This delay helps
/// prevent DoS if these tasks are crashing.
#[cfg(not(test))]
pub const RESET_DELAY_ON_FAILURE: std::time::Duration = std::time::Duration::from_secs(60);
#[cfg(test)]
pub const RESET_DELAY_ON_FAILURE: std::time::Duration = std::time::Duration::ZERO;

#[derive(Debug)]
pub enum SyncEvent {
    L1Update(EthereumStateUpdate),
    /// New L2 [block update](StateUpdate) found on gateway.
    DownloadedBlock(
        (
            Box<Block>,
            (TransactionCommitment, EventCommitment, ReceiptCommitment),
        ),
        Box<StateUpdate>,
        Box<BlockCommitmentSignature>,
        Box<StateDiffCommitment>,
        l2::Timings,
    ),
    /// A new L2 finalized block received from consensus. The consumer task is
    /// responsible for updating the state tries, computing the state
    /// commitment, and finally, the block hash.
    FinalizedConsensusBlock {
        /// L2 block finalized and decided upon by consensus.
        l2_block: Box<ConsensusFinalizedL2Block>,
        /// A oneshot channel to notify when the state tries update is done,
        /// returning the computed block hash and state commitment, which is
        /// necessary for the download block logic to continue its work.
        state_tries_updated_tx: tokio::sync::oneshot::Sender<(BlockHash, StateCommitment)>,
    },
    /// An L2 reorg was detected, contains the reorg-tail which
    /// indicates the oldest block which is now invalid
    /// i.e. reorg-tail - 1 should be the new head.
    Reorg(BlockNumber),
    /// A new unique L2 Cairo 0.x class was found.
    CairoClass {
        definition: SerializedCairoDefinition,
        hash: ClassHash,
    },
    /// A new unique L2 Cairo 1.x class was found.
    SierraClass {
        sierra_definition: SerializedSierraDefinition,
        sierra_hash: SierraHash,
        casm_definition: SerializedCasmDefinition,
        casm_hash: CasmHash,
        casm_hash_v2: CasmHash,
    },
    /// A new L2 pre-confirmed update was polled. Optionally contains
    /// [pre latest](PreLatestBlock) data.
    PreConfirmed {
        number: BlockNumber,
        block: Box<PreConfirmedBlock>,
        pre_latest_data: Option<Box<(BlockNumber, PreLatestBlock, StateUpdate)>>,
    },
}

pub struct SyncContext<G, E> {
    pub storage: Storage,
    pub ethereum: E,
    pub chain: Chain,
    pub chain_id: ChainId,
    pub core_address: H160,
    pub sequencer: G,
    pub state: Arc<SyncState>,
    pub head_poll_interval: Duration,
    pub l1_poll_interval: Duration,
    pub pending_data: WatchSender<PendingData>,
    pub submitted_tx_tracker: pathfinder_rpc::tracker::SubmittedTransactionTracker,
    pub block_validation_mode: l2::BlockValidationMode,
    pub notifications: Notifications,
    pub block_cache_size: usize,
    pub restart_delay: Duration,
    pub verify_tree_hashes: bool,
    pub sequencer_public_key: PublicKey,
    pub fetch_concurrency: std::num::NonZeroUsize,
    pub fetch_casm_from_fgw: bool,
    pub compiler_resource_limits: pathfinder_compiler::ResourceLimits,
    pub blockifier_libfuncs: pathfinder_compiler::BlockifierLibfuncs,
}

impl<G, E> From<&SyncContext<G, E>> for L1SyncContext<E>
where
    E: Clone,
{
    fn from(value: &SyncContext<G, E>) -> Self {
        Self {
            ethereum: value.ethereum.clone(),
            chain: value.chain,
            core_address: value.core_address,
            poll_interval: value.l1_poll_interval,
        }
    }
}

impl<G, E> From<&SyncContext<G, E>> for L2SyncContext<G>
where
    G: Clone,
{
    fn from(value: &SyncContext<G, E>) -> Self {
        Self {
            sequencer: value.sequencer.clone(),
            chain: value.chain,
            chain_id: value.chain_id,
            block_validation_mode: value.block_validation_mode,
            storage: value.storage.clone(),
            sequencer_public_key: value.sequencer_public_key,
            compiler_resource_limits: value.compiler_resource_limits,
            blockifier_libfuncs: value.blockifier_libfuncs,
            fetch_concurrency: value.fetch_concurrency,
            fetch_casm_from_fgw: value.fetch_casm_from_fgw,
        }
    }
}

/// Implements the main sync loop, where L1 and L2 sync results are combined.
pub async fn sync<SequencerClient, F1, F2, L1Sync, L2Sync>(
    context: SyncContext<SequencerClient, EthereumClient>,
    mut l1_sync: L1Sync,
    l2_sync: L2Sync,
) -> anyhow::Result<()>
where
    SequencerClient: GatewayApi + Clone + Send + Sync + 'static,
    F1: Future<Output = anyhow::Result<()>> + Send + 'static,
    F2: Future<Output = anyhow::Result<()>> + Send + 'static,
    L1Sync: FnMut(mpsc::Sender<SyncEvent>, L1SyncContext<EthereumClient>) -> F1,
    L2Sync: FnOnce(
            mpsc::Sender<SyncEvent>,
            L2SyncContext<SequencerClient>,
            Option<(BlockNumber, BlockHash, StateCommitment)>,
            BlockChain,
            watch::Receiver<(BlockNumber, BlockHash)>,
        ) -> F2
        + Copy,
{
    let l1_context = L1SyncContext::from(&context);
    let l2_context = L2SyncContext::from(&context);

    let SyncContext {
        storage,
        ethereum: _,
        chain: _,
        chain_id: _,
        core_address: _,
        sequencer,
        state,
        head_poll_interval,
        l1_poll_interval: _,
        pending_data,
        submitted_tx_tracker,
        block_validation_mode: _,
        notifications,
        block_cache_size,
        restart_delay,
        verify_tree_hashes: _,
        sequencer_public_key: _,
        compiler_resource_limits: _,
        blockifier_libfuncs: _,
        fetch_concurrency: _,
        fetch_casm_from_fgw: _,
    } = context;

    let mut db_conn = storage
        .connection()
        .context("Creating database connection")?;

    let (event_sender, event_receiver) = mpsc::channel(8);

    // Get the latest block from the database
    let l2_head = tokio::task::block_in_place(|| -> anyhow::Result<_> {
        let tx = db_conn.transaction()?;
        let l2_head = tx
            .block_header(BlockId::Latest)
            .context("Fetching latest block header from database")?
            .map(|header| (header.number, header.hash, header.state_commitment));

        Ok(l2_head)
    })?;

    // Get the latest block from the sequencer
    let gateway_latest = sequencer
        .head()
        .await
        .context("Fetching latest block from gateway")?;

    // Keep polling the sequencer for the latest block
    let (tx_latest, rx_latest) = tokio::sync::watch::channel(gateway_latest);
    let mut latest_handle = util::task::spawn(l2::poll_latest(
        sequencer.clone(),
        head_poll_interval,
        tx_latest,
    ));

    // Start update sync-status process.
    let (starting_block_num, starting_block_hash, _) = l2_head.unwrap_or((
        // start from genesis if storage is empty
        BlockNumber::GENESIS,
        BlockHash(Felt::ZERO),
        StateCommitment(Felt::ZERO),
    ));

    let _status_sync = util::task::spawn(update_sync_status_latest(
        Arc::clone(&state),
        starting_block_hash,
        starting_block_num,
        rx_latest.clone(),
    ));

    // Start L1 producer task. Clone the event sender so that the channel remains
    // open even if the producer task fails.
    let mut l1_handle = util::task::spawn(l1_sync(event_sender.clone(), l1_context.clone()));

    // Fetch latest blocks from storage
    let latest_blocks = latest_n_blocks(&mut db_conn, block_cache_size)
        .await
        .context("Fetching latest blocks from storage")?;
    let block_chain = BlockChain::with_capacity(block_cache_size, latest_blocks);

    // Start L2 producer task. Clone the event sender so that the channel remains
    // open even if the producer task fails.
    let mut l2_handle = util::task::spawn(l2_sync(
        event_sender.clone(),
        l2_context.clone(),
        l2_head,
        block_chain,
        rx_latest.clone(),
    ));

    let (current_num, current_hash, _) = l2_head.unwrap_or_default();
    let (tx_current, rx_current) = tokio::sync::watch::channel((current_num, current_hash));
    let consumer_context = ConsumerContext {
        storage: storage.clone(),
        state,
        submitted_tx_tracker,
        pending_data,
        verify_tree_hashes: context.verify_tree_hashes,
        notifications,
        sync_to_consensus_tx: None,
    };
    let mut consumer_handle =
        util::task::spawn(consumer(event_receiver, consumer_context, tx_current));

    let mut pending_handle = util::task::spawn(pending::poll_pre_confirmed(
        event_sender.clone(),
        sequencer.clone(),
        head_poll_interval,
        rx_latest.clone(),
        rx_current.clone(),
    ));

    loop {
        tokio::select! {
            _ = &mut pending_handle => {
                tracing::error!("Pre-confirmed tracking task ended unexpectedly");

                pending_handle = util::task::spawn(pending::poll_pre_confirmed(
                    event_sender.clone(),
                    sequencer.clone(),
                    Duration::from_secs(2),
                    rx_latest.clone(),
                    rx_current.clone(),
                ));
            },
            _ = &mut latest_handle => {
                tracing::error!("Tracking chain tip task ended unexpectedly");
                tracing::debug!("Shutting down other tasks");

                l1_handle.abort();
                l2_handle.abort();
                consumer_handle.abort();
                pending_handle.abort();

                _ = l1_handle.await;
                _ = l2_handle.await;
                _ = consumer_handle.await;
                _ = pending_handle.await;

                anyhow::bail!("Sync process terminated");
            },
            l1_producer_result = &mut l1_handle => {
                match l1_producer_result.context("Join L1 sync process handle")? {
                    Ok(()) => {
                        tracing::error!("L1 sync process terminated without an error.");
                    }
                    Err(e) => {
                        tracing::warn!("L1 sync process terminated with: {e:?}");
                    }
                }

                let fut = l1_sync(event_sender.clone(), l1_context.clone());
                l1_handle = util::task::spawn(async move {
                    tokio::time::sleep(RESET_DELAY_ON_FAILURE).await;
                    fut.await
                });
            },
            l2_producer_result = &mut l2_handle => {
                // L2 sync process failed; restart it.
                match l2_producer_result.context("Join L2 sync process handle")? {
                    Ok(()) => {
                        tracing::error!("L2 sync process terminated without an error.");
                    }
                    Err(e) => {
                        tracing::warn!("L2 sync process terminated with: {e:?}");
                    }
                }

                let l2_head = tokio::task::block_in_place(|| {
                    let tx = db_conn.transaction()?;
                    tx.block_header(BlockId::Latest)
                })
                .context("Query L2 head from database")?
                .map(|block| (block.number, block.hash, block.state_commitment));

                let latest_blocks = latest_n_blocks(&mut db_conn, block_cache_size).await.context("Fetching latest blocks from storage")?;
                let block_chain = BlockChain::with_capacity(1_000, latest_blocks);
                let fut = l2_sync(event_sender.clone(), l2_context.clone(), l2_head, block_chain, rx_latest.clone());

                l2_handle = util::task::spawn(async move {
                    tokio::time::sleep(restart_delay).await;
                    fut.await
                });
                tracing::info!("L2 sync process restarted.");
            },
            consumer_result = &mut consumer_handle => {
                match consumer_result {
                    Ok(Ok(())) => {
                        tracing::debug!("Sync consumer task exited gracefully");
                    },
                    Ok(Err(e)) => {
                        tracing::error!(reason=?e, "Sync consumer task terminated with an error");
                    }
                    Err(e) if e.is_cancelled() => {
                        tracing::debug!("Sync consumer task cancelled successfully");
                    },
                    Err(panic) => {
                        tracing::error!(%panic, "Sync consumer task panic'd");
                    }
                }

                // Shutdown the other processes.
                tracing::debug!("Shutting down L1 and L2 sync producer tasks");
                l1_handle.abort();
                l2_handle.abort();
                pending_handle.abort();
                latest_handle.abort();

                match l1_handle.await {
                    Ok(Ok(())) => {
                        tracing::debug!("L1 sync task exited gracefully");
                    },
                    Ok(Err(e)) => {
                        tracing::error!(reason=?e, "L1 sync task terminated with an error");
                    }
                    Err(e) if e.is_cancelled() => {
                        tracing::debug!("L1 sync task cancelled successfully");
                    },
                    Err(panic) => {
                        tracing::error!(%panic, "L1 sync task panic'd");
                    }
                }

                match l2_handle.await {
                    Ok(Ok(())) => {
                        tracing::debug!("L2 sync task exited gracefully");
                    },
                    Ok(Err(e)) => {
                        tracing::error!(reason=?e, "L2 sync task terminated with an error");
                    }
                    Err(e) if e.is_cancelled() => {
                        tracing::debug!("L2 sync task cancelled successfully");
                    },
                    Err(panic) => {
                        tracing::error!(%panic, "L2 sync task panic'd");
                    }
                }

                match latest_handle.await {
                    Ok(()) => {
                        tracing::debug!("Latest polling task exited gracefully");
                    },
                    Err(e) if e.is_cancelled() => {
                        tracing::debug!("Latest polling task cancelled successfully");
                    },
                    Err(panic) => {
                        tracing::error!(%panic, "Latest polling task panic'd");
                    }
                }

                _ = pending_handle.await;

                anyhow::bail!("Sync process terminated");
            }
        }
    }
}

/// Implements the main sync loop (like [sync]), where L1 and
/// **consensus-aware** L2 sync results are combined.
///
/// This function is also stripped of the sync status updater and pending block
/// poller, since this is a PoC for consensus integration and those features
/// are not needed here.
pub async fn consensus_sync<SequencerClient, F1, F2, L1Sync, L2Sync>(
    context: SyncContext<SequencerClient, EthereumClient>,
    mut l1_sync: L1Sync,
    l2_sync: L2Sync,
    consensus_channels: ConsensusChannels,
) -> anyhow::Result<()>
where
    SequencerClient: GatewayApi + Clone + Send + Sync + 'static,
    F1: Future<Output = anyhow::Result<()>> + Send + 'static,
    F2: Future<Output = anyhow::Result<()>> + Send + 'static,
    L1Sync: FnMut(mpsc::Sender<SyncEvent>, L1SyncContext<EthereumClient>) -> F1,
    L2Sync: FnOnce(
            mpsc::Sender<SyncEvent>,
            Option<ConsensusChannels>,
            L2SyncContext<SequencerClient>,
            Option<(BlockNumber, BlockHash, StateCommitment)>,
            BlockChain,
            watch::Receiver<(BlockNumber, BlockHash)>,
        ) -> F2
        + Copy,
{
    let l1_context = L1SyncContext::from(&context);
    let l2_context = L2SyncContext::from(&context);

    let SyncContext {
        storage,
        ethereum: _,
        chain: _,
        chain_id: _,
        core_address: _,
        sequencer,
        state,
        head_poll_interval,
        l1_poll_interval: _,
        pending_data,
        submitted_tx_tracker,
        block_validation_mode: _,
        notifications,
        block_cache_size,
        restart_delay,
        verify_tree_hashes: _,
        sequencer_public_key: _,
        compiler_resource_limits: _,
        blockifier_libfuncs: _,
        fetch_concurrency: _,
        fetch_casm_from_fgw: _,
    } = context;

    let mut db_conn = storage
        .connection()
        .context("Creating database connection")?;

    let (event_sender, event_receiver) = mpsc::channel(8);

    // Get the latest block from the database
    let l2_head = tokio::task::block_in_place(|| -> anyhow::Result<_> {
        let tx = db_conn.transaction()?;
        let l2_head = tx
            .block_header(BlockId::Latest)
            .context("Fetching latest block header from database")?
            .map(|header| (header.number, header.hash, header.state_commitment));

        Ok(l2_head)
    })?;

    // (Jan 2026) Although this will not happen on mainnet, nor on testnet, we can
    // imagine custom networks (in particular ad-hoc integration test networks)
    // which start from genesis, where the genesis block is decided upon in
    // consensus and it will not not be available at a feeder gateway until >=3
    // network participants actually decide upon the genesis block.
    let gateway_latest = match sequencer.head().await {
        Ok(gateway_latest) => gateway_latest,
        Err(SequencerError::StarknetError(e))
            if e.code == KnownStarknetErrorCode::BlockNotFound.into() =>
        {
            // Use some invalid initial values, the reason is that the API is common for
            // production sync and we don't want to introduce an Option-based runtime check
            // that could fail.
            (BlockNumber::GENESIS, BlockHash::ZERO)
        }
        // head() retries on non starknet errors so any other starknet error code indicates
        // a problem with the feeder gateway
        Err(error) => {
            tracing::error!(%error, "Error fetching latest block from gateway");
            Err(error).context("Fetching latest block from gateway")?
        }
    };

    // Keep polling the sequencer for the latest block
    let (tx_latest, rx_latest) = tokio::sync::watch::channel(gateway_latest);
    let mut latest_handle = util::task::spawn(l2::poll_latest(
        sequencer.clone(),
        head_poll_interval,
        tx_latest,
    ));

    // Start L1 producer task. Clone the event sender so that the channel remains
    // open even if the producer task fails.
    let mut l1_handle = util::task::spawn(l1_sync(event_sender.clone(), l1_context.clone()));

    // Fetch latest blocks from storage
    let latest_blocks = latest_n_blocks(&mut db_conn, block_cache_size)
        .await
        .context("Fetching latest blocks from storage")?;
    let block_chain = BlockChain::with_capacity(block_cache_size, latest_blocks);

    let sync_to_consensus_tx = consensus_channels.sync_to_consensus_tx.clone();

    // Start L2 producer task. Clone the event sender so that the channel remains
    // open even if the producer task fails.
    let mut l2_handle = util::task::spawn(l2_sync(
        event_sender.clone(),
        Some(consensus_channels.clone()),
        l2_context.clone(),
        l2_head,
        block_chain,
        rx_latest.clone(),
    ));

    let (current_num, current_hash, _) = l2_head.unwrap_or_default();
    let (tx_current, _rx_current) = tokio::sync::watch::channel((current_num, current_hash));
    let consumer_context = ConsumerContext {
        storage: storage.clone(),
        state,
        submitted_tx_tracker,
        pending_data,
        verify_tree_hashes: context.verify_tree_hashes,
        notifications,
        sync_to_consensus_tx: Some(sync_to_consensus_tx.clone()),
    };
    let mut consumer_handle =
        util::task::spawn(consumer(event_receiver, consumer_context, tx_current));

    loop {
        tokio::select! {
            _ = &mut latest_handle => {
                tracing::error!("Tracking chain tip task ended unexpectedly");
                tracing::debug!("Shutting down other tasks");

                l1_handle.abort();
                l2_handle.abort();
                consumer_handle.abort();

                _ = l1_handle.await;
                _ = l2_handle.await;
                _ = consumer_handle.await;

                anyhow::bail!("Sync process terminated");
            },
            l1_producer_result = &mut l1_handle => {
                match l1_producer_result.context("Join L1 sync process handle")? {
                    Ok(()) => {
                        tracing::error!("L1 sync process terminated without an error.");
                    }
                    Err(e) => {
                        tracing::warn!("L1 sync process terminated with: {e:?}");
                    }
                }

                let fut = l1_sync(event_sender.clone(), l1_context.clone());
                l1_handle = util::task::spawn(async move {
                    tokio::time::sleep(RESET_DELAY_ON_FAILURE).await;
                    fut.await
                });
            },
            l2_producer_result = &mut l2_handle => {
                // L2 sync process failed; restart it.
                match l2_producer_result.context("Join L2 sync process handle")? {
                    Ok(()) => {
                        tracing::error!("L2 sync process terminated without an error.");
                    }
                    Err(e) => {
                        tracing::warn!("L2 sync process terminated with: {e:?}");
                    }
                }

                let l2_head = tokio::task::block_in_place(|| {
                    let tx = db_conn.transaction()?;
                    tx.block_header(BlockId::Latest)
                })
                .context("Query L2 head from database")?
                .map(|block| (block.number, block.hash, block.state_commitment));

                let latest_blocks = latest_n_blocks(&mut db_conn, block_cache_size)
                    .await
                    .context("Fetching latest blocks from storage")?;
                let block_chain = BlockChain::with_capacity(1_000, latest_blocks);
                let fut = l2_sync(
                    event_sender.clone(),
                    Some(consensus_channels.clone()),
                    l2_context.clone(),
                    l2_head,
                    block_chain,
                    rx_latest.clone()
                );

                l2_handle = util::task::spawn(async move {
                    tokio::time::sleep(restart_delay).await;
                    fut.await
                });
                tracing::info!("L2 sync process restarted.");
            },
            consumer_result = &mut consumer_handle => {
                match consumer_result {
                    Ok(Ok(())) => {
                        tracing::debug!("Sync consumer task exited gracefully");
                    },
                    Ok(Err(e)) => {
                        tracing::error!(reason=?e, "Sync consumer task terminated with an error");
                    }
                    Err(e) if e.is_cancelled() => {
                        tracing::debug!("Sync consumer task cancelled successfully");
                    },
                    Err(panic) => {
                        tracing::error!(%panic, "Sync consumer task panic'd");
                    }
                }

                // Shutdown the other processes.
                tracing::debug!("Shutting down L1 and L2 sync producer tasks");
                l1_handle.abort();
                l2_handle.abort();
                latest_handle.abort();

                match l1_handle.await {
                    Ok(Ok(())) => {
                        tracing::debug!("L1 sync task exited gracefully");
                    },
                    Ok(Err(e)) => {
                        tracing::error!(reason=?e, "L1 sync task terminated with an error");
                    }
                    Err(e) if e.is_cancelled() => {
                        tracing::debug!("L1 sync task cancelled successfully");
                    },
                    Err(panic) => {
                        tracing::error!(%panic, "L1 sync task panic'd");
                    }
                }

                match l2_handle.await {
                    Ok(Ok(())) => {
                        tracing::debug!("L2 sync task exited gracefully");
                    },
                    Ok(Err(e)) => {
                        tracing::error!(reason=?e, "L2 sync task terminated with an error");
                    }
                    Err(e) if e.is_cancelled() => {
                        tracing::debug!("L2 sync task cancelled successfully");
                    },
                    Err(panic) => {
                        tracing::error!(%panic, "L2 sync task panic'd");
                    }
                }

                match latest_handle.await {
                    Ok(()) => {
                        tracing::debug!("Latest polling task exited gracefully");
                    },
                    Err(e) if e.is_cancelled() => {
                        tracing::debug!("Latest polling task cancelled successfully");
                    },
                    Err(panic) => {
                        tracing::error!(%panic, "Latest polling task panic'd");
                    }
                }

                anyhow::bail!("Sync process terminated");
            }
        }
    }
}

struct ConsumerContext {
    pub storage: Storage,
    pub state: Arc<SyncState>,
    pub submitted_tx_tracker: pathfinder_rpc::tracker::SubmittedTransactionTracker,
    pub pending_data: WatchSender<PendingData>,
    pub verify_tree_hashes: bool,
    pub notifications: Notifications,
    pub sync_to_consensus_tx: Option<mpsc::Sender<SyncMessageToConsensus>>,
}

async fn consumer(
    mut events: Receiver<SyncEvent>,
    context: ConsumerContext,
    current: tokio::sync::watch::Sender<(BlockNumber, BlockHash)>,
) -> anyhow::Result<()> {
    let ConsumerContext {
        storage,
        state,
        submitted_tx_tracker,
        pending_data,
        verify_tree_hashes,
        mut notifications,
        sync_to_consensus_tx,
    } = context;

    let mut last_block_start = std::time::Instant::now();
    let mut block_time_avg = std::time::Duration::ZERO;
    const BLOCK_TIME_WEIGHT: f32 = 0.05;

    let mut db_conn = storage
        .connection()
        .context("Creating database connection")?
        .with_retry()
        .context("Enabling retries for database connection")?;

    let (mut latest_timestamp, mut next_number) = tokio::task::block_in_place(|| {
        let tx = db_conn
            .transaction()
            .context("Creating database transaction")?;
        let latest = tx
            .block_header(BlockId::Latest)
            .context("Fetching latest block header")?
            .map(|b| (b.timestamp, b.number + 1))
            .unwrap_or_default();

        anyhow::Ok(latest)
    })
    .context("Fetching latest block time")?;

    while let Some(event) = events.recv().await {
        use SyncEvent::*;

        if let DownloadedBlock((block, _), _, _, _, _) = &event {
            if block.block_number < next_number {
                tracing::debug!(block_number=%block.block_number, "Ignoring duplicate block");
                continue;
            }

            let block_number = block.block_number;
            let block_hash = block.block_hash;

            // Update sync status
            match &mut *state.status.write().await {
                Syncing::False => {}
                Syncing::Status(status) => {
                    status.current = NumberedBlock::from((block_hash, block_number));

                    metrics::gauge!("current_block").set(block_number.get() as f64);

                    if status.highest.number <= block_number {
                        status.highest = status.current;
                        metrics::gauge!("highest_block").set(block_number.get() as f64);
                    }
                }
            }
        }

        let sync_to_consensus_msg = tokio::task::block_in_place(|| {
            let tx = db_conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .context("Create database transaction")?;

            let pruning_event = PruningEvent::from_sync_event(&event);

            let (notification, sync_to_consensus_msg) = match event {
                L1Update(update) => {
                    tracing::trace!("Updating L1 sync to block {}", update.block_number);
                    l1_update(&tx, &update)?;
                    tracing::info!("L1 sync updated to block {}", update.block_number);

                    (None, None)
                }
                DownloadedBlock(
                    (block, (tx_comm, ev_comm, rc_comm)),
                    state_update,
                    signature,
                    state_diff_commitment,
                    timings,
                ) => {
                    tracing::trace!("Updating L2 state to block {}", block.block_number);
                    if block.block_number < next_number {
                        tracing::debug!(block_number=%block.block_number, "Ignoring duplicate block");
                        return anyhow::Ok(None);
                    }

                    let block_number = block.block_number;
                    let block_hash = block.block_hash;
                    let block_timestamp = block.timestamp;
                    let storage_updates: usize = state_update
                        .contract_updates
                        .iter()
                        .map(|x| x.1.storage.len())
                        .sum();
                    let update_t = std::time::Instant::now();
                    let l2_block = l2_block_from_fgw_reply(
                        block,
                        tx_comm,
                        rc_comm,
                        ev_comm,
                        *state_diff_commitment,
                        *state_update,
                    )?;
                    let l2_block = l2_update(
                        &tx,
                        l2_block.into(),
                        *signature,
                        verify_tree_hashes,
                        storage.clone(),
                    )
                    .with_context(|| format!("Update L2 state to {block_number}"))?;
                    let block_time = last_block_start.elapsed();
                    let update_t = update_t.elapsed();
                    last_block_start = std::time::Instant::now();

                    block_time_avg = block_time_avg.mul_f32(1.0 - BLOCK_TIME_WEIGHT)
                        + block_time.mul_f32(BLOCK_TIME_WEIGHT);

                    _ = current.send((block_number, block_hash));

                    let now_timestamp = time::OffsetDateTime::now_utc().unix_timestamp() as u64;
                    let latency = now_timestamp.saturating_sub(block_timestamp.get());

                    let download_time =
                        (timings.block_download + timings.class_declaration).as_secs_f64();

                    metrics::gauge!("block_download").set(download_time);
                    metrics::gauge!("block_processing").set(update_t.as_secs_f64());
                    metrics::histogram!("block_processing_duration_seconds")
                        .record(update_t.as_secs_f64());
                    metrics::gauge!("block_latency").set(latency as f64);
                    if let Some(block_time_secs) =
                        block_timestamp.get().checked_sub(latest_timestamp.get())
                    {
                        metrics::gauge!("block_time").set(block_time_secs as f64);
                    }
                    latest_timestamp = block_timestamp;
                    next_number += 1;

                    // Give a simple log under INFO level, and a more verbose log
                    // with timing information under DEBUG+ level.
                    //
                    // This should be removed if we have a configurable log level.
                    // See the docs for LevelFilter for more information.
                    match tracing::level_filters::LevelFilter::current().into_level() {
                        None => {}
                        Some(level) if level <= tracing::Level::INFO => {
                            tracing::info!("Updated Starknet state with block {}", block_number)
                        }
                        Some(_) => {
                            tracing::debug!(
                                "Updated Starknet state with block {} after {:2}s ({:2}s avg). \
                                 contracts ({:2}s), {} storage updates ({:2}s). Block downloaded \
                                 in {:2}s",
                                block_number,
                                block_time.as_secs_f32(),
                                block_time_avg.as_secs_f32(),
                                timings.class_declaration.as_secs_f32(),
                                storage_updates,
                                update_t.as_secs_f32(),
                                timings.block_download.as_secs_f32(),
                            );
                        }
                    }

                    (
                        Some(Notification::L2Block(Arc::new(l2_block))),
                        Some(SyncMessageToConsensus::ConfirmBlockCommitted {
                            number: block_number,
                        }),
                    )
                }
                FinalizedConsensusBlock {
                    l2_block,
                    state_tries_updated_tx,
                } => {
                    if l2_block.header.number < next_number {
                        tracing::debug!(
                            "Ignoring duplicate finalized block {}",
                            l2_block.header.number
                        );
                        return anyhow::Ok(None);
                    }

                    let l2_block = l2_update(
                        &tx,
                        (*l2_block).into(),
                        BlockCommitmentSignature::default(),
                        verify_tree_hashes,
                        storage.clone(),
                    )?;

                    state_tries_updated_tx
                        .send((l2_block.header.hash, l2_block.header.state_commitment))
                        .expect(
                            "Receiver was dropped, which means that the consumer task exited and \
                             all sync related tasks, including this one, will be restarted.",
                        );

                    let number = l2_block.header.number;

                    (
                        Some(Notification::L2Block(Arc::new(l2_block))),
                        Some(SyncMessageToConsensus::ConfirmBlockCommitted { number }),
                    )
                }
                Reorg(reorg_tail) => {
                    tracing::trace!("Reorg L2 state to block {}", reorg_tail);
                    let reorg = l2_reorg(&tx, reorg_tail)
                        .with_context(|| format!("Reorg L2 state to {reorg_tail:?}"))?;

                    next_number = reorg_tail;
                    submitted_tx_tracker.clear();

                    let new_head = match reorg_tail {
                        BlockNumber::GENESIS => None,
                        other => Some(other - 1),
                    };
                    match new_head {
                        Some(head) => {
                            tracing::info!("L2 reorg occurred, new L2 head is block {}", head)
                        }
                        None => tracing::info!("L2 reorg occurred, new L2 head is genesis"),
                    }

                    (Some(Notification::L2Reorg(reorg)), None)
                }
                CairoClass { definition, hash } => {
                    tracing::trace!("Inserting new Cairo class with hash: {hash}");
                    tx.insert_cairo_class_definition(hash, &definition)
                        .context("Inserting new cairo class")?;

                    tracing::debug!(%hash, "Inserted new Cairo class");

                    (None, None)
                }
                SierraClass {
                    sierra_definition,
                    sierra_hash,
                    casm_definition,
                    casm_hash,
                    casm_hash_v2,
                } => {
                    tracing::trace!("Inserting new Sierra class with hash: {sierra_hash}");
                    tx.insert_sierra_class_definition(
                        &sierra_hash,
                        &sierra_definition,
                        &casm_definition,
                        &casm_hash_v2,
                    )
                    .context("Inserting sierra class")?;

                    tracing::debug!(sierra=%sierra_hash, casm=%casm_hash, "Inserted new Sierra class");

                    (None, None)
                }
                PreConfirmed {
                    number,
                    block,
                    pre_latest_data,
                } => {
                    tracing::trace!("Updating pre-confirmed data");
                    let (latest_block_number, _) = tx
                        .block_id(BlockId::Latest)
                        .context("Fetching latest block hash")?
                        .unwrap_or_default();

                    let next_block_number = pre_latest_data
                        .as_ref()
                        .map(|pre_latest| pre_latest.0)
                        .unwrap_or(number);

                    if next_block_number == latest_block_number + 1 {
                        match PendingData::try_from_pre_confirmed_and_pre_latest(
                            block,
                            number,
                            pre_latest_data,
                        ) {
                            Ok(pending) => {
                                let pre_latest_tx_count =
                                    pending.pre_latest_transactions().map(|txs| txs.len());
                                let pre_confirmed_tx_count =
                                    pending.pre_confirmed_transactions().len();
                                pending_data.send_replace(pending);
                                tracing::debug!(block_number = %number, %pre_confirmed_tx_count, ?pre_latest_tx_count, "Updated pre-confirmed data");
                            }
                            Err(e) => {
                                tracing::info!(block_number=%number, error=%e, "Failed to validate pre-confirmed data, skipping update");
                            }
                        }
                    }

                    (None, None)
                }
            };

            if let Some(pruning_event) = pruning_event {
                perform_blockchain_pruning(pruning_event, &tx).context("Pruning database")?;
            }
            let commit_result = tx.commit().context("Committing database transaction");

            // Now that the changes have been committed to storage we can send out the
            // notification. It is important that this is only ever done _after_
            // the commit otherwise clients could potentially see inconsistent
            // state.
            if let Some(notification) = notification {
                send_notification(notification, &mut notifications);
            }

            commit_result.map(|_| sync_to_consensus_msg)
        })?;

        if let (Some(sync_to_consensus_tx), Some(sync_to_consensus_msg)) =
            (sync_to_consensus_tx.clone(), sync_to_consensus_msg)
        {
            sync_to_consensus_tx
                .send(sync_to_consensus_msg)
                .await
                .context(
                    "Sending L2 consensus finalized and decided upon block committed message to \
                     consensus",
                )?;
        }
    }

    Ok(())
}

enum PruningEvent {
    L1Checkpoint(BlockNumber),
    L2Head(BlockNumber),
}

impl PruningEvent {
    fn from_sync_event(sync_event: &SyncEvent) -> Option<Self> {
        match sync_event {
            SyncEvent::L1Update(ethereum_state_update) => {
                Some(Self::L1Checkpoint(ethereum_state_update.block_number))
            }
            SyncEvent::DownloadedBlock((block, _), _, _, _, _) => {
                Some(Self::L2Head(block.block_number))
            }
            _ => None,
        }
    }
}

/// Perform [blockchain pruning](pathfinder_storage::pruning) upon receiving a
/// new sync event. There are two scenarios of interest:
///
/// 1. The sync event is an L1 update and the L2 head is ahead of the latest L1
///    checkpoint. In this case we:
///    - Prune blocks relative to the L1 checkpoint.
/// 2. The sync event is an L2 block and the L2 head is behind of the latest L1
///    checkpoint or there are no L1 checkpoints in the database yet. In this
///    case we:
///    - Prune blocks relative to the L2 head.
///
/// In any other scenario the function exits early.
fn perform_blockchain_pruning(
    pruning_event: PruningEvent,
    tx: &Transaction<'_>,
) -> anyhow::Result<()> {
    let BlockchainHistoryMode::Prune { num_blocks_kept } = tx.blockchain_history_mode else {
        return Ok(());
    };

    let (pruning_point_block, pruning_point_suffix) = match pruning_event {
        PruningEvent::L1Checkpoint(l1_checkpoint) => {
            let Some(l2_head) = tx
                .block_number(BlockId::Latest)
                .context("Querying latest block number")?
            else {
                // Empty database.
                return Ok(());
            };
            if l1_checkpoint >= l2_head {
                // We don't prune relative to L1 update if it is ahead of (or at) L2 head.
                return Ok(());
            }

            (l1_checkpoint, "L1 checkpoint")
        }
        PruningEvent::L2Head(l2_head) => {
            if l2_head == BlockNumber::GENESIS {
                // Empty database.
                return Ok(());
            }

            if let Some(latest_l1_checkpoint) = tx
                .latest_l1_checkpoint()
                .context("Querying latest L1 checkpoint")?
            {
                if l2_head > latest_l1_checkpoint {
                    // We don't prune relative to L2 head if it is ahead of latest L1 checkpoint.
                    return Ok(());
                }
            }

            (l2_head, "L2 head")
        }
    };

    let Some(last_kept_block) = pruning_point_block.get().checked_sub(num_blocks_kept) else {
        // Not ready to prune yet.
        return Ok(());
    };
    tracing::trace!(%last_kept_block, "Running blockchain pruning relative to {pruning_point_suffix}");

    let earliest = tx
        .earliest_block_number()
        .context("Querying earliest block number")?
        .expect("Blocks should exist in database")
        .get();

    let mut blocks_covered = 0;
    let start = std::time::Instant::now();
    // For L2 relative pruning this will _usually_ be a single block. The scenario
    // in which it will be more than that is when the L2 head passes the L1
    // checkpoint and the L2 relative pruning stops, then the node shuts down so the
    // L2 head falls behind the L1 checkpoint again. For L1 relative pruning, this
    // will cover the blocks between two L1 checkpoints.
    for block in earliest..last_kept_block {
        let block = BlockNumber::new_or_panic(block);
        if tx.block_exists(block.into())? {
            tx.prune_block(block)
                .with_context(|| format!("Pruning block {block}"))?;
            blocks_covered += 1;
        }
    }
    tracing::debug!(elapsed=?start.elapsed(), %blocks_covered, %last_kept_block, "Blockchain pruning done");

    anyhow::Ok(())
}

async fn latest_n_blocks(
    connection: &mut Connection,
    n: usize,
) -> anyhow::Result<Vec<(BlockNumber, BlockHash, StateCommitment)>> {
    tokio::task::block_in_place(|| {
        let tx = connection
            .transaction()
            .context("Creating database transaction")?;

        let mut current = BlockId::Latest;
        let mut blocks = Vec::new();

        for _ in 0..n {
            let header = tx.block_header(current).context("Fetching block header")?;
            let Some(header) = header else {
                break;
            };

            blocks.push((header.number, header.hash, header.state_commitment));

            if header.number == BlockNumber::GENESIS {
                break;
            }
            current = (header.number - 1).into();
        }

        // We need to reverse the order here because we want the last `N` blocks in
        // chronological order. Our sql query gives us the last `N` blocks but
        // in reverse order (ORDER BY DESC), so we undo that here.
        blocks.reverse();

        Ok(blocks)
    })
}

/// Periodically updates sync state with the latest block height.
///
/// If feature `p2p` is enabled and node type is `proxy`
/// propagates latest head after every change or otherwise every 2 minutes.
async fn update_sync_status_latest(
    state: Arc<SyncState>,
    starting_block_hash: BlockHash,
    starting_block_num: BlockNumber,
    mut latest: watch::Receiver<(BlockNumber, BlockHash)>,
) {
    let starting = NumberedBlock::from((starting_block_hash, starting_block_num));

    let mut latest_hash = BlockHash::default();
    loop {
        let Ok((number, hash)) = latest
            .wait_for(|(_, hash)| hash != &latest_hash)
            .await
            .as_deref()
            .copied()
        else {
            break;
        };

        latest_hash = hash;
        let latest = NumberedBlock::from((hash, number));
        match &mut *state.status.write().await {
            sync_status @ Syncing::False => {
                *sync_status = Syncing::Status(syncing::Status {
                    starting,
                    current: starting,
                    highest: latest,
                });

                metrics::gauge!("current_block").set(starting.number.get() as f64);
                metrics::gauge!("highest_block").set(latest.number.get() as f64);

                tracing::debug!(
                    status=%sync_status,
                    "Updated sync status",
                );
            }
            Syncing::Status(status) => {
                if status.highest.hash != latest.hash {
                    status.highest = latest;

                    metrics::gauge!("highest_block").set(latest.number.get() as f64);

                    tracing::debug!(
                        %status,
                        "Updated sync status",
                    );
                }
            }
        }
    }

    tracing::info!("Channel closed, exiting latest poll task");
}

fn l1_update(transaction: &Transaction<'_>, update: &EthereumStateUpdate) -> anyhow::Result<()> {
    transaction
        .upsert_l1_state(update)
        .context("Insert update")?;

    let l2_hash = transaction
        .block_hash(update.block_number.into())
        .context("Fetching block hash")?;

    if let Some(l2_hash) = l2_hash {
        if l2_hash == update.block_hash {
            transaction
                .update_l1_l2_pointer(Some(update.block_number))
                .context("Updating L1-L2 pointer")?;
            tracing::info!(block=?update.block_number, "Updated L1/L2 match");
        } else {
            tracing::warn!(block_number=?update.block_number, L1=?update.block_hash, L2=?l2_hash, "L1/L2 block hash mismatch");
            if let Some(matching_block_number) = transaction.l1_l2_pointer()? {
                tracing::warn!(block_number=?matching_block_number, "Most recent L1/L2 block hash match")
            }
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn l2_update(
    transaction: &Transaction<'_>,
    block: L2BlockToCommit,
    signature: BlockCommitmentSignature,
    verify_tree_hashes: bool,
    // we need this so that we can create extra read-only transactions for
    // parallel contract state updates
    storage: Storage,
) -> anyhow::Result<L2Block> {
    let (storage_commitment, class_commitment) = update_starknet_state::<PedersenHash, PoseidonHash>(
        transaction,
        block.state_update().as_ref(),
        verify_tree_hashes,
        block.number(),
        storage,
    )
    .context("Updating Starknet state")?;

    let state_commitment = StateCommitment::calculate(
        storage_commitment,
        class_commitment,
        block.starknet_version(),
    );

    if let Some(expected_state_commitment) = block.state_commitment() {
        // Ensure that roots match.. what should we do if it doesn't? For now the whole
        // sync process ends..
        anyhow::ensure!(
            state_commitment == expected_state_commitment,
            "State commitment mismatch"
        );
    }

    let block = match block {
        L2BlockToCommit::FromConsensus(block) => {
            let parent_hash = if let Some(parent_number) = block.header.number.parent() {
                transaction
                    .block_hash(BlockId::Number(parent_number))
                    .context("Fetching parent block hash")?
                    .context("Parent block missing - logic error in storage")?
            } else {
                BlockHash::ZERO
            };
            let ConsensusFinalizedL2Block {
                header,
                state_update,
                transactions_and_receipts,
                events,
                declared_classes,
            } = block;

            declared_classes.into_iter().try_for_each(
                |DeclaredClass {
                     sierra_hash,
                     casm_hash_v2,
                     sierra_def,
                     casm_def,
                 }| {
                    // Insert classes before state update because the latter will trigger
                    // `upsert_declared_at` and insert a NULL definition
                    //
                    // TODO so far `L2Block` does not contain the class definitions, due to the flow
                    // of the FGw sync.
                    transaction.insert_sierra_class_definition(
                        &sierra_hash,
                        &sierra_def,
                        &casm_def,
                        &casm_hash_v2,
                    )
                },
            )?;

            L2Block {
                header: header.compute_hash(
                    parent_hash,
                    state_commitment,
                    block_hash::compute_final_hash,
                ),
                state_update,
                transactions_and_receipts,
                events,
            }
        }
        L2BlockToCommit::FromFgw(block) => block,
    };

    // Update L2 database. These types shouldn't be options at this level,
    // but for now the unwraps are "safe" in that these should only ever be
    // None for pending queries to the sequencer, but we aren't using those here.
    // Nonetheless, the 0 defaults for l2_gas_price do show in the
    // database (for old blocks that don't really have that price),
    // and since the feeder gateway normally returns 1 in that case,
    // that should also be the default.
    transaction
        .insert_block_header(&block.header)
        .context("Inserting block header into database")?;

    transaction
        .insert_transaction_data(
            block.header.number,
            &block.transactions_and_receipts,
            Some(&block.events),
        )
        .context("Insert transaction data into database")?;

    // Insert state updates
    transaction
        .insert_state_update_data(block.header.number, &block.state_update)
        .context("Insert state update into database")?;

    // Insert signature
    transaction
        .insert_signature(block.header.number, &signature)
        .context("Insert signature into database")?;

    // Track combined L1 and L2 state.
    let l1_l2_head = transaction.l1_l2_pointer().context("Query L1-L2 head")?;
    let expected_next = l1_l2_head
        .map(|head| head + 1)
        .unwrap_or(BlockNumber::GENESIS);

    if expected_next == block.header.number {
        if let Some(l1_state) = transaction
            .l1_state_at_number(block.header.number)
            .context("Query L1 state")?
        {
            if l1_state.block_hash == block.header.hash {
                transaction
                    .update_l1_l2_pointer(Some(block.header.number))
                    .context("Update L1-L2 head")?;
            }
        }
    }

    Ok(block)
}

fn l2_block_from_fgw_reply(
    block: Box<Block>,
    transaction_commitment: TransactionCommitment,
    receipt_commitment: ReceiptCommitment,
    event_commitment: EventCommitment,
    state_diff_commitment: StateDiffCommitment,
    state_update: StateUpdate,
) -> anyhow::Result<L2Block> {
    anyhow::ensure!(
        block.transactions.len() == block.transaction_receipts.len(),
        "Transactions and receipts mismatch. There were {} transactions and {} receipts.",
        block.transactions.len(),
        block.transaction_receipts.len()
    );

    let transaction_count = block.transactions.len();
    let event_count = block
        .transaction_receipts
        .iter()
        .map(|(_, events)| events.len())
        .sum();

    let l2_gas_price = block.l2_gas_price.unwrap_or(GasPrices {
        price_in_wei: GasPrice(1),
        price_in_fri: GasPrice(1),
    });

    let state_update: StateUpdateData = state_update.into();

    let header = BlockHeader {
        hash: block.block_hash,
        parent_hash: block.parent_block_hash,
        number: block.block_number,
        timestamp: block.timestamp,
        // Default value for cairo <0.8.2 is 0
        eth_l1_gas_price: block.l1_gas_price.price_in_wei,
        // Default value for Starknet <0.13.0 is zero
        strk_l1_gas_price: block.l1_gas_price.price_in_fri,
        // Default value for Starknet <0.13.1 is zero
        eth_l1_data_gas_price: block.l1_data_gas_price.price_in_wei,
        // Default value for Starknet <0.13.1 is zero
        strk_l1_data_gas_price: block.l1_data_gas_price.price_in_fri,
        eth_l2_gas_price: l2_gas_price.price_in_wei,
        strk_l2_gas_price: l2_gas_price.price_in_fri,
        sequencer_address: block
            .sequencer_address
            .unwrap_or(SequencerAddress(Felt::ZERO)),
        starknet_version: block.starknet_version,
        event_commitment,
        state_commitment: block.state_commitment,
        transaction_commitment,
        transaction_count,
        event_count,
        l1_da_mode: block.l1_da_mode.into(),
        receipt_commitment,
        state_diff_commitment,
        state_diff_length: state_update.state_diff_length(),
    };

    let (transactions_and_receipts, events) = block
        .transactions
        .iter()
        .cloned()
        .zip(block.transaction_receipts.iter().cloned())
        .map(|(tx, (receipt, events))| ((tx, receipt), events))
        .unzip();

    Ok(L2Block {
        header,
        state_update,
        transactions_and_receipts,
        events,
    })
}

enum Notification {
    L2Block(Arc<L2Block>),
    L2Reorg(Reorg),
}

fn send_notification(notification: Notification, notifications: &mut Notifications) {
    match notification {
        Notification::L2Block(block) => {
            notifications
                .block_headers
                .send(Arc::new(block.header.clone()))
                // Ignore errors in case nobody is listening. New listeners may subscribe in the
                // future.
                .ok();
            notifications
                .l2_blocks
                .send(block)
                // Ignore errors in case nobody is listening. New listeners may subscribe in the
                // future.
                .ok();
        }
        Notification::L2Reorg(reorg) => {
            notifications
                .reorgs
                .send(reorg.into())
                // Ignore errors in case nobody is listening. New listeners may subscribe in the
                // future.
                .ok();
        }
    }
}

fn l2_reorg(transaction: &Transaction<'_>, reorg_tail: BlockNumber) -> anyhow::Result<Reorg> {
    let orphan_head = transaction
        .block_id(BlockId::Latest)
        .context("Querying latest block number")?
        .context("Latest block number is none during reorg")?
        .0;

    let Some(reorg_tail_hash) = transaction
        .block_hash(reorg_tail.into())
        .context("Fetching first block hash")?
    else {
        anyhow::bail!(
            r"Reorg tail (block number: {reorg_tail}) does not exist (likely due to blockchain history pruning).
Blockchain history must include the reorg tail and its parent block to perform a reorg."
        );
    };

    // Roll back Merkle trie updates.
    //
    // If we're rolling back genesis then there will be no blocks left so state will
    // be empty.
    if let Some(target_block) = reorg_tail.parent() {
        let Some(target_header) = transaction
            .block_header(target_block.into())
            .context("Fetching target block header")?
        else {
            anyhow::bail!(
                r"Reorg tail parent (block number: {target_block}) does not exist (likely due to blockchain history pruning).
Blockchain history must include the reorg tail and its parent block to perform a reorg."
            );
        };
        revert::revert_starknet_state(transaction, orphan_head, target_block, target_header)?;
    }

    let orphan_head_hash = transaction
        .block_hash(orphan_head.into())
        .context("Fetching orphan head hash")?
        .expect("Orphan head hash should exist because reorg tail exists");

    // Purge each block one at a time.
    //
    // This is done 1-by-1 to allow sending the reorg'd block data
    // to websocket subscriptions while keeping a constant memory footprint.
    //
    // This is acceptable performance because reorgs are rare and need not be
    // 100% optimal. However a large reorg could cause a massive memory spike
    // which is not acceptable.
    let mut new_head = orphan_head;
    while new_head >= reorg_tail {
        transaction
            .purge_block(new_head)
            .with_context(|| format!("Purging block {new_head} from database"))?;

        // No further blocks to purge if we just purged genesis.
        if new_head == BlockNumber::GENESIS {
            break;
        }

        new_head -= 1;
    }

    transaction
        .reset_in_memory_state(new_head)
        .context("Resetting in-memory DB state after reorg")?;

    // Track combined L1 and L2 state.
    let l1_l2_head = transaction.l1_l2_pointer().context("Query L1-L2 head")?;
    if let Some(l1_l2_head) = l1_l2_head {
        if reorg_tail == BlockNumber::GENESIS {
            // If we purged genesis then unset the L1 L2 pointer as well since there
            // are now no blocks remaining.
            transaction
                .update_l1_l2_pointer(None)
                .context("Unsetting L1-L2 head")?;
        } else if l1_l2_head >= reorg_tail {
            transaction
                .update_l1_l2_pointer(Some(reorg_tail - 1))
                .context("Updating L1-L2 head")?;
        }
    }

    Ok(Reorg {
        starting_block_number: reorg_tail,
        starting_block_hash: reorg_tail_hash,
        ending_block_number: orphan_head,
        ending_block_hash: orphan_head_hash,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use pathfinder_common::class_definition::{
        SerializedCairoDefinition,
        SerializedCasmDefinition,
        SerializedOpaqueClassDefinition,
        SerializedSierraDefinition,
    };
    use pathfinder_common::event::Event;
    use pathfinder_common::felt_bytes;
    use pathfinder_common::macro_prelude::*;
    use pathfinder_common::prelude::*;
    use pathfinder_common::receipt::Receipt;
    use pathfinder_common::transaction::{
        DeclareTransactionV0V1,
        DeclareTransactionV2,
        DeployAccountTransactionV1,
        DeployTransactionV0,
        InvokeTransactionV0,
        InvokeTransactionV1,
        Transaction,
        TransactionVariant,
    };
    use pathfinder_crypto::Felt;
    use pathfinder_rpc::SyncState;
    use pathfinder_storage::StorageBuilder;
    use starknet_gateway_types::reply::{self, Block, GasPrices};

    use super::l2;
    use crate::state::sync::{consumer, ConsumerContext, SyncEvent};

    /// Generate some arbitrary block chain data from genesis onwards.
    ///
    /// Note: not very realistic data but is enough to drive tests.
    #[allow(clippy::type_complexity)]
    fn generate_block_data() -> Vec<(
        (
            Box<Block>,
            (TransactionCommitment, EventCommitment, ReceiptCommitment),
        ),
        Box<StateUpdate>,
        Box<BlockCommitmentSignature>,
        Box<StateDiffCommitment>,
        l2::Timings,
    )> {
        block_data_with_state_updates(vec![StateUpdate::default(); 5])
    }

    #[allow(clippy::type_complexity)]
    fn block_data_with_state_updates(
        state_updates: Vec<StateUpdate>,
    ) -> Vec<(
        (
            Box<Block>,
            (TransactionCommitment, EventCommitment, ReceiptCommitment),
        ),
        Box<StateUpdate>,
        Box<BlockCommitmentSignature>,
        Box<StateDiffCommitment>,
        l2::Timings,
    )> {
        let genesis_header =
            BlockHeader::builder().finalize_with_hash(block_hash_bytes!(b"genesis block hash"));
        let mut headers = vec![genesis_header];
        state_updates
            .iter()
            .skip(1)
            .enumerate()
            .for_each(|(i, state_update)| {
                let block_hash = BlockHash(
                    // Adding 1 because we skipped one block.
                    Felt::from_be_slice(format!("{} block hash", i + 1).as_bytes()).unwrap(),
                );
                let header = headers
                    .last()
                    .unwrap()
                    .child_builder()
                    .state_commitment(state_update.state_commitment)
                    .finalize_with_hash(block_hash);
                headers.push(header);
            });

        let mut data = Vec::new();
        let timings = l2::Timings::default();
        let mut parent_state_commitment = StateCommitment::ZERO;
        for (block_num, header) in headers.iter().enumerate() {
            let state_update = Box::new({
                let state_update = state_updates[block_num].clone();
                state_update.with_parent_state_commitment(parent_state_commitment)
            });

            let transactions = [
                Transaction {
                    hash: transaction_hash_bytes!(
                        &format!("declare v0 tx hash {block_num}").into_bytes()
                    ),
                    variant: TransactionVariant::DeclareV0(DeclareTransactionV0V1 {
                        class_hash: class_hash_bytes!(b"declare v0 class hash"),
                        max_fee: fee_bytes!(b"declare v0 max fee"),
                        nonce: transaction_nonce_bytes!(b"declare v0 tx nonce"),
                        sender_address: contract_address_bytes!(b"declare v0 contract address"),
                        signature: vec![
                            transaction_signature_elem_bytes!(b"declare v0 tx sig 0"),
                            transaction_signature_elem_bytes!(b"declare v0 tx sig 1"),
                        ],
                    }),
                },
                Transaction {
                    hash: transaction_hash_bytes!(
                        &format!("declare v1 tx hash {block_num}").into_bytes()
                    ),
                    variant: TransactionVariant::DeclareV1(DeclareTransactionV0V1 {
                        class_hash: class_hash_bytes!(b"declare v1 class hash"),
                        max_fee: fee_bytes!(b"declare v1 max fee"),
                        nonce: transaction_nonce_bytes!(b"declare v1 tx nonce"),
                        sender_address: contract_address_bytes!(b"declare v1 contract address"),
                        signature: vec![
                            transaction_signature_elem_bytes!(b"declare v1 tx sig 0"),
                            transaction_signature_elem_bytes!(b"declare v1 tx sig 1"),
                        ],
                    }),
                },
                Transaction {
                    hash: transaction_hash_bytes!(
                        &format!("declare v2 tx hash {block_num}").into_bytes()
                    ),
                    variant: TransactionVariant::DeclareV2(DeclareTransactionV2 {
                        class_hash: class_hash_bytes!(b"declare v2 class hash"),
                        max_fee: fee_bytes!(b"declare v2 max fee"),
                        nonce: transaction_nonce_bytes!(b"declare v2 tx nonce"),
                        sender_address: contract_address_bytes!(b"declare v2 contract address"),
                        signature: vec![
                            transaction_signature_elem_bytes!(b"declare v2 tx sig 0"),
                            transaction_signature_elem_bytes!(b"declare v2 tx sig 1"),
                        ],
                        compiled_class_hash: casm_hash_bytes!(b"declare v2 casm hash"),
                    }),
                },
                Transaction {
                    hash: transaction_hash_bytes!(
                        &format!("deploy v0 tx hash {block_num}").into_bytes()
                    ),
                    variant: TransactionVariant::DeployV0(DeployTransactionV0 {
                        contract_address: contract_address_bytes!(b"deploy contract address"),
                        contract_address_salt: contract_address_salt_bytes!(
                            b"deploy contract address salt"
                        ),
                        class_hash: class_hash_bytes!(b"deploy class hash"),
                        constructor_calldata: vec![
                            constructor_param_bytes!(b"deploy call data 0"),
                            constructor_param_bytes!(b"deploy call data 1"),
                        ],
                    }),
                },
                Transaction {
                    hash: transaction_hash_bytes!(&format!(
                        "deploy account v1 tx hash {block_num}"
                    )
                    .into_bytes()),
                    variant: TransactionVariant::DeployAccountV1(DeployAccountTransactionV1 {
                        contract_address: contract_address_bytes!(
                            b"deploy account contract address"
                        ),
                        max_fee: fee_bytes!(b"deploy account max fee"),
                        signature: vec![
                            transaction_signature_elem_bytes!(b"deploy account tx sig 0"),
                            transaction_signature_elem_bytes!(b"deploy account tx sig 1"),
                        ],
                        nonce: transaction_nonce_bytes!(b"deploy account tx nonce"),
                        contract_address_salt: contract_address_salt_bytes!(
                            b"deploy account address salt"
                        ),
                        constructor_calldata: vec![
                            call_param_bytes!(b"deploy account call data 0"),
                            call_param_bytes!(b"deploy account call data 1"),
                        ],
                        class_hash: class_hash_bytes!(b"deploy account class hash"),
                    }),
                },
                Transaction {
                    hash: transaction_hash_bytes!(
                        &format!("invoke v0 tx hash {block_num}").into_bytes()
                    ),
                    variant: TransactionVariant::InvokeV0(InvokeTransactionV0 {
                        calldata: vec![
                            call_param_bytes!(b"invoke v0 call data 0"),
                            call_param_bytes!(b"invoke v0 call data 1"),
                        ],
                        sender_address: contract_address_bytes!(b"invoke v0 contract address"),
                        entry_point_selector: entry_point_bytes!(b"invoke v0 entry point"),
                        entry_point_type: None,
                        max_fee: fee_bytes!(b"invoke v0 max fee"),
                        signature: vec![
                            transaction_signature_elem_bytes!(b"invoke v0 tx sig 0"),
                            transaction_signature_elem_bytes!(b"invoke v0 tx sig 1"),
                        ],
                    }),
                },
                Transaction {
                    hash: transaction_hash_bytes!(
                        &format!("invoke v1 tx hash {block_num}").into_bytes()
                    ),
                    variant: TransactionVariant::InvokeV1(InvokeTransactionV1 {
                        calldata: vec![
                            call_param_bytes!(b"invoke v1 call data 0"),
                            call_param_bytes!(b"invoke v1 call data 1"),
                        ],
                        sender_address: contract_address_bytes!(b"invoke v1 contract address"),
                        max_fee: fee_bytes!(b"invoke v1 max fee"),
                        signature: vec![
                            transaction_signature_elem_bytes!(b"invoke v1 tx sig 0"),
                            transaction_signature_elem_bytes!(b"invoke v1 tx sig 1"),
                        ],
                        nonce: transaction_nonce_bytes!(b"invoke v1 tx nonce"),
                    }),
                },
            ];
            // Generate a random receipt for each transaction. Note that these won't make
            // physical sense but its enough for the tests.
            let transaction_receipts: Vec<(pathfinder_common::receipt::Receipt, Vec<Event>)> =
                transactions
                    .iter()
                    .enumerate()
                    .map(|(i, t)| {
                        (
                            Receipt {
                                transaction_hash: t.hash,
                                transaction_index: TransactionIndex::new_or_panic(i as u64),
                                ..Default::default()
                            },
                            vec![],
                        )
                    })
                    .collect();
            assert_eq!(transactions.len(), transaction_receipts.len());

            let transactions = vec![
                Transaction {
                    hash: transaction_hash_bytes!(
                        &format!("declare v0 tx hash {block_num}").into_bytes()
                    ),
                    variant: TransactionVariant::DeclareV0(DeclareTransactionV0V1 {
                        class_hash: class_hash_bytes!(b"declare v0 class hash"),
                        max_fee: fee_bytes!(b"declare v0 max fee"),
                        nonce: transaction_nonce_bytes!(b"declare v0 tx nonce"),
                        sender_address: contract_address_bytes!(b"declare v0 contract address"),
                        signature: vec![
                            transaction_signature_elem_bytes!(b"declare v0 tx sig 0"),
                            transaction_signature_elem_bytes!(b"declare v0 tx sig 1"),
                        ],
                    }),
                },
                Transaction {
                    hash: transaction_hash_bytes!(
                        &format!("declare v1 tx hash {block_num}").into_bytes()
                    ),
                    variant: TransactionVariant::DeclareV1(DeclareTransactionV0V1 {
                        class_hash: class_hash_bytes!(b"declare v1 class hash"),
                        max_fee: fee_bytes!(b"declare v1 max fee"),
                        nonce: transaction_nonce_bytes!(b"declare v1 tx nonce"),
                        sender_address: contract_address_bytes!(b"declare v1 contract address"),
                        signature: vec![
                            transaction_signature_elem_bytes!(b"declare v1 tx sig 0"),
                            transaction_signature_elem_bytes!(b"declare v1 tx sig 1"),
                        ],
                    }),
                },
                Transaction {
                    hash: transaction_hash_bytes!(
                        &format!("declare v2 tx hash {block_num}").into_bytes()
                    ),
                    variant: TransactionVariant::DeclareV2(DeclareTransactionV2 {
                        class_hash: class_hash_bytes!(b"declare v2 class hash"),
                        max_fee: fee_bytes!(b"declare v2 max fee"),
                        nonce: transaction_nonce_bytes!(b"declare v2 tx nonce"),
                        sender_address: contract_address_bytes!(b"declare v2 contract address"),
                        signature: vec![
                            transaction_signature_elem_bytes!(b"declare v2 tx sig 0"),
                            transaction_signature_elem_bytes!(b"declare v2 tx sig 1"),
                        ],
                        compiled_class_hash: casm_hash_bytes!(b"declare v2 casm hash"),
                    }),
                },
                Transaction {
                    hash: transaction_hash_bytes!(
                        &format!("deploy v0 tx hash {block_num}").into_bytes()
                    ),
                    variant: TransactionVariant::DeployV0(DeployTransactionV0 {
                        contract_address: contract_address_bytes!(b"deploy contract address"),
                        contract_address_salt: contract_address_salt_bytes!(
                            b"deploy contract address salt"
                        ),
                        class_hash: class_hash_bytes!(b"deploy class hash"),
                        constructor_calldata: vec![
                            constructor_param_bytes!(b"deploy call data 0"),
                            constructor_param_bytes!(b"deploy call data 1"),
                        ],
                    }),
                },
                Transaction {
                    hash: transaction_hash_bytes!(&format!(
                        "deploy account v1 tx hash {block_num}"
                    )
                    .into_bytes()),
                    variant: TransactionVariant::DeployAccountV1(DeployAccountTransactionV1 {
                        contract_address: contract_address_bytes!(
                            b"deploy account contract address"
                        ),
                        max_fee: fee_bytes!(b"deploy account max fee"),
                        signature: vec![
                            transaction_signature_elem_bytes!(b"deploy account tx sig 0"),
                            transaction_signature_elem_bytes!(b"deploy account tx sig 1"),
                        ],
                        nonce: transaction_nonce_bytes!(b"deploy account tx nonce"),
                        contract_address_salt: contract_address_salt_bytes!(
                            b"deploy account address salt"
                        ),
                        constructor_calldata: vec![
                            call_param_bytes!(b"deploy account call data 0"),
                            call_param_bytes!(b"deploy account call data 1"),
                        ],
                        class_hash: class_hash_bytes!(b"deploy account class hash"),
                    }),
                },
                Transaction {
                    hash: transaction_hash_bytes!(
                        &format!("invoke v0 tx hash {block_num}").into_bytes()
                    ),
                    variant: TransactionVariant::InvokeV0(InvokeTransactionV0 {
                        calldata: vec![
                            call_param_bytes!(b"invoke v0 call data 0"),
                            call_param_bytes!(b"invoke v0 call data 1"),
                        ],
                        sender_address: contract_address_bytes!(b"invoke v0 contract address"),
                        entry_point_selector: entry_point_bytes!(b"invoke v0 entry point"),
                        entry_point_type: None,
                        max_fee: fee_bytes!(b"invoke v0 max fee"),
                        signature: vec![
                            transaction_signature_elem_bytes!(b"invoke v0 tx sig 0"),
                            transaction_signature_elem_bytes!(b"invoke v0 tx sig 1"),
                        ],
                    }),
                },
                Transaction {
                    hash: transaction_hash_bytes!(
                        &format!("invoke v1 tx hash {block_num}").into_bytes()
                    ),
                    variant: TransactionVariant::InvokeV1(InvokeTransactionV1 {
                        calldata: vec![
                            call_param_bytes!(b"invoke v1 call data 0"),
                            call_param_bytes!(b"invoke v1 call data 1"),
                        ],
                        sender_address: contract_address_bytes!(b"invoke v1 contract address"),
                        max_fee: fee_bytes!(b"invoke v1 max fee"),
                        signature: vec![
                            transaction_signature_elem_bytes!(b"invoke v1 tx sig 0"),
                            transaction_signature_elem_bytes!(b"invoke v1 tx sig 1"),
                        ],
                        nonce: transaction_nonce_bytes!(b"invoke v1 tx nonce"),
                    }),
                },
            ];
            // Generate a random receipt for each transaction. Note that these won't make
            // physical sense but its enough for the tests.
            let transaction_receipts: Vec<(pathfinder_common::receipt::Receipt, Vec<Event>)> =
                transactions
                    .iter()
                    .enumerate()
                    .map(|(i, t)| {
                        (
                            Receipt {
                                transaction_hash: t.hash,
                                transaction_index: TransactionIndex::new_or_panic(i as u64),
                                ..Default::default()
                            },
                            vec![],
                        )
                    })
                    .collect();
            assert_eq!(transactions.len(), transaction_receipts.len());

            let block = Box::new(reply::Block {
                block_hash: header.hash,
                block_number: header.number,
                l1_gas_price: GasPrices {
                    price_in_wei: header.eth_l1_gas_price,
                    price_in_fri: header.strk_l1_gas_price,
                },
                l1_data_gas_price: GasPrices {
                    price_in_wei: header.eth_l1_data_gas_price,
                    price_in_fri: header.strk_l1_data_gas_price,
                },
                l2_gas_price: Some(GasPrices {
                    price_in_wei: header.eth_l2_gas_price,
                    price_in_fri: header.strk_l2_gas_price,
                }),
                parent_block_hash: header.parent_hash,
                sequencer_address: Some(header.sequencer_address),
                state_commitment: header.state_commitment,
                status: reply::Status::AcceptedOnL2,
                timestamp: header.timestamp,
                transaction_receipts,
                transactions,
                starknet_version: header.starknet_version,
                l1_da_mode: Default::default(),
                transaction_commitment: header.transaction_commitment,
                event_commitment: header.event_commitment,
                receipt_commitment: Some(ReceiptCommitment(
                    Felt::from_hex_str(&format!("0x100{}", header.number)).unwrap(),
                )),
                state_diff_commitment: Some(StateDiffCommitment(
                    Felt::from_hex_str(&format!("0x200{}", header.number)).unwrap(),
                )),
                state_diff_length: Some(header.number.get()),
            });

            let signature = Box::new(BlockCommitmentSignature {
                r: BlockCommitmentSignatureElem(
                    Felt::from_hex_str(&format!("0x300{}", header.number)).unwrap(),
                ),
                s: BlockCommitmentSignatureElem(
                    Felt::from_hex_str(&format!("0x400{}", header.number)).unwrap(),
                ),
            });

            let state_diff_commitment = Box::new(block.state_diff_commitment.unwrap());

            data.push((
                (
                    block,
                    (
                        header.transaction_commitment,
                        header.event_commitment,
                        header.receipt_commitment,
                    ),
                ),
                state_update,
                signature,
                state_diff_commitment,
                timings,
            ));

            parent_state_commitment = header.state_commitment;
        }

        data
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn block_updates() {
        let storage = StorageBuilder::in_memory_with_trie_pruning_and_pool_size(
            pathfinder_storage::TriePruneMode::Archive,
            std::num::NonZeroU32::new(5).unwrap(),
        )
        .unwrap();
        let mut connection = storage.connection().unwrap();

        let (event_tx, event_rx) = tokio::sync::mpsc::channel(100);

        let block_data = generate_block_data();
        let num_blocks = block_data.len();

        // Send block updates, followed by a reorg to genesis.
        for (a, b, c, d, e) in block_data {
            event_tx
                .send(SyncEvent::DownloadedBlock(a, b, c, d, e))
                .await
                .unwrap();
        }
        // Close the event channel which allows the consumer task to exit.
        drop(event_tx);

        let (tx, _rx) = tokio::sync::watch::channel(Default::default());
        let context = ConsumerContext {
            storage,
            state: Arc::new(SyncState::default()),
            submitted_tx_tracker: pathfinder_rpc::tracker::SubmittedTransactionTracker::new(10, 10),
            pending_data: tx,
            verify_tree_hashes: false,
            notifications: Default::default(),
            sync_to_consensus_tx: None,
        };

        let (tx, _rx) = tokio::sync::watch::channel(Default::default());
        consumer(event_rx, context, tx).await.unwrap();

        let tx = connection.transaction().unwrap();
        for i in 0..num_blocks {
            // TODO: Ideally we would test data consistency as well, but that will be easier
            // once we use the same types between storage, sync and gateway.
            let should_exist = tx
                .block_exists(BlockNumber::new_or_panic(i as u64).into())
                .unwrap();
            assert!(should_exist, "Block {i} should exist");
        }

        let should_not_exist = tx
            .block_exists(BlockNumber::new_or_panic(num_blocks as u64).into())
            .unwrap();
        assert!(!should_not_exist);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn reorg() {
        let storage = StorageBuilder::in_memory_with_trie_pruning_and_pool_size(
            pathfinder_storage::TriePruneMode::Archive,
            std::num::NonZeroU32::new(5).unwrap(),
        )
        .unwrap();
        let mut connection = storage.connection().unwrap();

        let (event_tx, event_rx) = tokio::sync::mpsc::channel(100);

        // Send block updates, followed by a reorg to genesis.
        for (a, b, c, d, e) in generate_block_data() {
            event_tx
                .send(SyncEvent::DownloadedBlock(a, b, c, d, e))
                .await
                .unwrap();
        }
        event_tx
            .send(SyncEvent::Reorg(BlockNumber::new_or_panic(2)))
            .await
            .unwrap();
        // Close the event channel which allows the consumer task to exit.
        drop(event_tx);

        let (tx, _rx) = tokio::sync::watch::channel(Default::default());
        let context = ConsumerContext {
            storage,
            state: Arc::new(SyncState::default()),
            submitted_tx_tracker: pathfinder_rpc::tracker::SubmittedTransactionTracker::new(10, 10),
            pending_data: tx,
            verify_tree_hashes: false,
            notifications: Default::default(),
            sync_to_consensus_tx: None,
        };

        let (tx, _rx) = tokio::sync::watch::channel(Default::default());
        consumer(event_rx, context, tx).await.unwrap();

        let tx = connection.transaction().unwrap();
        let genesis_exists = tx.block_exists(BlockNumber::GENESIS.into()).unwrap();
        assert!(genesis_exists);

        let block_1_exists = tx
            .block_exists(BlockNumber::new_or_panic(1).into())
            .unwrap();
        assert!(block_1_exists);

        let block_2_exists = tx
            .block_exists(BlockNumber::new_or_panic(2).into())
            .unwrap();
        assert!(!block_2_exists);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn blocks_are_not_skipped_after_a_reorg() {
        // A bug caused reorg'd block numbers to be skipped. This
        // was due to the expected block number not being updated
        // when handling the reorg.
        let storage = StorageBuilder::in_memory_with_trie_pruning_and_pool_size(
            pathfinder_storage::TriePruneMode::Archive,
            std::num::NonZeroU32::new(5).unwrap(),
        )
        .unwrap();
        let mut connection = storage.connection().unwrap();

        let (event_tx, event_rx) = tokio::sync::mpsc::channel(100);

        // Send block updates, followed by a reorg removing block 2.
        // Then republish block 2, which should succeed.
        let blocks = generate_block_data();
        let block2 = blocks[2].clone();
        for (a, b, c, d, e) in blocks {
            event_tx
                .send(SyncEvent::DownloadedBlock(a, b, c, d, e))
                .await
                .unwrap();
        }
        event_tx
            .send(SyncEvent::Reorg(block2.0 .0.block_number))
            .await
            .unwrap();
        // This previously failed as the expected next block number was never
        // updated after a reorg, causing the reorg'd block numbers to be considered
        // duplicates and skipped - breaking sync.
        event_tx
            .send(SyncEvent::DownloadedBlock(
                block2.0, block2.1, block2.2, block2.3, block2.4,
            ))
            .await
            .unwrap();
        // Close the event channel which allows the consumer task to exit.
        drop(event_tx);

        let (tx, _rx) = tokio::sync::watch::channel(Default::default());
        let context = ConsumerContext {
            storage,
            state: Arc::new(SyncState::default()),
            submitted_tx_tracker: pathfinder_rpc::tracker::SubmittedTransactionTracker::new(10, 10),
            pending_data: tx,
            verify_tree_hashes: false,
            notifications: Default::default(),
            sync_to_consensus_tx: None,
        };

        let (tx, _rx) = tokio::sync::watch::channel(Default::default());
        consumer(event_rx, context, tx).await.unwrap();

        let tx = connection.transaction().unwrap();
        let genesis_exists = tx.block_exists(BlockNumber::GENESIS.into()).unwrap();
        assert!(genesis_exists);

        let block_1_exists = tx
            .block_exists(BlockNumber::new_or_panic(1).into())
            .unwrap();
        assert!(block_1_exists);
        // Block 2 should actually exist as well again.
        let block_2_exists = tx
            .block_exists(BlockNumber::new_or_panic(2).into())
            .unwrap();
        assert!(block_2_exists);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn reorg_to_genesis() {
        let storage = StorageBuilder::in_memory_with_trie_pruning_and_pool_size(
            pathfinder_storage::TriePruneMode::Archive,
            std::num::NonZeroU32::new(5).unwrap(),
        )
        .unwrap();
        let mut connection = storage.connection().unwrap();

        let (event_tx, event_rx) = tokio::sync::mpsc::channel(100);

        // Send block updates, followed by a reorg to genesis.
        for (a, b, c, d, e) in generate_block_data() {
            event_tx
                .send(SyncEvent::DownloadedBlock(a, b, c, d, e))
                .await
                .unwrap();
        }
        event_tx
            .send(SyncEvent::Reorg(BlockNumber::GENESIS))
            .await
            .unwrap();
        // Close the event channel which allows the consumer task to exit.
        drop(event_tx);

        let (tx, _rx) = tokio::sync::watch::channel(Default::default());
        let context = ConsumerContext {
            storage,
            state: Arc::new(SyncState::default()),
            submitted_tx_tracker: pathfinder_rpc::tracker::SubmittedTransactionTracker::new(10, 10),
            pending_data: tx,
            verify_tree_hashes: false,
            notifications: Default::default(),
            sync_to_consensus_tx: None,
        };

        let (tx, _rx) = tokio::sync::watch::channel(Default::default());
        consumer(event_rx, context, tx).await.unwrap();

        let tx = connection.transaction().unwrap();
        let genesis_exists = tx.block_exists(BlockNumber::GENESIS.into()).unwrap();
        assert!(!genesis_exists);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn new_cairo_contract() {
        let storage = StorageBuilder::in_memory_with_trie_pruning_and_pool_size(
            pathfinder_storage::TriePruneMode::Archive,
            std::num::NonZeroU32::new(5).unwrap(),
        )
        .unwrap();
        let mut connection = storage.connection().unwrap();

        let (event_tx, event_rx) = tokio::sync::mpsc::channel(1);

        let class_hash = class_hash_bytes!(b"class hash");
        let expected_definition = SerializedCairoDefinition::from_slice(b"cairo class definition");

        event_tx
            .send(SyncEvent::CairoClass {
                definition: expected_definition.clone(),
                hash: class_hash,
            })
            .await
            .unwrap();
        // This closes the event channel which ends the consumer task.
        drop(event_tx);
        // UUT
        let (tx, _rx) = tokio::sync::watch::channel(Default::default());
        let context = ConsumerContext {
            storage,
            state: Arc::new(SyncState::default()),
            submitted_tx_tracker: pathfinder_rpc::tracker::SubmittedTransactionTracker::new(10, 10),
            pending_data: tx,
            verify_tree_hashes: false,
            notifications: Default::default(),
            sync_to_consensus_tx: None,
        };

        let (tx, _rx) = tokio::sync::watch::channel(Default::default());
        consumer(event_rx, context, tx).await.unwrap();

        let tx = connection.transaction().unwrap();
        let definition = tx.class_definition(class_hash).unwrap().unwrap();

        assert_eq!(
            definition,
            SerializedOpaqueClassDefinition::from(expected_definition)
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn new_sierra_contract() {
        let storage = StorageBuilder::in_memory_with_trie_pruning_and_pool_size(
            pathfinder_storage::TriePruneMode::Archive,
            std::num::NonZeroU32::new(5).unwrap(),
        )
        .unwrap();
        let mut connection = storage.connection().unwrap();

        let (event_tx, event_rx) = tokio::sync::mpsc::channel(1);

        let class_hash = felt_bytes!(b"class hash");
        let expected_definition =
            SerializedSierraDefinition::from_slice(b"sierra class definition");

        event_tx
            .send(SyncEvent::SierraClass {
                sierra_definition: expected_definition.clone(),
                sierra_hash: SierraHash(class_hash),
                casm_definition: SerializedCasmDefinition::from_slice(b"casm definition"),
                casm_hash: casm_hash_bytes!(b"casm hash"),
                casm_hash_v2: casm_hash_bytes!(b"casm hash blake"),
            })
            .await
            .unwrap();
        // This closes the event channel which ends the consumer task.
        drop(event_tx);

        let (tx, _rx) = tokio::sync::watch::channel(Default::default());
        let context = ConsumerContext {
            storage,
            state: Arc::new(SyncState::default()),
            submitted_tx_tracker: pathfinder_rpc::tracker::SubmittedTransactionTracker::new(10, 10),
            pending_data: tx,
            verify_tree_hashes: false,
            notifications: Default::default(),
            sync_to_consensus_tx: None,
        };

        let (tx, _rx) = tokio::sync::watch::channel(Default::default());
        consumer(event_rx, context, tx).await.unwrap();

        let tx = connection.transaction().unwrap();
        let definition = tx.class_definition(ClassHash(class_hash)).unwrap().unwrap();

        assert_eq!(
            definition,
            SerializedOpaqueClassDefinition::from(expected_definition)
        );

        let casm_hash_v2 = tx.casm_hash_v2(ClassHash(class_hash)).unwrap().unwrap();
        assert_eq!(casm_hash_v2, casm_hash_bytes!(b"casm hash blake"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn consumer_should_ignore_duplicate_blocks() {
        let storage = StorageBuilder::in_memory_with_trie_pruning_and_pool_size(
            pathfinder_storage::TriePruneMode::Archive,
            std::num::NonZeroU32::new(5).unwrap(),
        )
        .unwrap();

        let (event_tx, event_rx) = tokio::sync::mpsc::channel(5);

        let blocks = generate_block_data();
        let (a, b, c, d, e) = blocks[0].clone();

        event_tx
            .send(SyncEvent::DownloadedBlock(
                a.clone(),
                b.clone(),
                c.clone(),
                d.clone(),
                e,
            ))
            .await
            .unwrap();
        event_tx
            .send(SyncEvent::DownloadedBlock(a.clone(), b.clone(), c, d, e))
            .await
            .unwrap();
        drop(event_tx);

        let (tx, _rx) = tokio::sync::watch::channel(Default::default());
        let context = ConsumerContext {
            storage,
            state: Arc::new(SyncState::default()),
            submitted_tx_tracker: pathfinder_rpc::tracker::SubmittedTransactionTracker::new(10, 10),
            pending_data: tx,
            verify_tree_hashes: false,
            notifications: Default::default(),
            sync_to_consensus_tx: None,
        };

        let (tx, _rx) = tokio::sync::watch::channel(Default::default());
        consumer(event_rx, context, tx).await.unwrap();
    }

    mod blockchain_pruning {
        use pathfinder_common::BlockId;
        use pathfinder_ethereum::EthereumStateUpdate;

        use super::*;

        struct ReorgRegressionData {
            state_updates: Vec<StateUpdate>,
            reorg_tail: BlockNumber,
            removed_class_hash: ClassHash,
        }

        impl ReorgRegressionData {
            fn new() -> Self {
                let contract1 = contract_address_bytes!(b"contract 1");
                let class1 = class_hash_bytes!(b"class 1");
                let class2 = class_hash_bytes!(b"class 2");
                let class3 = class_hash_bytes!(b"class 3");

                let state_updates = vec![
                    StateUpdate::default(),
                    StateUpdate::default()
                        .with_declared_cairo_class(class1)
                        .with_declared_cairo_class(class2),
                    StateUpdate::default()
                        .with_deployed_contract(contract1, class1)
                        .with_state_commitment(state_commitment!(
                            "0x049EA1B5F078CA95BEAEF0880401AE973BCB702F116E98F7F5F63ECAF1F8036B"
                        )),
                    StateUpdate::default()
                        .with_contract_nonce(contract1, contract_nonce!("0x1"))
                        .with_replaced_class(contract1, class2)
                        .with_state_commitment(state_commitment!(
                            "0x038EEAFDC5F7CC010DB030EFEBFDFB3512EE43361C0C6E326DBA0C6D118D799E"
                        )),
                    StateUpdate::default()
                        .with_declared_cairo_class(class3)
                        .with_contract_nonce(contract1, contract_nonce!("0x2"))
                        .with_state_commitment(state_commitment!(
                            "0x0292FFCDA8FB1ADEED42CD1411E3235B4F0D739D1DD0E0D7D4DBD4C2D6283565"
                        )),
                    StateUpdate::default().with_state_commitment(state_commitment!(
                        "0x0292FFCDA8FB1ADEED42CD1411E3235B4F0D739D1DD0E0D7D4DBD4C2D6283565"
                    )),
                ];

                Self {
                    state_updates,
                    reorg_tail: BlockNumber::new_or_panic(3),
                    removed_class_hash: class3,
                }
            }
        }

        fn one_non_prunable_block() -> Vec<StateUpdate> {
            let contract1 = contract_address_bytes!(b"contract 1");
            let contract2 = contract_address_bytes!(b"contract 2");
            let class1 = class_hash_bytes!(b"class 1");
            let class2 = class_hash_bytes!(b"class 2");
            let storage_address1 = storage_address_bytes!(b"storage address 1");

            vec![
                StateUpdate::default(),
                StateUpdate::default()
                    .with_declared_cairo_class(class1)
                    .with_declared_cairo_class(class2),
                StateUpdate::default()
                    .with_state_commitment(state_commitment!(
                        "0x04403E18CF4B87E95FCBF146BC32D55F679DE144C8CC9AD9E79E28AED90B690A"
                    ))
                    .with_deployed_contract(contract1, class1)
                    // Contract 2 class is deployed and never replaced.
                    .with_deployed_contract(contract2, class1),
                StateUpdate::default()
                    .with_state_commitment(state_commitment!(
                        "0x0579F6BE90F9F98020316955A70EFFD78E4317E8AA684546144D0E852F247B96"
                    ))
                    .with_storage_update(contract1, storage_address1, storage_value!("0x100"))
                    .with_contract_nonce(contract1, contract_nonce!("0x1")),
                StateUpdate::default()
                    .with_state_commitment(state_commitment!(
                        "0x01283044BBD2E60462EF0A8F593CEC6616ED43D5FF5358EB87CD824F65F1ED7C"
                    ))
                    .with_replaced_class(contract1, class2)
                    // Final storage value update.
                    .with_storage_update(contract1, storage_address1, storage_value!("0x200"))
                    // Final nonce update
                    .with_contract_nonce(contract1, contract_nonce!("0x3")),
            ]
        }

        fn one_non_prunable_block_for_each_update() -> Vec<StateUpdate> {
            let contract1 = contract_address_bytes!(b"contract 1");
            let contract2 = contract_address_bytes!(b"contract 2");
            let class1 = class_hash_bytes!(b"class 1");
            let class2 = class_hash_bytes!(b"class 2");
            let storage_address1 = storage_address_bytes!(b"storage address 1");

            vec![
                StateUpdate::default(),
                StateUpdate::default()
                    .with_declared_cairo_class(class1)
                    .with_declared_cairo_class(class2),
                StateUpdate::default()
                    .with_state_commitment(state_commitment!(
                        "0x04403E18CF4B87E95FCBF146BC32D55F679DE144C8CC9AD9E79E28AED90B690A"
                    ))
                    .with_deployed_contract(contract1, class1)
                    // Contract 2 class is deployed and never replaced.
                    .with_deployed_contract(contract2, class1),
                StateUpdate::default()
                    .with_state_commitment(state_commitment!(
                        "0x044204D6012E3A2D4597D021A22ECB494A00D4D2433422038E806EA7346A3B66"
                    ))
                    // Final class replacement.
                    .with_replaced_class(contract1, class2)
                    .with_storage_update(contract1, storage_address1, storage_value!("0x100"))
                    .with_contract_nonce(contract1, contract_nonce!("0x1")),
                StateUpdate::default()
                    .with_state_commitment(state_commitment!(
                        "0x002343E7A9AEACD3D366D27D5A61095664C894B22F1C4A8309AF9765AF566A36"
                    ))
                    // Final storage value update.
                    .with_storage_update(contract1, storage_address1, storage_value!("0x200")),
                StateUpdate::default()
                    .with_state_commitment(state_commitment!(
                        "0x02D831BF9BB2B03A2B6593A003E9826B70D42E44CE8365230187989ECD66B754"
                    ))
                    // Final nonce update.
                    .with_contract_nonce(contract1, contract_nonce!("0x2")),
                StateUpdate::default().with_state_commitment(state_commitment!(
                    "0x02D831BF9BB2B03A2B6593A003E9826B70D42E44CE8365230187989ECD66B754"
                )),
            ]
        }

        fn state_update_reconstruction() -> Vec<StateUpdate> {
            let contract1 = contract_address_bytes!(b"contract 1");
            let class1 = class_hash_bytes!(b"class 1");
            let class2 = class_hash_bytes!(b"class 2");
            let class3 = class_hash_bytes!(b"class 3");

            vec![
                StateUpdate::default(),
                StateUpdate::default()
                    .with_declared_cairo_class(class1)
                    .with_declared_cairo_class(class2)
                    .with_declared_cairo_class(class3),
                StateUpdate::default()
                    .with_state_commitment(state_commitment!(
                        "0x049EA1B5F078CA95BEAEF0880401AE973BCB702F116E98F7F5F63ECAF1F8036B"
                    ))
                    .with_deployed_contract(contract1, class1),
                StateUpdate::default()
                    .with_state_commitment(state_commitment!(
                        "0x02217B6E78883EC62771BC63BEC0C34291FFA78EDCFFDE50C4DD8FDD4FE3158E"
                    ))
                    .with_replaced_class(contract1, class2),
                StateUpdate::default()
                    .with_state_commitment(state_commitment!(
                        "0x032D16947452A6E41512E1515048675480D935C26CCC982E9888AC96EF65C189"
                    ))
                    .with_contract_nonce(contract1, contract_nonce!("0x1"))
                    .with_replaced_class(contract1, class3),
                StateUpdate::default().with_state_commitment(state_commitment!(
                    "0x032D16947452A6E41512E1515048675480D935C26CCC982E9888AC96EF65C189"
                )),
            ]
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn blockchain_history_pruning() {
            let storage = StorageBuilder::in_memory_with_blockchain_pruning_and_pool_size(
                // Keep only the latest block.
                pathfinder_storage::pruning::BlockchainHistoryMode::Prune { num_blocks_kept: 0 },
                std::num::NonZeroU32::new(10).unwrap(),
            )
            .unwrap();
            let mut conn = storage.connection().unwrap();

            let (event_tx, event_rx) = tokio::sync::mpsc::channel(10);

            let blocks = block_data_with_state_updates(one_non_prunable_block());
            let num_blocks = blocks.len() as u64;
            // Send block updates.
            for (a, b, c, d, e) in blocks {
                event_tx
                    .send(SyncEvent::DownloadedBlock(a, b, c, d, e))
                    .await
                    .unwrap();
            }
            // Close the event channel which allows the consumer task to exit.
            drop(event_tx);

            let notifications = pathfinder_rpc::Notifications::default();

            let (tx, _rx) = tokio::sync::watch::channel(Default::default());
            let context = ConsumerContext {
                storage: storage.clone(),
                state: Arc::new(SyncState::default()),
                submitted_tx_tracker: pathfinder_rpc::tracker::SubmittedTransactionTracker::new(
                    10, 10,
                ),
                pending_data: tx,
                verify_tree_hashes: false,
                notifications,
                sync_to_consensus_tx: None,
            };

            let (tx, _rx) = tokio::sync::watch::channel(Default::default());
            consumer(event_rx, context, tx).await.unwrap();

            let tx = conn.transaction().unwrap();
            for block in 0..(num_blocks - 1) {
                let block_id: BlockId = BlockNumber::new_or_panic(block).into();
                // Transaction data has been pruned (as well as block so query returns None).
                assert!(tx.transactions_for_block(block_id).unwrap().is_none());
                assert!(tx.transaction_hashes_for_block(block_id).unwrap().is_none());
                // Block data has been pruned.
                assert!(!tx.block_exists(block_id).unwrap());
            }
            let latest = tx.block_number(BlockId::Latest).unwrap().unwrap();
            assert_eq!(latest, BlockNumber::new_or_panic(4));
            let transactions = tx.transactions_for_block(latest.into()).unwrap().unwrap();
            let transaction_hashes = tx
                .transaction_hashes_for_block(latest.into())
                .unwrap()
                .unwrap();
            // Latest block transaction data has not been pruned.
            assert!(!transactions.is_empty() && !transaction_hashes.is_empty());
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn non_prunable_blocks() {
            let storage = StorageBuilder::in_memory_with_blockchain_pruning_and_pool_size(
                // Keep only the latest block.
                pathfinder_storage::pruning::BlockchainHistoryMode::Prune { num_blocks_kept: 0 },
                std::num::NonZeroU32::new(10).unwrap(),
            )
            .unwrap();
            let mut conn = storage.connection().unwrap();

            let (event_tx, event_rx) = tokio::sync::mpsc::channel(10);

            let blocks = block_data_with_state_updates(one_non_prunable_block_for_each_update());
            let num_blocks = blocks.len() as u64;
            // Send block updates.
            for (a, b, c, d, e) in blocks {
                event_tx
                    .send(SyncEvent::DownloadedBlock(a, b, c, d, e))
                    .await
                    .unwrap();
            }
            // Close the event channel which allows the consumer task to exit.
            drop(event_tx);

            let (tx, _rx) = tokio::sync::watch::channel(Default::default());
            let context = ConsumerContext {
                storage: storage.clone(),
                state: Arc::new(SyncState::default()),
                submitted_tx_tracker: pathfinder_rpc::tracker::SubmittedTransactionTracker::new(
                    10, 10,
                ),
                pending_data: tx,
                verify_tree_hashes: false,
                notifications: Default::default(),
                sync_to_consensus_tx: None,
            };

            let (tx, _rx) = tokio::sync::watch::channel(Default::default());
            consumer(event_rx, context, tx).await.unwrap();

            let tx = conn.transaction().unwrap();

            for block in 0..(num_blocks - 1) {
                let block_id: BlockId = BlockNumber::new_or_panic(block).into();
                // Transaction data has been pruned (as well as block so query returns None).
                assert!(tx.transactions_for_block(block_id).unwrap().is_none());
                assert!(tx.transaction_hashes_for_block(block_id).unwrap().is_none());
                // Block data has been pruned.
                assert!(!tx.block_exists(block_id).unwrap());
            }

            // Check that non-obsolete state update data has not been pruned.
            assert_eq!(
                tx.contract_class_hash(
                    BlockId::Number(BlockNumber::new_or_panic(2)),
                    contract_address_bytes!(b"contract 2"),
                )
                .unwrap()
                .unwrap(),
                class_hash_bytes!(b"class 1"),
            );
            assert_eq!(
                tx.contract_class_hash(
                    BlockId::Number(BlockNumber::new_or_panic(3)),
                    contract_address_bytes!(b"contract 1"),
                )
                .unwrap()
                .unwrap(),
                class_hash_bytes!(b"class 2"),
            );
            assert_eq!(
                tx.storage_value(
                    BlockId::Number(BlockNumber::new_or_panic(4)),
                    contract_address_bytes!(b"contract 1"),
                    storage_address_bytes!(b"storage address 1"),
                )
                .unwrap()
                .unwrap(),
                storage_value!("0x200"),
            );
            assert_eq!(
                tx.contract_nonce(
                    contract_address_bytes!(b"contract 1"),
                    BlockId::Number(BlockNumber::new_or_panic(5)),
                )
                .unwrap()
                .unwrap(),
                contract_nonce!("0x2"),
            );

            let latest = tx.block_number(BlockId::Latest).unwrap().unwrap();
            assert_eq!(latest, BlockNumber::new_or_panic(num_blocks - 1));
            let transactions = tx.transactions_for_block(latest.into()).unwrap().unwrap();
            let transaction_hashes = tx
                .transaction_hashes_for_block(latest.into())
                .unwrap()
                .unwrap();

            // Latest block transaction data has not been pruned.
            assert!(!transactions.is_empty() && !transaction_hashes.is_empty());
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn reorg_error() {
            let storage = StorageBuilder::in_memory_with_blockchain_pruning_and_pool_size(
                // Keep only the latest block.
                pathfinder_storage::pruning::BlockchainHistoryMode::Prune { num_blocks_kept: 0 },
                std::num::NonZeroU32::new(10).unwrap(),
            )
            .unwrap();

            let (event_tx, event_rx) = tokio::sync::mpsc::channel(10);

            let blocks = generate_block_data();
            // Send block updates.
            for (a, b, c, d, e) in blocks {
                event_tx
                    .send(SyncEvent::DownloadedBlock(a, b, c, d, e))
                    .await
                    .unwrap();
            }
            // Close the event channel which allows the consumer task to exit.
            drop(event_tx);

            let notifications = pathfinder_rpc::Notifications::default();

            let (tx, _rx) = tokio::sync::watch::channel(Default::default());
            let context = ConsumerContext {
                storage: storage.clone(),
                state: Arc::new(SyncState::default()),
                submitted_tx_tracker: pathfinder_rpc::tracker::SubmittedTransactionTracker::new(
                    10, 10,
                ),
                pending_data: tx,
                verify_tree_hashes: false,
                notifications,
                sync_to_consensus_tx: None,
            };

            let (tx, _rx) = tokio::sync::watch::channel(Default::default());
            consumer(event_rx, context, tx).await.unwrap();

            let (event_tx, event_rx) = tokio::sync::mpsc::channel(1);

            event_tx
                .send(SyncEvent::Reorg(BlockNumber::GENESIS))
                .await
                .unwrap();
            // Close the event channel which allows the consumer task to exit.
            drop(event_tx);

            let notifications = pathfinder_rpc::Notifications::default();

            let (tx, _rx) = tokio::sync::watch::channel(Default::default());
            let context = ConsumerContext {
                storage: storage.clone(),
                state: Arc::new(SyncState::default()),
                submitted_tx_tracker: pathfinder_rpc::tracker::SubmittedTransactionTracker::new(
                    10, 10,
                ),
                pending_data: tx,
                verify_tree_hashes: false,
                notifications,
                sync_to_consensus_tx: None,
            };

            let (tx, _rx) = tokio::sync::watch::channel(Default::default());
            let err = consumer(event_rx, context, tx).await.unwrap_err();
            assert_eq!(
                err.root_cause().to_string(),
                r"Reorg tail (block number: 0) does not exist (likely due to blockchain history pruning).
Blockchain history must include the reorg tail and its parent block to perform a reorg."
            );

            let (event_tx, event_rx) = tokio::sync::mpsc::channel(1);

            event_tx
                .send(SyncEvent::Reorg(BlockNumber::GENESIS + 4))
                .await
                .unwrap();
            // Close the event channel which allows the consumer task to exit.
            drop(event_tx);

            let notifications = pathfinder_rpc::Notifications::default();

            let (tx, _rx) = tokio::sync::watch::channel(Default::default());
            let context = ConsumerContext {
                storage,
                state: Arc::new(SyncState::default()),
                submitted_tx_tracker: pathfinder_rpc::tracker::SubmittedTransactionTracker::new(
                    10, 10,
                ),
                pending_data: tx,
                verify_tree_hashes: false,
                notifications,
                sync_to_consensus_tx: None,
            };

            let (tx, _rx) = tokio::sync::watch::channel(Default::default());
            let err = consumer(event_rx, context, tx).await.unwrap_err();
            assert_eq!(
                err.root_cause().to_string(),
                r"Reorg tail parent (block number: 3) does not exist (likely due to blockchain history pruning).
Blockchain history must include the reorg tail and its parent block to perform a reorg."
            );
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn reorg_success() {
            let storage = StorageBuilder::in_memory_with_blockchain_pruning_and_pool_size(
                // Keep only the last 2 blocks + latest.
                pathfinder_storage::pruning::BlockchainHistoryMode::Prune { num_blocks_kept: 2 },
                std::num::NonZeroU32::new(10).unwrap(),
            )
            .unwrap();
            let mut conn = storage.connection().unwrap();

            let (event_tx, event_rx) = tokio::sync::mpsc::channel(10);

            let blocks = generate_block_data();
            let block_count = blocks.len();
            // Send block updates.
            for (a, b, c, d, e) in blocks {
                event_tx
                    .send(SyncEvent::DownloadedBlock(a, b, c, d, e))
                    .await
                    .unwrap();
            }
            // Close the event channel which allows the consumer task to exit.
            drop(event_tx);

            let notifications = pathfinder_rpc::Notifications::default();

            let (tx, _rx) = tokio::sync::watch::channel(Default::default());
            let context = ConsumerContext {
                storage: storage.clone(),
                state: Arc::new(SyncState::default()),
                submitted_tx_tracker: pathfinder_rpc::tracker::SubmittedTransactionTracker::new(
                    10, 10,
                ),
                pending_data: tx,
                verify_tree_hashes: false,
                notifications,
                sync_to_consensus_tx: None,
            };

            let (tx, _rx) = tokio::sync::watch::channel(Default::default());
            consumer(event_rx, context, tx).await.unwrap();

            let (event_tx, event_rx) = tokio::sync::mpsc::channel(1);

            event_tx
                .send(SyncEvent::Reorg(
                    // Reorg to latest - 3.
                    BlockNumber::GENESIS + block_count as u64 - 2,
                ))
                .await
                .unwrap();
            // Close the event channel which allows the consumer task to exit.
            drop(event_tx);

            let notifications = pathfinder_rpc::Notifications::default();

            let (tx, _rx) = tokio::sync::watch::channel(Default::default());
            let context = ConsumerContext {
                storage,
                state: Arc::new(SyncState::default()),
                submitted_tx_tracker: pathfinder_rpc::tracker::SubmittedTransactionTracker::new(
                    10, 10,
                ),
                pending_data: tx,
                verify_tree_hashes: false,
                notifications,
                sync_to_consensus_tx: None,
            };

            let (tx, _rx) = tokio::sync::watch::channel(Default::default());
            consumer(event_rx, context, tx).await.unwrap();

            let tx = conn.transaction().unwrap();
            let latest = tx.block_number(BlockId::Latest).unwrap().unwrap();
            assert_eq!(latest, BlockNumber::GENESIS + block_count as u64 - 3);

            let prunable_blocks = vec![0, 1];
            for block in prunable_blocks {
                let block_id: BlockId = BlockNumber::new_or_panic(block).into();
                // Transaction data has been pruned (as well as block so query returns None).
                assert!(tx.transactions_for_block(block_id).unwrap().is_none());
                assert!(tx.transaction_hashes_for_block(block_id).unwrap().is_none());
                // Block data has been pruned.
                assert!(!tx.block_exists(block_id).unwrap());
            }
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn pruning_does_not_break_state_update_reconstruction() {
            let blocks = block_data_with_state_updates(state_update_reconstruction());
            let num_blocks = blocks.len() as u64;

            let storage = StorageBuilder::in_memory_with_blockchain_pruning_and_pool_size(
                // Prune only blocks 0 and 1.
                pathfinder_storage::pruning::BlockchainHistoryMode::Prune {
                    num_blocks_kept: num_blocks - 1 /* latest */ - 2, /* keep two blocks */
                },
                std::num::NonZeroU32::new(10).unwrap(),
            )
            .unwrap();
            let mut conn = storage.connection().unwrap();

            let (event_tx, event_rx) = tokio::sync::mpsc::channel(10);

            // Send block updates.
            for (a, b, c, d, e) in blocks {
                event_tx
                    .send(SyncEvent::DownloadedBlock(a, b, c, d, e))
                    .await
                    .unwrap();
            }
            // Close the event channel which allows the consumer task to exit.
            drop(event_tx);

            let (tx, _rx) = tokio::sync::watch::channel(Default::default());
            let context = ConsumerContext {
                storage: storage.clone(),
                state: Arc::new(SyncState::default()),
                submitted_tx_tracker: pathfinder_rpc::tracker::SubmittedTransactionTracker::new(
                    10, 10,
                ),
                pending_data: tx,
                verify_tree_hashes: false,
                notifications: Default::default(),
                sync_to_consensus_tx: None,
            };

            let (tx, _rx) = tokio::sync::watch::channel(Default::default());
            consumer(event_rx, context, tx).await.unwrap();

            let tx = conn.transaction().unwrap();

            let pruned_blocks = [0, 1];

            for block in pruned_blocks {
                let block_id: BlockId = BlockNumber::new_or_panic(block).into();
                // Transaction data has been pruned (as well as block so query returns None).
                assert!(tx.transactions_for_block(block_id).unwrap().is_none());
                assert!(tx.transaction_hashes_for_block(block_id).unwrap().is_none());
                // Block data has been pruned.
                assert!(!tx.block_exists(block_id).unwrap());
            }

            // Block 2 is not pruned but also cannot be queried for state update since it
            // doesn't have a parent block.
            assert!(tx
                .block_exists(BlockNumber::new_or_panic(2).into())
                .unwrap());

            // Check that state update reconstruction still works.
            let state_update = StateUpdate::default()
                .with_block_hash(block_hash_bytes!(b"3 block hash"))
                .with_state_commitment(state_commitment!(
                    "0x02217B6E78883EC62771BC63BEC0C34291FFA78EDCFFDE50C4DD8FDD4FE3158E"
                ))
                .with_parent_state_commitment(state_commitment!(
                    "0x049EA1B5F078CA95BEAEF0880401AE973BCB702F116E98F7F5F63ECAF1F8036B"
                ))
                .with_replaced_class(
                    contract_address_bytes!(b"contract 1"),
                    class_hash_bytes!(b"class 2"),
                );
            let result = tx
                .state_update(BlockId::Number(BlockNumber::new_or_panic(3)))
                .unwrap()
                .unwrap();
            assert_eq!(result, state_update);

            let state_update = StateUpdate::default()
                .with_block_hash(block_hash_bytes!(b"4 block hash"))
                .with_state_commitment(state_commitment!(
                    "0x032D16947452A6E41512E1515048675480D935C26CCC982E9888AC96EF65C189"
                ))
                .with_parent_state_commitment(state_commitment!(
                    "0x02217B6E78883EC62771BC63BEC0C34291FFA78EDCFFDE50C4DD8FDD4FE3158E"
                ))
                .with_replaced_class(
                    contract_address_bytes!(b"contract 1"),
                    class_hash_bytes!(b"class 3"),
                )
                .with_contract_nonce(
                    contract_address_bytes!(b"contract 1"),
                    contract_nonce!("0x1"),
                );

            let result = tx
                .state_update(BlockId::Number(BlockNumber::new_or_panic(4)))
                .unwrap()
                .unwrap();
            assert_eq!(result, state_update);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn pruning_relative_to_l1_checkpoint() {
            let storage = StorageBuilder::in_memory_with_blockchain_pruning_and_pool_size(
                pathfinder_storage::pruning::BlockchainHistoryMode::Prune { num_blocks_kept: 1 },
                std::num::NonZeroU32::new(10).unwrap(),
            )
            .unwrap();
            let mut conn = storage.connection().unwrap();

            let (event_tx, event_rx) = tokio::sync::mpsc::channel(10);

            let blocks = generate_block_data();
            let latest = blocks.len() - 1;
            // Make sure pruning doesn't happen before next L1 checkpoint (by setting the
            // current L1 checkpoint to genesis).
            let genesis_state_update = EthereumStateUpdate {
                block_number: BlockNumber::GENESIS,
                ..Default::default()
            };
            event_tx
                .send(SyncEvent::L1Update(genesis_state_update))
                .await
                .unwrap();
            // Send block updates.
            for (a, b, c, d, e) in blocks.into_iter() {
                event_tx
                    .send(SyncEvent::DownloadedBlock(a, b, c, d, e))
                    .await
                    .unwrap();
            }
            // Trigger pruning relative to L1 checkpoint with this event.
            let l1_checkpoint = latest - 2;
            let eth_state_update = EthereumStateUpdate {
                block_number: BlockNumber::new_or_panic(l1_checkpoint as u64),
                ..Default::default()
            };
            event_tx
                .send(SyncEvent::L1Update(eth_state_update))
                .await
                .unwrap();
            // Close the event channel which allows the consumer task to exit.
            drop(event_tx);

            let (tx, _rx) = tokio::sync::watch::channel(Default::default());
            let context = ConsumerContext {
                storage: storage.clone(),
                state: Arc::new(SyncState::default()),
                submitted_tx_tracker: pathfinder_rpc::tracker::SubmittedTransactionTracker::new(
                    10, 10,
                ),
                pending_data: tx,
                verify_tree_hashes: false,
                notifications: Default::default(),
                sync_to_consensus_tx: None,
            };

            let (tx, _rx) = tokio::sync::watch::channel(Default::default());
            consumer(event_rx, context, tx).await.unwrap();

            let tx = conn.transaction().unwrap();
            let prunable_blocks = vec![0];
            let non_prunable_blocks = vec![1, 2, 3, 4];

            for block in prunable_blocks {
                let block_id: BlockId = BlockNumber::new_or_panic(block).into();
                // Transaction data has been pruned (as well as block so query returns None).
                assert!(tx.transactions_for_block(block_id).unwrap().is_none());
                assert!(tx.transaction_hashes_for_block(block_id).unwrap().is_none());
                // Block data has been pruned.
                assert!(!tx.block_exists(block_id).unwrap());
            }

            for block in non_prunable_blocks {
                let block_id: BlockId = BlockNumber::new_or_panic(block).into();
                // Transaction and block data has not been pruned.
                let transactions = tx.transactions_for_block(block_id).unwrap().unwrap();
                let transaction_hashes =
                    tx.transaction_hashes_for_block(block_id).unwrap().unwrap();
                assert!(!transactions.is_empty() && !transaction_hashes.is_empty());
                assert!(tx.block_exists(block_id).unwrap());
            }
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn event_filter_pruning() {
            use pathfinder_storage::AGGREGATE_BLOOM_BLOCK_RANGE_LEN;

            let tempdir = tempfile::TempDir::new().unwrap();
            let storage_pruning_mode =
                pathfinder_storage::pruning::BlockchainHistoryMode::Prune { num_blocks_kept: 0 };
            let storage_pool_size = std::num::NonZeroU32::new(10).unwrap();
            let storage =
                StorageBuilder::in_persisted_tempdir_with_blockchain_pruning_and_pool_size(
                    &tempdir,
                    storage_pruning_mode,
                    storage_pool_size,
                )
                .unwrap();
            let mut conn = storage.connection().unwrap();

            let first_filter_exists = {
                let db_tx = conn.transaction().unwrap();
                db_tx
                    .event_filter_exists(
                        BlockNumber::GENESIS,
                        BlockNumber::GENESIS + AGGREGATE_BLOOM_BLOCK_RANGE_LEN - 1,
                    )
                    .unwrap()
            };
            assert!(!first_filter_exists);

            let mut blocks = block_data_with_state_updates(vec![
                    StateUpdate::default();
                    // Insert a full aggregate filter range + 1 so that we store one filter.
                    AGGREGATE_BLOOM_BLOCK_RANGE_LEN as usize + 1
                ]);
            let last_block = blocks.pop().unwrap();

            let (event_tx, event_rx) =
                tokio::sync::mpsc::channel(AGGREGATE_BLOOM_BLOCK_RANGE_LEN as usize + 1);
            // Make sure L2 relative pruning starts immediately.
            let l1_state_update = EthereumStateUpdate {
                block_number: BlockNumber::GENESIS + 2 * AGGREGATE_BLOOM_BLOCK_RANGE_LEN,
                ..Default::default()
            };
            event_tx
                .send(SyncEvent::L1Update(l1_state_update))
                .await
                .unwrap();
            // Send all but one block update.
            for (a, b, c, d, e) in blocks.into_iter() {
                event_tx
                    .send(SyncEvent::DownloadedBlock(a, b, c, d, e))
                    .await
                    .unwrap();
            }
            // Close the event channel which allows the consumer task to exit.
            drop(event_tx);

            let (pending_tx, _) = tokio::sync::watch::channel(Default::default());
            let context = ConsumerContext {
                storage: storage.clone(),
                state: Arc::new(SyncState::default()),
                submitted_tx_tracker: pathfinder_rpc::tracker::SubmittedTransactionTracker::new(
                    10, 10,
                ),
                pending_data: pending_tx,
                verify_tree_hashes: false,
                notifications: Default::default(),
                sync_to_consensus_tx: None,
            };

            let (current_tx, _) = tokio::sync::watch::channel(Default::default());
            consumer(event_rx, context, current_tx).await.unwrap();

            let first_filter_exists = {
                let db_tx = conn.transaction().unwrap();
                db_tx
                    .event_filter_exists(
                        BlockNumber::GENESIS,
                        BlockNumber::GENESIS + AGGREGATE_BLOOM_BLOCK_RANGE_LEN - 1,
                    )
                    .unwrap()
            };
            assert!(first_filter_exists);

            let (event_tx, event_rx) =
                tokio::sync::mpsc::channel(AGGREGATE_BLOOM_BLOCK_RANGE_LEN as usize);
            // Send the last block update.
            let (a, b, c, d, e) = last_block;
            event_tx
                .send(SyncEvent::DownloadedBlock(a, b, c, d, e))
                .await
                .unwrap();
            // Close the event channel which allows the consumer task to exit.
            drop(event_tx);

            let (pending_tx, _) = tokio::sync::watch::channel(Default::default());
            let context = ConsumerContext {
                storage: storage.clone(),
                state: Arc::new(SyncState::default()),
                submitted_tx_tracker: pathfinder_rpc::tracker::SubmittedTransactionTracker::new(
                    10, 10,
                ),
                pending_data: pending_tx,
                verify_tree_hashes: false,
                notifications: Default::default(),
                sync_to_consensus_tx: None,
            };

            let (current_tx, _) = tokio::sync::watch::channel(Default::default());
            consumer(event_rx, context, current_tx).await.unwrap();

            let first_filter_exists = {
                let db_tx = conn.transaction().unwrap();
                db_tx
                    .event_filter_exists(
                        BlockNumber::GENESIS,
                        BlockNumber::GENESIS + AGGREGATE_BLOOM_BLOCK_RANGE_LEN - 1,
                    )
                    .unwrap()
            };
            assert!(!first_filter_exists);

            // Pretend like we shut down unexpectedly by dropping these.
            drop(conn);
            drop(storage);

            // Create a new storage object to make sure the running event filter is rebuilt
            // correctly even when filters are pruned.
            let storage =
                StorageBuilder::in_persisted_tempdir_with_blockchain_pruning_and_pool_size(
                    &tempdir,
                    storage_pruning_mode,
                    storage_pool_size,
                )
                .unwrap();
            let mut conn = storage.connection().unwrap();
            let db_tx = conn.transaction().unwrap();
            // Running event filter got rebuilt so its next expected block is latest + 1.
            assert_eq!(
                db_tx.next_block_without_events().get(),
                AGGREGATE_BLOOM_BLOCK_RANGE_LEN + 1
            );
        }

        /// A regression test related to block purging behavior during reorg
        /// that was affected by the database migration where blockchain
        /// [pruning](pathfinder_storage::pruning) was introduced, not to
        /// pruning itself.
        #[tokio::test(flavor = "multi_thread")]
        async fn reorg_purging_regression() {
            let storage = StorageBuilder::in_memory_with_trie_pruning_and_pool_size(
                pathfinder_storage::TriePruneMode::Archive,
                std::num::NonZeroU32::new(5).unwrap(),
            )
            .unwrap();
            let mut connection = storage.connection().unwrap();

            let (event_tx, event_rx) = tokio::sync::mpsc::channel(100);

            let reorg_regression_data = ReorgRegressionData::new();

            let removed_class_hash = SierraHash(reorg_regression_data.removed_class_hash.0);
            let sierra_definition = SerializedSierraDefinition::from_slice(b"sierra definition");
            let casm_definition = SerializedCasmDefinition::from_slice(b"casm definition");
            let casm_hash = casm_hash_bytes!(b"casm hash");
            let casm_hash_v2 = casm_hash_bytes!(b"casm hash blake");

            // Add the class definition.
            event_tx
                .send(SyncEvent::SierraClass {
                    sierra_definition,
                    sierra_hash: removed_class_hash,
                    casm_definition,
                    casm_hash,
                    casm_hash_v2,
                })
                .await
                .unwrap();

            // Send block updates.
            let blocks = block_data_with_state_updates(reorg_regression_data.state_updates);
            for (a, b, c, d, e) in blocks {
                event_tx
                    .send(SyncEvent::DownloadedBlock(a, b, c, d, e))
                    .await
                    .unwrap();
            }

            event_tx
                .send(SyncEvent::Reorg(reorg_regression_data.reorg_tail))
                .await
                .unwrap();
            // Close the event channel which allows the consumer task to exit.
            drop(event_tx);

            let (tx, _rx) = tokio::sync::watch::channel(Default::default());
            let context = ConsumerContext {
                storage,
                state: Arc::new(SyncState::default()),
                submitted_tx_tracker: pathfinder_rpc::tracker::SubmittedTransactionTracker::new(
                    10, 10,
                ),
                pending_data: tx,
                verify_tree_hashes: false,
                notifications: Default::default(),
                sync_to_consensus_tx: None,
            };

            let (tx, _rx) = tokio::sync::watch::channel(Default::default());
            consumer(event_rx, context, tx).await.unwrap();

            let mut tx = connection.transaction().unwrap();
            use pathfinder_storage::reorg_regression_checks;
            assert!(reorg_regression_checks::contract_updates_deleted(
                &mut tx,
                reorg_regression_data.reorg_tail
            ));
            assert!(reorg_regression_checks::nonce_updates_deleted(
                &mut tx,
                reorg_regression_data.reorg_tail
            ));
            assert!(reorg_regression_checks::class_definition_removed(
                &mut tx,
                reorg_regression_data.removed_class_hash
            ));
        }
    }
}
