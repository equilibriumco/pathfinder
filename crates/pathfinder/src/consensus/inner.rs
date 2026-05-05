mod batch_execution;
mod consensus_task;
mod fetch_proposers;
mod fetch_validators;
mod gossip_retry;
mod integration_testing;
mod p2p_task;
mod proposal_validator;

mod dummy_proposal;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use p2p::consensus::{Event, HeightAndRound};
use p2p_proto::consensus::ProposalPart;
use pathfinder_common::{
    consensus_info,
    BlockNumber,
    BlockTimestamp,
    ChainId,
    ConsensusFinalizedBlockHeader,
    ConsensusFinalizedL2Block,
    ContractAddress,
    ProposalCommitment,
    StarknetVersion,
};
use pathfinder_consensus::{ConsensusCommand, ConsensusEvent, NetworkMessage};
use pathfinder_gas_price::L1GasPriceProvider;
use pathfinder_storage::Storage;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, watch};

use super::{ConsensusChannels, ConsensusTaskHandles};
use crate::config::integration_testing::InjectFailureConfig;
use crate::config::ConsensusConfig;
use crate::SyncMessageToConsensus;

#[allow(clippy::too_many_arguments)]
pub fn start(
    config: ConsensusConfig,
    chain_id: ChainId,
    main_storage: Storage,
    p2p_consensus_client: p2p::consensus::Client,
    p2p_event_rx: mpsc::UnboundedReceiver<Event>,
    wal_directory: PathBuf,
    data_directory: &Path,
    gas_price_provider: Option<L1GasPriceProvider>,
    verify_tree_hashes: bool,
    compiler_resource_limits: pathfinder_compiler::ResourceLimits,
    blockifier_libfuncs: pathfinder_compiler::BlockifierLibfuncs,
    inject_failure_config: Option<InjectFailureConfig>,
) -> ConsensusTaskHandles {
    // Events that are produced by the P2P task and consumed by the consensus task.
    // TODO determine sufficient buffer size. 1 is not enough.
    let (tx_to_consensus, rx_from_p2p) = mpsc::channel::<ConsensusTaskEvent>(10);
    // Events that are produced by the consensus task and consumed by the P2P task.
    // TODO determine sufficient buffer size. 1 is not enough.
    let (tx_to_p2p, rx_from_consensus) = mpsc::channel::<P2PTaskEvent>(10);
    // Requests sent to consensus by the sync task.
    let (sync_to_consensus_tx, sync_to_consensus_rx) = mpsc::channel::<SyncMessageToConsensus>(10);

    let (info_watch_tx, consensus_info_watch) =
        watch::channel(consensus_info::ConsensusInfo::default());
    let finalized_blocks = HashMap::new();

    let (consensus_p2p_event_processing_handle, worker_pool) = p2p_task::spawn(
        chain_id,
        (&config).into(),
        p2p_consensus_client,
        p2p_event_rx,
        tx_to_consensus,
        rx_from_consensus,
        sync_to_consensus_rx,
        info_watch_tx,
        main_storage.clone(),
        finalized_blocks,
        data_directory,
        compiler_resource_limits,
        blockifier_libfuncs,
        verify_tree_hashes,
        gas_price_provider,
        inject_failure_config,
    );

    let consensus_engine_handle = consensus_task::spawn(
        chain_id,
        config,
        wal_directory,
        tx_to_p2p,
        rx_from_p2p,
        main_storage,
        data_directory,
        compiler_resource_limits,
        blockifier_libfuncs,
        inject_failure_config,
    );

    ConsensusTaskHandles {
        consensus_p2p_event_processing_handle,
        consensus_engine_handle,
        consensus_channels: Some(ConsensusChannels {
            consensus_info_watch,
            sync_to_consensus_tx,
        }),
        worker_pool: Some(worker_pool),
    }
}

/// Events handled by the consensus task.
enum ConsensusTaskEvent {
    /// The consensus engine informs us about an event that it wants us to
    /// handle.
    Event(ConsensusEvent<ConsensusValue, ContractAddress>),
    /// We received an event from the P2P network which has impact on
    /// consensus, so we issue a command to the consensus engine.
    CommandFromP2P(ConsensusCommand<ConsensusValue, ContractAddress>),
}

/// Events handled by the p2p task.
#[allow(clippy::large_enum_variant)]
enum P2PTaskEvent {
    /// An event coming from the P2P network (from the consensus P2P network
    /// main loop).
    P2PEvent(Event),
    /// A request coming from the sync task.
    SyncRequest(SyncMessageToConsensus),
    /// The consensus engine requested that we produce a proposal, so we
    /// create it, feed it back to the consensus engine, and we must
    /// cache it for gossiping when the engine requests so.
    CacheProposal(HeightAndRound, Vec<ProposalPart>, ConsensusFinalizedL2Block),
    /// Consensus requested that we gossip a message via the P2P network.
    GossipRequest(NetworkMessage<ConsensusValue, ContractAddress>),
    /// Indicate that the given block and state update can be committed to the
    /// database. All proposals for this height are removed from the cache. All
    /// other consensus finalized blocks for lower rounds at this height are
    /// discarded.
    MarkBlockAsDecidedAndCleanUp(HeightAndRound, ConsensusValue),
}

#[derive(Copy, Clone, Debug)]
struct P2PTaskConfig {
    my_starknet_version: StarknetVersion,
    my_validator_address: ContractAddress,
    history_depth: u64,
}

impl From<&ConsensusConfig> for P2PTaskConfig {
    fn from(config: &ConsensusConfig) -> Self {
        Self {
            my_starknet_version: config.my_starknet_version,
            my_validator_address: config.my_validator_address,
            history_depth: config.history_depth,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct ConsensusValue(ProposalCommitment);

impl std::fmt::Display for ConsensusValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Creates an empty finalized L2 block for the given height.
///
/// TODO: The consensus spec does not define this for empty proposals. However,
/// the validator logic and storage usage patterns currently require a finalized
/// block to be created even for empty proposals. For now, we create a (mostly)
/// default block header with the necessary fields filled in.
pub(crate) fn create_empty_block(
    height: u64,
    starknet_version: StarknetVersion,
) -> ConsensusFinalizedL2Block {
    let timestamp = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    ConsensusFinalizedL2Block {
        header: ConsensusFinalizedBlockHeader {
            number: BlockNumber::new_or_panic(height),
            timestamp: BlockTimestamp::new_or_panic(timestamp),
            starknet_version,
            ..Default::default()
        },
        ..Default::default()
    }
}
