#![deny(rust_2018_idioms)]

pub mod config;
#[cfg(feature = "p2p")]
pub mod consensus;
#[cfg(feature = "p2p")]
pub mod devnet;
pub mod monitoring;
pub mod p2p_network;
pub mod state;
pub mod sync;
pub enum SyncMessageToConsensus {
    /// Ask consensus for the finalized and **decided upon** block with given
    /// number. The only difference from a committed block is that the state
    /// tries are not updated yet, so the state commitment is not computed
    /// and hence the block hash cannot be computed yet.
    GetConsensusFinalizedBlock {
        number: pathfinder_common::BlockNumber,
        reply: ConsensusFinalizedBlockReply,
    },
    /// Notify consensus that a block has been committed to storage. This can be
    /// either a block that was downloaded from the feeder gateway or a block
    /// that was produced locally by the consensus engine.
    ConfirmBlockCommitted {
        number: pathfinder_common::BlockNumber,
    },
    #[cfg(feature = "p2p")]
    ValidateBlock {
        // TODO: Stubbed for now, as an example. When used by P2P sync it should contain the block
        // commit certificate. Also, since this is never sent, the result is not used. In the
        // future, the result is used to update peer scoring and is also updating the tip of the
        // chain state for sync.
        block: std::sync::Arc<pathfinder_common::L2Block>,
        reply: ValidateBlockReply,
    },
}

pub type ConsensusFinalizedBlockReply =
    tokio::sync::oneshot::Sender<Option<Box<pathfinder_common::ConsensusFinalizedL2Block>>>;

#[cfg(feature = "p2p")]
pub type ValidateBlockReply = tokio::sync::oneshot::Sender<pathfinder_validator::ValidationResult>;

/// Various channels used to communicate with the consensus engine.
#[derive(Clone)]
pub struct ConsensusChannels {
    /// Watcher for the latest
    /// [pathfinder_common::consensus_info::ConsensusInfo].
    pub consensus_info_watch:
        tokio::sync::watch::Receiver<pathfinder_common::consensus_info::ConsensusInfo>,
    /// Channel for the sync task to send requests to consensus.
    pub sync_to_consensus_tx: tokio::sync::mpsc::Sender<SyncMessageToConsensus>,
}
