use std::future::Future;

use libp2p::identity::Keypair;
use libp2p::swarm::NetworkBehaviour;
use pathfinder_common::ChainId;
use tokio::sync::{mpsc, oneshot};

/// Application-specific p2p network behaviour. This one handles consensus.
pub mod consensus;
/// Core p2p network behaviour. This is the foundation for all the other
/// application-specific behaviours.
pub mod core;
/// Application-specific p2p network behaviour. This one handles sync.
pub mod sync;

mod builder;
mod main_loop;
mod peer_data;
mod peers;
mod secret;
#[cfg(test)]
mod test_utils;
mod transport;

use builder::Builder;
pub use libp2p;
pub use peer_data::PeerData;
pub use transport::rate_limit::{
    MIN_BYTES_PER_SEC as TRANSPORT_MIN_BYTES_PER_SEC,
    MIN_BYTES_PER_SEC_PRETTY as TRANSPORT_MIN_BYTES_PER_SEC_PRETTY,
};

/// Creates a new sync P2P network.
pub fn new_sync(
    keypair: Keypair,
    core_config: core::Config,
    sync_config: sync::Config,
    chain_id: ChainId,
) -> (
    core::Client<sync::Command>,
    mpsc::UnboundedReceiver<sync::Event>,
    main_loop::MainLoop<sync::Behaviour>,
) {
    Builder::new(keypair, core_config, chain_id)
        .app_behaviour(sync::Behaviour::new(sync_config))
        .build()
}

/// Creates a new consensus P2P network.
pub fn new_consensus(
    keypair: Keypair,
    core_config: core::Config,
    chain_id: ChainId,
) -> (
    core::Client<consensus::Command>,
    mpsc::UnboundedReceiver<consensus::Event>,
    main_loop::MainLoop<consensus::Behaviour>,
) {
    Builder::new(keypair.clone(), core_config, chain_id)
        .app_behaviour(consensus::Behaviour::new(keypair))
        .build()
}

mod builder_phase {
    pub struct AppBehaviourUnset;
    pub struct AppBehaviourSet;
}

/// Defines how an application-specific p2p protocol (like sync or consensus)
/// interacts with the network:
/// - Commands: Actions requested by the application to be executed by the
///   network
/// - Events: Application-specific notifications from the network that the
///   application needs to handle
/// - TestEvents: Application-specific events that are only emitted in tests
/// - State: Data needed to track ongoing operations
///
/// This trait is implemented by application-specific network behaviors (like
/// sync, consensus) to define their p2p protocol logic.
pub trait ApplicationBehaviour: NetworkBehaviour {
    /// The type of commands that can be sent to the p2p network.
    type Command: std::fmt::Debug;
    /// The type of events that the p2p network can emit to the outside world.
    type Event;
    /// The type of events that the p2p network can emit to the outside world in
    /// tests only.
    type TestEvent;
    /// State needed to track pending network operations and their responses.
    type State;

    /// Handles a command from the outside world.
    fn handle_command(
        &mut self,
        command: Self::Command,
        state: &mut Self::State,
    ) -> impl Future<Output = ()> + Send;

    /// Handles an event from the inside of the p2p network.
    fn handle_event(
        &mut self,
        event: <Self as NetworkBehaviour>::ToSwarm,
        state: &mut Self::State,
        event_sender: mpsc::UnboundedSender<Self::Event>,
        test_event_sender: mpsc::UnboundedSender<Self::TestEvent>,
    ) -> impl Future<Output = ()> + Send;

    /// Returns the domain string used for marker file creation in integration
    /// testing, for example "sync" or "consensus".
    fn domain() -> &'static str;
}

type EmptyResultSender = oneshot::Sender<anyhow::Result<()>>;
