//! Core behaviour and other related utilities for p2p networks.
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;

use libp2p::{Multiaddr, PeerId};
use tokio::sync::{mpsc, oneshot};

mod behaviour;
mod client;
#[cfg(test)]
mod tests;

pub use behaviour::{kademlia_protocol_name, Behaviour, Builder, Event};
pub use client::Client;

use crate::peers::Peer;
use crate::EmptyResultSender;

/// Commands that can be sent to the p2p behaviour.
#[derive(Debug)]
pub enum Command<ApplicationCommand> {
    /// Listen for incoming connections on a specific address.
    Listen {
        addr: Multiaddr,
        sender: EmptyResultSender,
    },
    /// Dial a specific peer.
    Dial {
        peer_id: PeerId,
        addrs: Vec<Multiaddr>,
        sender: EmptyResultSender,
    },
    /// Disconnect from a specific peer.
    Disconnect {
        peer_id: PeerId,
        sender: EmptyResultSender,
    },
    /// Get the closest peers to a specific peer.
    GetClosestPeers {
        peer: PeerId,
        sender: mpsc::Sender<anyhow::Result<Vec<PeerId>>>,
    },
    /// Notify the p2p behaviour that a peer is not useful.
    NotUseful {
        peer_id: PeerId,
        sender: oneshot::Sender<()>,
    },
    /// Application-specific command.
    Application(ApplicationCommand),
    /// For testing purposes only
    _Test(TestCommand),
}

#[derive(Debug)]
pub enum TestCommand {
    GetPeersFromDHT(oneshot::Sender<HashSet<PeerId>>),
    GetConnectedPeers(oneshot::Sender<HashMap<PeerId, Peer>>),
}

#[derive(Debug)]
pub enum TestEvent {
    NewListenAddress(Multiaddr),
    KademliaBootstrapStarted,
    KademliaBootstrapCompleted(Result<PeerId, PeerId>),
    ConnectionEstablished { outbound: bool, remote: PeerId },
    ConnectionClosed { remote: PeerId },
    PeerAddedToDHT { remote: PeerId },
}

/// Configuration for the core p2p network.
pub mod config {
    use std::num::NonZeroU32;
    use std::path::PathBuf;
    use std::time::Duration;

    use ipnet::IpNet;

    /// Main configuration for the core p2p network.
    #[derive(Debug, Clone)]
    pub struct Config {
        /// A direct (not relayed) peer can only connect once in this period.
        pub direct_connection_timeout: Duration,
        /// A relayed peer can only connect once in this period.
        pub relay_connection_timeout: Duration,
        /// Maximum number of direct (non-relayed) inbound peers.
        pub max_inbound_direct_peers: usize,
        /// Maximum number of relayed inbound peers.
        pub max_inbound_relayed_peers: usize,
        /// Maximum number of outbound peers.
        pub max_outbound_peers: usize,
        /// How long to prevent evicted peers from reconnecting.
        pub eviction_timeout: Duration,
        pub ip_whitelist: Vec<IpNet>,
        /// If the number of peers is below the low watermark, the node will
        /// attempt periodic bootstrapping at this interval. If `None`,
        /// periodic bootstrap is disabled and only automatic bootstrap
        /// remains.
        pub bootstrap_period: Option<Duration>,
        pub inbound_connections_rate_limit: RateLimit,
        /// Maximum read bytes per second per connection.
        pub max_read_bytes_per_sec: Option<NonZeroU32>,
        /// Maximum write bytes per second per connection.
        pub max_write_bytes_per_sec: Option<NonZeroU32>,
        /// Custom protocol name for Kademlia
        pub kad_name: Option<String>,
        /// Data directory for Pathfinder.
        pub data_directory: PathBuf,
    }

    /// Rate limit for inbound connections.
    #[derive(Debug, Clone)]
    pub struct RateLimit {
        pub max: usize,
        pub interval: Duration,
    }

    #[cfg(test)]
    impl Config {
        pub fn for_test() -> Self {
            Self {
                direct_connection_timeout: Duration::from_secs(0),
                relay_connection_timeout: Duration::from_secs(0),
                max_inbound_direct_peers: 10,
                max_inbound_relayed_peers: 10,
                max_outbound_peers: 10,
                ip_whitelist: vec!["::1/0".parse().unwrap(), "0.0.0.0/0".parse().unwrap()],
                bootstrap_period: None,
                eviction_timeout: Duration::from_secs(15 * 60),
                inbound_connections_rate_limit: RateLimit {
                    max: 1000,
                    interval: Duration::from_secs(1),
                },
                max_read_bytes_per_sec: None,
                max_write_bytes_per_sec: None,
                kad_name: Default::default(),
                data_directory: PathBuf::default(),
            }
        }
    }
}

pub use config::Config;
