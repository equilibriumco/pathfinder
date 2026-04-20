use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::num::NonZeroUsize;
use std::path::PathBuf;

use futures::StreamExt;
use libp2p::kad::{self, BootstrapError, BootstrapOk, QueryId, QueryResult};
use libp2p::multiaddr::{Multiaddr, Protocol};
use libp2p::swarm::dial_opts::DialOpts;
use libp2p::swarm::{NetworkBehaviour, SwarmEvent};
use libp2p::{identify, PeerId};
use pathfinder_common::integration_testing;
use tokio::sync::mpsc;
use tokio::time::Duration;

use crate::core::{Behaviour, Command, Event, TestCommand, TestEvent};
#[cfg(test)]
use crate::test_utils;
use crate::{ApplicationBehaviour, EmptyResultSender};

const COMMAND_CHANNEL_SIZE_LIMIT: usize = 1024;

/// This is our main loop for P2P networking.
/// It handles the incoming events from the swarm and the commands from the
/// outside world (most likely a p2p client).
///
/// It's generic over the application specific P2P `ApplicationBehaviour`, which
/// defines the commands and events that the application behaviour can handle.
///
/// ### Important
///
/// The current implementation uses a pair of unbounded channels in opposing
/// directions to accept incoming commands from a client and emit async events
/// that correspond to events from the p2p network. Previously it was possible
/// for these channels to enter a deadlock when an previously emitted event has
/// not been taken from the event channel yet and in the meantime a command was
/// sent to the main loop and just after that another event on the network
/// occurred. The solution can be to either implement a mutually exclusive
/// "half duplex" channel pair that occupy the same buffer or use channel
/// implementations that do not `await` on `send()`. The latter solution is lock
/// free however we must ensure that there is rate limiting employed on the
/// network layer side so that the event channel does not actually grow
/// indefinitely in some situations.
pub struct MainLoop<B>
where
    B: ApplicationBehaviour,
{
    /// Handles all internal networking for the p2p network.
    swarm: libp2p::swarm::Swarm<Behaviour<B>>,
    /// Receives commands from the outside world.
    command_receiver: mpsc::UnboundedReceiver<Command<<B as ApplicationBehaviour>::Command>>,
    /// Sends application-specific events to the outside world.
    app_event_sender: mpsc::UnboundedSender<<B as ApplicationBehaviour>::Event>,
    /// Sends application-specific test events to the outside world, no-op in
    /// production.
    _app_test_event_sender: mpsc::UnboundedSender<<B as ApplicationBehaviour>::TestEvent>,
    /// Keeps track of pending dials and allows us to notify the caller when a
    /// dial succeeds or fails.
    pending_dials: PendingDials,
    /// Keeps track of pending queries and allows us to send the response
    /// payloads back to the caller.
    pending_queries: PendingQueries,
    /// Data directory for Pathfinder.
    data_directory: PathBuf,
    /// State of the application behaviour.
    state: State<B>,
    /// Used to send [`crate::core::TestEvent`]-s during tests, no-op in
    /// production.
    _core_test_event_sender: mpsc::UnboundedSender<TestEvent>,
    /// Used to receive:
    /// - [`crate::core::TestEvent`]-s during tests, no-op in production.
    /// - Application-specific test events during tests, no-op in production.
    _test_event_receivers: Option<(
        mpsc::UnboundedReceiver<TestEvent>,
        mpsc::UnboundedReceiver<<B as ApplicationBehaviour>::TestEvent>,
    )>,
    _pending_test_queries: TestQueries,
    /// We keep a single command sender instance at all times so that receiver
    /// can be polled even without any client instance available without
    /// returning `Poll::Ready(None)`. This is important in cases when the node
    /// does not initiate any actions via the client and all the client
    /// instances are dropped.
    _command_sender: mpsc::UnboundedSender<Command<<B as ApplicationBehaviour>::Command>>,
}

/// Used to notify the caller when a dial succeeds or fails.
type PendingDials = HashMap<PeerId, EmptyResultSender>;

/// Used to keep track of the different types of pending queries and allows us
/// to send the response payloads back to the caller.
#[derive(Debug, Default)]
struct PendingQueries {
    /// Keeps track of pending GetClosestPeers queries
    pub get_closest_peers: HashMap<QueryId, mpsc::Sender<anyhow::Result<Vec<PeerId>>>>,
}

/// State of the application behaviour.
type State<B> = <B as ApplicationBehaviour>::State;

impl<B> MainLoop<B>
where
    B: ApplicationBehaviour,
    <B as NetworkBehaviour>::ToSwarm: std::fmt::Debug,
    <B as ApplicationBehaviour>::State: Default,
{
    /// Create a new main loop.
    ///
    /// # Arguments
    ///
    /// * `swarm` - The libp2p swarm, including the network behaviour for this
    ///   loop.
    /// * `app_event_sender` - The sender for application-specific events to the
    ///   outside world.
    /// * `data_directory` - The data directory for Pathfinder.
    pub fn new(
        swarm: libp2p::swarm::Swarm<Behaviour<B>>,
        app_event_sender: mpsc::UnboundedSender<<B as ApplicationBehaviour>::Event>,
        data_directory: PathBuf,
    ) -> (
        Self,
        mpsc::UnboundedSender<Command<<B as ApplicationBehaviour>::Command>>,
    ) {
        // Test event buffers are not used outside tests, so it is safe to make them
        // unbounded as they will never contain any items in production.
        let (_core_test_event_sender, core_rx) = mpsc::unbounded_channel();
        let (_app_test_event_sender, app_rx) = mpsc::unbounded_channel();

        let (command_sender, command_receiver) = mpsc::unbounded_channel();

        (
            Self {
                swarm,
                command_receiver,
                app_event_sender,
                _app_test_event_sender,
                pending_dials: Default::default(),
                pending_queries: Default::default(),
                state: Default::default(),
                data_directory,
                _core_test_event_sender,
                _test_event_receivers: Some((core_rx, app_rx)),
                _pending_test_queries: Default::default(),
                _command_sender: command_sender.clone(),
            },
            command_sender,
        )
    }

    /// Runs the main loop.
    ///
    /// This function handles and forwards the incoming events from the swarm,
    /// as well as handling the commands from the outside world that are
    /// sent to the p2p network.
    ///
    /// Note: This function will block until the main loop is stopped.
    pub async fn run(mut self) {
        // Check the network status every 5 seconds
        let mut network_status_interval = tokio::time::interval(Duration::from_secs(5));
        // Check the peer status every 30 seconds
        let mut peer_status_interval = tokio::time::interval(Duration::from_secs(30));
        // Keep track of whether we've already emitted a warning about the
        // command channel size exceeding the limit, to avoid spamming the logs.
        let mut channel_size_warning_emitted = false;

        loop {
            let network_status_tick = network_status_interval.tick();
            tokio::pin!(network_status_tick);

            let peer_status_tick = peer_status_interval.tick();
            tokio::pin!(peer_status_tick);

            tokio::select! {
                _ = network_status_tick => self.dump_network_status(),
                _ = peer_status_tick => self.dump_dht_and_connected_peers(),
                // Handle commands from the outside world
                command = self.command_receiver.recv() => {
                    // Unbounded channel size monitoring.
                    let channel_size = self.command_receiver.len();
                    if channel_size > COMMAND_CHANNEL_SIZE_LIMIT {
                        if !channel_size_warning_emitted {
                            tracing::warn!(%channel_size, "Command channel size exceeded limit");
                            channel_size_warning_emitted = true;
                        }
                    } else {
                        channel_size_warning_emitted = false;
                    }

                    self.handle_command(command.expect("At least one sender is retained by the main loop")).await
                },
                // Handle events from the inside of the p2p network
                Some(event) = self.swarm.next() => self.handle_event(event).await,
            }
        }
    }

    /// Handles an incoming event from the p2p network.
    ///
    /// Connection management, kademlia, and other network-related events are
    /// handled here. Application-specific events are forwarded to the
    /// application behaviour implementation.
    async fn handle_event(&mut self, event: SwarmEvent<Event<B>>) {
        tracing::trace!(?event, "Handling swarm event");

        match event {
            // ===========================
            // Connection management
            // ===========================
            //
            // A connection is established.
            // If the connection is outbound, notify the caller that the dial succeeded.
            SwarmEvent::ConnectionEstablished {
                peer_id, endpoint, ..
            } => {
                if endpoint.is_dialer() {
                    if let Some(sender) = self.pending_dials.remove(&peer_id) {
                        let _ = sender.send(Ok(()));
                        tracing::debug!(%peer_id, "Established outbound connection");
                    }
                } else {
                    tracing::debug!(%peer_id, "Established inbound connection");
                }

                send_test_event(
                    &self._core_test_event_sender,
                    TestEvent::ConnectionEstablished {
                        outbound: endpoint.is_dialer(),
                        remote: peer_id,
                    },
                )
                .await;
            }
            // An outgoing connection fails to be established.
            // Notifies the caller that the dial failed.
            SwarmEvent::OutgoingConnectionError {
                connection_id,
                peer_id,
                error,
            } => {
                tracing::debug!(%connection_id, ?peer_id, %error, "Failed to establish outgoing connection");
                if let Some(peer_id) = peer_id {
                    if let Some(sender) = self.pending_dials.remove(&peer_id) {
                        let _ = sender.send(Err(error.into()));
                    }
                }
            }
            // An incoming connection fails to be established.
            SwarmEvent::IncomingConnectionError {
                connection_id,
                local_addr,
                send_back_addr,
                error,
                peer_id: _,
            } => {
                tracing::debug!(%connection_id, %local_addr, %send_back_addr, %error, "Failed to establish incoming connection");
            }
            // A connection is closed.
            SwarmEvent::ConnectionClosed {
                peer_id,
                num_established,
                connection_id: _, // TODO consider tracking connection IDs for peers
                ..
            } => {
                tracing::debug!(%peer_id, "Connection closed");
                if num_established == 0 {
                    send_test_event(
                        &self._core_test_event_sender,
                        TestEvent::ConnectionClosed { remote: peer_id },
                    )
                    .await;
                }
            }
            // A peer is being dialed.
            SwarmEvent::Dialing {
                // The only API available to the caller [`crate::core::Client`] only
                // allows for dialing **known** peers, so we can discard the `None`
                // case here.
                peer_id: Some(peer_id),
                connection_id: _, // TODO consider tracking connection ids for peers
            } => {
                tracing::debug!(%peer_id, "Dialing peer");
            }
            // ===========================
            // Identify
            // ===========================
            //
            // A peer is identified.
            // The peer's observed address is added to our swarm's external addresses.
            // If the peer supports Kademlia (our DHT protocol), the peer's listening addresses
            // are added to the DHT.
            SwarmEvent::Behaviour(Event::Identify(e)) => {
                if let identify::Event::Received {
                    peer_id,
                    info:
                        identify::Info {
                            listen_addrs,
                            protocols,
                            observed_addr,
                            ..
                        },
                    ..
                } = e
                {
                    // Important change in libp2p-v0.52 compared to v0.51:
                    //
                    // https://github.com/libp2p/rust-libp2p/releases/tag/libp2p-v0.52.0
                    //
                    // As a consequence, the observed address reported by identify is no longer
                    // considered an external address but just an address candidate.
                    //
                    // https://github.com/libp2p/rust-libp2p/blob/master/protocols/identify/CHANGELOG.md#0430
                    //
                    // Observed addresses (aka. external address candidates) of the local node,
                    // reported by a remote node via libp2p-identify,
                    // are no longer automatically considered confirmed external addresses, in other
                    // words they are no longer trusted by default.
                    // Instead users need to confirm the reported observed address either manually,
                    // or by using libp2p-autonat. In trusted environments users
                    // can simply extract observed addresses from a
                    // libp2p-identify::Event::Received { info: libp2p_identify::Info {
                    // observed_addr }} and confirm them via Swarm::add_external_address.

                    self.swarm.add_external_address(observed_addr);

                    if let Some(kad) = self.swarm.behaviour_mut().kademlia_mut().as_mut() {
                        let my_kad_names = kad.protocol_names();

                        if protocols.iter().any(|p| my_kad_names.contains(p)) {
                            for addr in &listen_addrs {
                                kad.add_address(&peer_id, addr.clone());
                            }

                            if listen_addrs.is_empty() {
                                tracing::warn!(%peer_id, "Failed to add peer to DHT, no listening addresses");
                            } else {
                                tracing::debug!(%peer_id, "Added peer to DHT");
                            }
                        }
                    }
                }
            }
            // ===========================
            // Pings
            // ===========================
            //
            // A ping is received.
            // Forwards the ping to the network behaviour implementation.
            SwarmEvent::Behaviour(Event::Ping(event)) => {
                self.swarm.behaviour_mut().pinged(event);
            }
            // ===========================
            // Discovery
            // ===========================
            //
            // A Kademlia event is received.
            // These events represent the progress of Kademlia DHT queries:
            // - Bootstrap queries: Used when joining the network. -> We just log the success or
            //   failure of the bootstrap.
            // - GetClosestPeers queries: Used to find the closest peers to a given peer. -> We send
            //   the response (the list of closest peers) back to the caller.
            // - RoutingUpdated: A peer is added to the DHT.
            SwarmEvent::Behaviour(Event::Kademlia(e)) => match e {
                kad::Event::OutboundQueryProgressed {
                    step, result, id, ..
                } => {
                    if step.last {
                        match result {
                            libp2p::kad::QueryResult::Bootstrap(result) => {
                                let network_info = self.swarm.network_info();
                                let num_peers = network_info.num_peers();
                                let connection_counters = network_info.connection_counters();
                                let num_connections = connection_counters.num_connections();

                                let result = match result {
                                    Ok(BootstrapOk { peer, .. }) => {
                                        tracing::debug!(%num_peers, %num_connections, "Periodic bootstrap completed");
                                        Ok(peer)
                                    }
                                    Err(BootstrapError::Timeout { peer, .. }) => {
                                        tracing::warn!(%num_peers, %num_connections, "Periodic bootstrap failed");
                                        Err(peer)
                                    }
                                };
                                send_test_event(
                                    &self._core_test_event_sender,
                                    TestEvent::KademliaBootstrapCompleted(result),
                                )
                                .await;
                            }
                            QueryResult::GetClosestPeers(result) => {
                                use libp2p::kad::GetClosestPeersOk;

                                let result = match result {
                                    Ok(GetClosestPeersOk { peers, .. }) => {
                                        Ok(peers.into_iter().map(|p| p.peer_id).collect())
                                    }
                                    Err(e) => Err(e.into()),
                                };

                                let sender = self
                                    .pending_queries
                                    .get_closest_peers
                                    .remove(&id)
                                    .expect("Query to be pending");

                                sender
                                    .send(result)
                                    .await
                                    .expect("Receiver not to be dropped");
                            }
                            _ => self.test_query_completed(id, result).await,
                        }
                    } else {
                        match result {
                            QueryResult::Bootstrap(_) => {
                                tracing::debug!("Checking low watermark");
                                // Starting from libp2p-v0.54.1 bootstrap queries are started
                                // automatically in the kad behaviour:
                                // 1. periodically,
                                // 2. after a peer is added to the routing table, if the number of
                                //    peers in the DHT is lower than 20. See `bootstrap_on_low_peers` for more details:
                                //    https://github.com/libp2p/rust-libp2p/blob/d7beb55f672dce54017fa4b30f67ecb8d66b9810/protocols/kad/src/behaviour.rs#L1401).
                                if step.count == NonZeroUsize::new(1).expect("1>0") {
                                    send_test_event(
                                        &self._core_test_event_sender,
                                        TestEvent::KademliaBootstrapStarted,
                                    )
                                    .await;
                                }
                            }
                            _ => self.test_query_progressed(id, result).await,
                        }
                    }
                }
                kad::Event::RoutingUpdated {
                    peer, is_new_peer, ..
                } if is_new_peer => {
                    send_test_event(
                        &self._core_test_event_sender,
                        TestEvent::PeerAddedToDHT { remote: peer },
                    )
                    .await
                }
                _ => {}
            },
            // ===========================
            // NAT hole punching
            // ===========================
            //
            // A DCUtR event is received.
            SwarmEvent::Behaviour(Event::Dcutr(event)) => {
                tracing::debug!(?event, "DCUtR event");
            }
            // ===========================
            // Application behaviour
            // specific events
            // ===========================
            //
            // An application-specific event is received.
            // Forwards the event to the application behaviour implementation.
            SwarmEvent::Behaviour(Event::Application(application_event)) => {
                self.swarm
                    .behaviour_mut()
                    .application_mut()
                    .handle_event(
                        application_event,
                        &mut self.state,
                        self.app_event_sender.clone(),
                        self._app_test_event_sender.clone(),
                    )
                    .await;
            }
            // ===========================
            // Ignored or forwarded for
            // test purposes
            // ===========================
            event => {
                match &event {
                    SwarmEvent::NewListenAddr { address, .. } => {
                        let my_peerid = *self.swarm.local_peer_id();
                        self.debug_create_port_marker_file(address);
                        let address = address.clone().with(Protocol::P2p(my_peerid));
                        tracing::debug!(%address, "New listen");
                    }
                    _ => tracing::trace!(?event, "Ignoring event"),
                }
                self.handle_event_for_test(event).await;
            }
        }
    }

    /// ## Important
    /// This function does nothing in production builds.
    ///
    /// ## Integration testing
    /// Extracts the TCP port from the given multiaddress and creates
    /// a marker file in the data directory indicating that the port has been
    /// assigned.
    ///
    /// ## Panics
    /// The function will panic if it fails to create the marker file.
    fn debug_create_port_marker_file(&self, address: &Multiaddr) {
        if let Some(port) = address.iter().find_map(|p| {
            if let Protocol::Tcp(port) = p {
                Some(port)
            } else {
                None
            }
        }) {
            integration_testing::debug_create_port_marker_file(
                B::domain(),
                port,
                &self.data_directory,
            );
        }
    }

    /// Handles a command from the outside world.
    async fn handle_command(&mut self, command: Command<<B as ApplicationBehaviour>::Command>) {
        match command {
            // Instruct the swarm to listen on a given address.
            Command::Listen { addr, sender } => {
                let _ = match self.swarm.listen_on(addr.clone()) {
                    Ok(_) => {
                        tracing::debug!(%addr, "Started listening");
                        sender.send(Ok(()))
                    }
                    Err(e) => sender.send(Err(e.into())),
                };
            }
            // Instruct the swarm to dial a given peer.
            Command::Dial {
                peer_id,
                addr,
                sender,
            } => {
                if let std::collections::hash_map::Entry::Vacant(e) =
                    self.pending_dials.entry(peer_id)
                {
                    match self.swarm.dial(
                        // Dial a known peer with a given address only if it's not connected yet
                        // and we haven't started dialing yet.
                        DialOpts::peer_id(peer_id)
                            .addresses(vec![addr.clone()])
                            .build(),
                    ) {
                        Ok(_) => {
                            tracing::debug!(%addr, "Dialed peer");
                            e.insert(sender);
                        }
                        Err(e) => {
                            let _ = sender.send(Err(e.into()));
                        }
                    };
                } else {
                    let _ = sender.send(Err(anyhow::anyhow!("Dialing is already pending")));
                }
            }
            // Disconnects the swarm from a given peer.
            Command::Disconnect { peer_id, sender } => {
                let _ = sender.send(self.disconnect(peer_id).await);
            }
            // Request the closest peers from a given peer.
            Command::GetClosestPeers { peer, sender } => {
                self.swarm
                    .behaviour_mut()
                    .get_closest_peers(peer)
                    .map(|query_id| {
                        self.pending_queries
                            .get_closest_peers
                            .insert(query_id, sender)
                    });
            }
            // Notifies the swarm that a peer is not useful.
            Command::NotUseful { peer_id, sender } => {
                self.swarm.behaviour_mut().not_useful(peer_id);
                let _ = sender.send(());
            }
            // Application-specific commands.
            Command::Application(application_command) => {
                self.swarm
                    .behaviour_mut()
                    .application_mut()
                    .handle_command(application_command, &mut self.state)
                    .await;
            }
            Command::_Test(command) => self.handle_test_command(command).await,
        };
    }

    /// Disconnects the swarm from a given peer.
    async fn disconnect(&mut self, peer_id: PeerId) -> anyhow::Result<()> {
        self.pending_dials.remove(&peer_id);
        match self.swarm.disconnect_peer_id(peer_id) {
            Ok(()) => {
                tracing::debug!(%peer_id, "Disconnected");
                Ok(())
            }
            Err(()) => Err(anyhow::anyhow!("Failed to disconnect: peer not connected")),
        }
    }

    /// No-op outside tests
    async fn handle_event_for_test(&mut self, _event: SwarmEvent<Event<B>>) {
        #[cfg(test)]
        test_utils::main_loop::handle_event(&self._core_test_event_sender, _event).await
    }

    /// No-op outside tests
    async fn handle_test_command(&mut self, _command: TestCommand) {
        #[cfg(test)]
        test_utils::main_loop::handle_command(
            self.swarm.behaviour_mut(),
            _command,
            &mut self._pending_test_queries.inner,
        )
        .await;
    }

    /// Handle the final stage of the query, no-op outside tests
    async fn test_query_completed(&mut self, _id: QueryId, _result: QueryResult) {
        #[cfg(test)]
        test_utils::main_loop::query_completed(
            &mut self._pending_test_queries.inner,
            &self._core_test_event_sender,
            _id,
            _result,
        )
        .await;
    }

    /// Handle all stages except the final one, no-op outside tests
    async fn test_query_progressed(&mut self, _id: QueryId, _result: QueryResult) {
        #[cfg(test)]
        test_utils::main_loop::query_progressed(&self._pending_test_queries.inner, _id, _result)
            .await
    }

    fn dump_network_status(&self) {
        let network_info = self.swarm.network_info();
        let num_peers = network_info.num_peers();
        let connection_counters = network_info.connection_counters();
        let num_established_connections = connection_counters.num_established();
        let num_pending_connections = connection_counters.num_pending();
        tracing::info!(%num_peers, %num_established_connections, %num_pending_connections, "Network status")
    }

    fn dump_dht_and_connected_peers(&mut self) {
        let me = *self.swarm.local_peer_id();
        if let Some(kad) = self.swarm.behaviour_mut().kademlia_mut() {
            let dht = kad
                .kbuckets()
                // Cannot .into_iter() a KBucketRef, hence the inner collect followed by
                // flat_map
                .map(|kbucket_ref| {
                    kbucket_ref
                        .iter()
                        .map(|entry_ref| *entry_ref.node.key.preimage())
                        .collect::<Vec<_>>()
                })
                .flat_map(|peers_in_bucket| peers_in_bucket.into_iter())
                .collect::<HashSet<_>>();
            tracing::info!(%me, ?dht, "Local DHT");
        }

        let connected = self
            .swarm
            .behaviour()
            .peers()
            .filter_map(|(peer_id, peer)| {
                if peer.is_connected() {
                    Some(peer_id)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        tracing::info!(%me, ?connected, "Connected peers");
    }
}

#[cfg(test)]
impl<B> MainLoop<B>
where
    B: ApplicationBehaviour,
{
    pub fn take_test_event_receivers(
        &mut self,
    ) -> (
        mpsc::UnboundedReceiver<TestEvent>,
        mpsc::UnboundedReceiver<<B as ApplicationBehaviour>::TestEvent>,
    ) {
        Option::take(&mut self._test_event_receivers)
            .expect("Core test event receiver not to have been taken before")
    }
}

/// No-op outside tests
async fn send_test_event(_event_sender: &mpsc::UnboundedSender<TestEvent>, _event: TestEvent) {
    #[cfg(test)]
    test_utils::main_loop::send_event(_event_sender, _event).await
}

#[derive(Debug, Default)]
struct TestQueries {
    #[cfg(test)]
    inner: test_utils::main_loop::PendingQueries,
}
