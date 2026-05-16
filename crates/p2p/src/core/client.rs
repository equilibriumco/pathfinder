use std::collections::HashSet;

use anyhow::Context;
use libp2p::{Multiaddr, PeerId};
use tokio::sync::mpsc::error::SendError;
use tokio::sync::{mpsc, oneshot};

use crate::core::Command;
#[cfg(test)]
use crate::test_utils;

/// The core p2p network client.
#[derive(Clone, Debug)]
pub struct Client<C> {
    sender: mpsc::UnboundedSender<Command<C>>,
    peer_id: PeerId,
}

impl<C> Client<C> {
    pub(crate) fn new(sender: mpsc::UnboundedSender<Command<C>>, peer_id: PeerId) -> Self {
        Self { sender, peer_id }
    }

    pub async fn send(&self, command: C) -> Result<(), SendError<Command<C>>> {
        self.sender.send(Command::Application(command))
    }

    pub fn peer_id(&self) -> &PeerId {
        &self.peer_id
    }

    pub fn as_pair(&self) -> (PeerId, mpsc::UnboundedSender<Command<C>>) {
        (self.peer_id, self.sender.clone())
    }

    pub async fn start_listening(&self, addr: Multiaddr) -> anyhow::Result<()> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(Command::Listen { addr, sender })
            .expect("Command receiver not to be dropped.");
        receiver.await.expect("Sender not to be dropped")
    }

    /// Dials the peer with the given [ID](PeerId) on a single address.
    ///
    /// To dial on multiple address at once, use [Client::dial_many].
    pub async fn dial(&self, peer_id: PeerId, addr: Multiaddr) -> anyhow::Result<()> {
        self.dial_many(peer_id, vec![addr]).await
    }

    /// Dials the peer with the given [ID](PeerId) on all specified addresses.
    ///
    /// To dial on a single address, use [Client::dial].
    pub async fn dial_many(&self, peer_id: PeerId, addrs: Vec<Multiaddr>) -> anyhow::Result<()> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(Command::Dial {
                peer_id,
                addrs,
                sender,
            })
            .expect("Command receiver not to be dropped");
        receiver.await.expect("Sender not to be dropped")
    }

    pub async fn disconnect(&self, peer_id: PeerId) -> anyhow::Result<()> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(Command::Disconnect { peer_id, sender })
            .expect("Command receiver not to be dropped");
        receiver.await.expect("Sender not to be dropped")
    }

    /// ### Important
    ///
    /// Triggers kademlia queries to other peers. This will cause `Io(Custom {
    /// kind: ConnectionRefused, error: "protocol not supported" })` error for
    /// each remote that does not support our kademlia protocol.
    pub async fn get_closest_peers(&self, peer: PeerId) -> anyhow::Result<HashSet<PeerId>> {
        let (sender, mut receiver) = mpsc::channel(1);
        self.sender
            .send(Command::GetClosestPeers { peer, sender })
            .expect("Command receiver not to be dropped");

        let mut peers = HashSet::new();

        while let Some(partial_result) = receiver.recv().await {
            let more_peers =
                partial_result.with_context(|| format!("Getting closest peers to {peer}"))?;
            peers.extend(more_peers);
        }

        Ok(peers)
    }

    /// Mark a peer as not useful.
    ///
    /// These peers will be candidates for outbound peer eviction.
    pub async fn not_useful(&self, peer_id: PeerId) {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(Command::NotUseful { peer_id, sender })
            .expect("Command receiver not to be dropped");
        receiver.await.expect("Sender not to be dropped")
    }

    #[cfg(test)]
    pub(crate) fn for_test(&self) -> test_utils::core::Client<C> {
        test_utils::core::Client::new(self.sender.clone())
    }
}
