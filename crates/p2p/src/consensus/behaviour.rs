use libp2p::gossipsub::{self, Sha256Topic};
use libp2p::swarm::NetworkBehaviour;
use libp2p::{identity, PeerId};
use p2p_proto::consensus::Vote;
use p2p_proto::ProtobufSerializable;
#[cfg(test)]
use rand::seq::SliceRandom;
use tokio::sync::mpsc;
use tracing::error;

use crate::consensus::stream::StreamMessage;
use crate::consensus::{
    create_outgoing_proposal_message,
    handle_incoming_proposal_message,
    peer_score,
    Event,
    EventKind,
    TOPIC_PROPOSALS,
    TOPIC_VOTES,
};
use crate::{consensus, ApplicationBehaviour};

/// The consensus P2P network behaviour.
#[derive(NetworkBehaviour)]
pub struct Behaviour {
    gossipsub: gossipsub::Behaviour,
}

impl ApplicationBehaviour for Behaviour {
    type Command = consensus::Command;
    type Event = consensus::Event;
    type TestEvent = consensus::TestEvent;
    type State = consensus::State;

    #[tracing::instrument(skip(self, state))]
    async fn handle_command(&mut self, command: Self::Command, state: &mut Self::State) {
        use consensus::Command as ConsensusCommand;
        match command {
            ConsensusCommand::Proposal {
                height_and_round,
                proposal,
                done_tx,
            } => {
                let stream_msgs = proposal
                    .into_iter()
                    .flat_map(|proposal_part| {
                        create_outgoing_proposal_message(state, height_and_round, proposal_part)
                    })
                    .collect::<Vec<_>>();

                let mut tx_result = Ok(());
                for msg in stream_msgs {
                    let topic = Sha256Topic::new(TOPIC_PROPOSALS);

                    if let Err(e) = self.gossipsub.publish(topic, msg.to_protobuf_bytes()) {
                        error!(
                            "Failed to publish proposal message, stream id {}, message id {}, \
                             error {e:?}",
                            msg.stream_id, msg.message_id
                        );
                        tx_result = Err(e);
                        break;
                    }
                }
                done_tx
                    .send(tx_result)
                    .await
                    .expect("Receiver not to be dropped");
            }
            ConsensusCommand::Vote { vote, done_tx } => {
                let cloned_vote = vote.clone();
                let data = vote.to_protobuf_bytes();
                let topic = Sha256Topic::new(TOPIC_VOTES);

                let tx_result = self
                    .gossipsub
                    .publish(topic, data)
                    .inspect_err(|e| {
                        error!("Failed to publish vote message {cloned_vote:?}, error {e:?}");
                    })
                    .map(|_| ());

                done_tx
                    .send(tx_result)
                    .await
                    .expect("Receiver not to be dropped");
            }
            ConsensusCommand::PeerScoreDecay => {
                let connected_peers: Vec<_> =
                    self.gossipsub.all_peers().map(|(id, _)| *id).collect();

                // Use the opportunity to remove scores for disconnected peers.
                state.peer_app_scores.retain(|peer_id, score| {
                    if connected_peers.contains(peer_id) {
                        *score *= peer_score::DECAY_FACTOR;
                        true
                    } else {
                        false
                    }
                });
            }
            ConsensusCommand::ChangePeerScore { peer_id, delta } => {
                let current_score = state
                    .peer_app_scores
                    .entry(peer_id)
                    .or_insert(peer_score::INITIAL_APPLICATION_SCORE);
                *current_score += delta;
                let done = self
                    .gossipsub
                    .set_application_score(&peer_id, *current_score);
                if !done {
                    // Peer scoring _should_ be active for consensus P2P, so the only reason we
                    // would fail to set the score is if the peer disconnected or its score
                    // expired.
                    // Either way, we can remove its score from our local state at this point.
                    state.peer_app_scores.remove(&peer_id);
                    tracing::debug!(
                        "Failed to set peer score for {peer_id}, peer may have disconnected"
                    );
                }
            }
            #[cfg(test)]
            ConsensusCommand::TestProposalStream(height_and_round, proposal_stream, shuffle) => {
                // This command is used to test out-of-order delivery of proposal streams.
                // The `message_id` must be assigned sequentially within
                // `create_outgoing_proposal_message`, so this test command
                // needs to live in the network layer rather than the application layer.
                let mut stream_msgs = Vec::new();
                for part in proposal_stream {
                    let msgs = create_outgoing_proposal_message(state, height_and_round, part);
                    stream_msgs.extend(msgs);
                }
                if shuffle {
                    stream_msgs.shuffle(&mut rand::thread_rng());
                }
                for msg in stream_msgs {
                    let topic = Sha256Topic::new(TOPIC_PROPOSALS);
                    if let Err(e) = self.gossipsub.publish(topic, msg.to_protobuf_bytes()) {
                        error!("Failed to publish proposal message: {}", e);
                    }
                }
            }
        }
    }

    #[tracing::instrument(skip(self))]
    async fn handle_event(
        &mut self,
        event: BehaviourEvent,
        state: &mut Self::State,
        event_sender: mpsc::UnboundedSender<Self::Event>,
        test_event_sender: mpsc::UnboundedSender<Self::TestEvent>,
    ) {
        use gossipsub::Event::*;
        let BehaviourEvent::Gossipsub(e) = event;

        tracing::trace!(event=?e, "Gossipsub event received");

        let topic_proposals_hash = Sha256Topic::new(TOPIC_PROPOSALS).hash();
        let topic_votes_hash = Sha256Topic::new(TOPIC_VOTES).hash();

        match e {
            Message {
                propagation_source,
                message_id,
                message,
            } => match message.topic {
                hash if hash == topic_proposals_hash => {
                    let Ok(stream_msg) = StreamMessage::from_protobuf_bytes(&message.data) else {
                        error!("Failed to parse proposal message with id: {}", message_id);
                        return;
                    };

                    if let Some(events) =
                        handle_incoming_proposal_message(state, stream_msg, propagation_source)
                    {
                        for event in events {
                            let _ = event_sender.send(event);
                        }
                    }
                }
                hash if hash == topic_votes_hash => {
                    if let Ok(vote) = Vote::from_protobuf_bytes(&message.data) {
                        let event = Event {
                            source: propagation_source,
                            kind: EventKind::Vote(vote),
                        };
                        let _ = event_sender.send(event);
                    } else {
                        error!("Failed to parse vote message with id: {}", message_id);
                    }
                }
                _ => {}
            },
            Subscribed { peer_id, topic } => {
                tracing::debug!("Peer {} subscribed to topic {}", peer_id, topic);

                #[cfg(test)]
                {
                    let _ = test_event_sender.send(consensus::TestEvent {
                        source: peer_id,
                        kind: consensus::TestEventKind::Subscribed(topic),
                    });
                }
            }
            _ => {
                // TODO: Do we care about any other Gossipsub events?
            }
        }
    }

    fn domain() -> &'static str {
        "p2p_consensus"
    }
}

impl Behaviour {
    /// Create a new consensus behaviour.
    pub fn new(keypair: identity::Keypair) -> Self {
        let peer_id = PeerId::from(keypair.public());

        let gossipsub_config = gossipsub::Config::default();
        let mut gossipsub = gossipsub::Behaviour::new(
            gossipsub::MessageAuthenticity::Signed(keypair),
            gossipsub_config,
        )
        .expect("Failed to create gossipsub behaviour");

        gossipsub
            .with_peer_score(
                peer_score::default_params(),
                peer_score::default_thresholds(),
            )
            .expect("Params should be valid and not already set");

        let proposals_topic = Sha256Topic::new(TOPIC_PROPOSALS);
        let votes_topic = Sha256Topic::new(TOPIC_VOTES);

        gossipsub
            .subscribe(&proposals_topic)
            .expect("Failed to subscribe to proposals topic");
        gossipsub
            .subscribe(&votes_topic)
            .expect("Failed to subscribe to votes topic");

        tracing::info!("Consensus node started with peer ID: {}", peer_id);
        tracing::info!("Subscribed to topics: {}, {}", proposals_topic, votes_topic);

        Behaviour { gossipsub }
    }
}
