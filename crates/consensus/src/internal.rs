mod malachite;
mod wal;

use std::collections::{BinaryHeap, VecDeque};

use malachite::*;
use malachite_consensus::{
    process,
    Effect,
    Error,
    Input,
    LocallyProposedValue,
    Params,
    Resumable,
    Resume,
    SignedConsensusMsg,
    State,
};
use malachite_signing_ed25519::Signature;
use malachite_types::{
    Height as _,
    SignedMessage,
    ThresholdParams,
    Timeout,
    TimeoutKind,
    ValuePayload,
};
use tokio::time::Instant;
use wal::*;

use crate::config::TimeoutValues;
use crate::error::ConsensusError;
use crate::wal::{WalEntry, WalSink};
use crate::{
    ConsensusCommand,
    ConsensusEvent,
    NetworkMessage,
    Proposal,
    ProposerSelector,
    Round,
    SignedProposal,
    SignedVote,
    ValidatorSet,
};

/// The parameters for the internal consensus engine.
pub struct InternalParams<A> {
    pub height: u64,
    pub validator_set: ValidatorSet<A>,
    pub address: A,
    pub threshold_params: ThresholdParams,
    pub value_payload: ValuePayload,
}

/// The [InternalConsensus] acts as a driver for the Malachite core.
///
/// It allows us to decouple all the Malachite intrinsics from our public
/// interface.
///
/// We spawn a new instance of this entity for each height, as we might be
/// involved in multiple consensus processes at the same time.
pub struct InternalConsensus<
    V: crate::ValuePayload + 'static,
    A: crate::ValidatorAddress + 'static,
    P: ProposerSelector<A> + Send + Sync + 'static,
> {
    state: State<MalachiteContext<V, A, P>>,
    metrics: malachite_metrics::Metrics,
    input_queue: VecDeque<ConsensusCommand<V, A>>,
    output_queue: VecDeque<ConsensusEvent<V, A>>,
    timeout_manager: TimeoutManager,
    wal: Box<dyn WalSink<V, A>>,
}

impl<
        V: crate::ValuePayload + 'static,
        A: crate::ValidatorAddress + 'static,
        P: ProposerSelector<A> + Send + Sync + 'static,
    > InternalConsensus<V, A, P>
{
    pub fn new(
        params: InternalParams<A>,
        timeout_values: TimeoutValues,
        wal: Box<dyn WalSink<V, A>>,
        proposer_selector: P,
    ) -> Self {
        let params = Params {
            initial_height: Height::new(params.height),
            initial_validator_set: params.validator_set.into(),
            address: ValidatorAddress::from(params.address),
            threshold_params: params.threshold_params,
            value_payload: params.value_payload,
        };

        let state = State::new(MalachiteContext::new(proposer_selector), params, 128);
        Self {
            state,
            metrics: Default::default(),
            input_queue: VecDeque::new(),
            output_queue: VecDeque::new(),
            timeout_manager: TimeoutManager {
                timeouts: BinaryHeap::new(),
                timeout_values,
            },
            wal,
        }
    }

    /// Recover the consensus from a list of write-ahead log entries.
    pub fn recover_from_wal(&mut self, entries: Vec<WalEntry<V, A>>) -> Option<Round> {
        tracing::debug!(
            validator = %self.state.address(),
            entry_count = entries.len(),
            "Recovering consensus from WAL entries"
        );

        // Check if any entry is a Decision, which indicates this height is finalized.
        let has_decision = entries
            .iter()
            .any(|e| matches!(e, WalEntry::Decision { .. }));

        // Mark the WAL as finalized if we're recovering from a Decision entry.
        if has_decision {
            self.wal.mark_as_finalized();
        }

        // Now process the entries.
        let mut max_round: Option<u32> = None;
        for (i, entry) in entries.into_iter().enumerate() {
            // We skip Decision entries as they're just markers.
            if matches!(entry, WalEntry::Decision { .. }) {
                continue;
            }

            // Find the last round with votes
            max_round = max_round.max(Self::get_vote_round(&entry));

            let input = convert_wal_entry_to_input(entry);
            if let Err(e) = self.process_input(input) {
                tracing::warn!(
                    validator = %self.state.address(),
                    entry_index = i,
                    error = %e,
                    "Failed to process WAL entry during recovery - skipping corrupted entry"
                );
                self.output_queue
                    .push_back(ConsensusEvent::Error(ConsensusError::wal_recovery(
                        e.into(),
                    )));
            }
        }

        tracing::debug!(
            validator = %self.state.address(),
            "Completed WAL recovery"
        );

        max_round.map(Round::from)
    }

    /// Schedule rebroadcast timeout.
    pub fn schedule_rebroadcast(&mut self, round: Round) {
        self.timeout_manager.schedule_timeout(Timeout {
            kind: TimeoutKind::Rebroadcast,
            round: round.into(),
        });
    }

    /// Check if this consensus engine has been finalized (i.e., a decision has
    /// been reached)
    pub fn is_finalized(&self) -> bool {
        self.wal.is_finalized()
    }

    /// Feed a command into the consensus engine.
    pub fn handle_command(&mut self, cmd: ConsensusCommand<V, A>) {
        self.input_queue.push_back(cmd);
    }

    /// Poll the internal consensus engine for the next event.
    pub async fn poll_internal(&mut self) -> Option<ConsensusEvent<V, A>> {
        // Process any timeouts that are due.
        let now = Instant::now();
        while let Some(next) = self.timeout_manager.timeouts.peek() {
            if now < next.due {
                break;
            }
            let ScheduledTimeout { timeout, .. } = self
                .timeout_manager
                .timeouts
                .pop()
                .expect("No timeout to pop");
            let input = Input::TimeoutElapsed(timeout);
            tracing::debug!(
                validator = %self.state.address(),
                timeout = ?timeout,
                "Timeout elapsed"
            );
            if let Err(e) = self.process_input(input) {
                self.output_queue
                    .push_back(ConsensusEvent::Error(ConsensusError::malachite(e)));
            }
        }

        // Process any commands that are waiting in the input queue.
        if let Some(cmd) = self.input_queue.pop_front() {
            let input = match cmd {
                ConsensusCommand::Vote(vote) => {
                    tracing::debug!(
                        validator = ?self.state.address(),
                        vote_type = ?vote.vote.r#type,
                        from = ?vote.vote.validator_address,
                        height = %vote.vote.height,
                        round = ?vote.vote.round,
                        "Received vote"
                    );
                    Input::Vote(convert_vote_in(vote))
                }
                ConsensusCommand::Proposal(proposal) => {
                    tracing::debug!(
                        validator = ?self.state.address(),
                        value = ?proposal.proposal.value,
                        from = ?proposal.proposal.proposer,
                        height = %proposal.proposal.height,
                        round = ?proposal.proposal.round,
                        "Received proposal"
                    );
                    let signed_msg = malachite_types::SignedProposal::new(
                        proposal.proposal.into(),
                        proposal.signature,
                    );
                    Input::Proposal(signed_msg)
                }
                ConsensusCommand::Propose(proposal) => {
                    tracing::debug!(
                        validator = %self.state.address(),
                        value = ?proposal.value,
                        height = %proposal.height,
                        round = ?proposal.round,
                        "Proposing value"
                    );
                    Input::Propose(LocallyProposedValue::new(
                        Height::new(proposal.height),
                        proposal.round.into(),
                        ConsensusValue::from(proposal.value),
                    ))
                }
                ConsensusCommand::StartHeight(height, validators) => {
                    tracing::info!(
                        validator = %self.state.address(),
                        height = %height,
                        validator_count = validators.count(),
                        "Starting new height"
                    );
                    Input::StartHeight(Height::new(height), validators.into())
                }
            };

            Self::assert_allowed_input_types(&input, line!());

            if let Err(e) = self.process_input(input) {
                self.output_queue
                    .push_back(ConsensusEvent::Error(ConsensusError::malachite(e)));
            }
        }

        self.output_queue.pop_front()
    }

    /// [`Input::ProposedValue`] is never fed into the engine during live
    /// operation. It's only used during WAL recovery to replay the proposed
    /// values, and when that happens, value origin is always set to
    /// [`malachite_types::ValueOrigin::Consensus`].
    ///
    /// See [`crate::internal::wal::convert_wal_entry_to_input`] for more
    /// details.
    ///
    /// Note: This assert is explicitly in the form of a dedicated function to
    /// ensure that the linked documentation path is actively checked by CI.
    fn assert_allowed_input_types(input: &Input<MalachiteContext<V, A, P>>, line: u32) {
        assert!(
            matches!(
                input,
                Input::Vote(_) | Input::Proposal(_) | Input::Propose(_) | Input::StartHeight(_, _)
            ),
            "Unexpected consensus input type {input:?} at line {line}"
        );
    }

    fn get_vote_round(entry: &WalEntry<V, A>) -> Option<u32> {
        if let WalEntry::SignedVote(signed) = entry {
            signed.vote.round.0
        } else {
            None
        }
    }

    #[allow(clippy::result_large_err)]
    fn process_input(
        &mut self,
        input: Input<MalachiteContext<V, A, P>>,
    ) -> Result<(), Error<MalachiteContext<V, A, P>>> {
        let output = &mut self.output_queue;
        let validator_set = self.state.validator_set().clone();

        process!(
            input: input,
            state: &mut self.state,
            metrics: &mut self.metrics,
            with: effect => handle_effect(effect, &validator_set, &mut self.timeout_manager, &mut self.wal, output)
        )
    }
}

#[allow(clippy::result_large_err)]
#[allow(clippy::type_complexity)]
fn handle_effect<
    V: crate::ValuePayload + 'static,
    A: crate::ValidatorAddress + 'static,
    P: ProposerSelector<A> + Send + Sync + 'static,
>(
    effect: Effect<MalachiteContext<V, A, P>>,
    validator_set: &malachite::ValidatorSet<V, A>,
    timeout_manager: &mut TimeoutManager,
    wal: &mut Box<dyn WalSink<V, A>>,
    output_queue: &mut VecDeque<ConsensusEvent<V, A>>,
) -> Result<Resume<MalachiteContext<V, A, P>>, Error<MalachiteContext<V, A, P>>> {
    match effect {
        // Start a new round.
        Effect::StartRound(height, round, address, role, resume) => {
            tracing::debug!(
                height = %height,
                round = %round,
                address = ?address,
                role = ?role,
                "Starting new round"
            );
            Ok(resume.resume_with(()))
        }
        // Get the validator set at a given height.
        Effect::GetValidatorSet(height, resume) => {
            tracing::debug!(
                height = %height,
                "Getting validator set"
            );
            Ok(resume.resume_with(Some(validator_set.clone())))
        }
        // Publish a message to peers.
        Effect::PublishConsensusMsg(msg, resume) => {
            match &msg {
                SignedConsensusMsg::Proposal(proposal) => {
                    tracing::debug!(
                        proposer = ?proposal.message.proposer,
                        "Publishing proposal"
                    );
                }
                SignedConsensusMsg::Vote(vote) => {
                    tracing::debug!(
                        validator = ?vote.message.validator_address,
                        vote_type = ?vote.message.r#type,
                        "Publishing vote"
                    );
                }
            }
            let event = create_event(msg);
            output_queue.push_back(event);
            Ok(resume.resume_with(()))
        }
        // Request the application to build a value for consensus to run on.
        Effect::GetValue(height, round, _, resume) => {
            tracing::debug!(
                height = %height,
                round = %round,
                "Requesting value"
            );
            assert!(round.is_defined(), "Round is expected to be defined");
            output_queue.push_back(ConsensusEvent::RequestProposal {
                height: height.as_u64(),
                round: round.as_u32().expect("Round is not Nil"),
            });
            Ok(resume.resume_with(()))
        }
        // Notifies the application that consensus has decided on a value.
        Effect::Decide(cert, _, resume) => {
            tracing::info!(
                height = %cert.height,
                value = ?cert.value_id,
                "Consensus decided on value"
            );
            output_queue.push_back(ConsensusEvent::Decision {
                height: cert.height.as_u64(),
                round: cert.round.as_u32().expect("Round is not Nil"),
                value: cert.value_id.clone(),
            });
            // We append the decision to the WAL so that in case of a crash,
            // we know this height has been finalized.
            wal.append(WalEntry::Decision {
                height: cert.height.as_u64(),
                value: cert.value_id,
            });
            Ok(resume.resume_with(()))
        }
        // Sign a vote.
        Effect::SignVote(vote, resume) => {
            tracing::debug!(
                vote_type = ?vote.r#type,
                "Signing vote (skipping)"
            );
            Ok(resume.resume_with(SignedMessage::new(vote, Signature::from_bytes([0; 64]))))
        }
        // Sign a proposal.
        Effect::SignProposal(proposal, resume) => {
            tracing::debug!(
                proposal = ?proposal,
                "Signing proposal (skipping)"
            );
            Ok(resume.resume_with(SignedMessage::new(proposal, Signature::from_bytes([0; 64]))))
        }
        // Verify a signature.
        Effect::VerifySignature(msg, _, resume) => {
            tracing::debug!(
                msg = ?msg.message,
                "Verifying signature (skipping)"
            );
            Ok(resume.resume_with(true))
        }
        // Restream a proposal.
        Effect::RestreamProposal(height, round, valid_round, address, value_id, resume) => {
            tracing::debug!(
                height = %height,
                round = %round,
                valid_round = %valid_round,
                address = %address,
                value_id = ?value_id,
                "Restreaming proposal"
            );
            output_queue.push_back(ConsensusEvent::Gossip(NetworkMessage::Proposal(
                SignedProposal {
                    proposal: Proposal {
                        height: height.as_u64(),
                        round: round.into(),
                        pol_round: valid_round.into(),
                        proposer: address.into_inner(),
                        value: value_id,
                    },
                    signature: Signature::from_bytes([0; 64]), // TODO: Replace with real signature
                },
            )));
            Ok(resume.resume_with(()))
        }
        // Republish a vote.
        Effect::RepublishVote(vote, resume) => {
            tracing::debug!(
                vote = ?vote,
                "Republishing vote"
            );
            output_queue.push_back(ConsensusEvent::Gossip(NetworkMessage::Vote(
                convert_vote_out(vote),
            )));
            Ok(resume.resume_with(()))
        }
        // Timeout management.
        Effect::ScheduleTimeout(timeout, resume) => {
            timeout_manager.schedule_timeout(timeout);
            Ok(resume.resume_with(()))
        }
        Effect::CancelTimeout(timeout, resume) => {
            timeout_manager.cancel_timeout(timeout);
            Ok(resume.resume_with(()))
        }
        Effect::CancelAllTimeouts(resume) => {
            timeout_manager.cancel_all_timeouts();
            Ok(resume.resume_with(()))
        }
        Effect::ResetTimeouts(resume) => {
            timeout_manager.reset_timeouts();
            Ok(resume.resume_with(()))
        }
        // WAL management.
        Effect::WalAppend(msg, resume) => {
            wal.append(msg.into());
            Ok(resume.resume_with(()))
        }
        // --------------------------------------------------------------
        // Effects we don't care about. They're not relevant to Starknet.
        // --------------------------------------------------------------
        // Sync a value.
        Effect::SyncValue(_, resume) => Ok(resume.resume_with(())),
        // Verify a commit certificate.
        Effect::VerifyCommitCertificate(_, _, _, resume) => Ok(resume.resume_with(Ok(()))),
        // Verify a polka certificate.
        Effect::VerifyPolkaCertificate(_, _, _, resume) => Ok(resume.resume_with(Ok(()))),
        // Verify a round certificate.
        Effect::VerifyRoundCertificate(_, _, _, resume) => Ok(resume.resume_with(Ok(()))),
        // Publish a liveness message to peers.
        Effect::PublishLivenessMsg(_, resume) => Ok(resume.resume_with(())),
        // Republish a round certificate.
        Effect::RepublishRoundCertificate(_, resume) => Ok(resume.resume_with(())),
        // Extend a vote.
        Effect::ExtendVote(_, _, _, resume) => Ok(resume.resume_with(None)),
        // Verify a vote extension.
        Effect::VerifyVoteExtension(_, _, _, _, _, resume) => Ok(resume.resume_with(Ok(()))),
    }
}

/// Convert a signed consensus message to a consensus event.
fn create_event<
    V: crate::ValuePayload + 'static,
    A: crate::ValidatorAddress + 'static,
    P: ProposerSelector<A> + Send + Sync + 'static,
>(
    msg: SignedConsensusMsg<MalachiteContext<V, A, P>>,
) -> ConsensusEvent<V, A> {
    use crate::NetworkMessage;

    let network_msg = match msg {
        SignedConsensusMsg::Proposal(proposal) => NetworkMessage::Proposal(crate::SignedProposal {
            proposal: proposal.message.into(),
            signature: proposal.signature,
        }),
        SignedConsensusMsg::Vote(vote) => NetworkMessage::Vote(convert_vote_out(vote)),
    };

    ConsensusEvent::Gossip(network_msg)
}

/// Convert a signed vote to a Malachite signed vote.
fn convert_vote_in<
    V: crate::ValuePayload + 'static,
    A: crate::ValidatorAddress + 'static,
    P: ProposerSelector<A> + Send + Sync + 'static,
>(
    vote: SignedVote<V, A>,
) -> malachite_types::SignedVote<MalachiteContext<V, A, P>> {
    malachite_types::SignedVote::new(vote.vote.into(), vote.signature)
}

/// Convert a Malachite signed vote to a signed vote.
fn convert_vote_out<
    V: crate::ValuePayload + 'static,
    A: crate::ValidatorAddress + 'static,
    P: ProposerSelector<A> + Send + Sync + 'static,
>(
    vote: malachite_types::SignedVote<MalachiteContext<V, A, P>>,
) -> SignedVote<V, A> {
    SignedVote {
        vote: vote.message.into(),
        signature: vote.signature,
    }
}

/// A scheduled timeout for a consensus event.
///
/// Malachite requests us to schedule timeouts for various consensus events.
struct ScheduledTimeout {
    timeout: Timeout,
    due: Instant,
}

impl PartialEq for ScheduledTimeout {
    fn eq(&self, other: &Self) -> bool {
        self.timeout == other.timeout && self.due == other.due
    }
}

impl Eq for ScheduledTimeout {}

impl Ord for ScheduledTimeout {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // We want a min-heap, so reverse ordering by due.
        other.due.cmp(&self.due)
    }
}

impl PartialOrd for ScheduledTimeout {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// A manager for consensus timeouts.
///
/// Keeps track of all the scheduled timeouts, as well as the timeout values for
/// each timeout kind.
struct TimeoutManager {
    timeouts: BinaryHeap<ScheduledTimeout>,
    timeout_values: TimeoutValues,
}

impl TimeoutManager {
    /// Schedule a new timeout.
    pub fn schedule_timeout(&mut self, timeout: Timeout) {
        let due = Instant::now() + self.timeout_values.get(timeout.kind);
        self.timeouts.push(ScheduledTimeout { timeout, due });

        tracing::debug!(
            timeout = ?timeout,
            due = ?due,
            "Scheduled timeout"
        );
    }

    /// Cancel a timeout.
    pub fn cancel_timeout(&mut self, timeout: Timeout) {
        self.timeouts = self
            .timeouts
            .drain()
            .filter(|st| st.timeout != timeout)
            .collect();

        tracing::debug!(
            timeout = ?timeout,
            "Cancelled timeout"
        );
    }

    /// Cancel all timeouts.
    pub fn cancel_all_timeouts(&mut self) {
        self.timeouts.clear();

        tracing::debug!("Cancelled all timeouts");
    }

    /// Reset all timeouts.
    pub fn reset_timeouts(&mut self) {
        let now = Instant::now();
        let mut reset = BinaryHeap::new();

        // Drain all timeouts and update their due times
        for mut scheduled_timeout in self.timeouts.drain() {
            scheduled_timeout.due = now + self.timeout_values.get(scheduled_timeout.timeout.kind);
            reset.push(scheduled_timeout);
        }

        self.timeouts = reset;

        tracing::debug!("Reset all timeouts");
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use malachite_types::{Round, Timeout, TimeoutKind};

    use crate::config::TimeoutValues;
    use crate::internal::TimeoutManager;

    /// Tests basic timeout scheduling and firing behavior.
    /// Verifies that timeouts are scheduled correctly and the shortest timeout
    /// fires first.
    #[tokio::test]
    async fn test_timeout_manager_scheduling() {
        let timeout_values = TimeoutValues {
            propose: Duration::from_millis(100),
            prevote: Duration::from_millis(200),
            precommit: Duration::from_millis(300),
            rebroadcast: Duration::from_millis(400),
        };

        let mut manager = TimeoutManager {
            timeouts: std::collections::BinaryHeap::new(),
            timeout_values,
        };

        // Schedule timeouts with different durations
        let timeout1 = Timeout::new(Round::Some(1), TimeoutKind::Propose);
        let timeout2 = Timeout::new(Round::Some(2), TimeoutKind::Prevote);
        let timeout3 = Timeout::new(Round::Some(3), TimeoutKind::Precommit);

        manager.schedule_timeout(timeout1);
        manager.schedule_timeout(timeout2);
        manager.schedule_timeout(timeout3);

        // Verify all timeouts are scheduled
        assert_eq!(manager.timeouts.len(), 3);

        // Wait for the shortest timeout to fire
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Check that the propose timeout (shortest) has fired
        let now = tokio::time::Instant::now();
        if let Some(next) = manager.timeouts.peek() {
            if now >= next.due {
                let scheduled_timeout = manager.timeouts.pop().expect("No timeout to pop");
                assert_eq!(scheduled_timeout.timeout.kind, TimeoutKind::Propose);
                assert_eq!(scheduled_timeout.timeout.round, Round::Some(1));
            }
        }

        // Verify remaining timeouts
        assert_eq!(manager.timeouts.len(), 2);
    }

    /// Tests timeout cancellation functionality.
    /// Verifies that specific timeouts can be cancelled and all timeouts can be
    /// cleared.
    #[tokio::test]
    async fn test_timeout_manager_cancellation() {
        let timeout_values = TimeoutValues::default();
        let mut manager = TimeoutManager {
            timeouts: std::collections::BinaryHeap::new(),
            timeout_values,
        };

        let timeout1 = Timeout::new(Round::Some(1), TimeoutKind::Propose);
        let timeout2 = Timeout::new(Round::Some(2), TimeoutKind::Prevote);
        let timeout3 = Timeout::new(Round::Some(3), TimeoutKind::Precommit);

        // Schedule timeouts
        manager.schedule_timeout(timeout1);
        manager.schedule_timeout(timeout2);
        manager.schedule_timeout(timeout3);

        assert_eq!(manager.timeouts.len(), 3);

        // Cancel a specific timeout
        manager.cancel_timeout(timeout2);
        assert_eq!(manager.timeouts.len(), 2);

        // Verify the cancelled timeout is not present
        for scheduled_timeout in manager.timeouts.iter() {
            assert_ne!(scheduled_timeout.timeout, timeout2);
        }

        // Cancel all timeouts
        manager.cancel_all_timeouts();
        assert_eq!(manager.timeouts.len(), 0);
    }

    /// Tests timeout reset functionality.
    /// Verifies that all timeouts can be reset with new due times while
    /// preserving their count.
    #[tokio::test]
    async fn test_timeout_manager_reset() {
        let timeout_values = TimeoutValues {
            propose: Duration::from_millis(100),
            prevote: Duration::from_millis(200),
            precommit: Duration::from_millis(300),
            rebroadcast: Duration::from_millis(400),
        };

        let mut manager = TimeoutManager {
            timeouts: std::collections::BinaryHeap::new(),
            timeout_values,
        };

        let timeout1 = Timeout::new(Round::Some(1), TimeoutKind::Propose);
        let timeout2 = Timeout::new(Round::Some(2), TimeoutKind::Prevote);

        // Schedule timeouts
        manager.schedule_timeout(timeout1);
        manager.schedule_timeout(timeout2);

        assert_eq!(manager.timeouts.len(), 2);

        // Wait a bit to ensure time has passed
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Reset timeouts
        manager.reset_timeouts();

        // Verify timeouts are still present but with updated due times
        assert_eq!(manager.timeouts.len(), 2);

        // Verify the due times are in the future
        let now = tokio::time::Instant::now();
        for scheduled_timeout in manager.timeouts.iter() {
            assert!(scheduled_timeout.due > now);
        }
    }

    /// Tests timeout ordering and firing sequence.
    /// Verifies that timeouts fire in the correct order (shortest duration
    /// first) regardless of scheduling order.
    #[tokio::test]
    async fn test_timeout_manager_ordering() {
        let timeout_values = TimeoutValues {
            propose: Duration::from_millis(300),   // Longest
            prevote: Duration::from_millis(100),   // Shortest
            precommit: Duration::from_millis(200), // Medium
            rebroadcast: Duration::from_millis(400),
        };

        let mut manager = TimeoutManager {
            timeouts: std::collections::BinaryHeap::new(),
            timeout_values,
        };

        // Schedule timeouts in reverse order
        let timeout1 = Timeout::new(Round::Some(1), TimeoutKind::Propose); // 300ms
        let timeout2 = Timeout::new(Round::Some(2), TimeoutKind::Prevote); // 100ms
        let timeout3 = Timeout::new(Round::Some(3), TimeoutKind::Precommit); // 200ms

        manager.schedule_timeout(timeout1);
        manager.schedule_timeout(timeout2);
        manager.schedule_timeout(timeout3);

        // First should be prevote (100ms)
        tokio::time::sleep(Duration::from_millis(150)).await;
        if let Some(scheduled_timeout) = manager.timeouts.pop() {
            assert_eq!(scheduled_timeout.timeout.kind, TimeoutKind::Prevote);
            assert_eq!(scheduled_timeout.timeout.round, Round::Some(2));
        }

        // Second should be precommit (200ms)
        tokio::time::sleep(Duration::from_millis(100)).await;
        if let Some(scheduled_timeout) = manager.timeouts.pop() {
            assert_eq!(scheduled_timeout.timeout.kind, TimeoutKind::Precommit);
            assert_eq!(scheduled_timeout.timeout.round, Round::Some(3));
        }

        // Third should be propose (300ms)
        tokio::time::sleep(Duration::from_millis(100)).await;
        if let Some(scheduled_timeout) = manager.timeouts.pop() {
            assert_eq!(scheduled_timeout.timeout.kind, TimeoutKind::Propose);
            assert_eq!(scheduled_timeout.timeout.round, Round::Some(1));
        }

        assert_eq!(manager.timeouts.len(), 0);
    }

    /// Tests edge cases and error conditions.
    /// Verifies behavior with non-existent timeouts, empty states, and
    /// duplicate timeouts.
    #[tokio::test]
    async fn test_timeout_manager_edge_cases() {
        let timeout_values = TimeoutValues::default();
        let mut manager = TimeoutManager {
            timeouts: std::collections::BinaryHeap::new(),
            timeout_values,
        };

        // Test cancelling non-existent timeout
        let non_existent_timeout = Timeout::new(Round::Some(999), TimeoutKind::Propose);
        manager.cancel_timeout(non_existent_timeout);
        assert_eq!(manager.timeouts.len(), 0);

        // Test cancelling all timeouts when empty
        manager.cancel_all_timeouts();
        assert_eq!(manager.timeouts.len(), 0);

        // Test resetting timeouts when empty
        manager.reset_timeouts();
        assert_eq!(manager.timeouts.len(), 0);

        // Test scheduling same timeout multiple times
        let timeout = Timeout::new(Round::Some(1), TimeoutKind::Propose);
        manager.schedule_timeout(timeout);
        manager.schedule_timeout(timeout);
        manager.schedule_timeout(timeout);

        assert_eq!(manager.timeouts.len(), 3);

        // Cancel all instances of this timeout (correct behavior)
        manager.cancel_timeout(timeout);
        assert_eq!(manager.timeouts.len(), 0);

        // Test scheduling different timeouts and cancelling one
        let timeout1 = Timeout::new(Round::Some(1), TimeoutKind::Propose);
        let timeout2 = Timeout::new(Round::Some(2), TimeoutKind::Prevote);

        manager.schedule_timeout(timeout1);
        manager.schedule_timeout(timeout2);

        assert_eq!(manager.timeouts.len(), 2);

        // Cancel one specific timeout
        manager.cancel_timeout(timeout1);
        assert_eq!(manager.timeouts.len(), 1);

        // Verify the remaining timeout is the correct one
        let remaining = manager.timeouts.peek().unwrap();
        assert_eq!(remaining.timeout, timeout2);
    }
}
