use malachite_consensus::{Input, SignedConsensusMsg, WalEntry as MalachiteWalEntry};
use malachite_types::Height as _;

use super::malachite::{Height, MalachiteContext};
use crate::internal::ValidatorAddress;
use crate::wal::WalEntry;
use crate::{ProposerSelector, SignedProposal, SignedVote};

// This is necessary because unfortunately most malachite types are not
// serializable.
impl<
        V: crate::ValuePayload,
        A: crate::ValidatorAddress,
        P: ProposerSelector<A> + Send + Sync + 'static,
    > From<MalachiteWalEntry<MalachiteContext<V, A, P>>> for WalEntry<V, A>
{
    fn from(entry: MalachiteWalEntry<MalachiteContext<V, A, P>>) -> Self {
        match entry {
            MalachiteWalEntry::ConsensusMsg(msg) => {
                let signature = *msg.signature();
                match msg {
                    SignedConsensusMsg::Proposal(proposal) => {
                        let proposal = proposal.message;
                        WalEntry::SignedProposal(SignedProposal {
                            proposal: proposal.into(),
                            signature,
                        })
                    }
                    SignedConsensusMsg::Vote(vote) => {
                        let vote = vote.message;
                        WalEntry::SignedVote(SignedVote {
                            vote: vote.into(),
                            signature,
                        })
                    }
                }
            }
            MalachiteWalEntry::Timeout(timeout) => {
                use malachite_types::TimeoutKind as Kind;
                WalEntry::Timeout {
                    kind: match timeout.kind {
                        Kind::Propose => "propose",
                        Kind::Prevote => "prevote",
                        Kind::Precommit => "precommit",
                        Kind::Rebroadcast => "rebroadcast",
                    }
                    .to_string(),
                    round: timeout.round.into(),
                }
            }
            MalachiteWalEntry::ProposedValue(proposed_value) => WalEntry::ProposedValue {
                height: proposed_value.height.as_u64(),
                round: proposed_value.round.into(),
                valid_round: proposed_value.valid_round.into(),
                proposer: proposed_value.proposer.into_inner(),
                value: proposed_value.value.into_inner(),
                validity: proposed_value.validity.is_valid(),
            },
        }
    }
}

impl<V, A> From<malachite_types::Timeout> for WalEntry<V, A> {
    fn from(timeout: malachite_types::Timeout) -> Self {
        use malachite_types::TimeoutKind as Kind;
        WalEntry::Timeout {
            kind: match timeout.kind {
                Kind::Propose => "propose",
                Kind::Prevote => "prevote",
                Kind::Precommit => "precommit",
                Kind::Rebroadcast => "rebroadcast",
            }
            .to_string(),
            round: timeout.round.into(),
        }
    }
}

/// Convert a WAL entry to the corresponding malachite Input.
pub(crate) fn convert_wal_entry_to_input<
    V: crate::ValuePayload,
    A: crate::ValidatorAddress,
    P: ProposerSelector<A> + Send + Sync + 'static,
>(
    entry: WalEntry<V, A>,
) -> Input<MalachiteContext<V, A, P>> {
    match entry {
        WalEntry::SignedProposal(proposal) => {
            tracing::debug!(
                value = ?proposal.proposal.value,
                from = ?proposal.proposal.proposer,
                height = %proposal.proposal.height,
                round = %proposal.proposal.round,
                "Recovering proposal from WAL"
            );
            let signed_msg =
                malachite_types::SignedProposal::new(proposal.proposal.into(), proposal.signature);
            Input::Proposal(signed_msg)
        }
        WalEntry::SignedVote(vote) => {
            tracing::debug!(
                vote_type = ?vote.vote.r#type,
                from = ?vote.vote.validator_address,
                height = %vote.vote.height,
                round = %vote.vote.round,
                "Recovering vote from WAL"
            );
            let signed_vote = malachite_types::SignedVote::new(vote.vote.into(), vote.signature);
            Input::Vote(signed_vote)
        }
        WalEntry::Timeout { kind, round } => {
            let timeout_kind = match kind.as_str() {
                "propose" => malachite_types::TimeoutKind::Propose,
                "prevote" => malachite_types::TimeoutKind::Prevote,
                "precommit" => malachite_types::TimeoutKind::Precommit,
                "rebroadcast" => malachite_types::TimeoutKind::Rebroadcast,
                _ => unreachable!(),
            };
            let timeout = malachite_types::Timeout::new(round.into(), timeout_kind);
            tracing::debug!(
                timeout = ?timeout,
                "Recovering timeout from WAL"
            );
            Input::TimeoutElapsed(timeout)
        }
        WalEntry::ProposedValue {
            height,
            round,
            valid_round,
            proposer,
            value,
            validity,
        } => {
            tracing::debug!(
                height = %height,
                round = %round,
                value = ?value,
                "Recovering proposed value from WAL"
            );
            let proposed_value = malachite_consensus::ProposedValue {
                height: Height::new(height),
                round: round.into(),
                valid_round: valid_round.into(),
                proposer: ValidatorAddress::from(proposer.clone()),
                value: value.into(),
                validity: if validity {
                    malachite_types::Validity::Valid
                } else {
                    malachite_types::Validity::Invalid
                },
            };
            // `ValueOrigin::Consensus` is always correct here because every
            // `WalEntry::ProposedValue` that can land in our WAL originated
            // from the consensus protocol — never from sync. The chain of
            // reasoning:
            //
            // 1. We configure malachite with `ValuePayload::ProposalOnly` (see
            //    `create_consensus` in `lib.rs`), so malachite's `parts_only()` check is
            //    always false.
            //
            // 2. We never feed `Input::ProposedValue` into malachite during live operation:
            //    the `ConsensusCommand` enum only has `Vote`, `Proposal`, `Propose`, and
            //    `StartHeight` variants, none of which map to `Input::ProposedValue`. The
            //    function here is the *only* place that constructs that input, and only on
            //    WAL replay.
            //
            // 3. We deliberately stub out every sync-subprotocol effect (`SyncValue`,
            //    `VerifyCommitCertificate`, `VerifyPolkaCertificate`,
            //    `VerifyRoundCertificate`, `PublishLivenessMsg`,
            //    `RepublishRoundCertificate`) as no-ops in `handle_effect` — pathfinder
            //    uses malachite in a sync-unaware way and runs its own sync logic
            //    separately.
            //
            // 4. Given (2), the only path inside malachite that writes a
            //    `WalEntry::ProposedValue` for us is `on_propose` (i.e. when *this* node is
            //    the local proposer for the round). The other writer, `on_proposed_value`,
            //    only fires in response to an external `Input::ProposedValue(_, origin)` —
            //    which by (2) we never emit, regardless of `origin`.
            //
            // Therefore every persisted entry replayed here was written as
            // a locally-proposed consensus value, and replaying it as
            // `ValueOrigin::Consensus` exactly reproduces the original
            // write-time semantics.
            Input::ProposedValue(proposed_value, malachite_types::ValueOrigin::Consensus)
        }
        _ => unreachable!(),
    }
}
