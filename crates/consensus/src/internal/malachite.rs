use std::fmt::{Debug, Display};
use std::marker::PhantomData;

use malachite_signing_ed25519::Ed25519;
use malachite_types::{Height as _, NilOrVal, Round, SignedExtension, ValueId, VoteType};
use serde::{Deserialize, Serialize};

use crate::{ProposerSelector, PublicKey, VotingPower};

/// A validator address used to identify participants in the consensus protocol.
#[derive(Ord, PartialOrd, Eq, PartialEq, Clone, Copy, Default, Hash, Serialize, Deserialize)]
pub(super) struct ValidatorAddress<A>(A);

impl<A> ValidatorAddress<A> {
    pub fn into_inner(self) -> A {
        self.0
    }
}

impl<A: crate::ValidatorAddress> From<A> for ValidatorAddress<A> {
    fn from(address: A) -> Self {
        Self(address)
    }
}

impl<A: crate::ValidatorAddress> malachite_types::Address for ValidatorAddress<A> {}

impl<A: Display> Display for ValidatorAddress<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl<A: Debug> Debug for ValidatorAddress<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

/// The height of a block in the consensus protocol.
#[derive(Ord, PartialOrd, Eq, PartialEq, Clone, Copy, Default)]
pub(super) struct Height(u64);

impl Height {
    pub fn new(height: impl Into<u64>) -> Self {
        Self(height.into())
    }
}

impl Display for Height {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Debug for Height {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl malachite_types::Height for Height {
    const ZERO: Self = Self(0);
    const INITIAL: Self = Self(0);

    fn increment_by(&self, n: u64) -> Self {
        Self(self.0 + n)
    }

    fn decrement_by(&self, n: u64) -> Option<Self> {
        self.0.checked_sub(n).map(Self)
    }

    fn as_u64(&self) -> u64 {
        self.0
    }

    fn increment(&self) -> Self {
        Self(self.0 + 1)
    }

    fn decrement(&self) -> Option<Self> {
        self.0.checked_sub(1).map(Self)
    }
}

impl From<Round> for crate::Round {
    fn from(value: Round) -> Self {
        match value {
            Round::Nil => crate::Round::nil(),
            Round::Some(round) => crate::Round::new(round),
        }
    }
}

impl From<crate::Round> for Round {
    fn from(value: crate::Round) -> Self {
        match value.0 {
            Some(round) => Round::Some(round),
            None => Round::Nil,
        }
    }
}

/// A proposal for a block value in a consensus round.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Proposal<V, A> {
    pub height: Height,
    pub round: Round,
    pub value: ConsensusValue<V>,
    pub pol_round: Round,
    pub proposer: ValidatorAddress<A>,
}

impl<V, A> From<crate::Proposal<V, A>> for Proposal<V, A> {
    fn from(proposal: crate::Proposal<V, A>) -> Self {
        Self {
            height: Height::new(proposal.height),
            round: proposal.round.into(),
            value: ConsensusValue(proposal.value),
            pol_round: proposal.pol_round.into(),
            proposer: ValidatorAddress(proposal.proposer),
        }
    }
}

impl<V, A> From<Proposal<V, A>> for crate::Proposal<V, A> {
    fn from(proposal: Proposal<V, A>) -> Self {
        Self {
            height: proposal.height.as_u64(),
            round: proposal.round.into(),
            value: proposal.value.into_inner(),
            pol_round: proposal.pol_round.into(),
            proposer: proposal.proposer.into_inner(),
        }
    }
}

impl<
        A: crate::ValidatorAddress + 'static,
        V: crate::ValuePayload + 'static,
        P: ProposerSelector<A> + Send + Sync + 'static,
    > malachite_types::Proposal<MalachiteContext<V, A, P>> for Proposal<V, A>
{
    fn height(&self) -> Height {
        self.height
    }

    fn round(&self) -> malachite_types::Round {
        self.round
    }

    fn value(&self) -> &ConsensusValue<V> {
        &self.value
    }

    fn take_value(self) -> ConsensusValue<V> {
        self.value
    }

    fn pol_round(&self) -> malachite_types::Round {
        self.pol_round
    }

    fn validator_address(&self) -> &ValidatorAddress<A> {
        &self.proposer
    }
}

/// A proposal part for a block value in a consensus round.
/// Note: This is not used anywhere, hence the empty struct.
#[derive(Debug, Clone, Eq, PartialEq)]
pub(super) struct ProposalPart;

impl<
        V: crate::ValuePayload + 'static,
        A: crate::ValidatorAddress + 'static,
        P: ProposerSelector<A> + Send + Sync + 'static,
    > malachite_types::ProposalPart<MalachiteContext<V, A, P>> for ProposalPart
{
    fn is_first(&self) -> bool {
        false
    }

    fn is_last(&self) -> bool {
        false
    }
}

/// A validator in the consensus protocol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Validator<A> {
    pub address: ValidatorAddress<A>,
    pub public_key: PublicKey,
    pub voting_power: VotingPower,
}

impl<A> From<crate::Validator<A>> for Validator<A> {
    fn from(validator: crate::Validator<A>) -> Self {
        Self {
            address: ValidatorAddress(validator.address),
            public_key: validator.public_key,
            voting_power: validator.voting_power,
        }
    }
}

impl<
        V: crate::ValuePayload + 'static,
        A: crate::ValidatorAddress + 'static,
        P: ProposerSelector<A> + Send + Sync + 'static,
    > malachite_types::Validator<MalachiteContext<V, A, P>> for Validator<A>
{
    fn address(&self) -> &ValidatorAddress<A> {
        &self.address
    }

    fn public_key(&self) -> &PublicKey {
        &self.public_key
    }

    fn voting_power(&self) -> malachite_types::VotingPower {
        self.voting_power
    }
}

/// A validator set for the consensus protocol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ValidatorSet<V, A> {
    pub validators: Vec<Validator<A>>,
    _phantom_v: PhantomData<V>,
}

impl<V, A> From<crate::ValidatorSet<A>> for ValidatorSet<V, A> {
    fn from(validator_set: crate::ValidatorSet<A>) -> Self {
        Self {
            validators: validator_set
                .validators
                .into_iter()
                .map(Into::into)
                .collect(),
            _phantom_v: PhantomData,
        }
    }
}

impl<
        V: crate::ValuePayload + 'static,
        A: crate::ValidatorAddress + 'static,
        P: ProposerSelector<A> + Send + Sync + 'static,
    > malachite_types::ValidatorSet<MalachiteContext<V, A, P>> for ValidatorSet<V, A>
{
    fn count(&self) -> usize {
        self.validators.len()
    }

    fn total_voting_power(&self) -> malachite_types::VotingPower {
        self.validators.iter().map(|v| v.voting_power).sum()
    }

    fn get_by_address(&self, address: &ValidatorAddress<A>) -> Option<&Validator<A>> {
        self.validators.iter().find(|v| &v.address == address)
    }

    fn get_by_index(&self, index: usize) -> Option<&Validator<A>> {
        self.validators.get(index)
    }
}

/// The value for the consensus protocol.
#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub(super) struct ConsensusValue<V>(V);

impl<V> ConsensusValue<V> {
    pub fn into_inner(self) -> V {
        self.0
    }
}

impl<V> From<V> for ConsensusValue<V> {
    fn from(value: V) -> Self {
        Self(value)
    }
}

impl<V: crate::ValuePayload + 'static> malachite_types::Value for ConsensusValue<V> {
    type Id = V;

    fn id(&self) -> Self::Id {
        self.0.clone()
    }
}

/// A vote for a block value in a consensus round.
#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub(super) struct Vote<V, A> {
    pub r#type: VoteType,
    pub height: Height,
    pub round: Round,
    pub value: NilOrVal<V>,
    pub validator_address: ValidatorAddress<A>,
}

impl<V, A> From<crate::Vote<V, A>> for Vote<V, A> {
    fn from(vote: crate::Vote<V, A>) -> Self {
        Self {
            r#type: match vote.r#type {
                crate::VoteType::Prevote => VoteType::Prevote,
                crate::VoteType::Precommit => VoteType::Precommit,
            },
            height: Height::new(vote.height),
            round: vote.round.into(),
            value: match vote.value {
                Some(value) => NilOrVal::Val(value),
                None => NilOrVal::Nil,
            },
            validator_address: ValidatorAddress(vote.validator_address),
        }
    }
}

impl<V, A> From<Vote<V, A>> for crate::Vote<V, A> {
    fn from(vote: Vote<V, A>) -> Self {
        Self {
            r#type: match vote.r#type {
                VoteType::Prevote => crate::VoteType::Prevote,
                VoteType::Precommit => crate::VoteType::Precommit,
            },
            height: vote.height.as_u64(),
            round: vote.round.into(),
            value: match vote.value {
                NilOrVal::Val(value) => Some(value),
                NilOrVal::Nil => None,
            },
            validator_address: vote.validator_address.into_inner(),
        }
    }
}

impl<
        V: crate::ValuePayload + 'static,
        A: crate::ValidatorAddress + 'static,
        P: ProposerSelector<A> + Send + Sync + 'static,
    > malachite_types::Vote<MalachiteContext<V, A, P>> for Vote<V, A>
{
    fn height(&self) -> Height {
        self.height
    }

    fn round(&self) -> Round {
        self.round
    }

    fn value(&self) -> &NilOrVal<V> {
        &self.value
    }

    fn take_value(self) -> NilOrVal<V> {
        self.value
    }

    fn vote_type(&self) -> VoteType {
        self.r#type
    }

    fn validator_address(&self) -> &ValidatorAddress<A> {
        &self.validator_address
    }

    fn extension(&self) -> Option<&SignedExtension<MalachiteContext<V, A, P>>> {
        None
    }

    fn take_extension(&mut self) -> Option<SignedExtension<MalachiteContext<V, A, P>>> {
        None
    }

    fn extend(self, _extension: SignedExtension<MalachiteContext<V, A, P>>) -> Self {
        self
    }
}

/// The malachite context for the consensus logic.
pub(super) struct MalachiteContext<
    V: Send + Sync + 'static,
    A: crate::ValidatorAddress + Send + Sync + 'static,
    P: ProposerSelector<A> + Send + Sync + 'static,
> {
    proposer_selector: P,
    _phantom_a: PhantomData<A>,
    _phantom_v: PhantomData<V>,
}

impl<
        V: Send + Sync + 'static,
        A: crate::ValidatorAddress + Send + Sync + 'static,
        P: ProposerSelector<A> + Send + Sync + 'static,
    > MalachiteContext<V, A, P>
{
    pub fn new(proposer_selector: P) -> Self {
        Self {
            proposer_selector,
            _phantom_a: PhantomData,
            _phantom_v: PhantomData,
        }
    }
}

impl<
        V: Send + Sync + 'static,
        A: crate::ValidatorAddress + Send + Sync + 'static,
        P: ProposerSelector<A> + Send + Sync + 'static,
    > Clone for MalachiteContext<V, A, P>
where
    P: Clone,
{
    fn clone(&self) -> Self {
        Self {
            proposer_selector: self.proposer_selector.clone(),
            _phantom_a: PhantomData,
            _phantom_v: PhantomData,
        }
    }
}

impl<
        V: crate::ValuePayload + 'static,
        A: crate::ValidatorAddress + 'static,
        P: ProposerSelector<A> + Send + Sync + 'static,
    > malachite_types::Context for MalachiteContext<V, A, P>
{
    type Address = ValidatorAddress<A>;
    type Height = Height;

    type ProposalPart = ProposalPart;
    type Proposal = Proposal<V, A>;

    type Validator = Validator<A>;
    type ValidatorSet = ValidatorSet<V, A>;

    type Value = ConsensusValue<V>;

    type Vote = Vote<V, A>;

    type Extension = Vec<u8>;

    type SigningScheme = Ed25519;

    fn select_proposer<'a>(
        &self,
        validator_set: &'a Self::ValidatorSet,
        height: Self::Height,
        round: Round,
    ) -> &'a Self::Validator {
        let round = round.as_u32().expect("round is not nil");

        // Convert the malachite validator set to our public API validator set
        let public_validator_set = crate::ValidatorSet {
            validators: validator_set
                .validators
                .iter()
                .map(|v| crate::Validator {
                    address: v.address.clone().into_inner(),
                    public_key: v.public_key,
                    voting_power: v.voting_power,
                })
                .collect(),
        };

        // Malachite's [`malachite_types::Context::select_proposer`] is infallible, so
        // we cannot propagate an error from here. The proposer set must be
        // pre-fetched at a fallible boundary when a height is started (see
        // `consensus_task.rs`), which makes this a cache hit in practice. Reaching
        // the failure arm therefore indicates a fatal infrastructure error.
        let selected_validator = self
            .proposer_selector
            .select_proposer(&public_validator_set, height.as_u64(), round)
            .expect(
                "proposer selection failed: the proposer set must be pre-fetched at StartHeight",
            );

        // Find the corresponding validator in the malachite validator set
        let proposer = validator_set
            .validators
            .iter()
            .find(|v| v.address.clone().into_inner() == selected_validator.address)
            .expect("selected validator not found in validator set");

        tracing::debug!(
            proposer = ?proposer,
            height = ?height,
            round = ?round,
            "Selected proposer for round"
        );
        proposer
    }

    fn new_proposal(
        &self,
        height: Self::Height,
        round: Round,
        value: Self::Value,
        pol_round: Round,
        address: Self::Address,
    ) -> Self::Proposal {
        tracing::debug!(
            validator = %address,
            value = ?value,
            height = %height,
            round = %round,
            pol_round = %pol_round,
            "Creating new proposal"
        );
        Proposal {
            height,
            round,
            value,
            proposer: address,
            pol_round,
        }
    }

    fn new_prevote(
        &self,
        height: Self::Height,
        round: Round,
        value_id: NilOrVal<ValueId<Self>>,
        address: Self::Address,
    ) -> Self::Vote {
        tracing::debug!(
            validator = %address,
            value = ?value_id,
            height = %height,
            round = %round,
            "Creating new prevote"
        );
        Vote {
            r#type: VoteType::Prevote,
            height,
            round,
            validator_address: address,
            value: value_id,
        }
    }

    fn new_precommit(
        &self,
        height: Self::Height,
        round: Round,
        value_id: NilOrVal<ValueId<Self>>,
        address: Self::Address,
    ) -> Self::Vote {
        tracing::debug!(
            validator = %address,
            value = ?value_id,
            height = %height,
            round = %round,
            "Creating new precommit"
        );
        Vote {
            r#type: VoteType::Precommit,
            height,
            round,
            validator_address: address,
            value: value_id,
        }
    }
}
