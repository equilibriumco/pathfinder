//! Resolves the expected proposer for an incoming proposal by delegating to
//! **the same** proposer-selection code the consensus engine uses, so the
//! validator's "expected proposer" and Malachite's selection cannot diverge.

use pathfinder_common::ContractAddress;
use pathfinder_consensus::{ProposerSelector, ValidatorSetProvider};
use pathfinder_validator::proposer::ExpectedProposer;

use super::fetch_proposers::L2ProposerSelector;
use super::fetch_validators::L2ValidatorSetProvider;

/// An [`ExpectedProposer`] whose answer is, by construction, identical to
/// what the consensus engine would select for the same `(height, round)`.
///
/// It composes the same [`L2ProposerSelector`] and [`L2ValidatorSetProvider`]
/// the engine consumes, and its body literally calls
/// [`ProposerSelector::select_proposer`] — the method Malachite invokes
/// internally.
#[derive(Clone)]
pub struct ConsensusProposerOracle {
    proposer_selector: L2ProposerSelector,
    validator_set_provider: L2ValidatorSetProvider,
}

impl ConsensusProposerOracle {
    pub fn new(
        proposer_selector: L2ProposerSelector,
        validator_set_provider: L2ValidatorSetProvider,
    ) -> Self {
        Self {
            proposer_selector,
            validator_set_provider,
        }
    }
}

impl ExpectedProposer for ConsensusProposerOracle {
    fn expected_proposer(&self, height: u64, round: u32) -> anyhow::Result<ContractAddress> {
        let validator_set = self.validator_set_provider.get_validator_set(height)?;
        // Same call the consensus engine makes for this `(height, round)`.
        let validator = self
            .proposer_selector
            .select_proposer(&validator_set, height, round);
        Ok(validator.address)
    }
}
