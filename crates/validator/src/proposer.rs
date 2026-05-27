//! Abstraction for resolving the expected proposer of a block.

use pathfinder_common::ContractAddress;

/// Resolves the proposer expected to author the block at a given consensus
/// `(height, round)`.
pub trait ExpectedProposer {
    /// Returns the address of the proposer expected to propose at
    /// `(height, round)`.
    ///
    /// Errors are infrastructural (e.g. failing to read the proposer set) and
    /// should be treated as fatal rather than as a rejected proposal.
    fn expected_proposer(&self, height: u64, round: u32) -> anyhow::Result<ContractAddress>;
}

/// An [`ExpectedProposer`] that always resolves to the same proposer,
/// regardless of height or round.
///
/// Useful for single-proposer and devnet setups (where we author our own
/// proposal and already know who the proposer is) and as a test double.
pub struct ConstantProposer(pub ContractAddress);

impl ExpectedProposer for ConstantProposer {
    fn expected_proposer(&self, _height: u64, _round: u32) -> anyhow::Result<ContractAddress> {
        Ok(self.0)
    }
}
