//! Abstraction for resolving the expected proposer of a block.

use pathfinder_common::ContractAddress;

/// Resolves the proposer expected to author the block at a given consensus
/// `(height, round)`.
///
/// Implementations must compute the expected proposer using the same logic
/// the consensus engine uses to select proposers, so the validator's
/// "expected proposer" and the engine's selection are guaranteed to agree.
pub trait ExpectedProposer: Send + Sync {
    /// Returns the address of the proposer expected to propose at
    /// `(height, round)`.
    ///
    /// Errors are infrastructural (e.g. failing to read the validator or
    /// proposer set) and should be treated as fatal rather than as a rejected
    /// proposal.
    fn expected_proposer(&self, height: u64, round: u32) -> anyhow::Result<ContractAddress>;
}

/// An [`ExpectedProposer`] that always resolves to the same proposer,
/// regardless of height or round.
pub struct ConstantProposer(pub ContractAddress);

impl ExpectedProposer for ConstantProposer {
    fn expected_proposer(&self, _height: u64, _round: u32) -> anyhow::Result<ContractAddress> {
        Ok(self.0)
    }
}
