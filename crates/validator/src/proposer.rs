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
