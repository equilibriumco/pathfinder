//! # Pathfinder Consensus
//!
//! A Byzantine Fault Tolerant (BFT) consensus engine for Starknet nodes.
//!
//! ## Overview
//!
//! This crate provides a consensus engine for Starknet nodes that wraps the
//! Malachite implementation of the Tendermint BFT consensus algorithm. It's
//! designed to be generic over validator addresses and consensus values, making
//! it suitable for Starknet's consensus requirements.
//!
//! ## Core Concepts
//!
//! ### ValidatorAddress Trait
//!
//! Your validator address type must implement the `ValidatorAddress` trait,
//! which requires:
//! - `Sync + Send`: Thread-safe and sendable across threads
//! - `Ord + Display + Debug + Default + Clone`: Standard Rust traits for
//!   ordering, display, debugging, default values, and cloning
//! - `Into<Vec<u8>>`: Convertible to bytes for serialization
//! - `Serialize + DeserializeOwned`: Serde serialization support
//!
//! ### ValuePayload Trait
//!
//! Your consensus value type must implement the `ValuePayload` trait, which
//! requires:
//! - `Sync + Send`: Thread-safe and sendable across threads
//! - `Ord + Display + Debug + Default + Clone`: Standard Rust traits
//! - `Serialize + DeserializeOwned`: Serde serialization support
//!
//! ### Consensus Engine
//!
//! The main `Consensus<V, A>` struct is generic over:
//! - `V`: Your consensus value type (must implement `ValuePayload`)
//! - `A`: Your validator address type (must implement `ValidatorAddress`)
//!
//! ## Usage Example
//!
//! ```rust
//! use pathfinder_consensus::*;
//! use serde::{Deserialize, Serialize};
//!
//! // Define your validator address type
//! #[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
//! struct MyAddress(String);
//!
//! impl std::fmt::Display for MyAddress {
//!     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//!         write!(f, "{}", self.0)
//!     }
//! }
//!
//! impl From<MyAddress> for Vec<u8> {
//!     fn from(addr: MyAddress) -> Self {
//!         addr.0.into_bytes()
//!     }
//! }
//!
//! // Define your consensus value type
//! #[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
//! struct BlockData(String);
//!
//! impl std::fmt::Display for BlockData {
//!     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//!         write!(f, "{}", self.0)
//!     }
//! }
//!
//! // Custom proposer selector that uses weighted selection
//! #[derive(Clone)]
//! struct WeightedProposerSelector;
//!
//! impl ProposerSelector<MyAddress> for WeightedProposerSelector {
//!     fn select_proposer<'a>(
//!         &self,
//!         validator_set: &'a ValidatorSet<MyAddress>,
//!         _height: u64,
//!         round: u32,
//!     ) -> &'a Validator<MyAddress> {
//!         // Simple weighted selection based on voting power
//!         let total_power: u64 = validator_set
//!             .validators
//!             .iter()
//!             .map(|v| v.voting_power)
//!             .sum();
//!         let selection = (round as u64) % total_power;
//!
//!         let mut cumulative = 0;
//!         for validator in &validator_set.validators {
//!             cumulative += validator.voting_power;
//!             if selection < cumulative {
//!                 return validator;
//!             }
//!         }
//!
//!         // Fallback to first validator
//!         &validator_set.validators[0]
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     // Create configuration
//!     let my_address = MyAddress("validator_1".to_string());
//!     let config = Config::new(my_address.clone());
//!
//!     // Create consensus engine with custom proposer selector
//!     let proposer_selector = WeightedProposerSelector;
//!     let mut consensus = Consensus::with_proposer_selector(config, proposer_selector);
//!
//!     // Or use the default round-robin selector (no additional configuration needed)
//!     // let mut consensus = Consensus::new(config);
//!
//!     // Start consensus at height 1
//!     let validator_set = ValidatorSet::new(vec![Validator::new(
//!         my_address.clone(),
//!         PublicKey::from_bytes([0; 32]),
//!     )
//!     .with_voting_power(10)]);
//!
//!     consensus.handle_command(ConsensusCommand::StartHeight(1, validator_set));
//!
//!     // Poll for events
//!     while let Some(event) = consensus.next_event().await {
//!         match event {
//!             ConsensusEvent::RequestProposal { height, round } => {
//!                 println!("Need to propose at height {}, round {}", height, round);
//!             }
//!             ConsensusEvent::Decision { height, value } => {
//!                 println!("Consensus reached at height {}: {:?}", height, value);
//!             }
//!             ConsensusEvent::Gossip(message) => {
//!                 println!("Need to gossip: {:?}", message);
//!             }
//!             ConsensusEvent::Error(error) => {
//!                 eprintln!("Consensus error: {}", error);
//!             }
//!         }
//!     }
//! }
//! ```
//!
//! ## Commands and Events
//!
//! The consensus engine operates on a command/event model:
//!
//! - **Commands**: Send commands to the consensus engine via `handle_command()`
//! - **Events**: Poll for events from the consensus engine via
//!   `next_event().await`
//!
//! ## Crash Recovery
//!
//! The consensus engine supports crash recovery through write-ahead logging:
//!
//! ```rust
//! // Recover from a previous crash
//! let validator_sets = Arc::new(StaticValidatorSetProvider::new(validator_set));
//! let mut consensus = Consensus::recover(config, validator_sets);
//! ```

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt::{Debug, Display};
use std::ops::{Add, Sub};
use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

// Re-export consensus types needed by the public API
pub use crate::config::{Config, TimeoutValues};
pub use crate::error::ConsensusError;
use crate::internal::{InternalConsensus, InternalParams};
use crate::wal::{delete_wal_file, FileWalSink, NoopWal, WalSink};

mod config;
mod error;
mod internal;
mod wal;

/// A cryptographic signature for consensus messages.
///
/// This type is used to sign proposals and votes in the consensus protocol
/// to ensure authenticity and integrity of consensus messages.
pub type Signature = malachite_signing_ed25519::Signature;

/// An Ed25519 signing key.
///
/// This is also called a secret key by other implementations.
pub type SigningKey = ed25519_consensus::SigningKey;

/// A trait for consensus validator addresses.
///
/// This trait defines the requirements for validator address types used in the
/// consensus engine. Your validator address type must implement all the
/// required traits to be compatible with the consensus engine.
///
/// ## Required Traits
///
/// - `Sync + Send`: Thread-safe and sendable across threads
/// - `Ord + Display + Debug + Default + Clone`: Standard Rust traits for
///   ordering, display, debugging, default values, and cloning
/// - `Into<Vec<u8>>`: Convertible to bytes for serialization
/// - `Serialize + DeserializeOwned`: Serde serialization support
///
/// ## Example Implementation
///
/// ```rust
/// #[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// struct MyAddress(String);
///
/// impl std::fmt::Display for MyAddress {
///     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
///         write!(f, "{}", self.0)
///     }
/// }
///
/// impl From<MyAddress> for Vec<u8> {
///     fn from(addr: MyAddress) -> Self {
///         addr.0.into_bytes()
///     }
/// }
/// ```
pub trait ValidatorAddress:
    Sync + Send + Ord + Display + Debug + Default + Clone + Into<Vec<u8>> + Serialize + DeserializeOwned
{
}
impl<T> ValidatorAddress for T where
    T: Sync
        + Send
        + Ord
        + Display
        + Debug
        + Default
        + Clone
        + Into<Vec<u8>>
        + Serialize
        + DeserializeOwned
{
}

/// A trait for consensus value payloads.
///
/// This trait defines the requirements for consensus value types used in the
/// consensus engine. Your consensus value type must implement all the required
/// traits to be compatible with the consensus engine.
///
/// ## Required Traits
///
/// - `Sync + Send`: Thread-safe and sendable across threads
/// - `Ord + Display + Debug + Default + Clone`: Standard Rust traits
/// - `Serialize + DeserializeOwned`: Serde serialization support
///
/// ## Example Implementation
///
/// ```rust
/// #[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// struct BlockData(String);
///
/// impl std::fmt::Display for BlockData {
///     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
///         write!(f, "{}", self.0)
///     }
/// }
/// ```
pub trait ValuePayload:
    Sync + Send + Ord + Display + Debug + Default + Clone + Serialize + DeserializeOwned
{
}
impl<T> ValuePayload for T where
    T: Sync + Send + Ord + Display + Debug + Default + Clone + Serialize + DeserializeOwned
{
}

/// # Pathfinder consensus engine
///
/// This is the main consensus engine for Starknet nodes that implements
/// Byzantine Fault Tolerant (BFT) consensus using the Malachite implementation
/// of Tendermint. It's generic over validator addresses, consensus values, and
/// proposer selection algorithms, making it suitable for Starknet's consensus
/// requirements.
///
/// ## Generic Parameters
///
/// - `V`: Your consensus value type (must implement `ValuePayload`)
/// - `A`: Your validator address type (must implement `ValidatorAddress`)
/// - `P`: Your proposer selector type (must implement `ProposerSelector<A>`)
///
/// ## Usage
///
/// ```rust
/// let config = Config::new(my_address);
/// let mut consensus = Consensus::new(config);
///
/// // Start consensus at a height
/// consensus.handle_command(ConsensusCommand::StartHeight(height, validator_set));
///
/// // Poll for events
/// while let Some(event) = consensus.next_event().await {
///     // Handle events
/// }
/// ```
///
/// ## Custom Proposer Selection
///
/// To use a custom proposer selector:
///
/// ```rust
/// let config = Config::new(my_address);
/// let custom_selector = WeightedProposerSelector;
/// let mut consensus = Consensus::with_proposer_selector(config, custom_selector);
/// ```
///
/// ## Crash Recovery
///
/// The consensus engine supports crash recovery through write-ahead logging:
///
/// ```rust
/// let validator_sets = Arc::new(StaticValidatorSetProvider::new(validator_set));
/// let mut consensus = Consensus::recover(config, validator_sets)?;
/// ```
pub struct Consensus<
    V: ValuePayload + 'static,
    A: ValidatorAddress + 'static,
    P: ProposerSelector<A> + Send + Sync + 'static,
> {
    internal: HashMap<u64, InternalConsensus<V, A, P>>,
    event_queue: VecDeque<ConsensusEvent<V, A>>,
    config: Config<A>,
    proposer_selector: P,
    min_kept_height: Option<u64>,
    last_decided_height: Option<u64>,
}

impl<
        V: ValuePayload + 'static,
        A: ValidatorAddress + 'static,
        P: ProposerSelector<A> + Send + Sync + 'static,
    > Consensus<V, A, P>
{
    /// Create a new consensus engine for the current validator.
    ///
    /// ## Arguments
    ///
    /// - `config`: The consensus configuration containing validator address,
    ///   timeouts, and other settings
    ///
    /// ## Example
    ///
    /// ```rust
    /// let config = Config::new(my_address);
    /// let mut consensus = Consensus::new(config);
    /// ```
    pub fn new(config: Config<A>) -> DefaultConsensus<V, A> {
        Consensus {
            internal: HashMap::new(),
            event_queue: VecDeque::new(),
            config,
            proposer_selector: RoundRobinProposerSelector,
            min_kept_height: None,
            last_decided_height: None,
        }
    }

    /// Create a new consensus engine with a custom proposer selector for this
    /// consensus engine.
    ///
    /// ## Arguments
    ///
    /// - `config`: The consensus configuration containing validator address,
    ///   timeouts, and other settings
    /// - `proposer_selector`: The proposer selection algorithm to use
    ///
    /// ## Example
    ///
    /// ```rust
    /// let config = Config::new(my_address);
    /// let custom_selector = WeightedProposerSelector;
    /// let mut consensus = Consensus::with_proposer_selector(config, custom_selector);
    /// ```
    pub fn with_proposer_selector<PS: ProposerSelector<A> + Send + Sync + 'static>(
        config: Config<A>,
        proposer_selector: PS,
    ) -> Consensus<V, A, PS> {
        Consensus {
            internal: HashMap::new(),
            event_queue: VecDeque::new(),
            config,
            proposer_selector,
            min_kept_height: None,
            last_decided_height: None,
        }
    }

    /// Recover recent heights from the write-ahead log.
    ///
    /// This method is used to recover consensus state after a crash or restart.
    /// It reads the write-ahead log and reconstructs the consensus state for
    /// all incomplete heights.
    ///
    /// ## Arguments
    ///
    /// - `config`: The consensus configuration
    /// - `validator_sets`: A provider for validator sets at different heights
    /// - `highest_committed`: The highest committed block in main storage
    ///
    /// ## Example
    ///
    /// ```rust
    /// let validator_sets = Arc::new(StaticValidatorSetProvider::new(validator_set));
    /// let mut consensus = Consensus::recover(config, validator_sets)?;
    /// ```
    pub fn recover<VS: ValidatorSetProvider<A> + 'static>(
        config: Config<A>,
        validator_sets: Arc<VS>,
        highest_committed: Option<u64>,
    ) -> anyhow::Result<DefaultConsensus<V, A>> {
        Self::recover_inner(
            Self::new(config.clone()),
            config,
            validator_sets,
            highest_committed,
        )
    }

    /// Recover recent heights from the write-ahead log with a custom proposer
    /// selector for this consensus engine.
    ///
    /// This method is used to recover consensus state after a crash or restart.
    /// It reads the write-ahead log and reconstructs the consensus state for
    /// all incomplete heights.
    ///
    /// ## Arguments
    ///
    /// - `config`: The consensus configuration
    /// - `validator_sets`: A provider for validator sets at different heights
    /// - `proposer_selector`: The proposer selection algorithm to use
    ///
    /// ## Example
    ///
    /// ```rust
    /// let validator_sets = Arc::new(StaticValidatorSetProvider::new(validator_set));
    /// let mut consensus = Consensus::recover(config, validator_sets)?;
    /// ```
    pub fn recover_with_proposal_selector<
        VS: ValidatorSetProvider<A> + 'static,
        PS: ProposerSelector<A> + Send + Sync + 'static,
    >(
        config: Config<A>,
        validator_sets: Arc<VS>,
        proposer_selector: PS,
        highest_committed: Option<u64>,
    ) -> anyhow::Result<Consensus<V, A, PS>> {
        Self::recover_inner(
            Self::with_proposer_selector(config.clone(), proposer_selector),
            config,
            validator_sets,
            highest_committed,
        )
    }

    fn recover_inner<
        VS: ValidatorSetProvider<A> + 'static,
        PS: ProposerSelector<A> + Send + Sync + 'static,
    >(
        mut consensus: Consensus<V, A, PS>,
        config: Config<A>,
        validator_sets: Arc<VS>,
        highest_committed: Option<u64>,
    ) -> anyhow::Result<Consensus<V, A, PS>> {
        use crate::wal::recovery;

        tracing::info!(
            validator = ?config.address,
            wal_dir = %config.wal_dir.display(),
            "Starting consensus recovery from WAL"
        );

        // Read the write-ahead log and recover all incomplete heights.
        // This also returns finalized heights and the highest Decision height found
        // (even in finalized heights).
        let (incomplete_heights, finalized_heights, highest_decision) =
            match recovery::recover_incomplete_heights(&config.wal_dir, highest_committed) {
                Ok((incomplete, finalized, decision_height)) => {
                    tracing::info!(
                        validator = ?config.address,
                        incomplete_heights = incomplete.len(),
                        finalized_heights = finalized.len(),
                        highest_decision = ?decision_height,
                        "Found incomplete and finalized heights in WAL"
                    );
                    (incomplete, finalized, decision_height)
                }
                Err(e) => {
                    tracing::error!(
                        validator = ?config.address,
                        wal_dir = %config.wal_dir.display(),
                        error = %e,
                        "Failed to recover incomplete heights from WAL"
                    );
                    (Vec::new(), Vec::new(), None)
                }
            };

        // Set last_decided_height from the highest Decision found during recovery.
        consensus.last_decided_height = highest_decision;
        if let Some(h) = highest_decision {
            tracing::info!(
                validator = ?consensus.config.address,
                last_decided_height = %h,
                "Set last_decided_height from WAL recovery"
            );
        }

        // Determine the maximum height we're recovering (incomplete or finalized).
        let max_height = incomplete_heights
            .iter()
            .chain(finalized_heights.iter())
            .map(|(height, _)| *height)
            .max()
            .or(highest_decision);

        // Calculate the minimum height to keep based on history_depth.
        // This matches the pruning logic used during normal operation.
        let min_height_to_restore =
            max_height.and_then(|max| max.checked_sub(config.history_depth));

        // Restore finalized heights that are within history_depth.
        // This ensures we can accept votes for these heights, matching the behavior
        // during normal operation where finalized heights remain in memory until
        // pruned.
        for (height, entries) in finalized_heights {
            // Only restore finalized heights that are within history_depth.
            // (if max_height < history_depth, restore all finalized heights)
            let should_restore = min_height_to_restore
                .map(|min| height >= min)
                .unwrap_or(true);

            if should_restore {
                tracing::info!(
                    validator = ?consensus.config.address,
                    height = %height,
                    "Restoring finalized height within history_depth"
                );

                let validator_set = validator_sets.get_validator_set(height)?;
                let mut internal_consensus = consensus.create_consensus(height, &validator_set);

                // Recover from WAL first to restore the engine state.
                let vote_round = internal_consensus.recover_from_wal(entries);
                if let Some(round) = vote_round {
                    // Schedule rebroadcast timeout.
                    // See https://github.com/equilibriumco/pathfinder/issues/3286 for motivation.
                    internal_consensus.schedule_rebroadcast(round);
                }

                // Only call StartHeight if the height is not already finalized.
                //
                // With one exception:
                // In an extremely rare case, where this node is required for consensus to move
                // forward (ie. avoid stalling when there are only 3 nodes) it may happen that
                // the other 2 nodes have not finalized their heights H and have timed out while
                // our node, prior to being restarted, has finalized the same height H. Without
                // our node restarting consensus at H the network will stall even though the
                // number of honest nodes is sufficient (ie. 3).
                if !internal_consensus.is_finalized()
                    || max_height.is_some_and(|max_height| max_height == height)
                {
                    internal_consensus
                        .handle_command(ConsensusCommand::StartHeight(height, validator_set));
                }

                consensus.internal.insert(height, internal_consensus);
            } else {
                tracing::debug!(
                    validator = ?consensus.config.address,
                    height = %height,
                    min_height = ?min_height_to_restore,
                    "Skipping finalized height outside history_depth"
                );
            }
        }

        // Manually recover all incomplete heights.
        for (height, entries) in incomplete_heights {
            tracing::info!(
                validator = ?consensus.config.address,
                height = %height,
                entry_count = entries.len(),
                "Recovering incomplete height from WAL"
            );

            let validator_set = validator_sets.get_validator_set(height)?;
            let mut internal_consensus = consensus.create_consensus(height, &validator_set);
            internal_consensus.handle_command(ConsensusCommand::StartHeight(height, validator_set));
            let vote_round = internal_consensus.recover_from_wal(entries);
            if let Some(round) = vote_round {
                // Schedule rebroadcast timeout.
                internal_consensus.schedule_rebroadcast(round);
            }
            consensus.internal.insert(height, internal_consensus);
        }

        // Set min_kept_height to match what we've restored, so that is_height_finalized
        // correctly identifies finalized heights that were pruned.
        consensus.min_kept_height = min_height_to_restore;

        tracing::info!(
            validator = ?consensus.config.address,
            recovered_heights = consensus.internal.len(),
            last_decided_height = ?consensus.last_decided_height,
            min_kept_height = ?consensus.min_kept_height,
            "Completed consensus recovery"
        );

        Ok(consensus)
    }

    fn create_consensus(
        &mut self,
        height: u64,
        validator_set: &ValidatorSet<A>,
    ) -> InternalConsensus<V, A, P> {
        let params = InternalParams {
            height,
            validator_set: validator_set.clone(),
            address: self.config.address.clone(),
            threshold_params: self.config.threshold_params,
            value_payload: malachite_types::ValuePayload::ProposalOnly,
        };

        // Create a WAL for the height. If we fail, use a NoopWal.
        let wal = match FileWalSink::new(&self.config.address, height, &self.config.wal_dir) {
            Ok(wal) => Box::new(wal) as Box<dyn WalSink<V, A>>,
            Err(e) => {
                tracing::error!(
                    validator = ?self.config.address,
                    height = %height,
                    error = %e,
                    "Failed to create wal for height"
                );
                Box::new(NoopWal)
            }
        };

        // A new consensus is created for every new height.
        InternalConsensus::new(
            params,
            self.config.timeout_values.clone(),
            wal,
            self.proposer_selector.clone(),
        )
    }

    /// Feed a command into the consensus engine.
    ///
    /// This method is the primary way to interact with the consensus engine.
    /// Commands include starting new heights, submitting proposals, and
    /// processing votes.
    ///
    /// ## Arguments
    ///
    /// - `cmd`: The command to process
    ///
    /// ## Example
    ///
    /// ```rust
    /// // Start a new height
    /// consensus.handle_command(ConsensusCommand::StartHeight(height, validator_set));
    ///
    /// // Submit a proposal
    /// consensus.handle_command(ConsensusCommand::Proposal(signed_proposal));
    ///
    /// // Process a vote
    /// consensus.handle_command(ConsensusCommand::Vote(signed_vote));
    /// ```
    pub fn handle_command(&mut self, cmd: ConsensusCommand<V, A>) {
        match cmd {
            // Start a new height.
            ConsensusCommand::StartHeight(height, validator_set) => {
                // A new consensus is created for every new height.
                let mut consensus = self.create_consensus(height, &validator_set);
                consensus.handle_command(ConsensusCommand::StartHeight(height, validator_set));
                self.internal.insert(height, consensus);
                tracing::debug!(
                    validator = ?self.config.address,
                    height = %height,
                    "Started new consensus"
                );
            }
            other => {
                let height = other.height();
                if let Some(engine) = self.internal.get_mut(&height) {
                    engine.handle_command(other);
                } else {
                    tracing::warn!(
                        validator = ?self.config.address,
                        height = %height,
                        command = ?other,
                        "Received command for unknown height"
                    );
                }
            }
        }
    }

    /// Poll all engines for an event.
    ///
    /// This method should be called regularly to process events from the
    /// consensus engine. Events include requests for proposals, decisions,
    /// gossip messages, and errors.
    ///
    /// ## Returns
    ///
    /// Returns `Some(event)` if an event is available, or `None` if no events
    /// are ready.
    ///
    /// ## Example
    ///
    /// ```rust
    /// while let Some(event) = consensus.next_event().await {
    ///     match event {
    ///         ConsensusEvent::RequestProposal { height, round } => {
    ///             // Build and submit a proposal
    ///         }
    ///         ConsensusEvent::Decision { height, value } => {
    ///             // Consensus reached, process the value
    ///         }
    ///         ConsensusEvent::Gossip(message) => {
    ///             // Send message to peers
    ///         }
    ///         ConsensusEvent::Error(error) => {
    ///             // Handle error
    ///         }
    ///     }
    /// }
    /// ```
    pub async fn next_event(&mut self) -> Option<ConsensusEvent<V, A>> {
        let mut finished_heights = Vec::new();
        // Drain each internal engine.
        for (height, engine) in self.internal.iter_mut() {
            if let Some(event) = engine.poll_internal().await {
                tracing::trace!(
                    validator = ?self.config.address,
                    height = %height,
                    event = ?event,
                    "Engine returned event"
                );
                // Track finished heights and update last_decided_height.
                if let ConsensusEvent::Decision { height, .. } = &event {
                    finished_heights.push(*height);
                    // Update last_decided_height to track the highest decided height.
                    self.last_decided_height = Some(
                        self.last_decided_height
                            .map(|h| h.max(*height))
                            .unwrap_or(*height),
                    );
                }
                // Push the event to the queue.
                self.event_queue.push_back(event);
            }
        }

        // Prune old engines if we have any finished heights.
        if !finished_heights.is_empty() {
            self.prune_old_engines();
        }

        // Return the first event from the queue.
        self.event_queue.pop_front()
    }

    /// Prune old engines from the internal map.
    fn prune_old_engines(&mut self) {
        let max_height = self.internal.keys().max().copied();
        if let Some(max_height) = max_height {
            let new_min_height = max_height.checked_sub(self.config.history_depth);

            if let Some(new_min) = new_min_height {
                // Collect heights that will be pruned (before we remove them from the map).
                let pruned_heights: Vec<u64> = self
                    .internal
                    .keys()
                    .filter(|height| **height < new_min)
                    .copied()
                    .collect();

                // Prune the internal map and set the new min_kept_height.
                self.min_kept_height = Some(new_min);
                self.internal.retain(|height, _| *height >= new_min);

                // Delete WAL files for pruned heights.
                for height in &pruned_heights {
                    if let Err(e) =
                        delete_wal_file(&self.config.address, *height, &self.config.wal_dir)
                    {
                        tracing::warn!(
                            validator = ?self.config.address,
                            height = %height,
                            error = %e,
                            "Failed to delete WAL file for pruned height"
                        );
                    }
                }

                tracing::debug!(
                    validator = ?self.config.address,
                    min_height = %new_min,
                    max_height = %max_height,
                    pruned_count = pruned_heights.len(),
                    "Pruned old consensus engines and deleted WAL files"
                );
            }
        }
    }

    /// Check if a specific height has been finalized (i.e., a decision has been
    /// reached)
    ///
    /// ## Arguments
    ///
    /// - `height`: The height to check
    ///
    /// ## Returns
    ///
    /// Returns `true` if the height has been finalized, `false` otherwise.
    ///
    /// ## Example
    ///
    /// ```rust
    /// if consensus.is_height_finalized(height) {
    ///     println!("Height {} has been finalized", height);
    /// }
    /// ```
    pub fn is_height_finalized(&self, height: u64) -> bool {
        if let Some(engine) = self.internal.get(&height) {
            engine.is_finalized()
        } else {
            // If the height is not in our internal map, it might have been pruned
            // after being finalized, so we assume it's finalized
            if let Some(min_height) = self.min_kept_height {
                if height < min_height {
                    return true;
                }
            }
            false
        }
    }

    /// Check if a specific height is actively tracked by the consensus engine.
    ///
    /// ## Arguments
    ///
    /// - `height`: The height to check
    ///
    /// ## Returns
    ///
    /// Returns `true` if the height is active, `false` otherwise.
    pub fn is_height_active(&self, height: u64) -> bool {
        self.internal.contains_key(&height)
    }

    /// Get the maximum height actively being tracked by the consensus engine.
    ///
    /// This returns the highest height that consensus is currently working on,
    /// which includes:
    ///  - incomplete heights that haven't reached a decision yet,
    ///  - finalized heights that have reached a decision but haven't been
    ///    pruned yet.
    ///
    /// Returns `None` if there are no actively tracked heights.
    pub fn max_active_height(&self) -> Option<u64> {
        self.internal.keys().max().copied()
    }

    /// Get the highest height that consensus has decided on.
    ///
    /// This returns the highest height that has a Decision entry, even if that
    /// height is no longer actively tracked (e.g., after recovery when it was
    /// skipped).
    ///
    /// Returns `None` if no decisions have been made yet.
    pub fn last_decided_height(&self) -> Option<u64> {
        self.last_decided_height
    }
}

/// A round number (or `None` if the round is nil).
///
/// This type represents a consensus round number. A round can be either a
/// specific round number or nil (None), which represents a special state in the
/// consensus protocol.
///
/// ## Example
///
/// ```rust
/// let round = Round::new(5); // Round 5
/// let nil_round = Round::nil(); // Nil round
/// ```
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Round(pub Option<u32>);

impl Round {
    /// Create a new round with the given round number.
    pub fn new(round: u32) -> Self {
        Self(Some(round))
    }

    /// Create a nil round.
    pub fn nil() -> Self {
        Self(None)
    }

    /// Get the round number as a u32, if it's not nil.
    pub fn as_u32(&self) -> Option<u32> {
        self.0
    }
}

impl From<u32> for Round {
    fn from(round: u32) -> Self {
        Self::new(round)
    }
}

impl Add<u32> for Round {
    type Output = Self;

    fn add(self, rhs: u32) -> Self::Output {
        Self(self.0.map(|round| round + rhs))
    }
}

impl Sub<u32> for Round {
    type Output = Self;

    fn sub(self, rhs: u32) -> Self::Output {
        Self(self.0.map(|round| round - rhs))
    }
}

impl Display for Round {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Some(round) => write!(f, "{round}"),
            None => write!(f, "Nil"),
        }
    }
}

impl Debug for Round {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

/// A proposal for a block value in a consensus round.
///
/// A proposal is created by the designated proposer for a given height and
/// round. It contains the proposed block value along with additional metadata.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Proposal<V, A> {
    /// The blockchain height
    pub height: u64,
    /// The consensus round number
    pub round: Round,
    /// The proposed consensus value
    pub value: V,
    /// The POL round for which the proposal is for
    pub pol_round: Round,
    /// The address of the proposer
    pub proposer: A,
}

impl<V: Debug, A: Debug> std::fmt::Debug for Proposal<V, A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "H:{} R:{} From:{:?} Val:{:?}",
            self.height, self.round, self.proposer, self.value
        )
    }
}

/// The type of vote.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum VoteType {
    /// A preliminary vote
    Prevote,
    /// A final vote that commits to a value
    Precommit,
}

/// A vote for a value in a consensus round.
///
/// A vote is cast by a validator to indicate their agreement or disagreement
/// with a proposed block value. The vote includes the validator's address, the
/// round number, and the block value being voted on.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Vote<V, A> {
    /// The type of vote (Prevote or Precommit)
    pub r#type: VoteType,
    /// The blockchain height
    pub height: u64,
    /// The consensus round number
    pub round: Round,
    /// The value being voted on (None for nil votes)
    pub value: Option<V>,
    /// The address of the validator casting the vote
    pub validator_address: A,
}

impl<V: Debug, A: Debug> std::fmt::Debug for Vote<V, A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let val = match &self.value {
            Some(val) => format!("{val:?}"),
            None => "Nil".to_string(),
        };
        write!(
            f,
            "H:{} R:{} {:?} From:{:?} Val:{val}",
            self.height, self.round, self.r#type, self.validator_address
        )
    }
}

impl<V, A> Vote<V, A> {
    /// Check if the vote is nil.
    ///
    /// A nil vote is a vote that does not commit to a value.
    pub fn is_nil(&self) -> bool {
        self.value.is_none()
    }
}

/// A fully validated, signed proposal ready to enter consensus.
///
/// This type wraps a proposal with a cryptographic signature to ensure
/// authenticity and integrity.
#[derive(Clone, Serialize, Deserialize)]
pub struct SignedProposal<V, A> {
    pub proposal: Proposal<V, A>,
    pub signature: Signature,
}

/// A signed vote.
///
/// This type wraps a vote with a cryptographic signature to ensure
/// authenticity and integrity.
#[derive(Clone, Serialize, Deserialize)]
pub struct SignedVote<V, A> {
    pub vote: Vote<V, A>,
    pub signature: Signature,
}

// Note: We intentionally ignore the signature as it's not used yet.
impl<V: Debug, A: Debug> std::fmt::Debug for SignedProposal<V, A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.proposal)
    }
}

// Note: We intentionally ignore the signature as it's not used yet.
impl<V: Debug, A: Debug> std::fmt::Debug for SignedVote<V, A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.vote)
    }
}

/// A public key for the consensus protocol.
///
/// This type is used to verify signatures on proposals and votes in the
/// consensus protocol. Each validator has an associated public key that is used
/// to authenticate their messages.
pub type PublicKey = malachite_signing_ed25519::PublicKey;

/// A validator's voting power.
pub type VotingPower = u64;

/// A validator in the consensus protocol.
///
/// Each validator has an associated address and public key to uniquely identify
/// them. The voting power determines their weight in consensus decisions.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Validator<A> {
    /// The validator's address
    pub address: A,
    /// The validator's public key for signature verification
    pub public_key: PublicKey,
    /// The validator's voting power (weight in consensus)
    pub voting_power: VotingPower,
}

impl<A: Debug> std::fmt::Debug for Validator<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?} ({})", self.address, self.voting_power)
    }
}

impl<A> Validator<A> {
    /// Create a new validator with the given address and public key.
    ///
    /// The voting power defaults to 1.
    pub fn new(address: A, public_key: PublicKey) -> Self {
        Self {
            address,
            public_key,
            voting_power: 1,
        }
    }

    /// Set the voting power for the validator.
    ///
    /// This method returns `self` for method chaining.
    pub fn with_voting_power(mut self, voting_power: VotingPower) -> Self {
        self.voting_power = voting_power;
        self
    }
}

/// A validator set represents a group of consensus participants.
///
/// The validator set defines who can participate in consensus at a given
/// height. Each validator in the set has a voting power that determines their
/// weight in consensus decisions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValidatorSet<A> {
    /// The list of validators in the set
    pub validators: Vec<Validator<A>>,
}

impl<A: Clone + Ord> ValidatorSet<A> {
    /// Create a new validator set with the given validators.
    pub fn new(validators: impl IntoIterator<Item = Validator<A>>) -> Self {
        // Ensure validators are unique by address.
        let validators: BTreeMap<A, Validator<A>> = validators
            .into_iter()
            .map(|v| (v.address.clone(), v))
            .collect();
        assert!(!validators.is_empty());
        let validators = validators.into_values().collect::<Vec<Validator<A>>>();
        Self { validators }
    }

    /// Get the number of validators in the set.
    pub fn count(&self) -> usize {
        self.validators.len()
    }
}

/// Commands that the application can send into the consensus engine.
///
/// These commands represent the primary interface for interacting with the
/// consensus engine. They allow the application to start new heights,
/// submit proposals, and process votes.
pub enum ConsensusCommand<V, A> {
    /// Start consensus at a given height with the validator set.
    StartHeight(u64, ValidatorSet<A>),
    /// A complete, locally validated and signed proposal that we create as the
    /// proposer for the current round.
    Propose(Proposal<V, A>),
    /// A complete, locally validated and signed proposal that was received over
    /// the network from another validator.
    Proposal(SignedProposal<V, A>),
    /// A signed vote received from the network.
    Vote(SignedVote<V, A>),
}

impl<V, A> ConsensusCommand<V, A> {
    /// Get the consensus height associated with the command.
    pub fn height(&self) -> u64 {
        match self {
            ConsensusCommand::StartHeight(height, _) => *height,
            ConsensusCommand::Propose(proposal) => proposal.height,
            ConsensusCommand::Proposal(proposal) => proposal.proposal.height,
            ConsensusCommand::Vote(vote) => vote.vote.height,
        }
    }
}

impl<V: Debug, A: Debug> std::fmt::Debug for ConsensusCommand<V, A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConsensusCommand::StartHeight(height, validator_set) => write!(
                f,
                "StartHeight({}, [{}])",
                height,
                validator_set
                    .validators
                    .iter()
                    .map(|v| format!("{:?}", v.address))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            ConsensusCommand::Propose(proposal) => write!(f, "Propose({proposal:?})"),
            ConsensusCommand::Proposal(proposal) => write!(f, "Proposal({proposal:?})"),
            ConsensusCommand::Vote(vote) => write!(f, "Vote({vote:?})"),
        }
    }
}

/// A message to be gossiped to peers.
///
/// These messages represent network communication that needs to be sent to
/// other validators in the network.
#[derive(Clone, Debug)]
pub enum NetworkMessage<V, A> {
    /// A complete, locally validated and signed proposal ready to be gossiped.
    Proposal(SignedProposal<V, A>),
    /// A vote received from the network.
    Vote(SignedVote<V, A>),
}

/// Events that the consensus engine emits for the application to handle.
///
/// These events represent the output of the consensus engine and tell the
/// application what actions it needs to take.
pub enum ConsensusEvent<V, A> {
    /// The consensus wants this message to be gossiped to peers.
    ///
    /// The application should send this message to all peers in the network.
    Gossip(NetworkMessage<V, A>),
    /// The consensus needs the app to build and inject a proposal.
    ///
    /// The application should create a proposal for the given height and round,
    /// then submit it to the consensus engine.
    RequestProposal { height: u64, round: u32 },
    /// The consensus has reached a decision and committed a block.
    ///
    /// This event indicates that consensus has been reached for the given
    /// height and the value should be committed to the blockchain.
    Decision { height: u64, round: u32, value: V },
    /// An internal error occurred in consensus.
    ///
    /// The application should handle this error appropriately, possibly by
    /// logging it or taking corrective action.
    Error(ConsensusError),
}

impl<V: Debug, A: Debug> std::fmt::Debug for ConsensusEvent<V, A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConsensusEvent::Gossip(msg) => match msg {
                NetworkMessage::Proposal(proposal) => write!(f, "Gossip(Proposal({proposal:?}))"),
                NetworkMessage::Vote(vote) => write!(f, "Gossip(Vote({vote:?}))"),
            },
            ConsensusEvent::RequestProposal { height, round, .. } => {
                write!(f, "RequestProposal(H:{height} R:{round})")
            }
            ConsensusEvent::Decision {
                height,
                round,
                value,
            } => {
                write!(f, "Decision(H:{height} R: {round} Val:{value:?})")
            }
            ConsensusEvent::Error(error) => write!(f, "Error({error:?})"),
        }
    }
}

/// A trait for retrieving the validator set at a specific blockchain height.
///
/// This trait allows consensus to dynamically determine the set of validators
/// that are eligible to participate in consensus at any given height.
///
/// This is useful for handling validator set changes across heights.
pub trait ValidatorSetProvider<A> {
    fn get_validator_set(&self, height: u64) -> Result<ValidatorSet<A>, anyhow::Error>;
}

/// A trait for selecting the proposer for a given height and round.
///
/// This trait allows consumers to provide custom proposer selection logic
/// instead of using the default round-robin approach. This is useful for
/// implementing more sophisticated proposer selection algorithms like
/// VRF-based selection, weighted selection, or other custom logic.
///
/// ## Example Implementation
///
/// ```rust
/// use pathfinder_consensus::*;
///
/// struct RoundRobinProposerSelector;
///
/// impl<A: ValidatorAddress> ProposerSelector<A> for RoundRobinProposerSelector {
///     fn select_proposer<'a>(
///         &self,
///         validator_set: &'a ValidatorSet<A>,
///         height: u64,
///         round: u32,
///     ) -> &'a Validator<A> {
///         let index = round as usize % validator_set.count();
///         &validator_set.validators[index]
///     }
/// }
/// ```
pub trait ProposerSelector<A: ValidatorAddress>: Clone + Send + Sync {
    /// Select the proposer for the given height and round.
    ///
    /// ## Arguments
    ///
    /// - `validator_set`: The set of validators eligible to propose
    /// - `height`: The blockchain height
    /// - `round`: The consensus round number
    ///
    /// ## Returns
    ///
    /// Returns a reference to the selected validator who should propose
    /// for the given height and round.
    fn select_proposer<'a>(
        &self,
        validator_set: &'a ValidatorSet<A>,
        height: u64,
        round: u32,
    ) -> &'a Validator<A>;
}

/// A default proposer selector that uses round-robin selection.
///
/// This is the default proposer selection algorithm that selects proposers
/// in a round-robin fashion based on the round number modulo the number of
/// validators.
#[derive(Clone, Default)]
pub struct RoundRobinProposerSelector;

impl<A: ValidatorAddress> ProposerSelector<A> for RoundRobinProposerSelector {
    fn select_proposer<'a>(
        &self,
        validator_set: &'a ValidatorSet<A>,
        _height: u64,
        round: u32,
    ) -> &'a Validator<A> {
        let index = round as usize % validator_set.count();
        &validator_set.validators[index]
    }
}

/// A type alias for consensus with the default round-robin proposer selector.
///
/// This provides a convenient way to create consensus instances without
/// specifying the proposer selector type.
pub type DefaultConsensus<V, A> = Consensus<V, A, RoundRobinProposerSelector>;

/// A validator set provider that always returns the same validator set.
///
/// This is a simple implementation of `ValidatorSetProvider` that returns
/// the same validator set for all heights. This is useful for testing or
/// for applications where the validator set doesn't change.
pub struct StaticValidatorSetProvider<A> {
    validator_set: ValidatorSet<A>,
}

impl<A> StaticValidatorSetProvider<A> {
    /// Create a new static validator set provider.
    ///
    /// ## Arguments
    ///
    /// - `validator_set`: The validator set to return for all heights
    pub fn new(validator_set: ValidatorSet<A>) -> Self {
        Self { validator_set }
    }
}

impl<A: Clone + Send + Sync> ValidatorSetProvider<A> for StaticValidatorSetProvider<A> {
    fn get_validator_set(&self, _height: u64) -> Result<ValidatorSet<A>, anyhow::Error> {
        Ok(self.validator_set.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn regression_validator_set_is_unique_by_address() {
        let with_duplicates = [1, 1, 2, 2, 2, 3, 3, 3, 3, 2, 1, 1, 2, 3, 2, 2, 1, 1, 3, 3]
            .into_iter()
            .map(|i| Validator::new(i, crate::PublicKey::from_bytes([0; 32])));
        let set = ValidatorSet::new(with_duplicates);

        assert_eq!(
            set.validators.iter().map(|v| v.address).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
    }
}
