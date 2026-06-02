use std::sync::Arc;

use pathfinder_common::{ChainId, ContractAddress};
use pathfinder_consensus::{PublicKey, SigningKey, Validator, ValidatorSet};
use pathfinder_consensus_fetcher as consensus_fetcher;
use pathfinder_storage::Storage;
use rand::rngs::OsRng;

use crate::config::ConsensusConfig;
use crate::consensus::validator_cache::ValidatorCache;

/// A proposer selector that fetches proposers from config or L2.
#[derive(Clone)]
pub struct L2ProposerSelector {
    storage: Storage,
    chain_id: ChainId,
    config: ConsensusConfig,
    /// Memoized proposer sets keyed by height. `select_proposer` is invoked
    /// multiple times per height (once per round, once per incoming proposal),
    /// and the underlying L2 lookup is expensive, so we cache the result.
    cache: ValidatorCache,
}

impl L2ProposerSelector {
    pub fn new(storage: Storage, chain_id: ChainId, config: ConsensusConfig) -> Self {
        Self {
            storage,
            chain_id,
            config,
            cache: ValidatorCache::default(),
        }
    }

    /// Returns the proposer set for `height`, fetching from L2 only on a cache
    /// miss.
    fn proposer_set_at(
        &self,
        height: u64,
    ) -> Result<Arc<ValidatorSet<ContractAddress>>, anyhow::Error> {
        // Upper bound on the number of distinct heights whose proposer sets
        // are kept in memory. Consensus advances monotonically, so when the
        // cache is full we evict the smallest key, which approximates LRU
        // for the expected workload.
        let max_cached_heights = self.config.history_depth.try_into().unwrap_or(10);
        self.cache
            .get_or_insert_with(height, max_cached_heights, || {
                fetch_proposers(&self.storage, self.chain_id, height, &self.config)
            })
    }
}

impl pathfinder_consensus::ProposerSelector<ContractAddress> for L2ProposerSelector {
    fn select_proposer<'a>(
        &self,
        validator_set: &'a ValidatorSet<ContractAddress>,
        height: u64,
        _round: u32,
    ) -> &'a Validator<ContractAddress> {
        let proposer_set = self
            .proposer_set_at(height)
            .expect("Failed to fetch proposers");

        // For now, just use the first proposer from the set
        let proposer_address = proposer_set
            .validators
            .first()
            .expect("No proposers found")
            .address;

        // Find the proposer in the validator set
        validator_set
            .validators
            .iter()
            .find(|v| v.address == proposer_address)
            .expect("Proposer must be in validator set")
    }
}

// TODO:
//
// Currently, the proposer fetching functionality lives in its own crate
// (consensus-fetcher) because we have a temporary internal RPC method that we
// use for convenient testing.
//
// This separation allows us to easily expose and test the functionality through
// the RPC while the specification for proposer fetching is still being
// finalized.
//
// Once we have a final spec, the functionality from the consensus-fetcher crate
// will be migrated into this file and the temporary crate (along with its RPC
// method) will be removed.
//
// For now I've just assumed we'll have a `priority` field in the proposer
// struct. Given this is just an assumption, I'm leveraging the existing
// `voting_power` field of the validator struct until we have a final spec.

/// Fetches proposers for a given height
///
/// Uses config-based proposers if proposer addresses are provided in config,
/// otherwise fetches proposers from the contract.
pub fn fetch_proposers(
    storage: &Storage,
    chain_id: ChainId,
    height: u64,
    config: &ConsensusConfig,
) -> Result<ValidatorSet<ContractAddress>, anyhow::Error> {
    if config.proposer_addresses.is_empty() {
        fetch_proposers_from_l2(storage, chain_id, height)
    } else {
        create_proposers_from_config(config)
    }
}

/// Creates proposers from consensus config
///
/// This creates proposers with random keys and equal priority.
pub fn create_proposers_from_config(
    config: &ConsensusConfig,
) -> Result<ValidatorSet<ContractAddress>, anyhow::Error> {
    let proposers = config
        .proposer_addresses
        .iter()
        .map(|address| {
            // TODO: This is obviously not production ready.
            let sk = SigningKey::new(OsRng);
            let vk = sk.verification_key();
            let public_key = PublicKey::from_bytes(vk.to_bytes());

            Validator {
                address: *address,
                public_key,
                voting_power: 1,
            }
        })
        .collect::<Vec<Validator<ContractAddress>>>();

    Ok(ValidatorSet::new(proposers))
}

/// Fetches proposers from the L2 contract
///
/// This logic is temporary until we have a final spec for proposer fetching.
fn fetch_proposers_from_l2(
    storage: &Storage,
    chain_id: ChainId,
    height: u64,
) -> Result<ValidatorSet<ContractAddress>, anyhow::Error> {
    let proposers = consensus_fetcher::get_proposers_at_height(storage, chain_id, height)?;
    let proposers = proposers
        .into_iter()
        .map(|proposer| Validator {
            address: proposer.address,
            public_key: proposer.public_key,
            voting_power: proposer.priority,
        })
        .collect::<Vec<Validator<ContractAddress>>>();
    Ok(ValidatorSet::new(proposers))
}
