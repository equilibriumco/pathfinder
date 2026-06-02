use std::sync::Arc;

use pathfinder_common::{ChainId, ContractAddress};
use pathfinder_consensus::{PublicKey, SigningKey, Validator, ValidatorSet};
use pathfinder_consensus_fetcher as consensus_fetcher;
use pathfinder_storage::Storage;
use rand::rngs::OsRng;

use crate::config::ConsensusConfig;
use crate::consensus::validator_cache::ValidatorCache;

#[derive(Clone)]
pub struct L2ValidatorSetProvider {
    storage: Storage,
    chain_id: ChainId,
    config: ConsensusConfig,
    /// Memoized validator sets keyed by height. Consensus repeatedly requests
    /// validator sets while processing a height, and the underlying L2 lookup
    /// is expensive, so we cache the result.
    cache: ValidatorCache,
}

impl L2ValidatorSetProvider {
    pub fn new(storage: Storage, chain_id: ChainId, config: ConsensusConfig) -> Self {
        Self {
            storage,
            chain_id,
            config,
            cache: ValidatorCache::default(),
        }
    }

    /// Returns the validator set for `height`, fetching from L2 only on a cache
    /// miss.
    fn validator_set_at(
        &self,
        height: u64,
    ) -> Result<Arc<ValidatorSet<ContractAddress>>, anyhow::Error> {
        // Upper bound on the number of distinct heights whose validator sets
        // are kept in memory. Consensus advances monotonically, so when the
        // cache is full we evict the smallest key, which approximates LRU for
        // the expected workload.
        let max_cached_heights = self.config.history_depth.try_into().unwrap_or(10);
        self.cache
            .get_or_insert_with(height, max_cached_heights, || {
                fetch_validators(&self.storage, self.chain_id, height, &self.config)
            })
    }
}

impl pathfinder_consensus::ValidatorSetProvider<ContractAddress> for L2ValidatorSetProvider {
    fn get_validator_set(&self, height: u64) -> anyhow::Result<ValidatorSet<ContractAddress>> {
        self.validator_set_at(height)
            .map(|vset| vset.as_ref().clone())
    }
}

// TODO:
//
// Currently, the validator fetching functionality lives in its own crate
// (validator-fetcher) because we have a temporary internal RPC method that we
// use for convenient testing.
//
// This separation allows us to easily expose and test the functionality through
// the RPC while the specification for validator fetching is still being
// finalized.
//
// Once we have a final spec, the functionality from the validator-fetcher crate
// will be migrated into this file and the temporary crate (along with its RPC
// method) will be removed.

/// Fetches validators for a given height
///
/// Uses config-based validators if validator addresses are provided in config,
/// otherwise fetches validators from the contract.
pub fn fetch_validators(
    storage: &Storage,
    chain_id: ChainId,
    height: u64,
    config: &ConsensusConfig,
) -> Result<ValidatorSet<ContractAddress>, anyhow::Error> {
    if config.validator_addresses.is_empty() {
        fetch_validators_from_l2(storage, chain_id, height)
    } else {
        create_validators_from_config(config)
    }
}

/// Creates validators from consensus config
///
/// This is the original logic that was in consensus_task.rs.
/// It creates validators with random keys and equal voting power.
fn create_validators_from_config(
    config: &ConsensusConfig,
) -> Result<ValidatorSet<ContractAddress>, anyhow::Error> {
    let validator_address = config.my_validator_address;

    let validators = std::iter::once(validator_address)
        .chain(config.validator_addresses.clone())
        .map(|address| {
            // TODO: This is obviously not production ready.
            let sk = SigningKey::new(OsRng);
            let vk = sk.verification_key();
            let public_key = PublicKey::from_bytes(vk.to_bytes());

            Validator {
                address,
                public_key,
                voting_power: 1,
            }
        })
        .collect::<Vec<Validator<ContractAddress>>>();

    Ok(ValidatorSet::new(validators))
}

/// Fetches validators from the L2 contract
///
/// This logic is temporary until we have a final spec for validator fetching.
fn fetch_validators_from_l2(
    storage: &Storage,
    chain_id: ChainId,
    height: u64,
) -> Result<ValidatorSet<ContractAddress>, anyhow::Error> {
    let validators = consensus_fetcher::get_validators_at_height(storage, chain_id, height)?;
    let validators = validators
        .into_iter()
        .map(|validator| Validator {
            address: validator.address,
            public_key: validator.public_key,
            voting_power: validator.voting_power,
        })
        .collect::<Vec<Validator<ContractAddress>>>();
    Ok(ValidatorSet::new(validators))
}
