use pathfinder_common::{ChainId, ContractAddress};
use pathfinder_consensus::{PublicKey, SigningKey, Validator, ValidatorSet};
use pathfinder_consensus_fetcher as consensus_fetcher;
use pathfinder_storage::Storage;
use rand::rngs::OsRng;

use crate::config::ConsensusConfig;

#[derive(Clone)]
pub struct L2ValidatorSetProvider {
    storage: Storage,
    chain_id: ChainId,
    config: ConsensusConfig,
}

impl L2ValidatorSetProvider {
    pub fn new(storage: Storage, chain_id: ChainId, config: ConsensusConfig) -> Self {
        Self {
            storage,
            chain_id,
            config,
        }
    }
}

impl pathfinder_consensus::ValidatorSetProvider<ContractAddress> for L2ValidatorSetProvider {
    fn get_validator_set(
        &self,
        height: u64,
    ) -> Result<ValidatorSet<ContractAddress>, anyhow::Error> {
        fetch_validators(&self.storage, self.chain_id, height, &self.config)
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
