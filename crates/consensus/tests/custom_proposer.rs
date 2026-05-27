use pathfinder_consensus::{ProposerSelector, PublicKey, SigningKey, Validator, ValidatorSet};

mod common;
use common::NodeAddress;

/// A proposer selector that always selects the first validator
#[derive(Clone)]
struct FirstValidatorSelector;

impl ProposerSelector<NodeAddress> for FirstValidatorSelector {
    fn select_proposer<'a>(
        &self,
        validator_set: &'a ValidatorSet<NodeAddress>,
        _height: u64,
        _round: u32,
    ) -> anyhow::Result<&'a Validator<NodeAddress>> {
        Ok(&validator_set.validators[0])
    }
}

/// A proposer selector that always selects the last validator
#[derive(Clone)]
struct LastValidatorSelector;

impl ProposerSelector<NodeAddress> for LastValidatorSelector {
    fn select_proposer<'a>(
        &self,
        validator_set: &'a ValidatorSet<NodeAddress>,
        _height: u64,
        _round: u32,
    ) -> anyhow::Result<&'a Validator<NodeAddress>> {
        let last_index = validator_set.validators.len() - 1;
        Ok(&validator_set.validators[last_index])
    }
}

#[test]
fn test_proposer_selection_logic_direct() {
    // Create validators with distinct addresses
    let sk1 = SigningKey::new(rand::rngs::OsRng);
    let pk1 = sk1.verification_key();
    let sk2 = SigningKey::new(rand::rngs::OsRng);
    let pk2 = sk2.verification_key();
    let sk3 = SigningKey::new(rand::rngs::OsRng);
    let pk3 = sk3.verification_key();

    let validator1 = NodeAddress("validator_1".to_string());
    let validator2 = NodeAddress("validator_2".to_string());
    let validator3 = NodeAddress("validator_3".to_string());

    let validators = ValidatorSet::new(vec![
        Validator::new(validator1.clone(), PublicKey::from_bytes(pk1.to_bytes())),
        Validator::new(validator2.clone(), PublicKey::from_bytes(pk2.to_bytes())),
        Validator::new(validator3.clone(), PublicKey::from_bytes(pk3.to_bytes())),
    ]);

    // Test first validator selector
    let first_selector = FirstValidatorSelector;
    let selected = first_selector.select_proposer(&validators, 1, 0).unwrap();
    assert_eq!(selected.address, validator1);

    // Test last validator selector
    let last_selector = LastValidatorSelector;
    let selected = last_selector.select_proposer(&validators, 1, 0).unwrap();
    assert_eq!(selected.address, validator3);

    // Test round-robin selector
    let round_robin = pathfinder_consensus::RoundRobinProposerSelector;

    // Round 0 should select first validator
    let selected = round_robin.select_proposer(&validators, 1, 0).unwrap();
    assert_eq!(selected.address, validator1);

    // Round 1 should select second validator
    let selected = round_robin.select_proposer(&validators, 1, 1).unwrap();
    assert_eq!(selected.address, validator2);

    // Round 2 should select third validator
    let selected = round_robin.select_proposer(&validators, 1, 2).unwrap();
    assert_eq!(selected.address, validator3);

    // Round 3 should wrap around to first validator
    let selected = round_robin.select_proposer(&validators, 1, 3).unwrap();
    assert_eq!(selected.address, validator1);
}
