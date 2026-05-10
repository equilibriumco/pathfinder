use p2p::consensus::HeightAndRound;
use p2p_proto::consensus::{ProposalInit, ProposalPart};
use pathfinder_common::ContractAddress;
use pathfinder_validator::error::{ProposalError, ProposalHandlingError};

/// Validates the structure of incoming proposal parts and stores them.
///
/// Enforces the following order:
/// 1. Proposal Init (must be first)
/// 2. Transaction Batch (zero or more times)
/// 3. Proposal Fin (must be last)
///
/// Empty proposals consist of Init + Fin only.
pub struct ProposalPartsValidator {
    height_and_round: HeightAndRound,
    parts: Vec<ProposalPart>,
    has_init: bool,
    has_fin: bool,
    transaction_batch_count: usize,
    proposer_address: Option<ContractAddress>,
    valid_round: Option<u32>,
}

/// Result of validating and accepting a proposal part.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidationResult {
    /// Part is valid; proposal is still being assembled.
    Accepted,
    /// Fin received: this is an empty proposal (Init + Fin only).
    EmptyProposal,
    /// Fin received: this is a non-empty proposal (Init + content + Fin).
    NonEmptyProposal,
}

impl ProposalPartsValidator {
    pub fn new(height_and_round: HeightAndRound) -> Self {
        Self {
            height_and_round,
            parts: Vec::new(),
            has_init: false,
            has_fin: false,
            transaction_batch_count: 0,
            proposer_address: None,
            valid_round: None,
        }
    }

    /// Validate structure and store the part.
    pub fn accept_part(
        &mut self,
        part: &ProposalPart,
    ) -> Result<ValidationResult, ProposalHandlingError> {
        match part {
            ProposalPart::Init(init) => self.accept_init(init),
            ProposalPart::TransactionBatch(tx_batch) => {
                self.accept_transaction_batch(tx_batch, part)
            }
            ProposalPart::Fin(_) => self.accept_fin(part),
        }
    }

    /// Returns the proposer address extracted from the Init part, or `None` if
    /// Init has not been accepted yet.
    pub fn proposer_address(&self) -> Option<ContractAddress> {
        self.proposer_address
    }

    /// Returns the valid round extracted from the Init part.
    pub fn valid_round(&self) -> Option<u32> {
        self.valid_round
    }

    /// Returns the stored proposal parts.
    pub fn parts(&self) -> &[ProposalPart] {
        &self.parts
    }

    fn accept_init(
        &mut self,
        init: &ProposalInit,
    ) -> Result<ValidationResult, ProposalHandlingError> {
        if !self.parts.is_empty() {
            return Err(ProposalHandlingError::Recoverable(
                ProposalError::UnexpectedProposalPart {
                    message: format!(
                        "Unexpected proposal Init for {} at position {}",
                        self.height_and_round,
                        self.parts.len()
                    ),
                },
            ));
        }

        self.has_init = true;
        self.proposer_address = Some(ContractAddress(init.proposer.0));
        self.valid_round = init.valid_round;
        self.parts.push(ProposalPart::Init(init.clone()));
        Ok(ValidationResult::Accepted)
    }

    fn accept_transaction_batch(
        &mut self,
        tx_batch: &[p2p_proto::consensus::Transaction],
        part: &ProposalPart,
    ) -> Result<ValidationResult, ProposalHandlingError> {
        if self.parts.is_empty() {
            return Err(ProposalHandlingError::Recoverable(
                ProposalError::UnexpectedProposalPart {
                    message: format!(
                        "Unexpected proposal TransactionBatch for {} at initial position",
                        self.height_and_round,
                    ),
                },
            ));
        }

        if tx_batch.is_empty() {
            return Err(ProposalHandlingError::Recoverable(
                ProposalError::UnexpectedProposalPart {
                    message: format!(
                        "Received empty TransactionBatch for {} at position {}",
                        self.height_and_round,
                        self.parts.len()
                    ),
                },
            ));
        }

        self.transaction_batch_count += 1;
        self.parts.push(part.clone());
        Ok(ValidationResult::Accepted)
    }

    fn accept_fin(
        &mut self,
        part: &ProposalPart,
    ) -> Result<ValidationResult, ProposalHandlingError> {
        if self.has_fin {
            return Err(ProposalHandlingError::Recoverable(
                ProposalError::UnexpectedProposalPart {
                    message: format!("Duplicate ProposalFin for {}", self.height_and_round),
                },
            ));
        }

        // Empty proposal: Init + Fin
        if self.parts.len() == 1 && self.has_init {
            self.has_fin = true;
            self.parts.push(part.clone());
            return Ok(ValidationResult::EmptyProposal);
        }

        // Non-empty proposal: at least Init + transaction batch before Fin
        if self.parts.len() >= 2 && self.has_init {
            self.has_fin = true;
            self.parts.push(part.clone());
            return Ok(ValidationResult::NonEmptyProposal);
        }

        Err(ProposalHandlingError::Recoverable(
            ProposalError::UnexpectedProposalPart {
                message: format!(
                    "Unexpected proposal ProposalFin for {} at position {}",
                    self.height_and_round,
                    self.parts.len()
                ),
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    //! Proptests for [`ProposalPartsValidator`] structural validation.
    //!
    //! Goal: Verify that the validator correctly accepts well-formed proposal
    //! part sequences and rejects malformed ones.
    //!
    //! These tests focus exclusively on structural rules: part ordering,
    //! duplicates, empty batches. Execution and transaction mapping are covered
    //! by the `p2p_task_tests` integration tests.
    //!
    //! How it works:
    //! 1. We generate valid proposals (empty and non-empty).
    //! 2. Apply random structural mutations (see [`Mutation`])
    //! 3. Assert that the validator rejects every mutated proposal.
    //!
    //! Every variant in [Mutation] documents what validation rule is being
    //! targeted.

    use fake::Fake;
    use p2p::consensus::HeightAndRound;
    use p2p_proto::consensus::{ProposalPart, Transaction};
    use proptest::prelude::*;
    use rand::seq::SliceRandom;
    use rand::{Rng, SeedableRng};

    use super::*;

    /// A single structural mutation to apply to a valid proposal to make it
    /// invalid. Each variant targets a specific validation rule.
    #[derive(Debug, Clone, Copy)]
    enum Mutation {
        /// Init must appear exactly once, first.
        RemoveInit,
        /// Init must appear exactly once, first.
        DuplicateInit,
        /// Non-empty proposals need at least one transaction batch.
        RemoveAllTransactionBatches,
        /// Empty batches are rejected.
        AddEmptyTransactionBatch,
        /// Fin must appear at most once.
        DuplicateFin,
        /// Random reordering breaks the required part sequence.
        ShuffleAll,
    }

    const ALL_MUTATIONS: &[Mutation] = &[
        Mutation::RemoveInit,
        Mutation::DuplicateInit,
        Mutation::RemoveAllTransactionBatches,
        Mutation::AddEmptyTransactionBatch,
        Mutation::DuplicateFin,
        Mutation::ShuffleAll,
    ];

    /// Generates a valid empty proposal: `[Init, Fin]`
    ///
    /// All fields (proposer, valid_round, commitment) are randomly generated.
    fn create_valid_empty_proposal(seed: u64) -> Vec<ProposalPart> {
        let mut rng = rand_chacha::ChaCha12Rng::seed_from_u64(seed);

        vec![
            ProposalPart::Init(fake::Faker.fake_with_rng(&mut rng)),
            ProposalPart::Fin(fake::Faker.fake_with_rng(&mut rng)),
        ]
    }

    /// Generates a valid non-empty proposal:
    /// `[Init, <random content parts>, Fin]`
    ///
    /// Content parts are 1..50 transactions split into randomly-sized batches,
    /// shuffled among each other.
    fn create_valid_non_empty_proposal(seed: u64) -> Vec<ProposalPart> {
        let mut rng = rand_chacha::ChaCha12Rng::seed_from_u64(seed);

        let mut parts = Vec::new();

        parts.push(ProposalPart::Init(fake::Faker.fake_with_rng(&mut rng)));

        // Content parts in random order
        let num_txns: usize = rng.gen_range(1..50);
        let transactions: Vec<Transaction> = (0..num_txns)
            .map(|_| fake::Faker.fake_with_rng(&mut rng))
            .collect();

        let mut content_parts: Vec<ProposalPart> = split_random(&transactions, &mut rng)
            .into_iter()
            .map(ProposalPart::TransactionBatch)
            .collect();

        content_parts.shuffle(&mut rng);

        parts.extend(content_parts);

        parts.push(ProposalPart::Fin(fake::Faker.fake_with_rng(&mut rng)));

        parts
    }

    /// Generates a structurally invalid proposal by taking a valid non-empty
    /// proposal and applying one random mutation. If the chosen mutation
    /// happens to not break validity (e.g. shuffle lands in a valid order),
    /// the proposal is force-broken by removing Init or prepending Fin.
    fn create_invalid_proposal(seed: u64) -> Vec<ProposalPart> {
        let mut rng = rand_chacha::ChaCha12Rng::seed_from_u64(seed);

        // Use a different seed for the base proposal so they don't correlate
        let mut parts = create_valid_non_empty_proposal(seed.wrapping_add(1));
        let mutation = ALL_MUTATIONS[rng.gen_range(0..ALL_MUTATIONS.len())];

        apply_mutation(&mut parts, &mut rng, mutation);

        // If the mutation didn't actually break the proposal, force-break it
        if !would_cause_error(&parts) {
            if !parts.is_empty() && parts[0].is_proposal_init() {
                parts.remove(0);
            } else {
                // Prepend a Fin so the first part isn't Init
                parts.insert(0, ProposalPart::Fin(fake::Faker.fake_with_rng(&mut rng)));
            }
        }

        // If parts became empty from mutations, create a minimal invalid sequence
        if parts.is_empty() {
            parts.push(ProposalPart::Fin(fake::Faker.fake_with_rng(&mut rng)));
        }

        parts
    }

    fn apply_mutation(parts: &mut Vec<ProposalPart>, rng: &mut impl Rng, mutation: Mutation) {
        match mutation {
            Mutation::RemoveInit => {
                parts.retain(|p| !p.is_proposal_init());
            }
            Mutation::DuplicateInit => {
                if let Some(init) = parts.iter().find(|p| p.is_proposal_init()).cloned() {
                    let pos = rng.gen_range(0..=parts.len());
                    parts.insert(pos, init);
                }
            }
            Mutation::RemoveAllTransactionBatches => {
                parts.retain(|p| !p.is_transaction_batch());
            }
            Mutation::AddEmptyTransactionBatch => {
                let pos = rng.gen_range(0..=parts.len());
                parts.insert(pos, ProposalPart::TransactionBatch(vec![]));
            }
            Mutation::DuplicateFin => {
                if let Some(fin) = parts.iter().find(|p| p.is_proposal_fin()).cloned() {
                    let pos = rng.gen_range(0..=parts.len());
                    parts.insert(pos, fin);
                }
            }
            Mutation::ShuffleAll => {
                parts.shuffle(rng);
            }
        }
    }

    /// Checks whether the given parts would cause at least one validation
    /// error when fed through a `ProposalPartsValidator`.
    fn would_cause_error(parts: &[ProposalPart]) -> bool {
        let mut validator = ProposalPartsValidator::new(HeightAndRound::new(0, 0));
        for part in parts {
            if validator.accept_part(part).is_err() {
                return true;
            }
        }
        false
    }

    /// Extracts the valid_round from the Init part of a proposal.
    fn extract_valid_round(parts: &[ProposalPart]) -> Option<u32> {
        match parts.first() {
            Some(ProposalPart::Init(init)) => init.valid_round,
            _ => panic!("first part should be Init"),
        }
    }

    fn part_name(part: &ProposalPart) -> &'static str {
        match part {
            ProposalPart::Init(_) => "Init",
            ProposalPart::Fin(_) => "Fin",
            ProposalPart::TransactionBatch(_) => "TransactionBatch",
        }
    }

    // An empty proposal [Init, Fin] must be fully accepted: Init returns
    // Accepted, Fin returns EmptyProposal, and the proposer address is captured.
    #[test]
    fn valid_empty_proposal_is_accepted() {
        let parts = create_valid_empty_proposal(42);
        let mut validator = ProposalPartsValidator::new(HeightAndRound::new(0, 0));

        let result = validator.accept_part(&parts[0]).unwrap();
        assert_eq!(result, ValidationResult::Accepted);

        let result = validator.accept_part(&parts[1]).unwrap();
        assert_eq!(result, ValidationResult::EmptyProposal);

        assert!(validator.proposer_address().is_some());
        assert_eq!(validator.valid_round(), extract_valid_round(&parts));
        assert_eq!(validator.parts().len(), 2);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(200))]

        // A non-empty proposal [Init, <content>, Fin] with
        // randomly-sized transaction batches in random order must be
        // fully accepted: all intermediate parts return Accepted, Fin
        // returns NonEmptyProposal, and all parts are stored.
        #[test]
        fn valid_non_empty_proposals_are_accepted(seed in any::<u64>()) {
            let parts = create_valid_non_empty_proposal(seed);
            let mut validator = ProposalPartsValidator::new(HeightAndRound::new(0, 0));
            let last_idx = parts.len() - 1;
            for (i, part) in parts.iter().enumerate() {
                let result = validator.accept_part(part)
                    .map_err(|e| TestCaseError::Fail(
                        format!(
                            "Part {} ({}) failed: {e}",
                            i,
                            part_name(part),
                        ).into()
                    ))?;
                if i < last_idx {
                    prop_assert_eq!(result, ValidationResult::Accepted,
                        "Part {} ({}) should be Accepted", i, part_name(part));
                } else {
                    prop_assert_eq!(result, ValidationResult::NonEmptyProposal,
                        "Last part should be NonEmptyProposal");
                }
            }

            prop_assert!(validator.proposer_address().is_some());
            prop_assert_eq!(validator.valid_round(), extract_valid_round(&parts));
            prop_assert_eq!(validator.parts().len(), parts.len());
        }

        // A valid non-empty proposal with one random structural mutation applied
        // must be rejected: at least one call to `accept_part` must return an error.
        #[test]
        fn invalid_proposals_are_rejected(seed in any::<u64>()) {
            let parts = create_invalid_proposal(seed);
            let mut validator = ProposalPartsValidator::new(HeightAndRound::new(0, 0));
            let mut had_error = false;
            for part in &parts {
                match validator.accept_part(part) {
                    Ok(_) => {}
                    Err(_) => {
                        had_error = true;
                        break;
                    }
                }
            }
            prop_assert!(had_error,
                "Expected at least one validation error for invalid proposal: {:?}",
                parts.iter().map(part_name).collect::<Vec<_>>()
            );
        }
    }

    /// Splits a slice into random non-empty contiguous chunks by randomly
    /// placing boundaries between elements.
    ///
    /// Example: `[A, B, C, D, E]` with a boundary at index 2
    ///  => boundaries = `[0, 2, 5]`
    ///  => chunks `[A,B]`, `[C,D,E]`
    fn split_random<T: Clone>(v: &[T], rng: &mut impl Rng) -> Vec<Vec<T>> {
        if v.is_empty() {
            return vec![];
        }

        // Always start at 0
        let mut boundaries = vec![0usize];

        // Walk between elements, 30% chance of placing a cut
        for i in 1..v.len() {
            if rng.gen_bool(0.3) {
                boundaries.push(i);
            }
        }

        // Always end at len
        boundaries.push(v.len());

        // Each pair of consecutive boundaries defines one chunk
        boundaries
            .windows(2)
            .map(|w| v[w[0]..w[1]].to_vec())
            .collect()
    }
}
