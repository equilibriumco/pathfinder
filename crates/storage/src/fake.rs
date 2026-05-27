//! Create fake blockchain storage for test purposes
use std::collections::{HashMap, HashSet};
use std::ops::RangeInclusive;

use fake::{Fake, Faker};
use pathfinder_class_hash::compute_class_hash;
use pathfinder_common::class_definition::{
    SerializedCairoDefinition,
    SerializedCasmDefinition,
    SerializedOpaqueClassDefinition,
    SerializedSierraDefinition,
};
use pathfinder_common::event::Event;
use pathfinder_common::prelude::*;
use pathfinder_common::receipt::Receipt;
use pathfinder_common::state_update::{
    ContractClassUpdate,
    ContractUpdate,
    StateUpdateError,
    StateUpdateRef,
    SystemContractUpdate,
};
use pathfinder_common::test_utils::fake_non_empty_with_rng;
use pathfinder_common::transaction::Transaction;
use pathfinder_common::SignedBlockHeader;
use pathfinder_crypto::signature::SignatureError;
use pathfinder_crypto::Felt;
use rand::seq::IteratorRandom;
use rand::Rng;

use crate::{Storage, StorageBuilder};

#[derive(Debug, Default, Clone, PartialEq)]
pub struct Block {
    pub header: SignedBlockHeader,
    pub transaction_data: Vec<(Transaction, Receipt, Vec<Event>)>,
    /// Wrapping in an [`Option`] allows for easy removal of the state update if
    /// the caller wishes to fill the db with _partial_ block data.
    /// [`generate`] will always populate this field with some fake state
    /// update and the user can choose to remove it prior to calling
    /// [`fill`] by setting it to `None`.
    pub state_update: Option<StateUpdate>,
    // Cairo 0 definitions
    pub cairo_defs: Vec<(ClassHash, SerializedCairoDefinition)>,
    // Sierra + Casm definitions + Casm Blake2 hash
    pub sierra_defs: Vec<(
        SierraHash,
        SerializedSierraDefinition,
        SerializedCasmDefinition,
        CasmHash,
    )>,
}

pub type BlockHashFn = Box<dyn Fn(&BlockHeader) -> BlockHash>;
pub type SignBlockHashFn = Box<dyn Fn(BlockHash) -> Result<(Felt, Felt), SignatureError>>;
pub type TransactionCommitmentFn =
    Box<dyn Fn(&[Transaction], StarknetVersion) -> anyhow::Result<TransactionCommitment>>;
pub type ReceiptCommitmentFn = Box<dyn Fn(&[Receipt]) -> anyhow::Result<ReceiptCommitment>>;
pub type EventCommitmentFn =
    Box<dyn Fn(&[(TransactionHash, &[Event])], StarknetVersion) -> anyhow::Result<EventCommitment>>;
pub type UpdateTriesFn = Box<
    dyn Fn(
        &crate::Transaction<'_>,
        StateUpdateRef<'_>,
        bool,
        BlockNumber,
        Storage,
    )
        -> Result<(StorageCommitment, ClassCommitment, crate::TrieBreakdown), StateUpdateError>,
>;

pub struct Config {
    pub calculate_block_hash: BlockHashFn,
    pub sign_block_hash: SignBlockHashFn,
    pub calculate_transaction_commitment: TransactionCommitmentFn,
    pub calculate_receipt_commitment: ReceiptCommitmentFn,
    pub calculate_event_commitment: EventCommitmentFn,
    pub update_tries: UpdateTriesFn,
    pub occurrence: OccurrencePerBlock,
}

pub struct OccurrencePerBlock {
    pub cairo: RangeInclusive<usize>,
    pub sierra: RangeInclusive<usize>,
    pub storage: RangeInclusive<usize>,
    pub nonce: RangeInclusive<usize>,
    /// Ranges longer than `0..=1` will be truncated to `0..=1`
    pub system_storage: RangeInclusive<usize>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            calculate_block_hash: Box::new(|_| Faker.fake()),
            sign_block_hash: Box::new(|_| Ok((Faker.fake(), Faker.fake()))),
            calculate_transaction_commitment: Box::new(|_, _| Ok(Faker.fake())),
            calculate_receipt_commitment: Box::new(|_| Ok(Faker.fake())),
            calculate_event_commitment: Box::new(|_, _| Ok(Faker.fake())),
            update_tries: Box::new(|_, _, _, _, _| {
                Ok((Faker.fake(), Faker.fake(), crate::TrieBreakdown::default()))
            }),
            occurrence: Default::default(),
        }
    }
}

impl Default for OccurrencePerBlock {
    fn default() -> Self {
        Self {
            cairo: 0..=10,
            sierra: 0..=10,
            storage: 0..=10,
            nonce: 0..=10,
            system_storage: 0..=1,
        }
    }
}

pub fn fill(storage: &Storage, blocks: &[Block], update_tries: Option<UpdateTriesFn>) {
    let mut db = storage.connection().unwrap();
    let db = db.transaction().unwrap();

    blocks.iter().for_each(
        |Block {
             header,
             transaction_data,
             state_update,
             cairo_defs,
             sierra_defs,
             ..
         }| {
            db.insert_block_header(&header.header).unwrap();
            db.insert_signature(header.header.number, &header.signature)
                .unwrap();
            db.insert_transaction_data(
                header.header.number,
                &transaction_data
                    .iter()
                    .cloned()
                    .map(|(tx, receipt, ..)| (tx, receipt))
                    .collect::<Vec<_>>(),
                Some(
                    &transaction_data
                        .iter()
                        .cloned()
                        .map(|(_, _, events)| events)
                        .collect::<Vec<_>>(),
                ),
            )
            .unwrap();

            if let Some(state_update) = state_update {
                db.insert_state_update(header.header.number, state_update)
                    .unwrap();

                if let Some(update_tries) = &update_tries {
                    update_tries(
                        &db,
                        state_update.into(),
                        false,
                        header.header.number,
                        storage.clone(),
                    )
                    .unwrap();
                }
            }

            cairo_defs.iter().for_each(|(cairo_hash, definition)| {
                db.update_cairo_class_definition(*cairo_hash, definition)
                    .unwrap()
            });

            sierra_defs.iter().for_each(
                |(sierra_hash, sierra_definition, casm_definition, casm_hash_v2)| {
                    db.update_sierra_class_definition(
                        sierra_hash,
                        sierra_definition,
                        casm_definition,
                        casm_hash_v2,
                    )
                    .unwrap()
                },
            );
        },
    );

    db.commit().unwrap();
}

/// Create fake blocks and state updates with __limited consistency
/// guarantees__:
/// - starknet version: 0.13.2
/// - chain id: `SEPOLIA_TESTNET`
/// - block headers:
///     - consecutive numbering starting from genesis (`0`) up to `n-1`
///     - parent hash of block N points to hash of block N-1, parent hash of
///       genesis is 0
///     - state commitment is a hash of storage and class commitments
///     - state diff length and commitment are correctly calculated from its
///       respective state update
/// - block bodies:
///     - transaction indices within a block
///     - transaction hashes in respective receipts
///     - at least 1 transaction with receipt per block
/// - state updates:
///     - block hashes
///     - parent state commitment of block N points to state commitment of block
///       N-1, parent state commitment of genesis is 0
///     - no replaced classes
///     - each storage diff has its respective nonce update
///     - storage entries constrain at least 1 element
///     - no implicitly declared classes (ie. as in the old deploy transactions
///       that were not preceded by a declare transaction)
/// - declared cairo|sierra definitions
///     - class definition is a serialized to JSON representation of
///       `class_definition::Cairo|Sierra` respectively with random fields
///     - all those definitions are very short and fall far below the soft limit
///       in protobuf encoding
///     - casm definitions for sierra classes are purely random Strings
///     - cairo class hashes and sierra class hashes are correctly calculated
///       from the definitions, casm hashes are random
///     - fake sierra classes never compile successfully
/// - transactions
///     - transaction hashes are calculated from their respective variant
pub mod generate {
    use pathfinder_common::class_definition::{self, SerializedClassDefinition};
    use pathfinder_common::{BlockCommitmentSignature, BlockCommitmentSignatureElem};

    use super::*;

    pub fn n_blocks(n: usize) -> Vec<Block> {
        with_config(n, Default::default())
    }

    pub fn with_config(n: usize, config: Config) -> Vec<Block> {
        with_rng_and_config(n, &mut rand::thread_rng(), config)
    }

    pub fn with_rng_and_config<R: Rng>(n: usize, rng: &mut R, config: Config) -> Vec<Block> {
        let Config {
            calculate_block_hash,
            sign_block_hash,
            calculate_transaction_commitment,
            calculate_receipt_commitment,
            calculate_event_commitment,
            update_tries,
            occurrence: min_per_block,
        } = config;

        let mut blocks = generate_inner(
            n,
            rng,
            calculate_transaction_commitment,
            calculate_receipt_commitment,
            calculate_event_commitment,
            min_per_block,
        );

        update_commitments(&mut blocks, update_tries);
        compute_block_hashes(&mut blocks, calculate_block_hash, sign_block_hash);
        blocks
    }

    fn generate_inner<R: Rng>(
        n: usize,
        rng: &mut R,
        calculate_transaction_commitment: TransactionCommitmentFn,
        calculate_receipt_commitment: ReceiptCommitmentFn,
        calculate_event_commitment: EventCommitmentFn,
        occurrence: OccurrencePerBlock,
    ) -> Vec<Block> {
        let mut init = Vec::with_capacity(n);
        let mut declared_classes_accum = HashSet::new();

        for i in 0..n {
            let mut header: BlockHeader = Faker.fake_with_rng(rng);
            header.starknet_version = StarknetVersion::V_0_13_2;
            header.number =
                BlockNumber::new_or_panic(i.try_into().expect("u64 is at least as wide as usize"));
            // Will be fixed after inserting tries
            header.state_commitment = StateCommitment::ZERO;

            // There must be at least 1 transaction per block
            let transaction_data = fake_non_empty_with_rng::<
                Vec<_>,
                crate::connection::transaction::dto::TransactionV3,
            >(rng)
            .into_iter()
            .enumerate()
            .map(|(i, t)| {
                let mut t: Transaction = t.into();
                let transaction_hash = t.variant.calculate_hash(ChainId::SEPOLIA_TESTNET, false);
                t.hash = transaction_hash;

                let r: Receipt = crate::connection::transaction::dto::ReceiptV3 {
                    transaction_hash: transaction_hash.as_inner().to_owned().into(),
                    transaction_index: TransactionIndex::new_or_panic(
                        i.try_into().expect("u64 is at least as wide as usize"),
                    ),
                    ..Faker.fake_with_rng(rng)
                }
                .into();
                let e: Vec<Event> = fake_non_empty_with_rng(rng);
                (t, r, e)
            })
            .collect::<Vec<_>>();

            header.transaction_commitment = (calculate_transaction_commitment)(
                &transaction_data
                    .iter()
                    .map(|(t, ..)| t.clone())
                    .collect::<Vec<_>>(),
                header.starknet_version,
            )
            .unwrap();

            header.event_commitment = (calculate_event_commitment)(
                &transaction_data
                    .iter()
                    .map(|(t, _, e)| (t.hash, e.as_slice()))
                    .collect::<Vec<_>>(),
                header.starknet_version,
            )
            .unwrap();

            header.receipt_commitment = (calculate_receipt_commitment)(
                &transaction_data
                    .iter()
                    .map(|(_, r, ..)| r.clone())
                    .collect::<Vec<_>>(),
            )
            .unwrap();

            header.transaction_count = transaction_data.len();
            header.event_count = transaction_data
                .iter()
                .map(|(_, _, events)| events.len())
                .sum();

            let num_cairo_classes = rng.gen_range(occurrence.cairo.clone());
            let num_sierra_classes = rng.gen_range(occurrence.sierra.clone());

            let cairo_defs = (0..num_cairo_classes)
                .map(|_| {
                    let def = serde_json::to_vec(
                        &Faker.fake_with_rng::<class_definition::Cairo<'_>, _>(rng),
                    )
                    .unwrap();
                    let def = SerializedOpaqueClassDefinition::from_vec(def);
                    let (hash, def) = compute_class_hash(def).unwrap();
                    let SerializedClassDefinition::Cairo(def) = def else {
                        panic!("Expected a Cairo class definition");
                    };
                    (hash.hash(), def)
                })
                .collect::<HashMap<_, _>>();
            let sierra_defs = (0..num_sierra_classes)
                .map(|_| {
                    let def = serde_json::to_vec(
                        &Faker.fake_with_rng::<class_definition::Sierra<'_>, _>(rng),
                    )
                    .unwrap();
                    let def = SerializedOpaqueClassDefinition::from_vec(def);
                    let (hash, sierra_def) = compute_class_hash(def).unwrap();
                    let SerializedClassDefinition::Sierra(sierra_def) = sierra_def else {
                        panic!("Expected a Sierra class definition");
                    };
                    let hash = SierraHash(hash.hash().0);
                    let casm_def = SerializedCasmDefinition::from_vec(
                        Faker.fake_with_rng::<String, _>(rng).into_bytes(),
                    );
                    (
                        hash,
                        (
                            sierra_def,
                            casm_def,
                            Faker.fake_with_rng::<CasmHash, _>(rng),
                        ),
                    )
                })
                .collect::<HashMap<_, _>>();

            let declared_cairo_classes = cairo_defs.keys().copied().collect::<HashSet<_>>();
            let declared_sierra_classes = sierra_defs
                .keys()
                .map(|sierra_hash| (*sierra_hash, Faker.fake()))
                .collect::<HashMap<_, _>>();

            let all_declared_classes_in_this_block = declared_cairo_classes
                .iter()
                .copied()
                .chain(declared_sierra_classes.keys().map(|x| ClassHash(x.0)))
                .collect::<HashSet<_>>();

            let storage_updates = rng.gen_range(occurrence.storage.clone());
            let nonce_updates = rng.gen_range(occurrence.nonce.clone());

            let num_contract_updates = storage_updates.max(nonce_updates);

            let mut do_storage_update = vec![false; num_contract_updates];
            (0..num_contract_updates)
                .choose_multiple(rng, storage_updates)
                .into_iter()
                .for_each(|i| do_storage_update[i] = true);
            let mut do_nonce_update = vec![false; num_contract_updates];
            (0..num_contract_updates)
                .choose_multiple(rng, nonce_updates)
                .into_iter()
                .for_each(|i| do_nonce_update[i] = true);

            init.push(Block {
                header: SignedBlockHeader {
                    header,
                    signature: Faker.fake_with_rng(rng),
                },
                transaction_data,
                state_update: Some(StateUpdate {
                    // Will be fixed after block hash computation
                    block_hash: BlockHash::ZERO,
                    // Will be fixed after inserting tries
                    state_commitment: StateCommitment::ZERO,
                    // Will be fixed after inserting tries
                    parent_state_commitment: StateCommitment::ZERO,
                    declared_cairo_classes,
                    declared_sierra_classes,
                    system_contract_updates: if occurrence.system_storage.contains(&1) {
                        [
                            (
                                ContractAddress::ONE,
                                SystemContractUpdate {
                                    storage: fake_non_empty_with_rng(rng),
                                },
                            ),
                            (
                                ContractAddress::TWO,
                                SystemContractUpdate {
                                    storage: fake_non_empty_with_rng(rng),
                                },
                            ),
                        ]
                        .into_iter()
                        .collect()
                    } else {
                        Default::default()
                    },
                    contract_updates: {
                        // We can only deploy what was declared so far in the chain
                        if declared_classes_accum.is_empty() {
                            Default::default()
                        } else {
                            (0..num_contract_updates)
                                .map(|i| {
                                    (
                                        Faker.fake_with_rng::<ContractAddress, _>(rng),
                                        ContractUpdate {
                                            class: Some(ContractClassUpdate::Deploy(
                                                *declared_classes_accum.iter().choose(rng).unwrap(),
                                            )),
                                            storage: if do_storage_update[i] {
                                                fake_non_empty_with_rng(rng)
                                            } else {
                                                Default::default()
                                            },
                                            nonce: if do_nonce_update[i] {
                                                Faker.fake()
                                            } else {
                                                Default::default()
                                            },
                                        },
                                    )
                                })
                                .collect()
                        }
                    },
                    migrated_compiled_classes: Default::default(),
                }),
                cairo_defs: cairo_defs.into_iter().collect(),
                sierra_defs: sierra_defs
                    .into_iter()
                    .map(|(h, (s, c, casm_hash_v2))| (h, s, c, casm_hash_v2))
                    .collect(),
            });

            // These new classes from this block can now be deployed in the next blocks
            declared_classes_accum.extend(all_declared_classes_in_this_block);
        }

        // FIXME Previous way of faking replaced classes made trie generation using
        // `update_starknet_state` unstable, ie. state roots did not match
        // between the generated block data and what was computed as a result of
        // checkpoint or tracking sync test.

        // Compute state diff length and commitment
        for Block {
            header:
                SignedBlockHeader {
                    header:
                        BlockHeader {
                            state_diff_length,
                            state_diff_commitment,
                            ..
                        },
                    ..
                },
            state_update,
            ..
        } in init.iter_mut()
        {
            *state_diff_length = state_update.as_ref().unwrap().state_diff_length();
            *state_diff_commitment = state_update
                .as_ref()
                .unwrap()
                .compute_state_diff_commitment();
        }

        init
    }

    // Updates class, storage and state commitments
    fn update_commitments(blocks: &mut [Block], update_tries: UpdateTriesFn) {
        // This dummy db is only necessary to build the tries whose roots are the
        // storage and class commitments
        let dummy_storage = StorageBuilder::in_tempdir().unwrap();
        let mut db = dummy_storage.connection().unwrap();
        let db = db.transaction().unwrap();

        for Block {
            header,
            state_update,
            ..
        } in blocks.iter_mut()
        {
            let state_update = state_update.as_mut().unwrap();

            // Required because of foreign key constraint
            db.insert_block_header(&header.header).unwrap();
            // Required for the tries
            db.insert_state_update(header.header.number, state_update)
                .unwrap();
            let (storage_commitment, class_commitment, _trie) = update_tries(
                &db,
                state_update.into(),
                false,
                header.header.number,
                dummy_storage.clone(),
            )
            .unwrap();
            let state_commitment = StateCommitment::calculate(
                storage_commitment,
                class_commitment,
                header.header.starknet_version,
            );
            header.header.state_commitment = state_commitment;
            state_update.state_commitment = state_commitment;
        }

        for i in 1..blocks.len() {
            let parent_state_commitment = blocks.get(i - 1).unwrap().header.header.state_commitment;
            let Block { state_update, .. } = blocks.get_mut(i).unwrap();
            let state_update = state_update.as_mut().unwrap();
            state_update.parent_state_commitment = parent_state_commitment;
        }
    }

    /// Computes block hashes, updates parent block hashes with the correct
    /// values, computes block hash signatures, updates those too
    fn compute_block_hashes(
        blocks: &mut [Block],
        calculate_block_hash: BlockHashFn,
        sign_block_hash: SignBlockHashFn,
    ) {
        if blocks.is_empty() {
            return;
        }

        let Block {
            header,
            state_update,
            ..
        } = blocks.get_mut(0).unwrap();
        header.header.parent_hash = BlockHash::ZERO;
        header.header.hash = calculate_block_hash(&header.header);
        let (r, s) = sign_block_hash(header.header.hash).unwrap();
        header.signature = BlockCommitmentSignature {
            r: BlockCommitmentSignatureElem(r),
            s: BlockCommitmentSignatureElem(s),
        };
        state_update.as_mut().unwrap().block_hash = header.header.hash;

        for i in 1..blocks.len() {
            let parent_hash = blocks
                .get(i - 1)
                .map(|Block { header, .. }| header.header.hash)
                .unwrap();
            let Block {
                header,
                state_update,
                ..
            } = blocks.get_mut(i).unwrap();

            header.header.parent_hash = parent_hash;
            header.header.hash = calculate_block_hash(&header.header);
            let (r, s) = sign_block_hash(header.header.hash).unwrap();
            header.signature = BlockCommitmentSignature {
                r: BlockCommitmentSignatureElem(r),
                s: BlockCommitmentSignatureElem(s),
            };
            state_update.as_mut().unwrap().block_hash = header.header.hash;
        }
    }
}
