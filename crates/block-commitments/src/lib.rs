use std::io::Write;
use std::sync::LazyLock;

use anyhow::{Context, Result};
use pathfinder_common::event::Event;
use pathfinder_common::hash::{FeltHash, PedersenHash, PoseidonHash};
use pathfinder_common::prelude::*;
use pathfinder_common::receipt::{ExecutionStatus, Receipt};
use pathfinder_common::transaction::{Transaction, TransactionVariant};
use pathfinder_common::{felt_bytes, Chain};
use pathfinder_crypto::hash::{pedersen_hash, poseidon_hash_many, HashChain, PoseidonHasher};
use pathfinder_crypto::{Felt, MontFelt};
use pathfinder_merkle_tree::TransactionOrEventTree;
use sha3::Digest;
use starknet_gateway_types::reply::Block;

const V_0_11_1: StarknetVersion = StarknetVersion::new(0, 11, 1, 0);

#[derive(Debug, PartialEq, Eq)]
pub enum VerifyResult {
    Match,
    Mismatch,
}

impl VerifyResult {
    pub fn is_match(&self) -> bool {
        matches!(self, Self::Match)
    }
}

pub fn verify_gateway_block_commitments_and_hash(
    block: &Block,
    state_diff_commitment: StateDiffCommitment,
    state_diff_length: u64,
    chain: Chain,
    chain_id: ChainId,
) -> Result<VerifyResult> {
    let mut header = header_from_gateway_block(block, state_diff_commitment, state_diff_length)?;

    let computed_transaction_commitment =
        calculate_transaction_commitment(&block.transactions, block.starknet_version)?;

    // Older blocks on mainnet don't carry a precalculated transaction commitment.
    if block.transaction_commitment == TransactionCommitment::ZERO {
        // Update with the computed transaction commitment, verification is not
        // possible.
        header.transaction_commitment = computed_transaction_commitment;
    } else if computed_transaction_commitment != header.transaction_commitment {
        tracing::debug!(%computed_transaction_commitment, actual_transaction_commitment=%header.transaction_commitment, "Transaction commitment mismatch");
        return Ok(VerifyResult::Mismatch);
    }

    let computed_receipt_commitment = calculate_receipt_commitment(
        block
            .transaction_receipts
            .iter()
            .map(|(r, _)| r.clone())
            .collect::<Vec<_>>()
            .as_slice(),
    )?;

    // Older blocks on mainnet don't carry a precalculated receipt commitment.
    if let Some(receipt_commitment) = block.receipt_commitment {
        if computed_receipt_commitment != receipt_commitment {
            tracing::debug!(%computed_receipt_commitment, actual_receipt_commitment=%receipt_commitment, "Receipt commitment mismatch");
            return Ok(VerifyResult::Mismatch);
        }
    } else {
        // Update with the computed transaction commitment, verification is not
        // possible.
        header.receipt_commitment = computed_receipt_commitment;
    }

    let event_commitment = calculate_event_commitment(
        &block
            .transaction_receipts
            .iter()
            .map(|(receipt, events)| (receipt.transaction_hash, events.as_slice()))
            .collect::<Vec<_>>(),
        block.starknet_version,
    )?;

    // Older blocks on mainnet don't carry a precalculated event
    // commitment.
    if block.event_commitment == EventCommitment::ZERO {
        // Update with the computed transaction commitment, verification is not
        // possible.
        header.event_commitment = event_commitment;
    } else if event_commitment != block.event_commitment {
        tracing::debug!(computed_event_commitment=%event_commitment, actual_event_commitment=%block.event_commitment, "Event commitment mismatch");
        return Ok(VerifyResult::Mismatch);
    }

    verify_block_hash(header, chain, chain_id)
}

pub fn header_from_gateway_block(
    block: &Block,
    state_diff_commitment: StateDiffCommitment,
    state_diff_length: u64,
) -> Result<BlockHeader> {
    Ok(BlockHeader {
        hash: block.block_hash,
        parent_hash: block.parent_block_hash,
        number: block.block_number,
        timestamp: block.timestamp,
        eth_l1_gas_price: block.l1_gas_price.price_in_wei,
        strk_l1_gas_price: block.l1_gas_price.price_in_fri,
        eth_l1_data_gas_price: block.l1_data_gas_price.price_in_wei,
        strk_l1_data_gas_price: block.l1_data_gas_price.price_in_fri,
        eth_l2_gas_price: block.l2_gas_price.unwrap_or_default().price_in_wei,
        strk_l2_gas_price: block.l2_gas_price.unwrap_or_default().price_in_fri,
        sequencer_address: block
            .sequencer_address
            .unwrap_or(SequencerAddress(Felt::ZERO)),
        starknet_version: block.starknet_version,
        event_commitment: block.event_commitment,
        state_commitment: block.state_commitment,
        transaction_commitment: block.transaction_commitment,
        transaction_count: block.transactions.len(),
        event_count: block
            .transaction_receipts
            .iter()
            .flat_map(|(_, events)| events)
            .count(),
        l1_da_mode: block.l1_da_mode.into(),
        receipt_commitment: block.receipt_commitment.unwrap_or_default(),
        state_diff_commitment,
        state_diff_length,
    })
}

/// Verify the block hash value.
///
/// The method to compute the block hash is documented
/// [here](https://docs.starknet.io/learn/protocol/blocks#block-hash).
///
/// Unfortunately that'a not-fully-correct description, since the transaction
/// commitment Merkle tree is not constructed directly with the transaction
/// hashes, but with a hash computed from the transaction hash and the signature
/// values (for invoke transactions).
pub fn verify_block_hash(
    header: BlockHeader,
    chain: Chain,
    chain_id: ChainId,
) -> Result<VerifyResult> {
    let meta_info = meta::for_chain(chain);

    let verified = if meta_info.uses_pre_0_7_hash_algorithm(header.number) {
        anyhow::ensure!(
            chain != Chain::Custom,
            "Chain::Custom should not have any pre 0.7 block hashes"
        );

        let computed_hash = compute_final_hash_pre_0_7(&header, chain_id);
        computed_hash == header.hash
    } else if header.starknet_version < StarknetVersion::V_0_13_2 {
        let computed_hash = compute_final_hash_pre_0_13_2(&header);
        if computed_hash == header.hash {
            true
        } else if let Some(fallback_sequencer_address) = meta_info.fallback_sequencer_address {
            // Try with the fallback sequencer address.
            let computed_hash = compute_final_hash_pre_0_13_2(&BlockHeader {
                sequencer_address: fallback_sequencer_address,
                ..header
            });
            computed_hash == header.hash
        } else {
            false
        }
    } else {
        let computed_hash = compute_final_hash(&header);
        computed_hash == header.hash
    };

    Ok(match verified {
        false => VerifyResult::Mismatch,
        true => VerifyResult::Match,
    })
}

mod meta {
    use pathfinder_common::{sequencer_address, BlockNumber, Chain, SequencerAddress};

    /// Metadata about Starknet chains we use for block hash calculation
    ///
    /// Since the method for calculating block hashes has changed during the
    /// operation of the Starknet alpha network, we need this information
    /// to be able to decide which method to use for block hash calculation.
    ///
    /// * Before the Starknet 0.7 release block hashes were calculated with a
    ///   slightly different algorithm (the Starknet chain ID was hashed into
    ///   the final value). Zero was used both instead of the block timestamp
    ///   and the sequencer value.
    /// * After Starknet 0.7 and before Starknet 0.8 the block hash does not
    ///   include the chain id anymore. The proper block timestamp is used but
    ///   zero is used as the sequencer address.
    /// * After Starknet 0.8 and before Starknet 0.8.2 the sequencer address is
    ///   non-zero and is used for the block hash calculation. However, the
    ///   blocks don't include the sequencer address that was used for the
    ///   calculation and for the majority of the blocks the block hash value is
    ///   irrecoverable.
    /// * After Starknet 0.8.2 all blocks include the correct sequencer address
    ///   value.
    #[derive(Clone)]
    pub struct BlockHashMetaInfo {
        /// The number of the first block that was hashed with the Starknet 0.7
        /// hash algorithm.
        pub first_0_7_block: BlockNumber,
        /// Fallback sequencer address to use for blocks that don't include the
        /// address.
        pub fallback_sequencer_address: Option<SequencerAddress>,
    }

    impl BlockHashMetaInfo {
        pub fn uses_pre_0_7_hash_algorithm(&self, block_number: BlockNumber) -> bool {
            block_number < self.first_0_7_block
        }
    }

    const MAINNET_METAINFO: BlockHashMetaInfo = BlockHashMetaInfo {
        first_0_7_block: BlockNumber::new_or_panic(833),
        fallback_sequencer_address: Some(sequencer_address!(
            "0x021f4b90b0377c82bf330b7b5295820769e72d79d8acd0effa0ebde6e9988bc5"
        )),
    };

    const SEPOLIA_TESTNET_METAINFO: BlockHashMetaInfo = BlockHashMetaInfo {
        first_0_7_block: BlockNumber::new_or_panic(0),
        fallback_sequencer_address: None,
    };

    const SEPOLIA_INTEGRATION_METAINFO: BlockHashMetaInfo = BlockHashMetaInfo {
        first_0_7_block: BlockNumber::new_or_panic(0),
        fallback_sequencer_address: None,
    };

    const CUSTOM_METAINFO: BlockHashMetaInfo = BlockHashMetaInfo {
        first_0_7_block: BlockNumber::new_or_panic(0),
        fallback_sequencer_address: None,
    };

    pub fn for_chain(chain: Chain) -> &'static BlockHashMetaInfo {
        match chain {
            Chain::Mainnet => &MAINNET_METAINFO,
            Chain::SepoliaTestnet => &SEPOLIA_TESTNET_METAINFO,
            Chain::SepoliaIntegration => &SEPOLIA_INTEGRATION_METAINFO,
            Chain::Custom => &CUSTOM_METAINFO,
        }
    }
}

/// Computes the final block hash for pre-0.7 blocks.
///
/// This deviates from later algorithms by hashing a chain-specific
/// ID into the final hash.
///
/// Note that for these blocks we're using zero for:
///   * timestamps
///   * sequencer addresses
///   * event number and event commitment
fn compute_final_hash_pre_0_7(header: &BlockHeader, chain_id: ChainId) -> BlockHash {
    let mut chain = HashChain::default();

    // block number
    chain.update(Felt::from(header.number.get()));
    // global state root
    chain.update(header.state_commitment.0);
    // sequencer address: these versions used 0 as the sequencer address
    chain.update(Felt::ZERO);
    // block timestamp: these versions used 0 as a timestamp for block hash
    // computation
    chain.update(Felt::ZERO);
    // number of transactions
    chain.update(Felt::from(header.transaction_count));
    // transaction commitment
    chain.update(header.transaction_commitment.0);
    // number of events
    chain.update(Felt::ZERO);
    // event commitment
    chain.update(Felt::ZERO);
    // reserved: protocol version
    chain.update(Felt::ZERO);
    // reserved: extra data
    chain.update(Felt::ZERO);
    // EXTRA FIELD: chain id
    chain.update(chain_id.0);
    // parent block hash
    chain.update(header.parent_hash.0);

    BlockHash(chain.finalize())
}

/// This implements the final hashing step for post-0.7, pre-0.13.2 blocks.
fn compute_final_hash_pre_0_13_2(header: &BlockHeader) -> BlockHash {
    let mut chain = HashChain::default();

    // block number
    chain.update(Felt::from(header.number.get()));
    // global state root
    chain.update(header.state_commitment.0);
    // sequencer address
    chain.update(header.sequencer_address.0);
    // block timestamp
    chain.update(Felt::from(header.timestamp.get()));
    // number of transactions
    chain.update(Felt::from(header.transaction_count));
    // transaction commitment
    chain.update(header.transaction_commitment.0);
    // number of events
    chain.update(Felt::from(header.event_count));
    // event commitment
    chain.update(header.event_commitment.0);
    // reserved: protocol version
    chain.update(Felt::ZERO);
    // reserved: extra data
    chain.update(Felt::ZERO);
    // parent block hash
    chain.update(header.parent_hash.0);

    BlockHash(chain.finalize())
}

fn compute_final_hash_v0(header: &BlockHeader, starknet_version_str: &str) -> BlockHash {
    // Hash the block header.
    let mut hasher = PoseidonHasher::new();
    hasher.write(felt_bytes!(b"STARKNET_BLOCK_HASH0").into());
    hasher.write(header.number.get().into());
    hasher.write(header.state_commitment.0.into());
    hasher.write(header.sequencer_address.0.into());
    hasher.write(header.timestamp.get().into());
    hasher.write(concatenate_counts(header));
    hasher.write(header.state_diff_commitment.0.into());
    hasher.write(header.transaction_commitment.0.into());
    hasher.write(header.event_commitment.0.into());
    hasher.write(header.receipt_commitment.0.into());
    hasher.write(header.eth_l1_gas_price.0.into());
    hasher.write(header.strk_l1_gas_price.0.into());
    hasher.write(header.eth_l1_data_gas_price.0.into());
    hasher.write(header.strk_l1_data_gas_price.0.into());
    hasher.write(
        Felt::from_be_slice(starknet_version_str.as_bytes())
            .expect("Starknet version should fit into a felt")
            .into(),
    );
    hasher.write(MontFelt::ZERO);
    hasher.write(header.parent_hash.0.into());
    BlockHash(hasher.finish().into())
}

// Bumps the initial STARKNET_BLOCK_HASH0 to STARKNET_BLOCK_HASH1,
// replaces gas price elements with gas_prices_hash.
fn compute_final_hash_v1(header: &BlockHeader, starknet_version_str: &str) -> BlockHash {
    // Hash the block header.
    let mut hasher = PoseidonHasher::new();
    hasher.write(felt_bytes!(b"STARKNET_BLOCK_HASH1").into());
    hasher.write(header.number.get().into());
    hasher.write(header.state_commitment.0.into());
    hasher.write(header.sequencer_address.0.into());
    hasher.write(header.timestamp.get().into());
    hasher.write(concatenate_counts(header));
    hasher.write(header.state_diff_commitment.0.into());
    hasher.write(header.transaction_commitment.0.into());
    hasher.write(header.event_commitment.0.into());
    hasher.write(header.receipt_commitment.0.into());
    hasher.write(gas_prices_to_hash(header));
    hasher.write(
        Felt::from_be_slice(starknet_version_str.as_bytes())
            .expect("Starknet version should fit into a felt")
            .into(),
    );
    hasher.write(MontFelt::ZERO);
    hasher.write(header.parent_hash.0.into());
    BlockHash(hasher.finish().into())
}

// TODO consider passing a representation of the block header that does not
// contain the hash itself.
pub fn compute_final_hash(header: &BlockHeader) -> BlockHash {
    compute_final_hash0(header, &header.starknet_version.to_string())
}

fn compute_final_hash0(header: &BlockHeader, starknet_version_str: &str) -> BlockHash {
    if header.starknet_version < StarknetVersion::V_0_13_4 {
        compute_final_hash_v0(header, starknet_version_str)
    } else {
        compute_final_hash_v1(header, starknet_version_str)
    }
}

/// Calculate transaction commitment hash value.
///
/// The transaction commitment is the root of the Patricia Merkle tree with
/// height 64 constructed by adding the (transaction_index,
/// transaction_hash_with_signature) key-value pairs to the tree and computing
/// the root hash.
pub fn calculate_transaction_commitment(
    transactions: &[Transaction],
    version: StarknetVersion,
) -> Result<TransactionCommitment> {
    use rayon::prelude::*;

    let final_hashes = transactions
        .par_iter()
        .map(|tx| {
            if version < V_0_11_1 {
                calculate_transaction_hash_with_signature_pre_0_11_1(tx)
            } else if version < StarknetVersion::V_0_13_2 {
                calculate_transaction_hash_with_signature_pre_0_13_2(tx)
            } else if version < StarknetVersion::V_0_13_4 {
                calculate_transaction_hash_with_signature_pre_0_13_4(tx)
            } else {
                calculate_transaction_hash_with_signature(tx)
            }
        })
        .collect();

    if version < StarknetVersion::V_0_13_2 {
        calculate_commitment_root::<PedersenHash>(final_hashes).map(TransactionCommitment)
    } else {
        calculate_commitment_root::<PoseidonHash>(final_hashes).map(TransactionCommitment)
    }
}

pub fn calculate_receipt_commitment(receipts: &[Receipt]) -> Result<ReceiptCommitment> {
    use rayon::prelude::*;

    let hashes = receipts
        .par_iter()
        .map(|receipt| {
            poseidon_hash_many(&[
                receipt.transaction_hash.0.into(),
                receipt.actual_fee.0.into(),
                // Calculate hash of messages sent.
                {
                    let mut hasher = PoseidonHasher::new();
                    hasher.write((receipt.l2_to_l1_messages.len() as u64).into());
                    for msg in &receipt.l2_to_l1_messages {
                        hasher.write(msg.from_address.0.into());
                        hasher.write(msg.to_address.0.into());
                        hasher.write((msg.payload.len() as u64).into());
                        for payload in &msg.payload {
                            hasher.write(payload.0.into());
                        }
                    }
                    hasher.finish()
                },
                // Revert reason.
                match &receipt.execution_status {
                    ExecutionStatus::Succeeded => MontFelt::ZERO,
                    ExecutionStatus::Reverted { reason } => {
                        let mut keccak = sha3::Keccak256::default();
                        keccak.update(reason.as_bytes());
                        let mut hashed_bytes: [u8; 32] = keccak.finalize().into();
                        hashed_bytes[0] &= 0b00000011_u8; // Discard the six MSBs.
                        MontFelt::from_be_bytes(hashed_bytes)
                    }
                },
                // Execution resources:
                // L2 gas
                MontFelt::ZERO,
                // L1 gas consumed
                receipt.execution_resources.total_gas_consumed.l1_gas.into(),
                // L1 data gas consumed
                receipt
                    .execution_resources
                    .total_gas_consumed
                    .l1_data_gas
                    .into(),
            ])
            .into()
        })
        .collect();

    calculate_commitment_root::<PoseidonHash>(hashes).map(ReceiptCommitment)
}

// Concatenate the transaction count, event count, state diff length,
// and L1 data availability mode into a single felt.
fn concatenate_counts(header: &BlockHeader) -> MontFelt {
    let mut concat_counts = [0u8; 32];
    let mut writer = concat_counts.as_mut_slice();
    writer
        .write_all(&header.transaction_count.to_be_bytes())
        .unwrap();
    writer.write_all(&header.event_count.to_be_bytes()).unwrap();
    writer
        .write_all(&header.state_diff_length.to_be_bytes())
        .unwrap();
    writer
        .write_all(&[match header.l1_da_mode {
            L1DataAvailabilityMode::Calldata => 0,
            L1DataAvailabilityMode::Blob => 0b10000000,
        }])
        .unwrap();
    MontFelt::from_be_bytes(concat_counts)
}

fn gas_prices_to_hash(header: &BlockHeader) -> MontFelt {
    let mut hasher = PoseidonHasher::new();
    hasher.write(felt_bytes!(b"STARKNET_GAS_PRICES0").into());
    hasher.write(header.eth_l1_gas_price.0.into());
    hasher.write(header.strk_l1_gas_price.0.into());
    hasher.write(header.eth_l1_data_gas_price.0.into());
    hasher.write(header.strk_l1_data_gas_price.0.into());
    hasher.write(header.eth_l2_gas_price.0.into());
    hasher.write(header.strk_l2_gas_price.0.into());
    hasher.finish()
}

fn calculate_commitment_root<H: FeltHash>(hashes: Vec<Felt>) -> Result<Felt> {
    let mut tree: TransactionOrEventTree<H> = Default::default();

    hashes
        .into_iter()
        .enumerate()
        .try_for_each(|(idx, final_hash)| {
            let idx: u64 = idx
                .try_into()
                .expect("too many transactions while calculating commitment");
            tree.set(idx, final_hash)
        })
        .context("Building transaction commitment tree")?;

    tree.commit()
}

/// Compute the combined hash of the transaction hash and the signature.
///
/// Since the transaction hash doesn't take the signature values as its input
/// computing the transaction commitent uses a hash value that combines
/// the transaction hash with the array of signature values.
///
/// Note that for non-invoke transactions we don't actually have signatures. The
/// cairo-lang uses an empty list (whose hash is not the ZERO value!) in that
/// case.
fn calculate_transaction_hash_with_signature_pre_0_11_1(tx: &Transaction) -> Felt {
    static HASH_OF_EMPTY_LIST: LazyLock<Felt> = LazyLock::new(|| HashChain::default().finalize());

    let signature_hash = match &tx.variant {
        TransactionVariant::InvokeV0(tx) => calculate_signature_hash(&tx.signature),
        TransactionVariant::InvokeV1(tx) => calculate_signature_hash(&tx.signature),
        TransactionVariant::InvokeV3(tx) => calculate_signature_hash(&tx.signature),
        TransactionVariant::DeclareV0(_)
        | TransactionVariant::DeclareV1(_)
        | TransactionVariant::DeclareV2(_)
        | TransactionVariant::DeclareV3(_)
        | TransactionVariant::DeployV0(_)
        | TransactionVariant::DeployV1(_)
        | TransactionVariant::DeployAccountV1(_)
        | TransactionVariant::DeployAccountV3(_)
        | TransactionVariant::L1Handler(_) => *HASH_OF_EMPTY_LIST,
    };

    pedersen_hash(tx.hash.0, signature_hash)
}

/// Compute the combined hash of the transaction hash and the signature for
/// block before v0.13.2.
///
/// Since the transaction hash doesn't take the signature values as its input
/// computing the transaction commitment uses a hash value that combines
/// the transaction hash with the array of signature values.
///
/// Note that for non-invoke transactions we don't actually have signatures. The
/// cairo-lang uses an empty list (whose hash is not the ZERO value!) in that
/// case.
fn calculate_transaction_hash_with_signature_pre_0_13_2(tx: &Transaction) -> Felt {
    static HASH_OF_EMPTY_LIST: LazyLock<Felt> = LazyLock::new(|| HashChain::default().finalize());

    let signature_hash = match &tx.variant {
        TransactionVariant::InvokeV0(tx) => calculate_signature_hash(&tx.signature),
        TransactionVariant::DeclareV0(tx) => calculate_signature_hash(&tx.signature),
        TransactionVariant::DeclareV1(tx) => calculate_signature_hash(&tx.signature),
        TransactionVariant::DeclareV2(tx) => calculate_signature_hash(&tx.signature),
        TransactionVariant::DeclareV3(tx) => calculate_signature_hash(&tx.signature),
        TransactionVariant::DeployAccountV1(tx) => calculate_signature_hash(&tx.signature),
        TransactionVariant::DeployAccountV3(tx) => calculate_signature_hash(&tx.signature),
        TransactionVariant::InvokeV1(tx) => calculate_signature_hash(&tx.signature),
        TransactionVariant::InvokeV3(tx) => calculate_signature_hash(&tx.signature),
        TransactionVariant::DeployV0(_)
        | TransactionVariant::DeployV1(_)
        | TransactionVariant::L1Handler(_) => *HASH_OF_EMPTY_LIST,
    };

    pedersen_hash(tx.hash.0, signature_hash)
}

/// Compute the combined hash of the transaction hash and the signature.
///
/// [Reference code from StarkWare](https://github.com/starkware-libs/starknet-api/blob/5565e5282f5fead364a41e49c173940fd83dee00/src/block_hash/block_hash_calculator.rs#L95-L98).
fn calculate_transaction_hash_with_signature_pre_0_13_4(tx: &Transaction) -> Felt {
    let signature = match &tx.variant {
        TransactionVariant::InvokeV0(tx) => tx.signature.as_slice(),
        TransactionVariant::DeclareV0(tx) => tx.signature.as_slice(),
        TransactionVariant::DeclareV1(tx) => tx.signature.as_slice(),
        TransactionVariant::DeclareV2(tx) => tx.signature.as_slice(),
        TransactionVariant::DeclareV3(tx) => tx.signature.as_slice(),
        TransactionVariant::DeployAccountV1(tx) => tx.signature.as_slice(),
        TransactionVariant::DeployAccountV3(tx) => tx.signature.as_slice(),
        TransactionVariant::InvokeV1(tx) => tx.signature.as_slice(),
        TransactionVariant::InvokeV3(tx) => tx.signature.as_slice(),
        TransactionVariant::DeployV0(_)
        | TransactionVariant::DeployV1(_)
        | TransactionVariant::L1Handler(_) => &[],
    };

    let signature = if signature.is_empty() {
        &[TransactionSignatureElem::ZERO]
    } else {
        signature
    };

    let mut hasher = PoseidonHasher::new();
    hasher.write(tx.hash.0.into());
    for elem in signature {
        hasher.write(elem.0.into());
    }
    hasher.finish().into()
}

fn calculate_transaction_hash_with_signature(tx: &Transaction) -> Felt {
    let signature = match &tx.variant {
        TransactionVariant::InvokeV0(tx) => tx.signature.as_slice(),
        TransactionVariant::DeclareV0(tx) => tx.signature.as_slice(),
        TransactionVariant::DeclareV1(tx) => tx.signature.as_slice(),
        TransactionVariant::DeclareV2(tx) => tx.signature.as_slice(),
        TransactionVariant::DeclareV3(tx) => tx.signature.as_slice(),
        TransactionVariant::DeployAccountV1(tx) => tx.signature.as_slice(),
        TransactionVariant::DeployAccountV3(tx) => tx.signature.as_slice(),
        TransactionVariant::InvokeV1(tx) => tx.signature.as_slice(),
        TransactionVariant::InvokeV3(tx) => tx.signature.as_slice(),
        TransactionVariant::DeployV0(_)
        | TransactionVariant::DeployV1(_)
        | TransactionVariant::L1Handler(_) => &[],
    };

    let mut hasher = PoseidonHasher::new();
    hasher.write(tx.hash.0.into());
    for elem in signature {
        hasher.write(elem.0.into());
    }
    hasher.finish().into()
}

fn calculate_signature_hash(signature: &[TransactionSignatureElem]) -> Felt {
    let mut hash = HashChain::default();
    for s in signature {
        hash.update(s.0);
    }
    hash.finalize()
}

/// Calculate event commitment hash value.
///
/// The event commitment is the root of the Patricia Merkle tree with height 64
/// constructed by adding the (event_index, event_hash) key-value pairs to the
/// tree and computing the root hash.
pub fn calculate_event_commitment(
    transaction_events: &[(TransactionHash, &[Event])],
    version: StarknetVersion,
) -> Result<EventCommitment> {
    use rayon::prelude::*;

    let event_hashes = transaction_events
        .par_iter()
        .flat_map(|(tx_hash, events)| events.par_iter().map(|e| (*tx_hash, e)))
        .map(|(tx_hash, e)| {
            if version < StarknetVersion::V_0_13_2 {
                calculate_event_hash_pre_0_13_2(e)
            } else {
                calculate_event_hash(e, tx_hash)
            }
        })
        .collect();

    if version < StarknetVersion::V_0_13_2 {
        calculate_commitment_root::<PedersenHash>(event_hashes).map(EventCommitment)
    } else {
        calculate_commitment_root::<PoseidonHash>(event_hashes).map(EventCommitment)
    }
}

/// Calculate the hash of a pre-v0.13.2 Starknet event.
///
/// See the [documentation](https://docs.starknet.io/learn/protocol/blocks#event-hash)
/// for details.
fn calculate_event_hash_pre_0_13_2(event: &Event) -> Felt {
    let mut keys_hash = HashChain::default();
    for key in event.keys.iter() {
        keys_hash.update(key.0);
    }
    let keys_hash = keys_hash.finalize();

    let mut data_hash = HashChain::default();
    for data in event.data.iter() {
        data_hash.update(data.0);
    }
    let data_hash = data_hash.finalize();

    let mut event_hash = HashChain::default();
    event_hash.update(*event.from_address.get());
    event_hash.update(keys_hash);
    event_hash.update(data_hash);

    event_hash.finalize()
}

/// Calculate the hash of an event.
/// [Reference code from StarkWare](https://github.com/starkware-libs/starknet-api/blob/5565e5282f5fead364a41e49c173940fd83dee00/src/block_hash/event_commitment.rs#L33).
fn calculate_event_hash(event: &Event, transaction_hash: TransactionHash) -> Felt {
    let mut hasher = PoseidonHasher::new();
    hasher.write(event.from_address.0.into());
    hasher.write(transaction_hash.0.into());
    hasher.write((event.keys.len() as u64).into());
    for key in &event.keys {
        hasher.write(key.0.into());
    }
    hasher.write((event.data.len() as u64).into());
    for data in &event.data {
        hasher.write(data.0.into());
    }
    hasher.finish().into()
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use pathfinder_common::macro_prelude::*;
    use pathfinder_common::prelude::*;
    use pathfinder_common::receipt::{ExecutionResources, L1Gas, L2ToL1Message};
    use pathfinder_common::transaction::{
        EntryPointType,
        InvokeTransactionV0,
        InvokeTransactionV3,
    };
    use pathfinder_crypto::Felt;
    use starknet_gateway_test_fixtures::{v0_13_2, v0_13_4};
    use starknet_gateway_types::reply::StateUpdate;

    use super::*;

    #[test]
    fn test_event_hash() {
        let event = Event {
            from_address: contract_address!("0xdeadbeef"),
            data: vec![
                event_data!("0x5"),
                event_data!("0x6"),
                event_data!("0x7"),
                event_data!("0x8"),
                event_data!("0x9"),
            ],
            keys: vec![
                event_key!("0x1"),
                event_key!("0x2"),
                event_key!("0x3"),
                event_key!("0x4"),
            ],
        };

        // produced by the cairo-lang Python implementation:
        // `hex(calculate_event_hash(0xdeadbeef, [1, 2, 3, 4], [5, 6, 7, 8, 9]))`
        let expected_event_hash =
            felt!("0xdb96455b3a61f9139f7921667188d31d1e1d49fb60a1aa3dbf3756dbe3a9b4");
        let calculated_event_hash = calculate_event_hash_pre_0_13_2(&event);
        assert_eq!(expected_event_hash, calculated_event_hash);
    }

    #[test]
    fn test_final_transaction_hash() {
        let transaction = Transaction {
            hash: transaction_hash!("0x1"),
            variant: TransactionVariant::InvokeV0(InvokeTransactionV0 {
                sender_address: contract_address!("0xdeadbeef"),
                entry_point_type: Some(EntryPointType::External),
                entry_point_selector: entry_point!("0xe"),
                signature: vec![
                    transaction_signature_elem!("0x2"),
                    transaction_signature_elem!("0x3"),
                ],
                ..Default::default()
            }),
        };

        // produced by the cairo-lang Python implementation:
        // `hex(calculate_single_tx_hash_with_signature(1, [2, 3],
        // hash_function=pedersen_hash))`
        let expected_final_hash =
            Felt::from_hex_str("0x259c3bd5a1951eafb2f41e0b783eab92cfe4e108b2b1f071e3736f06b909431")
                .unwrap();
        let calculated_final_hash =
            calculate_transaction_hash_with_signature_pre_0_13_2(&transaction);
        assert_eq!(expected_final_hash, calculated_final_hash);
    }

    /// Source:
    /// https://github.com/starkware-libs/sequencer/blob/main/crates/starknet_api/src/block_hash/block_hash_calculator_test.rs#L74-121
    #[rstest::rstest]
    fn test_final_transaction_hash_variants(
        #[values(StarknetVersion::V_0_13_2, StarknetVersion::V_0_13_4)]
        starknet_version: StarknetVersion,
    ) {
        let block_header = BlockHeader {
            hash: Default::default(),
            number: BlockNumber::new_or_panic(1),
            state_commitment: StateCommitment(2u64.into()),
            sequencer_address: SequencerAddress(3u64.into()),
            timestamp: BlockTimestamp::new_or_panic(4),
            l1_da_mode: L1DataAvailabilityMode::Blob,
            strk_l1_gas_price: GasPrice(6),
            eth_l1_gas_price: GasPrice(7),
            strk_l1_data_gas_price: GasPrice(10),
            eth_l1_data_gas_price: GasPrice(9),
            strk_l2_gas_price: GasPrice(11),
            eth_l2_gas_price: GasPrice(12),
            starknet_version,
            parent_hash: BlockHash(11u64.into()),
            transaction_commitment: TransactionCommitment(felt!(
                "0x72f432efa51e2a34f68404ac5e77514301e26eb53ec89badd8173f4e8561b95"
            )),
            transaction_count: 1,
            event_commitment: EventCommitment(Felt::ZERO),
            event_count: 0,
            state_diff_commitment: StateDiffCommitment(felt!(
                "0x281f5966e49ad7dad9323826d53d1d27c0c4e6ebe5525e2e2fbca549bfa0a67"
            )),
            state_diff_length: 10,
            receipt_commitment: ReceiptCommitment(felt!(
                "0x8e7dfb2772c2ac26e712fb97404355d66db0ba9555f0f64f30d61a56df9c76"
            )),
        };

        let expected_hash = BlockHash(match starknet_version {
            StarknetVersion::V_0_13_2 => {
                felt!("0xe248d6ce583f8fa48d1d401d4beb9ceced3733e38d8eacb0d8d3669a7d901c")
            }
            _ => {
                felt!("0x3d6174623c812f5dc03fa3faa07c42c06fd90ad425629ee5f39e149df65c3ca")
            }
        });

        assert_eq!(compute_final_hash(&block_header), expected_hash);
    }

    #[test]
    fn test_block_hash_without_sequencer_address() {
        // This tests with a post-0.7, pre-0.8.0 block where zero is used as the
        // sequencer address.
        let json = starknet_gateway_test_fixtures::v0_7_0::block::MAINNET_2240;
        let block: Block = serde_json::from_str(json).unwrap();

        assert_matches!(
            verify_gateway_block_commitments_and_hash(
                &block,
                Default::default(),
                0,
                Chain::Mainnet,
                ChainId::MAINNET
            )
            .unwrap(),
            VerifyResult::Match
        );
    }

    #[test]
    fn test_block_hash_with_sequencer_address() {
        // This tests with a post-0.8.2 block where we have correct sequencer address
        // information in the block itself.
        let json = starknet_gateway_test_fixtures::v0_9_0::block::MAINNET_2800;
        let block: Block = serde_json::from_str(json).unwrap();

        assert_matches!(
            verify_gateway_block_commitments_and_hash(
                &block,
                Default::default(),
                0,
                Chain::Mainnet,
                ChainId::MAINNET
            )
            .unwrap(),
            VerifyResult::Match
        );
    }

    #[test]
    fn test_block_hash_with_sequencer_address_unavailable_but_not_zero() {
        // This tests with a post-0.8.0 pre-0.8.2 block where we don't have the
        // sequencer address in the JSON but the block hash was calculated with
        // the magic value below instead of zero.
        let json = starknet_gateway_test_fixtures::v0_8_0::block::MAINNET_2500;
        let block: Block = serde_json::from_str(json).unwrap();

        assert_matches!(
            verify_gateway_block_commitments_and_hash(
                &block,
                Default::default(),
                0,
                Chain::Mainnet,
                ChainId::MAINNET
            )
            .unwrap(),
            VerifyResult::Match
        );
    }

    #[test]
    fn test_block_hash_0_11_1() {
        let json = starknet_gateway_test_fixtures::v0_11_1::block::MAINNET_65000;
        let block: Block = serde_json::from_str(json).unwrap();

        assert_matches!(
            verify_gateway_block_commitments_and_hash(
                &block,
                Default::default(),
                0,
                Chain::Mainnet,
                ChainId::MAINNET
            )
            .unwrap(),
            VerifyResult::Match
        );
    }

    #[test]
    fn test_block_hash_0() {
        // This tests with a pre-0.7 block where the chain ID was hashed into
        // the block hash.
        let json = starknet_gateway_test_fixtures::pre_0_7_0::block::MAINNET_GENESIS;
        let block: Block = serde_json::from_str(json).unwrap();

        assert_matches!(
            verify_gateway_block_commitments_and_hash(
                &block,
                Default::default(),
                0,
                Chain::Mainnet,
                ChainId::MAINNET
            )
            .unwrap(),
            VerifyResult::Match
        );
    }

    /// Source:
    /// https://github.com/starkware-libs/starknet-api/blob/5565e5282f5fead364a41e49c173940fd83dee00/src/block_hash/transaction_commitment_test.rs#L12-L29.
    #[test]
    fn test_transaction_hash_with_signature_0_13_2() {
        let transaction = Transaction {
            hash: TransactionHash(Felt::ONE),
            variant: TransactionVariant::InvokeV3(InvokeTransactionV3 {
                signature: vec![
                    TransactionSignatureElem(Felt::from_u64(2)),
                    TransactionSignatureElem(Felt::from_u64(3)),
                ],
                ..Default::default()
            }),
        };
        let expected = felt!("0x2f0d8840bcf3bc629598d8a6cc80cb7c0d9e52d93dab244bbf9cd0dca0ad082");
        assert_eq!(
            calculate_transaction_hash_with_signature_pre_0_13_4(&transaction),
            expected
        );

        let transaction = Transaction {
            hash: TransactionHash(Felt::ONE),
            variant: TransactionVariant::L1Handler(Default::default()),
        };
        let expected = felt!("0x00a93bf5e58b9378d093aa86ddc2f61a3295a1d1e665bd0ef3384dd07b30e033");
        assert_eq!(
            calculate_transaction_hash_with_signature_pre_0_13_4(&transaction),
            expected
        );
    }

    #[test]
    fn test_transaction_hash_with_signature_0_13_4() {
        let transaction = Transaction {
            hash: TransactionHash(Felt::ONE),
            variant: TransactionVariant::InvokeV3(InvokeTransactionV3 {
                signature: vec![
                    TransactionSignatureElem(Felt::from_u64(2)),
                    TransactionSignatureElem(Felt::from_u64(3)),
                ],
                ..Default::default()
            }),
        };
        let expected = felt!("0x2f0d8840bcf3bc629598d8a6cc80cb7c0d9e52d93dab244bbf9cd0dca0ad082");
        assert_eq!(
            calculate_transaction_hash_with_signature(&transaction),
            expected
        );

        let transaction = Transaction {
            hash: TransactionHash(Felt::ONE),
            variant: TransactionVariant::L1Handler(Default::default()),
        };
        let expected = felt!("0x00579E8877C7755365D5EC1EC7D3A94A457EFF5D1F40482BBE9729C064CDEAD2");
        assert_eq!(
            calculate_transaction_hash_with_signature(&transaction),
            expected
        );
    }

    /// Source:
    /// https://github.com/starkware-libs/starknet-api/blob/5565e5282f5fead364a41e49c173940fd83dee00/src/block_hash/transaction_commitment_test.rs#L32.
    #[test]
    fn test_transaction_commitment_0_13_2() {
        let transaction = Transaction {
            hash: TransactionHash(Felt::ONE),
            variant: TransactionVariant::InvokeV3(InvokeTransactionV3 {
                signature: vec![
                    TransactionSignatureElem(Felt::from_u64(2)),
                    TransactionSignatureElem(Felt::from_u64(3)),
                ],
                ..Default::default()
            }),
        };
        let expected = TransactionCommitment(felt!(
            "0x0282b635972328bd1cfa86496fe920d20bd9440cd78ee8dc90ae2b383d664dcf"
        ));
        assert_eq!(
            calculate_transaction_commitment(
                &[transaction.clone(), transaction],
                StarknetVersion::V_0_13_2
            )
            .unwrap(),
            expected
        );
    }

    /// Source:
    /// https://github.com/starkware-libs/starknet-api/blob/5565e5282f5fead364a41e49c173940fd83dee00/src/block_hash/event_commitment_test.rs#L10.
    #[test]
    fn test_event_commitment_0_13_2() {
        let events = &[get_event(0), get_event(1), get_event(2)];
        let expected = felt!("0x069bb140ddbbeb01d81c7201ecfb933031306e45dab9c77ff9f9ba3cd4c2b9c3");
        assert_eq!(
            calculate_event_commitment(
                &[(transaction_hash!("0x1234"), events)],
                StarknetVersion::V_0_13_2
            )
            .unwrap()
            .0,
            expected
        );

        fn get_event(seed: u64) -> Event {
            Event {
                from_address: ContractAddress(Felt::from_u64(seed + 8)),
                keys: [seed, seed + 1]
                    .iter()
                    .map(|key| EventKey(Felt::from(*key)))
                    .collect(),
                data: [seed + 2, seed + 3, seed + 4]
                    .into_iter()
                    .map(Felt::from)
                    .map(EventData)
                    .collect(),
            }
        }
    }

    // Source:
    // https://github.com/starkware-libs/starknet-api/blob/5565e5282f5fead364a41e49c173940fd83dee00/src/block_hash/receipt_commitment_test.rs#L16.
    #[test]
    fn test_receipt_commitment_0_13_2() {
        let receipt = Receipt {
            transaction_hash: TransactionHash(1234_u64.into()),
            actual_fee: Fee(99804_u64.into()),
            l2_to_l1_messages: vec![
                L2ToL1Message {
                    from_address: ContractAddress(34_u64.into()),
                    to_address: ContractAddress(35_u64.into()),
                    payload: vec![
                        L2ToL1MessagePayloadElem(36_u64.into()),
                        L2ToL1MessagePayloadElem(37_u64.into()),
                    ],
                },
                L2ToL1Message {
                    from_address: ContractAddress(56_u64.into()),
                    to_address: ContractAddress(57_u64.into()),
                    payload: vec![
                        L2ToL1MessagePayloadElem(58_u64.into()),
                        L2ToL1MessagePayloadElem(59_u64.into()),
                    ],
                },
            ],
            execution_resources: ExecutionResources {
                data_availability: L1Gas {
                    l1_gas: 0,
                    l1_data_gas: 32,
                },
                total_gas_consumed: L1Gas {
                    l1_gas: 16580,
                    l1_data_gas: 32,
                },
                ..Default::default()
            },
            execution_status: ExecutionStatus::Reverted {
                reason: "aborted".to_string(),
            },
            ..Default::default()
        };
        let expected_root = ReceiptCommitment(felt!(
            "0x31963cb891ebb825e83514deb748c89b6967b5368cbc48a9b56193a1464ca87"
        ));
        assert_eq!(
            calculate_receipt_commitment(&[receipt]).unwrap(),
            expected_root
        );
    }

    // Source:
    // https://github.com/starkware-libs/starknet-api/blob/5565e5282f5fead364a41e49c173940fd83dee00/src/block_hash/block_hash_calculator_test.rs#L51
    #[test]
    fn test_block_hash_0_13_2() {
        let header = BlockHeader {
            hash: Default::default(),
            number: BlockNumber::new_or_panic(1),
            state_commitment: StateCommitment(2u64.into()),
            sequencer_address: SequencerAddress(3u64.into()),
            timestamp: BlockTimestamp::new_or_panic(4),
            l1_da_mode: L1DataAvailabilityMode::Blob,
            strk_l1_gas_price: GasPrice(6),
            eth_l1_gas_price: GasPrice(7),
            strk_l1_data_gas_price: GasPrice(10),
            eth_l1_data_gas_price: GasPrice(9),
            strk_l2_gas_price: GasPrice(0), // not used for StarknetVersion::V_0_13_2
            eth_l2_gas_price: GasPrice(0),  // ditto
            starknet_version: StarknetVersion::V_0_13_2,
            parent_hash: BlockHash(11u64.into()),
            transaction_commitment: TransactionCommitment(felt!(
                "0x72f432efa51e2a34f68404ac5e77514301e26eb53ec89badd8173f4e8561b95"
            )),
            transaction_count: 1,
            event_commitment: EventCommitment(Felt::ZERO),
            event_count: 0,
            state_diff_commitment: StateDiffCommitment(felt!(
                "0x281f5966e49ad7dad9323826d53d1d27c0c4e6ebe5525e2e2fbca549bfa0a67"
            )),
            state_diff_length: 10,
            receipt_commitment: ReceiptCommitment(felt!(
                "0x8e7dfb2772c2ac26e712fb97404355d66db0ba9555f0f64f30d61a56df9c76"
            )),
        };
        let expected_hash = BlockHash(felt!(
            "0x061e4998d51a248f1d0288d7e17f6287757b0e5e6c5e1e58ddf740616e312134"
        ));
        assert_eq!(compute_final_hash0(&header, "10"), expected_hash);
    }

    // Source
    // https://integration-sepolia.starknet.io/feeder_gateway/get_block?blockNumber=35748
    #[test]
    fn test_block_hash_0_13_2_first_integration_block() {
        let block: Block = serde_json::from_str(v0_13_2::block::SEPOLIA_INTEGRATION_35748).unwrap();
        let expected_hash = block.block_hash;

        let state_update: StateUpdate =
            serde_json::from_str(v0_13_2::state_update::SEPOLIA_INTEGRATION_35748).unwrap();
        let state_update: pathfinder_common::StateUpdate = state_update.into();
        let state_diff_length = state_update.state_diff_length();
        let state_diff_commitment = state_update.compute_state_diff_commitment();

        assert_eq!(state_diff_length, block.state_diff_length.unwrap());
        assert_eq!(state_diff_commitment, block.state_diff_commitment.unwrap());

        let receipts: Vec<_> = block
            .transaction_receipts
            .iter()
            .map(|(receipt, _)| receipt.clone())
            .collect();
        assert_eq!(
            calculate_receipt_commitment(&receipts).unwrap(),
            block.receipt_commitment.unwrap()
        );

        let header = header_from_gateway_block(
            &block,
            block.state_diff_commitment.unwrap(),
            block.state_diff_length.unwrap(),
        )
        .unwrap();

        assert_eq!(compute_final_hash(&header), expected_hash);
    }

    // Source
    // https://integration-sepolia.starknet.io/feeder_gateway/get_block?blockNumber=63881
    #[test]
    fn test_block_hash_0_13_4_first_integration_block() {
        let block: Block = serde_json::from_str(v0_13_4::block::SEPOLIA_INTEGRATION_63881).unwrap();
        let expected_hash = block.block_hash;

        let state_update: StateUpdate =
            serde_json::from_str(v0_13_4::state_update::SEPOLIA_INTEGRATION_63881).unwrap();
        let state_update: pathfinder_common::StateUpdate = state_update.into();
        let state_diff_length = state_update.state_diff_length();
        let state_diff_commitment = state_update.compute_state_diff_commitment();

        assert_eq!(state_diff_length, block.state_diff_length.unwrap());
        assert_eq!(state_diff_commitment, block.state_diff_commitment.unwrap());

        let receipts: Vec<_> = block
            .transaction_receipts
            .iter()
            .map(|(receipt, _)| receipt.clone())
            .collect();
        assert_eq!(
            calculate_receipt_commitment(&receipts).unwrap(),
            block.receipt_commitment.unwrap()
        );

        let header = header_from_gateway_block(
            &block,
            block.state_diff_commitment.unwrap(),
            block.state_diff_length.unwrap(),
        )
        .unwrap();

        assert_eq!(compute_final_hash(&header), expected_hash);
    }
}
