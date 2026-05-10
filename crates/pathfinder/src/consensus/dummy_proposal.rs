//! Test helpers for consensus transaction testing
//!
//! This module provides utilities for creating realistic test transactions
//! and testing consensus scenarios with actual transaction execution.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use p2p_proto::common::{Address, Hash, L1DataAvailabilityMode};
use p2p_proto::consensus::{ProposalFin, ProposalInit, ProposalPart};
use pathfinder_common::{
    BlockId,
    BlockNumber,
    ChainId,
    ConsensusFinalizedL2Block,
    ContractAddress,
    DecidedBlocks,
    StarknetVersion,
};
use pathfinder_consensus::Round;
use pathfinder_crypto::Felt;
use pathfinder_executor::{ConcurrentStateReader, ExecutorWorkerPool};
use pathfinder_storage::Storage;
use pathfinder_validator::{ProdTransactionMapper, ValidatorBlockInfoStage};
use rand::seq::SliceRandom;
use rand::{thread_rng, Rng, SeedableRng};

use crate::devnet::{self, strictly_increasing_timestamp, Account};

// TODO consider waiting for the parent block to land in the decided blocks
/// Blocks consensus tasks's processing loop until the parent block of height is
/// committed in main storage without blocking the async runtime.
pub(crate) async fn wait_for_parent_committed(
    height: u64,
    main_storage: Storage,
    poll_interval: Duration,
) -> anyhow::Result<()> {
    let parent_number = height.checked_sub(1);

    tracing::debug!(
        %height,
        ?parent_number,
        "Waiting for parent block to be committed"
    );

    util::task::spawn_blocking(move |cancellation_token| {
        if let Some(parent_number) = parent_number {
            loop {
                if cancellation_token.is_cancelled() {
                    break;
                }

                {
                    let mut main_db_conn = main_storage.connection()?;
                    let main_db_txn = main_db_conn.transaction()?;

                    if main_db_txn
                        .block_exists(BlockId::Number(BlockNumber::new_or_panic(parent_number)))?
                    {
                        break;
                    }

                    // Drop the transaction and return the connection to the
                    // pool before sleeping to avoid holding locks on the DB or
                    // shrinking available DB connections in the pool
                    // for longer than necessary
                }

                tracing::debug!(
                    %height,
                    %parent_number,
                    "Parent block not yet committed, sleeping"
                );

                std::thread::sleep(poll_interval);
            }
        }

        anyhow::Ok(())
    })
    .await??;

    Ok(())
}

// Probability of producing an empty proposal (only Init and Fin, no
// transaction batches).
const EMPTY_PROPOSAL_PROBABILITY: f64 = 0.25;

/// Creates a dummy proposal for the given height and round, filling it with
/// realistic transactions based on the state of the main storage DB, if it is
/// bootstrapped, or with invalid L1 handler transactions otherwise.
#[allow(clippy::too_many_arguments)]
pub(crate) fn create(
    height: u64,
    round: Round,
    account: &Account,
    proposer: ContractAddress,
    main_storage: Storage,
    compiler_resource_limits: pathfinder_compiler::ResourceLimits,
    blockifier_libfuncs: pathfinder_compiler::BlockifierLibfuncs,
) -> anyhow::Result<(Vec<ProposalPart>, ConsensusFinalizedL2Block)> {
    let mut db_conn = main_storage.connection()?;
    let db_txn = db_conn.transaction()?;

    if devnet::devnet_genesis_exists(&db_txn)? {
        create_from_bootstrapped_devnet_db(
            &db_txn,
            height,
            round,
            account,
            proposer,
            main_storage,
            compiler_resource_limits,
            blockifier_libfuncs,
        )
    } else {
        create_with_invalid_l1_handler_transactions(
            &db_txn,
            height,
            round,
            proposer,
            main_storage,
            compiler_resource_limits,
            blockifier_libfuncs,
            None,
        )
    }
}

/// Creates a proposal with realistic transactions for the given height and
/// round, which:
/// - Deploys the "Hello Starknet" contract if it has not been deployed yet
/// - Invokes the "increase_balance" and "get_balance" functions of random
///   deployed instances
/// - Deploys more instances of the "Hello Starknet" contract randomly
#[allow(clippy::too_many_arguments)]
pub(crate) fn create_from_bootstrapped_devnet_db(
    db_txn: &pathfinder_storage::Transaction<'_>,
    height: u64,
    round: Round,
    account: &Account,
    proposer: ContractAddress,
    main_storage: Storage,
    compiler_resource_limits: pathfinder_compiler::ResourceLimits,
    blockifier_libfuncs: pathfinder_compiler::BlockifierLibfuncs,
) -> anyhow::Result<(Vec<ProposalPart>, ConsensusFinalizedL2Block)> {
    // TODO setting these constant to higher values can lead to weird panics in
    // other validator (Pathfinder) nodes, which we need to investigate
    const MAX_NUM_BATCHES: usize = 2;
    // Number of loop iterations per batch, each iteration can add at most 3
    // transactions, so max batch length is MAX_BATCH_TRIES * 3.
    const MAX_BATCH_TRIES: usize = 2;

    let round = round.as_u32().context(format!(
        "Attempted to create proposal with Nil round at height {height}"
    ))?;

    // We update the account entity each time, because the previously created
    // transactions could have not gone into the committed block and thus we
    // want to be sure we have the correct initial account nonce value and we are
    // sure of the previous deployments before we start the new proposal.
    account.update(
        db_txn,
        BlockNumber::new(height)
            .context("Height exceeds i64::MAX")?
            .parent(),
    )?;

    let deployed_in_db = account.deployed();

    // We generate up to 10 batches of up to 30 transactions and then randomly pick
    // how many of those transactions we execute.
    let seed = thread_rng().gen::<u64>();
    let mut rng = rand_chacha::ChaCha12Rng::seed_from_u64(seed);

    let mut batches = Vec::new();
    let mut next_txn_idx_start = 0;

    // HelloStarknet must be declared first, so empty proposals are only allowed
    // from height 2 onwards.
    let empty_proposal = height >= 2 && rng.gen_bool(EMPTY_PROPOSAL_PROBABILITY);

    tracing::debug!(%height, %round, %seed, %empty_proposal, "Creating dummy proposal");

    if empty_proposal {
        // Skip building any transaction batches.
    } else if height == 1 {
        // Bootstrapped devnet DB already contains the genesis block, so the declaration
        // of HelloStarknet falls into block number 1.
        let first_batch = vec![account.hello_starknet_declare()?];
        next_txn_idx_start += first_batch.len();
        batches.push(first_batch);
    } else {
        // HelloStarknet need to be deployed at least once before we can invoke it, so
        // if there are no deployments in the DB we just create a batch with the deploy
        // transaction.
        if deployed_in_db.is_empty() {
            // Declare goes into the first proposal, that's it
            let first_batch = vec![account.hello_starknet_deploy()?];
            next_txn_idx_start += first_batch.len();
            batches.push(first_batch);
        } else {
            let num_batches = rng.gen_range(1..=MAX_NUM_BATCHES);

            for _ in 0..num_batches {
                let batch_tries = rng.gen_range(1..=MAX_BATCH_TRIES);
                let mut batch = Vec::new();

                for _ in 0..batch_tries {
                    // Maybe deploy another instance
                    if rng.gen() {
                        batch.push(account.hello_starknet_deploy()?);
                    }

                    // Invoke a random contract instance if there are any deployments in the DB
                    if let Some(contract_address) = deployed_in_db.choose(&mut rng) {
                        batch.push(account.hello_starknet_increase_balance(
                            *contract_address,
                            rng.gen_range(1..=1000),
                        ));
                        // This is a view function, but it still gives us a realistic transaction
                        batch.push(account.hello_starknet_get_balance(*contract_address));
                    }

                    next_txn_idx_start += batch.len();
                }

                if !batch.is_empty() {
                    batches.push(batch);
                }
            }
        }
    }

    let num_executed_txns = if empty_proposal {
        0
    } else {
        rng.gen_range(1..=next_txn_idx_start)
    };
    let txns_to_execute = batches
        .iter()
        .flatten()
        .take(num_executed_txns)
        .cloned()
        .collect();

    // This is fine because `wait_for_parent_committed` is called first
    let latest_timestamp = db_txn.block_header(BlockId::Latest)?.map(|h| h.timestamp);

    let worker_pool = ExecutorWorkerPool::<ConcurrentStateReader>::auto().get();
    let (mut validator, mut parts) = devnet::init_proposal_and_validator(
        BlockNumber::new(height).context("Height exceeds i64::MAX")?,
        round,
        Address(proposer.0),
        latest_timestamp,
        main_storage.clone(),
        worker_pool.clone(),
    )?;
    validator.execute_batch::<ProdTransactionMapper>(
        txns_to_execute,
        compiler_resource_limits,
        blockifier_libfuncs,
    )?;
    let block = validator.consensus_finalize0()?;
    let worker_pool = Arc::into_inner(worker_pool).context("Failed join worker pool")?;
    worker_pool.join();

    if !empty_proposal {
        parts.extend(batches.into_iter().map(ProposalPart::TransactionBatch));
    }
    parts.push(ProposalPart::Fin(ProposalFin {
        proposal_commitment: Hash(block.header.state_diff_commitment.0),
        executed_transaction_count: num_executed_txns as u64,
        fin_payload: None,
    }));

    Ok((parts, block))
}

#[derive(Debug)]
pub(crate) struct ProposalCreationConfig {
    pub num_batches: NonZeroUsize,
    pub batch_len: NonZeroUsize,
    pub num_executed_txns: NonZeroUsize,
}

/// Creates a dummy proposal for the given height and round, filling it with
/// random L1 handler transactions, which all ultimately will be reverted.
#[allow(clippy::too_many_arguments)]
pub(crate) fn create_with_invalid_l1_handler_transactions(
    db_txn: &pathfinder_storage::Transaction<'_>,
    height: u64,
    round: Round,
    proposer: ContractAddress,
    main_storage: Storage,
    compiler_resource_limits: pathfinder_compiler::ResourceLimits,
    blockifier_libfuncs: pathfinder_compiler::BlockifierLibfuncs,
    config: Option<ProposalCreationConfig>,
) -> anyhow::Result<(Vec<ProposalPart>, ConsensusFinalizedL2Block)> {
    let round = round.as_u32().context(format!(
        "Attempted to create proposal with Nil round at height {height}"
    ))?;

    // This is fine because `wait_for_parent_committed` is called first
    let latest_timestamp = db_txn.block_header(BlockId::Latest)?.map(|h| h.timestamp);

    let seed = thread_rng().gen::<u64>();
    let mut rng = rand_chacha::ChaCha12Rng::seed_from_u64(seed);

    let empty_proposal = config.is_none() && rng.gen_bool(EMPTY_PROPOSAL_PROBABILITY);

    tracing::debug!(%height, %round, %seed, %empty_proposal, ?config, "Creating dummy proposal with invalid L1 handler transactions");

    let mut batches = Vec::new();
    let mut next_txn_idx_start = 0;

    if !empty_proposal {
        let num_batches = config
            .as_ref()
            .map(|c| c.num_batches.get())
            .unwrap_or_else(|| rng.gen_range(1..=10));

        for _ in 1..=num_batches {
            let batch_len = config
                .as_ref()
                .map(|c| c.batch_len.get())
                .unwrap_or_else(|| rng.gen_range(1..=10));

            let batch = create_transaction_batch(
                height as u32,
                next_txn_idx_start,
                batch_len,
                ChainId::SEPOLIA_TESTNET,
            );

            batches.push(batch);
            next_txn_idx_start += batch_len;
        }
    }

    let proposal_init = ProposalInit {
        height,
        round,
        valid_round: None,
        proposer: Address(proposer.0),
        timestamp: strictly_increasing_timestamp(latest_timestamp).get(),
        builder: Address(proposer.0),
        l1_da_mode: L1DataAvailabilityMode::Calldata,
        l2_gas_price_fri: 1_000_000,
        l1_gas_price_fri: 1_000_000,
        l1_data_gas_price_fri: 1_000_000,
        l1_gas_price_wei: 1_000_000,
        l1_data_gas_price_wei: 1_000_000,
        starknet_version: StarknetVersion::V_0_14_0.to_string(),
        version_constant_commitment: Hash::ZERO,
    };

    let mut parts = vec![ProposalPart::Init(proposal_init.clone())];

    let validator = ValidatorBlockInfoStage::new(ChainId::SEPOLIA_TESTNET, proposal_init)?;
    let worker_pool = ExecutorWorkerPool::<ConcurrentStateReader>::new(1).get();
    let mut validator =
        validator.skip_validation(main_storage, worker_pool.clone(), DecidedBlocks::default())?;

    let num_executed_txns = if empty_proposal {
        0
    } else {
        config
            .as_ref()
            .map(|c| c.num_executed_txns.get())
            .unwrap_or_else(|| rng.gen_range(1..=next_txn_idx_start))
    };

    let txns_to_execute = batches
        .iter()
        .flatten()
        .take(num_executed_txns)
        .cloned()
        .collect();

    if !empty_proposal {
        parts.extend(batches.into_iter().map(ProposalPart::TransactionBatch));
    }

    validator
        .execute_batch::<ProdTransactionMapper>(
            txns_to_execute,
            compiler_resource_limits,
            blockifier_libfuncs,
        )
        .unwrap();

    let block = validator.consensus_finalize0()?;

    parts.push(ProposalPart::Fin(ProposalFin {
        proposal_commitment: Hash(block.header.state_diff_commitment.0),
        executed_transaction_count: num_executed_txns as u64,
        fin_payload: None,
    }));

    let worker_pool = Arc::into_inner(worker_pool).context("Failed join worker pool")?;
    worker_pool.join();

    Ok((parts, block))
}

/// Creates a batch of transactions for testing
pub fn create_transaction_batch(
    seed: u32,
    start_index: usize,
    count: usize,
    chain_id: ChainId,
) -> Vec<p2p_proto::consensus::Transaction> {
    (start_index..start_index + count)
        .map(|i| create_l1_handler_transaction(seed, i, chain_id))
        .collect()
}

/// Creates a realistic L1Handler transaction for testing
///
/// `seed` is used to vary the transaction content independently of `index`,
/// so that we don't encounter duplicate transaction hashes across
/// multiple blocks.
pub fn create_l1_handler_transaction(
    seed: u32,
    index: usize,
    chain_id: ChainId,
) -> p2p_proto::consensus::Transaction {
    // base is a seed and index dependent value to avoid collisions but at the same
    // time easily allow to trace back which seed/index produced the transaction
    let base = index as u64 + ((seed as u64) << 32);
    let base = Felt::from_u64(base);

    // Create the L1Handler transaction
    let txn = p2p_proto::consensus::TransactionVariant::L1HandlerV0(
        p2p_proto::transaction::L1HandlerV0 {
            nonce: base,
            address: Address(base),
            entry_point_selector: base,
            calldata: vec![base],
        },
    );

    // Calculate the correct hash
    let l1_handler = pathfinder_common::transaction::L1HandlerTransaction {
        nonce: pathfinder_common::TransactionNonce(base),
        contract_address: ContractAddress::new_or_panic(base),
        entry_point_selector: pathfinder_common::EntryPoint(base),
        calldata: vec![pathfinder_common::CallParam(base)],
    };

    let hash = l1_handler.calculate_hash(chain_id);

    p2p_proto::consensus::Transaction {
        transaction_hash: p2p_proto::common::Hash(hash.0),
        txn,
    }
}

/// Creates a test proposal init and block info for the given height and round.
#[cfg(test)]
pub(crate) fn create_test_proposal_init(
    _chain_id: ChainId,
    height: u64,
    round: u32,
    proposer: ContractAddress,
) -> ProposalInit {
    let proposer_address = Address(proposer.0);
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    ProposalInit {
        height,
        round,
        valid_round: None,
        proposer: proposer_address,
        timestamp,
        builder: proposer_address,
        l1_da_mode: L1DataAvailabilityMode::default(),
        l2_gas_price_fri: 1,
        l1_gas_price_fri: 1_000_000_000,
        l1_data_gas_price_fri: 1,
        l1_gas_price_wei: 1_000_000_000,
        l1_data_gas_price_wei: 1,
        starknet_version: StarknetVersion::V_0_14_0.to_string(),
        version_constant_commitment: Default::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_l1_handler_transaction() {
        let chain_id = ChainId::SEPOLIA_TESTNET;
        let tx = create_l1_handler_transaction(0, 1, chain_id);

        // Verify the transaction has a valid hash
        assert!(!tx.transaction_hash.0.is_zero());

        // Verify it's an L1Handler transaction
        match tx.txn {
            p2p_proto::consensus::TransactionVariant::L1HandlerV0(_) => {}
            _ => panic!("Expected L1Handler transaction"),
        }
    }

    #[test]
    fn test_create_transaction_batch() {
        let chain_id = ChainId::SEPOLIA_TESTNET;
        let batch = create_transaction_batch(0, 10, 5, chain_id);

        assert_eq!(batch.len(), 5);

        // Verify all transactions have different hashes
        let hashes: std::collections::HashSet<_> =
            batch.iter().map(|tx| tx.transaction_hash.0).collect();
        assert_eq!(hashes.len(), 5); // All unique
    }
}
