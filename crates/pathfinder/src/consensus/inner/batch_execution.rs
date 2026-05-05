//! Batch execution manager with rollback support for executed transaction count
//!
//! This module provides functionality to handle optimistic execution
//! of transaction batches with the ability to rollback when the
//! executed transaction count indicates fewer transactions were
//! actually executed by the proposer.

use std::collections::{HashMap, HashSet};

use anyhow::Context;
use p2p::consensus::HeightAndRound;
use p2p_proto::consensus as proto_consensus;
use pathfinder_common::{DecidedBlocks, StarknetVersion};
use pathfinder_gas_price::{L1GasPriceProvider, L2GasPriceProvider};
use pathfinder_storage::Storage;
use pathfinder_validator::error::ProposalHandlingError;
use pathfinder_validator::{
    should_defer_validation,
    TransactionExt,
    ValidatorStage,
    ValidatorTransactionBatchStage,
    ValidatorWorkerPool,
};

/// Manages batch execution with rollback support for executed transaction count
#[derive(Clone)]
pub struct BatchExecutionManager {
    /// Tracks which proposals (height/round) have started execution.
    /// An entry exists here if at least one batch has been executed (not
    /// deferred).
    executing: HashSet<HeightAndRound>,
    /// Gas price provider for block info validation.
    gas_price_provider: Option<L1GasPriceProvider>,
    l2_gas_price_provider: Option<L2GasPriceProvider>,
    /// Worker pool for concurrent execution.
    worker_pool: ValidatorWorkerPool,
    compiler_resource_limits: pathfinder_compiler::ResourceLimits,
    blockifier_libfuncs: pathfinder_compiler::BlockifierLibfuncs,
    my_starknet_version: StarknetVersion,
}

impl BatchExecutionManager {
    /// Create a new batch execution manager
    pub fn new(
        gas_price_provider: Option<L1GasPriceProvider>,
        l2_gas_price_provider: Option<L2GasPriceProvider>,
        worker_pool: ValidatorWorkerPool,
        compiler_resource_limits: pathfinder_compiler::ResourceLimits,
        blockifier_libfuncs: pathfinder_compiler::BlockifierLibfuncs,
        my_starknet_version: StarknetVersion,
    ) -> Self {
        Self {
            executing: HashSet::new(),
            gas_price_provider,
            l2_gas_price_provider,
            worker_pool,
            compiler_resource_limits,
            blockifier_libfuncs,
            my_starknet_version,
        }
    }

    /// Check if execution has started for the given height and round
    ///
    /// Returns `true` if at least one batch has been executed (not deferred)
    /// for this height/round.
    #[cfg(test)]
    pub fn is_executing(&self, height_and_round: &HeightAndRound) -> bool {
        self.executing.contains(height_and_round)
    }

    /// Process a transaction batch with deferral support
    ///
    /// This is the main method that should be used by the P2P task
    pub fn process_batch_with_deferral<T: TransactionExt>(
        &mut self,
        height_and_round: HeightAndRound,
        transactions: Vec<proto_consensus::Transaction>,
        validator_stage: ValidatorStage,
        main_db: Storage,
        decided_blocks: DecidedBlocks,
        deferred_executions: &mut HashMap<HeightAndRound, DeferredExecution>,
    ) -> Result<ValidatorStage, ProposalHandlingError> {
        let mut main_db_conn = main_db
            .connection()
            .context("Creating database connection for batch execution with deferral")
            .map_err(ProposalHandlingError::Fatal)?;
        let main_db_tx = main_db_conn
            .transaction()
            .context("Creating database transaction for batch execution with deferral")
            .map_err(ProposalHandlingError::Fatal)?;
        // Check if execution should be deferred
        if should_defer_validation(
            height_and_round.height(),
            decided_blocks.clone(),
            &main_db_tx,
        )? {
            tracing::debug!(
                "🖧  ⚙️ transaction batch execution for height and round {height_and_round} is \
                 deferred"
            );

            // Defer execution - add to deferred_executions
            deferred_executions
                .entry(height_and_round)
                .or_default()
                .transactions
                .extend(transactions);
            return Ok(validator_stage);
        }

        let deferred = deferred_executions.remove(&height_and_round);

        // Execute any previously deferred transactions first
        let deferred_txns_len = deferred.as_ref().map_or(0, |d| d.transactions.len());
        let deferred_executed_transaction_count =
            deferred.as_ref().and_then(|d| d.executed_transaction_count);

        let mut all_transactions = transactions;
        let mut validator = if let Some(DeferredExecution {
            transactions: mut deferred_txns,
            ..
        }) = deferred
        {
            // Deferred transactions arrived first, so they should be executed first.
            // Prepend them to the new transactions.
            deferred_txns.extend(all_transactions);
            all_transactions = deferred_txns;
            match validator_stage {
                ValidatorStage::BlockInfo(stage) => {
                    stage.validate_block_info(
                        main_db.clone(),
                        decided_blocks,
                        self.gas_price_provider.clone(),
                        None, // TODO: Add L1ToFriValidator when oracle is available
                        self.l2_gas_price_provider.as_ref(),
                        self.worker_pool.clone(),
                        self.my_starknet_version,
                    )?
                }
                ValidatorStage::TransactionBatch(stage) => stage,
            }
        } else {
            validator_stage
                .try_into_transaction_batch_stage()
                .map_err(|e| ProposalHandlingError::Recoverable(e.into()))?
        };

        // Execute the batch
        validator.execute_batch::<T>(
            all_transactions,
            self.compiler_resource_limits,
            self.blockifier_libfuncs,
        )?;

        // Mark that execution has started for this height/round
        self.executing.insert(height_and_round);

        tracing::debug!(
            "Transaction batch execution for height and round {height_and_round} is complete, \
             additionally {deferred_txns_len} previously deferred transactions were executed",
        );

        // If executed transaction count was deferred (execution could
        // not start before ProposalFin arrived, because the parent
        // block wasn't finished yet), process it now that execution
        // has started.  Assuming message ordering is guaranteed...
        // (see p2p::consensus::handle_incoming_proposal_message)
        // ...if executed transaction count is set (by ProposalFin
        // processing), all transaction batches are also in the
        // deferred entry, so we can safely process executed
        // transaction count here.
        if let Some(executed_txn_count) = deferred_executed_transaction_count {
            tracing::debug!(
                "Processing deferred executed transaction count for {height_and_round} after \
                 batch execution started"
            );
            self.process_executed_transaction_count::<T>(
                height_and_round,
                executed_txn_count,
                &mut validator,
            )?;
        }

        Ok(ValidatorStage::TransactionBatch(validator))
    }

    /// Execute a batch of transactions and track execution state
    ///
    /// This is a simpler variant that doesn't handle deferral - used when we
    /// know execution should proceed immediately (e.g., when executing
    /// previously deferred transactions after the parent block is
    /// committed).
    pub fn execute_batch<T: TransactionExt>(
        &mut self,
        height_and_round: HeightAndRound,
        transactions: Vec<proto_consensus::Transaction>,
        validator: &mut ValidatorTransactionBatchStage,
    ) -> Result<(), ProposalHandlingError> {
        // Mark that execution has started for this height/round, even
        // if batch is empty.
        self.executing.insert(height_and_round);

        if transactions.is_empty() {
            tracing::debug!(
                "Empty transaction batch for height and round {height_and_round} - execution \
                 marked as started"
            );
            return Ok(());
        }

        // Execute the batch
        validator.execute_batch::<T>(
            transactions,
            self.compiler_resource_limits,
            self.blockifier_libfuncs,
        )?;

        tracing::debug!(
            "Transaction batch execution for height and round {height_and_round} is complete"
        );

        Ok(())
    }

    /// Processes executed transaction count immediately with rollback support.
    ///
    /// Assumes execution has already started (at least one batch executed).
    /// If transactions are deferred, deferral should be handled by the
    /// caller before calling this function.
    pub fn process_executed_transaction_count<T: TransactionExt>(
        &mut self,
        height_and_round: HeightAndRound,
        executed_transaction_count: u64,
        validator: &mut ValidatorTransactionBatchStage,
    ) -> Result<(), ProposalHandlingError> {
        // Verify that execution has started (at least one batch was executed, not
        // deferred)
        if !self.executing.contains(&height_and_round) {
            return Err(ProposalHandlingError::Fatal(anyhow::anyhow!(
                "No execution state found for {height_and_round}. Execution should have started \
                 before processing executed transaction count."
            )));
        }

        let target_transaction_count = executed_transaction_count as usize;
        let current_transaction_count = validator.transaction_count();

        tracing::debug!(
            height_and_round = ?height_and_round,
            target = target_transaction_count,
            current = current_transaction_count,
            "Processing executed transaction count"
        );

        if target_transaction_count < current_transaction_count {
            tracing::info!(
                "Rolling back {height_and_round} from {} to {} transactions",
                current_transaction_count,
                target_transaction_count
            );
            validator.rollback_to_transaction::<T>(target_transaction_count)?;
        } else if target_transaction_count > current_transaction_count {
            // This shouldn't happen with proper message ordering and no protocol errors.
            // Ordering is guaranteed by p2p::consensus::handle_incoming_proposal_message.
            // ProposalFin should arrive after all TransactionBatches, so we
            // should have at least as many transactions as its
            // executed transaction count indicates.
            tracing::warn!(
                "Executed transaction count for {height_and_round} indicates {} transactions, but \
                 we only have {} transactions. This may indicate a protocol violation or missing \
                 batches.",
                target_transaction_count,
                current_transaction_count
            );
        }

        let final_transaction_count = validator.transaction_count();
        tracing::info!(
            "Finalized {height_and_round} with {final_transaction_count} executed transactions"
        );

        Ok(())
    }

    /// Clean up completed executions
    pub fn cleanup(&mut self, height_and_round: &HeightAndRound) {
        let had_execution = self.executing.remove(height_and_round);
        if had_execution {
            tracing::debug!("Cleaned up execution state for {height_and_round}");
        }
    }
}

/// Represents transactions received from the network that are waiting
/// for previous block to be committed before they can be
/// executed. Also holds optional proposal commitment and executed
/// transaction count in case ProposalFin arrives while transactions
/// are deferred.
#[derive(Debug, Clone, Default)]
pub struct DeferredExecution {
    pub transactions: Vec<proto_consensus::Transaction>,
    pub commitment: Option<ProposalCommitmentWithOrigin>,
    pub executed_transaction_count: Option<u64>,
}

/// Proposal commitment and the address of its proposer.
#[derive(Debug, Clone)]
pub struct ProposalCommitmentWithOrigin {
    pub proposal_commitment: pathfinder_common::ProposalCommitment,
    pub proposer_address: pathfinder_common::ContractAddress,
    pub pol_round: pathfinder_consensus::Round,
}

impl Default for ProposalCommitmentWithOrigin {
    fn default() -> Self {
        Self {
            proposal_commitment: pathfinder_common::ProposalCommitment::default(),
            proposer_address: pathfinder_common::ContractAddress::default(),
            pol_round: pathfinder_consensus::Round::nil(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use p2p::consensus::HeightAndRound;
    use pathfinder_common::prelude::*;
    use pathfinder_common::BlockId;
    use pathfinder_crypto::Felt;
    use pathfinder_executor::{ConcurrentStateReader, ExecutorWorkerPool};
    use pathfinder_storage::StorageBuilder;
    use pathfinder_validator::{ProdTransactionMapper, ValidatorBlockInfoStage};

    use super::*;
    use crate::consensus::inner::dummy_proposal::{
        create_test_proposal_init,
        create_transaction_batch,
    };

    /// Creates a worker pool for tests.
    fn create_test_worker_pool() -> ValidatorWorkerPool {
        ExecutorWorkerPool::<ConcurrentStateReader>::new(1).get()
    }

    /// Helper function to create a committed parent block in storage
    fn create_committed_parent_block(
        storage: &pathfinder_storage::Storage,
        parent_height: u64,
    ) -> anyhow::Result<()> {
        let mut db_conn = storage.connection()?;
        let db_tx = db_conn.transaction()?;
        let block_id = BlockId::Number(BlockNumber::new_or_panic(parent_height));

        // Check if block already exists
        if db_tx.block_exists(block_id)? {
            return Ok(());
        }

        // Create a unique hash for this block to avoid conflicts
        let hash = BlockHash(Felt::from_hex_str(&format!("0x{parent_height:064x}")).unwrap());
        let parent_header = BlockHeader::builder()
            .number(BlockNumber::new_or_panic(parent_height))
            .timestamp(BlockTimestamp::new_or_panic(1000))
            .calculated_state_commitment(StorageCommitment(Felt::ZERO), ClassCommitment(Felt::ZERO))
            .sequencer_address(SequencerAddress::ZERO)
            .finalize_with_hash(hash);
        db_tx.insert_block_header(&parent_header)?;
        db_tx.commit()?;
        Ok(())
    }

    fn create_test_proposal(height: u64) -> p2p_proto::consensus::ProposalInit {
        p2p_proto::consensus::ProposalInit {
            height,
            round: 1,
            valid_round: None,
            proposer: p2p_proto::common::Address::default(),
            timestamp: 1000,
            builder: p2p_proto::common::Address::default(),
            l1_da_mode: p2p_proto::common::L1DataAvailabilityMode::Calldata,
            l2_gas_price_fri: 0,
            l1_gas_price_wei: 0,
            l1_data_gas_price_wei: 0,
            l1_gas_price_fri: 0,
            l1_data_gas_price_fri: 0,
            starknet_version: StarknetVersion::V_0_14_0.to_string(),
            version_constant_commitment: Default::default(),
        }
    }

    /// Test that BatchExecutionManager correctly tracks execution
    /// state.
    #[tokio::test]
    async fn test_execution_state_tracking() {
        let storage = StorageBuilder::in_tempdir().expect("Failed to create temp database");
        let chain_id = ChainId::SEPOLIA_TESTNET;
        let worker_pool = create_test_worker_pool();
        let proposal_init = create_test_proposal(1);

        let mut validator_stage = ValidatorBlockInfoStage::new(chain_id, proposal_init)
            .and_then(|v| {
                v.skip_validation(storage, Arc::clone(&worker_pool), DecidedBlocks::default())
            })
            .expect("Failed to create validator stage");
        let mut batch_execution_manager = BatchExecutionManager::new(
            None,
            None,
            worker_pool,
            pathfinder_compiler::ResourceLimits::for_test(),
            pathfinder_compiler::BlockifierLibfuncs::default(),
            StarknetVersion::V_0_14_0,
        );
        let height_and_round = HeightAndRound::new(2, 1);

        // Initially, execution should not have started
        assert!(
            !batch_execution_manager.is_executing(&height_and_round),
            "Execution should not have started initially"
        );

        // Execute a batch to start execution
        let transactions = create_transaction_batch(0, 0, 5, chain_id);
        batch_execution_manager
            .execute_batch::<ProdTransactionMapper>(
                height_and_round,
                transactions,
                &mut validator_stage,
            )
            .expect("Failed to execute batch");

        // Verify execution has started
        assert!(
            batch_execution_manager.is_executing(&height_and_round),
            "Execution should have started after execute_batch"
        );

        // Now process executed transaction count
        let executed_transaction_count = 5;
        batch_execution_manager
            .process_executed_transaction_count::<ProdTransactionMapper>(
                height_and_round,
                executed_transaction_count,
                &mut validator_stage,
            )
            .expect("Failed to process executed transaction count");
    }

    #[tokio::test]
    async fn test_executed_transaction_count_before_any_batch() {
        let storage = StorageBuilder::in_tempdir().expect("Failed to create temp database");
        let chain_id = ChainId::SEPOLIA_TESTNET;

        // Create and commit parent block (height 1) so height 2 won't be deferred
        {
            let mut db_conn = storage.connection().unwrap();
            let db_tx = db_conn.transaction().unwrap();
            let parent_header = BlockHeader::builder()
                .number(BlockNumber::new_or_panic(1))
                .timestamp(BlockTimestamp::new_or_panic(1000))
                .calculated_state_commitment(
                    StorageCommitment(Felt::ZERO),
                    ClassCommitment(Felt::ZERO),
                )
                .sequencer_address(SequencerAddress::ZERO)
                .finalize_with_hash(BlockHash(Felt::ZERO));
            db_tx.insert_block_header(&parent_header).unwrap();
            db_tx.commit().unwrap();
        }

        let height_and_round = HeightAndRound::new(2, 1);
        let proposer_address = p2p_proto::common::Address(Felt::from_hex_str("0x456").unwrap());
        let proposal_init = proto_consensus::ProposalInit {
            height: height_and_round.height(),
            round: height_and_round.round(),
            valid_round: None,
            proposer: proposer_address,
            timestamp: 2000,
            builder: proposer_address,
            l1_da_mode: p2p_proto::common::L1DataAvailabilityMode::Calldata,
            l2_gas_price_fri: 0,
            l1_gas_price_fri: 0,
            l1_data_gas_price_fri: 0,
            l1_gas_price_wei: 0,
            l1_data_gas_price_wei: 0,
            starknet_version: StarknetVersion::V_0_14_0.to_string(),
            version_constant_commitment: Default::default(),
        };

        let worker_pool = create_test_worker_pool();
        let mut batch_execution_manager = BatchExecutionManager::new(
            None,
            None,
            worker_pool,
            pathfinder_compiler::ResourceLimits::for_test(),
            pathfinder_compiler::BlockifierLibfuncs::default(),
            StarknetVersion::V_0_14_0,
        );

        let mut deferred_executions: std::collections::HashMap<HeightAndRound, DeferredExecution> =
            std::collections::HashMap::new();

        // Initially, no execution and no deferred entry
        assert!(
            !batch_execution_manager.is_executing(&height_and_round),
            "Execution should not have started initially"
        );
        assert!(
            !deferred_executions.contains_key(&height_and_round),
            "No deferred entry should exist initially"
        );

        // Step 1: executed transaction count arrives when execution
        // hasn't started yet (Note: With P2P message ordering
        // guarantees, ProposalFin will always arrive after all
        // TransactionBatches, but execution would not have started if
        // batches were deferred.)
        let executed_transaction_count = 5;

        // Simulate the fix: create deferred entry and store executed
        // transaction count.
        let deferred = deferred_executions.entry(height_and_round).or_default();
        deferred.executed_transaction_count = Some(executed_transaction_count);
        // Verify executed transaction count was stored
        assert!(
            deferred_executions
                .get(&height_and_round)
                .and_then(|d| d.executed_transaction_count.as_ref())
                .is_some(),
            "Executed transaction count should be stored in deferred entry"
        );

        let validator_stage = ValidatorBlockInfoStage::new(chain_id, proposal_init)
            .map(ValidatorStage::BlockInfo)
            .expect("Failed to create validator stage");

        // Step 2: TransactionBatch arrives and executes
        let transactions = create_transaction_batch(0, 0, 5, chain_id);
        let next_stage = batch_execution_manager
            .process_batch_with_deferral::<ProdTransactionMapper>(
                height_and_round,
                transactions,
                validator_stage,
                storage.clone(),
                DecidedBlocks::default(),
                &mut deferred_executions,
            )
            .expect("Failed to process batch");

        // Verify execution has started
        assert!(
            batch_execution_manager.is_executing(&height_and_round),
            "Execution should have started after batch execution"
        );

        // Verify validator state matches executed transaction count
        assert!(
            matches!(
                next_stage,
                ValidatorStage::TransactionBatch(stage) if stage.transaction_count() == 5
            ),
            "Validator should have 5 transactions matching executed transaction count"
        );
    }

    /// Test deferral and immediate execution of transaction batches.
    /// A few things are covered here:
    /// - deferral when parent not committed,
    /// - immediate execution when parent committed
    /// - deferred batch execution
    /// - multiple batches with mixed deferral
    #[tokio::test]
    async fn test_deferral_and_execution() {
        let storage = StorageBuilder::in_tempdir().expect("Failed to create temp database");
        let chain_id = ChainId::SEPOLIA_TESTNET;
        let worker_pool = create_test_worker_pool();

        let height_and_round = HeightAndRound::new(2, 1);
        let proposer_address = ContractAddress::new_or_panic(Felt::from_hex_str("0x456").unwrap());
        let proposal_init = create_test_proposal_init(
            chain_id,
            height_and_round.height(),
            height_and_round.round(),
            proposer_address,
        );
        let validator_stage = ValidatorBlockInfoStage::new(chain_id, proposal_init)
            .map(ValidatorStage::BlockInfo)
            .expect("Failed to create validator stage");

        let mut batch_execution_manager = BatchExecutionManager::new(
            None,
            None,
            worker_pool.clone(),
            pathfinder_compiler::ResourceLimits::for_test(),
            pathfinder_compiler::BlockifierLibfuncs::default(),
            StarknetVersion::V_0_14_0,
        );

        let mut deferred_executions: std::collections::HashMap<HeightAndRound, DeferredExecution> =
            std::collections::HashMap::new();
        deferred_executions.insert(height_and_round, DeferredExecution::default());

        // Test 1: Deferral when parent not committed
        let next_stage = {
            let transactions = create_transaction_batch(0, 0, 3, chain_id);

            let next_stage = batch_execution_manager
                .process_batch_with_deferral::<ProdTransactionMapper>(
                    height_and_round,
                    transactions,
                    validator_stage,
                    storage.clone(),
                    DecidedBlocks::default(),
                    &mut deferred_executions,
                )
                .expect("Failed to process batch");

            // Verify deferral: transactions stored, execution NOT started
            assert!(
                !batch_execution_manager.is_executing(&height_and_round),
                "Execution should NOT have started when parent not committed"
            );
            assert!(
                deferred_executions
                    .get(&height_and_round)
                    .map(|d| d.transactions.len())
                    .unwrap_or(0)
                    == 3,
                "Deferred transactions should be stored"
            );
            assert!(
                matches!(
                    next_stage,
                    ValidatorStage::BlockInfo(ref block_info) if block_info.proposal_height() == height_and_round.height()
                ),
                "Validator stage should remain at BlockInfo stage after deferral"
            );

            next_stage
        };

        // Test 2: Commit parent block and execute deferred batch
        // Create parent block at height 1 (required for height 2 to execute)
        create_committed_parent_block(&storage, 1).expect("Failed to create parent block");

        {
            let transactions = create_transaction_batch(0, 3, 2, chain_id);

            let next_stage = batch_execution_manager
                .process_batch_with_deferral::<ProdTransactionMapper>(
                    height_and_round,
                    transactions,
                    next_stage,
                    storage.clone(),
                    DecidedBlocks::default(),
                    &mut deferred_executions,
                )
                .expect("Failed to process batch");

            // Verify execution: deferred + new transactions executed, execution started
            assert!(
                batch_execution_manager.is_executing(&height_and_round),
                "Execution should have started after parent committed"
            );
            assert!(
                !deferred_executions.contains_key(&height_and_round),
                "Deferred entry should be removed after execution"
            );
            assert!(
                matches!(next_stage, ValidatorStage::TransactionBatch(ref stage) if stage.transaction_count() == 5),
                "Validator should transition to next stage and transactions (3 deferred + 2 new) \
                 should be executed"
            );
        }

        // Test 3: Multiple batches with immediate execution (parent already committed)
        // Create a new worker pool for the second validator to avoid potential issues
        // with the blockifier's ConcurrentTransactionExecutor and shared worker pools.
        let worker_pool_2 = create_test_worker_pool();
        let height_and_round_2 = HeightAndRound::new(3, 1);
        let proposal_init = create_test_proposal(height_and_round_2.height());
        let validator_stage_2 = ValidatorBlockInfoStage::new(chain_id, proposal_init)
            .and_then(|validator| {
                validator.skip_validation(
                    storage.clone(),
                    worker_pool_2.clone(),
                    DecidedBlocks::default(),
                )
            })
            .map(ValidatorStage::TransactionBatch)
            .expect("Failed to create validator stage");

        create_committed_parent_block(&storage, 2).expect("Failed to create parent block");

        {
            let mut next_stage = validator_stage_2;
            // Execute multiple batches
            for i in 0..3 {
                let transactions = create_transaction_batch(0, i * 2, 2, chain_id);
                next_stage = batch_execution_manager
                    .process_batch_with_deferral::<ProdTransactionMapper>(
                        height_and_round_2,
                        transactions,
                        next_stage,
                        storage.clone(),
                        DecidedBlocks::default(),
                        &mut deferred_executions,
                    )
                    .expect("Failed to process batch");
            }

            assert!(
                batch_execution_manager.is_executing(&height_and_round_2),
                "Execution should have started"
            );
            assert!(
                matches!(next_stage, ValidatorStage::TransactionBatch(stage) if stage.transaction_count() == 6),
                "All batches should be executed immediately"
            );
        }
    }

    /// Test executed transaction count processing with rollback support.
    #[tokio::test]
    async fn test_executed_transaction_count_rollback() {
        let storage = StorageBuilder::in_tempdir().expect("Failed to create temp database");
        let chain_id = ChainId::SEPOLIA_TESTNET;
        let worker_pool = create_test_worker_pool();
        let proposal_init = create_test_proposal(1);

        let mut validator_stage = ValidatorBlockInfoStage::new(chain_id, proposal_init)
            .and_then(|v| {
                v.skip_validation(storage, Arc::clone(&worker_pool), DecidedBlocks::default())
            })
            .expect("Failed to create validator stage");

        let mut batch_execution_manager = BatchExecutionManager::new(
            None,
            None,
            worker_pool,
            pathfinder_compiler::ResourceLimits::for_test(),
            pathfinder_compiler::BlockifierLibfuncs::default(),
            StarknetVersion::V_0_14_0,
        );
        let height_and_round = HeightAndRound::new(2, 1);

        // Execute multiple batches: 3 + 7 + 4 = 14 transactions total
        let batch1 = create_transaction_batch(0, 0, 3, chain_id);
        let batch2 = create_transaction_batch(0, 3, 7, chain_id);
        let batch3 = create_transaction_batch(0, 10, 4, chain_id);

        batch_execution_manager
            .execute_batch::<ProdTransactionMapper>(height_and_round, batch1, &mut validator_stage)
            .expect("Failed to execute batch 1");
        batch_execution_manager
            .execute_batch::<ProdTransactionMapper>(height_and_round, batch2, &mut validator_stage)
            .expect("Failed to execute batch 2");
        batch_execution_manager
            .execute_batch::<ProdTransactionMapper>(height_and_round, batch3, &mut validator_stage)
            .expect("Failed to execute batch 3");

        assert_eq!(
            validator_stage.transaction_count(),
            14,
            "Should have 14 transactions total"
        );

        // Test 1: Normal case - no rollback (executed transaction
        // count matches current count)
        {
            let executed_transaction_count = 14;

            batch_execution_manager
                .process_executed_transaction_count::<ProdTransactionMapper>(
                    height_and_round,
                    executed_transaction_count,
                    &mut validator_stage,
                )
                .expect("Failed to process executed transaction count");

            assert_eq!(
                validator_stage.transaction_count(),
                14,
                "Transaction count should remain 14 (no rollback)"
            );
        }

        // Test 2: Rollback case - executed transaction count
        // indicates fewer transactions. Create a new worker pool for
        // the second validator to avoid issues with blockifier's
        // ConcurrentTransactionExecutor and shared worker pools.
        let worker_pool_2 = create_test_worker_pool();

        // Re-execute batches to get back to 14 transactions
        let storage_2 = StorageBuilder::in_tempdir().expect("Failed to create temp database");
        let proposal_init = create_test_proposal(1);
        let mut validator_stage_2 = ValidatorBlockInfoStage::new(chain_id, proposal_init)
            .and_then(|validator| {
                validator.skip_validation(storage_2, worker_pool_2, DecidedBlocks::default())
            })
            .expect("Failed to create validator stage");

        let batch1_2 = create_transaction_batch(0, 0, 3, chain_id);
        let batch2_2 = create_transaction_batch(0, 3, 7, chain_id);
        let batch3_2 = create_transaction_batch(0, 10, 4, chain_id);

        let height_and_round_2 = HeightAndRound::new(3, 1);
        batch_execution_manager
            .execute_batch::<ProdTransactionMapper>(
                height_and_round_2,
                batch1_2,
                &mut validator_stage_2,
            )
            .expect("Failed to execute batch 1");
        batch_execution_manager
            .execute_batch::<ProdTransactionMapper>(
                height_and_round_2,
                batch2_2,
                &mut validator_stage_2,
            )
            .expect("Failed to execute batch 2");
        batch_execution_manager
            .execute_batch::<ProdTransactionMapper>(
                height_and_round_2,
                batch3_2,
                &mut validator_stage_2,
            )
            .expect("Failed to execute batch 3");

        let executed_transaction_count = 7; // Rollback from 14 to 7

        batch_execution_manager
            .process_executed_transaction_count::<ProdTransactionMapper>(
                height_and_round_2,
                executed_transaction_count,
                &mut validator_stage_2,
            )
            .expect("Failed to process executed transaction count with rollback");

        assert_eq!(
            validator_stage_2.transaction_count(),
            7,
            "Transaction count should be rolled back to 7 (matching executed transaction count)"
        );
    }

    /// Test empty batch handling.
    #[tokio::test]
    async fn test_empty_batch() {
        let storage = StorageBuilder::in_tempdir().expect("Failed to create temp database");
        let chain_id = ChainId::SEPOLIA_TESTNET;
        let worker_pool = create_test_worker_pool();
        let proposal_init = create_test_proposal(1);

        let mut validator_stage = ValidatorBlockInfoStage::new(chain_id, proposal_init)
            .and_then(|v| {
                v.skip_validation(storage, Arc::clone(&worker_pool), DecidedBlocks::default())
            })
            .expect("Failed to create validator stage");

        let mut batch_execution_manager = BatchExecutionManager::new(
            None,
            None,
            worker_pool,
            pathfinder_compiler::ResourceLimits::for_test(),
            pathfinder_compiler::BlockifierLibfuncs::default(),
            StarknetVersion::V_0_14_0,
        );
        let height_and_round = HeightAndRound::new(2, 1);

        // Empty batch still marks execution as started
        batch_execution_manager
            .execute_batch::<ProdTransactionMapper>(height_and_round, vec![], &mut validator_stage)
            .expect("Failed to execute empty batch");

        assert!(
            batch_execution_manager.is_executing(&height_and_round),
            "Execution should be marked as started even for empty batch"
        );
        assert_eq!(
            validator_stage.transaction_count(),
            0,
            "No transactions should be executed"
        );

        // executed transaction count can be processed after empty batch
        let executed_transaction_count = 0;

        batch_execution_manager
            .process_executed_transaction_count::<ProdTransactionMapper>(
                height_and_round,
                executed_transaction_count,
                &mut validator_stage,
            )
            .expect("Failed to process executed transaction count after empty batch");
    }

    /// Test that executed transaction count == 0 rolls back all
    /// transactions to zero. This covers the edge case where the
    /// proposer executed no transactions but the validator
    /// optimistically executed some.
    #[tokio::test]
    async fn test_executed_transaction_count_zero_rollback() {
        let storage = StorageBuilder::in_tempdir().expect("Failed to create temp database");
        let chain_id = ChainId::SEPOLIA_TESTNET;
        let worker_pool = create_test_worker_pool();
        let proposal_init = create_test_proposal(1);

        let mut validator_stage = ValidatorBlockInfoStage::new(chain_id, proposal_init)
            .and_then(|v| {
                v.skip_validation(storage, Arc::clone(&worker_pool), DecidedBlocks::default())
            })
            .expect("Failed to create validator stage");

        let mut batch_execution_manager = BatchExecutionManager::new(
            None,
            None,
            worker_pool,
            pathfinder_compiler::ResourceLimits::for_test(),
            pathfinder_compiler::BlockifierLibfuncs::default(),
            StarknetVersion::V_0_14_0,
        );
        let height_and_round = HeightAndRound::new(2, 1);

        // Execute a batch of 5 transactions
        let transactions = create_transaction_batch(0, 0, 5, chain_id);
        batch_execution_manager
            .execute_batch::<ProdTransactionMapper>(
                height_and_round,
                transactions,
                &mut validator_stage,
            )
            .expect("Failed to execute batch");

        assert_eq!(
            validator_stage.transaction_count(),
            5,
            "Should have 5 transactions before executed transaction count"
        );

        // ETC == 0 should roll back all transactions
        batch_execution_manager
            .process_executed_transaction_count::<ProdTransactionMapper>(
                height_and_round,
                0,
                &mut validator_stage,
            )
            .expect("Failed to process executed transaction count with zero rollback");

        assert_eq!(
            validator_stage.transaction_count(),
            0,
            "All transactions should be rolled back when ETC is 0"
        );
    }

    /// Test that executed transaction count > actual transaction
    /// count does not error or inflate the count. The validator
    /// continues with the transactions it has.
    #[tokio::test]
    async fn test_executed_transaction_count_exceeds_actual() {
        let storage = StorageBuilder::in_tempdir().expect("Failed to create temp database");
        let chain_id = ChainId::SEPOLIA_TESTNET;
        let worker_pool = create_test_worker_pool();
        let proposal_init = create_test_proposal(1);

        let mut validator_stage = ValidatorBlockInfoStage::new(chain_id, proposal_init)
            .and_then(|v| {
                v.skip_validation(storage, Arc::clone(&worker_pool), DecidedBlocks::default())
            })
            .expect("Failed to create validator stage");

        let mut batch_execution_manager = BatchExecutionManager::new(
            None,
            None,
            worker_pool,
            pathfinder_compiler::ResourceLimits::for_test(),
            pathfinder_compiler::BlockifierLibfuncs::default(),
            StarknetVersion::V_0_14_0,
        );
        let height_and_round = HeightAndRound::new(2, 1);

        // Execute a batch of 5 transactions
        let transactions = create_transaction_batch(0, 0, 5, chain_id);
        batch_execution_manager
            .execute_batch::<ProdTransactionMapper>(
                height_and_round,
                transactions,
                &mut validator_stage,
            )
            .expect("Failed to execute batch");

        assert_eq!(
            validator_stage.transaction_count(),
            5,
            "Should have 5 transactions before executed transaction count"
        );

        // ETC == 10 exceeds the 5 we have; should warn but not error
        batch_execution_manager
            .process_executed_transaction_count::<ProdTransactionMapper>(
                height_and_round,
                10,
                &mut validator_stage,
            )
            .expect("ETC exceeding actual count should not error");

        assert_eq!(
            validator_stage.transaction_count(),
            5,
            "Transaction count should remain unchanged when ETC exceeds actual"
        );
    }
}
