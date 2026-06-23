use std::sync::Arc;
use std::time::Instant;

use anyhow::Context;
use blockifier::blockifier::concurrent_transaction_executor::ConcurrentTransactionExecutor;
use blockifier::bouncer::BouncerConfig;
use blockifier::concurrency::worker_pool::WorkerPool;
use blockifier::context::BlockContext;
use blockifier::state::cached_state::CachedState;
use pathfinder_common::{ChainId, ClassHash, ContractAddress, DecidedBlocks, TransactionIndex};
use starknet_api::block::BlockHashAndNumber;

use crate::execution_state::{ExecutionState, VersionedConstantsMap};
use crate::pending::PendingStateReader;
use crate::state_reader::{ConcurrentStorageAdapter, PathfinderStateReader, StorageAdapter};
use crate::types::{
    to_receipt_and_events,
    transaction_declared_deprecated_class,
    transaction_type,
    BlockInfo,
    ReceiptAndEvents,
    StateDiff,
};
use crate::{IntoStarkFelt, Transaction, TransactionExecutionError};

/// Type alias for the concurrent executor's state reader.
pub type ConcurrentStateReader =
    PendingStateReader<PathfinderStateReader<ConcurrentStorageAdapter>>;

/// A block executor that uses blockifier's ConcurrentTransactionExecutor for
/// concurrent transaction execution with natural rollback support via
/// `close_block(n)`.
///
/// Note: When the executor is dropped without calling `close_block()` or
/// `abort_block()`, the Drop impl ensures the worker pool's scheduler is
/// halted, preventing deadlocks if the pool is reused.
pub struct ConcurrentBlockExecutor {
    executor: Option<ConcurrentTransactionExecutor<ConcurrentStateReader>>,
    block_context: Arc<BlockContext>,
    declared_deprecated_classes: Vec<ClassHash>,
    results: Vec<ReceiptAndEvents>,
    total_executed: usize,
}

impl ConcurrentBlockExecutor {
    /// Creates a new ConcurrentBlockExecutor for a block.
    ///
    /// This calls `pre_process_block` exactly once during initialization.
    /// The worker pool should be shared across multiple blocks for efficiency.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        chain_id: ChainId,
        block_info: BlockInfo,
        eth_fee_address: ContractAddress,
        strk_fee_address: ContractAddress,
        db_conn: pathfinder_storage::Connection,
        decided_blocks: DecidedBlocks,
        worker_pool: Arc<WorkerPool<CachedState<ConcurrentStateReader>>>,
        block_deadline: Option<Instant>,
    ) -> anyhow::Result<Self> {
        Self::new_with_config(
            chain_id,
            block_info,
            eth_fee_address,
            strk_fee_address,
            db_conn,
            decided_blocks,
            worker_pool,
            block_deadline,
            VersionedConstantsMap::default(),
        )
    }

    /// Creates a new ConcurrentBlockExecutor with custom versioned constants.
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_config(
        chain_id: ChainId,
        block_info: BlockInfo,
        eth_fee_address: ContractAddress,
        strk_fee_address: ContractAddress,
        db_conn: pathfinder_storage::Connection,
        decided_blocks: DecidedBlocks,
        worker_pool: Arc<WorkerPool<CachedState<ConcurrentStateReader>>>,
        block_deadline: Option<Instant>,
        versioned_constants_map: VersionedConstantsMap,
    ) -> anyhow::Result<Self> {
        let storage_adapter = ConcurrentStorageAdapter::new(db_conn, decided_blocks);

        let execution_state = ExecutionState::validation(
            chain_id,
            block_info,
            None,
            versioned_constants_map.clone(),
            eth_fee_address,
            strk_fee_address,
            None,
        );

        // Get block context and old block hash
        let (block_context, pending_state_reader, old_block_number_and_hash) =
            create_state_reader_components(
                &execution_state,
                storage_adapter,
                &block_info,
                &versioned_constants_map,
            )?;

        let block_context = Arc::new(block_context);

        // start_block calls pre_process_block exactly once
        let executor = ConcurrentTransactionExecutor::start_block(
            pending_state_reader,
            (*block_context).clone(),
            old_block_number_and_hash,
            worker_pool,
            block_deadline,
        )
        .context("Failed to start concurrent block executor")?;

        Ok(Self {
            executor: Some(executor),
            block_context,
            declared_deprecated_classes: Vec::new(),
            results: Vec::new(),
            total_executed: 0,
        })
    }

    /// Executes a batch of transactions concurrently.
    ///
    /// Results are accumulated internally and can be retrieved after execution
    /// or via `close_block()`.
    pub fn execute(
        &mut self,
        txns: Vec<Transaction>,
    ) -> Result<Vec<ReceiptAndEvents>, TransactionExecutionError> {
        if txns.is_empty() {
            return Ok(vec![]);
        }

        let start_tx_index = self.total_executed;
        let block_number = self.block_context.block_info().block_number;

        let _span = tracing::debug_span!(
            "ConcurrentBlockExecutor::execute",
            block_number = %block_number,
            from_tx_index = %start_tx_index,
            to_tx_index = %(start_tx_index + txns.len() - 1),
        )
        .entered();

        // Track deprecated classes
        for tx in &txns {
            if let Some(class) = transaction_declared_deprecated_class(tx) {
                self.declared_deprecated_classes.push(class);
            }
        }

        // Add transactions and wait for results
        let executor = self
            .executor
            .as_mut()
            .expect("executor should exist during execute");
        let execution_results = executor.add_txs_and_wait(&txns);

        let mut batch_results = Vec::with_capacity(execution_results.len());

        for (i, result) in execution_results.into_iter().enumerate() {
            let tx_index = start_tx_index + i;
            let tx = &txns[i];

            match result {
                Ok((tx_info, _state_maps)) => {
                    let tx_type = transaction_type(tx);
                    let gas_vector_computation_mode =
                        crate::transaction::gas_vector_computation_mode(tx);

                    let receipt_and_events = to_receipt_and_events(
                        tx_type,
                        TransactionIndex::new(tx_index.try_into().expect("ptr size is 64bits"))
                            .context("tx_index < i64::MAX")?,
                        tx_info,
                        self.block_context.versioned_constants(),
                        &gas_vector_computation_mode,
                    )
                    .map_err(TransactionExecutionError::Custom)?;

                    batch_results.push(receipt_and_events.clone());
                    self.results.push(receipt_and_events);
                }
                Err(error) => {
                    return Err(crate::error::TransactionExecutorError::new(tx_index, error).into());
                }
            }
        }

        self.total_executed += txns.len();

        Ok(batch_results)
    }

    /// Returns the total number of transactions executed so far.
    pub fn total_executed(&self) -> usize {
        self.total_executed
    }

    /// Returns all accumulated execution results.
    pub fn results(&self) -> &[ReceiptAndEvents] {
        &self.results
    }

    /// Closes the block and returns the final state diff.
    ///
    /// This commits only the first `n` transactions' state changes.
    /// Transactions after position `n` are discarded. This provides natural
    /// rollback support.
    ///
    /// `n` should be the number of transactions to include in the final block.
    /// If `n` equals `total_executed()`, all transactions are committed.
    /// If `n` is less than `total_executed()`, the later transactions are
    /// rolled back.
    ///
    /// After calling this method, the executor is consumed and cannot be used
    /// again. The Drop impl will not attempt to abort since the executor has
    /// been properly closed.
    pub fn close_block(&mut self, n: usize) -> anyhow::Result<StateDiff> {
        let mut executor = self.executor.take().context("executor already consumed")?;
        let summary = executor
            .close_block(n)
            .context("Failed to close concurrent block")?;

        // Convert the state diff from blockifier format
        let state_diff = convert_commitment_state_diff(
            summary.state_diff,
            &self.declared_deprecated_classes,
            &summary.compiled_class_hashes_for_migration,
        )?;

        // Truncate results and update count to match committed transactions
        self.results.truncate(n);
        self.total_executed = n;

        Ok(state_diff)
    }

    /// Aborts the block execution without committing any state changes.
    ///
    /// Use this when you need to abandon the current block entirely.
    /// After calling this, the executor is consumed.
    pub fn abort_block(&mut self) {
        if let Some(mut executor) = self.executor.take() {
            executor.abort_block();
        }
    }

    /// Returns true if the executor has been halted (block is full or deadline
    /// reached), or if the executor has already been consumed.
    pub fn is_done(&self) -> bool {
        self.executor.as_ref().map(|e| e.is_done()).unwrap_or(true)
    }
}

impl Drop for ConcurrentBlockExecutor {
    fn drop(&mut self) {
        // If the executor hasn't been consumed by close_block() or abort_block(),
        // we must abort it to halt the scheduler. Otherwise, worker threads will
        // remain blocked waiting for this executor's scheduler to signal completion,
        // which would cause deadlocks if the worker pool is reused for another block.
        if let Some(mut executor) = self.executor.take() {
            tracing::debug!(
                "ConcurrentBlockExecutor dropped without close_block/abort_block - aborting"
            );
            executor.abort_block();
        }
    }
}

/// Creates the state reader components needed for the concurrent executor.
fn create_state_reader_components<S: StorageAdapter + Clone>(
    execution_state: &ExecutionState,
    storage_adapter: S,
    block_info: &BlockInfo,
    versioned_constants_map: &VersionedConstantsMap,
) -> anyhow::Result<(
    BlockContext,
    PendingStateReader<PathfinderStateReader<S>>,
    Option<BlockHashAndNumber>,
)> {
    // Execute on parent state (N-1)
    let block_number = block_info.number.parent();

    let chain_info = execution_state.chain_info()?;
    let starknet_block_info = execution_state.starknet_block_info()?;

    // Get old block hash for pre_process_block
    let old_block_number_and_hash = if block_info.number.get() >= 10 {
        let block_number_whose_hash_becomes_available =
            pathfinder_common::BlockNumber::new_or_panic(block_info.number.get() - 10);

        let block_hash = storage_adapter
            .block_hash(block_number_whose_hash_becomes_available.into())?
            .context(format!(
                "Getting hash of historical block {block_number_whose_hash_becomes_available}"
            ))?;

        Some(BlockHashAndNumber {
            number: starknet_api::block::BlockNumber(
                block_number_whose_hash_becomes_available.get(),
            ),
            hash: starknet_api::block::BlockHash(block_hash.0.into_starkfelt()),
        })
    } else {
        None
    };

    let versioned_constants = versioned_constants_map.for_version(&block_info.starknet_version);

    let raw_reader = PathfinderStateReader::new(
        storage_adapter,
        block_number,
        false, // No pending state for concurrent executor
        None,  // No native class cache
        false, // No force native execution
    );
    let pending_state_reader = PendingStateReader::new(raw_reader, None);

    let block_context = BlockContext::new(
        starknet_block_info,
        chain_info,
        versioned_constants.clone(),
        BouncerConfig::max(),
    );

    Ok((
        block_context,
        pending_state_reader,
        old_block_number_and_hash,
    ))
}

/// Converts blockifier's CommitmentStateDiff to our StateDiff format.
fn convert_commitment_state_diff(
    commitment_diff: blockifier::state::cached_state::CommitmentStateDiff,
    deprecated_declared_classes: &[ClassHash],
    compiled_class_hashes_for_migration: &blockifier::blockifier::transaction_executor::CompiledClassHashesForMigration,
) -> anyhow::Result<StateDiff> {
    use std::collections::BTreeMap;

    use pathfinder_common::{CasmHash, ContractNonce, SierraHash, StorageAddress, StorageValue};

    use crate::felt::IntoFelt;
    use crate::types::{DeclaredSierraClass, DeployedContract, MigratedCompiledClass, StorageDiff};

    let mut deployed_contracts = Vec::new();

    // Process address to class hash mappings
    // In CommitmentStateDiff, these are all the contracts that changed their class
    // hash
    for (address, class_hash) in commitment_diff.address_to_class_hash {
        // For simplicity, we treat all entries as deployed contracts.
        // The close_block() mechanism ensures we get the correct state diff
        // as computed by blockifier's finalize.
        deployed_contracts.push(DeployedContract {
            address: ContractAddress::new_or_panic(address.0.key().into_felt()),
            class_hash: ClassHash(class_hash.0.into_felt()),
        });
    }

    // Process storage diffs
    let mut storage_diffs: BTreeMap<ContractAddress, Vec<StorageDiff>> = BTreeMap::new();
    for (address, storage_map) in commitment_diff.storage_updates {
        let addr = ContractAddress::new_or_panic(address.0.key().into_felt());
        let diffs: Vec<StorageDiff> = storage_map
            .into_iter()
            .map(|(key, value)| StorageDiff {
                key: StorageAddress::new_or_panic(key.0.key().into_felt()),
                value: StorageValue(value.into_felt()),
            })
            .collect();
        storage_diffs.insert(addr, diffs);
    }

    // Process nonces
    let nonces: BTreeMap<ContractAddress, ContractNonce> = commitment_diff
        .address_to_nonce
        .into_iter()
        .map(|(address, nonce)| {
            (
                ContractAddress::new_or_panic(address.0.key().into_felt()),
                ContractNonce(nonce.0.into_felt()),
            )
        })
        .collect();

    // Process declared classes
    let declared_classes: Vec<DeclaredSierraClass> = commitment_diff
        .class_hash_to_compiled_class_hash
        .into_iter()
        .map(|(class_hash, compiled_class_hash)| DeclaredSierraClass {
            class_hash: SierraHash(class_hash.0.into_felt()),
            compiled_class_hash: CasmHash(compiled_class_hash.0.into_felt()),
        })
        .collect();

    // Process migrated compiled classes from stateful compression
    // Each entry is (CompiledClassHash, CompiledClassHash) - v2 to v1 migration
    let migrated_compiled_classes: Vec<MigratedCompiledClass> = compiled_class_hashes_for_migration
        .iter()
        .map(
            |(compiled_class_hash_v2, compiled_class_hash_v1)| MigratedCompiledClass {
                // The first element is the sierra/v2 compiled class hash
                class_hash: SierraHash(compiled_class_hash_v2.0.into_felt()),
                // The second element is the casm/v1 compiled class hash
                compiled_class_hash: CasmHash(compiled_class_hash_v1.0.into_felt()),
            },
        )
        .collect();

    Ok(StateDiff {
        storage_diffs,
        deployed_contracts,
        deprecated_declared_classes: deprecated_declared_classes.iter().copied().collect(),
        declared_classes,
        nonces,
        replaced_classes: Vec::new(),
        migrated_compiled_classes,
    })
}

#[cfg(test)]
mod tests {
    // Integration tests would need a database setup
}
