use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Context;
use p2p::sync::client::conv::TryFromDto;
use p2p_proto::class::Cairo1Class;
use p2p_proto::common::Hash;
use p2p_proto::consensus::{ProposalInit, TransactionVariant as ConsensusVariant};
use p2p_proto::sync::transaction::{DeclareV3WithoutClass, TransactionVariant as SyncVariant};
use p2p_proto::transaction::DeclareV3WithClass;
use pathfinder_class_hash::compute_sierra_class_hash;
use pathfinder_class_hash::json::SierraContractDefinition;
use pathfinder_common::class_definition::{
    SelectorAndFunctionIndex,
    SerializedSierraDefinition,
    SierraEntryPoints,
};
use pathfinder_common::event::Event;
use pathfinder_common::receipt::Receipt;
use pathfinder_common::state_update::StateUpdateData;
use pathfinder_common::transaction::{Transaction, TransactionVariant};
use pathfinder_common::{
    class_definition,
    BlockId,
    BlockNumber,
    ChainId,
    ConsensusFinalizedBlockHeader,
    ConsensusFinalizedL2Block,
    DecidedBlocks,
    DeclaredClass,
    EntryPoint,
    L1DataAvailabilityMode,
    ProposalCommitment,
    SequencerAddress,
    SierraHash,
    StarknetVersion,
    TransactionHash,
};
use pathfinder_executor::types::{to_starknet_api_transaction, BlockInfoPriceConverter};
use pathfinder_executor::{
    ClassInfo,
    ConcurrentBlockExecutor,
    ConcurrentStateReader,
    IntoStarkFelt,
};
use pathfinder_rpc::context::{ETH_FEE_TOKEN_ADDRESS, STRK_FEE_TOKEN_ADDRESS};
use pathfinder_storage::{Storage, Transaction as DbTransaction};
use rayon::prelude::*;
use tracing::debug;

/// Type alias for the worker pool used by the concurrent executor.
pub type ValidatorWorkerPool = Arc<
    pathfinder_executor::blockifier_reexports::WorkerPool<
        pathfinder_executor::blockifier_reexports::CachedState<ConcurrentStateReader>,
    >,
>;

use pathfinder_block_commitments::{
    calculate_event_commitment,
    calculate_receipt_commitment,
    calculate_transaction_commitment,
};
use pathfinder_gas_price::{
    L1GasPriceProvider,
    L1GasPriceValidationResult,
    L1ToFriValidationResult,
    L1ToFriValidator,
    L2GasPriceProvider,
    L2GasPriceValidationResult,
};

pub mod error;

use crate::error::ProposalHandlingError;

/// TODO: Use this type as validation result.
pub enum ValidationResult {
    Valid,
    Invalid,
    Error(anyhow::Error),
}

/// Determines whether validation of the proposal should be deferred based on
/// the presence of the parent block in the decided blocks or DB.
pub fn should_defer_validation(
    height: u64,
    decided_blocks: DecidedBlocks,
    db_tx: &DbTransaction<'_>,
) -> Result<bool, ProposalHandlingError> {
    let Some(parent_height) = height.checked_sub(1) else {
        // Genesis block - no deferral needed.
        return Ok(false);
    };

    let is_parent_decided = {
        let decided_blocks = decided_blocks.read().unwrap();
        decided_blocks.contains_key(
            &BlockNumber::new(parent_height).context("Block number exceeds i64::MAX")?,
        )
    };

    let defer = if is_parent_decided {
        // The node observed parent block get decided - no deferral needed.
        false
    } else {
        // The node did not observe parent block get decided - either it has not been
        // decided on yet, or the node joined the network too late to observe it. Fall
        // back to checking the committed blocks in DB.
        let parent_block = BlockNumber::new(parent_height)
            .context("Block number is larger than i64::MAX")
            .map_err(ProposalHandlingError::Fatal)?;
        let parent_block = BlockId::Number(parent_block);
        let parent_committed = db_tx
            .block_exists(parent_block)
            .map_err(ProposalHandlingError::Fatal)?;
        !parent_committed
    };

    Ok(defer)
}

pub fn new(
    chain_id: ChainId,
    proposal_init: ProposalInit,
) -> Result<ValidatorBlockInfoStage, ProposalHandlingError> {
    ValidatorBlockInfoStage::new(chain_id, proposal_init)
}

/// Validates the basic block metadata and proposal information before any
/// transaction processing.
#[derive(Debug)]
pub struct ValidatorBlockInfoStage {
    chain_id: ChainId,
    proposal_init: ProposalInit,
}

impl ValidatorBlockInfoStage {
    pub fn new(
        chain_id: ChainId,
        proposal_init: ProposalInit,
    ) -> Result<ValidatorBlockInfoStage, ProposalHandlingError> {
        // TODO(validator) how can we validate the proposal init?
        let _proposal_height = BlockNumber::new(proposal_init.height)
            .context("ProposalInit height exceeds i64::MAX")
            .map_err(ProposalHandlingError::recoverable)?;
        Ok(ValidatorBlockInfoStage {
            chain_id,
            proposal_init,
        })
    }

    pub fn chain_id(&self) -> ChainId {
        self.chain_id
    }

    pub fn proposal_height(&self) -> u64 {
        self.proposal_init.height
    }

    /// Validate the block info against the parent block and L1 gas price data,
    /// then transition to the [ValidatorTransactionBatchStage].
    #[allow(clippy::too_many_arguments)]
    pub fn validate_block_info(
        self,
        main_storage: Storage,
        decided_blocks: DecidedBlocks,
        gas_price_provider: Option<L1GasPriceProvider>,
        l1_to_fri_validator: Option<&L1ToFriValidator>,
        l2_gas_price_provider: Option<&L2GasPriceProvider>,
        worker_pool: ValidatorWorkerPool,
        my_starknet_version: StarknetVersion,
    ) -> Result<ValidatorTransactionBatchStage, ProposalHandlingError> {
        let Self {
            chain_id,
            proposal_init,
        } = self;
        let ProposalInit {
            height,
            round: _,
            valid_round: _,
            proposer: _,
            timestamp,
            builder,
            l1_da_mode,
            l2_gas_price_fri,
            l1_gas_price_fri,
            l1_data_gas_price_fri,
            l1_gas_price_wei,
            l1_data_gas_price_wei,
            starknet_version,
            version_constant_commitment: _,
        } = proposal_init;
        let _span = tracing::debug_span!(
            "Validator::validate_block_info",
            %height,
            %timestamp,
            builder = %builder.0,
        )
        .entered();

        let starknet_version = validate_starknet_version(starknet_version, my_starknet_version)?;
        validate_block_info_timestamp(height, timestamp, &main_storage, decided_blocks.clone())?;

        // Validate L1 gas prices if a provider is available
        if let Some(ref provider) = gas_price_provider {
            validate_l1_gas_prices(timestamp, l1_gas_price_wei, l1_data_gas_price_wei, provider)?;
        }

        // Validate L1 gas prices in FRI terms
        if let Some(validator) = l1_to_fri_validator {
            validate_l1_to_fri_prices(
                timestamp,
                l1_gas_price_fri,
                l1_data_gas_price_fri,
                validator,
            )?;
        }

        if let Some(provider) = l2_gas_price_provider {
            validate_l2_gas_price(l2_gas_price_fri, provider)?;
        }

        let block_info = pathfinder_executor::types::BlockInfo::try_from_proposal(
            height,
            timestamp,
            SequencerAddress(builder.0),
            match l1_da_mode {
                p2p_proto::common::L1DataAvailabilityMode::Blob => L1DataAvailabilityMode::Blob,
                p2p_proto::common::L1DataAvailabilityMode::Calldata => {
                    L1DataAvailabilityMode::Calldata
                }
            },
            BlockInfoPriceConverter::consensus(
                l2_gas_price_fri,
                l1_gas_price_fri,
                l1_data_gas_price_fri,
                l1_gas_price_wei,
                l1_data_gas_price_wei,
            ),
            starknet_version,
        )
        .context("Creating internal BlockInfo representation")
        .map_err(ProposalHandlingError::recoverable)?;

        Ok(ValidatorTransactionBatchStage {
            chain_id,
            block_info,
            transactions: Vec::new(),
            receipts: Vec::new(),
            events: Vec::new(),
            executor: None,
            worker_pool,
            main_storage,
            declared_classes: Vec::new(),
            decided_blocks,
        })
    }

    /// Skip block info validation and transition directly to the
    /// [ValidatorTransactionBatchStage].
    ///
    /// Used only for testing and dummy proposal creation.
    pub fn skip_validation(
        self,
        main_storage: Storage,
        worker_pool: ValidatorWorkerPool,
        decided_blocks: DecidedBlocks,
    ) -> Result<ValidatorTransactionBatchStage, ProposalHandlingError> {
        let Self {
            chain_id,
            proposal_init,
        } = self;
        let ProposalInit {
            height,
            round: _,
            valid_round: _,
            proposer: _,
            timestamp,
            builder,
            l1_da_mode,
            l2_gas_price_fri,
            l1_gas_price_fri,
            l1_data_gas_price_fri,
            l1_gas_price_wei,
            l1_data_gas_price_wei,
            starknet_version,
            version_constant_commitment: _,
        } = proposal_init;
        let _span = tracing::debug_span!(
            "Validator::skip_block_info_validation",
            height = %height,
            timestamp = %timestamp,
            builder = %builder.0,
        )
        .entered();

        tracing::debug!("Skipping block info validation for height {}", height);

        let starknet_version: StarknetVersion = starknet_version
            .parse()
            .map_err(ProposalHandlingError::recoverable)?;

        let builder = SequencerAddress(builder.0);
        let l1_da_mode = match l1_da_mode {
            p2p_proto::common::L1DataAvailabilityMode::Blob => L1DataAvailabilityMode::Blob,
            p2p_proto::common::L1DataAvailabilityMode::Calldata => L1DataAvailabilityMode::Calldata,
        };
        let price_converter = BlockInfoPriceConverter::consensus(
            l2_gas_price_fri,
            l1_gas_price_fri,
            l1_data_gas_price_fri,
            l1_gas_price_wei,
            l1_data_gas_price_wei,
        );

        let block_info = pathfinder_executor::types::BlockInfo::try_from_proposal(
            height,
            timestamp,
            builder,
            l1_da_mode,
            price_converter,
            starknet_version,
        )
        .context("Creating internal BlockInfo representation")
        .map_err(ProposalHandlingError::recoverable)?;

        Ok(ValidatorTransactionBatchStage {
            chain_id,
            block_info,
            transactions: Vec::new(),
            receipts: Vec::new(),
            events: Vec::new(),
            executor: None,
            worker_pool,
            main_storage,
            declared_classes: Vec::new(),
            decided_blocks,
        })
    }
}

fn validate_starknet_version(
    proposal_starknet_version: String,
    my_starknet_version: StarknetVersion,
) -> Result<StarknetVersion, ProposalHandlingError> {
    let starknet_version: StarknetVersion = proposal_starknet_version
        .parse()
        .context("parsing starknet version")
        .map_err(ProposalHandlingError::recoverable)?;

    if starknet_version != my_starknet_version {
        let msg = format!(
            "Starknet version mismatch: proposed {}; expected {}",
            starknet_version, my_starknet_version
        );
        return Err(ProposalHandlingError::recoverable_msg(msg));
    }

    Ok(starknet_version)
}

fn validate_block_info_timestamp(
    height: u64,
    proposal_timestamp: u64,
    main_storage: &Storage,
    decided_blocks: DecidedBlocks,
) -> Result<(), ProposalHandlingError> {
    let Some(parent_height) = height.checked_sub(1) else {
        // Genesis block, no parent to validate against.
        return Ok(());
    };

    let decided_parent_timestamp = {
        let decided_blocks = decided_blocks.read().unwrap();
        decided_blocks
            .get(&BlockNumber::new(parent_height).context("Block number exceeds i64::MAX")?)
            .map(|decided_parent| decided_parent.block.header.timestamp)
    };

    let parent_timestamp = match decided_parent_timestamp {
        Some(ts) => ts,
        None => {
            let mut db_conn = main_storage
                .connection()
                .context("Creating database connection for timestamp validation")
                .map_err(ProposalHandlingError::fatal)?;
            let db_tx = db_conn
                .transaction()
                .context("Creating DB transaction for timestamp validation")
                .map_err(ProposalHandlingError::fatal)?;

            let block_num = BlockNumber::new_or_panic(parent_height);
            db_tx
                .block_header(BlockId::Number(block_num))
                .context("Fetching block header for timestamp validation")
                .map_err(ProposalHandlingError::fatal)?
                .map(|parent_header| parent_header.timestamp)
                .expect(
                    "BlockInfo validation should be deferred until parent block is decided or \
                     committed",
                )
        }
    };

    if proposal_timestamp <= parent_timestamp.get() {
        let msg = format!(
            "Proposal timestamp must be strictly greater than parent block timestamp: {} <= {}",
            proposal_timestamp, parent_timestamp
        );
        return Err(ProposalHandlingError::recoverable_msg(msg));
    }

    Ok(())
}

/// Validates L1 gas prices in the proposal.
///
/// Note: During cold start when the provider doesn't have enough data,
/// proposals are allowed with a warning.
fn validate_l1_gas_prices(
    proposal_timestamp: u64,
    l1_gas_price_wei: u128,
    l1_data_gas_price_wei: u128,
    provider: &L1GasPriceProvider,
) -> Result<(), ProposalHandlingError> {
    match provider.validate(proposal_timestamp, l1_gas_price_wei, l1_data_gas_price_wei) {
        L1GasPriceValidationResult::Valid => Ok(()),
        L1GasPriceValidationResult::Invalid(error) => {
            tracing::warn!(
                l1_gas_price_wei,
                l1_data_gas_price_wei,
                error = %error,
                "L1 gas price validation failed"
            );
            Err(ProposalHandlingError::recoverable_msg(format!(
                "L1 gas price validation failed: {error}"
            )))
        }
        L1GasPriceValidationResult::InsufficientData => {
            tracing::debug!(
                l1_gas_price_wei,
                l1_data_gas_price_wei,
                "L1 gas price validation skipped: insufficient data"
            );
            Ok(())
        }
    }
}

/// Validates L1 gas prices in FRI terms (Apollo style).
///
/// This validation converts both proposer's and validator's L1 gas prices to
/// FRI using their respective ETH/FRI conversion rates. The final FRI prices
/// are compared with a 10% tolerance margin.
///
/// Rate mismatches are logged as metrics but do not cause rejection, following
/// Apollo's approach that prioritizes liveness over strict determinism.
fn validate_l1_to_fri_prices(
    timestamp: u64,
    l1_gas_price_fri: u128,
    l1_data_gas_price_fri: u128,
    validator: &L1ToFriValidator,
) -> Result<(), ProposalHandlingError> {
    match validator.validate(timestamp, l1_gas_price_fri, l1_data_gas_price_fri) {
        L1ToFriValidationResult::Valid => Ok(()),
        L1ToFriValidationResult::InvalidFriDeviation {
            proposed_fri,
            expected_fri,
            deviation_pct,
        } => {
            tracing::warn!(
                proposed_fri,
                expected_fri,
                deviation_pct,
                "L1-to-FRI price validation failed: FRI price deviation too high"
            );
            Err(ProposalHandlingError::recoverable_msg(format!(
                "L1-to-FRI price deviation too high: {deviation_pct:.2}%"
            )))
        }
        L1ToFriValidationResult::InsufficientData => {
            tracing::debug!("L1-to-FRI validation skipped: insufficient data (cold start)");
            Ok(())
        }
    }
}

fn validate_l2_gas_price(
    proposed_l2_gas_price_fri: u128,
    provider: &L2GasPriceProvider,
) -> Result<(), ProposalHandlingError> {
    match provider.validate(proposed_l2_gas_price_fri) {
        L2GasPriceValidationResult::Valid => Ok(()),
        L2GasPriceValidationResult::Invalid { proposed, expected } => {
            tracing::warn!(proposed, expected, "L2 gas price validation failed");
            Err(ProposalHandlingError::recoverable_msg(format!(
                "L2 gas price mismatch: proposed {proposed}, expected {expected}"
            )))
        }
        L2GasPriceValidationResult::InsufficientData => {
            tracing::debug!(
                proposed_l2_gas_price_fri,
                "L2 gas price validation skipped: insufficient data"
            );
            Ok(())
        }
    }
}

/// Executes transactions and manages the block execution state.
///
/// Uses blockifier's ConcurrentBlockExecutor which provides natural
/// rollback support through `close_block(n)`.
pub struct ValidatorTransactionBatchStage {
    chain_id: ChainId,
    block_info: pathfinder_executor::types::BlockInfo,
    /// Accumulated executed transactions across batches
    transactions: Vec<Transaction>,
    /// Accumulated receipts corresponding to executed transactions
    receipts: Vec<Receipt>,
    /// Accumulated events corresponding to executed transactions
    events: Vec<Vec<Event>>,
    executor: Option<ConcurrentBlockExecutor>,
    worker_pool: ValidatorWorkerPool,
    /// Storage for creating new connections
    main_storage: Storage,
    /// Accumulated declared classes across batches. Only non-reverted
    /// declarations are included.
    declared_classes: Vec<DeclaredClass>,
    /// A view into the current decided blocks, used for initializing the
    /// executor's state reader.
    decided_blocks: DecidedBlocks,
}

impl ValidatorTransactionBatchStage {
    /// Get the current number of executed transactions.
    pub fn transaction_count(&self) -> usize {
        self.transactions.len()
    }

    /// Execute a batch of transactions using the concurrent executor.
    pub fn execute_batch<T: TransactionExt>(
        &mut self,
        transactions: Vec<p2p_proto::consensus::Transaction>,
        compiler_resource_limits: pathfinder_compiler::ResourceLimits,
        blockifier_libfuncs: pathfinder_compiler::BlockifierLibfuncs,
    ) -> Result<(), ProposalHandlingError> {
        if transactions.is_empty() {
            return Ok(());
        }

        let batch_size = transactions.len();

        tracing::debug!(
            "Executing batch with {} transactions (total so far: {})",
            batch_size,
            self.transactions.len()
        );

        // Convert transactions to executor format, use `par_iter` because any declare
        // transactions will require sierra compilation and casm hash computation. Both
        // are blocking.
        let txns = transactions
            .par_iter()
            .map(|t| {
                T::try_map_transaction(t.clone(), compiler_resource_limits, blockifier_libfuncs)
            })
            .collect::<anyhow::Result<Vec<_>>>()
            .map_err(ProposalHandlingError::recoverable)?;
        let mut common_txns = Vec::with_capacity(txns.len());
        let mut executor_txns = Vec::with_capacity(txns.len());
        let mut declared_classes = Vec::with_capacity(txns.len());

        txns.into_iter().for_each(|txn| {
            common_txns.push(txn.common);
            executor_txns.push(txn.executor);
            declared_classes.push(txn.class_definition);
        });

        // Verify transaction hashes
        let txn_hashes = common_txns
            .par_iter()
            .map(|t| {
                if T::verify_hash(t, self.chain_id) {
                    Ok(t.hash)
                } else {
                    Err(anyhow::anyhow!(
                        "Transaction hash mismatch, expected: {}",
                        t.hash
                    ))
                }
            })
            .collect::<anyhow::Result<Vec<_>>>()
            .context("Verifying transaction hashes")
            .map_err(ProposalHandlingError::recoverable)?;

        // Initialize executor on first batch
        if self.executor.is_none() {
            self.executor = Some(
                ConcurrentBlockExecutor::new(
                    self.chain_id,
                    self.block_info,
                    ETH_FEE_TOKEN_ADDRESS,
                    STRK_FEE_TOKEN_ADDRESS,
                    self.main_storage.connection().map_err(|e| {
                        ProposalHandlingError::fatal(
                            anyhow::Error::from(e).context("Creating database connection"),
                        )
                    })?,
                    self.decided_blocks.clone(),
                    self.worker_pool.clone(),
                    None, // No deadline
                )
                .map_err(ProposalHandlingError::fatal)?,
            );
        }

        // Execute the batch
        let executor = self
            .executor
            .as_mut()
            .context("Executor should be initialized")
            .map_err(ProposalHandlingError::fatal)?;

        let (receipts, events): (Vec<_>, Vec<_>) =
            executor.execute(executor_txns)?.into_iter().unzip();

        // Convert receipts to common format
        let base_transaction_index = self.transactions.len();
        let receipts: Vec<Receipt> = receipts
            .into_iter()
            .zip(txn_hashes)
            .enumerate()
            .map(|(batch_idx, (receipt, hash))| Receipt {
                transaction_hash: hash,
                actual_fee: receipt.actual_fee,
                execution_resources: receipt.execution_resources,
                l2_to_l1_messages: receipt.l2_to_l1_messages,
                execution_status: receipt.execution_status,
                transaction_index: pathfinder_common::TransactionIndex::new(
                    (base_transaction_index + batch_idx) as u64,
                )
                .expect("Transaction index should be valid"),
            })
            .collect();

        // Update accumulated state
        //
        // IMPORTANT
        // Filter out declarations which were not reverted and add only those to
        // the declared_classes list, which if the block is decide, goes to storage and
        // hence updates the state.
        receipts
            .iter()
            .zip(declared_classes)
            .for_each(|(receipt, class_def)| {
                if let Some(class_def) = class_def {
                    if !receipt.is_reverted() {
                        self.declared_classes.push(class_def);
                    }
                }
            });

        // Update accumulated state (continued)
        self.transactions.extend(common_txns);
        self.receipts.extend(receipts);
        self.events.extend(events);

        tracing::debug!(
            "Executed {} transactions, total: {}",
            batch_size,
            self.transactions.len()
        );

        Ok(())
    }

    /// Rollback to a specific transaction count.
    ///
    /// With the concurrent executor, actual state rollback happens at
    /// `close_block(n)`. This method just truncates the output vectors
    /// (transactions, receipts, events) to prepare for finalization.
    pub fn rollback_to_transaction<T: TransactionExt>(
        &mut self,
        target_count: usize,
    ) -> Result<(), ProposalHandlingError> {
        let current_count = self.transactions.len();

        if target_count > current_count {
            return Err(ProposalHandlingError::recoverable_msg(format!(
                "Target count {} exceeds executed transactions {}",
                target_count, current_count
            )));
        }

        if target_count == current_count {
            return Ok(());
        }

        tracing::debug!(
            "Rolling back from {} to {} transactions (state rollback at close_block)",
            current_count,
            target_count
        );

        // Truncate output vectors to match target count
        self.transactions.truncate(target_count);
        self.receipts.truncate(target_count);
        self.events.truncate(target_count);

        Ok(())
    }

    #[cfg(test)]
    /// Finalize with the current state (up to the last executed transaction)
    pub fn finalize(
        &mut self,
    ) -> Result<Option<pathfinder_executor::types::StateDiff>, ProposalHandlingError> {
        if self.executor.is_none() {
            return Ok(None);
        }

        // Take the executor and close the block
        let mut executor = self
            .executor
            .take()
            .context("Executor should exist")
            .map_err(ProposalHandlingError::fatal)?;

        // close_block(n) commits only the first n transactions' state changes
        let state_diff = executor
            .close_block(self.transactions.len())
            .map_err(ProposalHandlingError::Fatal)?;

        Ok(Some(state_diff))
    }

    /// Get the number of receipts
    pub fn receipt_count(&self) -> usize {
        self.receipts.len()
    }

    /// Get the number of events
    pub fn event_count(&self) -> usize {
        self.events.len()
    }

    /// Get a reference to the receipts (for testing)
    #[cfg(test)]
    pub fn receipts(&self) -> &[Receipt] {
        &self.receipts
    }

    /// Validate that the validator state is consistent
    pub fn validate_state_consistency(&self) -> Result<(), ProposalHandlingError> {
        // Validate that receipts and events match transaction count
        if self.receipts.len() != self.transactions.len() {
            return Err(ProposalHandlingError::recoverable_msg(format!(
                "State inconsistency: {} receipts but {} transactions",
                self.receipts.len(),
                self.transactions.len()
            )));
        }

        if self.events.len() != self.transactions.len() {
            return Err(ProposalHandlingError::recoverable_msg(format!(
                "State inconsistency: {} event arrays but {} transactions",
                self.events.len(),
                self.transactions.len()
            )));
        }
        Ok(())
    }

    /// Finalizes the block, producing a header with all commitments except
    /// the state commitment and block hash, which are computed in the sync task
    /// just before the block is committed into main storage. Also verifies that
    /// the computed proposal commitment matches the expected one.
    pub fn consensus_finalize(
        self,
        expected_proposal_commitment: ProposalCommitment,
    ) -> Result<ConsensusFinalizedL2Block, ProposalHandlingError> {
        let height = self.block_info.number;
        let block = self.consensus_finalize0()?;
        let actual_proposal_commitment = block.header.state_diff_commitment;

        // Skip commitment validation in tests when using dummy commitment (ZERO)
        // This allows e2e tests to focus on batch execution logic without commitment
        // complexity
        #[cfg(any(test, feature = "skip-commitment-validation"))]
        if expected_proposal_commitment.0.is_zero() {
            return Ok(block);
        }

        if actual_proposal_commitment.0 == expected_proposal_commitment.0 {
            Ok(block)
        } else {
            Err(ProposalHandlingError::recoverable_msg(format!(
                "proposal commitment mismatch at height {height}, expected \
                 {expected_proposal_commitment}, actual {actual_proposal_commitment}"
            )))
        }
    }

    /// Finalizes the block, producing a header with all commitments except
    /// the state commitment and block hash, which are computed in the last
    /// stage.
    pub fn consensus_finalize0(self) -> Result<ConsensusFinalizedL2Block, ProposalHandlingError> {
        let Self {
            block_info,
            executor,
            transactions,
            receipts,
            events,
            declared_classes,
            ..
        } = self;

        let _span = tracing::debug_span!(
            "ConcurrentValidator::consensus_finalize",
            height = %block_info.number,
            num_transactions = %transactions.len(),
        )
        .entered();

        let start = Instant::now();

        // For empty proposals (no transactions), we don't need an executor.
        // Use an empty state diff instead.
        let state_update = if executor.is_none() && transactions.is_empty() {
            StateUpdateData::default()
        } else {
            let mut executor = executor
                .context("Executor should exist for finalization")
                .map_err(ProposalHandlingError::fatal)?;

            // close_block(n) commits only the first n transactions' state changes.
            let state_diff = executor
                .close_block(transactions.len())
                .map_err(ProposalHandlingError::fatal)?;

            StateUpdateData::from(state_diff)
        };

        let transaction_commitment =
            calculate_transaction_commitment(&transactions, block_info.starknet_version)
                .map_err(ProposalHandlingError::fatal)?;
        let receipt_commitment =
            calculate_receipt_commitment(&receipts).map_err(ProposalHandlingError::fatal)?;
        let events_ref_by_txn = events
            .iter()
            .zip(transactions.iter().map(|t| t.hash))
            .map(|(e, h)| (h, e.as_slice()))
            .collect::<Vec<_>>();
        let event_commitment =
            calculate_event_commitment(&events_ref_by_txn, block_info.starknet_version)
                .map_err(ProposalHandlingError::fatal)?;
        let state_diff_commitment = state_update.compute_state_diff_commitment();
        let event_count = events.iter().map(|e| e.len()).sum();

        let l2_gas_consumed: u128 = receipts
            .iter()
            .map(|r| r.execution_resources.l2_gas.0)
            .sum();

        let header = ConsensusFinalizedBlockHeader {
            number: block_info.number,
            timestamp: block_info.timestamp,
            eth_l1_gas_price: block_info.eth_l1_gas_price,
            strk_l1_gas_price: block_info.strk_l1_gas_price,
            eth_l1_data_gas_price: block_info.eth_l1_data_gas_price,
            strk_l1_data_gas_price: block_info.strk_l1_data_gas_price,
            eth_l2_gas_price: block_info.eth_l2_gas_price,
            strk_l2_gas_price: block_info.strk_l2_gas_price,
            sequencer_address: block_info.sequencer_address,
            starknet_version: block_info.starknet_version,
            event_commitment,
            transaction_commitment,
            transaction_count: transactions.len(),
            event_count,
            l1_da_mode: self.block_info.l1_da_mode,
            receipt_commitment,
            state_diff_commitment,
            state_diff_length: state_update.state_diff_length(),
            l2_gas_consumed,
        };

        debug!(
            "Block {} finalized in {} ms",
            block_info.number,
            start.elapsed().as_millis()
        );

        Ok(ConsensusFinalizedL2Block {
            header,
            state_update,
            transactions_and_receipts: transactions.into_iter().zip(receipts).collect::<Vec<_>>(),
            events,
            declared_classes,
        })
    }
}

impl std::fmt::Debug for ValidatorTransactionBatchStage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ValidatorTransactionBatchStage")
            .field("chain_id", &self.chain_id)
            .field("block_info", &self.block_info)
            .field("transactions_len", &self.transactions.len())
            .field("receipts_len", &self.receipts.len())
            .field("events_len", &self.events.len())
            .field("executor_initialized", &self.executor.is_some())
            // Intentionally skipping decided blocks
            .finish()
    }
}

pub enum ValidatorStage {
    BlockInfo(ValidatorBlockInfoStage),
    TransactionBatch(ValidatorTransactionBatchStage),
}

/// Error indicating that a validator stage conversion failed because the stage
/// type was incorrect.
#[derive(Debug, thiserror::Error)]
#[error("Expected {expected} stage, got {actual}")]
pub struct WrongValidatorStageError {
    pub expected: &'static str,
    pub actual: &'static str,
}

impl ValidatorStage {
    pub fn try_into_block_info_stage(
        self,
    ) -> Result<ValidatorBlockInfoStage, WrongValidatorStageError> {
        match self {
            ValidatorStage::BlockInfo(stage) => Ok(stage),
            _ => Err(WrongValidatorStageError {
                expected: "block info",
                actual: self.variant_name(),
            }),
        }
    }

    pub fn try_into_transaction_batch_stage(
        self,
    ) -> Result<ValidatorTransactionBatchStage, WrongValidatorStageError> {
        match self {
            ValidatorStage::TransactionBatch(stage) => Ok(stage),
            _ => Err(WrongValidatorStageError {
                expected: "transaction batch",
                actual: self.variant_name(),
            }),
        }
    }

    fn variant_name(&self) -> &'static str {
        match self {
            ValidatorStage::BlockInfo(_) => "BlockInfo",
            ValidatorStage::TransactionBatch(_) => "TransactionBatch",
        }
    }
}

pub trait TransactionExt {
    /// Maps consensus transaction to a pair of:
    /// - common transaction, which is used for verifying the transaction hash
    /// - executor transaction, which is used for executing the transaction
    ///
    /// For certain transactions, there is a compilation step which can be
    /// resource-limited via `compiler_resource_limits` and verified using
    /// `blockifier_libfuncs`.
    fn try_map_transaction(
        transaction: p2p_proto::consensus::Transaction,
        compiler_resource_limits: pathfinder_compiler::ResourceLimits,
        blockifier_libfuncs: pathfinder_compiler::BlockifierLibfuncs,
    ) -> anyhow::Result<MappedTransaction>;

    fn verify_hash(transaction: &Transaction, chain_id: ChainId) -> bool;
}

pub struct MappedTransaction {
    pub common: pathfinder_common::transaction::Transaction,
    pub executor: pathfinder_executor::Transaction,
    pub class_definition: Option<DeclaredClass>,
}

pub struct ProdTransactionMapper;

impl TransactionExt for ProdTransactionMapper {
    fn try_map_transaction(
        transaction: p2p_proto::consensus::Transaction,
        compiler_resource_limits: pathfinder_compiler::ResourceLimits,
        blockifier_libfuncs: pathfinder_compiler::BlockifierLibfuncs,
    ) -> anyhow::Result<MappedTransaction> {
        let p2p_proto::consensus::Transaction {
            txn,
            transaction_hash,
        } = transaction;
        let (variant, class_info) = match txn {
            ConsensusVariant::DeclareV3(DeclareV3WithClass { common, class }) => {
                let mut entry_points_by_type = HashMap::new();
                class.entry_points.constructors.iter().for_each(|x| {
                    entry_points_by_type
                        .entry(pathfinder_common::class_definition::EntryPointType::Constructor)
                        .or_insert_with(Vec::new)
                        .push(SelectorAndFunctionIndex {
                            selector: EntryPoint(x.selector),
                            function_idx: x.index,
                        });
                });
                class.entry_points.externals.iter().for_each(|x| {
                    entry_points_by_type
                        .entry(pathfinder_common::class_definition::EntryPointType::External)
                        .or_insert_with(Vec::new)
                        .push(SelectorAndFunctionIndex {
                            selector: EntryPoint(x.selector),
                            function_idx: x.index,
                        });
                });
                class.entry_points.l1_handlers.iter().for_each(|x| {
                    entry_points_by_type
                        .entry(pathfinder_common::class_definition::EntryPointType::L1Handler)
                        .or_insert_with(Vec::new)
                        .push(SelectorAndFunctionIndex {
                            selector: EntryPoint(x.selector),
                            function_idx: x.index,
                        });
                });

                let def = SierraContractDefinition {
                    abi: class.abi.clone().into(),
                    sierra_program: class.program.clone(),
                    contract_class_version: class.contract_class_version.clone().into(),
                    entry_points_by_type,
                };
                let class_hash = compute_sierra_class_hash(def)?;

                (
                    SyncVariant::DeclareV3(DeclareV3WithoutClass {
                        common,
                        class_hash: Hash(class_hash.0),
                    }),
                    Some(class_info(
                        SierraHash(class_hash.0),
                        class,
                        compiler_resource_limits,
                        blockifier_libfuncs,
                    )?),
                )
            }
            ConsensusVariant::DeployAccountV3(v) => (SyncVariant::DeployAccountV3(v), None),
            ConsensusVariant::InvokeV3(v) => (SyncVariant::InvokeV3(v.invoke), None),
            ConsensusVariant::L1HandlerV0(v) => (SyncVariant::L1HandlerV0(v), None),
        };
        let (class_info, for_storage) = class_info.unzip();

        let common_txn_variant = TransactionVariant::try_from_dto(variant)?;

        let deployed_address = deployed_address(&common_txn_variant);

        // TODO(validator) why 10^12?
        let paid_fee_on_l1 = match &common_txn_variant {
            TransactionVariant::L1Handler(_) => {
                Some(starknet_api::transaction::fields::Fee(1_000_000_000_000))
            }
            _ => None,
        };

        let api_txn = to_starknet_api_transaction(common_txn_variant.clone())?;
        let tx_hash =
            starknet_api::transaction::TransactionHash(transaction_hash.0.into_starkfelt());
        let executor_txn = pathfinder_executor::Transaction::from_api(
            api_txn,
            tx_hash,
            class_info,
            paid_fee_on_l1,
            deployed_address,
            pathfinder_executor::AccountTransactionExecutionFlags::default(),
        )?;
        let common_txn = pathfinder_common::transaction::Transaction {
            hash: TransactionHash(transaction_hash.0),
            variant: common_txn_variant,
        };

        Ok(MappedTransaction {
            common: common_txn,
            executor: executor_txn,
            class_definition: for_storage,
        })
    }

    fn verify_hash(transaction: &Transaction, chain_id: ChainId) -> bool {
        transaction.verify_hash(chain_id)
    }
}

fn class_info(
    sierra_hash: SierraHash,
    class: Cairo1Class,
    compiler_resource_limits: pathfinder_compiler::ResourceLimits,
    blockifier_libfuncs: pathfinder_compiler::BlockifierLibfuncs,
) -> anyhow::Result<(ClassInfo, DeclaredClass)> {
    let Cairo1Class {
        abi,
        entry_points,
        program,
        contract_class_version,
    } = class;

    let abi_length = abi.len();
    let sierra_program_length = program.len();
    let sierra_version =
        starknet_api::contract_class::SierraVersion::from_str(&contract_class_version)
            .context("Getting sierra version")?;

    let definition = class_definition::Sierra {
        abi: abi.into(),
        sierra_program: program,
        contract_class_version: contract_class_version.into(),
        entry_points_by_type: SierraEntryPoints {
            constructor: entry_points
                .constructors
                .into_iter()
                .map(|x| SelectorAndFunctionIndex {
                    selector: EntryPoint(x.selector),
                    function_idx: x.index,
                })
                .collect(),
            external: entry_points
                .externals
                .into_iter()
                .map(|x| SelectorAndFunctionIndex {
                    selector: EntryPoint(x.selector),
                    function_idx: x.index,
                })
                .collect(),
            l1_handler: entry_points
                .l1_handlers
                .into_iter()
                .map(|x| SelectorAndFunctionIndex {
                    selector: EntryPoint(x.selector),
                    function_idx: x.index,
                })
                .collect(),
        },
    };
    let sierra_def = SerializedSierraDefinition::from_vec(serde_json::to_vec(&definition)?);
    let casm_def = pathfinder_compiler::compile_sierra_to_casm(
        &sierra_def,
        compiler_resource_limits,
        blockifier_libfuncs,
    )?;
    let casm_hash_v2 = pathfinder_compiler::casm_class_hash_v2(&casm_def)?;

    let for_storage = DeclaredClass {
        sierra_hash,
        casm_hash_v2,
        sierra_def,
        casm_def: casm_def.clone(),
    };

    let casm_contract_definition =
        pathfinder_executor::parse_casm_definition(casm_def, sierra_version.clone())
            .context("Parsing CASM contract definition")?;
    let ci = ClassInfo::new(
        &casm_contract_definition,
        sierra_program_length,
        abi_length,
        sierra_version,
    )?;
    Ok((ci, for_storage))
}

pub fn deployed_address(txnv: &TransactionVariant) -> Option<starknet_api::core::ContractAddress> {
    match txnv {
        TransactionVariant::DeployAccountV3(t) => Some(starknet_api::core::ContractAddress(
            starknet_api::core::PatriciaKey::try_from(t.contract_address.get().into_starkfelt())
                .expect("No contract address overflow expected"),
        )),
        TransactionVariant::DeclareV3(_)
        | TransactionVariant::InvokeV3(_)
        | TransactionVariant::L1Handler(_) => None,
        TransactionVariant::DeclareV0(_)
        | TransactionVariant::DeclareV1(_)
        | TransactionVariant::DeclareV2(_)
        | TransactionVariant::DeployV0(_)
        | TransactionVariant::DeployV1(_)
        | TransactionVariant::DeployAccountV1(_)
        | TransactionVariant::InvokeV0(_)
        | TransactionVariant::InvokeV1(_) => {
            unreachable!("Proposal parts don't carry older transaction versions: {txnv:?}")
        }
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use p2p::consensus::HeightAndRound;
    use p2p_proto::consensus::TransactionVariant;
    use p2p_proto::transaction::L1HandlerV0;
    use pathfinder_common::{
        block_hash_bytes,
        BlockHash,
        BlockHeader,
        BlockNumber,
        BlockTimestamp,
        ChainId,
    };
    use pathfinder_crypto::Felt;
    use pathfinder_executor::ExecutorWorkerPool;
    use pathfinder_storage::StorageBuilder;
    use rstest::rstest;

    use super::*;
    use crate::error::ProposalError;

    /// Creates a worker pool for tests.
    fn create_test_worker_pool() -> ValidatorWorkerPool {
        ExecutorWorkerPool::<ConcurrentStateReader>::new(1).get()
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

    fn create_test_transaction(index: usize) -> p2p_proto::consensus::Transaction {
        let txn = TransactionVariant::L1HandlerV0(L1HandlerV0 {
            nonce: Felt::from_hex_str(&format!("0x{index}")).unwrap(),
            address: p2p_proto::common::Address(
                Felt::from_hex_str(&format!("0x{index:x}")).unwrap(),
            ),
            entry_point_selector: Felt::from_hex_str(&format!("0x{index}")).unwrap(),
            calldata: vec![Felt::from_hex_str(&format!("0x{index}")).unwrap()],
        });

        let l1_handler = pathfinder_common::transaction::L1HandlerTransaction {
            nonce: pathfinder_common::TransactionNonce(
                Felt::from_hex_str(&format!("0x{index}")).unwrap(),
            ),
            contract_address: pathfinder_common::ContractAddress::new_or_panic(
                Felt::from_hex_str(&format!("0x{index:x}")).unwrap(),
            ),
            entry_point_selector: pathfinder_common::EntryPoint(
                Felt::from_hex_str(&format!("0x{index}")).unwrap(),
            ),
            calldata: vec![pathfinder_common::CallParam(
                Felt::from_hex_str(&format!("0x{index}")).unwrap(),
            )],
        };

        let chain_id = ChainId::SEPOLIA_TESTNET;
        let hash = l1_handler.calculate_hash(chain_id);

        p2p_proto::consensus::Transaction {
            transaction_hash: p2p_proto::common::Hash(hash.0),
            txn,
        }
    }

    /// Tests that concurrent executor batch execution works.
    ///
    /// Note: Unlike the old checkpoint-based implementation, the concurrent
    /// executor does not support re-execution after rollback. Rollback is
    /// "logical" and only affects finalization via close_block(n).
    #[test]
    fn test_concurrent_executor_batch_execution() {
        let storage = StorageBuilder::in_tempdir().expect("Failed to create temp database");
        let chain_id = ChainId::SEPOLIA_TESTNET;
        let worker_pool = create_test_worker_pool();
        let proposal_init = create_test_proposal(1);

        let mut validator_stage = ValidatorBlockInfoStage::new(chain_id, proposal_init)
            .and_then(|validator| {
                validator.skip_validation(storage, worker_pool, DecidedBlocks::default())
            })
            .expect("Failed to create validator stage");

        // Create batches: 3 batches with 2 transactions each
        let batches = [
            vec![create_test_transaction(0), create_test_transaction(1)],
            vec![create_test_transaction(2), create_test_transaction(3)],
            vec![create_test_transaction(4), create_test_transaction(5)],
        ];

        // Execute batch 1
        validator_stage
            .execute_batch::<ProdTransactionMapper>(
                batches[0].clone(),
                pathfinder_compiler::ResourceLimits::for_test(),
                pathfinder_compiler::BlockifierLibfuncs::default(),
            )
            .expect("Failed to execute batch 1");
        assert_eq!(
            validator_stage.transaction_count(),
            2,
            "Should have 2 transactions"
        );

        // Execute batch 2
        validator_stage
            .execute_batch::<ProdTransactionMapper>(
                batches[1].clone(),
                pathfinder_compiler::ResourceLimits::for_test(),
                pathfinder_compiler::BlockifierLibfuncs::default(),
            )
            .expect("Failed to execute batch 2");
        assert_eq!(
            validator_stage.transaction_count(),
            4,
            "Should have 4 transactions"
        );

        // Execute batch 3
        validator_stage
            .execute_batch::<ProdTransactionMapper>(
                batches[2].clone(),
                pathfinder_compiler::ResourceLimits::for_test(),
                pathfinder_compiler::BlockifierLibfuncs::default(),
            )
            .expect("Failed to execute batch 3");
        assert_eq!(
            validator_stage.transaction_count(),
            6,
            "Should have 6 transactions"
        );

        // Receipts should be consistent
        let receipts = validator_stage.receipts();
        assert_eq!(receipts.len(), 6, "Should have 6 receipts");

        // Verify transaction indices are sequential
        for (i, receipt) in receipts.iter().enumerate() {
            assert_eq!(
                receipt.transaction_index.get(),
                i as u64,
                "Transaction index mismatch at position {i}"
            );
        }

        // Finalize should work with concurrent executor
        // Note: State diffs may be empty for L1Handler transactions, which is fine
        let _state_diff = validator_stage
            .finalize()
            .expect("Failed to finalize")
            .expect("Should have state diff");
    }

    /// Tests rollback_to_transaction edge cases.
    ///
    /// Note: With the concurrent executor, rollback is "logical" - the actual
    /// state rollback happens at close_block(n). This test verifies that the
    /// tracking vectors (transactions, receipts, events) are truncated
    /// correctly.
    ///
    /// Key semantic: rollback_to_transaction(N) keeps exactly N transactions
    /// (the first N, i.e., indices 0..N).
    #[test]
    fn test_rollback_edge_cases() {
        let storage = StorageBuilder::in_tempdir().expect("Failed to create temp database");
        let chain_id = ChainId::SEPOLIA_TESTNET;
        let worker_pool = create_test_worker_pool();
        let proposal_init = create_test_proposal(1);

        let mut validator_stage = ValidatorBlockInfoStage::new(chain_id, proposal_init)
            .and_then(|validator| {
                validator.skip_validation(storage, worker_pool, DecidedBlocks::default())
            })
            .expect("Failed to create validator stage");

        // Create batches with different sizes to test boundary conditions
        // Batch 0: 3 transactions (indices 0, 1, 2)
        // Batch 1: 2 transactions (indices 3, 4)
        // Batch 2: 2 transactions (indices 5, 6)
        let batches = [
            vec![
                create_test_transaction(0),
                create_test_transaction(1),
                create_test_transaction(2),
            ],
            vec![create_test_transaction(3), create_test_transaction(4)],
            vec![create_test_transaction(5), create_test_transaction(6)],
        ];

        // Execute all batches
        validator_stage
            .execute_batch::<ProdTransactionMapper>(
                batches[0].clone(),
                pathfinder_compiler::ResourceLimits::for_test(),
                pathfinder_compiler::BlockifierLibfuncs::default(),
            )
            .expect("Failed to execute batch 0");
        validator_stage
            .execute_batch::<ProdTransactionMapper>(
                batches[1].clone(),
                pathfinder_compiler::ResourceLimits::for_test(),
                pathfinder_compiler::BlockifierLibfuncs::default(),
            )
            .expect("Failed to execute batch 1");
        validator_stage
            .execute_batch::<ProdTransactionMapper>(
                batches[2].clone(),
                pathfinder_compiler::ResourceLimits::for_test(),
                pathfinder_compiler::BlockifierLibfuncs::default(),
            )
            .expect("Failed to execute batch 2");

        assert_eq!(validator_stage.transaction_count(), 7);
        assert_eq!(validator_stage.receipt_count(), 7);
        assert_eq!(validator_stage.event_count(), 7);

        // Rollback to current count (no-op)
        validator_stage
            .rollback_to_transaction::<ProdTransactionMapper>(7)
            .expect("Rollback to current count should succeed");
        assert_eq!(
            validator_stage.transaction_count(),
            7,
            "No-op rollback should not change count"
        );

        // Rollback to batch boundary (keep first 5 = end of batch 1)
        validator_stage
            .rollback_to_transaction::<ProdTransactionMapper>(5)
            .expect("Failed to rollback to 5");
        assert_eq!(validator_stage.transaction_count(), 5);
        assert_eq!(validator_stage.receipt_count(), 5);
        assert_eq!(validator_stage.event_count(), 5);

        // Rollback to batch boundary (keep first 3 = end of batch 0)
        validator_stage
            .rollback_to_transaction::<ProdTransactionMapper>(3)
            .expect("Failed to rollback to 3");
        assert_eq!(validator_stage.transaction_count(), 3);
        assert_eq!(validator_stage.receipt_count(), 3);
        assert_eq!(validator_stage.event_count(), 3);

        // Rollback to middle of what remains (keep first 2)
        validator_stage
            .rollback_to_transaction::<ProdTransactionMapper>(2)
            .expect("Failed to rollback to 2");
        assert_eq!(validator_stage.transaction_count(), 2);
        assert_eq!(validator_stage.receipt_count(), 2);
        assert_eq!(validator_stage.event_count(), 2);

        // Rollback to keep only 1 transaction
        validator_stage
            .rollback_to_transaction::<ProdTransactionMapper>(1)
            .expect("Failed to rollback to 1");
        assert_eq!(validator_stage.transaction_count(), 1);
        assert_eq!(validator_stage.receipt_count(), 1);
        assert_eq!(validator_stage.event_count(), 1);

        // Rollback to 0 (empty)
        validator_stage
            .rollback_to_transaction::<ProdTransactionMapper>(0)
            .expect("Failed to rollback to 0");
        assert_eq!(validator_stage.transaction_count(), 0);
        assert_eq!(validator_stage.receipt_count(), 0);
        assert_eq!(validator_stage.event_count(), 0);

        // Out of bounds rollback should error
        let result = validator_stage.rollback_to_transaction::<ProdTransactionMapper>(10);
        assert!(
            result.is_err(),
            "Rollback beyond current count should error"
        );
    }

    /// Tests that empty proposals (no transactions, no executor) can be
    /// finalized.
    ///
    /// This test covers the case where a proposal has no transactions and
    /// therefore no executor is created. The finalization should succeed with
    /// an empty state diff.
    #[test]
    fn test_empty_proposal_finalization() {
        let main_storage = StorageBuilder::in_tempdir().expect("Failed to create temp database");
        let chain_id = ChainId::SEPOLIA_TESTNET;
        let worker_pool = create_test_worker_pool();

        let hnr = HeightAndRound::new(0, 0);

        // Create a proposal init for height 0
        let proposal_init = p2p_proto::consensus::ProposalInit {
            height: hnr.height(),
            round: hnr.round(),
            valid_round: None,
            proposer: p2p_proto::common::Address(Felt::from_hex_str("0x1").unwrap()),
            timestamp: 1000,
            builder: p2p_proto::common::Address(Felt::from_hex_str("0x1").unwrap()),
            l1_da_mode: p2p_proto::common::L1DataAvailabilityMode::Calldata,
            l2_gas_price_fri: 1,
            l1_gas_price_fri: 1_000_000_000,
            l1_data_gas_price_fri: 1,
            l1_gas_price_wei: 1_000_000_000,
            l1_data_gas_price_wei: 1,
            starknet_version: StarknetVersion::V_0_14_0.to_string(),
            version_constant_commitment: Default::default(),
        };

        // Create validator stages (empty proposal path)
        let validator_block_info = ValidatorBlockInfoStage::new(chain_id, proposal_init)
            .expect("Failed to create ValidatorBlockInfoStage");

        let validator_transaction_batch = validator_block_info
            .validate_block_info(
                main_storage.clone(),
                DecidedBlocks::default(),
                None,
                None,
                None,
                worker_pool,
                StarknetVersion::V_0_14_0,
            )
            .expect("Failed to validate block info");

        // Verify the validator is in the expected empty state
        assert_eq!(
            validator_transaction_batch.transaction_count(),
            0,
            "Empty proposal should have 0 transactions"
        );
        assert!(
            validator_transaction_batch.executor.is_none(),
            "Empty proposal should have no executor"
        );

        // Finalize the empty proposal - this should succeed without an executor
        let validator_finalize = validator_transaction_batch
            .consensus_finalize0()
            .expect("Empty proposal finalization should succeed");

        // Verify the finalized header has correct empty commitments
        assert_eq!(
            validator_finalize.header.transaction_count, 0,
            "Empty proposal should have 0 transaction count"
        );
        assert_eq!(
            validator_finalize.header.event_count, 0,
            "Empty proposal should have 0 event count"
        );
        assert_eq!(
            validator_finalize.state_update.contract_updates.len(),
            0,
            "Empty proposal should have no contract updates"
        );
        assert_eq!(
            validator_finalize
                .state_update
                .system_contract_updates
                .len(),
            0,
            "Empty proposal should have no system contract updates"
        );
        assert_eq!(
            validator_finalize.state_update.declared_cairo_classes.len(),
            0,
            "Empty proposal should have no declared Cairo classes"
        );
        assert_eq!(
            validator_finalize
                .state_update
                .declared_sierra_classes
                .len(),
            0,
            "Empty proposal should have no declared Sierra classes"
        );
    }

    #[rstest]
    #[case::later_than_parent(2000, None)]
    #[case::equal_to_parent(
        1000,
        Some(String::from(
            "Proposal timestamp must be strictly greater than parent block timestamp: 1000 <= 1000"
        ))
    )]
    #[case::earlier_than_parent(
        700,
        Some(String::from(
            "Proposal timestamp must be strictly greater than parent block timestamp: 700 <= 1000"
        ))
    )]
    fn timestamp_validation_parent_block_found(
        #[case] proposal_timestamp: u64,
        #[case] expected_error_message: Option<String>,
    ) {
        let storage = StorageBuilder::in_tempdir().expect("Failed to create temp database");
        let worker_pool = create_test_worker_pool();
        let mut db_conn = storage.connection().expect("Failed to get DB connection");
        let db_tx = db_conn
            .transaction()
            .expect("Failed to begin DB transaction");

        // Insert parent header.
        let header0 = BlockHeader {
            hash: block_hash_bytes!(b"block hash 0"),
            parent_hash: BlockHash::default(),
            number: BlockNumber::new_or_panic(0),
            timestamp: BlockTimestamp::new_or_panic(1000),
            ..Default::default()
        };
        db_tx
            .insert_block_header(&header0)
            .expect("Failed to insert block header 0");
        db_tx.commit().expect("Failed to commit DB transaction");

        let chain_id = ChainId::SEPOLIA_TESTNET;
        let proposal_init1 = p2p_proto::consensus::ProposalInit {
            height: 1,
            round: 0,
            valid_round: None,
            proposer: p2p_proto::common::Address(Felt::from_hex_str("0x1").unwrap()),
            timestamp: proposal_timestamp,
            builder: p2p_proto::common::Address(Felt::from_hex_str("0x1").unwrap()),
            l1_da_mode: p2p_proto::common::L1DataAvailabilityMode::Calldata,
            l2_gas_price_fri: 1,
            l1_gas_price_fri: 1_000_000_000,
            l1_data_gas_price_fri: 1,
            l1_gas_price_wei: 1_000_000_000,
            l1_data_gas_price_wei: 1,
            starknet_version: StarknetVersion::V_0_14_0.to_string(),
            version_constant_commitment: Default::default(),
        };

        let validator_block_info1 = ValidatorBlockInfoStage::new(chain_id, proposal_init1)
            .expect("Failed to create ValidatorBlockInfoStage");

        let result = validator_block_info1.validate_block_info(
            storage,
            DecidedBlocks::default(),
            None,
            None,
            None,
            worker_pool,
            StarknetVersion::V_0_14_0,
        );

        if let Some(expected_error_message) = expected_error_message {
            let err = result.unwrap_err();
            assert_matches!(
                err,
                ProposalHandlingError::Recoverable(
                    ProposalError::ValidationFailed { message }
                ) if message == expected_error_message,
                "Proposal validation error did not match expected value",
            );
        } else {
            assert!(result.is_ok());
        }
    }

    #[rstest]
    #[case::genesis_parent(BlockNumber::GENESIS)]
    #[should_panic(
        expected = "BlockInfo validation should be deferred until parent block is decided or \
                    committed"
    )]
    #[case::non_genesis_parent(BlockNumber::new_or_panic(42))]
    fn timestamp_validation_parent_block_not_found(#[case] proposal_height: BlockNumber) {
        let storage = StorageBuilder::in_tempdir().expect("Failed to create temp database");
        let chain_id = ChainId::SEPOLIA_TESTNET;
        let worker_pool = create_test_worker_pool();

        let proposal_init = p2p_proto::consensus::ProposalInit {
            height: proposal_height.get(),
            round: 0,
            valid_round: None,
            proposer: p2p_proto::common::Address(Felt::from_hex_str("0x1").unwrap()),
            timestamp: 1000,
            builder: p2p_proto::common::Address(Felt::from_hex_str("0x1").unwrap()),
            l1_da_mode: p2p_proto::common::L1DataAvailabilityMode::Calldata,
            l2_gas_price_fri: 1,
            l1_gas_price_fri: 1_000_000_000,
            l1_data_gas_price_fri: 1,
            l1_gas_price_wei: 1_000_000_000,
            l1_data_gas_price_wei: 1,
            starknet_version: StarknetVersion::V_0_14_0.to_string(),
            version_constant_commitment: Default::default(),
        };

        let validator_block_info = ValidatorBlockInfoStage::new(chain_id, proposal_init)
            .expect("Failed to create ValidatorBlockInfoStage");

        if proposal_height == BlockNumber::GENESIS {
            // Genesis block should pass timestamp validation even though it does not have a
            // parent.
            assert!(
                validator_block_info
                    .validate_block_info(
                        storage,
                        DecidedBlocks::default(),
                        None,
                        None,
                        None,
                        worker_pool,
                        StarknetVersion::V_0_14_0,
                    )
                    .is_ok(),
                "Genesis block timestamp validation should pass even without parent"
            );
        } else {
            let err = validator_block_info
                .validate_block_info(
                    storage,
                    DecidedBlocks::default(),
                    None,
                    None,
                    None,
                    worker_pool,
                    StarknetVersion::V_0_14_0,
                )
                .unwrap_err();
            let expected_err_message = format!(
                "Parent block header not found for height {}",
                proposal_height.get() - 1,
            );
            assert_matches!(
                err,
                ProposalHandlingError::Recoverable(
                    ProposalError::ValidationFailed { message }
                ) if message == expected_err_message,
                "Timestamp validation without parent should fail",
            );
        }
    }
}
