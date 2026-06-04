use std::num::{NonZeroU32, NonZeroUsize};

use anyhow::Context;
use pathfinder_common::receipt::Receipt;
use pathfinder_common::transaction::Transaction;
use pathfinder_common::{BlockHeader, BlockId, BlockNumber, ChainId};
use pathfinder_executor::{ExecutionState, NativeClassCache, TryIntoStarkFee as _};
use pathfinder_rpc::context::{ETH_FEE_TOKEN_ADDRESS, STRK_FEE_TOKEN_ADDRESS};
use pathfinder_storage::Storage;
use rayon::prelude::*;
use util::percentage::Percentage;

// The Cairo VM allocates felts on the stack, so during execution it's making
// a huge number of allocations. We get roughly two times better execution
// performance by using jemalloc (compared to the Linux glibc allocator).
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// Re-execute transactions in a range of blocks.
///
/// Iterates over specified blocks in the database and re-executes all
/// transactions within those blocks
///
/// Usage:
/// `cargo run --release -p pathfinder --example re_execute ./mainnet.sqlite
/// 50000 51000`
fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
        .compact()
        .init();

    let n_cpus = rayon::current_num_threads();

    let database_path = std::env::args().nth(1).unwrap();
    let storage = pathfinder_storage::StorageBuilder::file(database_path.into())
        .migrate()?
        .create_pool(NonZeroU32::new(n_cpus as u32 * 2).unwrap())?;
    let mut db = storage
        .connection()
        .context("Opening database connection")?;

    let first_block = std::env::args().nth(2).unwrap();
    let first_block: u64 = first_block.parse().unwrap();

    let (latest_block, chain_id) = {
        let tx = db.transaction().unwrap();
        let (latest_block, _) = tx.block_id(BlockId::Latest)?.unwrap();
        let latest_block = latest_block.get();
        let chain_id = get_chain_id(&tx).unwrap();
        (latest_block, chain_id)
    };

    let last_block = std::env::args()
        .nth(3)
        .map(|s| str::parse(&s).unwrap())
        .unwrap_or(latest_block);

    tracing::info!(%first_block, %last_block, "Re-executing blocks");

    let start_time = std::time::Instant::now();
    let mut num_transactions: usize = 0;

    let native_class_cache = NativeClassCache::spawn(NonZeroUsize::new(512).unwrap(), 2);

    (first_block..=last_block)
        .map(|block_number| {
            let transaction = db.transaction().unwrap();
            let block_id = BlockId::Number(BlockNumber::new_or_panic(block_number));
            let block_header = transaction.block_header(block_id).unwrap().unwrap();
            let transactions_and_receipts = transaction
                .transaction_data_for_block(block_id)
                .unwrap()
                .context("Getting transactions for block")
                .unwrap();
            drop(transaction);

            let (transactions, receipts): (Vec<_>, Vec<_>) = transactions_and_receipts
                .into_iter()
                .map(|(tx, rx, _ev)| (tx, rx))
                .unzip();

            num_transactions += transactions.len();

            Work {
                header: block_header,
                transactions,
                receipts,
            }
        })
        .par_bridge()
        .for_each_with(
            (storage, native_class_cache),
            |(storage, native_class_cache), block| {
                execute(storage, chain_id, block, native_class_cache.clone())
            },
        );

    let elapsed = start_time.elapsed();

    tracing::info!(%num_transactions, ?elapsed, "Finished");

    Ok(())
}

fn get_chain_id(tx: &pathfinder_storage::Transaction<'_>) -> anyhow::Result<ChainId> {
    use pathfinder_common::consts::{
        MAINNET_GENESIS_HASH,
        SEPOLIA_INTEGRATION_GENESIS_HASH,
        SEPOLIA_TESTNET_GENESIS_HASH,
    };

    let (_, genesis_hash) = tx
        .block_id(BlockNumber::GENESIS.into())
        .unwrap()
        .context("Getting genesis hash")?;

    let chain = match genesis_hash {
        MAINNET_GENESIS_HASH => ChainId::MAINNET,
        SEPOLIA_TESTNET_GENESIS_HASH => ChainId::SEPOLIA_TESTNET,
        SEPOLIA_INTEGRATION_GENESIS_HASH => ChainId::SEPOLIA_INTEGRATION,
        _ => anyhow::bail!("Unknown chain"),
    };

    Ok(chain)
}

#[derive(Debug)]
struct Work {
    header: BlockHeader,
    transactions: Vec<Transaction>,
    receipts: Vec<Receipt>,
}

fn execute(
    storage: &mut Storage,
    chain_id: ChainId,
    work: Work,
    native_class_cache: NativeClassCache,
) {
    let start_time = std::time::Instant::now();
    let num_transactions = work.transactions.len();

    let mut db_conn = storage.connection().unwrap();

    let db_tx = db_conn.transaction().expect("Create transaction");

    let execution_state = ExecutionState::trace(
        chain_id,
        work.header.clone(),
        None,
        Default::default(),
        ETH_FEE_TOKEN_ADDRESS,
        STRK_FEE_TOKEN_ADDRESS,
        Some(native_class_cache),
        false,
    );

    let transactions = work
        .transactions
        .iter()
        .map(|tx| pathfinder_rpc::compose_executor_transaction(tx, &db_tx))
        .collect::<Result<Vec<_>, _>>();

    let transactions = match transactions {
        Ok(transactions) => transactions,
        Err(error) => {
            tracing::error!(block_number=%work.header.number, %error, "Transaction conversion failed");
            return;
        }
    };

    match pathfinder_executor::simulate(
        db_tx,
        execution_state,
        transactions,
        Percentage::new(0),
        false,
    ) {
        Ok(simulations) => {
            for (simulation, (receipt, transaction)) in simulations
                .0
                .iter()
                .zip(work.receipts.iter().zip(work.transactions.iter()))
            {
                // Check revert status
                if simulation.revert_reason().is_some() != receipt.revert_reason().is_some() {
                    let simulated_revert_reason = simulation.revert_reason();
                    let actual_revert_reason = receipt.revert_reason();
                    tracing::warn!(block_number=%work.header.number, transaction_hash=%receipt.transaction_hash, ?simulated_revert_reason, ?actual_revert_reason, "Revert status differs");
                }

                let actual_fee = receipt
                    .actual_fee
                    .try_into_starkfee()
                    .expect("Fee fits into u128")
                    .0;

                // L1 handler transactions have a fee of zero in the receipt.
                if actual_fee == 0 {
                    continue;
                }

                let estimate = &simulation.fee_estimation;

                let actual_data_gas_consumed =
                    receipt.execution_resources.total_gas_consumed.l1_data_gas;
                let actual_gas_consumed = receipt.execution_resources.total_gas_consumed.l1_gas;
                let actual_l2_gas_consumed = receipt.execution_resources.l2_gas.0;

                let estimated_gas_consumed = estimate.l1_gas_consumed.as_u128();
                let estimated_data_gas_consumed = estimate.l1_data_gas_consumed.as_u128();
                let estimated_l2_gas_consumed = estimate.l2_gas_consumed.as_u128();

                let gas_diff = actual_gas_consumed.abs_diff(estimated_gas_consumed);
                let data_gas_diff = actual_data_gas_consumed.abs_diff(estimated_data_gas_consumed);
                let l2_gas_diff = actual_l2_gas_consumed.abs_diff(estimated_l2_gas_consumed);
                let estimate_diff = estimate.overall_fee.abs_diff(actual_fee.into());

                if gas_diff > 0 || data_gas_diff > 0 || l2_gas_diff > 0 || estimate_diff > 0.into()
                {
                    tracing::warn!(block_number=%work.header.number, transaction_hash=%receipt.transaction_hash, execution_status=?receipt.execution_status, transaction=?transaction.variant, %estimated_gas_consumed, %actual_gas_consumed, %estimated_data_gas_consumed, %actual_data_gas_consumed, %estimated_l2_gas_consumed, %actual_l2_gas_consumed, estimated_fee=%estimate.overall_fee, %actual_fee, "Estimation mismatch");
                } else {
                    tracing::debug!(block_number=%work.header.number, transaction_hash=%receipt.transaction_hash, %estimated_gas_consumed, %actual_gas_consumed, %estimated_data_gas_consumed, %actual_data_gas_consumed, %estimated_l2_gas_consumed, %actual_l2_gas_consumed, estimated_fee=%estimate.overall_fee, %actual_fee, "Estimation matches");
                }
            }
        }
        Err(error) => {
            tracing::error!(block_number=%work.header.number, ?error, "Transaction re-execution failed");
        }
    }

    let elapsed = start_time.elapsed().as_millis();

    tracing::debug!(block_number=%work.header.number, %num_transactions, %elapsed, "Re-executed block");
}
