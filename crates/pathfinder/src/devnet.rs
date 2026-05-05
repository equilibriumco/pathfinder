//! Utilities for initializing a devnet for development and testing purposes.
//! Heavily inspired by [starknet-devnet](https://github.com/0xSpaceShard/starknet-devnet) v0.7.1.
//! Unfortunately we cannot use `starknet-devnet` directly because it is not
//! state/storage API agnostic.

use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread::available_parallelism;
use std::time::{Instant, SystemTime};

use anyhow::{Context, Ok};
use p2p::sync::client::conv::ToDto as _;
use p2p_proto::common::{Address, Hash};
use p2p_proto::consensus::{ProposalInit, ProposalPart};
use p2p_proto::sync::transaction::DeclareV3WithoutClass;
use pathfinder_block_commitments::compute_final_hash;
use pathfinder_common::state_update::StateUpdateData;
use pathfinder_common::transaction::{
    DataAvailabilityMode,
    DeclareTransactionV3,
    TransactionVariant,
};
use pathfinder_common::{
    BlockHash,
    BlockHeader,
    BlockId,
    BlockNumber,
    BlockTimestamp,
    ChainId,
    ClassHash,
    ConsensusFinalizedL2Block,
    DecidedBlocks,
    EventCommitment,
    L1DataAvailabilityMode,
    ReceiptCommitment,
    SequencerAddress,
    StarknetVersion,
    StateCommitment,
    Tip,
    TransactionCommitment,
    TransactionSignatureElem,
};
use pathfinder_crypto::signature::ecdsa_sign;
use pathfinder_crypto::Felt;
use pathfinder_executor::{ConcurrentStateReader, ExecutorWorkerPool};
use pathfinder_merkle_tree::starknet_state::update_starknet_state;
use pathfinder_storage::pruning::BlockchainHistoryMode;
use pathfinder_storage::{Storage, StorageBuilder, TriePruneMode};
use pathfinder_validator::{
    ProdTransactionMapper,
    ValidatorBlockInfoStage,
    ValidatorTransactionBatchStage,
    ValidatorWorkerPool,
};

pub use crate::devnet::account::Account;
use crate::devnet::class::{preprocess_sierra, PrepocessedSierra};
use crate::devnet::fixtures::RESOURCE_BOUNDS;

mod account;
mod class;
mod contract;
mod fixtures;
mod utils;

use fixtures::GAS_PRICE;

/// Initializes a devnet DB. The following contracts are predeclared,
/// predeployed and initialized if necessary: Cairo 1 account, ETH and STRK
/// ERC20s, and the UDC. The following contract is already declared but not
/// deployed: Hello Starknet.
pub fn init_db(db_dir: &Path, proposer: Address) -> anyhow::Result<BootDb> {
    let stopwatch = Instant::now();

    let timestamp = strictly_increasing_timestamp(None);
    let db_file_path = db_dir.join("bootstrap.sqlite");

    let storage = StorageBuilder::file(db_file_path.clone())
        .trie_prune_mode(Some(TriePruneMode::Archive))
        .blockchain_history_mode(Some(BlockchainHistoryMode::Archive))
        .migrate()?
        .create_pool(
            NonZeroU32::new(5 + available_parallelism().unwrap().get() as u32).expect(">0"),
        )?;

    tracing::info!(
        "Initialized devnet bootstrap DB in {}",
        db_file_path.display(),
    );

    let mut db_conn = storage.connection()?;
    let db_txn = db_conn.transaction()?;
    let mut state_update = StateUpdateData::default();

    let _account = predeploy_contracts(&db_txn, &mut state_update)?;

    let block_number = BlockNumber::GENESIS;
    let (storage_commitment, class_commitment) = update_starknet_state(
        &db_txn,
        (&state_update).into(),
        true,
        block_number,
        storage.clone(),
    )?;
    let state_commitment = StateCommitment::calculate(
        storage_commitment,
        class_commitment,
        StarknetVersion::V_0_14_0,
    );

    let mut genesis_header = BlockHeader {
        hash: BlockHash::ZERO, // Will be updated
        parent_hash: BlockHash::ZERO,
        number: block_number,
        timestamp,
        eth_l1_gas_price: GAS_PRICE,
        strk_l1_gas_price: GAS_PRICE,
        eth_l1_data_gas_price: GAS_PRICE,
        strk_l1_data_gas_price: GAS_PRICE,
        eth_l2_gas_price: GAS_PRICE,
        strk_l2_gas_price: GAS_PRICE,
        // Alice has this address in integration tests
        sequencer_address: SequencerAddress(proposer.0),
        starknet_version: StarknetVersion::V_0_14_0,
        // No events in genesis, so the root of an empty tree is zero
        event_commitment: EventCommitment(Felt::ZERO),
        state_commitment,
        // No transactions in genesis, so the root of an empty tree is zero
        transaction_commitment: TransactionCommitment(Felt::ZERO),
        transaction_count: 0,
        event_count: 0,
        l1_da_mode: L1DataAvailabilityMode::Calldata,
        // No transactions (and hence no receipts) in genesis, so the root of an empty tree is zero
        receipt_commitment: ReceiptCommitment(Felt::ZERO),
        state_diff_commitment: state_update.compute_state_diff_commitment(),
        state_diff_length: state_update.state_diff_length(),
    };

    genesis_header.hash = compute_final_hash(&genesis_header);

    db_txn.insert_block_header(&genesis_header).unwrap();
    db_txn
        .insert_state_update_data(block_number, &state_update)
        .unwrap();
    db_txn.commit().unwrap();

    tracing::info!(
        "Initialized devnet bootstrap DB genesis block in {} ms",
        stopwatch.elapsed().as_millis(),
    );

    Ok(BootDb {
        db_file_path,
        num_boot_blocks: block_number.get() + 1,
    })
}

/// Returns `Ok(true)` if the DB is bootstrapped and `Ok(false)` if it is not.
pub fn devnet_genesis_exists(db_txn: &pathfinder_storage::Transaction<'_>) -> anyhow::Result<bool> {
    let Some(block_0_commitment) = db_txn.state_diff_commitment(BlockNumber::GENESIS)? else {
        return Ok(false);
    };

    if block_0_commitment != fixtures::GENESIS_COMMITMENT {
        return Ok(false);
    }

    Ok(true)
}

#[derive(Debug)]
pub struct BootDb {
    pub db_file_path: PathBuf,
    pub num_boot_blocks: u64,
}

/// Declare a Cairo 1 class (sierra bytecode) via the DeclareV3
/// transaction.
pub fn declare(
    storage: Storage,
    db_txn: pathfinder_storage::Transaction<'_>,
    account: &Account,
    serialized_sierra: &[u8],
    proposer: Address,
) -> anyhow::Result<()> {
    let stopwatch = Instant::now();

    let PrepocessedSierra {
        sierra_class_hash,
        cairo1_class_p2p,
        sierra_class_ser,
        casm_hash_v2,
        casm,
    } = preprocess_sierra(serialized_sierra, None)?;

    let worker_pool: ValidatorWorkerPool =
        ExecutorWorkerPool::<ConcurrentStateReader>::new(1).get();
    let latest_header = db_txn
        .block_header(BlockId::Latest)?
        .context("DB is empty")?;
    let next_block_number = latest_header.number + 1;

    let (mut validator, _) = init_proposal_and_validator(
        next_block_number,
        0,
        proposer,
        Some(latest_header.timestamp),
        storage.clone(),
        worker_pool.clone(),
    )?;

    let declare = DeclareTransactionV3 {
        class_hash: ClassHash(sierra_class_hash.0),
        nonce: account.fetch_add_nonce(),
        nonce_data_availability_mode: DataAvailabilityMode::L1,
        fee_data_availability_mode: DataAvailabilityMode::L1,
        resource_bounds: RESOURCE_BOUNDS,
        tip: Tip(0),
        paymaster_data: vec![],
        signature: vec![/* Will be filled after signing */],
        account_deployment_data: vec![],
        sender_address: account.address(),
        compiled_class_hash: casm_hash_v2,
    };
    let mut variant = TransactionVariant::DeclareV3(declare);
    let txn_hash = variant.calculate_hash(ChainId::SEPOLIA_TESTNET, false);
    let (r, s) = ecdsa_sign(account.private_key(), txn_hash.0)?;
    let TransactionVariant::DeclareV3(declare) = &mut variant else {
        unreachable!();
    };
    declare.signature = vec![TransactionSignatureElem(r), TransactionSignatureElem(s)];

    let variant = variant.to_dto();

    let p2p_proto::sync::transaction::TransactionVariant::DeclareV3(DeclareV3WithoutClass {
        common,
        ..
    }) = variant
    else {
        unreachable!();
    };

    let declare = p2p_proto::transaction::DeclareV3WithClass {
        common,
        class: cairo1_class_p2p,
    };
    let declare = p2p_proto::consensus::Transaction {
        txn: p2p_proto::consensus::TransactionVariant::DeclareV3(declare),
        transaction_hash: Hash(txn_hash.0),
    };

    validator.execute_batch::<ProdTransactionMapper>(
        vec![declare],
        pathfinder_compiler::ResourceLimits::for_test(),
        pathfinder_compiler::BlockifierLibfuncs::default(),
    )?;

    let next_block = validator.consensus_finalize0()?;

    let (storage_commitment, class_commitment) = update_starknet_state(
        &db_txn,
        (&next_block.state_update).into(),
        true,
        next_block.header.number,
        storage.clone(),
    )?;
    let state_commitment = StateCommitment::calculate(
        storage_commitment,
        class_commitment,
        StarknetVersion::V_0_14_0,
    );

    let ConsensusFinalizedL2Block {
        header,
        state_update,
        transactions_and_receipts,
        events,
        declared_classes,
    } = next_block;

    assert_eq!(declared_classes.len(), 1);
    let declared_class = &declared_classes[0];
    assert_eq!(declared_class.sierra_hash, sierra_class_hash);
    assert_eq!(declared_class.casm_hash_v2, casm_hash_v2);

    let next_header = header.compute_hash(latest_header.hash, state_commitment, compute_final_hash);

    db_txn.insert_block_header(&next_header)?;

    // Insert classes before state update because the latter will trigger
    // `upsert_declared_at` and insert a NULL definition
    db_txn.insert_sierra_class_definition(
        &sierra_class_hash,
        &sierra_class_ser,
        &casm,
        &casm_hash_v2,
    )?;

    db_txn.insert_state_update_data(next_header.number, &state_update)?;
    db_txn.insert_transaction_data(
        next_header.number,
        &transactions_and_receipts,
        Some(&events),
    )?;
    db_txn.commit()?;

    let worker_pool = Arc::into_inner(worker_pool).expect("Refcount is 1");
    worker_pool.join();

    tracing::info!(
        "Declared class {sierra_class_hash} in block {} in {} ms",
        next_header.number,
        stopwatch.elapsed().as_millis(),
    );

    Ok(())
}

/// Predeclare, predeploy, and initialize if necessary: Cairo 1 account, ETH and
/// STRK ERC20s, and the UDC.
fn predeploy_contracts(
    db_txn: &pathfinder_storage::Transaction<'_>,
    state_update: &mut StateUpdateData,
) -> Result<Account, anyhow::Error> {
    fixtures::PREDECLARED_CLASSES
        .iter()
        .copied()
        .try_for_each(|(class, sierra_hash)| {
            class::predeclare(db_txn, state_update, class, Some(sierra_hash))
        })?;
    fixtures::PREDEPLOYED_CONTRACTS
        .iter()
        .copied()
        .try_for_each(|(contract_address, sierra_hash)| {
            contract::predeploy(state_update, contract_address, sierra_hash)
        })?;
    fixtures::ERC20S
        .iter()
        .copied()
        .try_for_each(|(contract_address, name, symbol)| {
            contract::erc20_init(state_update, contract_address, name, symbol)
        })?;
    let account = Account::new_from_fixture();
    account.predeploy(state_update)?;
    Ok(account)
}

/// Returns the current UNIX timestamp, ensuring that it is strictly increasing
/// across calls, if the previous timestamp is provided.
pub fn strictly_increasing_timestamp(prev: Option<BlockTimestamp>) -> BlockTimestamp {
    let current = BlockTimestamp::new_or_panic(
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
    );
    match prev {
        Some(prev) if current.get() <= prev.get() => BlockTimestamp::new_or_panic(prev.get() + 1),
        _ => current,
    }
}

pub fn init_proposal_and_validator(
    height: BlockNumber,
    round: u32,
    proposer: Address,
    prev_timestamp: Option<BlockTimestamp>,
    storage: Storage,
    worker_pool: ValidatorWorkerPool,
) -> anyhow::Result<(ValidatorTransactionBatchStage, Vec<ProposalPart>)> {
    let proposal_init = ProposalInit {
        height: height.get(),
        round,
        valid_round: None,
        proposer,
        timestamp: strictly_increasing_timestamp(prev_timestamp).get(),
        builder: proposer,
        l1_da_mode: p2p_proto::common::L1DataAvailabilityMode::Calldata,
        l2_gas_price_fri: GAS_PRICE.0,
        l1_gas_price_fri: GAS_PRICE.0,
        l1_data_gas_price_fri: GAS_PRICE.0,
        l1_gas_price_wei: GAS_PRICE.0,
        l1_data_gas_price_wei: GAS_PRICE.0,
        starknet_version: StarknetVersion::V_0_14_0.to_string(),
        version_constant_commitment: Hash::ZERO,
    };
    let validator = ValidatorBlockInfoStage::new(ChainId::SEPOLIA_TESTNET, proposal_init.clone())
        .expect("valid block height");

    let validator = validator.validate_block_info(
        storage.clone(),
        DecidedBlocks::default(),
        None,
        None,
        None,
        worker_pool.clone(),
        StarknetVersion::V_0_14_0,
    )?;
    Ok((validator, vec![ProposalPart::Init(proposal_init)]))
}

#[cfg(test)]
pub mod tests {
    use std::num::NonZeroU32;
    use std::sync::Arc;
    use std::thread::available_parallelism;

    use p2p_proto::common::Address;
    use pathfinder_block_commitments::compute_final_hash;
    use pathfinder_common::{
        BlockHash,
        BlockHeader,
        BlockId,
        ClassHash,
        ConsensusFinalizedL2Block,
        ContractAddress,
        DeclaredClass,
        StarknetVersion,
        StateCommitment,
    };
    use pathfinder_crypto::Felt;
    use pathfinder_executor::{ConcurrentStateReader, ExecutorWorkerPool};
    use pathfinder_merkle_tree::starknet_state::update_starknet_state;
    use pathfinder_storage::{Storage, StorageBuilder};
    use pathfinder_validator::{ProdTransactionMapper, ValidatorWorkerPool};
    use tempfile::TempDir;

    use crate::devnet::account::Account;
    use crate::devnet::{fixtures, init_db, init_proposal_and_validator, BootDb};

    #[test_log::test]
    fn init_declare_deploy_invoke_hello_abi() {
        // Block 0 - predeploys and initializes contracts, including the account we'll
        // use for testing
        let proposer = Address(Felt::ONE);
        let db_dir = TempDir::new().unwrap();
        let BootDb { db_file_path, .. } = init_db(db_dir.path(), proposer).unwrap();

        let storage = StorageBuilder::file(db_file_path)
            .migrate()
            .unwrap()
            .create_pool(
                NonZeroU32::new(5 + available_parallelism().unwrap().get() as u32).unwrap(),
            )
            .unwrap();

        let mut db_conn = storage.connection().unwrap();
        let db_txn = db_conn.transaction().unwrap();

        let block_0_header = db_txn.block_header(BlockId::Latest).unwrap().unwrap();
        assert_eq!(
            block_0_header.state_diff_commitment,
            fixtures::GENESIS_COMMITMENT
        );

        let account = Account::from_storage(&db_txn).unwrap();
        drop(db_txn);

        let worker_pool: ValidatorWorkerPool =
            ExecutorWorkerPool::<ConcurrentStateReader>::new(1).get();

        // Block 1 - declare the Hello Starknet contract class
        let block_1_number = block_0_header.number + 1;
        let (mut validator, _) = init_proposal_and_validator(
            block_1_number,
            0,
            proposer,
            Some(block_0_header.timestamp),
            storage.clone(),
            worker_pool.clone(),
        )
        .unwrap();
        let declare = account.hello_starknet_declare().unwrap();
        validator
            .execute_batch::<ProdTransactionMapper>(
                vec![declare],
                pathfinder_compiler::ResourceLimits::for_test(),
                pathfinder_compiler::BlockifierLibfuncs::default(),
            )
            .unwrap();
        let block_1 = validator.consensus_finalize0().unwrap();

        let db_txn = db_conn.transaction().unwrap();
        let (block_1_header, _) =
            insert_block(storage.clone(), db_txn, block_1, block_0_header.hash);

        // Block 2 - deploy a Hello Starknet contract instance via the UDC
        let block_2_number = block_1_header.number + 1;
        let (mut validator, _) = init_proposal_and_validator(
            block_2_number,
            0,
            proposer,
            Some(block_1_header.timestamp),
            storage.clone(),
            worker_pool.clone(),
        )
        .unwrap();
        let deploy = account.hello_starknet_deploy().unwrap();
        validator
            .execute_batch::<ProdTransactionMapper>(
                vec![deploy],
                pathfinder_compiler::ResourceLimits::for_test(),
                pathfinder_compiler::BlockifierLibfuncs::default(),
            )
            .unwrap();
        let block_2 = validator.consensus_finalize0().unwrap();

        let db_txn = db_conn.transaction().unwrap();
        let (block_2_header, hello_contract_address) =
            insert_block(storage.clone(), db_txn, block_2, block_1_header.hash);
        let hello_contract_address = hello_contract_address.unwrap();

        // Block 3 - invoke increase_balance and get_balance on the deployed Hello
        // Starknet instance
        let block_3_number = block_2_header.number + 1;
        let (mut validator, _) = init_proposal_and_validator(
            block_3_number,
            0,
            proposer,
            Some(block_2_header.timestamp),
            storage.clone(),
            worker_pool.clone(),
        )
        .unwrap();
        let increase_balance =
            account.hello_starknet_increase_balance(hello_contract_address, 0xABCD);
        let get_balance = account.hello_starknet_get_balance(hello_contract_address);
        validator
            .execute_batch::<ProdTransactionMapper>(
                vec![increase_balance, get_balance],
                pathfinder_compiler::ResourceLimits::for_test(),
                pathfinder_compiler::BlockifierLibfuncs::default(),
            )
            .unwrap();
        let block_3 = validator.consensus_finalize0().unwrap();

        let db_txn = db_conn.transaction().unwrap();
        insert_block(storage.clone(), db_txn, block_3, block_2_header.hash);

        let worker_pool = Arc::into_inner(worker_pool).unwrap();
        worker_pool.join();
    }

    fn insert_block(
        storage: Storage,
        db_txn: pathfinder_storage::Transaction<'_>,
        block: ConsensusFinalizedL2Block,
        parent_hash: BlockHash,
    ) -> (BlockHeader, Option<ContractAddress>) {
        let (storage_commitment, class_commitment) = update_starknet_state(
            &db_txn,
            (&block.state_update).into(),
            true,
            block.header.number,
            storage.clone(),
        )
        .unwrap();
        let state_commitment = StateCommitment::calculate(
            storage_commitment,
            class_commitment,
            StarknetVersion::V_0_14_0,
        );

        let ConsensusFinalizedL2Block {
            header,
            state_update,
            transactions_and_receipts,
            events,
            declared_classes,
        } = block;

        declared_classes
            .into_iter()
            .try_for_each(
                |DeclaredClass {
                     sierra_hash,
                     casm_hash_v2,
                     sierra_def,
                     casm_def,
                 }| {
                    db_txn.insert_sierra_class_definition(
                        &sierra_hash,
                        &sierra_def,
                        &casm_def,
                        &casm_hash_v2,
                    )
                },
            )
            .unwrap();

        let hello_contract_address =
            state_update
                .contract_updates
                .iter()
                .find_map(|(contract_address, update)| {
                    update
                        .class
                        .is_some_and(|x| x.class_hash() == ClassHash(fixtures::HELLO_CLASS_HASH.0))
                        .then_some(*contract_address)
                });

        let header = header.compute_hash(parent_hash, state_commitment, compute_final_hash);

        db_txn.insert_block_header(&header).unwrap();
        db_txn
            .insert_state_update_data(header.number, &state_update)
            .unwrap();
        db_txn
            .insert_transaction_data(header.number, &transactions_and_receipts, Some(&events))
            .unwrap();
        db_txn.commit().unwrap();
        (header, hello_contract_address)
    }
}
