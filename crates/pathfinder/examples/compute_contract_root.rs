use std::num::NonZeroU32;

use anyhow::Context;
use pathfinder_common::hash::PedersenHash;
use pathfinder_common::{BlockNumber, ContractAddress, StorageAddress, StorageValue};
use pathfinder_crypto::Felt;
use pathfinder_merkle_tree::ContractsStorageTree;

pub fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
        .compact()
        .init();

    let database_path = std::env::args().nth(1).unwrap();
    let storage = pathfinder_storage::StorageBuilder::file(database_path.into())
        .migrate()?
        .create_pool(NonZeroU32::new(1).unwrap())?;
    let mut db = storage
        .connection()
        .context("Opening database connection")?;
    let transaction = db.transaction().context("Creating database transaction")?;

    let block_number = std::env::args().nth(2).unwrap();
    let block_number: u64 = block_number.parse().unwrap();
    let block_number = BlockNumber::new_or_panic(block_number);

    let contract_address = std::env::args().nth(3).unwrap();
    let contract_address =
        Felt::from_hex_str(&contract_address).expect("Failed to parse contract address");
    let contract_address = ContractAddress::new_or_panic(contract_address);

    let storage_address = std::env::args().nth(4).unwrap();
    let storage_address =
        Felt::from_hex_str(&storage_address).expect("Failed to parse storage address");
    let storage_address = StorageAddress::new_or_panic(storage_address);

    let storage_value = std::env::args().nth(5).unwrap();
    let storage_value = Felt::from_hex_str(&storage_value).expect("Failed to parse storage value");
    let storage_value = StorageValue(storage_value);

    let mut contract_tree =
        ContractsStorageTree::<PedersenHash>::load(&transaction, contract_address, block_number)?;
    contract_tree.set(storage_address, storage_value)?;

    let (root, _trie_update) = contract_tree.commit()?;

    println!("Contract storage root: {:?}", root);

    Ok(())
}
