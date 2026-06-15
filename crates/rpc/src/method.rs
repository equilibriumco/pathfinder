pub mod add_declare_transaction;
pub mod add_deploy_account_transaction;
pub mod add_invoke_transaction;
pub mod block_hash_and_number;
pub mod block_number;
pub mod call;
pub mod chain_id;
pub mod consensus_info;
pub mod estimate_fee;
pub mod estimate_message_fee;
pub mod fetch_proposers;
pub mod fetch_validators;
pub mod get_block_transaction_count;
pub mod get_block_with_receipts;
pub mod get_block_with_tx_hashes;
pub mod get_block_with_txs;
pub mod get_class;
pub mod get_class_at;
pub mod get_class_hash_at;
pub mod get_compiled_casm;
pub mod get_events;
pub mod get_messages_status;
pub mod get_nonce;
pub mod get_state_update;
pub mod get_storage_at;
pub mod get_storage_proof;
pub mod get_transaction_by_block_id_and_index;
pub mod get_transaction_by_hash;
pub mod get_transaction_receipt;
pub mod get_transaction_status;
pub mod last_l1_accepted_block_hash_and_number;
pub mod simulate_transactions;
pub mod subscribe_events;
pub mod subscribe_new_heads;
pub mod subscribe_new_transaction_receipts;
pub mod subscribe_new_transactions;
pub mod subscribe_transaction_status;
pub mod syncing;
pub mod trace_block_transactions;
pub mod trace_transaction;

pub use block_hash_and_number::block_hash_and_number;
pub use block_number::block_number;
pub use call::call;
pub use chain_id::chain_id;
pub use estimate_fee::estimate_fee;
pub use estimate_message_fee::estimate_message_fee;
pub use fetch_proposers::fetch_proposers;
pub use fetch_validators::fetch_validators;
pub use get_block_transaction_count::get_block_transaction_count;
pub use get_block_with_receipts::get_block_with_receipts;
pub use get_block_with_tx_hashes::get_block_with_tx_hashes;
pub use get_block_with_txs::get_block_with_txs;
pub use get_class::get_class;
pub use get_class_at::get_class_at;
pub use get_class_hash_at::get_class_hash_at;
pub use get_compiled_casm::get_compiled_casm;
pub use get_events::get_events;
pub use get_messages_status::get_messages_status;
pub use get_nonce::get_nonce;
pub use get_state_update::get_state_update;
pub use get_storage_at::get_storage_at;
pub use get_storage_proof::get_storage_proof;
pub use get_transaction_by_block_id_and_index::get_transaction_by_block_id_and_index;
pub use get_transaction_by_hash::get_transaction_by_hash;
pub use get_transaction_receipt::get_transaction_receipt;
pub use get_transaction_status::get_transaction_status;
pub use last_l1_accepted_block_hash_and_number::last_l1_accepted_block_hash_and_number;
pub use simulate_transactions::simulate_transactions;
pub use syncing::syncing;
pub use trace_block_transactions::trace_block_transactions;
pub use trace_transaction::trace_transaction;

const REORG_SUBSCRIPTION_NAME: &str = "starknet_subscriptionReorg";

/// A helper function used in a few RPC methods.
pub(crate) fn get_latest_block_or_genesis(
    storage: &pathfinder_storage::Storage,
) -> anyhow::Result<pathfinder_common::BlockNumber> {
    use anyhow::Context;

    let mut conn = storage
        .connection()
        .context("Failed to open DB connection")?;
    let db = conn
        .transaction()
        .context("Failed to create DB transaction")?;
    db.block_number(pathfinder_common::BlockId::Latest)
        .context("Failed to get latest block number")
        .map(|latest| latest.unwrap_or_default())
}
