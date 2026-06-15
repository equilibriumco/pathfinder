use crate::jsonrpc::{RpcRouter, RpcRouterBuilder};
use crate::method::subscribe_events::SubscribeEvents;
use crate::method::subscribe_new_heads::SubscribeNewHeads;
use crate::method::subscribe_new_transaction_receipts::SubscribeNewTransactionReceipts;
use crate::method::subscribe_new_transactions::SubscribeNewTransactions;
use crate::method::subscribe_transaction_status::SubscribeTransactionStatus;
// re-using v09-specific methods
use crate::v09::method as v09_method;

#[rustfmt::skip]
pub fn register_routes() -> RpcRouterBuilder {
    RpcRouter::builder(crate::RpcVersion::V10)
        .register("pathfinder_lastL1AcceptedBlockHashAndNumber",  crate::method::last_l1_accepted_block_hash_and_number)
        .register("starknet_addDeclareTransaction",               v09_method::add_declare_transaction)
        .register("starknet_addDeployAccountTransaction",         v09_method::add_deploy_account_transaction)
        .register("starknet_addInvokeTransaction",                v09_method::add_invoke_transaction)
        .register("starknet_blockHashAndNumber",                  crate::method::block_hash_and_number)
        .register("starknet_blockNumber",                         crate::method::block_number)
        .register("starknet_call",                                crate::method::call)
        .register("starknet_chainId",                             crate::method::chain_id)
        .register("starknet_estimateFee",                         crate::method::estimate_fee)
        .register("starknet_estimateMessageFee",                  crate::method::estimate_message_fee)
        .register("starknet_getBlockTransactionCount",            crate::method::get_block_transaction_count)
        .register("starknet_getBlockWithTxHashes",                crate::method::get_block_with_tx_hashes)
        .register("starknet_getBlockWithTxs",                     crate::method::get_block_with_txs)
        .register("starknet_getClass",                            crate::method::get_class)
        .register("starknet_getClassAt",                          crate::method::get_class_at)
        .register("starknet_getClassHashAt",                      crate::method::get_class_hash_at)
        .register("starknet_getEvents",                           crate::method::get_events)
        .register("starknet_getMessagesStatus",                   crate::method::get_messages_status)
        .register("starknet_getNonce",                            crate::method::get_nonce)
        .register("starknet_getStateUpdate",                      crate::method::get_state_update)
        .register("starknet_getStorageAt",                        crate::method::get_storage_at)
        .register("starknet_getStorageProof",                     crate::method::get_storage_proof)
        .register("starknet_getTransactionByBlockIdAndIndex",     crate::method::get_transaction_by_block_id_and_index)
        .register("starknet_getTransactionByHash",                crate::method::get_transaction_by_hash)
        .register("starknet_getTransactionReceipt",               crate::method::get_transaction_receipt)
        .register("starknet_getTransactionStatus",                crate::method::get_transaction_status)
        .register("starknet_getBlockWithReceipts",                crate::method::get_block_with_receipts)
        .register("starknet_simulateTransactions",                crate::method::simulate_transactions)
        .register("starknet_subscribeNewHeads",                   SubscribeNewHeads)
        .register("starknet_subscribeNewTransactionReceipts",     SubscribeNewTransactionReceipts)
        .register("starknet_subscribeNewTransactions",            SubscribeNewTransactions)
        .register("starknet_subscribeEvents",                     SubscribeEvents)
        .register("starknet_subscribeTransactionStatus",          SubscribeTransactionStatus)
        .register("starknet_specVersion",                         || "0.10.3-rc.0")
        .register("starknet_syncing",                             crate::method::syncing)
        .register("starknet_traceBlockTransactions",              crate::method::trace_block_transactions)
        .register("starknet_traceTransaction",                    crate::method::trace_transaction)
        .register("starknet_getCompiledCasm",                     crate::method::get_compiled_casm)
}
