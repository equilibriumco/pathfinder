use goose::prelude::*;
use pathfinder_crypto::Felt;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::json;

use crate::types::{
    Block,
    ContractClass,
    FeeEstimate,
    StateUpdate,
    Transaction,
    TransactionReceipt,
};

pub type MethodResult<T> = Result<T, Box<goose::goose::TransactionError>>;

pub async fn get_block_by_number(user: &mut GooseUser, block_number: u64) -> MethodResult<Block> {
    post_jsonrpc_request(
        user,
        "starknet_getBlockWithTxHashes",
        json!({ "block_id": { "block_number": block_number } }),
    )
    .await
}

pub async fn get_block_by_hash(user: &mut GooseUser, block_hash: Felt) -> MethodResult<Block> {
    post_jsonrpc_request(
        user,
        "starknet_getBlockWithTxHashes",
        json!({ "block_id": { "block_hash": block_hash } }),
    )
    .await
}

pub async fn get_state_update(user: &mut GooseUser, block_hash: Felt) -> MethodResult<StateUpdate> {
    post_jsonrpc_request(
        user,
        "starknet_getStateUpdate",
        json!({ "block_id": { "block_hash": block_hash }}),
    )
    .await
}

pub async fn get_transaction_by_hash(
    user: &mut GooseUser,
    hash: Felt,
) -> MethodResult<Transaction> {
    post_jsonrpc_request(
        user,
        "starknet_getTransactionByHash",
        json!({ "transaction_hash": hash }),
    )
    .await
}

pub async fn get_transaction_by_block_hash_and_index(
    user: &mut GooseUser,
    block_hash: Felt,
    index: usize,
) -> MethodResult<Transaction> {
    post_jsonrpc_request(
        user,
        "starknet_getTransactionByBlockIdAndIndex",
        json!({ "block_id": {"block_hash": block_hash}, "index": index }),
    )
    .await
}

pub async fn get_transaction_by_block_number_and_index(
    user: &mut GooseUser,
    block_number: u64,
    index: usize,
) -> MethodResult<Transaction> {
    post_jsonrpc_request(
        user,
        "starknet_getTransactionByBlockIdAndIndex",
        json!({ "block_id": {"block_number": block_number}, "index": index }),
    )
    .await
}

pub async fn get_transaction_receipt_by_hash(
    user: &mut GooseUser,
    hash: Felt,
) -> MethodResult<TransactionReceipt> {
    post_jsonrpc_request(
        user,
        "starknet_getTransactionReceipt",
        json!({ "transaction_hash": hash }),
    )
    .await
}

pub async fn get_block_transaction_count_by_hash(
    user: &mut GooseUser,
    hash: Felt,
) -> MethodResult<u64> {
    post_jsonrpc_request(
        user,
        "starknet_getBlockTransactionCount",
        json!({ "block_id": { "block_hash": hash } }),
    )
    .await
}

pub async fn get_block_transaction_count_by_number(
    user: &mut GooseUser,
    number: u64,
) -> MethodResult<u64> {
    post_jsonrpc_request(
        user,
        "starknet_getBlockTransactionCount",
        json!({ "block_id": { "block_number": number } }),
    )
    .await
}

pub async fn get_class(
    user: &mut GooseUser,
    block_hash: Felt,
    class_hash: Felt,
) -> MethodResult<ContractClass> {
    post_jsonrpc_request(
        user,
        "starknet_getClass",
        json!({ "block_id": { "block_hash": block_hash }, "class_hash": class_hash }),
    )
    .await
}

pub async fn get_class_hash_at(
    user: &mut GooseUser,
    block_hash: Felt,
    contract_address: Felt,
) -> MethodResult<Felt> {
    post_jsonrpc_request(
        user,
        "starknet_getClassHashAt",
        json!({ "block_id": { "block_hash": block_hash }, "contract_address": contract_address }),
    )
    .await
}

pub async fn get_class_at(
    user: &mut GooseUser,
    block_hash: Felt,
    contract_address: Felt,
) -> MethodResult<ContractClass> {
    post_jsonrpc_request(
        user,
        "starknet_getClassAt",
        json!({ "block_id": { "block_hash": block_hash }, "contract_address": contract_address }),
    )
    .await
}

pub async fn block_number(user: &mut GooseUser) -> MethodResult<u64> {
    post_jsonrpc_request(user, "starknet_blockNumber", json!({})).await
}

pub async fn syncing(user: &mut GooseUser) -> MethodResult<serde_json::Value> {
    post_jsonrpc_request(user, "starknet_syncing", json!({})).await
}

pub async fn chain_id(user: &mut GooseUser) -> MethodResult<String> {
    post_jsonrpc_request(user, "starknet_chainId", json!({})).await
}

pub async fn get_events(
    user: &mut GooseUser,
    filter: EventFilter,
) -> MethodResult<GetEventsResult> {
    let from_block = block_number_to_block_id(filter.from_block);
    let to_block = block_number_to_block_id(filter.to_block);
    post_jsonrpc_request(
        user,
        "starknet_getEvents",
        json!({ "filter": {
            "from_block": from_block,
            "to_block": to_block,
            "address": filter.address,
            "keys": filter.keys,
            "chunk_size": filter.chunk_size,
        }}),
    )
    .await
}

pub struct EventFilter {
    pub from_block: Option<u64>,
    pub to_block: Option<u64>,
    pub address: Option<Felt>,
    pub keys: Vec<Vec<Felt>>,
    pub chunk_size: u64,
}

#[derive(Clone, Debug, serde::Deserialize, PartialEq, Eq)]
pub struct GetEventsResult {
    pub events: Vec<serde_json::Value>,
    pub continuation_token: Option<String>,
}

fn block_number_to_block_id(number: Option<u64>) -> serde_json::Value {
    match number {
        Some(number) => json!({ "block_number": number }),
        None => serde_json::Value::Null,
    }
}

pub async fn get_storage_at(
    user: &mut GooseUser,
    contract_address: Felt,
    key: Felt,
    block_hash: Felt,
) -> MethodResult<Felt> {
    post_jsonrpc_request(
        user,
        "starknet_getStorageAt",
        json!({ "contract_address": contract_address, "key": key, "block_id": {"block_hash": block_hash} }),
    )
    .await
}

pub async fn call(
    user: &mut GooseUser,
    contract_address: Felt,
    call_data: &[&str],
    entry_point_selector: &str,
) -> MethodResult<Vec<String>> {
    post_jsonrpc_request(
        user,
        "starknet_call",
        json!({
            "request": {
                "contract_address": contract_address,
                "calldata": call_data,
                "entry_point_selector": entry_point_selector,
            },
            "block_id": "pre_confirmed",
        }),
    )
    .await
}

pub async fn estimate_fee_for_invoke(
    user: &mut GooseUser,
    sender_address: Felt,
    call_data: &[Felt],
    nonce: Felt,
    max_fee: Felt,
) -> MethodResult<Vec<FeeEstimate>> {
    post_jsonrpc_request(
        user,
        "starknet_estimateFee",
        json!({
            "request": [{
                "type": "INVOKE",
                "version": "0x1",
                "max_fee": max_fee,
                "signature": [],
                "nonce": nonce,
                "sender_address": sender_address,
                "calldata": call_data,
            }],
            // Skip validation so the historical nonce and empty signature don't fail the validate/nonce
            // checks - we only want to exercise the execution path under load.
            "simulation_flags": ["SKIP_VALIDATE"],
            // Estimate against the state just before the transaction was included in a block
            // (it was included in block 500000), so the transfer's balance check matches
            // the historical state instead of drifting with the tip of the chain.
            "block_id": {"block_number": 499999}
        }),
    )
    .await
}

pub async fn get_nonce(user: &mut GooseUser, contract_address: Felt) -> MethodResult<Felt> {
    post_jsonrpc_request(
        user,
        "starknet_getNonce",
        json!({
            "block_id": "pre_confirmed",
            "contract_address": contract_address
        }),
    )
    .await
}

async fn post_jsonrpc_request<T: DeserializeOwned>(
    user: &mut GooseUser,
    method: &str,
    params: serde_json::Value,
) -> MethodResult<T> {
    let request = jsonrpc_request(method, params);
    let response = user
        .post_json("/rpc/v0_9", &request)
        .await?
        .response
        .map_err(|e| Box::new(e.into()))?;
    #[derive(Deserialize)]
    struct TransactionReceiptResponse<T> {
        result: T,
    }
    let response: TransactionReceiptResponse<T> =
        response.json().await.map_err(|e| Box::new(e.into()))?;

    Ok(response.result)
}

fn jsonrpc_request(method: &str, params: serde_json::Value) -> serde_json::Value {
    json!({
        "jsonrpc": "2.0",
        "id": "0",
        "method": method,
        "params": params,
    })
}
