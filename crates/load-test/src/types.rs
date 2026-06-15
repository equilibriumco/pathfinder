//! Contains simplified types for parsing JSON-RPC responses.
//!
//! In order not to depend on pathfinder_lib these types "duplicate" similar
//! functionality already found in pathfinder. However, these types are
//! simplified and are missing fields that are irrelevant for load tests.
use pathfinder_crypto::Felt;

#[derive(Clone, Debug, serde::Deserialize, PartialEq, Eq)]
pub struct Block {
    pub block_hash: Felt,
    pub parent_hash: Felt,
    pub block_number: u64,
    pub new_root: Felt,
    pub timestamp: u64,
    pub sequencer_address: Felt,
    pub transactions: Vec<Felt>,
}

#[derive(Clone, Debug, serde::Deserialize, PartialEq, Eq)]
pub struct Transaction {
    pub r#type: String,
    pub transaction_hash: Felt,
}

#[derive(Clone, Debug, serde::Deserialize, PartialEq, Eq)]
pub struct TransactionReceipt {
    pub r#type: String,
    pub transaction_hash: Felt,
}

#[derive(Clone, Debug, serde::Deserialize, PartialEq, Eq)]
pub struct StateUpdate {
    pub block_hash: Felt,
    pub new_root: Felt,
    pub old_root: Felt,
}

#[derive(Clone, Debug, serde::Deserialize, PartialEq, Eq)]
pub struct ContractClass {
    pub abi: serde_json::Value,
    pub program: String,
}

#[derive(Clone, Debug, serde::Deserialize, PartialEq, Eq)]
pub struct FeeEstimate {
    pub l1_gas_consumed: String,
    pub l1_gas_price: String,
    pub l1_data_gas_consumed: String,
    pub l1_data_gas_price: String,
    pub l2_gas_consumed: String,
    pub l2_gas_price: String,
    pub overall_fee: String,
    pub unit: String,
}
