use anyhow::Context;
use pathfinder_common::{L1TransactionHash, TransactionHash};

use crate::context::RpcContext;
use crate::dto::TxnExecutionStatus;
use crate::method::get_transaction_status;
use crate::RpcVersion;

#[derive(Debug, PartialEq, Eq)]
pub struct Input {
    transaction_hash: L1TransactionHash,
}

impl crate::dto::DeserializeForVersion for Input {
    fn deserialize(value: crate::dto::Value) -> Result<Self, serde_json::Error> {
        value.deserialize_map(|value| {
            Ok(Self {
                transaction_hash: value
                    .deserialize("transaction_hash")
                    .map(L1TransactionHash::new)?,
            })
        })
    }
}

#[derive(Clone, Debug)]
enum FinalityStatus {
    Received,
    PreConfirmed,
    AcceptedOnL2,
    AcceptedOnL1,
}

impl crate::dto::SerializeForVersion for FinalityStatus {
    fn serialize(
        &self,
        serializer: crate::dto::Serializer,
    ) -> Result<crate::dto::Ok, crate::dto::Error> {
        let status_str = match self {
            FinalityStatus::Received => "RECEIVED",
            FinalityStatus::PreConfirmed => "PRE_CONFIRMED",
            FinalityStatus::AcceptedOnL2 => "ACCEPTED_ON_L2",
            FinalityStatus::AcceptedOnL1 => "ACCEPTED_ON_L1",
        };
        serializer.serialize_str(status_str)
    }
}

#[derive(Clone, Debug)]
pub struct L1HandlerTransactionStatus {
    transaction_hash: TransactionHash,
    finality_status: FinalityStatus,
    execution_status: Option<TxnExecutionStatus>,
}

#[derive(Debug)]
pub struct Output(Vec<L1HandlerTransactionStatus>);

crate::error::generate_rpc_error_subset!(Error: TxnHashNotFound);

pub async fn get_messages_status(
    context: RpcContext,
    input: Input,
    rpc_version: RpcVersion,
) -> Result<Output, Error> {
    let span = tracing::Span::current();

    let _g = span.enter();

    // Fetch the L1 handler transactions for the given transaction hash
    let ethereum = context.ethereum.clone();

    let l1_handler_txs = ethereum
        .get_l1_handler_txs(
            &context.contract_addresses.l1_contract_address,
            &input.transaction_hash,
        )
        .await
        .context("Fetching L1 handler tx hashes")
        .map_err(|_| Error::TxnHashNotFound)?;

    let mut res = vec![];
    for tx in l1_handler_txs {
        let tx_hash = tx.calculate_hash(context.chain_id);

        let input = get_transaction_status::Input::new(tx_hash);
        let status = get_transaction_status(context.clone(), input, rpc_version)
            .await
            .map_err(|_| Error::TxnHashNotFound)?;

        use get_transaction_status::Output as TxStatus;
        let (finality_status, execution_status) = match status {
            // Since Starknet 0.14, get_transaction_status isn't supposed to return Received or
            // Rejected for L1 handler transactions; the cases are kept for backwards compatibility,
            // more explicit error handling can be added if/when they actually happen. Moreover
            // Transaction finality status Rejected has been removed in RPC v0.9.
            TxStatus::Received => (FinalityStatus::Received, None),
            TxStatus::PreConfirmed(ref exec_status) => {
                (FinalityStatus::PreConfirmed, Some(exec_status.clone()))
            }
            TxStatus::AcceptedOnL1(ref exec_status) => {
                (FinalityStatus::AcceptedOnL1, Some(exec_status.clone()))
            }
            TxStatus::AcceptedOnL2(ref exec_status) => {
                (FinalityStatus::AcceptedOnL2, Some(exec_status.clone()))
            }
        };

        if execution_status.is_none() {
            continue; // Skip if execution status is not available, since it's
                      // required for V09+
        }

        res.push(L1HandlerTransactionStatus {
            transaction_hash: tx.calculate_hash(context.chain_id),
            finality_status,
            execution_status,
        });
    }

    Ok(Output(res))
}

impl crate::dto::SerializeForVersion for Output {
    fn serialize(
        &self,
        serializer: crate::dto::Serializer,
    ) -> Result<crate::dto::Ok, crate::dto::Error> {
        serializer.serialize_iter(self.0.len(), &mut self.0.clone().into_iter())
    }
}

impl crate::dto::SerializeForVersion for L1HandlerTransactionStatus {
    fn serialize(
        &self,
        serializer: crate::dto::Serializer,
    ) -> Result<crate::dto::Ok, crate::dto::Error> {
        let mut serializer = serializer.serialize_struct()?;
        serializer.serialize_field("transaction_hash", &self.transaction_hash)?;
        serializer.serialize_field("finality_status", &self.finality_status)?;
        serializer.serialize_optional("execution_status", self.execution_status.clone())?;
        serializer.end()
    }
}
