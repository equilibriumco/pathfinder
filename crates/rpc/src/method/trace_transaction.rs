use anyhow::Context;
use pathfinder_common::{ChainId, TransactionHash};
use pathfinder_executor::TransactionExecutionError;
use starknet_gateway_client::GatewayApi;

use crate::context::RpcContext;
use crate::dto::TransactionTrace;
use crate::error::{ApplicationError, TraceError};
use crate::executor::{
    ExecutionStateError,
    MAINNET_RANGE_WHERE_RE_EXECUTION_IS_IMPOSSIBLE_END,
    MAINNET_RANGE_WHERE_RE_EXECUTION_IS_IMPOSSIBLE_START,
    VERSIONS_LOWER_THAN_THIS_SHOULD_FALL_BACK_TO_FETCHING_TRACE_FROM_GATEWAY,
};
use crate::method::trace_block_transactions::map_gateway_trace;
use crate::{compose_executor_transaction, RpcVersion};

#[derive(Debug)]
pub struct Input {
    pub transaction_hash: TransactionHash,
}

impl crate::dto::DeserializeForVersion for Input {
    fn deserialize(value: crate::dto::Value) -> Result<Self, serde_json::Error> {
        value.deserialize_map(|value| {
            Ok(Self {
                transaction_hash: value.deserialize("transaction_hash").map(TransactionHash)?,
            })
        })
    }
}

#[derive(Debug)]
pub struct Output(TransactionTrace);

impl crate::dto::SerializeForVersion for Output {
    fn serialize(
        &self,
        serializer: crate::dto::Serializer,
    ) -> Result<crate::dto::Ok, crate::dto::Error> {
        self.0.serialize(serializer)
    }
}

pub async fn trace_transaction(
    context: RpcContext,
    input: Input,
    rpc_version: RpcVersion,
) -> Result<Output, TraceTransactionError> {
    #[allow(clippy::large_enum_variant)]
    enum LocalExecution {
        Success(pathfinder_executor::types::TransactionTrace),
        Unsupported(pathfinder_common::transaction::Transaction),
    }

    let span = tracing::Span::current();
    let local =
        util::task::spawn_blocking(move |_| -> Result<LocalExecution, TraceTransactionError> {
            let _g = span.enter();

            let mut db_conn = context
                .execution_storage
                .connection()
                .context("Creating database connection")?;
            let db_tx = db_conn
                .transaction()
                .context("Creating database transaction")?;

            // Find the transaction's block.
            let pending = context.pending_data.get(&db_tx, rpc_version)?;

            let (header, transactions, cache) = if let Some(pending_tx) = pending
                .pre_confirmed_transactions()
                .iter()
                .find(|tx| tx.hash == input.transaction_hash)
            {
                let header = pending.pre_confirmed_header();

                if header.starknet_version
                    < VERSIONS_LOWER_THAN_THIS_SHOULD_FALL_BACK_TO_FETCHING_TRACE_FROM_GATEWAY
                {
                    return Ok(LocalExecution::Unsupported(pending_tx.clone()));
                }

                (
                    header,
                    pending.pre_confirmed_transactions().to_vec(),
                    // Can't use the cache for pending blocks since they have no block hash.
                    pathfinder_executor::TraceCache::default(),
                )
            } else if let Some(pre_latest_tx) = pending.pre_latest_block().and_then(|pre_latest| {
                pre_latest
                    .transactions
                    .iter()
                    .find(|tx| tx.hash == input.transaction_hash)
                    .cloned()
            }) {
                let header = pending
                    .pre_latest_header()
                    .expect("Pre-latest block exists");

                if header.starknet_version
                    < VERSIONS_LOWER_THAN_THIS_SHOULD_FALL_BACK_TO_FETCHING_TRACE_FROM_GATEWAY
                {
                    return Ok(LocalExecution::Unsupported(pre_latest_tx.clone()));
                }

                let txs = pending
                    .pre_latest_block()
                    .expect("Pre-latest block exists")
                    .transactions
                    .clone();

                (
                    header,
                    txs,
                    // Can't use the cache for pre-latest blocks since they have no block hash.
                    pathfinder_executor::TraceCache::default(),
                )
            } else {
                let block_hash = db_tx
                    .transaction_block_hash(input.transaction_hash)?
                    .ok_or(TraceTransactionError::TxnHashNotFound)?;

                let header = db_tx
                    .block_header(block_hash.into())
                    .context("Fetching block header")?
                    .context("Block header is missing")?;

                if header.starknet_version
                    < VERSIONS_LOWER_THAN_THIS_SHOULD_FALL_BACK_TO_FETCHING_TRACE_FROM_GATEWAY
                {
                    let transaction = db_tx
                        .transaction(input.transaction_hash)
                        .context("Fetching transaction data")?
                        .context("Transaction data missing")?;

                    return Ok(LocalExecution::Unsupported(transaction));
                }

                // Mainnet has a block range where re-execution is not possible (we get a
                // different state diff due to a bug that was present on the
                // sequencer when these blocks were produced). We should fall
                // back to fetching traces from the feeder gateway instead.
                if context.chain_id == ChainId::MAINNET
                    && header.number >= MAINNET_RANGE_WHERE_RE_EXECUTION_IS_IMPOSSIBLE_START
                    && header.number <= MAINNET_RANGE_WHERE_RE_EXECUTION_IS_IMPOSSIBLE_END
                {
                    let transaction = db_tx
                        .transaction(input.transaction_hash)
                        .context("Fetching transaction data")?
                        .context("Transaction data missing")?;

                    return Ok(LocalExecution::Unsupported(transaction));
                }

                let transactions = db_tx
                    .transactions_for_block(header.number.into())
                    .context("Fetching block transactions")?
                    .context("Block transactions missing")?
                    .into_iter()
                    .collect::<Vec<_>>();

                (header, transactions.clone(), context.cache.clone())
            };

            let hash = header.hash;
            let state = pathfinder_executor::ExecutionState::trace(
                context.chain_id,
                header,
                None,
                context.config.versioned_constants_map,
                context.contract_addresses.eth_l2_token_address,
                context.contract_addresses.strk_l2_token_address,
                context.native_class_cache,
                context
                    .config
                    .native_execution_force_use_for_incompatible_classes,
            );

            // The flag is not included in the spec for this method. Moreover, it isn't
            // possible to return per-transaction initial reads at the moment.
            let return_initial_reads = false;

            let executor_transactions = transactions
                .iter()
                .map(|transaction| compose_executor_transaction(transaction, &db_tx))
                .collect::<Result<Vec<_>, _>>()?;

            match pathfinder_executor::trace(
                db_tx,
                state,
                cache,
                hash,
                executor_transactions,
                return_initial_reads,
            ) {
                Ok(pathfinder_executor::BlockTraces::TracesOnly(traces)) => {
                    let trace = traces
                        .into_iter()
                        .find_map(|(tx_hash, trace)| {
                            if tx_hash == input.transaction_hash {
                                Some(trace)
                            } else {
                                None
                            }
                        })
                        .ok_or_else(|| {
                            TraceTransactionError::Internal(anyhow::anyhow!(
                                "Transaction trace missing from block: {}",
                                input.transaction_hash
                            ))
                        })?;
                    Ok(LocalExecution::Success(trace))
                }
                Ok(pathfinder_executor::BlockTraces::TracesWithInitialReads { .. }) => {
                    unreachable!("return_initial_reads is false")
                }
                Err(e) => Err(e.into()),
            }
        })
        .await
        .context("trace_transaction: execution")??;

    let transaction = match local {
        LocalExecution::Success(trace) => {
            return Ok(Output(TransactionTrace {
                trace: trace.clone(),
                include_state_diff: false,
            }));
        }
        LocalExecution::Unsupported(tx) => tx,
    };

    let trace = context
        .sequencer
        .transaction_trace(input.transaction_hash)
        .await
        .context("Proxying call to feeder gateway")?;

    let trace = map_gateway_trace(transaction, trace)?;

    Ok(Output(TransactionTrace {
        trace: trace.clone(),
        // State diffs are not available for traces fetched from the gateway.
        include_state_diff: false,
    }))
}

#[derive(Debug)]
pub enum TraceTransactionError {
    Internal(anyhow::Error),
    Custom(anyhow::Error),
    TxnHashNotFound,
    NoTraceAvailable(TraceError),
}

impl From<ExecutionStateError> for TraceTransactionError {
    fn from(value: ExecutionStateError) -> Self {
        match value {
            ExecutionStateError::BlockNotFound => Self::Custom(anyhow::anyhow!("Block not found")),
            ExecutionStateError::Internal(e) => Self::Internal(e),
        }
    }
}

impl From<TransactionExecutionError> for TraceTransactionError {
    fn from(value: TransactionExecutionError) -> Self {
        use TransactionExecutionError::*;
        match value {
            ExecutionError {
                transaction_index,
                error,
                error_stack: _,
            } => Self::Custom(anyhow::anyhow!(
                "Transaction execution failed at index {transaction_index}: {error}"
            )),
            Internal(e) => Self::Internal(e),
            Custom(e) => Self::Custom(e),
        }
    }
}

impl From<anyhow::Error> for TraceTransactionError {
    fn from(e: anyhow::Error) -> Self {
        Self::Internal(e)
    }
}

crate::error::impl_from_pending_read_error!(TraceTransactionError);

impl From<super::trace_block_transactions::TraceBlockTransactionsError> for TraceTransactionError {
    fn from(e: super::trace_block_transactions::TraceBlockTransactionsError) -> Self {
        use super::trace_block_transactions::TraceBlockTransactionsError::*;
        match e {
            Internal(e) => Self::Internal(e),
            BlockNotFound => Self::Custom(anyhow::anyhow!("Block not found")),
            Custom(e) => Self::Custom(e),
        }
    }
}

impl From<TraceTransactionError> for ApplicationError {
    fn from(value: TraceTransactionError) -> Self {
        match value {
            TraceTransactionError::TxnHashNotFound => ApplicationError::TxnHashNotFound,
            TraceTransactionError::NoTraceAvailable(status) => {
                ApplicationError::NoTraceAvailable(status)
            }
            TraceTransactionError::Internal(e) => ApplicationError::Internal(e),
            TraceTransactionError::Custom(e) => ApplicationError::Custom(e),
        }
    }
}

#[cfg(test)]
pub mod tests {

    use super::super::trace_block_transactions::tests::{
        setup_multi_tx_trace_pre_confirmed_test,
        setup_multi_tx_trace_pre_latest_test,
        setup_multi_tx_trace_test,
    };
    use super::{trace_transaction, Input, Output};
    use crate::dto::{SerializeForVersion, Serializer};
    use crate::RpcVersion;

    const RPC_VERSION: RpcVersion = RpcVersion::V09;

    #[tokio::test]
    async fn test_multiple_transactions() -> anyhow::Result<()> {
        let (context, _, traces) = setup_multi_tx_trace_test().await?;

        for trace in traces {
            let input = Input {
                transaction_hash: trace.transaction_hash,
            };
            let output = trace_transaction(context.clone(), input, RPC_VERSION)
                .await
                .unwrap();
            let expected = Output(crate::dto::TransactionTrace {
                trace: trace.trace_root,
                include_state_diff: false,
            });
            pretty_assertions_sorted::assert_eq!(
                output
                    .serialize(Serializer {
                        version: RPC_VERSION
                    })
                    .unwrap(),
                expected
                    .serialize(Serializer {
                        version: RPC_VERSION
                    })
                    .unwrap()
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_pre_latest_transactions() -> anyhow::Result<()> {
        let (context, traces) = setup_multi_tx_trace_pre_latest_test().await?;

        for trace in traces {
            let input = Input {
                transaction_hash: trace.transaction_hash,
            };
            let output = trace_transaction(context.clone(), input, RPC_VERSION)
                .await
                .unwrap();
            let expected = Output(crate::dto::TransactionTrace {
                trace: trace.trace_root,
                include_state_diff: false,
            });
            pretty_assertions_sorted::assert_eq!(
                output
                    .serialize(Serializer {
                        version: RPC_VERSION
                    })
                    .unwrap(),
                expected
                    .serialize(Serializer {
                        version: RPC_VERSION
                    })
                    .unwrap()
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_pre_confirmed_transactions() -> anyhow::Result<()> {
        let (context, traces) = setup_multi_tx_trace_pre_confirmed_test().await?;

        for trace in traces {
            let input = Input {
                transaction_hash: trace.transaction_hash,
            };
            let output = trace_transaction(context.clone(), input, RPC_VERSION)
                .await
                .unwrap();
            let expected = Output(crate::dto::TransactionTrace {
                trace: trace.trace_root,
                include_state_diff: false,
            });
            pretty_assertions_sorted::assert_eq!(
                output
                    .serialize(Serializer {
                        version: RPC_VERSION
                    })
                    .unwrap(),
                expected
                    .serialize(Serializer {
                        version: RPC_VERSION
                    })
                    .unwrap()
            );
        }

        Ok(())
    }
}
