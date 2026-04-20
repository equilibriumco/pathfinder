//! Defines [ApplicationError], the Starknet JSON-RPC specification's error
//! variants.
//!
//! In addition, it supplies the [generate_rpc_error_subset!] macro which should
//! be used by each JSON-RPC method to trivially create its subset of
//! [ApplicationError] along with the boilerplate involved.
#![macro_use]
use pathfinder_common::TransactionHash;
use serde_json::json;

#[derive(serde::Serialize, Clone, Copy, Debug)]
pub enum TraceError {
    Received,
    Rejected,
}

/// The Starknet JSON-RPC error variants.
#[derive(thiserror::Error, Debug)]
pub enum ApplicationError {
    #[error("Failed to write transaction")]
    FailedToReceiveTxn,
    #[error("Contract not found")]
    ContractNotFound,
    #[error("Requested entrypoint does not exist in the contract")]
    EntrypointNotFound,
    #[error("Block not found")]
    BlockNotFound,
    #[error("Invalid transaction index in a block")]
    InvalidTxnIndex,
    #[error("Invalid transaction hash")]
    InvalidTxnHash,
    #[error("Invalid block hash")]
    InvalidBlockHash,
    #[error("Class hash not found")]
    ClassHashNotFound,
    #[error("Transaction hash not found")]
    TxnHashNotFound,
    #[error("Requested page size is too big")]
    PageSizeTooBig,
    #[error("There are no blocks")]
    NoBlocks,
    #[error("No trace available")]
    NoTraceAvailable(TraceError),
    #[error("The supplied continuation token is invalid or unknown")]
    InvalidContinuationToken,
    #[error("Too many keys provided in a filter")]
    TooManyKeysInFilter { limit: usize, requested: usize },
    #[error("Contract error")]
    ContractError {
        revert_error: Option<String>,
        revert_error_stack: pathfinder_executor::ErrorStack,
    },
    #[error("Invalid contract class")]
    InvalidContractClass,
    #[error("Class already declared")]
    ClassAlreadyDeclared,
    #[error("Invalid transaction nonce")]
    InvalidTransactionNonce { data: String },
    #[error("The transaction's resources don't cover validation or the minimal transaction fee")]
    InsufficientResourcesForValidate,
    #[error(
        "Account balance is smaller than the transaction's maximal fee (calculated as the sum of \
         each resource's limit x max price)"
    )]
    InsufficientAccountBalance,
    #[error("Account validation failed")]
    ValidationFailure,
    #[error("Account validation failed")]
    ValidationFailureV06(String),
    #[error("Compilation failed")]
    CompilationFailed { data: String },
    #[error("Contract class size is too large")]
    ContractClassSizeIsTooLarge,
    #[error("Sender address is not an account contract")]
    NonAccount,
    #[error("A transaction with the same hash already exists in the mempool")]
    DuplicateTransaction,
    #[error("The compiled class hash did not match the one supplied in the transaction")]
    CompiledClassHashMismatch,
    #[error("The transaction version is not supported")]
    UnsupportedTxVersion,
    #[error("The contract class version is not supported")]
    UnsupportedContractClassVersion,
    #[error("An unexpected error occurred")]
    UnexpectedError { data: String },
    #[error("Too many storage keys requested")]
    ProofLimitExceeded { limit: u32, requested: u32 },
    #[error("The proof field in the invoke v3 transaction is invalid")]
    InvalidProof,
    #[error("Internal error")]
    GatewayError(starknet_gateway_types::error::StarknetError),
    /// Gateway HTTP errors whose status is forwarded.
    #[error("Internal error")]
    ForwardedError(reqwest::Error),
    #[error("Transaction execution error")]
    TransactionExecutionError {
        transaction_index: usize,
        error: String,
        error_stack: pathfinder_executor::ErrorStack,
    },
    #[error("Transaction hash not found in websocket subscription")]
    SubscriptionTransactionHashNotFound {
        subscription_id: u32,
        transaction_hash: TransactionHash,
    },
    #[error("Gateway is down")]
    SubscriptionGatewayDown { subscription_id: u32 },
    #[error("The node doesn't support storage proofs for blocks that are too far in the past")]
    StorageProofNotSupported,
    #[error("Proof is missing")]
    ProofMissing,
    #[error("Invalid subscription id")]
    InvalidSubscriptionID,
    #[error("Too many addresses in filter sender_address filter")]
    TooManyAddressesInFilter,
    #[error("Cannot go back more than {limit} blocks")]
    TooManyBlocksBack { limit: usize, requested: usize },
    /// Internal errors are errors whose details we don't want to show to the
    /// end user. These are logged, and a simple "internal error" message is
    /// shown to the end user.
    #[error("Internal error")]
    Internal(anyhow::Error),
    /// Custom errors are mostly treated as internal errors with the big
    /// difference that the error details aren't logged and are eventually
    /// displayed to the end user.
    #[error("Internal error")]
    Custom(anyhow::Error),
}

impl ApplicationError {
    pub fn code(&self, version: RpcVersion) -> i32 {
        match self {
            // Taken from the official starknet json rpc api.
            // https://github.com/starkware-libs/starknet-specs
            ApplicationError::FailedToReceiveTxn => 1,
            ApplicationError::NoTraceAvailable(_) => 10,
            ApplicationError::ContractNotFound => 20,
            ApplicationError::EntrypointNotFound => {
                if version >= RpcVersion::V08 {
                    21
                } else {
                    -32603 /* Custom - new code not available */
                }
            }
            ApplicationError::BlockNotFound => 24,
            ApplicationError::InvalidTxnHash => 25,
            ApplicationError::InvalidBlockHash => 26,
            ApplicationError::InvalidTxnIndex => 27,
            ApplicationError::ClassHashNotFound => 28,
            ApplicationError::TxnHashNotFound => 29,
            ApplicationError::PageSizeTooBig => 31,
            ApplicationError::NoBlocks => 32,
            ApplicationError::InvalidContinuationToken => 33,
            ApplicationError::TooManyKeysInFilter { .. } => 34,
            ApplicationError::ContractError { .. } => 40,
            ApplicationError::TransactionExecutionError { .. } => 41,
            ApplicationError::StorageProofNotSupported => 42,
            ApplicationError::InvalidContractClass => 50,
            ApplicationError::ClassAlreadyDeclared => 51,
            ApplicationError::InvalidTransactionNonce { .. } => 52,
            ApplicationError::InsufficientResourcesForValidate => 53,
            ApplicationError::InsufficientAccountBalance => 54,
            ApplicationError::ValidationFailure | ApplicationError::ValidationFailureV06(_) => 55,
            ApplicationError::CompilationFailed { .. } => 56,
            ApplicationError::ContractClassSizeIsTooLarge => 57,
            ApplicationError::NonAccount => 58,
            ApplicationError::DuplicateTransaction => 59,
            ApplicationError::CompiledClassHashMismatch => 60,
            ApplicationError::UnsupportedTxVersion => 61,
            ApplicationError::UnsupportedContractClassVersion => 62,
            ApplicationError::UnexpectedError { .. } => 63,
            ApplicationError::InvalidProof => 69,
            // specs/rpc/pathfinder_rpc_api.json
            ApplicationError::ProofLimitExceeded { .. } => 10000,
            ApplicationError::ProofMissing => 10001,
            ApplicationError::SubscriptionTransactionHashNotFound { .. } => 10029,
            ApplicationError::SubscriptionGatewayDown { .. } => 10030,
            // specs/rpc/starknet_ws_api.json
            ApplicationError::InvalidSubscriptionID => 66,
            ApplicationError::TooManyAddressesInFilter => 67,
            ApplicationError::TooManyBlocksBack { .. } => 68,
            // https://www.jsonrpc.org/specification#error_object
            ApplicationError::GatewayError(_)
            | ApplicationError::ForwardedError(_)
            | ApplicationError::Internal(_)
            | ApplicationError::Custom(_) => -32603,
        }
    }

    pub fn message(&self, version: RpcVersion) -> String {
        match self {
            ApplicationError::EntrypointNotFound => {
                if version >= RpcVersion::V08 {
                    self.to_string()
                } else {
                    "Invalid message selector".to_string()
                }
            }
            ApplicationError::InsufficientResourcesForValidate => match version {
                RpcVersion::V06 | RpcVersion::V07 => "Max fee is smaller than the minimal \
                                                      transaction cost (validation plus fee \
                                                      transfer)"
                    .to_string(),
                _ => self.to_string(),
            },
            _ => self.to_string(),
        }
    }

    pub fn data(&self, version: RpcVersion) -> Option<serde_json::Value> {
        // We purposefully don't use a catch-all branch to force us to update
        // here whenever a new variant is added. This will prevent adding a stateful
        // error variant but forgetting to forward its data.
        match self {
            ApplicationError::FailedToReceiveTxn => None,
            ApplicationError::ContractNotFound => None,
            ApplicationError::EntrypointNotFound => None,
            ApplicationError::BlockNotFound => None,
            ApplicationError::InvalidTxnIndex => None,
            ApplicationError::InvalidTxnHash => None,
            ApplicationError::InvalidBlockHash => None,
            ApplicationError::ClassHashNotFound => None,
            ApplicationError::TxnHashNotFound => None,
            ApplicationError::PageSizeTooBig => None,
            ApplicationError::NoBlocks => None,
            ApplicationError::InvalidContinuationToken => None,
            ApplicationError::InvalidContractClass => None,
            ApplicationError::ClassAlreadyDeclared => None,
            ApplicationError::InvalidTransactionNonce { data } => {
                if version >= RpcVersion::V09 {
                    Some(json!(data))
                } else {
                    None
                }
            }
            ApplicationError::InsufficientResourcesForValidate => None,
            ApplicationError::InsufficientAccountBalance => None,
            ApplicationError::ValidationFailure => None,
            ApplicationError::CompilationFailed { data } => match version {
                RpcVersion::V06 | RpcVersion::V07 => None,
                _ => Some(json!(data)),
            },
            ApplicationError::ContractClassSizeIsTooLarge => None,
            ApplicationError::NonAccount => None,
            ApplicationError::DuplicateTransaction => None,
            ApplicationError::CompiledClassHashMismatch => None,
            ApplicationError::UnsupportedTxVersion => None,
            ApplicationError::UnsupportedContractClassVersion => None,
            ApplicationError::InvalidSubscriptionID => None,
            ApplicationError::TooManyAddressesInFilter => None,
            ApplicationError::TooManyBlocksBack { limit, requested } => Some(json!({
                "limit": limit,
                "requested": requested,
            })),
            ApplicationError::GatewayError(error) => Some(json!({
                "error": error,
            })),
            ApplicationError::ForwardedError(error) => Some(json!({
                "error": error.to_string(),
            })),
            ApplicationError::TransactionExecutionError {
                transaction_index,
                error,
                error_stack,
            } => {
                if version >= RpcVersion::V08 {
                    let error_stack = error_stack_frames_to_json(&error_stack.0);
                    Some(json!({
                        "transaction_index": transaction_index,
                        "execution_error": error_stack,
                    }))
                } else {
                    Some(json!({
                        "transaction_index": transaction_index,
                        "execution_error": error,
                    }))
                }
            }
            ApplicationError::Internal(_) => None,
            ApplicationError::Custom(cause) => {
                let cause = cause.to_string();
                if cause.is_empty() {
                    None
                } else {
                    Some(json!({
                        "error": cause.to_string(),
                    }))
                }
            }
            ApplicationError::NoTraceAvailable(error) => Some(json!({
                "error": error,
            })),
            ApplicationError::ContractError {
                revert_error,
                revert_error_stack,
            } => {
                if version >= RpcVersion::V08 {
                    let revert_error_stack = error_stack_frames_to_json(&revert_error_stack.0);
                    Some(json!({
                        "revert_error": revert_error_stack
                    }))
                } else {
                    Some(json!({
                        "revert_error": revert_error
                    }))
                }
            }
            ApplicationError::TooManyKeysInFilter { limit, requested } => Some(json!({
                "limit": limit,
                "requested": requested,
            })),
            ApplicationError::UnexpectedError { data } => Some(json!(data)),
            ApplicationError::ProofLimitExceeded { limit, requested } => Some(json!({
                "limit": limit,
                "requested": requested,
            })),
            ApplicationError::InvalidProof => None,
            ApplicationError::StorageProofNotSupported => None,
            ApplicationError::ProofMissing => None,
            ApplicationError::SubscriptionTransactionHashNotFound {
                subscription_id,
                transaction_hash,
            } => Some(json!({
                "subscription_id": subscription_id,
                "transaction_hash": transaction_hash,
            })),
            ApplicationError::SubscriptionGatewayDown { subscription_id } => Some(json!({
                "subscription_id": subscription_id,
            })),
            ApplicationError::ValidationFailureV06(error) => Some(json!(error)),
        }
    }
}

// TODO: This can be safely removed once the temporary fetching methods are
// removed.
impl From<pathfinder_consensus_fetcher::ConsensusFetcherError> for ApplicationError {
    fn from(err: pathfinder_consensus_fetcher::ConsensusFetcherError) -> Self {
        use pathfinder_consensus_fetcher::ConsensusFetcherError::*;
        match err {
            Database(e) => ApplicationError::Internal(e),
            ContractCall(msg) => {
                if msg.contains("Contract not found") {
                    ApplicationError::ContractNotFound
                } else {
                    ApplicationError::Custom(anyhow::anyhow!(msg))
                }
            }
            NoValidators { .. } => ApplicationError::Custom(anyhow::anyhow!("No validators found")),
            NoProposers { .. } => ApplicationError::Custom(anyhow::anyhow!("No proposers found")),
            InvalidValidatorData(msg) => ApplicationError::Custom(anyhow::anyhow!(msg)),
            InvalidProposerData(msg) => ApplicationError::Custom(anyhow::anyhow!(msg)),
            BlockNotFound => ApplicationError::BlockNotFound,
            UnsupportedNetwork(chain_id) => {
                ApplicationError::Custom(anyhow::anyhow!("Unsupported network: {chain_id}"))
            }
        }
    }
}

// TODO: This can be safely removed once the temporary fetching methods are
// removed.
impl From<anyhow::Error> for ApplicationError {
    fn from(err: anyhow::Error) -> Self {
        ApplicationError::Internal(err)
    }
}

fn error_stack_frames_to_json(frames: &[pathfinder_executor::Frame]) -> serde_json::Value {
    let last_string_frame_contents = frames
        .iter()
        .rev()
        .filter_map(|frame| match frame {
            pathfinder_executor::Frame::StringFrame(string) => Some(string),
            _ => None,
        })
        .next()
        .cloned()
        .unwrap_or_else(|| "Unknown error, no string frame available.".to_string());

    let call_frames = frames.iter().filter_map(|frame| match frame {
        pathfinder_executor::Frame::CallFrame(call_frame) => Some(call_frame),
        _ => None,
    });
    call_frames
        .rev()
        .fold(json!(last_string_frame_contents), |child, frame| {
            json!({
                "contract_address": frame.storage_address,
                "class_hash": frame.class_hash,
                "selector": frame.selector,
                "error": child,
            })
        })
}

/// Generates an enum subset of [ApplicationError] along with boilerplate for
/// mapping the variants back to [ApplicationError].
///
/// This is useful for RPC methods which only emit a few of the
/// [ApplicationError] variants as this macro can be used to quickly create the
/// enum-subset with the required glue code. This greatly improves the type
/// safety of the method.
///
/// ## Usage
/// ```ignore
/// generate_rpc_error_subset!(<enum_name>: <variant a>, <variant b>, <variant N>);
/// ```
/// Note that the variants __must__ match the [ApplicationError] variant names
/// and that [ApplicationError::Internal] and [ApplicationError::Custom] are
/// always included by default (and therefore should not be part of macro
/// input).
///
/// An `Internal` only variant can be generated using
/// `generate_rpc_error_subset!(<enum_name>)`.
///
/// ## Specifics
/// This macro generates the following:
///
/// 1. New enum definition with `#[derive(Debug)]`
/// 2. `Internal(anyhow::Error)` and `Custom(anyhow::Error)` variants
/// 3. `impl From<NewEnum> for RpcError`
/// 4. `impl From<anyhow::Error> for NewEnum`, mapping to the `Internal` variant
///
/// ## Example with expansion
/// This macro invocation:
/// ```ignore
/// generate_rpc_error_subset!(MyEnum: BlockNotFound, NoBlocks);
/// ```
/// expands to:
/// ```ignore
/// #[derive(debug)]
/// pub enum MyError {
///     BlockNotFound,
///     NoBlocks,
///     /// See [`crate::error::ApplicationError::Internal`]
///     Internal(anyhow::Error),
///     /// See [`crate::error::ApplicationError::Custom`]
///     Custom(anyhow::Error),
/// }
///
/// impl From<MyError> for RpcError {
///     fn from(x: MyError) -> Self {
///         match x {
///             MyError::BlockNotFound => Self::BlockNotFound,
///             MyError::NoBlocks => Self::NoBlocks,
///             MyError::Internal(internal) => Self::Internal(internal),
///         }
///     }
/// }
///
/// impl From<anyhow::Error> for MyError {
///     fn from(e: anyhow::Error) -> Self {
///         Self::Internal(e)
///     }
/// }
/// ```
#[allow(unused_macros)]
macro_rules! generate_rpc_error_subset {
    // This macro uses the following advanced techniques:
    //   - tt-muncher (https://danielkeep.github.io/tlborm/book/pat-incremental-tt-munchers.html)
    //   - push-down-accumulation (https://danielkeep.github.io/tlborm/book/pat-push-down-accumulation.html)
    //
    // All macro arms (except the entry-point) begin with `@XXX` to prevent accidental usage.
    //
    // It is possible to allow for custom `#[derive()]` and other attributes. These are currently not required
    // and therefore not implemented.

    // Entry-point for empty variant (with colon suffix)
    ($enum_name:ident:) => {
        generate_rpc_error_subset!($enum_name);
    };
    // Entry-point for empty variant (without colon suffix)
    ($enum_name:ident) => {
        generate_rpc_error_subset!(@enum_def, $enum_name,);
        generate_rpc_error_subset!(@from_anyhow, $enum_name);
        generate_rpc_error_subset!(@from_def, $enum_name,);
    };
    // Main entry-point for the macro
    ($enum_name:ident: $($subset:tt),+) => {
        generate_rpc_error_subset!(@enum_def, $enum_name, $($subset),+);
        generate_rpc_error_subset!(@from_anyhow, $enum_name);
        generate_rpc_error_subset!(@from_def, $enum_name, $($subset),+);
    };
    // Generates the enum definition, nothing tricky here.
    (@enum_def, $enum_name:ident, $($subset:tt),*) => {
        #[derive(Debug)]
        pub enum $enum_name {
            /// See [`crate::error::ApplicationError::Internal`]
            Internal(anyhow::Error),
            /// See [`crate::error::ApplicationError::Custom`]
            Custom(anyhow::Error),
            $($subset),*
        }
    };
    // Generates From<anyhow::Error>, nothing tricky here.
    (@from_anyhow, $enum_name:ident) => {
        impl From<anyhow::Error> for $enum_name {
            fn from(e: anyhow::Error) -> Self {
                Self::Internal(e)
            }
        }
    };
    // Generates From<$enum_name> for RpcError, this macro arm itself is not tricky,
    // however its child calls are.
    //
    // We pass down the variants, which will get tt-munched until we reach the base case.
    // We also initialize the match "arms" (`{}`) as empty. These will be accumulated until
    // we reach the base case at which point the match itself is generated using the arms.
    //
    // We cannot generate the match in this macro arm as each macro invocation must result in
    // a valid AST (I think). This means that having the match at this level would require the
    // child calls only return arm(s) -- which is not valid syntax by itself. Instead, the invalid
    // Rust is "pushed-down" as input to the child call -- instead of being the output (illegal).
    //
    // By pushing the arms from this level downwards, and creating the match statement at the lowest
    // level, we guarantee that only valid valid Rust will bubble back up.
    (@from_def, $enum_name:ident, $($variants:ident),*) => {
        impl From<$enum_name> for crate::error::ApplicationError {
            fn from(x: $enum_name) -> Self {
                generate_rpc_error_subset!(@parse, x, $enum_name, {}, $($variants),*)
            }
        }
    };
    // Termination case (no further input to munch). We generate the match statement here.
    (@parse, $var:ident, $enum_name:ident, {$($arms:tt)*}, $(,)*) => {
        match $var {
            $($arms)*
            $enum_name::Internal(internal) => Self::Internal(internal),
            $enum_name::Custom(error) => Self::Custom(error),
        }
    };
    // Special case for single variant. This could probably be folded into one of the other
    // cases but I struggled to do so correctly.
    (@parse, $var:ident, $enum_name:ident, {$($arms:tt)*}, $variant:ident) => {
        generate_rpc_error_subset!(
            @parse, $var, $enum_name,
            {
                $($arms)*
                $enum_name::$variant => Self::$variant,
            },
        )
    };
    // Append this variant to arms. Continue parsing the remaining variants.
    (@parse, $var:ident, $enum_name:ident, {$($arms:tt)*}, $variant:ident, $($tail:ident),*) => {
        generate_rpc_error_subset!(
            @parse, $var, $enum_name,
            {
                $($arms)*
                $enum_name::$variant => Self::$variant,
            },
            $($tail),*
        )
    };
}

#[allow(dead_code, unused_imports)]
pub(super) use generate_rpc_error_subset;

use crate::RpcVersion;

#[cfg(test)]
mod tests {
    mod rpc_error_subset {
        use assert_matches::assert_matches;

        use super::super::ApplicationError;

        #[test]
        fn no_variant() {
            generate_rpc_error_subset!(Empty:);
            generate_rpc_error_subset!(EmptyNoColon);
        }

        #[test]
        fn single_variant() {
            generate_rpc_error_subset!(Single: ContractNotFound);

            let original = ApplicationError::from(Single::ContractNotFound);

            assert_matches!(original, ApplicationError::ContractNotFound);
        }

        #[test]
        fn multi_variant() {
            generate_rpc_error_subset!(Multi: ContractNotFound, NoBlocks);

            let contract_not_found = ApplicationError::from(Multi::ContractNotFound);
            let no_blocks = ApplicationError::from(Multi::NoBlocks);

            assert_matches!(contract_not_found, ApplicationError::ContractNotFound);
            assert_matches!(no_blocks, ApplicationError::NoBlocks);
        }
    }

    mod error_stack {
        use pathfinder_common::{class_hash, contract_address, entry_point};
        use pathfinder_executor::{CallFrame, Frame};
        use serde_json::json;

        use super::super::error_stack_frames_to_json;

        #[test]
        fn json_representation() {
            let frames = vec![
                // Top-level call
                Frame::CallFrame(CallFrame {
                    storage_address: contract_address!("0xdeadbeef"),
                    class_hash: class_hash!("0xcaadd"),
                    selector: Some(entry_point!("0xeeeee")),
                }),
                // An interim string representation of the in-contract call trace
                Frame::StringFrame(
                    "Error at pc=0:4273:\nCairo traceback (most recent call last):\nUnknown \
                     location (pc=0:67)\nUnknown location (pc=0:1997)\nUnknown location \
                     (pc=0:2727)\nUnknown location (pc=0:3582)\n"
                        .to_string(),
                ),
                // Another call
                Frame::CallFrame(CallFrame {
                    storage_address: contract_address!("0x2222deadbeef"),
                    class_hash: class_hash!("0x2222caadd"),
                    selector: Some(entry_point!("0x2222eeeee")),
                }),
                // An interim string representation of the in-contract call trace
                Frame::StringFrame(
                    "Error at pc=0:4273:\nCairo traceback (most recent call last):\nUnknown \
                     location (pc=0:67)\nUnknown location (pc=0:1997)\nUnknown location \
                     (pc=0:2727)\nUnknown location (pc=0:3582)\n"
                        .to_string(),
                ),
                // Final error
                Frame::StringFrame("test".to_string()),
            ];

            assert_eq!(
                error_stack_frames_to_json(&frames),
                json!({
                    "contract_address": "0xdeadbeef",
                    "class_hash": "0xcaadd",
                    "selector": "0xeeeee",
                    "error": json!({
                        "contract_address": "0x2222deadbeef",
                        "class_hash": "0x2222caadd",
                        "selector": "0x2222eeeee",
                        "error": "test",
                    }),
                })
            );
        }
    }
}
