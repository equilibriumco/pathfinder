//! Error types for proposal handling.

use p2p::consensus::HeightAndRound;
use pathfinder_storage::StorageError;

use crate::WrongValidatorStageError;

/// Errors that can occur when handling incoming proposal parts.
///
/// This enum wraps all possible error types, with explicit classification:
/// - `ProposalError` is always recoverable (from peers)
/// - `StorageError` is always fatal (DB errors, connection failures, etc.)
/// - `TransactionExecutionError` is always recoverable (malformed/invalid
///   transactions)
/// - Other errors require explicit classification via `.fatal()` or
///   `.recoverable()`
///
/// Note: We do NOT implement From<anyhow::Error> to avoid making assumptions
/// about error classification. Use ProposalHandlingError::fatal() or
/// ProposalHandlingError::recoverable() explicitly, or use the specific From
/// implementations for known error types.
#[derive(Debug, thiserror::Error)]
pub enum ProposalHandlingError {
    /// Recoverable error from peer data (malformed proposals, out-of-order
    /// parts, validation failures, execution errors).
    #[error(transparent)]
    Recoverable(#[from] ProposalError),

    /// Fatal error (DB errors, state corruption, storage failures, logic
    /// errors, etc.).
    #[error(transparent)]
    Fatal(#[from] anyhow::Error),
}

impl ProposalHandlingError {
    /// Check if this error is recoverable.
    pub fn is_recoverable(&self) -> bool {
        matches!(self, Self::Recoverable(_))
    }

    /// Check if this error is fatal.
    pub fn is_fatal(&self) -> bool {
        matches!(self, Self::Fatal(_))
    }

    /// Get the error message for logging.
    pub fn error_message(&self) -> String {
        match self {
            Self::Recoverable(e) => format!("{e}"),
            Self::Fatal(e) => format!("{e:#}"),
        }
    }

    /// Create a fatal error explicitly.
    pub fn fatal(error: impl Into<anyhow::Error>) -> Self {
        Self::Fatal(error.into())
    }

    /// Create a recoverable error from an error.
    ///
    /// This checks if the error is actually a storage error (fatal) and
    /// extracts the full error chain as a message otherwise.
    pub fn recoverable(error: impl Into<anyhow::Error>) -> Self {
        let err = error.into();
        // Check if it's actually a storage error (shouldn't happen, but be safe)
        if is_storage_error(&err) {
            Self::Fatal(err)
        } else {
            // Extract the full error chain as a message
            Self::Recoverable(ProposalError::ValidationFailed {
                message: format!("{:#}", err),
            })
        }
    }

    /// Create a recoverable error from a message string.
    ///
    /// Use this when you have a simple message string and don't need to
    /// preserve an error chain.
    pub fn recoverable_msg(msg: impl Into<String>) -> Self {
        Self::Recoverable(ProposalError::ValidationFailed {
            message: msg.into(),
        })
    }
}

impl From<pathfinder_storage::StorageError> for ProposalHandlingError {
    fn from(value: pathfinder_storage::StorageError) -> Self {
        // StorageError is always fatal (DB errors, connection failures, etc.)
        Self::Fatal(anyhow::Error::from(value))
    }
}

impl From<pathfinder_executor::TransactionExecutionError> for ProposalHandlingError {
    fn from(error: pathfinder_executor::TransactionExecutionError) -> Self {
        // Execution errors are recoverable (malformed/invalid transactions)
        Self::Recoverable(ProposalError::ValidationFailed {
            message: format!("{}", error),
        })
    }
}

/// Check if `StorageError` appears anywhere in the error chain.
fn is_storage_error(error: &anyhow::Error) -> bool {
    // Check the root error
    if error.downcast_ref::<StorageError>().is_some() {
        return true;
    }

    // Walk the error chain
    let mut current: Option<&dyn std::error::Error> = error.source();
    while let Some(err) = current {
        if err.downcast_ref::<StorageError>().is_some() {
            return true;
        }
        current = err.source();
    }

    false
}

/// Errors that can occur when handling incoming proposal parts.
///
/// These errors are classified as recoverable (from peers) or fatal (our
/// state).
#[derive(Debug, thiserror::Error)]
pub enum ProposalError {
    /// Unexpected proposal part received (e.g., Init when expecting BlockInfo).
    #[error("Unexpected proposal part: {message}")]
    UnexpectedProposalPart { message: String },

    /// Validator stage not found in cache.
    #[error("No ValidatorStage for height and round {0}")]
    ValidatorStageNotFound(HeightAndRound),

    /// Wrong validator stage type (e.g., expected BlockInfo but got
    /// TransactionBatch).
    #[error("Wrong validator stage: {message}")]
    WrongValidatorStage { message: String },

    /// Execution or validation failed due to proposal content (malformed
    /// transactions, invalid commitments, hash mismatches, etc.).
    #[error("Validation/execution failed: {message}")]
    ValidationFailed { message: String },
}

impl From<WrongValidatorStageError> for ProposalError {
    fn from(err: WrongValidatorStageError) -> Self {
        ProposalError::WrongValidatorStage {
            message: format!("{err}"),
        }
    }
}

impl From<WrongValidatorStageError> for ProposalHandlingError {
    fn from(err: WrongValidatorStageError) -> Self {
        Self::Recoverable(err.into())
    }
}
