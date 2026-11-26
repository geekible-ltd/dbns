//! Shared error and result types for the NDB workspace.

use thiserror::Error;

/// Standard result type used across NDB crates.
pub type Result<T> = std::result::Result<T, Error>;

/// Top-level error type.
///
/// Crates with substantial internal error surface should define their own
/// error enum and convert into this at subsystem boundaries.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// A persisted structure failed checksum or format validation.
    #[error("corruption detected: {0}")]
    Corruption(String),

    /// The requested entity does not exist.
    #[error("not found: {0}")]
    NotFound(String),

    /// Input failed validation.
    #[error("invalid input: {0}")]
    InvalidInput(String),

    /// Configuration loading or validation failed.
    #[error("configuration error: {0}")]
    Config(String),

    /// Transaction lifecycle error (conflict, timeout, invalid state).
    #[error("transaction error: {0}")]
    Transaction(String),

    /// Storage engine failure.
    #[error("storage error: {0}")]
    Storage(String),

    /// Search subsystem failure.
    #[error("search error: {0}")]
    Search(String),

    /// Authentication or authorisation failure.
    #[error("security error: {0}")]
    Security(String),

    /// Cluster or replication failure.
    #[error("cluster error: {0}")]
    Cluster(String),

    /// Wraps [`std::io::Error`] for lower-level I/O failures.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// Catch-all for errors that do not fit a more specific variant.
    #[error("internal error: {0}")]
    Internal(String),
}
