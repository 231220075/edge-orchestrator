//! Unified error types for the Edge-Cloud Orchestrator.
//!
//! Uses `thiserror` to define structured error enums that compose
//! across crate boundaries. Library crates return `CoreError` (or
//! their own crate-specific error wrapping it); the binary uses
//! `anyhow` for top-level error reporting.

use thiserror::Error;

/// Top-level result type used across all crates.
pub type Result<T> = std::result::Result<T, CoreError>;

/// Unified error type for the edge-orchestrator.
///
/// Each variant corresponds to a subsystem that can fail. The
/// `#[from]` attribute enables automatic conversion with `?`.
#[derive(Error, Debug)]
pub enum CoreError {
    // ------------------------------------------------------------------
    // Network / P2P errors
    // ------------------------------------------------------------------
    /// Failed to establish or maintain a P2P connection.
    #[error("network error: {0}")]
    Network(String),

    /// Peer was not found in the local peer table.
    #[error("peer not found: {0}")]
    PeerNotFound(String),

    /// Timed out waiting for a response from a peer.
    #[error("peer timeout: {0}")]
    PeerTimeout(String),

    /// Failed to start or configure the libp2p swarm.
    #[error("swarm error: {0}")]
    Swarm(String),

    // ------------------------------------------------------------------
    // Storage / CAS errors
    // ------------------------------------------------------------------
    /// Object not found in the content-addressed store.
    #[error("object not found: {0}")]
    ObjectNotFound(String),

    /// Hash verification failed (data doesn't match expected hash).
    #[error("hash mismatch: expected {expected}, computed {computed}")]
    HashMismatch {
        /// The expected hash.
        expected: String,
        /// The hash computed from the data.
        computed: String,
    },

    /// I/O error during storage operations.
    #[error("storage I/O error: {0}")]
    StorageIo(String),

    /// Garbage collection or packfile corruption.
    #[error("storage corruption: {0}")]
    StorageCorruption(String),

    // ------------------------------------------------------------------
    // Raft / Consensus errors
    // ------------------------------------------------------------------
    /// Raft consensus protocol error.
    #[error("raft error: {0}")]
    Raft(String),

    /// Proposal was rejected (e.g., by validation).
    #[error("proposal rejected: {0}")]
    ProposalRejected(String),

    /// Not the current leader; forward to leader.
    #[error("not leader — leader is {0:?}")]
    NotLeader(Option<u64>),

    // ------------------------------------------------------------------
    // Sandbox / Execution errors
    // ------------------------------------------------------------------
    /// Sandbox execution failed.
    #[error("sandbox execution error: {0}")]
    SandboxExecution(String),

    /// Resource limit exceeded during sandbox execution.
    #[error("resource limit exceeded: {0}")]
    ResourceLimitExceeded(String),

    /// The requested runtime kind is not supported on this platform.
    #[error("unsupported runtime: {0}")]
    UnsupportedRuntime(String),

    /// The requested sandbox kind is not supported on this platform.
    #[error("unsupported platform for sandbox: {0}")]
    UnsupportedPlatform(String),

    // ------------------------------------------------------------------
    // Configuration errors
    // ------------------------------------------------------------------
    /// Configuration file is invalid or missing.
    #[error("configuration error: {0}")]
    Configuration(String),

    /// Invalid argument or state transition.
    #[error("invalid state: {0}")]
    InvalidState(String),

    // ------------------------------------------------------------------
    // Serialization errors
    // ------------------------------------------------------------------
    /// Failed to serialize or deserialize data.
    #[error("serialization error: {0}")]
    Serialization(String),

    // ------------------------------------------------------------------
    // Internal / catch-all
    // ------------------------------------------------------------------
    /// An internal error that should not normally occur.
    #[error("internal error: {0}")]
    Internal(String),
}

// Convenience conversions from common external error types.

impl From<std::io::Error> for CoreError {
    fn from(e: std::io::Error) -> Self {
        CoreError::StorageIo(e.to_string())
    }
}

impl From<serde_json::Error> for CoreError {
    fn from(e: serde_json::Error) -> Self {
        CoreError::Serialization(e.to_string())
    }
}

impl From<uuid::Error> for CoreError {
    fn from(e: uuid::Error) -> Self {
        CoreError::InvalidState(format!("invalid UUID: {e}"))
    }
}
