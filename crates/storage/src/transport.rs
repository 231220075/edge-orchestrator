//! P2P object fetch/push transport.
//!
//! Protocol: `/edge-orch/storage/1.0.0`
//!
//! This is a stub that will be fully implemented in Task 3.4
//! when the storage service is built on top of the P2P layer.

use eo_core::error::Result;
use eo_core::types::Hash;

/// Transport for fetching and pushing objects over the P2P network.
///
/// This is a placeholder that will be implemented when the
/// storage service is integrated with the P2P layer in Phase 3.
#[derive(Debug, Clone)]
pub struct P2pObjectTransport {
    /// Placeholder: will hold an mpsc sender to the swarm task.
    _private: (),
}

impl P2pObjectTransport {
    /// Create a new transport (stub — not yet connected to P2P).
    pub fn new() -> Self {
        Self { _private: () }
    }

    /// Fetch an object by hash from the P2P network.
    ///
    /// First checks local store, then queries peers.
    /// Stub: always returns `ObjectNotFound`.
    #[allow(unused_variables)]
    pub async fn fetch(&self, hash: &Hash) -> Result<Vec<u8>> {
        Err(eo_core::error::CoreError::ObjectNotFound(format!(
            "P2P transport not yet implemented — hash: {hash}"
        )))
    }

    /// Announce that we have a new object available.
    #[allow(unused_variables)]
    pub async fn announce(&self, hash: &Hash) {
        // Stub: no-op until P2P integration
        tracing::debug!("P2P announce stub: {hash}");
    }
}

impl Default for P2pObjectTransport {
    fn default() -> Self {
        Self::new()
    }
}
