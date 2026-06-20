//! Edge-Cloud Orchestrator — Content-Addressed Storage
//!
//! Git-model content-addressed storage with:
//!
//! - **Objects**: Blob, Tree, Commit, Tag with SHA-256 content hashing
//! - **Store**: Local filesystem-backed object store (zlib-compressed JSON)
//! - **Index**: Packfile index for efficient batch operations
//! - **Diff**: Tree diffing for state synchronization
//! - **Transport**: P2P object fetch/push (stub — Phase 3)

pub mod diff;
pub mod index;
pub mod objects;
pub mod store;
pub mod transport;

// Re-export commonly used types
pub use objects::{hash_blob, hash_commit, hash_tree, Blob, Commit, Object, Tag, Tree};
pub use store::LocalObjectStore;
