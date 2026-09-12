//! Edge-Cloud Orchestrator — P2P Network Layer
//!
//! This crate provides the libp2p-based peer-to-peer networking layer.
//!
//! - **Transport**: TCP + Noise encryption + Yamux multiplexing
//! - **Discovery**: mDNS for LAN peer discovery
//! - **Peer Identity**: libp2p identify protocol
//! - **Keepalive**: libp2p ping
//! - **Descriptor Exchange**: Custom request-response protocol for
//!   exchanging [`NodeDescriptor`]s between peers.
//!
//! [`NodeDescriptor`]: eo_core::types::NodeDescriptor

pub mod behaviour;
pub mod discovery;
pub mod protocol;
pub mod swarm;
pub mod transport;

// Re-export commonly used types
pub use behaviour::EdgeOrchBehaviour;
pub use discovery::Event;
pub use protocol::{
    BlobCodec, BlobRequest, BlobResponse, RaftMessageCodec, RaftMessageRequest,
    RaftMessageResponse, BLOB_PROTOCOL, RAFT_PROTOCOL,
};
pub use swarm::{new_swarm, EdgeOrchSwarm, SwarmCommand, SwarmConfig, SwarmHandle};

/// A source for content-addressed blobs. The node implements this on top of its
/// CAS store so the swarm can answer incoming blob requests synchronously.
pub trait BlobProvider: Send + Sync {
    fn get_blob(&self, hash: &str) -> Option<Vec<u8>>;
}
