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
pub use swarm::{new_swarm, EdgeOrchSwarm, SwarmCommand, SwarmConfig, SwarmHandle};
