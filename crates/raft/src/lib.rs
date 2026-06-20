//! Edge-Cloud Orchestrator — Raft Consensus Layer
//!
//! Implements distributed consensus using tikv/raft-rs.

pub mod network;
pub mod node;
pub mod proposal;
pub mod state_machine;
pub mod storage;

pub use network::{create_raft_transport, Libp2pRaftTransport, PeerRegistry};
pub use node::RaftNode;
pub use proposal::Proposal;
pub use state_machine::ClusterState;
pub use storage::CasRaftStorage;
