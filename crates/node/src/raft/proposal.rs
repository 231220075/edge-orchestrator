//! Proposal types for the Raft state machine.
//!
//! Proposals are the operations that can be submitted to the Raft cluster
//! for replication and commitment.
//!
//! Task submission/assignment/completion variants were removed with the Wasm
//! lane; see `docs/v3-modules/22-wasm-lane移除记录.md`.

use eo_core::types::{NodeDescriptor, NodeId, Role};
use serde::{Deserialize, Serialize};

/// A proposal submitted to the Raft cluster for consensus.
///
/// Each variant represents an operation that modifies the cluster state.
/// When a proposal is committed through Raft, it is applied to the
/// [`ClusterState`].
///
/// [`ClusterState`]: crate::state_machine::ClusterState
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Proposal {
    /// Register a new node (or update an existing one).
    RegisterNode(NodeDescriptor),

    /// Remove a node from the cluster.
    DeregisterNode(NodeId),

    /// Assign a role to a node.
    AssignRole {
        /// The node to assign the role to.
        node_id: NodeId,
        /// The role to assign.
        role: Role,
    },

    /// Revoke a role from a node.
    RevokeRole {
        /// The node to revoke the role from.
        node_id: NodeId,
        /// The role to revoke.
        role: Role,
    },
}

impl Proposal {
    /// Encode a proposal to JSON bytes for inclusion in a Raft log entry.
    pub fn encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    /// Decode a proposal from JSON bytes.
    pub fn decode(data: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(data)
    }
}

/// The result of applying a proposal to the state machine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ApplyResult {
    /// Proposal applied successfully.
    Ok,
    /// Node was registered.
    NodeRegistered(NodeId),
    /// Node was deregistered.
    NodeDeregistered(NodeId),
    /// Role was assigned.
    RoleAssigned { node_id: NodeId, role: Role },
    /// Role was revoked.
    RoleRevoked { node_id: NodeId, role: Role },
    /// Proposal was rejected (invalid state transition).
    Rejected(String),
}
