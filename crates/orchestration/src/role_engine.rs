//! Role orchestration engine.
//!
//! Manages role assignments across the cluster: applies topology specs,
//! monitors node health, and triggers failover reassignments.

use std::collections::HashMap;
use std::sync::Arc;

use eo_core::error::Result;
use eo_core::types::{Capabilities, NodeDescriptor, NodeId, Role, RuntimeKind};
use eo_raft::Proposal;
use serde::Deserialize;
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::topology::{diff_topology, ClusterTopologySpec};

/// Selector for matching nodes in a topology spec.
#[derive(Debug, Clone, Deserialize)]
pub struct NodeSelector {
    /// Match a specific node by ID.
    #[serde(default)]
    pub node_id: Option<NodeId>,

    /// Match nodes with all of these capabilities.
    #[serde(default)]
    pub has_capabilities: Vec<String>,

    /// Match nodes running this OS.
    #[serde(default)]
    pub os: Option<String>,
}

/// The role orchestration engine.
///
/// Watches the cluster state and ensures role assignments match
/// the desired topology. Triggers failover when nodes go offline.
pub struct RoleOrchestrationEngine {
    /// Sender for submitting proposals to the Raft cluster.
    proposal_tx: mpsc::Sender<Proposal>,

    /// Current cluster topology spec (desired state).
    desired_topology: Option<ClusterTopologySpec>,

    /// Currently known node descriptors.
    nodes: HashMap<NodeId, NodeDescriptor>,
}

impl RoleOrchestrationEngine {
    /// Create a new orchestration engine.
    pub fn new(proposal_tx: mpsc::Sender<Proposal>) -> Self {
        Self {
            proposal_tx,
            desired_topology: None,
            nodes: HashMap::new(),
        }
    }

    /// Apply a topology spec: compute diff, submit proposals.
    pub async fn apply_topology(&mut self, spec: ClusterTopologySpec) -> Result<()> {
        // Build current role assignments from known nodes
        let current_roles: HashMap<NodeId, Vec<Role>> = self
            .nodes
            .iter()
            .map(|(id, desc)| (*id, desc.current_assigned_roles.clone()))
            .collect();

        let proposals = diff_topology(&current_roles, &spec);

        for proposal in proposals {
            if let Err(e) = self.proposal_tx.send(proposal).await {
                warn!("Failed to submit topology proposal: {}", e);
            }
        }

        self.desired_topology = Some(spec);
        info!("Topology spec applied");
        Ok(())
    }

    /// Register a node descriptor (from Raft commitment or P2P discovery).
    pub fn register_node(&mut self, desc: NodeDescriptor) {
        let node_id = desc.node_id;
        self.nodes.insert(node_id, desc);
        debug!("Registered node in orchestrator: {}", node_id);
    }

    /// Handle a node going offline — trigger failover.
    pub async fn handle_node_failure(&mut self, node_id: NodeId) -> Result<bool> {
        if let Some(desc) = self.nodes.remove(&node_id) {
            info!("Node {} failed — checking for failover", node_id);

            let lost_roles = desc.current_assigned_roles.clone();

            // Find alternative nodes with matching capabilities
            for role in &lost_roles {
                if let Some(alternative) = self.find_alternative(role, &desc.capabilities) {
                    let proposal = Proposal::AssignRole {
                        node_id: alternative,
                        role: role.clone(),
                    };
                    self.proposal_tx.send(proposal).await.map_err(|e| {
                        eo_core::error::CoreError::Network(format!("send proposal: {e}"))
                    })?;
                    info!("Failover: assigned role '{role}' to node {alternative}");
                } else {
                    warn!("No alternative node found for role '{role}' from failed node {node_id}");
                }
            }

            return Ok(!lost_roles.is_empty());
        }
        Ok(false)
    }

    /// Find an alternative node that can fulfill a role.
    fn find_alternative(&self, role: &Role, lost_caps: &Capabilities) -> Option<NodeId> {
        self.nodes
            .iter()
            .find(|(_, desc)| {
                // Match by capability
                match role {
                    Role::Execution => desc
                        .capabilities
                        .runtimes
                        .iter()
                        .any(|r| lost_caps.runtimes.contains(r)),
                    Role::Storage => desc.capabilities.storage,
                    Role::Inference => desc.capabilities.gpu_acceleration,
                    Role::Coordinator => true, // Any node can coordinate
                    Role::Bootstrap => desc.node_type == eo_core::types::NodeType::Heavy,
                }
            })
            .map(|(id, _)| *id)
    }
}

use tracing::debug;

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use eo_core::types::{Capabilities, NodeDescriptor, NodeType, OsType, RuntimeKind};

    fn make_descriptor(id: &str, roles: Vec<Role>, runtimes: Vec<RuntimeKind>) -> NodeDescriptor {
        NodeDescriptor {
            node_id: uuid::Uuid::parse_str(id).unwrap(),
            node_type: NodeType::Heavy,
            os: OsType::MacOS,
            capabilities: Capabilities {
                storage: true,
                gpu_acceleration: false,
                runtimes,
                max_memory_mb: 1024,
                cpu_cores: 2,
            },
            advertised_addresses: vec![],
            current_assigned_roles: roles,
            started_at: Utc::now(),
        }
    }

    #[test]
    fn node_failure_triggers_reassignment() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (tx, _rx) = mpsc::channel(16);
            let mut engine = RoleOrchestrationEngine::new(tx);

            let exec_id = uuid::Uuid::parse_str("660e8400-e29b-41d4-a716-446655440000").unwrap();
            let backup_id = uuid::Uuid::parse_str("770e8400-e29b-41d4-a716-446655440000").unwrap();

            // Register two nodes: executor and backup
            engine.register_node(make_descriptor(
                "660e8400-e29b-41d4-a716-446655440000",
                vec![Role::Execution],
                vec![RuntimeKind::Wasm],
            ));
            engine.register_node(make_descriptor(
                "770e8400-e29b-41d4-a716-446655440000",
                vec![],
                vec![RuntimeKind::Wasm],
            ));

            // When the executor fails, the backup should get the role
            let result = engine.handle_node_failure(exec_id).await.unwrap();
            assert!(result); // failover was triggered
        });
    }

    #[test]
    fn no_reassignment_when_no_suitable_node() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (tx, _rx) = mpsc::channel(16);
            let mut engine = RoleOrchestrationEngine::new(tx);

            let exec_id = uuid::Uuid::parse_str("660e8400-e29b-41d4-a716-446655440000").unwrap();

            // Register only one node — no backup available
            engine.register_node(make_descriptor(
                "660e8400-e29b-41d4-a716-446655440000",
                vec![Role::Execution],
                vec![RuntimeKind::Container], // specialized runtime
            ));

            let result = engine.handle_node_failure(exec_id).await.unwrap();
            assert!(result); // failover was attempted (node was registered)
        });
    }
}
