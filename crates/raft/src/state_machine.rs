//! Raft state machine: maintains the cluster's replicated state.
//!
//! The state machine holds:
//! 1. **Node Registry**: Which nodes exist and what they can do
//! 2. **Role Assignments**: What each node is currently assigned to do
//! 3. **Task Queue**: Pending and completed tasks

use std::collections::{HashMap, VecDeque};

use eo_core::types::{Hash, NodeDescriptor, NodeId, Role, ScheduledTask, TaskId};
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use crate::proposal::{ApplyResult, Proposal};

/// The replicated cluster state maintained by the Raft state machine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterState {
    /// Registry of all known nodes.
    pub nodes: HashMap<NodeId, NodeDescriptor>,

    /// Current role assignments: node → roles.
    pub role_assignments: HashMap<NodeId, Vec<Role>>,

    /// Pending tasks waiting to be scheduled.
    pub task_queue: VecDeque<ScheduledTask>,

    /// Completed tasks: task_id → result hash.
    pub completed_tasks: HashMap<TaskId, Hash>,

    /// The last applied Raft log index.
    pub last_applied_index: u64,
}

impl Default for ClusterState {
    fn default() -> Self {
        Self {
            nodes: HashMap::new(),
            role_assignments: HashMap::new(),
            task_queue: VecDeque::new(),
            completed_tasks: HashMap::new(),
            last_applied_index: 0,
        }
    }
}

impl ClusterState {
    /// Apply a committed proposal to the state machine.
    pub fn apply(&mut self, proposal: Proposal) -> ApplyResult {
        match proposal {
            Proposal::RegisterNode(desc) => {
                let node_id = desc.node_id;
                let is_new = !self.nodes.contains_key(&node_id);
                self.nodes.insert(node_id, desc);
                if is_new {
                    info!("Node registered: {}", node_id);
                    ApplyResult::NodeRegistered(node_id)
                } else {
                    debug!("Node updated: {}", node_id);
                    ApplyResult::Ok
                }
            }

            Proposal::DeregisterNode(node_id) => {
                self.nodes.remove(&node_id);
                self.role_assignments.remove(&node_id);
                info!("Node deregistered: {}", node_id);
                ApplyResult::NodeDeregistered(node_id)
            }

            Proposal::AssignRole { node_id, role } => {
                if !self.nodes.contains_key(&node_id) {
                    return ApplyResult::Rejected(format!(
                        "Cannot assign role to unknown node: {node_id}"
                    ));
                }
                let roles = self.role_assignments.entry(node_id).or_default();
                if !roles.contains(&role) {
                    roles.push(role.clone());
                    info!("Role '{}' assigned to node {}", role, node_id);
                    return ApplyResult::RoleAssigned { node_id, role };
                }
                ApplyResult::Ok
            }

            Proposal::RevokeRole { node_id, role } => {
                if let Some(roles) = self.role_assignments.get_mut(&node_id) {
                    if let Some(pos) = roles.iter().position(|r| *r == role) {
                        roles.remove(pos);
                        info!("Role '{}' revoked from node {}", role, node_id);
                        return ApplyResult::RoleRevoked { node_id, role };
                    }
                }
                ApplyResult::Ok
            }

            Proposal::SubmitTask(task) => {
                let task_id = task.task_id;
                self.task_queue.push_back(task);
                debug!("Task submitted: {}", task_id);
                ApplyResult::TaskSubmitted(task_id)
            }

            Proposal::CompleteTask {
                task_id,
                result_hash,
            } => {
                self.completed_tasks.insert(task_id, result_hash);
                debug!("Task completed: {}", task_id);
                ApplyResult::TaskCompleted(task_id)
            }
        }
    }

    /// Create a snapshot of the current state.
    pub fn snapshot(&self) -> ClusterSnapshot {
        ClusterSnapshot {
            nodes: self.nodes.clone(),
            role_assignments: self.role_assignments.clone(),
            task_queue: self.task_queue.clone(),
            completed_tasks: self.completed_tasks.clone(),
            last_applied_index: self.last_applied_index,
        }
    }

    /// Restore state from a snapshot.
    pub fn restore(snapshot: ClusterSnapshot) -> Self {
        Self {
            nodes: snapshot.nodes,
            role_assignments: snapshot.role_assignments,
            task_queue: snapshot.task_queue,
            completed_tasks: snapshot.completed_tasks,
            last_applied_index: snapshot.last_applied_index,
        }
    }
}

/// A point-in-time snapshot of [`ClusterState`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterSnapshot {
    pub nodes: HashMap<NodeId, NodeDescriptor>,
    pub role_assignments: HashMap<NodeId, Vec<Role>>,
    pub task_queue: VecDeque<ScheduledTask>,
    pub completed_tasks: HashMap<TaskId, Hash>,
    pub last_applied_index: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use eo_core::types::{Capabilities, NodeType, OsType};

    fn make_descriptor(id: &str) -> NodeDescriptor {
        NodeDescriptor {
            node_id: uuid::Uuid::parse_str(id).unwrap(),
            node_type: NodeType::Heavy,
            os: OsType::MacOS,
            capabilities: Capabilities::default(),
            advertised_addresses: vec![],
            current_assigned_roles: vec![],
            started_at: Utc::now(),
        }
    }

    #[test]
    fn apply_register_node_updates_registry() {
        let mut state = ClusterState::default();
        let desc = make_descriptor("550e8400-e29b-41d4-a716-446655440000");
        let id = desc.node_id;

        let result = state.apply(Proposal::RegisterNode(desc.clone()));
        assert!(matches!(result, ApplyResult::NodeRegistered(_)));
        assert!(state.nodes.contains_key(&id));
        assert_eq!(state.nodes[&id], desc);
    }

    #[test]
    fn apply_assign_role() {
        let mut state = ClusterState::default();
        let desc = make_descriptor("550e8400-e29b-41d4-a716-446655440000");
        let id = desc.node_id;

        state.apply(Proposal::RegisterNode(desc));
        let result = state.apply(Proposal::AssignRole {
            node_id: id,
            role: Role::Execution,
        });
        assert!(matches!(result, ApplyResult::RoleAssigned { .. }));
        assert!(state.role_assignments[&id].contains(&Role::Execution));
    }

    #[test]
    fn snapshot_restore_roundtrip() {
        let mut state = ClusterState::default();
        let desc = make_descriptor("550e8400-e29b-41d4-a716-446655440000");
        state.apply(Proposal::RegisterNode(desc));
        state.apply(Proposal::AssignRole {
            node_id: uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap(),
            role: Role::Storage,
        });

        let snap = state.snapshot();
        let restored = ClusterState::restore(snap);

        assert_eq!(state.nodes.len(), restored.nodes.len());
        assert_eq!(state.role_assignments, restored.role_assignments);
    }

    #[test]
    fn deterministic_apply_order() {
        let mut state1 = ClusterState::default();
        let mut state2 = ClusterState::default();
        let desc = make_descriptor("550e8400-e29b-41d4-a716-446655440000");

        for state in [&mut state1, &mut state2] {
            state.apply(Proposal::RegisterNode(desc.clone()));
            state.apply(Proposal::AssignRole {
                node_id: desc.node_id,
                role: Role::Execution,
            });
        }

        assert_eq!(state1.nodes, state2.nodes);
        assert_eq!(state1.role_assignments, state2.role_assignments);
    }
}
