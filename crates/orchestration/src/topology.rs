//! Cluster topology specification parser.
//!
//! Reads a YAML topology spec describing desired node→role assignments
//! and computes the diff against the current state, generating Raft proposals.

use std::collections::HashMap;

use eo_core::error::Result;
use eo_core::types::{NodeId, Role};
use eo_raft::Proposal;
use serde::Deserialize;

use crate::role_engine::NodeSelector;

/// A cluster topology specification (desired state).
#[derive(Debug, Clone, Deserialize)]
pub struct ClusterTopologySpec {
    /// Version identifier (e.g., "1.0").
    pub version: String,
    /// Desired node assignments.
    #[serde(default)]
    pub assignments: Vec<NodeAssignment>,
}

/// Desired role assignment for a set of nodes.
#[derive(Debug, Clone, Deserialize)]
pub struct NodeAssignment {
    /// How to select nodes for this assignment.
    pub node_selector: NodeSelector,
    /// Roles to assign to the selected nodes.
    pub roles: Vec<Role>,
}

/// Parse a YAML topology spec string.
pub fn parse_topology_spec(yaml: &str) -> Result<ClusterTopologySpec> {
    let spec: ClusterTopologySpec = serde_yaml::from_str(yaml).map_err(|e| {
        eo_core::error::CoreError::Configuration(format!("invalid topology spec: {e}"))
    })?;
    Ok(spec)
}

/// Compute the diff between current role assignments and desired topology.
///
/// Returns a list of Raft proposals that, when applied, will bring the
/// cluster to the desired state.
pub fn diff_topology(
    current: &HashMap<NodeId, Vec<Role>>,
    desired: &ClusterTopologySpec,
) -> Vec<Proposal> {
    let mut proposals = Vec::new();

    // Build a map of desired roles per node
    let mut desired_roles: HashMap<NodeId, Vec<Role>> = HashMap::new();

    for assignment in &desired.assignments {
        // For now, we assume the selector includes a specific node_id
        if let Some(node_id) = assignment.node_selector.node_id {
            let entry = desired_roles.entry(node_id).or_default();
            for role in &assignment.roles {
                if !entry.contains(role) {
                    entry.push(role.clone());
                }
            }
        }
    }

    // Generate AssignRole proposals for roles that are desired but missing
    for (node_id, roles) in &desired_roles {
        let current_roles = current.get(node_id).cloned().unwrap_or_default();
        for role in roles {
            if !current_roles.contains(role) {
                proposals.push(Proposal::AssignRole {
                    node_id: *node_id,
                    role: role.clone(),
                });
            }
        }
    }

    // Generate RevokeRole proposals for roles that exist but are not desired
    for (node_id, current_roles) in current {
        let desired = desired_roles.get(node_id).cloned().unwrap_or_default();
        for role in current_roles {
            if !desired.contains(role) {
                proposals.push(Proposal::RevokeRole {
                    node_id: *node_id,
                    role: role.clone(),
                });
            }
        }
    }

    proposals
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_valid_topology_spec() {
        let yaml = r#"
version: "1.0"
assignments:
  - node_selector:
      node_id: "550e8400-e29b-41d4-a716-446655440000"
    roles:
      - Storage
      - Execution
"#;
        let spec = parse_topology_spec(yaml).unwrap();
        assert_eq!(spec.version, "1.0");
        assert_eq!(spec.assignments.len(), 1);
        assert_eq!(spec.assignments[0].roles.len(), 2);
    }

    #[test]
    fn diff_generates_correct_proposals() {
        let node_id = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let yaml = format!(
            r#"
version: "1.0"
assignments:
  - node_selector:
      node_id: "{}"
    roles:
      - Storage
"#,
            node_id
        );

        let spec = parse_topology_spec(&yaml).unwrap();
        let mut current = HashMap::new();

        // First diff: no current roles → should assign Storage
        let proposals = diff_topology(&current, &spec);
        assert_eq!(proposals.len(), 1);
        assert!(matches!(
            &proposals[0],
            Proposal::AssignRole {
                role: Role::Storage,
                ..
            }
        ));

        // Apply the assignment
        current.insert(node_id, vec![Role::Storage]);

        // Second diff: Storage already assigned → no changes
        let proposals = diff_topology(&current, &spec);
        assert!(proposals.is_empty());
    }

    #[test]
    fn diff_handles_role_reassignment() {
        let node_id = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let yaml = format!(
            r#"
version: "1.0"
assignments:
  - node_selector:
      node_id: "{}"
    roles:
      - Execution
"#,
            node_id
        );

        let spec = parse_topology_spec(&yaml).unwrap();
        let mut current = HashMap::new();
        current.insert(node_id, vec![Role::Storage]);

        // Should revoke Storage, assign Execution
        let proposals = diff_topology(&current, &spec);
        assert_eq!(proposals.len(), 2);

        let has_revoke = proposals.iter().any(|p| {
            matches!(
                p,
                Proposal::RevokeRole {
                    role: Role::Storage,
                    ..
                }
            )
        });
        let has_assign = proposals.iter().any(|p| {
            matches!(
                p,
                Proposal::AssignRole {
                    role: Role::Execution,
                    ..
                }
            )
        });
        assert!(has_revoke);
        assert!(has_assign);
    }
}
