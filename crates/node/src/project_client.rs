//! Master-side project submission: pack a local directory, route a
//! ProjectTask to a capable executor, and track returned results.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use eo_core::error::{CoreError, Result};
use eo_core::types::{NodeId, ProjectResult, ProjectTask, ResourceLimits, TaskId};
use tokio::sync::mpsc;

use crate::project_snapshot::snapshot_from_dir;
use crate::raft::network::RaftIdRegistry;
use crate::raft::state_machine::ClusterState;

pub struct ProjectClient {
    swarm_commands: mpsc::Sender<p2p::SwarmCommand>,
    registry: RaftIdRegistry,
    state: Arc<Mutex<ClusterState>>,
    results: Arc<Mutex<HashMap<TaskId, ProjectResult>>>,
    self_node_id: NodeId,
}

impl ProjectClient {
    pub fn new(
        swarm_commands: mpsc::Sender<p2p::SwarmCommand>,
        registry: RaftIdRegistry,
        state: Arc<Mutex<ClusterState>>,
        results: Arc<Mutex<HashMap<TaskId, ProjectResult>>>,
        self_node_id: NodeId,
    ) -> Self {
        Self {
            swarm_commands,
            registry,
            state,
            results,
            self_node_id,
        }
    }

    fn resolve_peer(&self, target: Option<NodeId>) -> Result<libp2p::PeerId> {
        let (raft_id, chosen) = {
            let state = self.state.lock().map_err(|_| poisoned())?;
            let node = match target {
                Some(id) => state
                    .nodes
                    .get(&id)
                    .ok_or_else(|| CoreError::InvalidState(format!("unknown target node {id}")))?,
                None => {
                    let self_id = self.self_node_id;
                    state
                        .nodes
                        .values()
                        .find(|d| d.capabilities.project_sandbox && d.node_id != self_id)
                        .ok_or_else(|| {
                            CoreError::InvalidState(
                                "no remote node with project_sandbox capability".into(),
                            )
                        })?
                }
            };
            let rid = node
                .raft_id
                .ok_or_else(|| CoreError::InvalidState("target node has no raft_id".into()))?;
            (rid, node.node_id)
        };
        self.registry.get(raft_id).ok_or_else(|| {
            CoreError::Network(format!("no PeerId for raft id {raft_id} (node {chosen})"))
        })
    }

    /// Pack a local directory and send a ProjectTask to a capable executor.
    pub async fn submit_local_project(
        &self,
        local_dir: &str,
        work_dir: &str,
        build_cmd: Vec<String>,
        run_cmd: Vec<String>,
        timeout_ms: u64,
        target: Option<NodeId>,
    ) -> Result<TaskId> {
        let snapshot = snapshot_from_dir(Path::new(local_dir))?;
        let peer_id = self.resolve_peer(target)?;
        let task_id = uuid::Uuid::new_v4();
        let task = ProjectTask {
            task_id,
            snapshot,
            work_dir: work_dir.to_string(),
            build_cmd,
            run_cmd,
            timeout_ms,
            resource_limits: ResourceLimits::default(),
            pinned_node: target,
        };
        self.swarm_commands
            .send(p2p::SwarmCommand::SendProjectTask { peer_id, task })
            .await
            .map_err(|e| CoreError::Network(format!("send project task: {e}")))?;
        Ok(task_id)
    }

    /// Record a result pushed back by an executor.
    pub fn record_result(&self, result: ProjectResult) {
        if let Ok(mut map) = self.results.lock() {
            map.insert(result.task_id, result);
        }
    }

    /// Fetch a previously recorded result.
    pub fn get_result(&self, task_id: &TaskId) -> Option<ProjectResult> {
        self.results
            .lock()
            .ok()
            .and_then(|m| m.get(task_id).cloned())
    }
}

fn poisoned() -> CoreError {
    CoreError::Internal("project client state poisoned".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use eo_core::types::{Capabilities, NodeDescriptor, NodeType, OsType};

    fn make_node(node_id: NodeId, raft_id: u64, project_sandbox: bool) -> NodeDescriptor {
        NodeDescriptor {
            node_id,
            node_type: NodeType::Heavy,
            os: OsType::Linux,
            capabilities: Capabilities {
                project_sandbox,
                ..Capabilities::default()
            },
            advertised_addresses: vec![],
            current_assigned_roles: vec![],
            started_at: Utc::now(),
            raft_id: Some(raft_id),
        }
    }

    fn peer() -> libp2p::PeerId {
        libp2p::identity::Keypair::generate_ed25519()
            .public()
            .to_peer_id()
    }

    fn client(state: ClusterState, registry: RaftIdRegistry, self_id: NodeId) -> ProjectClient {
        let (tx, _rx) = mpsc::channel(4);
        ProjectClient::new(
            tx,
            registry,
            Arc::new(Mutex::new(state)),
            Arc::new(Mutex::new(HashMap::new())),
            self_id,
        )
    }

    #[test]
    fn resolve_peer_picks_capable_node() {
        let node_id = uuid::Uuid::new_v4();
        let mut state = ClusterState::default();
        state.nodes.insert(node_id, make_node(node_id, 1, true));
        let registry = RaftIdRegistry::new();
        let pid = peer();
        registry.insert(1, pid);
        let c = client(state, registry, uuid::Uuid::new_v4());
        assert_eq!(c.resolve_peer(None).unwrap(), pid);
    }

    #[test]
    fn resolve_peer_rejects_without_capable_node() {
        let node_id = uuid::Uuid::new_v4();
        let mut state = ClusterState::default();
        state.nodes.insert(node_id, make_node(node_id, 1, false));
        let c = client(state, RaftIdRegistry::new(), uuid::Uuid::new_v4());
        assert!(c.resolve_peer(None).is_err());
    }

    #[tokio::test]
    async fn submit_local_project_packs_and_sends_task() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.txt"), b"hi").unwrap();
        let node_id = uuid::Uuid::new_v4();
        let mut state = ClusterState::default();
        state.nodes.insert(node_id, make_node(node_id, 1, true));
        let registry = RaftIdRegistry::new();
        let pid = peer();
        registry.insert(1, pid);
        let (tx, mut rx) = mpsc::channel(4);
        let c = ProjectClient::new(
            tx,
            registry,
            Arc::new(Mutex::new(state)),
            Arc::new(Mutex::new(HashMap::new())),
            uuid::Uuid::new_v4(),
        );
        let task_id = c
            .submit_local_project(
                dir.path().to_str().unwrap(),
                "/root/project",
                vec!["true".into()],
                vec![],
                1000,
                None,
            )
            .await
            .unwrap();
        match rx.recv().await.unwrap() {
            p2p::SwarmCommand::SendProjectTask { peer_id, task } => {
                assert_eq!(peer_id, pid);
                assert_eq!(task.task_id, task_id);
                assert!(!task.snapshot.tar_bytes.is_empty());
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }
}
