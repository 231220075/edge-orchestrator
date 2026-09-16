//! Master-side project submission: pack a local directory, route a
//! ProjectTask to a capable executor, and track returned results.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use eo_core::error::{CoreError, Result};
use eo_core::types::{NodeDescriptor, NodeId, ProjectResult, ProjectTask, ResourceLimits, TaskId};
use tokio::sync::mpsc;
use tracing::info;

use crate::project_snapshot::snapshot_from_dir;
use crate::raft::network::RaftIdRegistry;
use crate::raft::state_machine::ClusterState;

/// How long a task may stay dispatched before the master declares it failed.
/// The executor's own request-response window is 1800s; the grace period is that
/// plus slack, so a task can never be pending forever.
const RESULT_GRACE: Duration = Duration::from_secs(1920);

/// Lifecycle of a submitted project task on the master side.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskState {
    /// Packed and handed to the swarm; no result yet.
    Dispatched,
    /// Terminal failure that is known before (or instead of) a result.
    Failed(String),
    /// Terminal success/failure reported by the executor.
    Done,
}

/// How a master resolves a target node to a libp2p PeerId.
pub enum Catalog {
    /// Full cluster member: topology from Raft state (node_id -> raft_id) plus
    /// the raft_id -> PeerId registry.
    Raft {
        state: Arc<Mutex<ClusterState>>,
        registry: RaftIdRegistry,
    },
    /// Light client (not a Raft member): topology learned from peer
    /// descriptors exchanged over the mesh (peer_id -> descriptor).
    Peers(Arc<Mutex<HashMap<libp2p::PeerId, NodeDescriptor>>>),
}

pub struct ProjectClient {
    swarm_commands: mpsc::Sender<p2p::SwarmCommand>,
    catalog: Catalog,
    results: Arc<Mutex<HashMap<TaskId, ProjectResult>>>,
    /// task_id -> (state, dispatched_at). Lets `fetch_project_result` report
    /// something more useful than a bare `pending`.
    tasks: Arc<Mutex<HashMap<TaskId, (TaskState, Instant)>>>,
    self_node_id: NodeId,
}

impl ProjectClient {
    pub fn new(
        swarm_commands: mpsc::Sender<p2p::SwarmCommand>,
        catalog: Catalog,
        results: Arc<Mutex<HashMap<TaskId, ProjectResult>>>,
        self_node_id: NodeId,
    ) -> Self {
        Self {
            swarm_commands,
            catalog,
            results,
            tasks: Arc::new(Mutex::new(HashMap::new())),
            self_node_id,
        }
    }

    fn resolve_peer(&self, target: Option<NodeId>) -> Result<libp2p::PeerId> {
        let self_id = self.self_node_id;
        match &self.catalog {
            Catalog::Raft { state, registry } => {
                let (raft_id, chosen) = {
                    let state = state.lock().map_err(|_| poisoned())?;
                    let node = match target {
                        Some(id) => state.nodes.get(&id).ok_or_else(|| {
                            CoreError::InvalidState(format!("unknown target node {id}"))
                        })?,
                        None => {
                            let mut c: Vec<&NodeDescriptor> = state
                                .nodes
                                .values()
                                .filter(|d| d.capabilities.project_sandbox && d.node_id != self_id)
                                .collect();
                            c.sort_by_key(|d| d.raft_id);
                            *c.first().ok_or_else(|| {
                                CoreError::InvalidState(
                                    "no remote node with project_sandbox capability".into(),
                                )
                            })?
                        }
                    };
                    let rid = node.raft_id.ok_or_else(|| {
                        CoreError::InvalidState("target node has no raft_id".into())
                    })?;
                    (rid, node.node_id)
                };
                registry.get(raft_id).ok_or_else(|| {
                    CoreError::Network(format!("no PeerId for raft id {raft_id} (node {chosen})"))
                })
            }
            Catalog::Peers(peers) => {
                let peers = peers.lock().map_err(|_| poisoned())?;
                let pick = match target {
                    Some(id) => peers.iter().find(|(_, d)| d.node_id == id).map(|(p, _)| *p),
                    None => {
                        let mut c: Vec<(&libp2p::PeerId, &NodeDescriptor)> = peers
                            .iter()
                            .filter(|(_, d)| d.capabilities.project_sandbox && d.node_id != self_id)
                            .collect();
                        c.sort_by_key(|(_, d)| d.raft_id);
                        c.first().map(|(p, _)| **p)
                    }
                };
                pick.ok_or_else(|| {
                    CoreError::InvalidState(
                        "no reachable node with project_sandbox capability".into(),
                    )
                })
            }
        }
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
        let task_size = snapshot.tar_bytes.len();
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
        info!(
            "project {task_id}: dispatched to peer {peer_id} (snapshot {} bytes, work_dir={work_dir}, \
             timeout={timeout_ms}ms); polling from here on",
            task_size
        );
        if let Ok(mut tasks) = self.tasks.lock() {
            tasks.insert(task_id, (TaskState::Dispatched, Instant::now()));
        }
        Ok(task_id)
    }

    /// Record a result pushed back by an executor.
    pub fn record_result(&self, result: ProjectResult) {
        info!(
            "project {}: result received (exit={}, {}ms, {} stdout / {} stderr bytes)",
            result.task_id,
            result.exit_code,
            result.execution_time_ms,
            result.stdout.len(),
            result.stderr.len()
        );
        if let Ok(mut tasks) = self.tasks.lock() {
            tasks.insert(result.task_id, (TaskState::Done, Instant::now()));
        }
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

    /// Lifecycle state of a task, for status reporting over IPC.
    ///
    /// A dispatched task that outlives [`RESULT_GRACE`] is reported as failed
    /// rather than staying `pending` forever: without this, a request that is
    /// never answered (executor offline, protocol timeout) is indistinguishable
    /// from a slow build.
    pub fn task_status(&self, task_id: &TaskId) -> TaskState {
        let Ok(tasks) = self.tasks.lock() else {
            return TaskState::Failed("project client state poisoned".into());
        };
        let Some((state, dispatched_at)) = tasks.get(task_id) else {
            return TaskState::Failed(format!("unknown task_id {task_id}"));
        };
        match state {
            TaskState::Dispatched if dispatched_at.elapsed() > RESULT_GRACE => {
                TaskState::Failed(format!(
                    "no result within {}s (executor did not answer: check the execution node's \
                     logs and whether its project executor is reachable)",
                    RESULT_GRACE.as_secs()
                ))
            }
            other => other.clone(),
        }
    }

    /// Number of tasks this client has dispatched (observability only).
    #[allow(dead_code)]
    pub fn tracked_tasks(&self) -> usize {
        self.tasks.lock().map(|t| t.len()).unwrap_or(0)
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
            Catalog::Raft {
                state: Arc::new(Mutex::new(state)),
                registry,
            },
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
            Catalog::Raft {
                state: Arc::new(Mutex::new(state)),
                registry,
            },
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

    #[test]
    fn resolve_peer_is_deterministic_by_raft_id() {
        let mut state = ClusterState::default();
        let n_large = uuid::Uuid::new_v4();
        let n_small = uuid::Uuid::new_v4();
        state.nodes.insert(n_large, make_node(n_large, 9, true));
        state.nodes.insert(n_small, make_node(n_small, 2, true));
        let registry = RaftIdRegistry::new();
        let pid_small = peer();
        registry.insert(2, pid_small);
        registry.insert(9, peer());
        let c = client(state, registry, uuid::Uuid::new_v4());
        assert_eq!(c.resolve_peer(None).unwrap(), pid_small);
    }
    #[test]
    fn peers_catalog_resolves_from_descriptors() {
        let mut peers = HashMap::new();
        let pid = peer();
        let nid = uuid::Uuid::new_v4();
        peers.insert(pid, make_node(nid, 1, true));
        let (tx, _rx) = mpsc::channel(4);
        let c = ProjectClient::new(
            tx,
            Catalog::Peers(Arc::new(Mutex::new(peers))),
            Arc::new(Mutex::new(HashMap::new())),
            uuid::Uuid::new_v4(),
        );
        assert_eq!(c.resolve_peer(None).unwrap(), pid);
        assert_eq!(c.resolve_peer(Some(nid)).unwrap(), pid);
    }
}
