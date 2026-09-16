//! Node registration loop, plus the coordinator/executor loop scaffolding.
//!
//! Today the registration loop is what actually uses Raft: each node keeps
//! proposing `RegisterNode` until it appears in the replicated state, which is
//! what makes capability routing (and role re-assignment on node failure)
//! possible.
//!
//! The coordinator/executor loops previously scheduled and ran
//! `RuntimeKind::Wasm` tasks through the consensus log. That execution backend
//! is gone (see `docs/v3-modules/22-wasm-lane移除记录.md`), and project tasks
//! currently use a direct request-response path instead. The loops are kept as
//! explicit scaffolding for the next step (routing ProjectTasks through the
//! consensus log); they deliberately do not fabricate work in the meantime.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use eo_core::types::Role;
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::raft::network::RaftIdRegistry;
use crate::raft::proposal::Proposal;
use crate::raft::state_machine::ClusterState;

const REGISTER_INTERVAL: Duration = Duration::from_secs(2);

pub fn pick_executor(state: &ClusterState, exclude: Option<u64>) -> Option<u64> {
    state
        .nodes
        .values()
        .filter(|d| d.raft_id.is_some())
        .filter(|d| d.current_assigned_roles.contains(&Role::Execution))
        .map(|d| d.raft_id.unwrap())
        .find(|rid| Some(*rid) != exclude)
}

/// Spawn the coordinator and executor loops for this node.
///
/// Every node runs both loops: the leader is the only one whose AssignTask
/// proposals win consensus, and the executor loop is a no-op unless a task is
/// assigned to this node.
/// Periodically (re)propose RegisterNode until this node appears in state.
pub fn spawn_register_loop(
    self_raft_id: u64,
    self_desc: eo_core::types::NodeDescriptor,
    proposal_tx: mpsc::Sender<Proposal>,
    state: Arc<Mutex<ClusterState>>,
) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(REGISTER_INTERVAL).await;
            let registered = {
                let guard = state.lock().expect("state poisoned");
                guard
                    .nodes
                    .values()
                    .any(|d| d.raft_id == Some(self_raft_id))
            };
            if !registered {
                let _ = proposal_tx
                    .send(Proposal::RegisterNode(self_desc.clone()))
                    .await;
            }
        }
    });
}

/// Spawn the coordinator/executor loops for this node.
///
/// NOTE: these loops have no work source right now. They used to drain
/// `ClusterState::task_queue` and execute Wasm modules; with that backend
/// removed the replicated state carries node registration and role assignments
/// only. Routing ProjectTasks (with reroute-on-failure) through this log is the
/// next milestone, and this is where it lands.
pub fn spawn_runtime(
    self_raft_id: u64,
    state: Arc<Mutex<ClusterState>>,
    proposal_tx: mpsc::Sender<Proposal>,
    store: Arc<storage::LocalObjectStore>,
    swarm_commands: mpsc::Sender<p2p::SwarmCommand>,
    raft_registry: RaftIdRegistry,
) {
    let _ = (
        self_raft_id,
        state,
        proposal_tx,
        store,
        swarm_commands,
        raft_registry,
    );
    tracing::debug!(
        "runtime loops are scaffolding: no task source is wired (project tasks use the \
         request-response path). See docs/v3-modules/22-wasm-lane移除记录.md"
    );
}
