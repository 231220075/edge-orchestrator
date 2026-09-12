//! Coordinator scheduler loop + executor loop.
//! Both loops read the replicated ClusterState and propose state changes
//! through the raft proposal channel. Task routing decisions live inside the
//! consensus log (no ad-hoc RPC), giving at-least-once execution semantics.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use eo_core::traits::Sandbox as _;
use eo_core::types::{ExecutionResult, Role, TaskId};
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::raft::network::RaftIdRegistry;
use crate::raft::proposal::Proposal;
use crate::raft::state_machine::ClusterState;

const LOOP_INTERVAL: Duration = Duration::from_millis(500);
const REROUTE_TIMEOUT_MS: u64 = 5000;

pub fn now_ms() -> u64 {
    chrono::Utc::now().timestamp_millis() as u64
}

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
            tokio::time::sleep(Duration::from_secs(2)).await;
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

pub fn spawn_runtime(
    self_raft_id: u64,
    state: Arc<Mutex<ClusterState>>,
    proposal_tx: mpsc::Sender<Proposal>,
    store: Arc<storage::LocalObjectStore>,
    swarm_commands: mpsc::Sender<p2p::SwarmCommand>,
    raft_registry: RaftIdRegistry,
) {
    let state2 = Arc::clone(&state);
    let tx2 = proposal_tx.clone();
    tokio::spawn(async move {
        coordinator_loop(self_raft_id, state, proposal_tx).await;
    });
    tokio::spawn(async move {
        executor_loop(
            self_raft_id,
            state2,
            tx2,
            store,
            swarm_commands,
            raft_registry,
        )
        .await;
    });
}

pub async fn coordinator_loop(
    _self_raft_id: u64,
    state: Arc<Mutex<ClusterState>>,
    proposal_tx: mpsc::Sender<Proposal>,
) {
    loop {
        tokio::time::sleep(LOOP_INTERVAL).await;
        let snap = {
            let guard = state.lock().expect("state poisoned");
            guard.clone()
        };

        for task in snap.task_queue.iter() {
            if snap.assigned_tasks.contains_key(&task.task_id) {
                continue;
            }
            if let Some(eid) = pick_executor(&snap, None) {
                let _ = proposal_tx
                    .send(Proposal::AssignTask {
                        task_id: task.task_id,
                        executor_raft_id: eid,
                    })
                    .await;
                info!("SCHED assign task {} -> raft {}", task.task_id, eid);
            }
        }

        let now = now_ms();
        for (tid, a) in snap.assigned_tasks.iter() {
            if snap.completed_tasks.contains_key(tid) {
                continue;
            }
            if now.saturating_sub(a.assigned_at_ms) > REROUTE_TIMEOUT_MS {
                if let Some(eid) = pick_executor(&snap, Some(a.executor_raft_id)) {
                    let _ = proposal_tx
                        .send(Proposal::AssignTask {
                            task_id: *tid,
                            executor_raft_id: eid,
                        })
                        .await;
                    warn!("SCHED reroute task {} -> raft {} (timeout)", tid, eid);
                }
            }
        }
    }
}

/// A node-side executor: runs any task assigned to `self_raft_id`.
pub async fn executor_loop(
    self_raft_id: u64,
    state: Arc<Mutex<ClusterState>>,
    proposal_tx: mpsc::Sender<Proposal>,
    store: Arc<storage::LocalObjectStore>,
    swarm_commands: mpsc::Sender<p2p::SwarmCommand>,
    raft_registry: RaftIdRegistry,
) {
    loop {
        tokio::time::sleep(LOOP_INTERVAL).await;
        let snap = {
            let guard = state.lock().expect("state poisoned");
            guard.clone()
        };

        for (tid, a) in snap.assigned_tasks.iter() {
            if a.executor_raft_id != self_raft_id {
                continue;
            }
            if snap.completed_tasks.contains_key(tid) {
                continue;
            }
            let Some(task) = snap.task_queue.iter().find(|t| t.task_id == *tid) else {
                continue;
            };

            // Fetch code: inline first, else local CAS by hash; on a local
            // miss, request the blob from peers and retry next iteration.
            let code = match &task.code_inline {
                Some(bytes) => bytes.clone(),
                None => match store.get_blob(&task.code_hash) {
                    Ok(bytes) => bytes,
                    Err(e) => {
                        for (rid, pid) in raft_registry.snapshot() {
                            let _ = swarm_commands
                                .send(p2p::SwarmCommand::RequestBlob {
                                    peer_id: pid,
                                    hash: task.code_hash.clone(),
                                })
                                .await;
                            warn!(
                                "executor {}: blob {} requested from raft {}",
                                self_raft_id, task.code_hash, rid
                            );
                        }
                        warn!(
                            "executor {}: code {} missing: {}",
                            self_raft_id, task.code_hash, e
                        );
                        continue;
                    }
                },
            };

            let result = match task.required_runtime.clone() {
                eo_core::types::RuntimeKind::Wasm => execute_wasm(code, task.timeout_ms),
                other => ExecutionResult {
                    exit_code: -1,
                    stdout: Vec::new(),
                    stderr: format!("unsupported runtime: {:?}", other).into_bytes(),
                    execution_time_ms: 0,
                    peak_memory_bytes: 0,
                    result_hash: None,
                },
            };

            let result_json = serde_json::to_vec(&result).unwrap_or_default();
            let Ok(result_hash) = store.put_blob(&result_json) else {
                warn!("executor {}: failed to store result", self_raft_id);
                continue;
            };
            if proposal_tx
                .send(Proposal::CompleteTask {
                    task_id: *tid,
                    result_hash,
                })
                .await
                .is_ok()
            {
                info!(
                    "EXEC raft {} finished task {} exit={}",
                    self_raft_id, tid, result.exit_code
                );
            }
        }
    }
}

fn execute_wasm(code: Vec<u8>, timeout_ms: u64) -> ExecutionResult {
    // Wasmtime sandbox: exercise the real StoreLimits + epoch interruption
    // implemented in M2. Reuse the sandbox crate's default registry.
    let wasm_sandbox = match sandbox::WasmtimeSandbox::new() {
        Ok(s) => s,
        Err(e) => {
            return ExecutionResult {
                exit_code: -1,
                stdout: Vec::new(),
                stderr: format!("create sandbox: {e:?}").into_bytes(),
                execution_time_ms: 0,
                peak_memory_bytes: 0,
                result_hash: None,
            };
        }
    };
    let _ = timeout_ms;
    wasm_sandbox
        .execute_code(code)
        .unwrap_or_else(|e| ExecutionResult {
            exit_code: -1,
            stdout: Vec::new(),
            stderr: format!("execute: {e:?}").into_bytes(),
            execution_time_ms: 0,
            peak_memory_bytes: 0,
            result_hash: None,
        })
}
