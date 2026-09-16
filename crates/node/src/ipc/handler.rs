//! JSON-RPC 2.0 method handler.
//!
//! Dispatches incoming method calls to the appropriate Rust subsystem
//! (CAS storage, Raft proposal channel, cluster state).

use std::sync::Arc;

use crate::raft::Proposal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use storage::LocalObjectStore;

use crate::project_client::ProjectClient;
use tokio::sync::mpsc;
use tracing::{debug, warn};

// ── JSON-RPC wire types ────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    pub method: String,
    #[serde(default)]
    pub params: Value,
    pub id: Value,
}

#[derive(Debug, Serialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcErrorPayload>,
    pub id: Value,
}

#[derive(Debug, Serialize)]
pub struct JsonRpcErrorPayload {
    pub code: i32,
    pub message: String,
}

// ── Method-specific params / results ───────────────────────────────────
// Params for the removed Wasm-lane methods (`submit_to_cas_and_raft`,
// `fetch_execution_result`) were dropped with them; see
// docs/v3-modules/22-wasm-lane移除记录.md.

fn default_timeout() -> u64 {
    30000
}

// ── Handler ────────────────────────────────────────────────────────────

/// Holds handles to all subsystems needed to service JSON-RPC methods.
pub struct JsonRpcHandler {
    /// Channel to submit proposals to the Raft node.
    pub raft_proposal_tx: mpsc::Sender<Proposal>,

    /// Content-addressed object store.
    pub object_store: Arc<LocalObjectStore>,

    /// Total tasks completed (monotonically increasing counter).
    pub tasks_completed: std::sync::atomic::AtomicU64,

    /// Master-side project submitter (None on nodes without cluster state).
    pub project_client: Option<Arc<ProjectClient>>,
}

impl JsonRpcHandler {
    /// Create a new handler.
    pub fn new(
        raft_proposal_tx: mpsc::Sender<Proposal>,
        object_store: Arc<LocalObjectStore>,
        project_client: Option<Arc<ProjectClient>>,
    ) -> Self {
        Self {
            raft_proposal_tx,
            object_store,
            tasks_completed: std::sync::atomic::AtomicU64::new(0),
            project_client,
        }
    }

    /// Dispatch a JSON-RPC request to the appropriate method handler.
    pub async fn handle(&self, request: JsonRpcRequest) -> JsonRpcResponse {
        let id = request.id;

        let result = match request.method.as_str() {
            "get_cluster_topology" => self.get_cluster_topology().await,
            "submit_project" => self.submit_project(request.params).await,
            "fetch_project_result" => self.fetch_project_result(request.params).await,
            unknown => Err(json_rpc_error(
                -32601,
                format!("Method not found: {unknown}"),
            )),
        };

        match result {
            Ok(value) => JsonRpcResponse {
                jsonrpc: "2.0",
                result: Some(value),
                error: None,
                id,
            },
            Err(error) => JsonRpcResponse {
                jsonrpc: "2.0",
                result: None,
                error: Some(error),
                id,
            },
        }
    }

    // ── Method implementations ────────────────────────────────────────

    async fn get_cluster_topology(&self) -> Result<Value, JsonRpcErrorPayload> {
        // Return a static topology for now — in production this would
        // read from ClusterState (which is held by the Raft state machine).
        let topology = serde_json::json!({
            "nodes": [],
            "role_assignments": {},
            "tasks_pending": 0,
            "tasks_completed": self.tasks_completed.load(std::sync::atomic::Ordering::Relaxed),
        });

        debug!("get_cluster_topology: returned topology");
        Ok(topology)
    }

    // ── Project submission ────────────────────────────────────────────

    async fn submit_project(&self, params: Value) -> Result<Value, JsonRpcErrorPayload> {
        #[derive(Deserialize)]
        struct SubmitProjectParams {
            project_dir: String,
            #[serde(default = "default_project_work_dir")]
            work_dir: String,
            #[serde(default)]
            build_cmd: Vec<String>,
            #[serde(default)]
            run_cmd: Vec<String>,
            #[serde(default = "default_timeout")]
            timeout_ms: u64,
            #[serde(default)]
            target_node: Option<String>,
        }

        let p: SubmitProjectParams = serde_json::from_value(params)
            .map_err(|e| json_rpc_error(-32602, format!("Invalid params: {e}")))?;

        let client = self.project_client.as_ref().ok_or_else(|| {
            json_rpc_error(
                -32010,
                "node has no cluster state; cannot submit projects".into(),
            )
        })?;

        let target = match p.target_node {
            Some(s) => Some(
                uuid::Uuid::parse_str(&s)
                    .map_err(|e| json_rpc_error(-32602, format!("invalid target_node: {e}")))?,
            ),
            None => None,
        };

        let task_id = client
            .submit_local_project(
                &p.project_dir,
                &p.work_dir,
                p.build_cmd,
                p.run_cmd,
                p.timeout_ms,
                target,
            )
            .await
            .map_err(|e| json_rpc_error(-32011, format!("submit project failed: {e}")))?;

        Ok(serde_json::json!({ "task_id": task_id.to_string() }))
    }

    async fn fetch_project_result(&self, params: Value) -> Result<Value, JsonRpcErrorPayload> {
        #[derive(Deserialize)]
        struct FetchProjectParams {
            task_id: String,
        }

        let p: FetchProjectParams = serde_json::from_value(params)
            .map_err(|e| json_rpc_error(-32602, format!("Invalid params: {e}")))?;

        let client = self
            .project_client
            .as_ref()
            .ok_or_else(|| json_rpc_error(-32010, "node has no cluster state".into()))?;

        let task_id = uuid::Uuid::parse_str(&p.task_id)
            .map_err(|e| json_rpc_error(-32602, format!("invalid task_id: {e}")))?;

        // Report the real lifecycle state instead of collapsing everything that
        // is not a stored result into "pending": a failed dispatch, an executor
        // rejection, or a request nobody ever answered must be distinguishable
        // from a slow build, otherwise the client polls until it gives up.
        match client.task_status(&task_id) {
            crate::project_client::TaskState::Failed(msg) => {
                warn!("fetch_project_result {task_id}: failed: {msg}");
                return Ok(serde_json::json!({
                    "status": "failed",
                    "error": msg,
                }));
            }
            crate::project_client::TaskState::Done => {}
            crate::project_client::TaskState::Dispatched => {}
        }

        match client.get_result(&task_id) {
            Some(r) => {
                use base64::Engine;
                let stdout = base64::engine::general_purpose::STANDARD.encode(&r.stdout);
                let stderr = base64::engine::general_purpose::STANDARD.encode(&r.stderr);
                Ok(serde_json::json!({
                    "status": "completed",
                    "exit_code": r.exit_code,
                    "stdout": stdout,
                    "stderr": stderr,
                    "execution_time_ms": r.execution_time_ms,
                    "executed_on": r.executed_on.to_string(),
                }))
            }
            None => Ok(serde_json::json!({ "status": "pending" })),
        }
    }
}

// ── Helpers ────────────────────────────────────────────────────────────

fn default_project_work_dir() -> String {
    "/root/project".into()
}

fn json_rpc_error(code: i32, message: String) -> JsonRpcErrorPayload {
    JsonRpcErrorPayload { code, message }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn make_handler() -> (JsonRpcHandler, TempDir, mpsc::Receiver<Proposal>) {
        let dir = TempDir::new().unwrap();
        let store = Arc::new(LocalObjectStore::new(dir.path().to_path_buf()).unwrap());
        let (tx, rx) = mpsc::channel(16);
        let handler = JsonRpcHandler::new(tx, store, None);
        (handler, dir, rx)
    }

    #[tokio::test]
    async fn get_cluster_topology_returns_valid_json() {
        let (handler, _dir, _rx) = make_handler();

        let req = JsonRpcRequest {
            jsonrpc: "2.0".into(),
            method: "get_cluster_topology".into(),
            params: Value::Object(Default::default()),
            id: Value::Number(1.into()),
        };

        let resp = handler.handle(req).await;
        assert!(resp.error.is_none(), "unexpected error: {:?}", resp.error);
        let result = resp.result.unwrap();
        assert!(result.get("nodes").is_some());
        assert!(result.get("tasks_completed").is_some());
    }

    #[tokio::test]
    async fn unknown_method_returns_error() {
        let (handler, _dir, _rx) = make_handler();

        let req = JsonRpcRequest {
            jsonrpc: "2.0".into(),
            method: "nonexistent_method".into(),
            params: Value::Object(Default::default()),
            id: Value::Number(5.into()),
        };

        let resp = handler.handle(req).await;
        assert!(resp.error.is_some());
        assert_eq!(resp.error.unwrap().code, -32601); // Method not found
    }
}
