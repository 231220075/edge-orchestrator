//! JSON-RPC 2.0 method handler.
//!
//! Dispatches incoming method calls to the appropriate Rust subsystem
//! (CAS storage, Raft proposal channel, cluster state).

use std::process;
use std::sync::Arc;

use eo_core::types::{ExecutionResult, ResourceLimits, RoutingStrategy, ScheduledTask};
use eo_raft::Proposal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use storage::LocalObjectStore;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

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

#[derive(Debug, Deserialize)]
struct SubmitParams {
    code: String, // base64-encoded
    #[serde(default = "default_code_language")]
    code_language: String,
    #[serde(default = "default_runtime")]
    required_runtime: String,
    #[serde(default = "default_routing")]
    routing: String,
    #[serde(default = "default_timeout")]
    timeout_ms: u64,
}

fn default_code_language() -> String {
    "python".into()
}
fn default_runtime() -> String {
    "Wasm".into()
}
fn default_routing() -> String {
    "AnyExecutor".into()
}
fn default_timeout() -> u64 {
    30000
}

#[derive(Debug, Deserialize)]
struct FetchParams {
    result_hash: String,
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

    /// This node's own descriptor, for inclusion in topology responses.
    pub self_descriptor: Option<eo_core::types::NodeDescriptor>,

    /// Whether to execute tasks inline (single-node mode).
    /// When true, submitted code is executed immediately after storage.
    pub inline_execution: bool,
}

impl JsonRpcHandler {
    /// Create a new handler.
    pub fn new(
        raft_proposal_tx: mpsc::Sender<Proposal>,
        object_store: Arc<LocalObjectStore>,
    ) -> Self {
        Self {
            raft_proposal_tx,
            object_store,
            tasks_completed: std::sync::atomic::AtomicU64::new(0),
            self_descriptor: None,
            inline_execution: true, // default: execute inline for single-node
        }
    }

    /// Set this node's descriptor (called after bootstrap completes).
    pub fn set_self_descriptor(&mut self, desc: eo_core::types::NodeDescriptor) {
        self.self_descriptor = Some(desc);
    }

    /// Dispatch a JSON-RPC request to the appropriate method handler.
    pub async fn handle(&self, request: JsonRpcRequest) -> JsonRpcResponse {
        let id = request.id;

        let result = match request.method.as_str() {
            "get_cluster_topology" => self.get_cluster_topology().await,
            "submit_to_cas_and_raft" => self.submit_to_cas_and_raft(request.params).await,
            "fetch_execution_result" => self.fetch_execution_result(request.params).await,
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
        // Build node list from self descriptor (multi-node discovery
        // reads from ClusterState in the Raft state machine — this is
        // the single-node fallback that prevents returning empty lists).
        let mut nodes = Vec::new();
        let mut role_assignments = serde_json::Map::new();

        if let Some(ref desc) = self.self_descriptor {
            nodes.push(serde_json::json!({
                "node_id": desc.node_id.to_string(),
                "node_type": match desc.node_type {
                    eo_core::types::NodeType::Heavy => "Heavy",
                    eo_core::types::NodeType::Light => "Light",
                },
                "os": format!("{:?}", desc.os),
                "capabilities": {
                    "storage": desc.capabilities.storage,
                    "gpu_acceleration": desc.capabilities.gpu_acceleration,
                    "runtimes": desc.capabilities.runtimes.iter().map(|r| format!("{r:?}")).collect::<Vec<_>>(),
                    "max_memory_mb": desc.capabilities.max_memory_mb,
                    "cpu_cores": desc.capabilities.cpu_cores,
                },
                "advertised_addresses": desc.advertised_addresses.iter().map(|a| a.to_string()).collect::<Vec<_>>(),
                "current_assigned_roles": desc.current_assigned_roles.iter().map(|r| r.to_string()).collect::<Vec<_>>(),
                "started_at": desc.started_at.to_rfc3339(),
            }));

            let roles: Vec<String> = desc.current_assigned_roles.iter().map(|r| r.to_string()).collect();
            role_assignments.insert(desc.node_id.to_string(), serde_json::Value::Array(
                roles.iter().map(|r| serde_json::Value::String(r.clone())).collect()
            ));
        }

        let topology = serde_json::json!({
            "nodes": nodes,
            "role_assignments": role_assignments,
            "tasks_pending": 0,
            "tasks_completed": self.tasks_completed.load(std::sync::atomic::Ordering::Relaxed),
        });

        debug!("get_cluster_topology: returned topology with {} node(s)", nodes.len());
        Ok(topology)
    }

    async fn submit_to_cas_and_raft(&self, params: Value) -> Result<Value, JsonRpcErrorPayload> {
        let SubmitParams {
            code,
            code_language,
            required_runtime,
            routing,
            timeout_ms,
        } = serde_json::from_value(params)
            .map_err(|e| json_rpc_error(-32602, format!("Invalid params: {e}")))?;

        // Base64-decode the code
        use base64::Engine;
        let code_bytes = base64::engine::general_purpose::STANDARD
            .decode(code.as_bytes())
            .map_err(|e| json_rpc_error(-32602, format!("Base64 decode failed: {e}")))?;

        // Enforce maximum code size (64 KB)
        if code_bytes.len() > 64 * 1024 {
            return Err(json_rpc_error(
                -32001,
                format!("Code too large: {} bytes (max 65536)", code_bytes.len()),
            ));
        }

        // Compute hash and store blob in CAS
        let code_hash = storage::hash_blob(&code_bytes);
        self.object_store
            .put_blob(&code_bytes)
            .map_err(|e| json_rpc_error(-32002, format!("CAS store error: {e}")))?;

        // Parse runtime and routing
        let required_runtime = match required_runtime.as_str() {
            "Wasm" => eo_core::types::RuntimeKind::Wasm,
            "NativePosix" => eo_core::types::RuntimeKind::NativePosix,
            "Container" => eo_core::types::RuntimeKind::Container,
            other => {
                return Err(json_rpc_error(-32602, format!("Unknown runtime: {other}")));
            }
        };

        let routing = match routing.as_str() {
            "AnyExecutor" => RoutingStrategy::AnyExecutor,
            "PreferWasm" => RoutingStrategy::PreferWasm,
            "PreferNative" => RoutingStrategy::PreferNative,
            s if s.starts_with("Pinned:") => {
                let node_id_str = s.strip_prefix("Pinned:").unwrap();
                let node_id = uuid::Uuid::parse_str(node_id_str).map_err(|e| {
                    json_rpc_error(-32602, format!("Invalid node_id in Pinned: {e}"))
                })?;
                RoutingStrategy::Pinned(node_id)
            }
            other => {
                return Err(json_rpc_error(
                    -32602,
                    format!("Unknown routing strategy: {other}"),
                ));
            }
        };

        let task_id = uuid::Uuid::new_v4();

        let task = ScheduledTask {
            task_id,
            code_hash: code_hash.clone(),
            required_runtime,
            routing,
            timeout_ms,
            resource_limits: ResourceLimits::default(),
            submitted_at: chrono::Utc::now(),
            pinned_node: None,
        };

        // Propose task to Raft
        self.raft_proposal_tx
            .send(Proposal::SubmitTask(task))
            .await
            .map_err(|e| json_rpc_error(-32003, format!("Raft proposal failed: {e}")))?;

        debug!("submit_to_cas_and_raft: code_hash={code_hash}, task_id={task_id}");

        // ── Inline execution (single-node mode) ──────────────────────
        // When running as a single node, execute the task immediately
        // so results are available for the agent's evaluation step.
        let mut response = serde_json::json!({
            "code_hash": code_hash,
            "task_id": task_id.to_string(),
        });

        if self.inline_execution {
            let start = std::time::Instant::now();
            match self.execute_inline(&code_bytes, &code_language, timeout_ms) {
                Ok(exec_result) => {
                    let elapsed = start.elapsed();
                    let exec_result = ExecutionResult {
                        exit_code: exec_result.exit_code,
                        stdout: exec_result.stdout,
                        stderr: exec_result.stderr,
                        execution_time_ms: elapsed.as_millis() as u64,
                        peak_memory_bytes: exec_result.peak_memory_bytes,
                        result_hash: None,
                    };

                    // Serialize and store result in CAS
                    match serde_json::to_vec(&exec_result) {
                        Ok(result_json) => {
                            let result_hash = self.object_store.put_blob(&result_json)
                                .map_err(|e| json_rpc_error(-32002, format!("Result storage error: {e}")))?;

                            self.tasks_completed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                            // Propose completion to Raft
                            let _ = self.raft_proposal_tx
                                .send(Proposal::CompleteTask {
                                    task_id,
                                    result_hash: result_hash.clone(),
                                })
                                .await;

                            response["result_hash"] = serde_json::Value::String(result_hash);
                            info!(
                                "Task {task_id} executed inline: exit_code={}, time={elapsed:?}",
                                exec_result.exit_code
                            );
                        }
                        Err(e) => {
                            warn!("Failed to serialize execution result: {e}");
                        }
                    }
                }
                Err(e) => {
                    warn!("Inline execution failed for task {task_id}: {e}");
                    // Store the error as a result so the agent can see it
                    let error_result = ExecutionResult {
                        exit_code: -1,
                        stdout: vec![],
                        stderr: e.as_bytes().to_vec(),
                        execution_time_ms: 0,
                        peak_memory_bytes: 0,
                        result_hash: None,
                    };
                    if let Ok(result_json) = serde_json::to_vec(&error_result) {
                        if let Ok(result_hash) = self.object_store.put_blob(&result_json) {
                            response["result_hash"] = serde_json::Value::String(result_hash);
                        }
                    }
                }
            }
        }

        Ok(response)
    }

    /// Execute code inline using the appropriate runtime.
    ///
    /// For Python code, spawns ``python3 -c <code>`` as a subprocess.
    /// For Wasm, uses the Wasmtime sandbox.
    /// For other languages, attempts to detect and execute appropriately.
    fn execute_inline(
        &self,
        code_bytes: &[u8],
        code_language: &str,
        _timeout_ms: u64,
    ) -> Result<InlineResult, String> {
        match code_language.to_lowercase().as_str() {
            "python" | "py" => {
                // Execute Python code via subprocess
                let code_str = String::from_utf8_lossy(code_bytes).to_string();

                // Try python3 first, then python
                let python = if process::Command::new("python3")
                    .arg("--version")
                    .output()
                    .is_ok()
                {
                    "python3"
                } else {
                    "python"
                };

                let output = process::Command::new(python)
                    .arg("-c")
                    .arg(&code_str)
                    .stdout(process::Stdio::piped())
                    .stderr(process::Stdio::piped())
                    .output()
                    .map_err(|e| format!("Failed to spawn {python}: {e}"))?;

                Ok(InlineResult {
                    exit_code: output.status.code().unwrap_or(-1),
                    stdout: output.stdout,
                    stderr: output.stderr,
                    peak_memory_bytes: 0, // not tracked for subprocess
                })
            }
            "wasm" | "wat" => {
                // For Wasm: try to execute via WasmtimeSandbox if available
                // Fall back to reporting that container execution is needed
                #[cfg(target_arch = "wasm32")]
                {
                    Err("Wasm execution not supported in wasm32 target".into())
                }
                #[cfg(not(target_arch = "wasm32"))]
                {
                    // Try to compile and run via wasmtime
                    use eo_core::traits::Sandbox;
                    let sandbox = sandbox::wasm::WasmtimeSandbox::new()
                        .map_err(|e| format!("Failed to create Wasmtime sandbox: {e}"))?;
                    match sandbox.execute_code(code_bytes.to_vec()) {
                        Ok(result) => Ok(InlineResult {
                            exit_code: result.exit_code,
                            stdout: result.stdout,
                            stderr: result.stderr,
                            peak_memory_bytes: result.peak_memory_bytes,
                        }),
                        Err(e) => {
                            // If Wasm execution fails (e.g. invalid bytecode), report it
                            let msg = format!("Wasm execution failed: {e}");
                            warn!("{msg}");
                            Err(msg)
                        }
                    }
                }
            }
            "posix" | "shell" | "bash" => {
                // Execute shell code
                let code_str = String::from_utf8_lossy(code_bytes).to_string();
                let output = process::Command::new("bash")
                    .arg("-c")
                    .arg(&code_str)
                    .stdout(process::Stdio::piped())
                    .stderr(process::Stdio::piped())
                    .output()
                    .map_err(|e| format!("Failed to spawn bash: {e}"))?;

                Ok(InlineResult {
                    exit_code: output.status.code().unwrap_or(-1),
                    stdout: output.stdout,
                    stderr: output.stderr,
                    peak_memory_bytes: 0,
                })
            }
            _ => {
                // Unknown language — try as Python
                let code_str = String::from_utf8_lossy(code_bytes).to_string();
                let output = process::Command::new("python3")
                    .arg("-c")
                    .arg(&code_str)
                    .stdout(process::Stdio::piped())
                    .stderr(process::Stdio::piped())
                    .output()
                    .map_err(|e| format!("Failed to spawn python3: {e}"))?;

                Ok(InlineResult {
                    exit_code: output.status.code().unwrap_or(-1),
                    stdout: output.stdout,
                    stderr: output.stderr,
                    peak_memory_bytes: 0,
                })
            }
        }
    }

    async fn fetch_execution_result(&self, params: Value) -> Result<Value, JsonRpcErrorPayload> {
        let FetchParams { result_hash } = serde_json::from_value(params)
            .map_err(|e| json_rpc_error(-32602, format!("Invalid params: {e}")))?;

        let data = self.object_store.get_blob(&result_hash).map_err(|e| {
            let msg = format!("Object not found: {e}");
            warn!("fetch_execution_result: {msg}");
            json_rpc_error(-32004, msg)
        })?;

        // Try to deserialize as ExecutionResult (JSON)
        let result: eo_core::types::ExecutionResult = serde_json::from_slice(&data)
            .map_err(|e| json_rpc_error(-32005, format!("Corrupted result blob: {e}")))?;

        use base64::Engine;
        let stdout_b64 = base64::engine::general_purpose::STANDARD.encode(&result.stdout);
        let stderr_b64 = base64::engine::general_purpose::STANDARD.encode(&result.stderr);

        Ok(serde_json::json!({
            "exit_code": result.exit_code,
            "stdout": stdout_b64,
            "stderr": stderr_b64,
            "execution_time_ms": result.execution_time_ms,
            "peak_memory_bytes": result.peak_memory_bytes,
        }))
    }
}

// ── Inline execution result (before wrapping in CAS types) ───────────

struct InlineResult {
    exit_code: i32,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    peak_memory_bytes: u64,
}

// ── Helpers ────────────────────────────────────────────────────────────

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
        let mut handler = JsonRpcHandler::new(tx, store);
        handler.inline_execution = false; // disable for tests
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
    async fn submit_to_cas_and_raft_stores_blob_and_proposes() {
        let (handler, _dir, mut rx) = make_handler();
        let code = b"def hello(): return 42";

        use base64::Engine;
        let code_b64 = base64::engine::general_purpose::STANDARD.encode(code);

        let req = JsonRpcRequest {
            jsonrpc: "2.0".into(),
            method: "submit_to_cas_and_raft".into(),
            params: serde_json::json!({
                "code": code_b64,
                "required_runtime": "Wasm",
                "routing": "AnyExecutor",
            }),
            id: Value::Number(2.into()),
        };

        let resp = handler.handle(req).await;
        assert!(resp.error.is_none(), "unexpected error: {:?}", resp.error);

        let result = resp.result.unwrap();
        let code_hash = result["code_hash"].as_str().unwrap();
        let task_id = result["task_id"].as_str().unwrap();
        assert!(!code_hash.is_empty());
        assert!(!task_id.is_empty());

        // Verify blob is retrievable
        let hash_string = code_hash.to_string();
        let data = handler.object_store.get_blob(&hash_string).unwrap();
        assert_eq!(data, code);

        // Verify proposal was sent
        let proposal = rx.try_recv().unwrap();
        match proposal {
            Proposal::SubmitTask(task) => {
                assert_eq!(task.code_hash, code_hash);
                assert_eq!(task.task_id.to_string(), task_id);
            }
            other => panic!("expected SubmitTask, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn fetch_execution_result_retrieves_stored_result() {
        let (handler, _dir, _rx) = make_handler();

        // Store an ExecutionResult
        let exec_result = eo_core::types::ExecutionResult {
            exit_code: 0,
            stdout: b"Hello, world!".to_vec(),
            stderr: vec![],
            execution_time_ms: 42,
            peak_memory_bytes: 8192,
            result_hash: None,
        };
        let result_json = serde_json::to_vec(&exec_result).unwrap();
        let result_hash = handler.object_store.put_blob(&result_json).unwrap();

        let req = JsonRpcRequest {
            jsonrpc: "2.0".into(),
            method: "fetch_execution_result".into(),
            params: serde_json::json!({"result_hash": result_hash}),
            id: Value::Number(3.into()),
        };

        let resp = handler.handle(req).await;
        assert!(resp.error.is_none(), "unexpected error: {:?}", resp.error);

        let result = resp.result.unwrap();
        assert_eq!(result["exit_code"], 0);
        assert_eq!(result["execution_time_ms"], 42);
        assert_eq!(result["peak_memory_bytes"], 8192);

        // Decode stdout
        use base64::Engine;
        let stdout_bytes = base64::engine::general_purpose::STANDARD
            .decode(result["stdout"].as_str().unwrap())
            .unwrap();
        assert_eq!(stdout_bytes, b"Hello, world!");
    }

    #[tokio::test]
    async fn submit_rejects_oversized_code() {
        let (handler, _dir, _rx) = make_handler();
        // 65KB of zeros
        let big_code = vec![0u8; 65 * 1024];

        use base64::Engine;
        let code_b64 = base64::engine::general_purpose::STANDARD.encode(&big_code);

        let req = JsonRpcRequest {
            jsonrpc: "2.0".into(),
            method: "submit_to_cas_and_raft".into(),
            params: serde_json::json!({"code": code_b64}),
            id: Value::Number(4.into()),
        };

        let resp = handler.handle(req).await;
        assert!(resp.error.is_some());
        assert_eq!(resp.error.as_ref().unwrap().code, -32001);
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

    #[tokio::test]
    async fn inline_execution_python_code_succeeds() {
        let (mut handler, _dir, mut rx) = make_handler();
        handler.inline_execution = true;
        // Drain the channel setup
        while rx.try_recv().is_ok() {}

        let code = b"print('hello from sandbox')\n";
        use base64::Engine;
        let code_b64 = base64::engine::general_purpose::STANDARD.encode(code);

        let req = JsonRpcRequest {
            jsonrpc: "2.0".into(),
            method: "submit_to_cas_and_raft".into(),
            params: serde_json::json!({
                "code": code_b64,
                "code_language": "python",
                "required_runtime": "Wasm",
                "routing": "AnyExecutor",
            }),
            id: Value::Number(6.into()),
        };

        let resp = handler.handle(req).await;
        assert!(resp.error.is_none(), "unexpected error: {:?}", resp.error);
        let result = resp.result.unwrap();

        // Should have result_hash from inline execution
        let result_hash = result["result_hash"].as_str().unwrap();
        assert!(!result_hash.is_empty());

        // Fetch and verify the result
        let data = handler.object_store.get_blob(&result_hash.to_string()).unwrap();
        let exec_result: eo_core::types::ExecutionResult =
            serde_json::from_slice(&data).unwrap();
        assert_eq!(exec_result.exit_code, 0);
        let stdout = String::from_utf8_lossy(&exec_result.stdout);
        assert!(stdout.contains("hello from sandbox"), "got: {stdout}");
    }
}
