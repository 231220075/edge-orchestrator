//! IPC server for the Edge-Cloud Orchestrator.
//!
//! Provides a Unix Domain Socket JSON-RPC 2.0 server that bridges
//! external clients (e.g., the Python ``eo-agent``) to the Rust
//! orchestration, storage, and raft subsystems.
//!
//! ## Protocol
//!
//! Newline-delimited JSON-RPC 2.0: each request is a single JSON line,
//! each response is a single JSON line. Connection is closed after the
//! response (stateless, one-shot).
//!
//! ## Methods
//!
//! | Method | Params | Returns |
//! |--------|--------|---------|
//! | ``get_cluster_topology`` | ``{}`` | Nodes, roles, task counts |
//! | ``submit_to_cas_and_raft`` | ``{code, ...}`` | ``{code_hash, task_id}`` |
//! | ``fetch_execution_result`` | ``{result_hash}`` | ExecutionResult fields |

pub mod handler;
pub mod server;

pub use handler::JsonRpcHandler;
pub use server::IpcServer;
