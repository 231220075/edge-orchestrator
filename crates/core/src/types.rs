//! Core types for the Edge-Cloud Orchestrator.
//!
//! Defines the fundamental data structures shared across all crates:
//! node descriptors, capabilities, tasks, roles, and execution results.

use chrono::{DateTime, Utc};
use multiaddr::Multiaddr;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Unique identifier for a node in the cluster.
pub type NodeId = Uuid;

/// Unique identifier for a scheduled task.
pub type TaskId = Uuid;

/// SHA-256 content hash, stored as a hex string.
pub type Hash = String;

/// Describes a node in the edge-orchestrator cluster.
///
/// This is the fundamental unit of cluster membership. Every node
/// advertises its descriptor to peers via the P2P descriptor exchange
/// protocol, and the descriptor is stored in the Raft-maintained
/// node registry.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeDescriptor {
    /// Unique identifier for this node (UUID v4).
    pub node_id: NodeId,
    /// Whether this is a Heavy (full-featured) or Light (constrained) node.
    pub node_type: NodeType,
    /// Operating system this node runs on.
    pub os: OsType,
    /// What this node is capable of doing.
    pub capabilities: Capabilities,
    /// Addresses this node can be reached at on the P2P network.
    pub advertised_addresses: Vec<Multiaddr>,
    /// Roles currently assigned to this node by the orchestration engine.
    pub current_assigned_roles: Vec<Role>,
    /// When this node started up.
    pub started_at: DateTime<Utc>,
}

/// Classification of a node's resource profile.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum NodeType {
    /// Full-featured node: persistent storage, multiple runtimes, high resources.
    Heavy,
    /// Constrained node (e.g., mobile, IoT): limited resources, may sleep/disconnect.
    Light,
}

/// Operating system running on a node.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum OsType {
    MacOS,
    Linux,
    Windows,
    Ios,
    Android,
    Unknown,
}

/// The set of capabilities a node advertises to the cluster.
///
/// The orchestration engine uses capabilities to match nodes to roles
/// and to route tasks to appropriate executors.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Capabilities {
    /// Whether this node has persistent disk storage available.
    pub storage: bool,
    /// Whether GPU acceleration is available.
    pub gpu_acceleration: bool,
    /// Which execution runtimes this node supports.
    pub runtimes: Vec<RuntimeKind>,
    /// Maximum memory available for sandbox execution, in megabytes.
    pub max_memory_mb: u64,
    /// Number of CPU cores available for execution.
    pub cpu_cores: u32,
}

impl Default for Capabilities {
    fn default() -> Self {
        Self {
            storage: true,
            gpu_acceleration: false,
            runtimes: vec![RuntimeKind::Wasm],
            max_memory_mb: 1024,
            cpu_cores: 2,
        }
    }
}

/// Kinds of execution runtimes a node can provide.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum RuntimeKind {
    /// WebAssembly runtime (Wasmtime) — available on all platforms.
    Wasm,
    /// Native POSIX process execution (Linux container/namespace sandbox).
    NativePosix,
    /// Docker/OCI container execution.
    Container,
}

/// A role assigned to a node by the orchestration engine.
///
/// Roles determine what services a node runs. The node's bootstrap
/// monitors its assigned roles and starts/stops services accordingly.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Role {
    /// Storage node: serves CAS objects to peers.
    Storage,
    /// Execution node: runs sandboxed code.
    Execution,
    /// Inference node: runs ML/AI inference workloads.
    Inference,
    /// Coordinator node: runs the orchestration engine and scheduler.
    Coordinator,
    /// Bootstrap node: initial cluster seed.
    Bootstrap,
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Role::Storage => write!(f, "Storage"),
            Role::Execution => write!(f, "Execution"),
            Role::Inference => write!(f, "Inference"),
            Role::Coordinator => write!(f, "Coordinator"),
            Role::Bootstrap => write!(f, "Bootstrap"),
        }
    }
}

/// A unit of work scheduled onto the cluster.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScheduledTask {
    /// Unique identifier for this task.
    pub task_id: TaskId,
    /// The code/bytecode to execute, content-addressed by its hash.
    pub code_hash: Hash,
    /// What kind of runtime is required to execute this task.
    pub required_runtime: RuntimeKind,
    /// Preferred routing strategy for the scheduler.
    pub routing: RoutingStrategy,
    /// Execution timeout in milliseconds.
    pub timeout_ms: u64,
    /// Resource limits for sandbox execution.
    pub resource_limits: ResourceLimits,
    /// When this task was submitted.
    pub submitted_at: DateTime<Utc>,
    /// Optional: pin execution to a specific node.
    pub pinned_node: Option<NodeId>,
}

/// Routing strategy for task-to-node assignment.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RoutingStrategy {
    /// Send to any node with the Execution role.
    AnyExecutor,
    /// Send to a specific node.
    Pinned(NodeId),
    /// Prefer a Wasm executor if available.
    PreferWasm,
    /// Prefer a native (container/process) executor if available.
    PreferNative,
}

/// Resource limits enforced by the sandbox layer during execution.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResourceLimits {
    /// Maximum memory in megabytes the sandbox can use.
    pub max_memory_mb: u64,
    /// Maximum CPU time in milliseconds.
    pub max_cpu_time_ms: u64,
    /// Maximum disk space in megabytes (for WASI preopened dirs).
    pub max_disk_mb: u64,
    /// Whether network access is allowed.
    pub allow_network: bool,
    /// Maximum number of WASI file descriptors.
    pub max_fds: u32,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            max_memory_mb: 256,
            max_cpu_time_ms: 30_000,
            max_disk_mb: 100,
            allow_network: false,
            max_fds: 64,
        }
    }
}

/// Result of executing code in a sandbox.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExecutionResult {
    /// Process exit code (0 = success).
    pub exit_code: i32,
    /// Captured stdout.
    pub stdout: Vec<u8>,
    /// Captured stderr.
    pub stderr: Vec<u8>,
    /// Wall-clock execution time in milliseconds.
    pub execution_time_ms: u64,
    /// Peak memory usage during execution, in bytes.
    pub peak_memory_bytes: u64,
    /// Content hash of the result stored in CAS (if persisted).
    pub result_hash: Option<Hash>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_descriptor_serde_roundtrip() {
        let desc = NodeDescriptor {
            node_id: Uuid::new_v4(),
            node_type: NodeType::Heavy,
            os: OsType::MacOS,
            capabilities: Capabilities::default(),
            advertised_addresses: vec![],
            current_assigned_roles: vec![Role::Execution],
            started_at: Utc::now(),
        };

        let json = serde_json::to_string(&desc).expect("serialize");
        let desc2: NodeDescriptor = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(desc, desc2);
    }

    #[test]
    fn capabilities_default_has_wasm() {
        let caps = Capabilities::default();
        assert!(caps.runtimes.contains(&RuntimeKind::Wasm));
        assert!(!caps.gpu_acceleration);
    }

    #[test]
    fn resource_limits_default_sane() {
        let limits = ResourceLimits::default();
        assert_eq!(limits.max_memory_mb, 256);
        assert!(!limits.allow_network);
    }

    #[test]
    fn role_display_formatting() {
        assert_eq!(Role::Storage.to_string(), "Storage");
        assert_eq!(Role::Execution.to_string(), "Execution");
        assert_eq!(Role::Inference.to_string(), "Inference");
    }

    #[test]
    fn scheduled_task_serde_roundtrip() {
        let task = ScheduledTask {
            task_id: Uuid::new_v4(),
            code_hash: "abc123".into(),
            required_runtime: RuntimeKind::Wasm,
            routing: RoutingStrategy::AnyExecutor,
            timeout_ms: 5000,
            resource_limits: ResourceLimits::default(),
            submitted_at: Utc::now(),
            pinned_node: None,
        };

        let json = serde_json::to_string(&task).expect("serialize");
        let task2: ScheduledTask = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(task, task2);
    }

    #[test]
    fn execution_result_serde_roundtrip() {
        let result = ExecutionResult {
            exit_code: 0,
            stdout: b"hello world".to_vec(),
            stderr: vec![],
            execution_time_ms: 42,
            peak_memory_bytes: 1024 * 1024,
            result_hash: Some("def456".into()),
        };

        let json = serde_json::to_string(&result).expect("serialize");
        let result2: ExecutionResult = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(result, result2);
    }
}
