//! Shared traits for sandbox execution and content-addressed storage.
//!
//! These traits define the plugin points that allow the orchestration
//! layer to work with different sandbox backends (Wasm, containers, etc.)
//! and different storage backends (local filesystem, distributed CAS).

use crate::error::Result;
use crate::types::{ExecutionResult, Hash, ResourceLimits};

// ---------------------------------------------------------------------------
// Sandbox trait
// ---------------------------------------------------------------------------

/// A polymorphic sandbox for executing untrusted code.
///
/// Implementations include:
/// - [`WasmtimeSandbox`] — WebAssembly via Wasmtime (all platforms)
/// - [`LinuxContainerSandbox`] — Linux namespaces + cgroups (Linux only)
///
/// [`WasmtimeSandbox`]: (wasmtime)
/// [`LinuxContainerSandbox`]: (container)
pub trait Sandbox: Send + Sync {
    /// Prepare the execution environment with the given resource limits.
    ///
    /// This is called before `execute_code` to set up cgroups, WASI
    /// preopens, memory limits, etc. Must be idempotent — calling it
    /// multiple times with the same limits should be safe.
    fn prepare_env(&self, limits: ResourceLimits) -> Result<()>;

    /// Execute bytecode inside the sandbox.
    ///
    /// # Arguments
    /// * `bytecode` — The compiled module (Wasm `.wasm` bytes, native ELF, etc.)
    ///
    /// # Returns
    /// An [`ExecutionResult`] containing exit code, captured stdout/stderr,
    /// timing, and memory usage.
    fn execute_code(&self, bytecode: Vec<u8>) -> Result<ExecutionResult>;

    /// Tear down the sandbox, freeing all resources.
    ///
    /// After `destroy` is called, the sandbox should not be reused.
    fn destroy(&self) -> Result<()>;
}

// ---------------------------------------------------------------------------
// ObjectStore trait
// ---------------------------------------------------------------------------

/// A tree entry within a Tree object in the content-addressed store.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TreeEntry {
    /// File or directory name.
    pub name: String,
    /// SHA-256 hash of the object this entry points to.
    pub hash: Hash,
    /// Whether this entry is a Blob, Tree, or Executable.
    pub mode: ObjectMode,
}

/// The type of object a tree entry references.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ObjectMode {
    /// Regular file (blob).
    Blob,
    /// Directory (tree).
    Tree,
    /// Executable file (blob with +x).
    Executable,
}

/// Content-addressed object storage following the Git model.
///
/// Objects are content-addressed by SHA-256 hash. The store supports
/// four object types following Git's model:
///
/// - **Blob**: Arbitrary bytes.
/// - **Tree**: A directory listing of (name, hash, mode) entries.
/// - **Commit**: A pointer to a Tree with metadata (author, timestamp, message).
/// - **Tag**: A named reference to a Commit (used for snapshots).
///
/// Implementations can be local (on-disk) or distributed (P2P).
pub trait ObjectStore: Send + Sync {
    /// Store arbitrary bytes and return their content hash.
    fn put_blob(&self, data: &[u8]) -> Result<Hash>;

    /// Retrieve a blob by its content hash.
    fn get_blob(&self, hash: &Hash) -> Result<Vec<u8>>;

    /// Store a directory tree and return its content hash.
    ///
    /// The tree's hash is computed from the sorted entries, so
    /// identical trees produce identical hashes.
    fn put_tree(&self, entries: Vec<TreeEntry>) -> Result<Hash>;

    /// Retrieve a directory tree by its content hash.
    fn get_tree(&self, hash: &Hash) -> Result<Vec<TreeEntry>>;

    /// Create a commit pointing to a tree and return the commit hash.
    ///
    /// # Arguments
    /// * `tree_hash` — The hash of the tree this commit points to.
    /// * `parent_hashes` — Parent commit hashes (empty for initial commit).
    /// * `author` — Author identifier for this commit.
    /// * `message` — Human-readable description of the change.
    fn commit(
        &self,
        tree_hash: Hash,
        parent_hashes: Vec<Hash>,
        author: &str,
        message: &str,
    ) -> Result<Hash>;

    /// Check whether an object with the given hash exists in the store.
    fn exists(&self, hash: &Hash) -> bool;
}

// ---------------------------------------------------------------------------
// Runtime trait (for future use)
// ---------------------------------------------------------------------------

/// A runtime that can manage the lifecycle of sandboxes and storage.
///
/// This trait is extended by components that coordinate multiple
/// subsystems (sandbox + storage + network).
pub trait Runtime: Send + Sync {
    /// Start the runtime, initializing all subsystems.
    fn start(&self) -> Result<()>;

    /// Perform a graceful shutdown of all subsystems.
    fn shutdown(&self) -> Result<()>;

    /// Check whether the runtime is healthy.
    fn is_healthy(&self) -> bool;
}
