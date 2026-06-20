//! Edge-Cloud Orchestrator — Polymorphic Sandbox Layer
//!
//! Provides execution sandboxes for untrusted code:
//! - **Wasmtime**: WebAssembly sandbox (all platforms)
//! - **LinuxContainer**: Namespace+cgroup isolation (Linux only)

pub mod container;
pub mod registry;
pub mod wasm;

// Re-export core traits
pub use eo_core::traits::Sandbox;
pub use registry::{default_registry, SandboxFactory, SandboxRegistry};
