//! Edge-Cloud Orchestrator — Sandbox Layer
//!
//! Provides execution sandboxes:
//! - **Qlean**: KVM virtual machine sandbox for complete projects
//!   (workspace + build/run commands). Linux + KVM only.
//!
//! The WebAssembly lane (Wasmtime) and its `Sandbox` trait, container stub and
//! `SandboxRegistry` were removed: they could not execute a workspace at all
//! (they took pre-compiled bytecode), nothing in the product path used the
//! registry, and the `ScheduledTask` fields they depended on were decorative.
//! See `docs/v3-modules/22-wasm-lane移除记录.md`.

pub mod qlean;

pub use eo_core::traits::ProjectSandbox;
pub use qlean::QleanSandbox;
