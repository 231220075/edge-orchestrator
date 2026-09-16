#![allow(dead_code)]
#![allow(unused_imports)]
//! Edge-Cloud Orchestrator — Orchestration Engine
//!
//! Provides the role orchestration engine, runtime loops (node registration,
//! coordinator/executor proposals), topology spec parser and health reporter.
//!
//! The standalone `TaskScheduler` was removed with the Wasm lane: it was never
//! instantiated (it only routed `RuntimeKind::Wasm` tasks by string capability
//! names) — see `docs/v3-modules/22-wasm-lane移除记录.md`.

pub mod reporter;
pub mod role_engine;
pub mod runtime_loop;
pub mod topology;

pub use reporter::{HealthStatus, NodeReport, Reporter};
pub use role_engine::RoleOrchestrationEngine;
pub use topology::{diff_topology, parse_topology_spec, ClusterTopologySpec};
