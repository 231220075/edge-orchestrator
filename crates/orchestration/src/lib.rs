//! Edge-Cloud Orchestrator — Orchestration Engine
//!
//! Provides the role orchestration engine, task scheduler,
//! topology spec parser, and health reporter.

pub mod reporter;
pub mod role_engine;
pub mod scheduler;
pub mod topology;

pub use reporter::{HealthStatus, NodeReport, Reporter};
pub use role_engine::RoleOrchestrationEngine;
pub use scheduler::TaskScheduler;
pub use topology::{diff_topology, parse_topology_spec, ClusterTopologySpec};
