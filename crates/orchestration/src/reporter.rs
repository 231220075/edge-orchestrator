//! Node health and metrics reporter.
//!
//! Collects and reports node health metrics to the orchestration engine.

use std::collections::HashMap;

use eo_core::types::NodeId;

/// Health status of a node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HealthStatus {
    /// Node is healthy and responsive.
    Healthy,
    /// Node may be experiencing issues.
    Degraded,
    /// Node is unreachable.
    Unhealthy,
    /// Node status is unknown.
    Unknown,
}

/// Report of a node's current health and metrics.
#[derive(Debug, Clone)]
pub struct NodeReport {
    /// The node being reported on.
    pub node_id: NodeId,
    /// Current health status.
    pub health: HealthStatus,
    /// Number of tasks completed by this node.
    pub tasks_completed: u64,
    /// Number of tasks currently in flight.
    pub tasks_in_flight: u64,
    /// Average execution time in milliseconds.
    pub avg_execution_time_ms: u64,
    /// Peer count (how many peers this node sees).
    pub peer_count: u32,
}

/// Collects and aggregates node reports.
#[derive(Default)]
pub struct Reporter {
    /// Latest report for each node.
    reports: HashMap<NodeId, NodeReport>,
}

impl Reporter {
    /// Create a new reporter.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a node report.
    pub fn report(&mut self, report: NodeReport) {
        self.reports.insert(report.node_id, report);
    }

    /// Remove a node's reports (node went offline).
    pub fn remove(&mut self, node_id: &NodeId) {
        self.reports.remove(node_id);
    }

    /// Get all currently unhealthy nodes.
    pub fn unhealthy_nodes(&self) -> Vec<NodeId> {
        self.reports
            .iter()
            .filter(|(_, r)| r.health == HealthStatus::Unhealthy)
            .map(|(id, _)| *id)
            .collect()
    }

    /// Get a summary of cluster health.
    pub fn cluster_summary(&self) -> ClusterSummary {
        let total = self.reports.len() as u64;
        let healthy = self
            .reports
            .values()
            .filter(|r| r.health == HealthStatus::Healthy)
            .count() as u64;

        ClusterSummary {
            total_nodes: total,
            healthy_nodes: healthy,
            total_tasks_completed: self.reports.values().map(|r| r.tasks_completed).sum(),
            total_tasks_in_flight: self.reports.values().map(|r| r.tasks_in_flight).sum(),
        }
    }
}

/// Summary of cluster health.
#[derive(Debug, Clone)]
pub struct ClusterSummary {
    pub total_nodes: u64,
    pub healthy_nodes: u64,
    pub total_tasks_completed: u64,
    pub total_tasks_in_flight: u64,
}
