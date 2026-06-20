//! Task scheduler — routes tasks to execution nodes.
//!
//! Watches the cluster state for pending tasks and routes them to
//! appropriate execution nodes based on the routing strategy.

use std::collections::{HashMap, VecDeque};

use eo_core::types::{NodeId, Role, RoutingStrategy, ScheduledTask, TaskId};
use tracing::debug;

/// The task scheduler routes pending tasks to execution nodes.
#[derive(Default)]
pub struct TaskScheduler {
    /// Pending tasks waiting to be routed.
    pending: VecDeque<ScheduledTask>,

    /// Nodes currently holding the Execution role.
    executors: Vec<NodeId>,

    /// Node capabilities (for routing decisions).
    node_caps: HashMap<NodeId, Vec<String>>,
}

impl TaskScheduler {
    /// Create a new scheduler.
    pub fn new() -> Self {
        Self::default()
    }

    /// Submit a task for scheduling.
    pub fn submit(&mut self, task: ScheduledTask) {
        debug!("Task submitted: {}", task.task_id);
        self.pending.push_back(task);
    }

    /// Add or update an execution node.
    pub fn register_executor(&mut self, node_id: NodeId, _roles: Vec<Role>) {
        if !self.executors.contains(&node_id) {
            self.executors.push(node_id);
            debug!("Executor registered: {}", node_id);
        }
    }

    /// Remove an execution node.
    pub fn remove_executor(&mut self, node_id: &NodeId) {
        self.executors.retain(|id| id != node_id);
        debug!("Executor removed: {}", node_id);
    }

    /// Route pending tasks to available executors.
    ///
    /// Returns a list of (task_id, executor_node_id) assignments.
    pub fn route_pending(&mut self) -> Vec<(TaskId, NodeId)> {
        let mut assignments = Vec::new();

        if self.executors.is_empty() {
            return assignments;
        }

        let mut executor_idx = 0;

        while let Some(task) = self.pending.pop_front() {
            let executor = self.route_task(&task, &mut executor_idx);
            assignments.push((task.task_id, executor));
        }

        debug!("Routed {} tasks to executors", assignments.len());
        assignments
    }

    /// Route a single task to an executor based on its strategy.
    fn route_task(&self, task: &ScheduledTask, round_robin_idx: &mut usize) -> NodeId {
        match &task.routing {
            RoutingStrategy::Pinned(node_id) => *node_id,

            RoutingStrategy::PreferWasm => {
                // Find first executor that has Wasm capability
                self.executors
                    .iter()
                    .find(|id| {
                        self.node_caps
                            .get(id)
                            .map(|caps| caps.contains(&"wasm".to_string()))
                            .unwrap_or(false)
                    })
                    .copied()
                    .unwrap_or_else(|| self.round_robin(round_robin_idx))
            }

            RoutingStrategy::PreferNative => {
                // Find first executor that has native/container capability
                self.executors
                    .iter()
                    .find(|id| {
                        self.node_caps
                            .get(id)
                            .map(|caps| {
                                caps.contains(&"native".to_string())
                                    || caps.contains(&"container".to_string())
                            })
                            .unwrap_or(false)
                    })
                    .copied()
                    .unwrap_or_else(|| self.round_robin(round_robin_idx))
            }

            RoutingStrategy::AnyExecutor => self.round_robin(round_robin_idx),
        }
    }

    /// Simple round-robin executor selection.
    fn round_robin(&self, idx: &mut usize) -> NodeId {
        let executor = self.executors[*idx % self.executors.len()];
        *idx += 1;
        executor
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use eo_core::types::{ResourceLimits, RuntimeKind};

    fn make_task(strategy: RoutingStrategy) -> ScheduledTask {
        ScheduledTask {
            task_id: uuid::Uuid::new_v4(),
            code_hash: "abc123".into(),
            required_runtime: RuntimeKind::Wasm,
            routing: strategy,
            timeout_ms: 5000,
            resource_limits: ResourceLimits::default(),
            submitted_at: Utc::now(),
            pinned_node: None,
        }
    }

    #[test]
    fn task_routed_to_execution_node() {
        let mut sched = TaskScheduler::new();
        let node = uuid::Uuid::new_v4();

        sched.register_executor(node, vec![Role::Execution]);
        sched.submit(make_task(RoutingStrategy::AnyExecutor));

        let assignments = sched.route_pending();
        assert_eq!(assignments.len(), 1);
        assert_eq!(assignments[0].1, node);
    }

    #[test]
    fn no_routing_when_no_executors() {
        let mut sched = TaskScheduler::new();
        sched.submit(make_task(RoutingStrategy::AnyExecutor));

        let assignments = sched.route_pending();
        assert!(assignments.is_empty());
    }

    #[test]
    fn prefer_native_routes_to_capable_executor() {
        let mut sched = TaskScheduler::new();
        let wasm_node = uuid::Uuid::new_v4();
        let native_node = uuid::Uuid::new_v4();

        sched.register_executor(wasm_node, vec![Role::Execution]);
        sched.register_executor(native_node, vec![Role::Execution]);

        sched.node_caps.insert(wasm_node, vec!["wasm".into()]);
        sched
            .node_caps
            .insert(native_node, vec!["native".into(), "container".into()]);

        sched.submit(make_task(RoutingStrategy::PreferNative));

        let assignments = sched.route_pending();
        assert_eq!(assignments.len(), 1);
        assert_eq!(assignments[0].1, native_node);
    }
}
