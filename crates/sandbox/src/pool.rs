//! Pre-booted VM pool policy.
//!
//! Why this exists: in `VmMode::Fresh` the machine (and its private overlay disk)
//! is thrown away after every task, so the next task pays a full VM boot. Booting
//! the replacement *while the worker is idle* turns that cost into background
//! latency instead of user-visible latency.
//!
//! What this does NOT do (deliberately, and stated so it is not mistaken for a
//! solution): the pooled machines are freshly booted from the base image, so they
//! carry no toolchain. A task that needs `gcc` still installs it, on its own
//! disposable disk, every time. Making `Fresh` cheap *and* practical therefore
//! needs a template image with the toolchain baked in (`ImageConfig.source`
//! already supports pointing at one) — the pool only removes the boot wait.
//!
//! The policy is pure logic on purpose: `qlean::Machine` is not `Send`, so the
//! pool itself lives on the sandbox worker thread and cannot be unit tested on a
//! dev host. Keeping the decisions in [`PoolPlan`] means the part that can be
//! wrong is covered by tests anywhere, and the worker only executes the plan.

/// What the worker should do with pre-booted machines after a job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolAction {
    /// Keep the machine that just ran (reuse mode).
    Keep,
    /// Discard the machine that just ran (fresh mode); its disk is per-task.
    Discard,
    /// Boot an extra machine up to `target` to hide the next task's boot.
    Refill { target: usize },
}

/// Resolved pool policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolPlan {
    /// Whether a machine survives a task.
    pub reuse: bool,
    /// How many idle machines to keep ready (0 when not pooling).
    pub target: usize,
}

/// Upper bound on pooled machines: each one is a booted VM holding CPU/RAM, and
/// the pool must never grow past what the operator asked for by accident.
pub const MAX_POOL_TARGET: usize = 8;

impl PoolPlan {
    /// Build a plan for a mode and a configured pool size.
    ///
    /// `Reuse` never pools more than the one machine it already keeps; a
    /// configured size of 0 disables pooling entirely (fresh boots on demand).
    pub fn new(reuse: bool, configured_size: usize) -> Self {
        let target = if reuse {
            // Reuse keeps exactly one live machine across tasks.
            0
        } else {
            configured_size.min(MAX_POOL_TARGET)
        };
        Self { reuse, target }
    }

    /// What to do after finishing a job.
    pub fn after_job(&self) -> Vec<PoolAction> {
        if self.reuse {
            vec![PoolAction::Keep]
        } else if self.target == 0 {
            vec![PoolAction::Discard]
        } else {
            vec![
                PoolAction::Discard,
                PoolAction::Refill {
                    target: self.target,
                },
            ]
        }
    }

    /// How many boots are needed to reach the target from `idle`.
    pub fn boots_needed(&self, idle: usize) -> usize {
        self.target.saturating_sub(idle)
    }

    /// Whether the worker should use a pooled machine instead of booting one.
    pub fn take_from_pool_when_available(&self) -> bool {
        !self.reuse && self.target > 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reuse_mode_keeps_one_machine_and_never_pools() {
        let plan = PoolPlan::new(true, 4);
        assert_eq!(plan.target, 0, "reuse must not pre-boot extra VMs");
        assert_eq!(plan.after_job(), vec![PoolAction::Keep]);
        assert!(!plan.take_from_pool_when_available());
    }

    #[test]
    fn fresh_without_a_pool_discards_everything() {
        let plan = PoolPlan::new(false, 0);
        assert_eq!(plan.after_job(), vec![PoolAction::Discard]);
        assert_eq!(plan.boots_needed(0), 0);
        assert!(!plan.take_from_pool_when_available());
    }

    #[test]
    fn fresh_with_a_pool_discards_then_refills() {
        let plan = PoolPlan::new(false, 2);
        assert_eq!(
            plan.after_job(),
            vec![PoolAction::Discard, PoolAction::Refill { target: 2 }]
        );
        assert!(plan.take_from_pool_when_available());
        assert_eq!(plan.boots_needed(2), 0, "a full pool needs no boots");
        assert_eq!(plan.boots_needed(0), 2);
    }

    #[test]
    fn pool_size_is_capped() {
        let plan = PoolPlan::new(false, MAX_POOL_TARGET + 100);
        assert_eq!(plan.target, MAX_POOL_TARGET);
    }
}
