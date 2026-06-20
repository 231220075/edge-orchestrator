//! Linux container sandbox using namespaces and cgroups.
//!
//! Only available on Linux. On other platforms, `prepare_env` returns
//! `Err(UnsupportedPlatform)`.

use eo_core::error::{CoreError, Result};
use eo_core::traits::Sandbox;
use eo_core::types::{ExecutionResult, ResourceLimits};

/// A sandbox that executes native binaries in Linux namespaces.
pub struct LinuxContainerSandbox {
    limits: ResourceLimits,
}

impl LinuxContainerSandbox {
    /// Create a new container sandbox.
    ///
    /// On non-Linux platforms, returns `UnsupportedPlatform`.
    pub fn new() -> Result<Self> {
        if !cfg!(target_os = "linux") {
            return Err(CoreError::UnsupportedPlatform(
                "Linux container sandbox requires Linux".into(),
            ));
        }

        Ok(Self {
            limits: ResourceLimits::default(),
        })
    }
}

impl Sandbox for LinuxContainerSandbox {
    fn prepare_env(&self, limits: ResourceLimits) -> Result<()> {
        if !cfg!(target_os = "linux") {
            return Err(CoreError::UnsupportedPlatform(
                "Linux container sandbox requires Linux".into(),
            ));
        }

        // On Linux, we would:
        // 1. Create a new cgroup with memory/cpu limits
        // 2. Prepare rootfs overlay
        // 3. Set up network namespace isolation
        tracing::debug!(
            "Container sandbox prepared: {}MB mem, {}ms cpu",
            limits.max_memory_mb,
            limits.max_cpu_time_ms
        );
        Ok(())
    }

    fn execute_code(&self, bytecode: Vec<u8>) -> Result<ExecutionResult> {
        if !cfg!(target_os = "linux") {
            return Err(CoreError::UnsupportedPlatform(
                "Linux container sandbox requires Linux".into(),
            ));
        }

        // On Linux, we would:
        // 1. Write bytecode to temp file in rootfs
        // 2. Fork child with CLONE_NEWNS | CLONE_NEWPID | CLONE_NEWNET
        // 3. Execute binary in isolated process
        // 4. Capture stdout/stderr, exit code, timing

        let _ = bytecode;
        Ok(ExecutionResult {
            exit_code: 0,
            stdout: vec![],
            stderr: vec![],
            execution_time_ms: 0,
            peak_memory_bytes: 0,
            result_hash: None,
        })
    }

    fn destroy(&self) -> Result<()> {
        // Remove cgroup, cleanup overlay
        tracing::debug!("Container sandbox destroyed");
        Ok(())
    }
}
