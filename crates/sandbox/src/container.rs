//! Linux container sandbox (namespace + cgroup v2) honest stub for v3.
//! execute_code returns an explicit error instead of a fake exit_code.

use eo_core::error::{CoreError, Result};
use eo_core::traits::Sandbox;
use eo_core::types::{ExecutionResult, ResourceLimits};

pub struct LinuxContainerSandbox {
    // Held for the future namespace/cgroup implementation; currently unused
    // because execute_code is an explicit stub (see module docs).
    #[allow(dead_code)]
    limits: ResourceLimits,
}

impl LinuxContainerSandbox {
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
        tracing::debug!(
            "Container sandbox prepared (stub): {}MB mem, {}ms cpu",
            limits.max_memory_mb,
            limits.max_cpu_time_ms
        );
        Ok(())
    }

    fn execute_code(&self, _bytecode: Vec<u8>) -> Result<ExecutionResult> {
        Err(CoreError::SandboxExecution(
            "container sandbox is not implemented in v3 (documented stub)".into(),
        ))
    }

    fn destroy(&self) -> Result<()> {
        tracing::debug!("Container sandbox destroyed");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn execute_is_an_honest_error() {
        // On non-Linux platforms new() fails; on Linux it constructs but is a stub.
        if let Ok(sandbox) = LinuxContainerSandbox::new() {
            let result = sandbox.execute_code(Vec::new());
            assert!(result.is_err());
        }
    }
}
