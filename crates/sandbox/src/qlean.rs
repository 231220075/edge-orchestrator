// Qlean-based Linux sandbox: boots a KVM VM and executes build/run commands.
// Only available on Linux hosts with KVM; other platforms get a stub with the
// same name that returns an explicit UnsupportedPlatform error.

use eo_core::error::{CoreError, Result};
use eo_core::traits::ProjectSandbox;
use eo_core::types::{ExecutionResult, ProjectSpec};
#[cfg(target_os = "linux")]
mod linux {
    use super::*;
    use std::time::Instant;
    pub struct QleanSandbox {
        image: qlean::Image,
        config: qlean::MachineConfig,
    }
    impl QleanSandbox {
        pub fn new() -> Result<Self> {
            Ok(Self {
                image: qlean::Image::new(qlean::ImageConfig::default())
                    .map_err(|e| CoreError::SandboxExecution(format!("qlean image: {e}")))?,
                config: qlean::MachineConfig::default(),
            })
        }
    }
    impl ProjectSandbox for QleanSandbox {
        fn run_project(&self, spec: ProjectSpec) -> Result<ExecutionResult> {
            let start = Instant::now();
            let mut out = ExecutionResult {
                exit_code: 0,
                stdout: Vec::new(),
                stderr: Vec::new(),
                execution_time_ms: 0,
                peak_memory_bytes: 0,
                result_hash: None,
            };
            let build = spec.build_cmd.join(" ");
            let run = spec.run_cmd.join(" ");
            let res = qlean::with_machine(&self.image, &self.config, |vm| {
                Box::pin(async move {
                    let b = vm
                        .exec(&build)
                        .await
                        .map_err(|e| CoreError::SandboxExecution(format!("build: {e}")))?;
                    if !b.status.success() {
                        out.exit_code = b.status.code().unwrap_or(1);
                        out.stderr.extend_from_slice(&b.stderr);
                        out.execution_time_ms = start.elapsed().as_millis() as u64;
                        return Ok(());
                    }
                    let r = vm
                        .exec(&run)
                        .await
                        .map_err(|e| CoreError::SandboxExecution(format!("run: {e}")))?;
                    out.exit_code = r.status.code().unwrap_or(0);
                    out.stdout.extend_from_slice(&r.stdout);
                    out.stderr.extend_from_slice(&r.stderr);
                    out.execution_time_ms = start.elapsed().as_millis() as u64;
                    Ok(())
                })
            });
            res.map(|_| out)
                .map_err(|e| CoreError::SandboxExecution(format!("qlean: {e}")))
        }
    }
}

#[cfg(not(target_os = "linux"))]
pub struct QleanSandbox;
#[cfg(not(target_os = "linux"))]
impl ProjectSandbox for QleanSandbox {
    fn run_project(&self, _spec: ProjectSpec) -> Result<ExecutionResult> {
        Err(CoreError::UnsupportedPlatform(
            "Qlean sandbox requires a Linux host with KVM".into(),
        ))
    }
}
#[cfg(target_os = "linux")]
pub use linux::QleanSandbox;
