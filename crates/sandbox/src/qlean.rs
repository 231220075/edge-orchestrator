// Qlean-based Linux sandbox: boots a KVM VM and executes build/run commands.
// Only available on Linux hosts with KVM; other platforms get a stub with the
// same name that returns an explicit UnsupportedPlatform error.

use eo_core::error::{CoreError, Result};
use eo_core::traits::ProjectSandbox;
use eo_core::types::{ExecutionResult, ProjectSpec};

#[cfg(target_os = "linux")]
mod linux {
    use super::*;
    use std::path::Path;
    use std::time::Instant;

    pub struct QleanSandbox {
        image: qlean::Image,
        config: qlean::MachineConfig,
    }

    impl QleanSandbox {
        pub async fn new() -> Result<Self> {
            let image = qlean::Image::new(qlean::ImageConfig::default())
                .await
                .map_err(|e| CoreError::SandboxExecution(format!("qlean image: {e}")))?;
            Ok(Self {
                image,
                config: qlean::MachineConfig::default(),
            })
        }
    }

    impl ProjectSandbox for QleanSandbox {
        async fn run_project(&self, spec: ProjectSpec) -> Result<ExecutionResult> {
            let local_dir = spec.local_project_dir.clone();
            let work_dir = spec.work_dir.clone();
            let build = spec.build_cmd.join(" ");
            let run = spec.run_cmd.join(" ");
            let timeout_ms = spec.timeout_ms;

            let result = qlean::with_machine(&self.image, &self.config, |vm| {
                Box::pin(async move {
                    let start = Instant::now();
                    let mut out = ExecutionResult {
                        exit_code: 0,
                        stdout: Vec::new(),
                        stderr: Vec::new(),
                        execution_time_ms: 0,
                        peak_memory_bytes: 0,
                        result_hash: None,
                    };

                    if let Some(dir) = &local_dir {
                        if Path::new(dir).is_dir() {
                            // qlean upload mirrors a directory into
                            // remote_path/basename; upload into the parent of
                            // work_dir to land exactly at work_dir.
                            let parent = Path::new(&work_dir)
                                .parent()
                                .map(|p| p.to_path_buf())
                                .unwrap_or_else(|| Path::new("/root").to_path_buf());
                            vm.upload(dir, parent).await?;
                        }
                    }

                    if !build.is_empty() {
                        let cmd = format!("cd {} && {}", work_dir, build);
                        let b = vm.exec(&cmd).await?;
                        out.stdout.extend_from_slice(&b.stdout);
                        out.stderr.extend_from_slice(&b.stderr);
                        if !b.status.success() {
                            out.exit_code = b.status.code().unwrap_or(1);
                            out.execution_time_ms = start.elapsed().as_millis() as u64;
                            return Ok(out);
                        }
                    }

                    if start.elapsed().as_millis() as u64 > timeout_ms {
                        out.exit_code = 124;
                        out.stderr
                            .extend_from_slice(b"timeout: project execution exceeded budget");
                        out.execution_time_ms = start.elapsed().as_millis() as u64;
                        return Ok(out);
                    }

                    let run_cmd = format!("cd {} && {}", work_dir, run);
                    let r = vm.exec(&run_cmd).await?;
                    out.exit_code = r.status.code().unwrap_or(0);
                    out.stdout.extend_from_slice(&r.stdout);
                    out.stderr.extend_from_slice(&r.stderr);
                    out.execution_time_ms = start.elapsed().as_millis() as u64;
                    Ok(out)
                })
            })
            .await
            .map_err(|e| CoreError::SandboxExecution(format!("qlean run: {e}")))?;

            Ok(result)
        }
    }
}

#[cfg(not(target_os = "linux"))]
pub struct QleanSandbox;
#[cfg(not(target_os = "linux"))]
impl ProjectSandbox for QleanSandbox {
    async fn run_project(&self, _spec: ProjectSpec) -> Result<ExecutionResult> {
        Err(CoreError::UnsupportedPlatform(
            "Qlean sandbox requires a Linux host with KVM".into(),
        ))
    }
}
#[cfg(target_os = "linux")]
pub use linux::QleanSandbox;
