// Qlean-based Linux sandbox: boots a KVM VM and executes build/run commands.
// Only available on Linux hosts with KVM; other platforms get a stub.

use eo_core::error::{CoreError, Result};
use eo_core::traits::ProjectSandbox;
use eo_core::types::{ExecutionResult, ProjectSpec};

#[cfg(target_os = "linux")]
mod linux {
    use super::*;
    use std::path::Path;
    use std::time::Instant;

    pub struct QleanSandbox {
        image_config: qlean::ImageConfig,
        machine_config: qlean::MachineConfig,
    }

    impl QleanSandbox {
        pub fn new() -> Result<Self> {
            Ok(Self {
                image_config: qlean::ImageConfig::default(),
                machine_config: qlean::MachineConfig::default(),
            })
        }
    }

    impl ProjectSandbox for QleanSandbox {
        fn run_project(&self, spec: ProjectSpec) -> Result<ExecutionResult> {
            let image_cfg = self.image_config.clone();
            let machine_cfg = self.machine_config.clone();

            // qlean futures are not Send, so run on a dedicated thread with
            // its own single-thread runtime and join.
            let handle = std::thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| CoreError::Internal(format!("runtime: {e}")))?;
                rt.block_on(async move {
                    let image = qlean::Image::new(image_cfg)
                        .await
                        .map_err(|e| CoreError::SandboxExecution(format!("image: {e}")))?;

                    let outcome: anyhow::Result<ExecutionResult> =
                        qlean::with_machine(&image, &machine_cfg, |vm| {
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

                                if let Some(dir) = &spec.local_project_dir {
                                    if Path::new(dir).is_dir() {
                                        let work = spec.work_dir.clone();
                                        let parent = Path::new(&work)
                                            .parent()
                                            .map(|p| p.to_path_buf())
                                            .unwrap_or_else(|| Path::new("/root").to_path_buf());
                                        vm.upload(dir, parent).await?;
                                    }
                                }

                                let work = spec.work_dir.clone();
                                if !spec.build_cmd.is_empty() {
                                    let cmd =
                                        format!("cd {} && {}", work, spec.build_cmd.join(" "));
                                    let b = vm.exec(&cmd).await?;
                                    out.stdout.extend_from_slice(&b.stdout);
                                    out.stderr.extend_from_slice(&b.stderr);
                                    if !b.status.success() {
                                        out.exit_code = b.status.code().unwrap_or(1);
                                        out.execution_time_ms = start.elapsed().as_millis() as u64;
                                        return Ok(out);
                                    }
                                }

                                if start.elapsed().as_millis() as u64 > spec.timeout_ms {
                                    out.exit_code = 124;
                                    out.stderr.extend_from_slice(b"timeout exceeded");
                                    out.execution_time_ms = start.elapsed().as_millis() as u64;
                                    return Ok(out);
                                }

                                let run_cmd = format!("cd {} && {}", work, spec.run_cmd.join(" "));
                                let r = vm.exec(&run_cmd).await?;
                                out.exit_code = r.status.code().unwrap_or(0);
                                out.stdout.extend_from_slice(&r.stdout);
                                out.stderr.extend_from_slice(&r.stderr);
                                out.execution_time_ms = start.elapsed().as_millis() as u64;
                                Ok(out)
                            })
                        })
                        .await;

                    outcome.map_err(|e| CoreError::SandboxExecution(format!("qlean: {e}")))
                })
            });

            handle
                .join()
                .map_err(|_| CoreError::Internal("qlean thread panicked".into()))?
        }
    }
}

#[cfg(not(target_os = "linux"))]
pub struct QleanSandbox;
#[cfg(not(target_os = "linux"))]
impl QleanSandbox {
    pub fn new() -> Result<Self> {
        Ok(Self)
    }
}
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
