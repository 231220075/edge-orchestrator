// Qlean-based Linux sandbox.
//
// A persistent worker thread owns one KVM VM (booted once) and reuses it
// across project runs, avoiding per-task VM boot + cloud-init cost.
// Only available on Linux hosts with KVM; other platforms get a stub.

use eo_core::error::{CoreError, Result};
use eo_core::traits::ProjectSandbox;
use eo_core::types::{ExecutionResult, ProjectSpec};

#[cfg(target_os = "linux")]
mod linux {
    use super::*;
    use std::path::Path;
    use std::sync::mpsc as std_mpsc;
    use std::time::{Duration, Instant};
    use tokio::time::timeout;

    /// Hard ceiling for cold-path stages regardless of the task budget: a stuck
    /// `Image` package download or a VM that never finishes cloud-init must fail
    /// loudly instead of hanging the single qlean worker forever.
    const IMAGE_PREPARE_TIMEOUT: Duration = Duration::from_secs(300);
    const BOOT_TIMEOUT: Duration = Duration::from_secs(600);
    const UPLOAD_TIMEOUT: Duration = Duration::from_secs(300);

    /// Per-phase slices of the task budget, used when the caller gave a small
    /// `timeout_ms` (the default 300s would otherwise grant every phase 300s).
    #[derive(Clone, Copy)]
    enum PhaseBudget {
        Image,
        Boot,
        Upload,
        Build,
        Run,
    }

    fn phase_budget(deadline: Instant, phase: PhaseBudget) -> Duration {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let share = match phase {
            PhaseBudget::Image => remaining / 3,
            PhaseBudget::Boot => remaining / 2,
            PhaseBudget::Upload | PhaseBudget::Build => remaining,
            PhaseBudget::Run => remaining.saturating_sub(Duration::from_secs(5)),
        };
        let cap = match phase {
            PhaseBudget::Image => IMAGE_PREPARE_TIMEOUT,
            PhaseBudget::Boot => BOOT_TIMEOUT,
            PhaseBudget::Upload => UPLOAD_TIMEOUT,
            PhaseBudget::Build | PhaseBudget::Run => Duration::from_secs(1800),
        };
        share.min(cap).max(Duration::from_secs(1))
    }

    fn deadline_in(ms: u64) -> Instant {
        Instant::now() + Duration::from_millis(ms)
    }

    /// Human-readable rendering of a sandbox error chain (anyhow inside).
    fn describe_err(e: &CoreError) -> String {
        format!("{e:#}")
    }

    struct Job {
        spec: ProjectSpec,
        deadline: Instant,
        reply: std_mpsc::Sender<Result<ExecutionResult>>,
    }

    pub struct QleanSandbox {
        tx: std_mpsc::Sender<Job>,
    }

    impl QleanSandbox {
        pub fn new() -> Result<Self> {
            let (tx, rx) = std_mpsc::channel::<Job>();
            std::thread::Builder::new()
                .name("qlean-worker".into())
                .spawn(move || worker(rx))
                .map_err(|e| CoreError::Internal(format!("spawn qlean worker: {e}")))?;
            Ok(Self { tx })
        }
    }

    impl ProjectSandbox for QleanSandbox {
        fn run_project(&self, spec: ProjectSpec) -> Result<ExecutionResult> {
            let timeout_ms = spec.timeout_ms.max(1000);
            let deadline = deadline_in(timeout_ms);
            let (reply_tx, reply_rx) = std_mpsc::channel();
            self.tx
                .send(Job {
                    spec,
                    deadline,
                    reply: reply_tx,
                })
                .map_err(|_| CoreError::Internal("qlean worker gone".into()))?;
            match reply_rx.recv() {
                Ok(res) => res,
                Err(_) => Err(CoreError::Internal(
                    "qlean worker dropped the reply (worker thread died; see node logs)".into(),
                )),
            }
        }
    }
    fn worker(rx: std_mpsc::Receiver<Job>) {
        let rt = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(rt) => rt,
            Err(e) => {
                tracing::error!("qlean worker runtime: {e}");
                return;
            }
        };

        let image_config = qlean::ImageConfig::default();
        let machine_config = qlean::MachineConfig::default();
        let mut image: Option<qlean::Image> = None;
        let mut machine: Option<qlean::Machine> = None;

        while let Ok(job) = rx.recv() {
            let task_id = job.spec.snapshot_hash.clone();
            tracing::info!(
                "qlean worker: job start (snapshot {task_id}, budget {}ms, vm_cached={})",
                job.spec.timeout_ms,
                machine.is_some()
            );
            let result = rt.block_on(run_job(
                &image_config,
                &machine_config,
                &mut image,
                &mut machine,
                job.spec,
                job.deadline,
            ));
            match &result {
                Ok(r) => tracing::info!(
                    "qlean worker: job done exit={} in {}ms",
                    r.exit_code,
                    r.execution_time_ms
                ),
                Err(e) => {
                    tracing::error!("qlean worker: job failed: {}", describe_err(e));
                    // A failed/timed-out VM is not trustworthy: drop it so the
                    // next task boots a fresh one instead of reusing a broken guest.
                    machine = None;
                }
            }
            let _ = job.reply.send(result);
        }

        // Best-effort shutdown when the channel closes.
        if let Some(m) = machine.as_mut() {
            let _ = rt.block_on(async { m.shutdown().await });
        }
    }

    async fn run_job(
        image_config: &qlean::ImageConfig,
        machine_config: &qlean::MachineConfig,
        image: &mut Option<qlean::Image>,
        machine: &mut Option<qlean::Machine>,
        spec: ProjectSpec,
        deadline: Instant,
    ) -> Result<ExecutionResult> {
        if image.is_none() {
            let budget = phase_budget(deadline, PhaseBudget::Image);
            tracing::info!(
                "qlean: preparing image (budget {}s; may download a cloud image on first use)",
                budget.as_secs()
            );
            let img = match timeout(budget, qlean::Image::new(image_config.clone())).await {
                Ok(Ok(img)) => img,
                Ok(Err(e)) => {
                    return Err(CoreError::SandboxExecution(format!("image: {e}")));
                }
                Err(_) => {
                    return Err(CoreError::SandboxExecution(format!(
                        "image preparation timed out after {}s (image download/cache unavailable?)",
                        budget.as_secs()
                    )));
                }
            };
            tracing::info!("qlean: image ready");
            *image = Some(img);
        }

        let need_boot = match machine.as_ref() {
            Some(m) => !m.is_running().await.unwrap_or(false),
            None => true,
        };
        if need_boot {
            let img = image.as_ref().expect("image set above");
            let budget = phase_budget(deadline, PhaseBudget::Boot);
            let boot_start = Instant::now();
            tracing::info!(
                "qlean: cold boot start (budget {}s; cloud-init can take tens of seconds)",
                budget.as_secs()
            );
            let mut m = match timeout(budget, qlean::Machine::new(img, machine_config)).await {
                Ok(Ok(m)) => m,
                Ok(Err(e)) => {
                    return Err(CoreError::SandboxExecution(format!("machine: {e}")));
                }
                Err(_) => {
                    return Err(CoreError::SandboxExecution(format!(
                        "machine creation timed out after {}s",
                        budget.as_secs()
                    )));
                }
            };
            match timeout(budget, m.init()).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    return Err(CoreError::SandboxExecution(format!("boot: {e}")));
                }
                Err(_) => {
                    return Err(CoreError::SandboxExecution(format!(
                        "VM boot timed out after {}s (cloud-init stuck? check /dev/kvm and \
                         qemu-bridge-helper permissions)",
                        budget.as_secs()
                    )));
                }
            }
            tracing::info!("qlean: cold boot done in {}ms", boot_start.elapsed().as_millis());
            *machine = Some(m);
        } else {
            tracing::info!("qlean: reusing cached VM");
        }

        let vm = machine.as_mut().expect("machine booted above");
        execute_on_vm(vm, spec, deadline).await
    }
    async fn execute_on_vm(
        vm: &mut qlean::Machine,
        spec: ProjectSpec,
        deadline: Instant,
    ) -> Result<ExecutionResult> {
        let start = Instant::now();
        let work = spec.work_dir.clone();

        // Clean any previous project and upload the new snapshot.
        let _ = vm.exec(format!("rm -rf {work}")).await;
        if let Some(dir) = &spec.local_project_dir {
            if Path::new(dir).is_dir() {
                let parent = Path::new(&work)
                    .parent()
                    .map(|p| p.to_path_buf())
                    .unwrap_or_else(|| Path::new("/root").to_path_buf());
                let budget = phase_budget(deadline, PhaseBudget::Upload);
                // NB: `.display()` is deliberate. `{parent}` (inline capture)
                // only compiles on rustc versions where `PathBuf: Display` exists;
                // older toolchains reject it, and this is the crate that is only
                // compiled on the target platform, so the failure would surface
                // there and nowhere else.
                tracing::info!(
                    "qlean upload start (budget {}s) {} -> {}",
                    budget.as_secs(),
                    dir,
                    parent.display()
                );
                match timeout(budget, vm.upload(dir, parent)).await {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        return Err(CoreError::SandboxExecution(format!("upload: {e}")));
                    }
                    Err(_) => {
                        return Err(CoreError::SandboxExecution(format!(
                            "upload timed out after {}s (guest unreachable or snapshot too large)",
                            budget.as_secs()
                        )));
                    }
                }
                tracing::info!("qlean upload done in {}ms", start.elapsed().as_millis());
            } else {
                tracing::warn!("qlean: local project dir {} missing on executor", dir);
            }
        }

        let mut out = ExecutionResult {
            exit_code: 0,
            stdout: Vec::new(),
            stderr: Vec::new(),
            execution_time_ms: 0,
            peak_memory_bytes: 0,
            result_hash: None,
        };

        if !spec.build_cmd.is_empty() {
            let cmd = format!("cd {work} && {}", spec.build_cmd.join(" "));
            let budget = phase_budget(deadline, PhaseBudget::Build);
            tracing::info!(
                "qlean build start (budget {}s): {}",
                budget.as_secs(),
                spec.build_cmd.join(" ")
            );
            let b = match timeout(budget, vm.exec(&cmd)).await {
                Ok(Ok(b)) => b,
                Ok(Err(e)) => {
                    return Err(CoreError::SandboxExecution(format!("build: {e}")));
                }
                Err(_) => {
                    return Err(CoreError::SandboxExecution(format!(
                        "build timed out after {}s (command did not return; the VM may be \
                         rebooting or apt-get may be stuck)",
                        budget.as_secs()
                    )));
                }
            };
            tracing::info!("qlean build done in {}ms", start.elapsed().as_millis());
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

        let run_cmd = format!("cd {work} && {}", spec.run_cmd.join(" "));
        let budget = phase_budget(deadline, PhaseBudget::Run);
        tracing::info!("qlean run start (budget {}s): {}", budget.as_secs(), spec.run_cmd.join(" "));
        let r = match timeout(budget, vm.exec(&run_cmd)).await {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => return Err(CoreError::SandboxExecution(format!("run: {e}"))),
            Err(_) => {
                return Err(CoreError::SandboxExecution(format!(
                    "run timed out after {}s (guest command did not return; no hard kill is \
                     available for a single in-guest command)",
                    budget.as_secs()
                )));
            }
        };
        out.exit_code = r.status.code().unwrap_or(0);
        out.stdout.extend_from_slice(&r.stdout);
        out.stderr.extend_from_slice(&r.stderr);
        out.execution_time_ms = start.elapsed().as_millis() as u64;
        tracing::info!(
            "qlean run done in {}ms exit={}",
            out.execution_time_ms,
            out.exit_code
        );
        Ok(out)
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
