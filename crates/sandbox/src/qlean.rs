// Qlean-based Linux sandbox.
//
// A persistent worker thread owns at most one KVM VM and, depending on
// [`VmMode`], either reuses it across project runs (fast, but tasks share the
// writable overlay) or boots a fresh machine per task (isolated, but pays the
// ~14s boot every time). qlean already gives every machine its own qcow2 overlay
// on top of the base image, so "fresh" mode really is a clean root disk.
//
// Only available on Linux hosts with KVM; other platforms get a stub.

use eo_core::error::{CoreError, Result};
use eo_core::traits::ProjectSandbox;
use eo_core::types::{ExecutionResult, ProjectSpec};

/// How the sandbox treats the VM between tasks.
///
/// Measured trade-off on a Linux+KVM host (see docs/v3-modules/24-per-task隔离.md):
/// `Reuse` finishes a warm task in ~60ms but every task shares the same writable
/// overlay — a task killed mid-`apt` left locks behind that broke the next task.
/// `Fresh` pays ~14s of boot per task and cannot leak state between tasks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum VmMode {
    /// Keep one booted VM and reuse it (default: fastest, least isolated).
    #[default]
    Reuse,
    /// Boot a new machine (its own overlay disk) for every task, then drop it.
    Fresh,
}

impl VmMode {
    /// Parse the config value; unknown values are reported rather than guessed.
    pub fn parse(raw: &str) -> Result<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "" | "reuse" | "warm" => Ok(VmMode::Reuse),
            "fresh" | "per-task" | "isolated" => Ok(VmMode::Fresh),
            other => Err(CoreError::Configuration(format!(
                "unknown project sandbox vm_mode '{other}' (expected 'reuse' or 'fresh')"
            ))),
        }
    }

    /// Whether a machine may survive a task.
    pub fn allows_reuse(self) -> bool {
        matches!(self, VmMode::Reuse)
    }
}

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

    /// Exit code for a guest command. A signal-terminated process has no exit
    /// code; reporting that as 0 would hide every broken sandbox command.
    fn exit_code_of(status: &std::process::ExitStatus) -> i32 {
        status.code().unwrap_or_else(|| {
            #[cfg(unix)]
            {
                use std::os::unix::process::ExitStatusExt;
                if let Some(sig) = status.signal() {
                    return 128 + sig;
                }
            }
            1
        })
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

    /// Everything the worker thread needs to know before it starts.
    #[derive(Debug, Clone)]
    struct WorkerConfig {
        mode: VmMode,
        pool_size: usize,
        template: Option<ImageTemplate>,
    }

    impl QleanSandbox {
        /// The default VmMode (`Reuse`).
        pub fn new() -> Result<Self> {
            Self::with_mode(VmMode::Reuse)
        }

        /// Create a sandbox with an explicit VM lifecycle policy and pool size.
        pub fn with_policy(mode: VmMode, pool_size: usize) -> Result<Self> {
            Self::with_worker_config(WorkerConfig {
                mode,
                pool_size,
                template: None,
            })
        }

        /// Create a sandbox with a custom base image ("template"), e.g. one with
        /// the toolchain pre-installed so `fresh` mode does not reinstall it.
        pub fn with_template(
            mode: VmMode,
            pool_size: usize,
            template: Option<ImageTemplate>,
        ) -> Result<Self> {
            Self::with_worker_config(WorkerConfig {
                mode,
                pool_size,
                template,
            })
        }

        fn with_worker_config(config: WorkerConfig) -> Result<Self> {
            let (tx, rx) = std_mpsc::channel::<Job>();
            std::thread::Builder::new()
                .name("qlean-worker".into())
                .spawn(move || worker(rx, config))
                .map_err(|e| CoreError::Internal(format!("spawn qlean worker: {e}")))?;
            Ok(Self { tx })
        }

        /// Create a sandbox with an explicit VM lifecycle policy (no pooling).
        pub fn with_mode(mode: VmMode) -> Result<Self> {
            Self::with_policy(mode, 0)
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
    fn worker(rx: std_mpsc::Receiver<Job>, config: WorkerConfig) {
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

        let mode = config.mode;
        let image_config = match &config.template {
            Some(t) => {
                tracing::info!(
                    "qlean worker: using custom base image {} (digest {}) — a fresh machine \
                     from this template should already carry the toolchain",
                    t.source,
                    t.digest
                );
                t.to_qlean_config()
            }
            None => qlean::ImageConfig::default(),
        };
        let machine_config = qlean::MachineConfig::default();
        let mut image: Option<qlean::Image> = None;
        let mut machine: Option<qlean::Machine> = None;

        let plan = crate::pool::PoolPlan::new(mode.allows_reuse(), config.pool_size);
        tracing::info!(
            "qlean worker: mode={mode:?}, pool_target={} ({}), reuse={}",
            plan.target,
            if plan.target == 0 {
                "pooling disabled"
            } else {
                "pre-booting to hide the next task's boot"
            },
            plan.reuse
        );

        while let Ok(job) = rx.recv() {
            let task_id = job.spec.snapshot_hash.clone();
            tracing::info!(
                "qlean worker: job start (snapshot {task_id}, budget {}ms, vm_cached={}, mode={mode:?})",
                job.spec.timeout_ms,
                machine.is_some()
            );

            let deadline = job.deadline;
            let spec = job.spec;

            // Drive the stages explicitly with block_on: `worker` is a sync function,
            // and separate calls also keep `&mut machine` from being held across an
            // await point (which the borrow checker rejects — this is the Linux-gated
            // path, so a mistake here only shows up on the target platform).
            //
            // "prepare" and "execute" stay separate so a preparation failure keeps its
            // own error instead of being disguised as a job result.
            let prepared: Result<()> = (|| {
                rt.block_on(ensure_image(&image_config, &mut image, deadline))?;

                let need_boot = match machine.as_ref() {
                    Some(m) => !rt.block_on(async { m.is_running().await }).unwrap_or(false),
                    None => true,
                };
                if need_boot {
                    let img = image.as_ref().expect("image ensured above");
                    let budget = phase_budget(deadline, PhaseBudget::Boot);
                    let booted =
                        rt.block_on(boot_machine(img, &machine_config, budget, "task machine"))?;
                    machine = Some(booted);
                } else {
                    tracing::info!("qlean: reusing cached VM");
                }
                Ok(())
            })();

            let outcome = match prepared {
                Ok(()) => {
                    let vm = machine.as_mut().expect("machine booted above");
                    rt.block_on(run_on_machine(vm, spec, deadline))
                }
                Err(e) => Err(e),
            };

            match &outcome {
                Ok(r) => tracing::info!(
                    "qlean worker: job done exit={} in {}ms (mode={mode:?})",
                    r.exit_code,
                    r.execution_time_ms
                ),
                Err(e) => {
                    tracing::error!("qlean worker: job failed: {}", describe_err(e));
                    // A failed/timed-out VM is not trustworthy: drop it so the next
                    // task boots a fresh one instead of reusing a broken guest.
                    machine = None;
                }
            }

            // Follow the pool plan: keep (reuse) or discard + refill (fresh).
            for action in plan.after_job() {
                match action {
                    crate::pool::PoolAction::Keep => {}
                    crate::pool::PoolAction::Discard => {
                        if let Some(mut m) = machine.take() {
                            match rt.block_on(async { m.shutdown().await }) {
                                Ok(()) => tracing::info!("qlean: machine discarded (fresh mode)"),
                                Err(e) => {
                                    tracing::warn!(
                                        "qlean: machine shutdown failed (fresh mode): {e}"
                                    )
                                }
                            }
                        }
                    }
                    // Refill only once the image exists: pre-booting before the first
                    // task would double the cold cost for nothing.
                    crate::pool::PoolAction::Refill { .. } => {
                        let Some(img) = image.as_ref() else {
                            tracing::debug!("qlean: pool refill skipped (image not prepared yet)");
                            continue;
                        };
                        match rt.block_on(boot_machine(
                            img,
                            &machine_config,
                            BOOT_TIMEOUT,
                            "pool refill",
                        )) {
                            Ok(ready) => {
                                // The worker is serial, so one idle machine is the
                                // useful maximum: more would only hold RAM.
                                machine = Some(ready);
                                tracing::info!(
                                    "qlean: pool refilled (1 idle VM ready for the next task)"
                                );
                            }
                            Err(e) => tracing::warn!(
                                "qlean: pool refill failed, the next task will boot on demand: {}",
                                describe_err(&e)
                            ),
                        }
                    }
                }
            }

            let _ = job.reply.send(outcome);
        }

        // Best-effort shutdown when the channel closes.
        if let Some(m) = machine.as_mut() {
            let _ = rt.block_on(async { m.shutdown().await });
        }
    }

    /// Make sure `image` exists locally (downloading on first use).
    async fn ensure_image(
        image_config: &qlean::ImageConfig,
        image: &mut Option<qlean::Image>,
        deadline: Instant,
    ) -> Result<()> {
        if image.is_some() {
            return Ok(());
        }
        let budget = phase_budget(deadline, PhaseBudget::Image);
        tracing::info!(
            "qlean: preparing image (budget {}s; may download a cloud image on first use)",
            budget.as_secs()
        );
        let img = match timeout(budget, qlean::Image::new(image_config.clone())).await {
            Ok(Ok(img)) => img,
            Ok(Err(e)) => return Err(CoreError::SandboxExecution(format!("image: {e}"))),
            Err(_) => {
                return Err(CoreError::SandboxExecution(format!(
                    "image preparation timed out after {}s (image download/cache unavailable?)",
                    budget.as_secs()
                )));
            }
        };
        tracing::info!("qlean: image ready");
        *image = Some(img);
        Ok(())
    }

    /// Boot one machine from the (already prepared) image.
    ///
    /// Used both for a task's own machine and for pool refills, so a pooled
    /// machine and an on-demand one are identical by construction.
    async fn boot_machine(
        image: &qlean::Image,
        machine_config: &qlean::MachineConfig,
        budget: Duration,
        why: &str,
    ) -> Result<qlean::Machine> {
        let boot_start = Instant::now();
        tracing::info!(
            "qlean: cold boot start (budget {}s, {why}; cloud-init can take tens of seconds)",
            budget.as_secs()
        );
        let mut m = match timeout(budget, qlean::Machine::new(image, machine_config)).await {
            Ok(Ok(m)) => m,
            Ok(Err(e)) => return Err(CoreError::SandboxExecution(format!("machine: {e}"))),
            Err(_) => {
                return Err(CoreError::SandboxExecution(format!(
                    "machine creation timed out after {}s",
                    budget.as_secs()
                )));
            }
        };
        match timeout(budget, m.init()).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(CoreError::SandboxExecution(format!("boot: {e}"))),
            Err(_) => {
                return Err(CoreError::SandboxExecution(format!(
                    "VM boot timed out after {}s (cloud-init stuck? check /dev/kvm and \
                     qemu-bridge-helper permissions)",
                    budget.as_secs()
                )));
            }
        }
        tracing::info!(
            "qlean: cold boot done in {}ms ({why})",
            boot_start.elapsed().as_millis()
        );
        Ok(m)
    }

    /// Execute one job on a booted machine.
    async fn run_on_machine(
        machine: &mut qlean::Machine,
        spec: ProjectSpec,
        deadline: Instant,
    ) -> Result<ExecutionResult> {
        execute_on_vm(machine, spec, deadline).await
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
                out.exit_code = exit_code_of(&b.status);
                out.execution_time_ms = start.elapsed().as_millis() as u64;
                return Ok(out);
            }
        }

        let run_cmd = format!("cd {work} && {}", spec.run_cmd.join(" "));
        let budget = phase_budget(deadline, PhaseBudget::Run);
        tracing::info!(
            "qlean run start (budget {}s): {}",
            budget.as_secs(),
            spec.run_cmd.join(" ")
        );
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
        // NOT `unwrap_or(0)`: a signal-killed command has no exit code, and
        // reporting that as success hid a failing `./app` behind exit 0.
        out.exit_code = exit_code_of(&r.status);
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

/// A custom base image ("template") for the sandbox guest.
///
/// Motivated by the one thing VM reuse cannot give us: a *fresh* machine per task
/// that still has the toolchain. qlean can fetch a custom image, but only when
/// `source` and `digest` are both configured — it verifies the download against
/// the digest — so a template without a digest is rejected here rather than
/// failing later with an opaque error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageTemplate {
    /// URL or local path of the qcow2 image.
    pub source: String,
    /// `sha256:<hex>` (or `sha512:<hex>`) of that image.
    pub digest: String,
}

impl ImageTemplate {
    /// Parse `source`/`digest` config values; `None` when no template is set.
    pub fn from_config(source: Option<&str>, digest: Option<&str>) -> Result<Option<Self>> {
        let source = source.map(str::trim).filter(|s| !s.is_empty());
        let digest = digest.map(str::trim).filter(|s| !s.is_empty());
        match (source, digest) {
            (None, None) => Ok(None),
            (Some(_), None) => Err(CoreError::Configuration(
                "sandbox image template needs both source and digest: qlean verifies the \
                 downloaded image against the digest, and guessing it would silently disable \
                 that check"
                    .into(),
            )),
            (None, Some(_)) => Err(CoreError::Configuration(
                "sandbox image template digest given without a source".into(),
            )),
            (Some(source), Some(digest)) => {
                let hex = digest.split_once(':').map(|(_, hex)| hex).unwrap_or(digest);
                if !digest.contains(':')
                    || hex.len() < 32
                    || !hex.chars().all(|c| c.is_ascii_hexdigit())
                {
                    return Err(CoreError::Configuration(format!(
                        "sandbox image digest '{digest}' must look like 'sha256:<hex>'"
                    )));
                }
                Ok(Some(Self {
                    source: source.to_string(),
                    digest: digest.to_string(),
                }))
            }
        }
    }

    /// qlean `ImageConfig` pointing at this template.
    ///
    /// Linux-gated: the `qlean` crate is only a dependency on the target platform.
    #[cfg(target_os = "linux")]
    pub fn to_qlean_config(&self) -> qlean::ImageConfig {
        qlean::ImageConfig::default()
            .with_source(self.source.clone())
            .with_digest(self.digest.clone())
    }
}

#[cfg(test)]
mod template_tests {
    use super::ImageTemplate;

    #[test]
    fn no_config_means_builtin_image() {
        assert_eq!(ImageTemplate::from_config(None, None).unwrap(), None);
        assert_eq!(
            ImageTemplate::from_config(Some("  "), Some("")).unwrap(),
            None
        );
    }

    #[test]
    fn digest_is_mandatory() {
        let err = ImageTemplate::from_config(Some("/var/lib/eo/toolchain.qcow2"), None)
            .expect_err("a template without a digest must be rejected");
        assert!(format!("{err}").contains("both source and digest"), "{err}");
        let err = ImageTemplate::from_config(None, Some("sha256:deadbeef"))
            .expect_err("a digest without a source is meaningless");
        assert!(format!("{err}").contains("without a source"), "{err}");
    }

    #[test]
    fn malformed_digest_is_rejected_before_the_fetch() {
        for bad in ["sha256:xyz", "deadbeef", "sha256:", "sha256:1234"] {
            assert!(
                ImageTemplate::from_config(Some("/img.qcow2"), Some(bad)).is_err(),
                "'{bad}' must not pass as a digest"
            );
        }
    }

    #[test]
    fn a_complete_template_is_accepted() {
        let hex = "a".repeat(64);
        let t = ImageTemplate::from_config(
            Some("/var/lib/eo/toolchain.qcow2"),
            Some(&format!("sha256:{hex}")),
        )
        .unwrap()
        .expect("source + digest is a valid template");
        assert_eq!(t.source, "/var/lib/eo/toolchain.qcow2");
        assert!(t.digest.starts_with("sha256:"));
    }
}

#[cfg(test)]
mod mode_tests {
    use super::VmMode;

    #[test]
    fn parses_both_modes_and_rejects_typos() {
        assert_eq!(VmMode::parse("reuse").unwrap(), VmMode::Reuse);
        assert_eq!(VmMode::parse("").unwrap(), VmMode::Reuse);
        assert_eq!(VmMode::parse("FRESH").unwrap(), VmMode::Fresh);
        assert_eq!(VmMode::parse("per-task").unwrap(), VmMode::Fresh);
        assert!(
            VmMode::parse("isoltion").is_err(),
            "typos must not be guessed"
        );
    }

    #[test]
    fn only_reuse_mode_keeps_the_machine() {
        assert!(VmMode::Reuse.allows_reuse());
        assert!(!VmMode::Fresh.allows_reuse());
    }
}
