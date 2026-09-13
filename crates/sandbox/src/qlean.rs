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
    use std::time::Instant;

    struct Job {
        spec: ProjectSpec,
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
            let (reply_tx, reply_rx) = std_mpsc::channel();
            self.tx
                .send(Job {
                    spec,
                    reply: reply_tx,
                })
                .map_err(|_| CoreError::Internal("qlean worker gone".into()))?;
            reply_rx
                .recv()
                .map_err(|_| CoreError::Internal("qlean worker dropped reply".into()))?
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
            let result = rt.block_on(run_job(
                &image_config,
                &machine_config,
                &mut image,
                &mut machine,
                job.spec,
            ));
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
    ) -> Result<ExecutionResult> {
        if image.is_none() {
            let img = qlean::Image::new(image_config.clone())
                .await
                .map_err(|e| CoreError::SandboxExecution(format!("image: {e}")))?;
            *image = Some(img);
        }

        let need_boot = match machine.as_ref() {
            Some(m) => !m.is_running().await.unwrap_or(false),
            None => true,
        };
        if need_boot {
            let img = image.as_ref().expect("image set above");
            let mut m = qlean::Machine::new(img, machine_config)
                .await
                .map_err(|e| CoreError::SandboxExecution(format!("machine: {e}")))?;
            m.init()
                .await
                .map_err(|e| CoreError::SandboxExecution(format!("boot: {e}")))?;
            *machine = Some(m);
        }

        let vm = machine.as_mut().expect("machine booted above");
        execute_on_vm(vm, spec).await
    }
    async fn execute_on_vm(vm: &mut qlean::Machine, spec: ProjectSpec) -> Result<ExecutionResult> {
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
                vm.upload(dir, parent)
                    .await
                    .map_err(|e| CoreError::SandboxExecution(format!("upload: {e}")))?;
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
            let b = vm
                .exec(&cmd)
                .await
                .map_err(|e| CoreError::SandboxExecution(format!("build: {e}")))?;
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
        let r = vm
            .exec(&run_cmd)
            .await
            .map_err(|e| CoreError::SandboxExecution(format!("run: {e}")))?;
        out.exit_code = r.status.code().unwrap_or(0);
        out.stdout.extend_from_slice(&r.stdout);
        out.stderr.extend_from_slice(&r.stderr);
        out.execution_time_ms = start.elapsed().as_millis() as u64;
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
