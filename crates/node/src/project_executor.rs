// ProjectExecutor adapter: runs a ProjectTask via QleanSandbox on Linux.

use std::sync::Arc;

use eo_core::types::{NodeId, ProjectResult, ProjectTask};

#[cfg(target_os = "linux")]
mod imp {
    use super::*;
    use crate::cas_fetch::DEFAULT_FETCH_TIMEOUT;
    use crate::project_snapshot::extract_snapshot;
    use eo_core::traits::ProjectSandbox as _;
    use eo_core::types::ProjectSpec;

    pub struct QleanProjectExecutor {
        sandbox: Arc<sandbox::QleanSandbox>,
        self_node_id: NodeId,
        /// Pull-side CAS: the snapshot usually lives on the submitting node.
        cas: Arc<crate::cas_fetch::BlobFetcher>,
        /// Local CAS, where a fetched snapshot lands and is read back from.
        store: Arc<storage::LocalObjectStore>,
    }

    impl QleanProjectExecutor {
        pub fn new(
            sandbox: Arc<sandbox::QleanSandbox>,
            self_node_id: NodeId,
            cas: Arc<crate::cas_fetch::BlobFetcher>,
            store: Arc<storage::LocalObjectStore>,
        ) -> Self {
            Self {
                sandbox,
                self_node_id,
                cas,
                store,
            }
        }
    }

    #[async_trait::async_trait]
    impl p2p::ProjectExecutor for QleanProjectExecutor {
        async fn run(&self, task: ProjectTask) -> anyhow::Result<ProjectResult> {
            // Ensure the snapshot bytes are available before doing anything else:
            // the master sends only a hash, so a missing blob is what "the build
            // never started" used to look like.
            match self
                .cas
                .ensure_blob(&task.snapshot.hash, DEFAULT_FETCH_TIMEOUT)
                .await
            {
                Ok(bytes) => tracing::info!(
                    "project {}: snapshot {} ready locally ({} bytes)",
                    task.task_id,
                    task.snapshot.hash,
                    bytes.len()
                ),
                Err(e) => {
                    return Err(anyhow::anyhow!(
                        "snapshot {} unavailable: {e:#}",
                        task.snapshot.hash
                    ));
                }
            }

            let tmp = tempfile::tempdir()?;
            // Name the unpacked dir after work_dir's basename so that qlean's
            // upload-mirror semantics land it exactly at work_dir.
            let base = std::path::Path::new(&task.work_dir)
                .file_name()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or_else(|| "project".to_string());
            let project_dir = tmp.path().join(base);
            extract_snapshot(&task.snapshot, &self.store, &project_dir)
                .map_err(|e| anyhow::anyhow!("extract snapshot: {e}"))?;

            let spec = ProjectSpec {
                snapshot_hash: task.snapshot.hash,
                local_project_dir: Some(project_dir.to_string_lossy().to_string()),
                work_dir: task.work_dir.clone(),
                build_cmd: task.build_cmd.clone(),
                run_cmd: task.run_cmd.clone(),
                timeout_ms: task.timeout_ms,
                resource_limits: task.resource_limits.clone(),
            };

            let sandbox = std::sync::Arc::clone(&self.sandbox);
            let er = tokio::task::spawn_blocking(move || sandbox.run_project(spec))
                .await
                .map_err(|e| anyhow::anyhow!("join: {e}"))?
                .map_err(|e| anyhow::anyhow!("qlean run_project: {e:#}"))?;

            Ok(ProjectResult {
                task_id: task.task_id,
                exit_code: er.exit_code,
                stdout: er.stdout,
                stderr: er.stderr,
                execution_time_ms: er.execution_time_ms,
                executed_on: self.self_node_id,
            })
        }
    }
}

/// Non-Linux stub. A node on another platform never advertises
/// `project_sandbox` and `make_project_executor` returns `None`, so this is
/// unreachable in practice; it exists so the crate compiles everywhere.
#[cfg(not(target_os = "linux"))]
#[allow(dead_code)]
pub struct QleanProjectExecutor;

#[cfg(not(target_os = "linux"))]
#[allow(dead_code)]
impl QleanProjectExecutor {
    pub fn new(_sandbox: Arc<sandbox::QleanSandbox>, _node: NodeId) -> Self {
        Self
    }
}

#[cfg(not(target_os = "linux"))]
#[async_trait::async_trait]
impl p2p::ProjectExecutor for QleanProjectExecutor {
    async fn run(&self, _task: ProjectTask) -> anyhow::Result<ProjectResult> {
        anyhow::bail!("Qlean project executor requires Linux")
    }
}

#[cfg(target_os = "linux")]
#[allow(unused_imports)]
pub use imp::QleanProjectExecutor;
