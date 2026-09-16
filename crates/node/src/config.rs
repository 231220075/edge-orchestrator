//! Configuration file loading and validation.
//!
//! Reads a YAML configuration file and produces a validated [`NodeConfig`].

use std::path::Path;

use anyhow::{Context, Result};
use chrono::Utc;
use eo_core::types::{Capabilities, NodeDescriptor, NodeType, OsType, Role};
use libp2p::Multiaddr;
use serde::Deserialize;
use uuid::Uuid;

/// Top-level configuration structure deserialized from YAML.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeConfig {
    /// Unique node identifier. Auto-generated if empty or missing.
    #[serde(default)]
    pub node_id: String,

    /// Node type: "Heavy" or "Light".
    #[serde(default = "default_node_type")]
    pub node_type: String,

    /// Addresses to listen on for P2P connections.
    #[serde(default = "default_listen_addresses")]
    pub listen_addresses: Vec<String>,

    /// Bootstrap peers (multiaddrs).
    #[serde(default)]
    #[allow(dead_code)]
    pub bootstrap_peers: Vec<String>,

    /// Node capabilities.
    #[serde(default)]
    pub capabilities: CapabilitiesConfig,

    /// Roles to request on startup.
    #[serde(default)]
    pub roles: Vec<String>,

    /// Static raft participant id (1..=N). None = not a raft voter (e.g. a
    /// light client trigger node).
    #[serde(default)]
    pub raft_id: Option<u64>,

    /// Static cluster membership: the raft ids that vote from the start.
    /// Every raft voter must use the SAME list (e.g. [1, 2, 3]).
    #[serde(default)]
    pub raft_peers: Vec<u64>,

    /// Optional 32-byte hex seed for a deterministic libp2p identity. When
    /// set, the node's PeerId is stable across restarts, which enables fixed
    /// bootstrap addresses in integration tests and production setups.
    #[serde(default)]
    pub identity_seed: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilitiesConfig {
    #[serde(default)]
    pub storage: bool,

    #[serde(default)]
    pub gpu_acceleration: bool,

    #[serde(default = "default_runtimes")]
    pub runtimes: Vec<String>,

    #[serde(default = "default_max_memory_mb")]
    pub max_memory_mb: u64,

    #[serde(default = "default_cpu_cores")]
    pub cpu_cores: u32,

    /// Whether this node can run project sandboxes (Linux + KVM + qlean).
    #[serde(default)]
    pub project_sandbox: bool,

    /// VM lifecycle for project tasks: "reuse" keeps one warm VM (fast, tasks
    /// share its disk) or "fresh" boots a new machine per task (isolated, ~14s
    /// of boot each time). See docs/v3-modules/24-per-task隔离.md.
    #[serde(default = "default_project_vm_mode")]
    pub project_vm_mode: String,

    /// How many idle VMs to pre-boot for `fresh` mode (0 disables pooling).
    /// Pooling only removes the *boot* wait: a fresh machine has no toolchain, so
    /// a build that needs one still installs it per task.
    #[serde(default = "default_project_vm_pool")]
    pub project_vm_pool_size: usize,

    /// Optional custom base image ("template") for the sandbox guest, e.g. one
    /// with the toolchain pre-installed. Requires the digest: qlean verifies the
    /// download against it.
    #[serde(default)]
    pub project_image_source: Option<String>,

    /// Digest of `project_image_source`, e.g. "sha256:<hex>".
    #[serde(default)]
    pub project_image_digest: Option<String>,
}

fn default_project_vm_mode() -> String {
    "reuse".into()
}

fn default_project_vm_pool() -> usize {
    1
}

impl Default for CapabilitiesConfig {
    fn default() -> Self {
        Self {
            storage: true,
            gpu_acceleration: false,
            runtimes: default_runtimes(),
            max_memory_mb: default_max_memory_mb(),
            cpu_cores: default_cpu_cores(),
            project_sandbox: false,
            project_vm_mode: default_project_vm_mode(),
            project_vm_pool_size: default_project_vm_pool(),
            project_image_source: None,
            project_image_digest: None,
        }
    }
}

// Default-value helpers
fn default_node_type() -> String {
    "Heavy".into()
}

fn default_listen_addresses() -> Vec<String> {
    vec!["/ip4/0.0.0.0/tcp/0".into()]
}

fn default_runtimes() -> Vec<String> {
    vec!["qlean".into()]
}

fn default_max_memory_mb() -> u64 {
    16384
}

fn default_cpu_cores() -> u32 {
    4
}

impl NodeConfig {
    /// Load and validate a configuration file from the given path.
    pub fn load(path: &Path) -> Result<Self> {
        let contents = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path.display()))?;

        let config: NodeConfig = serde_yaml::from_str(&contents)
            .with_context(|| format!("Failed to parse config file: {}", path.display()))?;

        config.validate()?;
        Ok(config)
    }

    /// Validate the configuration values.
    fn validate(&self) -> Result<()> {
        if !self.node_id.is_empty() {
            Uuid::parse_str(&self.node_id)
                .with_context(|| format!("Invalid node_id UUID: {}", self.node_id))?;
        }

        if !matches!(self.node_type.as_str(), "Heavy" | "Light") {
            anyhow::bail!(
                "node_type must be 'Heavy' or 'Light', got '{}'",
                self.node_type
            );
        }

        for addr_str in &self.listen_addresses {
            addr_str
                .parse::<Multiaddr>()
                .with_context(|| format!("Invalid listen address: {}", addr_str))?;
        }

        Ok(())
    }

    /// Convert this config into a [`NodeDescriptor`] for P2P advertisement.
    /// Parsed custom sandbox image, if configured.
    ///
    /// Validated here so a missing digest is reported at startup (with the exact
    /// reason) instead of surfacing as an opaque download failure later.
    pub fn project_image_template(&self) -> Result<Option<sandbox::ImageTemplate>, String> {
        sandbox::ImageTemplate::from_config(
            self.capabilities.project_image_source.as_deref(),
            self.capabilities.project_image_digest.as_deref(),
        )
        .map_err(|e| format!("{e}"))
    }

    /// Configured pool size for `fresh` mode, clamped to the sandbox's bound.
    pub fn project_vm_pool_size(&self) -> usize {
        self.capabilities
            .project_vm_pool_size
            .min(sandbox::MAX_POOL_TARGET)
    }

    /// Apply command-line overrides for the project sandbox policy.
    ///
    /// Available on every platform on purpose: the value is validated (and
    /// reported) wherever the config is loaded, and only *acts* on Linux+KVM.
    pub fn apply_sandbox_overrides(&mut self, vm_mode: Option<&str>, pool_size: Option<usize>) {
        if let Some(mode) = vm_mode {
            self.capabilities.project_vm_mode = mode.to_string();
        }
        if let Some(size) = pool_size {
            self.capabilities.project_vm_pool_size = size;
        }
    }

    /// Parsed `capabilities.project_sandbox` + `project_vm_mode`.
    ///
    /// Available on every platform so a bad value is reported wherever the config
    /// is loaded (the value only *takes effect* on a Linux+KVM host).
    pub fn project_sandbox_policy(&self) -> (bool, sandbox::VmMode) {
        (self.capabilities.project_sandbox, self.project_vm_mode())
    }

    /// VM lifecycle policy for project tasks, parsed (and validated) at load.
    pub fn project_vm_mode(&self) -> sandbox::VmMode {
        match sandbox::VmMode::parse(&self.capabilities.project_vm_mode) {
            Ok(mode) => mode,
            Err(e) => {
                tracing::warn!(
                    "invalid capabilities.project_vm_mode '{}': {e}; falling back to 'reuse'",
                    self.capabilities.project_vm_mode
                );
                sandbox::VmMode::Reuse
            }
        }
    }

    pub fn to_descriptor(&self) -> NodeDescriptor {
        let node_id = if self.node_id.is_empty() {
            Uuid::new_v4()
        } else {
            Uuid::parse_str(&self.node_id).unwrap_or_else(|_| Uuid::new_v4())
        };

        let node_type = match self.node_type.as_str() {
            "Light" => NodeType::Light,
            _ => NodeType::Heavy,
        };

        let os = detect_os();

        // Runtime names are kept verbatim: they are free-form capability labels
        // used for node-compatibility comparison. The only execution backend is
        // the qlean project sandbox, declared by its own capability flag.
        let runtimes: Vec<String> = self.capabilities.runtimes.clone();

        let roles: Vec<Role> = self
            .roles
            .iter()
            .filter_map(|r| match r.as_str() {
                "Storage" => Some(Role::Storage),
                "Execution" => Some(Role::Execution),
                "Inference" => Some(Role::Inference),
                "Coordinator" => Some(Role::Coordinator),
                "Bootstrap" => Some(Role::Bootstrap),
                _ => None,
            })
            .collect();

        let capabilities = Capabilities {
            storage: self.capabilities.storage,
            gpu_acceleration: self.capabilities.gpu_acceleration,
            runtimes,
            max_memory_mb: self.capabilities.max_memory_mb,
            cpu_cores: self.capabilities.cpu_cores,
            project_sandbox: self.capabilities.project_sandbox,
        };

        NodeDescriptor {
            node_id,
            node_type,
            os,
            capabilities,
            advertised_addresses: vec![],
            current_assigned_roles: roles,
            started_at: Utc::now(),
            raft_id: self.raft_id,
        }
    }
}

/// Detect the current operating system.
fn detect_os() -> OsType {
    if cfg!(target_os = "macos") {
        OsType::MacOS
    } else if cfg!(target_os = "linux") {
        OsType::Linux
    } else if cfg!(target_os = "windows") {
        OsType::Windows
    } else if cfg!(target_os = "ios") {
        OsType::Ios
    } else if cfg!(target_os = "android") {
        OsType::Android
    } else {
        OsType::Unknown
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sandbox::VmMode;

    /// The shipped default pool size (kept next to the test that asserts it).
    impl NodeConfig {
        fn default_pool() -> usize {
            default_project_vm_pool()
        }

        fn default_template() -> Option<sandbox::ImageTemplate> {
            let config: NodeConfig = serde_yaml::from_str("capabilities: {}").unwrap();
            config.project_image_template().unwrap()
        }
    }

    /// Parse a minimal YAML config; the `vm_mode` helper reads the parsed value.
    fn config_with_mode(value: &str) -> NodeConfig {
        let yaml = format!("capabilities:\n  project_sandbox: true\n  project_vm_mode: {value}\n");
        serde_yaml::from_str(&yaml).expect("test config must parse")
    }

    #[test]
    fn vm_mode_defaults_to_reuse() {
        let config: NodeConfig = serde_yaml::from_str("capabilities: {}").unwrap();
        assert_eq!(config.project_vm_mode(), VmMode::Reuse);
    }

    #[test]
    fn vm_mode_is_parsed_and_typos_fall_back_loudly() {
        assert_eq!(config_with_mode("fresh").project_vm_mode(), VmMode::Fresh);
        // A typo must not silently pick a mode; it falls back to the safe default.
        assert_eq!(config_with_mode("fressh").project_vm_mode(), VmMode::Reuse);
    }

    #[test]
    fn image_template_needs_source_and_digest() {
        let yaml = "capabilities:\n  project_image_source: /var/lib/eo/toolchain.qcow2\n";
        let config: NodeConfig = serde_yaml::from_str(yaml).unwrap();
        let err = config
            .project_image_template()
            .expect_err("digest is mandatory");
        assert!(err.contains("both source and digest"), "{err}");
        assert!(NodeConfig::default_template().is_none());
    }

    #[test]
    fn pool_size_defaults_and_is_clamped() {
        assert_eq!(NodeConfig::default_pool(), 1);
        let yaml = format!(
            "capabilities:\n  project_vm_pool_size: {}\n",
            sandbox::MAX_POOL_TARGET + 50
        );
        let config: NodeConfig = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(
            config.project_vm_pool_size(),
            sandbox::MAX_POOL_TARGET,
            "a too-large pool must be clamped, not honoured"
        );
    }
}
