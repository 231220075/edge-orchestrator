//! Configuration file loading and validation.
//!
//! Reads a YAML configuration file and produces a validated [`NodeConfig`].

use std::path::Path;

use anyhow::{Context, Result};
use chrono::Utc;
use eo_core::types::{Capabilities, NodeDescriptor, NodeType, OsType, Role, RuntimeKind};
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
}

impl Default for CapabilitiesConfig {
    fn default() -> Self {
        Self {
            storage: true,
            gpu_acceleration: false,
            runtimes: default_runtimes(),
            max_memory_mb: default_max_memory_mb(),
            cpu_cores: default_cpu_cores(),
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
    vec!["Wasm".into()]
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

        let runtimes: Vec<RuntimeKind> = self
            .capabilities
            .runtimes
            .iter()
            .filter_map(|r| match r.as_str() {
                "Wasm" => Some(RuntimeKind::Wasm),
                "NativePosix" => Some(RuntimeKind::NativePosix),
                "Container" => Some(RuntimeKind::Container),
                _ => None,
            })
            .collect();

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
        };

        NodeDescriptor {
            node_id,
            node_type,
            os,
            capabilities,
            advertised_addresses: vec![],
            current_assigned_roles: roles,
            started_at: Utc::now(),
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
