//! Command-line interface for the edge-orchestrator node binary.
//!
//! Uses `clap` derive macros to define command-line arguments.

use std::path::PathBuf;

use clap::Parser;

/// Edge-Cloud Orchestrator — a distributed orchestration platform.
#[derive(Parser, Debug)]
#[command(name = "edge-orchestrator")]
#[command(version, about, long_about = None)]
pub struct Args {
    /// Path to the YAML configuration file.
    #[arg(short, long, default_value = "config.yaml")]
    pub config: PathBuf,

    /// Log level (trace, debug, info, warn, error).
    #[arg(short, long, default_value = "info")]
    pub log_level: String,

    /// Override the node ID (generates a new one if not set).
    #[arg(long)]
    pub node_id: Option<String>,

    /// Override the listen address.
    #[arg(long)]
    pub listen_address: Option<String>,

    /// Path to the Unix Domain Socket for the IPC server.
    #[arg(long, default_value = "~/.edge-orchestrator/ipc.sock")]
    pub ipc_socket: PathBuf,

    /// Disable the IPC server (useful for development).
    #[arg(long)]
    pub no_ipc: bool,

    /// Directory for the CAS object store.
    #[arg(long, default_value = "~/.edge-orchestrator/objects")]
    pub store_dir: PathBuf,
}
