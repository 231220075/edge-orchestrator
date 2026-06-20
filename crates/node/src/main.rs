//! Edge-Cloud Orchestrator — Node Binary Entrypoint.
//!
//! Starts the node process: parses CLI args, loads config, initializes
//! the P2P swarm, CAS storage, Raft consensus, IPC server, and runs
//! the event monitor until shutdown.

mod bootstrap;
mod cli;
mod config;
mod signals;

use std::path::PathBuf;

use clap::Parser;
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::cli::Args;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Parse CLI arguments
    let args = Args::parse();

    // Initialize tracing/logging
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&args.log_level)),
        )
        .with_target(false)
        .with_thread_ids(false)
        .init();

    info!("Edge-Cloud Orchestrator Node starting...");

    // Resolve paths (expand ~)
    let store_dir = expand_tilde(&args.store_dir);
    let ipc_socket_path: Option<PathBuf> = if args.no_ipc {
        None
    } else {
        Some(expand_tilde(&args.ipc_socket))
    };

    // Bootstrap the node
    let mut node = bootstrap::Node::bootstrap(
        &args.config,
        args.node_id.as_deref(),
        ipc_socket_path.as_deref(),
        &store_dir,
    )
    .await?;

    info!(
        "Node {} started successfully — waiting for peers...",
        node.descriptor.node_id
    );

    // Run the event monitor until shutdown
    tokio::select! {
        result = node.run_event_monitor() => {
            if let Err(e) = result {
                tracing::error!("Event monitor error: {:#}", e);
            }
        }
        sig = signals::wait_for_shutdown() => {
            info!("Received {} — shutting down gracefully", sig);
        }
    }

    // IPC server handle drops here — the background task will clean up
    if let Some(_handle) = node.ipc_handle.take() {
        info!("IPC server stopped");
    }

    info!("Node shut down complete. Goodbye.");
    Ok(())
}

/// Expand a leading ``~`` in a path to the user's home directory.
fn expand_tilde(path: &std::path::Path) -> std::path::PathBuf {
    if let Some(s) = path.to_str() {
        if let Some(rest) = s.strip_prefix("~/") {
            if let Ok(home) = std::env::var("HOME") {
                return std::path::PathBuf::from(home).join(rest);
            }
        }
        if s == "~" {
            if let Ok(home) = std::env::var("HOME") {
                return std::path::PathBuf::from(home);
            }
        }
    }
    path.to_path_buf()
}
