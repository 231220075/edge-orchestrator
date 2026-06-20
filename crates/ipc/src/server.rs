//! Unix Domain Socket JSON-RPC server.
//!
//! Binds to a local socket, accepts connections in a loop, and spawns
//! a tokio task per connection to handle JSON-RPC requests.

use std::path::PathBuf;
use std::sync::Arc;

use eo_core::error::{CoreError, Result};
use tokio::net::UnixListener;
use tracing::{error, info};

use crate::handler::{JsonRpcHandler, JsonRpcRequest};

/// A Unix Domain Socket server that accepts JSON-RPC 2.0 requests.
///
/// Each connection reads one newline-delimited JSON request, dispatches
/// it to the :class:`JsonRpcHandler`, writes back the JSON response,
/// and closes the connection (one-shot, stateless).
pub struct IpcServer {
    socket_path: PathBuf,
    handler: Arc<JsonRpcHandler>,
}

/// Handle returned by :meth:`IpcServer::start` for graceful shutdown.
pub struct IpcServerHandle {
    /// Signal this to shut down the server.
    pub shutdown_tx: tokio::sync::oneshot::Sender<()>,
}

impl IpcServer {
    /// Create a new IPC server.
    ///
    /// * `socket_path` — filesystem path for the UDS (e.g. ``~/.edge-orchestrator/ipc.sock``).
    /// * `handler`   — the JSON-RPC method dispatcher.
    pub fn new(socket_path: PathBuf, handler: JsonRpcHandler) -> Self {
        Self {
            socket_path,
            handler: Arc::new(handler),
        }
    }

    /// Start the server on a background tokio task.
    ///
    /// Returns an :class:`IpcServerHandle` that can be used to signal
    /// graceful shutdown (drop the sender or send ``()``).
    pub fn start(self) -> IpcServerHandle {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

        tokio::spawn(async move {
            if let Err(e) = self.run(shutdown_rx).await {
                error!("IPC server error: {e}");
            }
        });

        IpcServerHandle { shutdown_tx }
    }

    /// Internal run loop. Binds, accepts, and dispatches until shutdown
    /// is signalled or the listener dies.
    async fn run(self, shutdown_rx: tokio::sync::oneshot::Receiver<()>) -> Result<()> {
        // Ensure parent directory exists
        if let Some(parent) = self.socket_path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|e| {
                CoreError::Configuration(format!(
                    "cannot create socket directory {}: {e}",
                    parent.display()
                ))
            })?;
        }

        // Remove stale socket file
        if self.socket_path.exists() {
            tokio::fs::remove_file(&self.socket_path)
                .await
                .map_err(|e| {
                    CoreError::Configuration(format!(
                        "cannot remove stale socket {}: {e}",
                        self.socket_path.display()
                    ))
                })?;
        }

        let listener = UnixListener::bind(&self.socket_path).map_err(|e| {
            CoreError::Network(format!(
                "cannot bind to {}: {e}",
                self.socket_path.display()
            ))
        })?;

        info!("IPC server listening on {}", self.socket_path.display());

        let mut shutdown_rx = shutdown_rx;

        loop {
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((stream, peer_addr)) => {
                            let handler = Arc::clone(&self.handler);
                            tokio::spawn(async move {
                                if let Err(e) = handle_connection(stream, &handler).await {
                                    error!("IPC connection error (peer={:?}): {e}", peer_addr);
                                }
                            });
                        }
                        Err(e) => {
                            error!("IPC accept error: {e}");
                        }
                    }
                }
                _ = &mut shutdown_rx => {
                    info!("IPC server shutting down");
                    break;
                }
            }
        }

        // Clean up socket file on shutdown
        let _ = tokio::fs::remove_file(&self.socket_path).await;

        Ok(())
    }
}

/// Handle a single client connection: read one JSON-RPC request,
/// dispatch, write response, close.
async fn handle_connection(stream: tokio::net::UnixStream, handler: &JsonRpcHandler) -> Result<()> {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    let (reader, mut writer) = stream.into_split();
    let mut buf_reader = BufReader::new(reader);
    let mut line = String::new();

    // Read one line (newline-delimited JSON)
    let n = buf_reader
        .read_line(&mut line)
        .await
        .map_err(|e| CoreError::Network(format!("IPC read error: {e}")))?;

    if n == 0 {
        return Ok(()); // client disconnected before sending
    }

    // Parse the JSON-RPC request
    let request: JsonRpcRequest = serde_json::from_str(line.trim())
        .map_err(|e| CoreError::Serialization(format!("invalid JSON-RPC request: {e}")))?;

    // Dispatch
    let response = handler.handle(request).await;

    // Serialize and write response
    let mut response_json = serde_json::to_string(&response)
        .map_err(|e| CoreError::Serialization(format!("failed to serialize response: {e}")))?;
    response_json.push('\n');

    writer
        .write_all(response_json.as_bytes())
        .await
        .map_err(|e| CoreError::Network(format!("IPC write error: {e}")))?;

    writer
        .shutdown()
        .await
        .map_err(|e| CoreError::Network(format!("IPC shutdown error: {e}")))?;

    Ok(())
}
