//! Signal handling for graceful shutdown.
//!
//! Catches SIGTERM and SIGINT (Ctrl+C) and shuts down the node cleanly.

use tokio::signal;

/// Wait for a shutdown signal (SIGTERM or SIGINT).
///
/// Returns the signal that was received.
pub async fn wait_for_shutdown() -> &'static str {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
        "SIGINT"
    };

    let terminate = async {
        #[cfg(unix)]
        {
            let mut sigterm =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("failed to install SIGTERM handler");
            sigterm.recv().await;
            "SIGTERM"
        }
        #[cfg(not(unix))]
        {
            std::future::pending::<()>().await;
            ""
        }
    };

    tokio::select! {
        sig = ctrl_c => sig,
        sig = terminate => sig,
    }
}
