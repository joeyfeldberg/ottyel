//! Receiver lifecycle: bind both listeners, serve, and shut down within one deadline.

use std::{net::SocketAddr, sync::OnceLock, time::Instant};

use anyhow::{Context, Result, anyhow};
use tokio::{net::TcpListener, sync::watch, task::JoinHandle};

use super::{
    IngestLimits, IngestState, serve_grpc_listener, serve_http_listener, stats::IngestProbe,
    wait_for_shutdown,
};
use crate::store::{Store, WriterDrain};

/// The OTLP HTTP and gRPC receiver for one store.
pub struct Receiver {
    state: IngestState,
}

impl Receiver {
    pub fn new(store: Store, limits: IngestLimits) -> Self {
        Self {
            state: IngestState::new(store, limits),
        }
    }

    /// Returns a handle that samples receiver statistics while the receiver runs.
    pub fn probe(&self) -> IngestProbe {
        self.state.probe()
    }

    /// Binds both listeners, so address and permission errors surface before serving starts.
    pub async fn bind(self, http_bind: &str, grpc_bind: &str) -> Result<BoundReceiver> {
        let http_addr: SocketAddr = http_bind
            .parse()
            .with_context(|| format!("invalid HTTP bind addr {http_bind}"))?;
        let grpc_addr: SocketAddr = grpc_bind
            .parse()
            .with_context(|| format!("invalid gRPC bind addr {grpc_bind}"))?;
        let http = TcpListener::bind(http_addr)
            .await
            .with_context(|| format!("failed to bind OTLP/HTTP listener on {http_addr}"))?;
        let grpc = TcpListener::bind(grpc_addr)
            .await
            .with_context(|| format!("failed to bind OTLP/gRPC listener on {grpc_addr}"))?;
        Ok(BoundReceiver {
            state: self.state,
            http,
            grpc,
        })
    }
}

/// A receiver whose listeners are bound and ready to accept connections.
pub struct BoundReceiver {
    state: IngestState,
    http: TcpListener,
    grpc: TcpListener,
}

/// What shutdown finished and what it abandoned at the deadline.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShutdownReport {
    /// Whether in-flight requests were still running when the deadline passed. Shutdown stops
    /// waiting for them; they end when the process exits.
    pub requests_abandoned: bool,
    /// The writer drain outcome, or `None` when the store has no writer.
    pub writer: Option<WriterDrain>,
}

impl ShutdownReport {
    pub fn is_clean(&self) -> bool {
        !self.requests_abandoned && self.writer.is_none_or(|writer| writer.completed)
    }
}

impl BoundReceiver {
    pub fn http_addr(&self) -> Result<SocketAddr> {
        Ok(self.http.local_addr()?)
    }

    pub fn grpc_addr(&self) -> Result<SocketAddr> {
        Ok(self.grpc.local_addr()?)
    }

    /// Serves until `shutdown` becomes true, then stops intake, gives in-flight requests and
    /// admitted SQLite writes the configured shutdown timeout in total, and reports whatever
    /// had to be abandoned.
    pub async fn serve(self, shutdown: watch::Receiver<bool>) -> Result<ShutdownReport> {
        let Self { state, http, grpc } = self;
        let timeout = state.limits.shutdown_timeout;
        let mut http = tokio::spawn(serve_http_listener(http, state.clone(), shutdown.clone()));
        let mut grpc = tokio::spawn(serve_grpc_listener(grpc, state.clone(), shutdown.clone()));
        let http_abort = http.abort_handle();
        let grpc_abort = grpc.abort_handle();

        let shutdown_started = OnceLock::new();
        let deadline = async {
            wait_for_shutdown(shutdown).await;
            let _ = shutdown_started.set(Instant::now());
            tokio::time::sleep(timeout).await;
        };
        let requests_abandoned = tokio::select! {
            served = async { tokio::try_join!(listener(&mut http), listener(&mut grpc)) } => {
                if let Err(error) = served {
                    http_abort.abort();
                    grpc_abort.abort();
                    return Err(error);
                }
                false
            }
            () = deadline => {
                // This stops accepting connections. Connection tasks already running are no longer
                // awaited, and accepted writes still get the remaining drain budget below.
                http_abort.abort();
                grpc_abort.abort();
                true
            }
        };

        let remaining = shutdown_started
            .get()
            .map_or(timeout, |started| timeout.saturating_sub(started.elapsed()));
        let store = state.store.clone();
        let writer = tokio::task::spawn_blocking(move || store.close_writer(remaining))
            .await
            .context("writer drain task failed")?;
        Ok(ShutdownReport {
            requests_abandoned,
            writer,
        })
    }
}

async fn listener(handle: &mut JoinHandle<Result<()>>) -> Result<()> {
    handle
        .await
        .map_err(|error| anyhow!("OTLP listener task failed: {error}"))?
}

#[cfg(test)]
#[path = "receiver_tests.rs"]
mod tests;
