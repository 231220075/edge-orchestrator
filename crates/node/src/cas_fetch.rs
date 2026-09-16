//! Pull-side CAS access: fetch a blob from a peer when it is missing locally.
//!
//! The blob protocol is answer-only: peers reply to `BlobRequest` with the bytes
//! (see `p2p::protocol`), and there is no push/upload direction. So a node that
//! needs someone else's blob must ask for it, which means the requester needs a
//! way to *wait* for the answer while the swarm event loop keeps running — that
//! is what [`BlobFetcher`] provides.
//!
//! Flow for a project snapshot:
//! 1. master packs the workspace and stores it in **its own** CAS;
//! 2. the `ProjectTask` carries only the hash;
//! 3. the executor calls [`BlobFetcher::ensure_blob`]; on a local miss it asks
//!    every peer that has answered a descriptor request and waits for the data;
//! 4. the swarm loop stores the arriving blob AND wakes the waiter here.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use eo_core::error::{CoreError, Result};
use eo_core::types::Hash;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, warn};

use crate::raft::network::RaftIdRegistry;

/// Default bound for a blob fetch. The project executor passes this explicitly
/// (a snapshot gates a whole task, so the bound is generous but never infinite:
/// waiting forever is the failure mode this codebase spent a long time chasing).
pub const DEFAULT_FETCH_TIMEOUT: Duration = Duration::from_secs(300);

type Waiter = oneshot::Sender<Vec<u8>>;

/// Shared handle to the local CAS plus the ability to pull missing blobs.
pub struct BlobFetcher {
    store: Arc<storage::LocalObjectStore>,
    swarm_commands: mpsc::Sender<p2p::SwarmCommand>,
    raft_registry: RaftIdRegistry,
    /// hash -> waiters that are blocked on that blob arriving.
    waiters: Mutex<HashMap<Hash, Vec<Waiter>>>,
}

impl BlobFetcher {
    pub fn new(
        store: Arc<storage::LocalObjectStore>,
        swarm_commands: mpsc::Sender<p2p::SwarmCommand>,
        raft_registry: RaftIdRegistry,
    ) -> Self {
        debug!(
            "cas: fetcher ready ({} peer(s) known for blob requests)",
            raft_registry.snapshot().len()
        );
        Self {
            store,
            swarm_commands,
            raft_registry,
            waiters: Mutex::new(HashMap::new()),
        }
    }

    /// Blob bytes from the local CAS, if present.
    pub fn get_local(&self, hash: &Hash) -> Option<Vec<u8>> {
        self.store.get_blob(hash).ok()
    }

    /// Store a blob locally (used before handing a task to the executor).
    pub fn put(&self, data: &[u8]) -> Result<Hash> {
        self.store.put_blob(data)
    }

    /// Called by the swarm event loop when a `BlobResponse` arrives.
    ///
    /// Stores the blob and wakes every waiter for that hash. Returns true when
    /// the data was usable, so the caller can log meaningfully.
    pub fn on_blob_received(&self, hash: &Hash, found: bool, data: Vec<u8>) -> bool {
        if !found || data.is_empty() {
            return false;
        }
        if let Err(e) = self.store.put_blob(&data) {
            warn!("cas: could not store fetched blob {hash}: {e}");
            return false;
        }
        let waiters = self
            .waiters
            .lock()
            .map(|mut w| w.remove(hash).unwrap_or_default())
            .unwrap_or_default();
        if waiters.is_empty() {
            debug!(
                "cas: blob {hash} fetched ({} bytes) but nobody was waiting",
                data.len()
            );
        }
        for waiter in waiters {
            let _ = waiter.send(data.clone());
        }
        true
    }

    /// Make sure `hash` is in the local CAS, fetching it from peers if needed.
    ///
    /// Returns the blob path requirements as bytes only on a fresh fetch; callers
    /// that just need presence can ignore the value (it is re-read from the store
    /// by the extractor).
    pub async fn ensure_blob(&self, hash: &Hash, timeout: Duration) -> Result<Vec<u8>> {
        if let Ok(bytes) = self.store.get_blob(hash) {
            debug!(
                "cas: blob {hash} served from local CAS ({} bytes)",
                bytes.len()
            );
            return Ok(bytes);
        }

        let (tx, rx) = oneshot::channel::<Vec<u8>>();
        {
            let mut waiters = self
                .waiters
                .lock()
                .map_err(|_| CoreError::Internal("cas waiters poisoned".into()))?;
            waiters.entry(hash.clone()).or_default().push(tx);
        }

        let peers = self.raft_registry.snapshot();
        if peers.is_empty() {
            self.drop_waiter(hash);
            return Err(CoreError::Network(format!(
                "blob {hash} is not in the local CAS and no peer is known to ask \
                 (no descriptor exchange completed yet)"
            )));
        }
        for (raft_id, peer_id) in &peers {
            debug!("cas: requesting blob {hash} from raft {raft_id} ({peer_id})");
            let _ = self
                .swarm_commands
                .send(p2p::SwarmCommand::RequestBlob {
                    peer_id: *peer_id,
                    hash: hash.clone(),
                })
                .await;
        }

        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(bytes)) => {
                debug!(
                    "cas: blob {hash} fetched from a peer ({} bytes)",
                    bytes.len()
                );
                Ok(bytes)
            }
            Ok(Err(_)) => {
                self.drop_waiter(hash);
                Err(CoreError::Network(format!(
                    "blob {hash} request was dropped before any peer answered"
                )))
            }
            Err(_) => {
                self.drop_waiter(hash);
                Err(CoreError::Network(format!(
                    "timed out after {}s waiting for blob {hash} from {} peer(s); \
                     is the node that submitted the project still up?",
                    timeout.as_secs(),
                    peers.len()
                )))
            }
        }
    }

    fn drop_waiter(&self, hash: &Hash) {
        if let Ok(mut waiters) = self.waiters.lock() {
            waiters.remove(hash);
        }
    }

    /// Number of hashes with pending waiters (observability).
    pub fn pending_fetches(&self) -> usize {
        self.waiters.lock().map(|w| w.len()).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn fetcher() -> (BlobFetcher, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(storage::LocalObjectStore::new(PathBuf::from(dir.path())).unwrap());
        let (tx, _rx) = mpsc::channel(4);
        (BlobFetcher::new(store, tx, RaftIdRegistry::new()), dir)
    }

    #[tokio::test]
    async fn local_hit_needs_no_network() {
        let (f, _d) = fetcher();
        let hash = f.put(b"snapshot-bytes").unwrap();
        let got = f
            .ensure_blob(&hash, Duration::from_millis(50))
            .await
            .unwrap();
        assert_eq!(got, b"snapshot-bytes");
    }

    #[tokio::test]
    async fn missing_blob_without_peers_fails_fast_and_clearly() {
        let (f, _d) = fetcher();
        let err = f
            .ensure_blob(&"deadbeef".to_string(), Duration::from_millis(50))
            .await
            .expect_err("must not invent data");
        let msg = format!("{err}");
        assert!(msg.contains("no peer is known"), "{msg}");
        assert_eq!(
            f.pending_fetches(),
            0,
            "a failed fetch must not leak a waiter"
        );
    }

    #[tokio::test]
    async fn arriving_blob_wakes_the_waiter_and_lands_in_cas() {
        let (f, _d) = fetcher();
        // A peer must be known, otherwise ensure_blob fails before waiting.
        f.raft_registry.insert(
            1,
            libp2p::identity::Keypair::generate_ed25519()
                .public()
                .to_peer_id(),
        );
        let store = f.store.clone();
        // The store is content-addressed: the key must be the hash of the bytes
        // the peer will send, otherwise the fetch could never satisfy a reader.
        let hash = storage::hash_blob(b"from-peer");

        // Simulate the swarm event loop delivering the response shortly after the
        // request goes out.
        let f2 = Arc::new(f);
        let f3 = Arc::clone(&f2);
        let h2 = hash.clone();
        let responder = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            f3.on_blob_received(&h2, true, b"from-peer".to_vec())
        });

        let got = f2.ensure_blob(&hash, Duration::from_secs(2)).await.unwrap();
        assert!(responder.await.unwrap(), "delivery must report success");
        assert_eq!(got, b"from-peer");
        assert_eq!(
            store.get_blob(&hash).unwrap(),
            b"from-peer",
            "a fetched blob must be cached in CAS"
        );
        assert_eq!(f2.pending_fetches(), 0);
    }

    #[tokio::test]
    async fn fetch_timeout_reports_the_wait_not_a_silent_pending() {
        let (f, _d) = fetcher();
        f.raft_registry.insert(
            1,
            libp2p::identity::Keypair::generate_ed25519()
                .public()
                .to_peer_id(),
        );
        let err = f
            .ensure_blob(&"missing".to_string(), Duration::from_millis(50))
            .await
            .expect_err("no data can arrive here");
        assert!(format!("{err}").contains("timed out after"), "{err}");
        assert_eq!(f.pending_fetches(), 0);
    }
}
