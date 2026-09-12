//! bytes carried over request-response protocol.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use eo_core::error::Result;
use raft::eraftpb::Message as RaftMessage;
use tokio::sync::mpsc;
use tracing::{debug, warn};

use p2p::SwarmCommand;

#[derive(Debug, Clone, Default)]
pub struct RaftIdRegistry {
    inner: Arc<RwLock<HashMap<u64, libp2p::PeerId>>>,
}

impl RaftIdRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&self, raft_id: u64, peer_id: libp2p::PeerId) {
        self.inner
            .write()
            .expect("registry poisoned")
            .insert(raft_id, peer_id);
    }

    pub fn get(&self, raft_id: u64) -> Option<libp2p::PeerId> {
        self.inner
            .read()
            .expect("registry poisoned")
            .get(&raft_id)
            .copied()
    }

    pub fn remove(&self, raft_id: u64) {
        self.inner
            .write()
            .expect("registry poisoned")
            .remove(&raft_id);
    }

    pub fn knows_all(&self, peer_ids: &[u64]) -> bool {
        let guard = self.inner.read().expect("registry poisoned");
        peer_ids.iter().all(|id| guard.contains_key(id))
    }
}

#[derive(Debug, Clone)]
pub struct RaftEnvelope {
    pub data: Vec<u8>,
}

impl RaftEnvelope {
    pub fn new(_from: u64, _to: u64, msg: &RaftMessage) -> Result<Self> {
        Ok(Self {
            data: prost::Message::encode_to_vec(msg),
        })
    }

    pub fn decode(&self) -> std::result::Result<RaftMessage, prost::DecodeError> {
        prost::Message::decode(self.data.as_slice())
    }
}

pub struct Libp2pRaftTransport {
    swarm_commands: mpsc::Sender<SwarmCommand>,
    incoming: mpsc::Receiver<RaftEnvelope>,
    registry: RaftIdRegistry,
}

pub fn create_raft_transport(
    swarm_commands: mpsc::Sender<SwarmCommand>,
) -> (Libp2pRaftTransport, mpsc::Sender<RaftEnvelope>) {
    let (incoming_tx, incoming_rx) = mpsc::channel(256);
    let transport = Libp2pRaftTransport {
        swarm_commands,
        incoming: incoming_rx,
        registry: RaftIdRegistry::new(),
    };
    (transport, incoming_tx)
}

impl Libp2pRaftTransport {
    pub fn registry(&self) -> &RaftIdRegistry {
        &self.registry
    }

    pub fn send(&self, to: u64, msg: &RaftMessage) -> Result<()> {
        let peer_id = self.registry.get(to).ok_or_else(|| {
            eo_core::error::CoreError::Network(format!(
                "no libp2p PeerId for raft id {to} (descriptor not received yet)"
            ))
        })?;

        let envelope = RaftEnvelope::new(0, to, msg)?;
        self.swarm_commands
            .try_send(SwarmCommand::SendRaftMessage {
                peer_id,
                data: envelope.data,
            })
            .map_err(|e| {
                eo_core::error::CoreError::Network(format!(
                    "failed to queue raft message for {to}: {e}"
                ))
            })?;
        debug!("Queued raft message to raft id {}", to);
        Ok(())
    }

    pub async fn recv(&mut self) -> Option<RaftEnvelope> {
        self.incoming.recv().await
    }
}

pub fn envelope_from_data(data: Vec<u8>) -> RaftEnvelope {
    RaftEnvelope { data }
}
