//! Raft network transport over libp2p.
//!
//! Routes Raft messages between nodes using the P2P mesh. Messages
//! are serialized with protobuf (via the prost-codec) and sent over
//! a custom libp2p request-response protocol.

use eo_core::error::Result;
use raft::eraftpb::Message as RaftMessage;
use tokio::sync::mpsc;
use tracing::debug;

/// A Raft message wrapped for transport over the P2P network.
#[derive(Debug, Clone)]
pub struct RaftEnvelope {
    /// The sender's Raft node ID.
    pub from: u64,
    /// The intended recipient's Raft node ID.
    pub to: u64,
    /// The serialized Raft message (protobuf bytes).
    pub data: Vec<u8>,
}

impl RaftEnvelope {
    /// Create a new envelope for a Raft message.
    pub fn new(from: u64, to: u64, msg: &RaftMessage) -> Result<Self> {
        let data = prost::Message::encode_to_vec(msg);
        Ok(Self { from, to, data })
    }

    /// Decode the enclosed Raft message.
    pub fn decode(&self) -> std::result::Result<RaftMessage, prost::DecodeError> {
        prost::Message::decode(self.data.as_slice())
    }
}

/// Network transport for sending and receiving Raft messages over libp2p.
///
/// Uses an mpsc channel to communicate with the P2P swarm task.
/// The swarm task handles the actual network I/O; this transport
/// provides a simple send/recv interface for the Raft node.
pub struct Libp2pRaftTransport {
    /// Sender for sending Raft messages to the P2P layer.
    swarm_sender: mpsc::Sender<SwarmRaftCommand>,

    /// Receiver for incoming Raft messages from the P2P layer.
    raft_receiver: mpsc::Receiver<RaftEnvelope>,
}

/// Commands sent from Raft to the P2P swarm.
#[derive(Debug)]
pub enum SwarmRaftCommand {
    /// Send a Raft message to a peer.
    SendRaftMessage {
        /// Target peer (identified by Raft node ID).
        to: u64,
        /// The serialized Raft protobuf message.
        data: Vec<u8>,
    },
}

/// Create a paired transport — returns the Raft-side transport and
/// the swarm-side sender/receiver for integrating with the P2P task.
pub fn create_raft_transport() -> (
    Libp2pRaftTransport,
    mpsc::Receiver<SwarmRaftCommand>,
    mpsc::Sender<RaftEnvelope>,
) {
    let (cmd_tx, cmd_rx) = mpsc::channel(256);
    let (msg_tx, msg_rx) = mpsc::channel(256);

    let transport = Libp2pRaftTransport {
        swarm_sender: cmd_tx,
        raft_receiver: msg_rx,
    };

    (transport, cmd_rx, msg_tx)
}

impl Libp2pRaftTransport {
    /// Send a Raft message to a peer.
    ///
    /// # Arguments
    /// * `to` — The recipient's Raft node ID.
    /// * `msg` — The Raft message to send.
    pub fn send(&self, to: u64, msg: &RaftMessage) -> Result<()> {
        let data = prost::Message::encode_to_vec(msg);

        self.swarm_sender
            .try_send(SwarmRaftCommand::SendRaftMessage { to, data })
            .map_err(|e| {
                eo_core::error::CoreError::Network(format!(
                    "failed to send Raft message to {to}: {e}"
                ))
            })?;

        debug!("Sent Raft message to {}", to);
        Ok(())
    }

    /// Receive the next Raft message from the network.
    ///
    /// Returns `None` if the transport has been shut down.
    pub async fn recv(&mut self) -> Option<RaftEnvelope> {
        self.raft_receiver.recv().await
    }
}

/// Map Raft node IDs to libp2p [`PeerId`]s.
///
/// In the current implementation, Raft node IDs are u64 values
/// that need to be resolved to libp2p PeerIds for actual message
/// delivery. This mapping is maintained by the P2P layer based
/// on peer discovery and descriptor exchange.
#[derive(Debug, Clone, Default)]
pub struct PeerRegistry {
    // Maps Raft node ID → libp2p PeerId
    // TODO: Implement full mapping registry in Phase 2.6
}

impl PeerRegistry {
    /// Create a new empty peer registry.
    pub fn new() -> Self {
        Self::default()
    }
}
