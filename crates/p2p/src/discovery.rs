//! Application-level events emitted by the P2P swarm.
//!
//! These events are produced by the swarm event loop and consumed
//! by the node's bootstrap and orchestration layers.

use eo_core::types::{NodeDescriptor, ProjectResult};
use libp2p::PeerId;

/// Application-level events emitted by the P2P swarm.
///
/// Consumers receive these via the `mpsc::Receiver<Event>` returned
/// by [`new_swarm`](crate::swarm::new_swarm).
#[derive(Debug, Clone)]
pub enum Event {
    /// A new peer was discovered via mDNS, with its advertised address.
    PeerDiscovered {
        /// The libp2p [`PeerId`] of the discovered peer.
        peer_id: PeerId,
        /// The address this peer advertised.
        address: libp2p::Multiaddr,
    },

    /// A previously discovered peer has expired (mDNS TTL elapsed).
    PeerExpired {
        /// The libp2p [`PeerId`] of the expired peer.
        peer_id: PeerId,
    },

    /// We received a [`NodeDescriptor`] from a peer.
    DescriptorReceived {
        /// The peer that sent the descriptor.
        peer_id: PeerId,
        /// The deserialized descriptor.
        descriptor: NodeDescriptor,
    },

    /// We sent our descriptor to a peer who requested it.
    DescriptorSent {
        /// The peer that received our descriptor.
        peer_id: PeerId,
    },

    /// A new inbound P2P listen address is active.
    NewListenAddr {
        /// The address we are now listening on.
        address: libp2p::Multiaddr,
    },

    /// A peer's identify information was received.
    Identified {
        /// The peer.
        peer_id: PeerId,
        /// The identify info.
        info: libp2p::identify::Info,
    },

    /// Connection closed with a peer.
    ConnectionClosed {
        /// The peer.
        peer_id: PeerId,
    },

    /// Outbound or inbound connection has been established with a peer.
    /// This is the safe point to send request-response requests.
    PeerConnected {
        /// The peer.
        peer_id: PeerId,
    },

    /// Result of a project execution returned from an executor.
    ProjectResultReceived {
        peer_id: PeerId,
        result: ProjectResult,
    },

    /// A Raft protobuf message arrived over the network.
    RaftMessageReceived {
        /// The libp2p peer that sent it.
        peer_id: PeerId,
        /// Serialized raft message payload (prost bytes).
        data: Vec<u8>,
    },

    /// A peer asked us for a CAS blob (we must answer via the swarm).
    BlobRequestReceived {
        /// The peer requesting the blob.
        peer_id: PeerId,
        /// The requested content hash.
        hash: String,
    },

    /// A peer answered our blob request.
    BlobResponseReceived {
        /// The peer that answered.
        peer_id: PeerId,
        /// The content hash requested.
        hash: String,
        /// Whether the peer had the blob.
        found: bool,
        /// The blob bytes (empty when found is false).
        data: Vec<u8>,
    },
}
