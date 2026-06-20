//! Custom [`NetworkBehaviour`] composing identify, mDNS, ping, and
//! a custom descriptor-exchange protocol.
//!
//! This is the heart of the P2P layer — it determines how the node
//! discovers peers, shares its identity, and exchanges application-level
//! metadata.

use libp2p::identify;
use libp2p::mdns;
use libp2p::ping;
use libp2p::request_response;
use libp2p::swarm::NetworkBehaviour;

use crate::protocol::DescriptorCodec;

/// The composite network behaviour for an edge-orchestrator node.
///
/// Composes four libp2p behaviours:
/// 1. [`identify::Behaviour`] — exchange node identity (agent version, protocols)
/// 2. [`mdns::tokio::Behaviour`] — LAN peer discovery via mDNS
/// 3. [`ping::Behaviour`] — keepalive pings
/// 4. [`request_response::Behaviour<DescriptorCodec>`] — custom descriptor exchange
#[derive(NetworkBehaviour)]
pub struct EdgeOrchBehaviour {
    /// Identify protocol: exchanges `IdentifyInfo` on connection.
    pub identify: identify::Behaviour,

    /// mDNS service discovery: finds peers on the local network.
    pub mdns: mdns::tokio::Behaviour,

    /// Keepalive ping: detects dead peers.
    pub ping: ping::Behaviour,

    /// Custom request-response protocol for exchanging `NodeDescriptor`s.
    pub descriptor_exchange: request_response::Behaviour<DescriptorCodec>,
}

impl EdgeOrchBehaviour {
    /// Create a new composite behaviour.
    ///
    /// # Arguments
    /// * `local_public_key` — This node's public key for the identify protocol.
    /// * `identify_config` — Configuration for the identify protocol.
    pub fn new(
        local_public_key: libp2p::identity::PublicKey,
        identify_config: identify::Config,
    ) -> Self {
        let mdns =
            mdns::tokio::Behaviour::new(mdns::Config::default(), local_public_key.to_peer_id())
                .expect("mDNS behaviour should build");

        let descriptor_exchange = request_response::Behaviour::new(
            std::iter::once((
                DescriptorCodec::protocol(),
                request_response::ProtocolSupport::Full,
            )),
            request_response::Config::default(),
        );

        Self {
            identify: identify::Behaviour::new(identify_config),
            mdns,
            ping: ping::Behaviour::new(ping::Config::default()),
            descriptor_exchange,
        }
    }
}
