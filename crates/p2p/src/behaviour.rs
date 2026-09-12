//! Custom NetworkBehaviour composing identify, mDNS, ping, descriptor exchange,
//! Raft message transport, and blob distribution.

use libp2p::identify;
use libp2p::mdns;
use libp2p::ping;
use libp2p::request_response;
use libp2p::swarm::NetworkBehaviour;

use crate::protocol::{BlobCodec, DescriptorCodec, RaftMessageCodec};

#[derive(NetworkBehaviour)]
pub struct EdgeOrchBehaviour {
    pub identify: identify::Behaviour,
    pub mdns: mdns::tokio::Behaviour,
    pub ping: ping::Behaviour,
    pub descriptor_exchange: request_response::Behaviour<DescriptorCodec>,
    pub raft_exchange: request_response::Behaviour<RaftMessageCodec>,
    pub blob_exchange: request_response::Behaviour<BlobCodec>,
}

impl EdgeOrchBehaviour {
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

        let raft_exchange = request_response::Behaviour::new(
            std::iter::once((
                RaftMessageCodec::protocol(),
                request_response::ProtocolSupport::Full,
            )),
            request_response::Config::default(),
        );

        let blob_exchange = request_response::Behaviour::new(
            std::iter::once((
                BlobCodec::protocol(),
                request_response::ProtocolSupport::Full,
            )),
            request_response::Config::default(),
        );

        Self {
            identify: identify::Behaviour::new(identify_config),
            mdns,
            ping: ping::Behaviour::new(ping::Config::default()),
            descriptor_exchange,
            raft_exchange,
            blob_exchange,
        }
    }
}
