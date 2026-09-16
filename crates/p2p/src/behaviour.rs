//! Custom NetworkBehaviour composing identify, mDNS, ping, descriptor exchange,
//! Raft message transport, and blob distribution.

use libp2p::identify;
use libp2p::mdns;
use libp2p::ping;
use libp2p::request_response;
use libp2p::swarm::NetworkBehaviour;
use std::time::Duration;

use crate::protocol::{BlobCodec, DescriptorCodec, ProjectCodec, RaftMessageCodec};

#[derive(NetworkBehaviour)]
pub struct EdgeOrchBehaviour {
    pub identify: identify::Behaviour,
    pub mdns: mdns::tokio::Behaviour,
    pub ping: ping::Behaviour,
    pub descriptor_exchange: request_response::Behaviour<DescriptorCodec>,
    pub raft_exchange: request_response::Behaviour<RaftMessageCodec>,
    pub blob_exchange: request_response::Behaviour<BlobCodec>,
    pub project_exchange: request_response::Behaviour<ProjectCodec>,
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

        // Blobs are no longer just small code/module payloads: a project snapshot
        // travels this way and can be tens of MB, so the default 10s request
        // timeout is far too tight.
        let blob_exchange = request_response::Behaviour::new(
            std::iter::once((
                BlobCodec::protocol(),
                request_response::ProtocolSupport::Full,
            )),
            request_response::Config::default().with_request_timeout(Duration::from_secs(900)),
        );

        // Project execution can take minutes (VM boot + build), far beyond the
        // 10s default request timeout, so allow a long window.
        let project_exchange = request_response::Behaviour::new(
            std::iter::once((
                ProjectCodec::protocol(),
                request_response::ProtocolSupport::Full,
            )),
            request_response::Config::default().with_request_timeout(Duration::from_secs(1800)),
        );

        Self {
            identify: identify::Behaviour::new(identify_config),
            mdns,
            ping: ping::Behaviour::new(ping::Config::default()),
            descriptor_exchange,
            raft_exchange,
            blob_exchange,
            project_exchange,
        }
    }
}
