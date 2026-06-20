//! TCP transport with Noise XX handshake and Yamux stream multiplexing.
//!
//! Builds a libp2p [`Transport`] that encrypts all traffic and multiplexes
//! multiple streams over a single TCP connection.

use libp2p::core::muxing::StreamMuxerBox;
use libp2p::core::transport::Boxed;
use libp2p::noise as libp2p_noise;
use libp2p::tcp::tokio::Transport as TcpTransport;
use libp2p::yamux;
use libp2p::{identity, PeerId, Transport};

/// Build a TCP + Noise + Yamux transport boxed for use with a [`Swarm`].
///
/// # Arguments
/// * `keypair` — The node's libp2p identity keypair for Noise authentication.
///
/// # Returns
/// A boxed transport that can be passed to [`SwarmBuilder`].
///
/// [`Swarm`]: libp2p::Swarm
/// [`SwarmBuilder`]: libp2p::SwarmBuilder
pub fn build_transport(keypair: &identity::Keypair) -> Boxed<(PeerId, StreamMuxerBox)> {
    // Noise XX handshake: reciprocal authentication using keypair
    let noise_config = libp2p_noise::Config::new(keypair)
        .expect("failed to build noise config from valid keypair");

    // TCP transport using tokio
    let tcp = TcpTransport::default()
        .upgrade(libp2p::core::upgrade::Version::V1)
        .authenticate(noise_config)
        .multiplex(yamux::Config::default())
        .boxed();

    tcp
}
