//! Swarm manager — spawns the libp2p [`Swarm`] on a tokio task and
//! translates its raw events into application-level [`Event`]s.

use std::time::Duration;

use eo_core::error::Result;
use eo_core::types::NodeDescriptor;
use futures::StreamExt;
use libp2p::swarm::SwarmEvent;
use libp2p::{identify, identity, Multiaddr, PeerId, SwarmBuilder};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::behaviour::{EdgeOrchBehaviour, EdgeOrchBehaviourEvent};
use crate::discovery::Event;
use crate::protocol::{DescriptorRequest, DescriptorResponse};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Convenience alias for the concrete swarm type.
pub type EdgeOrchSwarm = libp2p::Swarm<EdgeOrchBehaviour>;

/// Commands that can be sent to the swarm task.
#[derive(Debug)]
pub enum SwarmCommand {
    /// Dial a peer at the given address.
    Dial {
        /// The address to dial.
        addr: Multiaddr,
    },
    /// Request a descriptor from a specific peer.
    RequestDescriptor {
        /// The peer to request from.
        peer_id: PeerId,
    },
}

/// Handle for interacting with a running swarm.
///
/// Created by [`new_swarm`]. The swarm runs in a background tokio task.
pub struct SwarmHandle {
    /// Send commands to the swarm task.
    pub commands: mpsc::Sender<SwarmCommand>,
    /// Receive application-level events from the swarm.
    pub events: mpsc::Receiver<Event>,
}

// ---------------------------------------------------------------------------
// Swarm creation
// ---------------------------------------------------------------------------

/// Create a new libp2p swarm and spawn its event loop on a tokio task.
///
/// Returns a [`SwarmHandle`] that can be used to send commands and
/// receive application-level events.
pub fn new_swarm(
    keypair: identity::Keypair,
    config: SwarmConfig,
    self_descriptor: NodeDescriptor,
) -> Result<SwarmHandle> {
    let local_peer_id = keypair.public().to_peer_id();
    let local_public_key = keypair.public();

    let identify_config =
        identify::Config::new("/edge-orch/1.0.0".into(), local_public_key.clone());
    let behaviour = EdgeOrchBehaviour::new(local_public_key, identify_config);

    let mut swarm = SwarmBuilder::with_existing_identity(keypair)
        .with_tokio()
        .with_tcp(
            libp2p::tcp::Config::default(),
            libp2p::noise::Config::new,
            libp2p::yamux::Config::default,
        )
        .map_err(|e| eo_core::error::CoreError::Swarm(e.to_string()))?
        .with_behaviour(|_key| Ok(behaviour))
        .map_err(|e| eo_core::error::CoreError::Swarm(e.to_string()))?
        .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(Duration::from_secs(60)))
        .build();

    for addr in &config.listen_addresses {
        swarm
            .listen_on(addr.clone())
            .map_err(|e| eo_core::error::CoreError::Swarm(format!("listen on {addr}: {e}")))?;
        info!("Listening on {}", addr);
    }

    let (event_tx, event_rx) = mpsc::channel(256);
    let (cmd_tx, cmd_rx) = mpsc::channel(64);

    tokio::spawn(async move {
        run_event_loop(swarm, event_tx, cmd_rx, local_peer_id, self_descriptor).await;
    });

    Ok(SwarmHandle {
        commands: cmd_tx,
        events: event_rx,
    })
}

// ---------------------------------------------------------------------------
// Event loop
// ---------------------------------------------------------------------------

async fn run_event_loop(
    mut swarm: EdgeOrchSwarm,
    event_tx: mpsc::Sender<Event>,
    mut cmd_rx: mpsc::Receiver<SwarmCommand>,
    self_peer_id: PeerId,
    self_descriptor: NodeDescriptor,
) {
    loop {
        tokio::select! {
            swarm_event = swarm.next() => {
                let app_event = match swarm_event {
                    Some(SwarmEvent::NewListenAddr { address, .. }) => {
                        info!("New listen address: {}", address);
                        Some(Event::NewListenAddr { address })
                    }
                    Some(SwarmEvent::ConnectionClosed { peer_id, .. }) => {
                        debug!("Connection closed: {}", peer_id);
                        Some(Event::ConnectionClosed { peer_id })
                    }
                    Some(SwarmEvent::Behaviour(event)) => {
                        handle_behaviour_event(event, self_peer_id, &self_descriptor)
                    }
                    Some(_) => None,
                    None => break,
                };

                if let Some(event) = app_event {
                    if event_tx.send(event).await.is_err() {
                        debug!("Event receiver dropped, shutting down swarm event loop");
                        break;
                    }
                }
            }

            cmd = cmd_rx.recv() => {
                match cmd {
                    Some(SwarmCommand::Dial { addr }) => {
                        if let Err(e) = swarm.dial(addr.clone()) {
                            warn!("Failed to dial {}: {}", addr, e);
                        }
                    }
                    Some(SwarmCommand::RequestDescriptor { peer_id }) => {
                        let request = DescriptorRequest {
                            requester_id: self_descriptor.node_id.to_string(),
                        };
                        swarm.behaviour_mut().descriptor_exchange
                            .send_request(&peer_id, request);
                    }
                    None => {
                        debug!("Command sender dropped, shutting down swarm event loop");
                        break;
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Behaviour event handlers
// ---------------------------------------------------------------------------

fn handle_behaviour_event(
    event: EdgeOrchBehaviourEvent,
    self_peer_id: PeerId,
    self_descriptor: &NodeDescriptor,
) -> Option<Event> {
    match event {
        EdgeOrchBehaviourEvent::Mdns(mdns_event) => match mdns_event {
            libp2p::mdns::Event::Discovered(peers) => {
                for (peer_id, _addr) in peers {
                    if peer_id != self_peer_id {
                        return Some(Event::PeerDiscovered { peer_id });
                    }
                }
                None
            }
            libp2p::mdns::Event::Expired(peers) => {
                for (peer_id, _addr) in peers {
                    if peer_id != self_peer_id {
                        return Some(Event::PeerExpired { peer_id });
                    }
                }
                None
            }
        },

        EdgeOrchBehaviourEvent::Identify(identify::Event::Received { peer_id, info, .. }) => {
            info!(
                "Identified peer {}: agent={}, protocols={:?}",
                peer_id, info.agent_version, info.protocols
            );
            Some(Event::Identified { peer_id, info })
        }
        EdgeOrchBehaviourEvent::Identify(_) => None,

        EdgeOrchBehaviourEvent::Ping(ping_event) => {
            match ping_event.result {
                Ok(latency) => debug!("Ping {} OK: {}ms", ping_event.peer, latency.as_millis()),
                Err(e) => warn!("Ping {} failed: {}", ping_event.peer, e),
            }
            None
        }

        EdgeOrchBehaviourEvent::DescriptorExchange(req_resp_event) => {
            handle_descriptor_exchange(req_resp_event, self_descriptor)
        }
    }
}

fn handle_descriptor_exchange(
    event: libp2p::request_response::Event<DescriptorRequest, DescriptorResponse>,
    _self_descriptor: &NodeDescriptor,
) -> Option<Event> {
    use libp2p::request_response::{Event as RREvent, Message};

    match event {
        RREvent::Message { peer, message } => match message {
            Message::Request { request, .. } => {
                debug!(
                    "Descriptor request from {} (requester: {})",
                    peer, request.requester_id
                );
                Some(Event::DescriptorSent { peer_id: peer })
            }
            Message::Response { response, .. } => match response.to_descriptor() {
                Ok(descriptor) => {
                    info!(
                        "Received descriptor from {}: node_type={:?}",
                        peer, descriptor.node_type
                    );
                    Some(Event::DescriptorReceived {
                        peer_id: peer,
                        descriptor,
                    })
                }
                Err(e) => {
                    warn!("Failed to parse descriptor from {}: {}", peer, e);
                    None
                }
            },
        },
        RREvent::OutboundFailure { peer, error, .. } => {
            warn!("Outbound descriptor request failed to {}: {}", peer, error);
            None
        }
        RREvent::InboundFailure { peer, error, .. } => {
            warn!("Inbound descriptor request failed from {}: {}", peer, error);
            None
        }
        RREvent::ResponseSent { peer, .. } => {
            debug!("Sent descriptor response to {}", peer);
            None
        }
    }
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for starting a swarm.
#[derive(Debug, Clone)]
pub struct SwarmConfig {
    /// Addresses to listen on (e.g., `/ip4/0.0.0.0/tcp/0`).
    pub listen_addresses: Vec<Multiaddr>,
    /// Known bootstrap peers (may be empty for mDNS-only discovery).
    pub bootstrap_peers: Vec<Multiaddr>,
}
