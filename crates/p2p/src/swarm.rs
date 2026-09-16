//! Swarm manager — spawns the libp2p [`Swarm`] on a tokio task and
//! translates its raw events into application-level [`Event`]s.

use std::collections::HashMap;
use std::time::Duration;

use eo_core::error::Result;
use eo_core::types::{NodeDescriptor, ProjectResult, ProjectTask, TaskId};
use futures::StreamExt;
use libp2p::swarm::SwarmEvent;
use libp2p::{identify, identity, Multiaddr, PeerId, SwarmBuilder};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::behaviour::{EdgeOrchBehaviour, EdgeOrchBehaviourEvent};
use crate::discovery::Event;
use crate::protocol::{BlobRequest, BlobResponse, DescriptorRequest, DescriptorResponse};
use crate::protocol::{RaftMessageRequest, RaftMessageResponse};
use crate::{BlobProvider, ProjectExecutor};

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
    /// Send a raw raft protobuf message to a peer (target resolved by caller).
    SendRaftMessage {
        /// The recipient libp2p peer.
        peer_id: PeerId,
        /// Serialized protobuf bytes.
        data: Vec<u8>,
    },
    /// Ask a peer for a CAS blob by hash.
    RequestBlob { peer_id: PeerId, hash: String },
    /// Send a project-task to a peer (master -> executor).
    SendProjectTask { peer_id: PeerId, task: ProjectTask },
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
    blob_provider: Option<std::sync::Arc<dyn BlobProvider>>,
    project_executor: Option<std::sync::Arc<dyn ProjectExecutor>>,
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
    // Finished project jobs come back through this channel: only the event loop
    // may call `Behaviour::send_response`, so the job cannot answer by itself.
    let (project_tx, project_rx) = mpsc::channel::<ProjectOutcome>(8);

    tokio::spawn(async move {
        let bootstrap_peers = config.bootstrap_peers.clone();
        run_event_loop(
            swarm,
            event_tx,
            cmd_rx,
            local_peer_id,
            self_descriptor,
            bootstrap_peers,
            blob_provider,
            project_executor,
            project_tx,
            project_rx,
        )
        .await;
    });

    Ok(SwarmHandle {
        commands: cmd_tx,
        events: event_rx,
    })
}

// ---------------------------------------------------------------------------
// Event loop
// ---------------------------------------------------------------------------

// TODO(Phase 2 cleanup): fold providers/executor into a RuntimeHandles struct
// to reduce argument count instead of this lint allowance.
#[allow(clippy::too_many_arguments)]
async fn run_event_loop(
    mut swarm: EdgeOrchSwarm,
    event_tx: mpsc::Sender<Event>,
    mut cmd_rx: mpsc::Receiver<SwarmCommand>,
    self_peer_id: PeerId,
    self_descriptor: NodeDescriptor,
    bootstrap_peers: Vec<Multiaddr>,
    blob_provider: Option<std::sync::Arc<dyn BlobProvider>>,
    project_executor: Option<std::sync::Arc<dyn ProjectExecutor>>,
    project_tx: mpsc::Sender<ProjectOutcome>,
    mut project_rx: mpsc::Receiver<ProjectOutcome>,
) {
    // Response channels of project jobs still running, keyed by task id. The
    // event loop stays free while a job runs; the job hands its result back here.
    let mut pending: HashMap<TaskId, libp2p::request_response::ResponseChannel<ProjectResult>> =
        HashMap::new();

    // Dial every explicitly-configured bootstrap peer once at startup. This
    // guarantees a full mesh even when mDNS races or a LAN is flaky.
    info!("Bootstrap peers configured: {}", bootstrap_peers.len());
    for addr in &bootstrap_peers {
        if let Err(e) = swarm.dial(addr.clone()) {
            warn!("Failed to dial bootstrap peer {}: {}", addr, e);
        }
    }

    loop {
        tokio::select! {
            swarm_event = swarm.next() => {
                let app_event = match swarm_event {
                    Some(SwarmEvent::NewListenAddr { address, .. }) => {
                        info!("New listen address: {}", address);
                        vec![Event::NewListenAddr { address }]
                    }
                    Some(SwarmEvent::ConnectionClosed { peer_id, .. }) => {
                        debug!("Connection closed: {}", peer_id);
                        vec![Event::ConnectionClosed { peer_id }]
                    }
                    Some(SwarmEvent::ConnectionEstablished { peer_id, .. }) => {
                        info!("Connection established with {}", peer_id);
                        vec![Event::PeerConnected { peer_id }]
                    }
                    Some(SwarmEvent::Behaviour(event)) => {
                        handle_behaviour_event(event, self_peer_id, &self_descriptor, &mut swarm, blob_provider.as_deref(), project_executor.clone(), &event_tx, &project_tx, &mut pending).await
                    }
                    Some(_) => Vec::new(),
                    None => break,
                };

                for event in app_event {
                    if event_tx.send(event).await.is_err() {
                        debug!("Event receiver dropped, shutting down swarm event loop");
                        break;
                    }
                }
            }

            Some(outcome) = project_rx.recv() => {
                let task_id = outcome.task_id;
                let channel = pending.remove(&task_id);
                if let Some(ch) = channel {
                    if let Err(res) = swarm
                        .behaviour_mut()
                        .project_exchange
                        .send_response(ch, outcome.result)
                    {
                        tracing::warn!(
                            "project task {task_id}: response channel closed before delivery \
                             (exit={})",
                            res.exit_code
                        );
                    }
                } else {
                    // Very unlikely: the job finished before the event loop
                    // registered the channel. Fail loudly instead of silently.
                    error!("project task {task_id}: finished with no pending response channel");
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
                    Some(SwarmCommand::SendRaftMessage { peer_id, data }) => {
                        let request = RaftMessageRequest { data };
                        swarm.behaviour_mut().raft_exchange
                            .send_request(&peer_id, request);
                    }
                    Some(SwarmCommand::RequestBlob { peer_id, hash }) => {
                        let request = BlobRequest { hash };
                        swarm.behaviour_mut().blob_exchange
                            .send_request(&peer_id, request);
                    }
                    Some(SwarmCommand::SendProjectTask { peer_id, task }) => {
                        swarm.behaviour_mut().project_exchange
                            .send_request(&peer_id, task);
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

#[allow(clippy::too_many_arguments)]
async fn handle_behaviour_event(
    event: EdgeOrchBehaviourEvent,
    self_peer_id: PeerId,
    self_descriptor: &NodeDescriptor,
    swarm: &mut EdgeOrchSwarm,
    blob_provider: Option<&dyn BlobProvider>,
    project_executor: Option<std::sync::Arc<dyn ProjectExecutor>>,
    event_tx: &mpsc::Sender<Event>,
    project_tx: &mpsc::Sender<ProjectOutcome>,
    pending: &mut HashMap<TaskId, libp2p::request_response::ResponseChannel<ProjectResult>>,
) -> Vec<Event> {
    match event {
        EdgeOrchBehaviourEvent::Mdns(mdns_event) => match mdns_event {
            libp2p::mdns::Event::Discovered(peers) => {
                let mut out = Vec::new();
                for (peer_id, addr) in peers {
                    if peer_id != self_peer_id {
                        out.push(Event::PeerDiscovered {
                            peer_id,
                            address: addr,
                        });
                    }
                }
                out
            }
            libp2p::mdns::Event::Expired(peers) => {
                let mut out = Vec::new();
                for (peer_id, _addr) in peers {
                    if peer_id != self_peer_id {
                        out.push(Event::PeerExpired { peer_id });
                    }
                }
                out
            }
        },

        EdgeOrchBehaviourEvent::Identify(identify::Event::Received { peer_id, info, .. }) => {
            info!(
                "Identified peer {}: agent={}, protocols={:?}",
                peer_id, info.agent_version, info.protocols
            );
            vec![Event::Identified { peer_id, info }]
        }
        EdgeOrchBehaviourEvent::Identify(_) => Vec::new(),

        EdgeOrchBehaviourEvent::Ping(ping_event) => {
            match ping_event.result {
                Ok(latency) => debug!("Ping {} OK: {}ms", ping_event.peer, latency.as_millis()),
                Err(e) => warn!("Ping {} failed: {}", ping_event.peer, e),
            }
            Vec::new()
        }

        EdgeOrchBehaviourEvent::DescriptorExchange(req_resp_event) => {
            handle_descriptor_exchange(req_resp_event, self_descriptor, swarm)
                .into_iter()
                .collect::<Vec<Event>>()
        }

        EdgeOrchBehaviourEvent::RaftExchange(raft_event) => handle_raft_exchange(raft_event, swarm)
            .into_iter()
            .collect::<Vec<Event>>(),
        EdgeOrchBehaviourEvent::BlobExchange(blob_event) => {
            handle_blob_exchange(blob_event, swarm, blob_provider)
                .into_iter()
                .collect::<Vec<Event>>()
        }
        EdgeOrchBehaviourEvent::ProjectExchange(pe) => {
            handle_project_exchange(pe, swarm, project_executor, event_tx, project_tx, pending)
                .await
                .into_iter()
                .collect::<Vec<Event>>()
        }
        .into_iter()
        .collect(),
    }
}

/// Failure result for a project task that could not be executed.
fn project_failure(task: &ProjectTask, exit_code: i32, msg: String) -> ProjectResult {
    ProjectResult {
        task_id: task.task_id,
        exit_code,
        stdout: Vec::new(),
        stderr: msg.into_bytes(),
        execution_time_ms: 0,
        executed_on: uuid::Uuid::nil(),
    }
}

/// A finished project job, handed back to the event loop for the response.
struct ProjectOutcome {
    task_id: TaskId,
    result: ProjectResult,
}

/// Handle an incoming project task.
///
/// The execution itself is long (VM boot + toolchain install + build + run:
/// tens of seconds to minutes) so it must NOT run on the swarm event loop: while
/// the loop is parked inside the job it processes no other event, which starves
/// every other protocol on that node (descriptor, raft, blob) and can tear down
/// the very connection that has to carry the result.
///
/// Therefore this handler returns immediately: it parks the request's response
/// channel in a pending map and spawns the job. When the job finishes it hands
/// the result back to the event loop, which is the only place allowed to call
/// `Behaviour::send_response`.
///
/// Awaiting the job here — even on a spawned task while awaiting only a oneshot —
/// does NOT work: the response channel is just a sender into the request-response
/// handler, so the response is only written when the swarm is polled again. Any
/// await of the job inside the event loop keeps the whole node (this protocol,
/// descriptor exchange, raft, blob) frozen for the duration of the build.
///
/// Every terminal outcome must produce a response: a silently dropped request
/// leaves the master polling `pending` until the 1800s protocol timeout.
async fn handle_project_exchange(
    event: libp2p::request_response::Event<ProjectTask, ProjectResult>,
    swarm: &mut EdgeOrchSwarm,
    project_executor: Option<std::sync::Arc<dyn ProjectExecutor>>,
    event_tx: &mpsc::Sender<Event>,
    project_tx: &mpsc::Sender<ProjectOutcome>,
    pending: &mut HashMap<TaskId, libp2p::request_response::ResponseChannel<ProjectResult>>,
) -> Option<Event> {
    use libp2p::request_response::{Event as RREvent, Message};
    match event {
        RREvent::Message { peer, message } => match message {
            Message::Request {
                request, channel, ..
            } => {
                let task_id = request.task_id;
                let Some(ex) = project_executor else {
                    let msg = "no project executor on this node (Linux+KVM required); \
                               task rejected instead of silently dropped"
                        .to_string();
                    warn!("project task {task_id} from {peer} rejected: {msg}");
                    let _ = swarm
                        .behaviour_mut()
                        .project_exchange
                        .send_response(channel, project_failure(&request, 101, msg));
                    return None;
                };

                info!(
                    "project task {task_id} accepted from {peer} (snapshot {} bytes, work_dir={}, \
                     timeout={}ms, build={:?}, run={:?})",
                    request.snapshot.tar_bytes.len(),
                    request.work_dir,
                    request.timeout_ms,
                    request.build_cmd,
                    request.run_cmd
                );

                // Run the minutes-long job on a detached task and let the event
                // loop keep running: the result travels back as a ProjectOutcome
                // and the loop completes the request-response transaction.
                let forward_tx = event_tx.clone();
                let outcome_tx = project_tx.clone();
                // Register the response channel first, then spawn: the job can
                // only finish after this, so the loop always finds the channel.
                pending.insert(task_id, channel);
                tokio::spawn(async move {
                    let result = match ex.run(request.clone()).await {
                        Ok(res) => {
                            info!(
                                "project task {task_id} finished: exit={} ({}ms) on node {}",
                                res.exit_code, res.execution_time_ms, res.executed_on
                            );
                            res
                        }
                        Err(e) => {
                            error!("project task {task_id} failed: {e:#}");
                            project_failure(&request, 1, format!("{e:#}"))
                        }
                    };
                    // Log the output tail: without it a failing build is
                    // invisible on the execution node.
                    if result.exit_code == 0 {
                        info!(
                            "project task {task_id} stdout tail: {}",
                            tail(&result.stdout, 3)
                        );
                    } else {
                        warn!(
                            "project task {task_id} exit={} stdout tail: {}",
                            result.exit_code,
                            tail(&result.stdout, 5)
                        );
                        warn!(
                            "project task {task_id} stderr tail: {}",
                            tail(&result.stderr, 10)
                        );
                    }
                    let _ = forward_tx
                        .send(Event::ProjectResultReceived {
                            peer_id: peer,
                            result: result.clone(),
                        })
                        .await;
                    if outcome_tx
                        .send(ProjectOutcome {
                            task_id,
                            result: result.clone(),
                        })
                        .await
                        .is_err()
                    {
                        error!(
                            "project task {task_id}: event loop gone, result (exit={}) dropped",
                            result.exit_code
                        );
                    }
                });
                None
            }
            Message::Response { response, .. } => Some(Event::ProjectResultReceived {
                peer_id: peer,
                result: response,
            }),
        },
        RREvent::OutboundFailure { peer, error, .. } => {
            warn!("Outbound project request failed to {}: {}", peer, error);
            None
        }
        RREvent::InboundFailure { peer, error, .. } => {
            warn!("Inbound project request failed from {}: {}", peer, error);
            None
        }
        RREvent::ResponseSent { .. } => None,
    }
}

/// Last `n` lines of a byte buffer, lossily decoded (for log tails).
fn tail(bytes: &[u8], n: usize) -> String {
    let text = String::from_utf8_lossy(bytes);
    let lines: Vec<&str> = text.lines().collect();
    let start = lines.len().saturating_sub(n);
    lines[start..].join(" | ")
}

fn handle_blob_exchange(
    event: libp2p::request_response::Event<BlobRequest, BlobResponse>,
    swarm: &mut EdgeOrchSwarm,
    blob_provider: Option<&dyn BlobProvider>,
) -> Option<Event> {
    use libp2p::request_response::{Event as RREvent, Message};

    match event {
        RREvent::Message { peer, message } => match message {
            Message::Request {
                request, channel, ..
            } => {
                let hash = request.hash.clone();
                let (found, data) = match blob_provider.and_then(|p| p.get_blob(&hash)) {
                    Some(bytes) => (true, bytes),
                    None => (false, Vec::new()),
                };
                let _ = swarm.behaviour_mut().blob_exchange.send_response(
                    channel,
                    BlobResponse {
                        hash: hash.clone(),
                        found,
                        data: data.clone(),
                    },
                );
                Some(Event::BlobRequestReceived {
                    peer_id: peer,
                    hash,
                })
            }
            Message::Response { response, .. } => Some(Event::BlobResponseReceived {
                peer_id: peer,
                hash: response.hash,
                found: response.found,
                data: response.data,
            }),
        },
        RREvent::OutboundFailure { peer, error, .. } => {
            warn!("Outbound blob request failed to {}: {}", peer, error);
            None
        }
        RREvent::InboundFailure { peer, error, .. } => {
            warn!("Inbound blob request failed from {}: {}", peer, error);
            None
        }
        RREvent::ResponseSent { .. } => None,
    }
}

fn handle_raft_exchange(
    event: libp2p::request_response::Event<RaftMessageRequest, RaftMessageResponse>,
    swarm: &mut EdgeOrchSwarm,
) -> Option<Event> {
    use libp2p::request_response::{Event as RREvent, Message};

    match event {
        RREvent::Message { peer, message } => match message {
            Message::Request {
                request, channel, ..
            } => {
                // Respond so the sender's request-response transaction completes;
                // the actual payload is delivered as an app event below.
                let _ = swarm
                    .behaviour_mut()
                    .raft_exchange
                    .send_response(channel, RaftMessageResponse { accepted: true });
                Some(Event::RaftMessageReceived {
                    peer_id: peer,
                    data: request.data,
                })
            }
            Message::Response { .. } => None,
        },
        RREvent::OutboundFailure { peer, error, .. } => {
            warn!("Outbound raft request failed to {}: {}", peer, error);
            None
        }
        RREvent::InboundFailure { peer, error, .. } => {
            warn!("Inbound raft request failed from {}: {}", peer, error);
            None
        }
        RREvent::ResponseSent { .. } => None,
    }
}

fn handle_descriptor_exchange(
    event: libp2p::request_response::Event<DescriptorRequest, DescriptorResponse>,
    self_descriptor: &NodeDescriptor,
    swarm: &mut EdgeOrchSwarm,
) -> Option<Event> {
    use libp2p::request_response::{Event as RREvent, Message};

    match event {
        RREvent::Message { peer, message } => match message {
            Message::Request {
                request, channel, ..
            } => {
                debug!(
                    "Descriptor request from {} (requester: {})",
                    peer, request.requester_id
                );
                let response = DescriptorResponse::from_descriptor(self_descriptor).ok()?;
                // Send the actual response so the remote can build raft mapping.
                let _ = swarm
                    .behaviour_mut()
                    .descriptor_exchange
                    .send_response(channel, response);
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
