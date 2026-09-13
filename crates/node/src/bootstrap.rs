//! Startup sequence for the edge-orchestrator node.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use eo_core::types::{NodeDescriptor, NodeId, ProjectResult, TaskId};
use libp2p::identity;
use p2p::{new_swarm, Event, SwarmConfig, SwarmHandle};
use tracing::{debug, info, warn};

use crate::config::NodeConfig;
use crate::raft::network::{envelope_from_data, RaftIdRegistry};
use crate::raft::RaftEnvelope;

/// Wraps the CAS store so the p2p crate can answer blob requests.
struct CasBlobProvider {
    store: Arc<storage::LocalObjectStore>,
}

impl p2p::BlobProvider for CasBlobProvider {
    fn get_blob(&self, hash: &str) -> Option<Vec<u8>> {
        self.store.get_blob(&hash.to_string()).ok()
    }
}

/// Build a project executor. Only Linux+KVM nodes can actually run projects,
/// so non-Linux nodes return None (they never accept project tasks).
#[cfg(target_os = "linux")]
fn make_project_executor(node_id: NodeId) -> Option<Arc<dyn p2p::ProjectExecutor>> {
    match sandbox::QleanSandbox::new() {
        Ok(sb) => Some(Arc::new(
            crate::project_executor::QleanProjectExecutor::new(Arc::new(sb), node_id),
        )),
        Err(e) => {
            warn!("qlean sandbox unavailable, projects disabled: {e}");
            None
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn make_project_executor(_node_id: NodeId) -> Option<Arc<dyn p2p::ProjectExecutor>> {
    None
}

pub struct Node {
    pub descriptor: NodeDescriptor,
    pub swarm: SwarmHandle,
    // Held for future blob-distribution wiring; IPC already gets its own Arc.
    #[allow(dead_code)]
    pub object_store: Arc<storage::LocalObjectStore>,
    pub ipc_handle: Option<crate::ipc::server::IpcServerHandle>,
    raft_incoming: tokio::sync::mpsc::Sender<RaftEnvelope>,
    raft_registry: RaftIdRegistry,
    /// Known peer addresses (PeerId -> last advertised Multiaddr) for
    /// re-dialing when a connection closes.
    peer_addrs: std::collections::HashMap<libp2p::PeerId, libp2p::Multiaddr>,
    /// Configured bootstrap peers, re-dialed periodically to form a mesh.
    bootstrap_addrs: Vec<libp2p::Multiaddr>,
    /// Master-side project submitter (present on cluster members).
    project_client: Option<Arc<crate::project_client::ProjectClient>>,
}

impl Node {
    pub async fn bootstrap(
        config_path: &Path,
        node_id_override: Option<&str>,
        ipc_socket_path: Option<&Path>,
        store_dir: &Path,
    ) -> Result<Self> {
        // 1. Load config
        let mut config = NodeConfig::load(config_path)?;
        if let Some(id) = node_id_override {
            config.node_id = id.to_string();
        }
        info!(
            "Loaded config: node_type={}, listen_addresses={:?}",
            config.node_type, config.listen_addresses
        );

        // 2. Identity: deterministic seed if provided, else fresh ed25519.
        let keypair = match &config.identity_seed {
            Some(hex_seed) => {
                let bytes =
                    hex::decode(hex_seed).with_context(|| "identity_seed must be hex-encoded")?;
                let secret = libp2p::identity::ed25519::SecretKey::try_from_bytes(bytes)
                    .map_err(|e| anyhow::anyhow!("bad identity_seed: {e}"))?;
                identity::Keypair::from(libp2p::identity::ed25519::Keypair::from(secret))
            }
            None => identity::Keypair::generate_ed25519(),
        };
        let peer_id = keypair.public().to_peer_id();
        info!("Identity ready: peer_id={}", peer_id);

        // 3. Build descriptor (raft_id comes from config)
        let descriptor = config.to_descriptor();
        info!(
            "Node descriptor: node_id={}, raft_id={:?}, capabilities={:?}",
            descriptor.node_id, descriptor.raft_id, descriptor.capabilities
        );

        // 3b. Initialize CAS object store (needed by the swarm blob protocol)
        let store_root = store_dir.to_path_buf();
        let object_store = Arc::new(
            storage::LocalObjectStore::new(store_root.clone())
                .context("Failed to initialize CAS object store")?,
        );
        info!("CAS object store initialized at {}", store_root.display());

        // 4. Build and start P2P swarm
        let listen_addresses: Vec<libp2p::Multiaddr> = config
            .listen_addresses
            .iter()
            .map(|a| a.parse())
            .collect::<std::result::Result<Vec<_>, _>>()
            .with_context(|| "Failed to parse listen addresses")?;

        let bootstrap_addrs: Vec<libp2p::Multiaddr> = config
            .bootstrap_peers
            .iter()
            .filter_map(|s| s.parse().ok())
            .collect();
        let swarm_config = SwarmConfig {
            listen_addresses,
            bootstrap_peers: bootstrap_addrs.clone(),
        };

        let blob_provider = Arc::new(CasBlobProvider {
            store: Arc::clone(&object_store),
        });
        let swarm = new_swarm(
            keypair,
            swarm_config,
            descriptor.clone(),
            Some(blob_provider),
            make_project_executor(descriptor.node_id),
        )
        .context("Failed to start P2P swarm")?;
        info!("P2P swarm started successfully");

        // 6. Initialize Raft consensus (static cluster)
        // A node without raft_id is a light client: still runs storage/IPC but
        // does not vote. Its proposal channel is unused (see below).
        let raft_id = config.raft_id.unwrap_or(0);
        let raft_peers = config.raft_peers.clone();

        let (transport, incoming_tx) =
            crate::raft::network::create_raft_transport(swarm.commands.clone());
        let raft_registry = transport.registry().clone();

        let (proposal_tx, state_handle_opt) = if raft_id == 0 {
            let (tx, _rx) = tokio::sync::mpsc::channel(1);
            (tx, None)
        } else {
            let mut raft_node = crate::raft::RaftNode::new(
                raft_id,
                raft_peers.clone(),
                Arc::clone(&object_store),
                transport,
            )
            .await
            .context("Failed to create Raft node")?;

            let tx = raft_node.proposal_sender();
            let state_handle = raft_node.state_handle();
            info!(
                "Raft consensus initialized: raft_id={}, peers={:?}",
                raft_id, raft_peers
            );

            tokio::spawn(async move {
                if let Err(e) = raft_node.run().await {
                    tracing::error!("Raft node event loop error: {:#}", e);
                }
            });

            // Register this node in the replicated state so the scheduler can
            // discover its raft_id and Execution role. Registration is retried
            // in spawn_runtime until a leader commits it (the first propose is
            // dropped if sent before the initial election finishes).
            let mut self_desc = descriptor.clone();
            self_desc.current_assigned_roles = config
                .roles
                .clone()
                .into_iter()
                .filter_map(|r| match r.as_str() {
                    "Storage" => Some(eo_core::types::Role::Storage),
                    "Execution" => Some(eo_core::types::Role::Execution),
                    "Inference" => Some(eo_core::types::Role::Inference),
                    "Coordinator" => Some(eo_core::types::Role::Coordinator),
                    "Bootstrap" => Some(eo_core::types::Role::Bootstrap),
                    _ => None,
                })
                .collect();
            crate::orchestration::runtime_loop::spawn_register_loop(
                raft_id,
                self_desc,
                tx.clone(),
                state_handle.clone(),
            );

            // Scheduler (every node may propose AssignTask) and executor
            // (only runs tasks assigned to itself). Both read replicated state.
            crate::orchestration::runtime_loop::spawn_runtime(
                raft_id,
                state_handle.clone(),
                tx.clone(),
                Arc::clone(&object_store),
                swarm.commands.clone(),
                raft_registry.clone(),
            );

            (tx, Some(state_handle))
        };

        // 6b. Master-side project client: routes ProjectTask to capable
        // executors and tracks returned results. Only cluster members (with
        // replicated state) can resolve executors, so light clients get None.
        let project_results: Arc<Mutex<HashMap<TaskId, ProjectResult>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let project_client = state_handle_opt.map(|st| {
            Arc::new(crate::project_client::ProjectClient::new(
                swarm.commands.clone(),
                raft_registry.clone(),
                st,
                Arc::clone(&project_results),
                descriptor.node_id,
            ))
        });

        // 7. Start IPC server
        let ipc_handle = if let Some(socket_path) = ipc_socket_path {
            let ipc_handler = crate::ipc::JsonRpcHandler::new(
                proposal_tx,
                Arc::clone(&object_store),
                project_client.clone(),
            );
            let ipc_server = crate::ipc::IpcServer::new(socket_path.to_path_buf(), ipc_handler);
            let handle = ipc_server.start();
            info!("IPC server listening on {}", socket_path.display());
            Some(handle)
        } else {
            info!("IPC server disabled (--no-ipc)");
            None
        };

        Ok(Node {
            descriptor,
            swarm,
            object_store,
            ipc_handle,
            raft_incoming: incoming_tx,
            raft_registry,
            peer_addrs: HashMap::new(),
            bootstrap_addrs,
            project_client,
        })
    }

    pub async fn run_event_monitor(&mut self) -> Result<()> {
        info!("Node event monitor started");
        let mut dial_tick = tokio::time::interval(std::time::Duration::from_secs(2));

        loop {
            tokio::select! {
                _ = dial_tick.tick() => {
                    // Periodic mesh maintenance: re-dial configured bootstrap
                    // peers and every peer we have seen. libp2p dial is cheap
                    // when already connected and repairs races/churn.
                    for addr in self.bootstrap_addrs.clone() {
                        let _ = self.swarm.commands
                            .send(p2p::SwarmCommand::Dial { addr })
                            .await;
                    }
                    for addr in self.peer_addrs.values().cloned() {
                        let _ = self.swarm.commands
                            .send(p2p::SwarmCommand::Dial { addr })
                            .await;
                    }
                }
                ev = self.swarm.events.recv() => {
            match ev {
                Some(Event::PeerDiscovered { peer_id, address }) => {
                    info!("mDNS: discovered peer {} at {}", peer_id, address);
                    self.peer_addrs.insert(peer_id, address.clone());
                    // Establish a connection; descriptor request happens on
                    // PeerConnected once dialing has finished.
                    let _ = self
                        .swarm
                        .commands
                        .send(p2p::SwarmCommand::Dial { addr: address })
                        .await;
                }
                Some(Event::PeerConnected { peer_id }) => {
                    info!("Connected to peer {}", peer_id);
                    let _ = self
                        .swarm
                        .commands
                        .send(p2p::SwarmCommand::RequestDescriptor { peer_id })
                        .await;
                }
                Some(Event::PeerExpired { peer_id }) => {
                    info!("mDNS: peer expired {}", peer_id);
                }
                Some(Event::DescriptorReceived {
                    peer_id,
                    descriptor,
                }) => {
                    info!(
                        "Descriptor from {}: node_type={:?}, raft_id={:?}",
                        peer_id, descriptor.node_type, descriptor.raft_id
                    );
                    if let Some(raft_id) = descriptor.raft_id {
                        self.raft_registry.insert(raft_id, peer_id);
                        info!("Mapped raft id {} -> peer {}", raft_id, peer_id);
                    }
                }
                Some(Event::BlobRequestReceived { peer_id, hash }) => {
                    // Swarm already answered with the provider; here we just
                    // observe requests for observability.
                    debug!("Blob {} requested by {}", hash, peer_id);
                }
                Some(Event::BlobResponseReceived {
                    peer_id,
                    hash,
                    found,
                    data,
                }) => {
                    if found && !data.is_empty() {
                        let _ = self.object_store.put_blob(&data);
                        info!("Blob {} fetched from {} ({} bytes)", hash, peer_id, data.len());
                    }
                }
                Some(Event::ProjectTaskReceived { peer_id, task }) => {
                    // Swarm already ran the executor and replied; this is
                    // observability on the serving node.
                    info!(
                        "project task {} received from {}",
                        task.task_id, peer_id
                    );
                }
                Some(Event::ProjectResultReceived { peer_id, result }) => {
                    info!(
                        "project result {} from {} exit={}",
                        result.task_id, peer_id, result.exit_code
                    );
                    if let Some(pc) = &self.project_client {
                        pc.record_result(result);
                    }
                }
                Some(Event::RaftMessageReceived { peer_id, data }) => {
                    debug!("Got raft bytes from {}", peer_id);
                    if self
                        .raft_incoming
                        .send(envelope_from_data(data))
                        .await
                        .is_err()
                    {
                        warn!("Raft incoming channel closed");
                    }
                }
                Some(Event::DescriptorSent { peer_id }) => {
                    info!("Sent descriptor to {}", peer_id);
                }
                Some(Event::NewListenAddr { address }) => {
                    info!("Listening on {}", address);
                }
                Some(Event::Identified { peer_id, info }) => {
                    info!("Identified {}: agent={}", peer_id, info.agent_version);
                }
                Some(Event::ConnectionClosed { peer_id }) => {
                    info!("Connection closed with {}", peer_id);
                    if let Some(addr) = self.peer_addrs.get(&peer_id).cloned() {
                        let _ = self
                            .swarm
                            .commands
                            .send(p2p::SwarmCommand::Dial { addr })
                            .await;
                    }
                }
                None => {
                    info!("Event stream closed");
                    break;
                }
            }
            }
            }
        }
        Ok(())
    }
}
