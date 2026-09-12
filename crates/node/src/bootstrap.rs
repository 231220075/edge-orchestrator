//! Startup sequence for the edge-orchestrator node.

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use eo_core::types::NodeDescriptor;
use libp2p::identity;
use p2p::{new_swarm, Event, SwarmConfig, SwarmHandle};
use tracing::{debug, info, warn};

use crate::config::NodeConfig;
use crate::raft::network::{envelope_from_data, RaftIdRegistry};
use crate::raft::RaftEnvelope;

pub struct Node {
    pub descriptor: NodeDescriptor,
    pub swarm: SwarmHandle,
    // Held for future blob-distribution wiring; IPC already gets its own Arc.
    #[allow(dead_code)]
    pub object_store: Arc<storage::LocalObjectStore>,
    pub ipc_handle: Option<crate::ipc::server::IpcServerHandle>,
    raft_incoming: tokio::sync::mpsc::Sender<RaftEnvelope>,
    raft_registry: RaftIdRegistry,
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

        // 2. Generate identity keypair
        let keypair = identity::Keypair::generate_ed25519();
        let peer_id = keypair.public().to_peer_id();
        info!("Generated identity: peer_id={}", peer_id);

        // 3. Build descriptor (raft_id comes from config)
        let descriptor = config.to_descriptor();
        info!(
            "Node descriptor: node_id={}, raft_id={:?}, capabilities={:?}",
            descriptor.node_id, descriptor.raft_id, descriptor.capabilities
        );

        // 4. Build and start P2P swarm
        let listen_addresses: Vec<libp2p::Multiaddr> = config
            .listen_addresses
            .iter()
            .map(|a| a.parse())
            .collect::<std::result::Result<Vec<_>, _>>()
            .with_context(|| "Failed to parse listen addresses")?;

        let swarm_config = SwarmConfig {
            listen_addresses,
            bootstrap_peers: config
                .bootstrap_peers
                .iter()
                .filter_map(|s| s.parse().ok())
                .collect(),
        };

        let swarm = new_swarm(keypair, swarm_config, descriptor.clone())
            .context("Failed to start P2P swarm")?;
        info!("P2P swarm started successfully");

        // 5. Initialize CAS object store
        let store_root = store_dir.to_path_buf();
        let object_store = Arc::new(
            storage::LocalObjectStore::new(store_root.clone())
                .context("Failed to initialize CAS object store")?,
        );
        info!("CAS object store initialized at {}", store_root.display());

        // 6. Initialize Raft consensus (static cluster)
        // A node without raft_id is a light client: still runs storage/IPC but
        // does not vote. Its proposal channel is unused (see below).
        let raft_id = config.raft_id.unwrap_or(0);
        let raft_peers = config.raft_peers.clone();

        let (transport, incoming_tx) =
            crate::raft::network::create_raft_transport(swarm.commands.clone());
        let raft_registry = transport.registry().clone();

        let proposal_tx = if raft_id == 0 {
            let (tx, _rx) = tokio::sync::mpsc::channel(1);
            tx
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
            info!(
                "Raft consensus initialized: raft_id={}, peers={:?}",
                raft_id, raft_peers
            );

            tokio::spawn(async move {
                if let Err(e) = raft_node.run().await {
                    tracing::error!("Raft node event loop error: {:#}", e);
                }
            });
            tx
        };

        // 7. Start IPC server
        let ipc_handle = if let Some(socket_path) = ipc_socket_path {
            let ipc_handler =
                crate::ipc::JsonRpcHandler::new(proposal_tx, Arc::clone(&object_store));
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
        })
    }

    pub async fn run_event_monitor(&mut self) -> Result<()> {
        info!("Node event monitor started");

        loop {
            match self.swarm.events.recv().await {
                Some(Event::PeerDiscovered { peer_id }) => {
                    info!("mDNS: discovered peer {}", peer_id);
                    // Learn the peer's raft_id by fetching its descriptor.
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
                }
                None => {
                    info!("Event stream closed");
                    break;
                }
            }
        }
        Ok(())
    }
}
