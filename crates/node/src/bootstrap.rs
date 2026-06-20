//! Startup sequence for the edge-orchestrator node.
//!
//! Orchestrates: config → identity → P2P swarm → CAS storage →
//! Raft consensus → IPC server → event monitoring.

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use eo_core::types::NodeDescriptor;
use libp2p::identity;
use p2p::{new_swarm, Event, SwarmConfig, SwarmHandle};
use tracing::info;

use crate::config::NodeConfig;

/// The fully-initialized node runtime.
pub struct Node {
    /// The node's descriptor advertised to peers.
    pub descriptor: NodeDescriptor,
    /// Handle for interacting with the running P2P swarm.
    pub swarm: SwarmHandle,
    /// Content-addressed object store.
    pub object_store: Arc<storage::LocalObjectStore>,
    /// IPC server handle (None if IPC is disabled).
    pub ipc_handle: Option<eo_ipc::server::IpcServerHandle>,
}

impl Node {
    /// Execute the full startup sequence.
    ///
    /// 1. Load configuration
    /// 2. Generate or load identity keypair
    /// 3. Build [`NodeDescriptor`]
    /// 4. Start P2P swarm
    /// 5. Initialize CAS object store
    /// 6. Initialize Raft consensus
    /// 7. Start IPC server (unless ``--no-ipc``)
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

        // 3. Build descriptor
        let descriptor = config.to_descriptor();
        info!(
            "Node descriptor: node_id={}, node_type={:?}, os={:?}, capabilities={:?}",
            descriptor.node_id, descriptor.node_type, descriptor.os, descriptor.capabilities
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
            bootstrap_peers: vec![],
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

        // 6. Initialize Raft consensus (single-node cluster)
        let raft_node_id = 1;
        let raft_peers = vec![raft_node_id]; // self is the only peer

        let _cas_storage = eo_raft::CasRaftStorage::new(Arc::clone(&object_store));
        let (transport, _cmd_rx, _msg_tx) = eo_raft::create_raft_transport();

        let mut raft_node = eo_raft::RaftNode::new(raft_node_id, raft_peers.clone(), transport)
            .await
            .context("Failed to create Raft node")?;

        let proposal_tx = raft_node.proposal_sender();
        info!(
            "Raft consensus initialized: node_id={}, peers={:?}",
            raft_node_id, raft_peers
        );

        // Spawn the Raft event loop on a background task
        tokio::spawn(async move {
            if let Err(e) = raft_node.run().await {
                tracing::error!("Raft node event loop error: {:#}", e);
            }
        });
        info!("Raft event loop spawned");

        // 7. Start IPC server (unless disabled)
        let ipc_handle = if let Some(socket_path) = ipc_socket_path {
            let ipc_handler = eo_ipc::JsonRpcHandler::new(proposal_tx, Arc::clone(&object_store));
            let ipc_server = eo_ipc::IpcServer::new(socket_path.to_path_buf(), ipc_handler);
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
        })
    }

    /// Monitor P2P events and log them.
    pub async fn run_event_monitor(&mut self) -> Result<()> {
        info!("Node event monitor started — waiting for P2P events...");

        loop {
            match self.swarm.events.recv().await {
                Some(Event::PeerDiscovered { peer_id }) => {
                    info!("mDNS: discovered peer {}", peer_id);
                }
                Some(Event::PeerExpired { peer_id }) => {
                    info!("mDNS: peer expired {}", peer_id);
                }
                Some(Event::DescriptorReceived {
                    peer_id,
                    descriptor,
                }) => {
                    info!(
                        "Received descriptor from {}: node_type={:?}, capabilities={:?}",
                        peer_id, descriptor.node_type, descriptor.capabilities
                    );
                }
                Some(Event::DescriptorSent { peer_id }) => {
                    info!("Sent descriptor to {}", peer_id);
                }
                Some(Event::NewListenAddr { address }) => {
                    info!("Listening on {}", address);
                }
                Some(Event::Identified { peer_id, info }) => {
                    info!(
                        "Identified peer {}: agent={}, protocols={:?}",
                        peer_id, info.agent_version, info.protocols
                    );
                }
                Some(Event::ConnectionClosed { peer_id }) => {
                    info!("Connection closed with {}", peer_id);
                }
                None => {
                    info!("Event stream closed — shutting down");
                    break;
                }
            }
        }

        Ok(())
    }
}
