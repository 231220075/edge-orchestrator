//! Startup sequence for the edge-orchestrator node.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
#[cfg(target_os = "linux")]
use eo_core::types::NodeId;
use eo_core::types::{NodeDescriptor, ProjectResult, TaskId};
use libp2p::identity;
use p2p::{Event, SwarmConfig, SwarmHandle};
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

/// Startup preflight for the project sandbox (Linux + KVM + qlean).
///
/// Reports every precondition and, when something is wrong, logs the exact
/// command that fixes it. Rationale: the failure modes here are silent — a guest
/// that boots without a NIC still answers every `apt-get` with exit code 0, so
/// the pipeline looks healthy while installing nothing. Host-side facts (KVM
/// permissions, the bridge, the bridge helper, upstream reachability) can all be
/// checked without booting a VM, so they are checked at every startup instead of
/// being rediscovered during a debugging session.
#[cfg(target_os = "linux")]
fn sandbox_preflight() {
    use std::path::Path;

    if !Path::new("/dev/kvm").exists() {
        warn!("preflight: /dev/kvm missing — the sandbox cannot boot a VM (enable virtualization / nested KVM)");
    } else if !is_rw("/dev/kvm") {
        warn!(
            "preflight: no rw access to /dev/kvm — run: sudo usermod -aG kvm $USER && \
             re-login (current groups: {})",
            std::env::var("USER").unwrap_or_else(|_| "?".into())
        );
    }

    let bridge = std::env::var("EO_BRIDGE").unwrap_or_else(|_| "qlbr0".into());
    if Path::new(&format!("/sys/class/net/{bridge}")).exists() {
        info!("preflight: bridge {bridge} present");
    } else {
        warn!(
            "preflight: bridge {bridge} missing — the guest will boot WITHOUT a NIC and every \
             apt command will silently do nothing. Fix: sudo ./scripts/host_network_check.sh --fix"
        );
    }

    let helper = [
        "/usr/lib/qemu/qemu-bridge-helper",
        "/usr/libexec/qemu-bridge-helper",
    ]
    .iter()
    .find(|p| Path::new(p).exists());
    match helper {
        Some(_) => debug!("preflight: qemu-bridge-helper found"),
        None => warn!("preflight: qemu-bridge-helper not found — install qemu-system-common"),
    }
    if let Ok(conf) = std::fs::read_to_string("/etc/qemu/bridge.conf") {
        if !conf.lines().any(|l| l.trim() == format!("allow {bridge}")) {
            warn!("preflight: /etc/qemu/bridge.conf has no 'allow {bridge}' line");
        }
    } else {
        warn!("preflight: /etc/qemu/bridge.conf missing (add 'allow {bridge}')");
    }

    // Cheapest end-to-end signal: can this host fetch a Debian index at all? If
    // not, no amount of guest-side mirror configuration can help.
    match std::process::Command::new("curl")
        .args([
            "-sf",
            "-o",
            "/dev/null",
            "--max-time",
            "8",
            "http://deb.debian.org/debian/dists/trixie/Release",
        ])
        .status()
    {
        Ok(st) if st.success() => {
            info!("preflight: upstream Debian mirror reachable from this host")
        }
        _ => warn!(
            "preflight: cannot reach deb.debian.org from this host — project toolchain installs \
             will fail; run ./scripts/host_network_check.sh for details"
        ),
    }
}

#[cfg(target_os = "linux")]
fn is_rw(path: &str) -> bool {
    use std::fs::OpenOptions;
    OpenOptions::new().read(true).write(true).open(path).is_ok()
}

/// Build a project executor. Only Linux+KVM nodes can actually run projects,
/// so non-Linux nodes return None (they never accept project tasks).
#[cfg(target_os = "linux")]
fn make_project_executor(
    node_id: NodeId,
    cas: Arc<crate::cas_fetch::BlobFetcher>,
    store: Arc<storage::LocalObjectStore>,
    vm_mode: sandbox::VmMode,
    pool_size: usize,
    template: Option<sandbox::ImageTemplate>,
) -> Option<Arc<dyn p2p::ProjectExecutor>> {
    match sandbox::QleanSandbox::with_template(vm_mode, pool_size, template) {
        Ok(sb) => Some(Arc::new(
            crate::project_executor::QleanProjectExecutor::new(Arc::new(sb), node_id, cas, store),
        )),
        Err(e) => {
            warn!("qlean sandbox unavailable, projects disabled: {e}");
            None
        }
    }
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
    /// Master-side project submitter (cluster members and light clients).
    project_client: Arc<crate::project_client::ProjectClient>,
    /// peer_id -> descriptor, learned over the mesh (light-client topology).
    known_peers: Arc<Mutex<HashMap<libp2p::PeerId, NodeDescriptor>>>,
    /// Pull-side CAS access (fetch a blob this node does not have).
    #[cfg(target_os = "linux")]
    cas: Arc<crate::cas_fetch::BlobFetcher>,
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
        let mut descriptor = config.to_descriptor();

        // Report the effective sandbox policy in the config: this is the answer to
        // "why did my task behave like that" (fresh vs reuse changes isolation),
        // and it also surfaces a typo in project_vm_mode on any platform.
        let (wants_project_sandbox, project_vm_mode) = config.project_sandbox_policy();
        // A bad template config must be reported now, not as a failed download later.
        let custom_image = match config.project_image_template() {
            Ok(t) => t,
            Err(e) => {
                warn!("ignoring custom sandbox image config: {e}");
                None
            }
        };
        info!(
            "Project sandbox policy: enabled={wants_project_sandbox}, vm_mode={project_vm_mode:?}, \
             pool={}, base_image={} (reuse = warm VM shared between tasks, fresh = a new \
             machine per task; pool pre-boots replacements to hide the boot wait)",
            config.project_vm_pool_size(),
            match &custom_image {
                Some(t) => t.source.as_str(),
                None => "builtin",
            }
        );

        // 3. Preflight the sandbox in the configuration that claims to support it,
        // so a broken host reports itself instead of producing silent no-op tasks.
        if descriptor.capabilities.project_sandbox {
            #[cfg(target_os = "linux")]
            sandbox_preflight();
        }

        // 3b. Initialize CAS object store. Needed before the executor: the
        // snapshot arrives over the blob protocol and lands here.
        let store_root = store_dir.to_path_buf();
        let object_store = Arc::new(
            storage::LocalObjectStore::new(store_root.clone())
                .context("Failed to initialize CAS object store")?,
        );
        info!("CAS object store initialized at {}", store_root.display());

        // (The project executor is resolved after the swarm: it needs the CAS
        // fetcher so it can pull a snapshot it does not have locally.)

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

        // The swarm command channel is created here, before the swarm itself:
        // the project executor (which the swarm calls) needs it to fetch
        // snapshots from the CAS, so one half goes to the executor's fetcher and
        // the other to the swarm event loop.
        let (swarm_cmd_tx, swarm_cmd_rx) = tokio::sync::mpsc::channel::<p2p::SwarmCommand>(64);

        // 4b. Raft transport and its peer registry: the registry tells the fetcher
        // which peers to ask for a blob it does not have.
        let (transport, incoming_tx) =
            crate::raft::network::create_raft_transport(swarm_cmd_tx.clone());
        let raft_registry = transport.registry().clone();

        // 4c. Pull-side CAS access, shared with the project executor below.
        #[cfg(target_os = "linux")]
        let cas_fetcher = Arc::new(crate::cas_fetch::BlobFetcher::new(
            Arc::clone(&object_store),
            swarm_cmd_tx.clone(),
            raft_registry.clone(),
        ));

        // 4d. Project executor: resolved BEFORE advertising the capability, so a
        // node that cannot really execute projects never claims it.
        #[cfg(target_os = "linux")]
        let project_executor = make_project_executor(
            descriptor.node_id,
            Arc::clone(&cas_fetcher),
            Arc::clone(&object_store),
            config.project_vm_mode(),
        );
        #[cfg(not(target_os = "linux"))]
        let project_executor: Option<Arc<dyn p2p::ProjectExecutor>> = None;
        if project_executor.is_none() && descriptor.capabilities.project_sandbox {
            warn!(
                "no usable project sandbox on this node (Linux + KVM + qlean required); \
                 downgrading advertised project_sandbox=false so the master does not route \
                 project tasks here"
            );
            descriptor.capabilities.project_sandbox = false;
        }
        info!(
            "Node descriptor: node_id={}, raft_id={:?}, capabilities={:?}",
            descriptor.node_id, descriptor.raft_id, descriptor.capabilities
        );

        let swarm = p2p::new_swarm_with_commands(
            keypair,
            swarm_config,
            descriptor.clone(),
            Some(blob_provider),
            project_executor,
            swarm_cmd_tx,
            swarm_cmd_rx,
        )
        .context("Failed to start P2P swarm")?;
        info!("P2P swarm started successfully");

        // 6. Initialize Raft consensus (static cluster)
        // A node without raft_id is a light client: still runs storage/IPC but
        // does not vote. Its proposal channel is unused (see below).
        let raft_id = config.raft_id.unwrap_or(0);
        let raft_peers = config.raft_peers.clone();

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

        // 6b. Master-side project client. Cluster members resolve executors
        // from Raft state; light clients (no raft_id) resolve them from peer
        // descriptors learned over the mesh.
        let project_results: Arc<Mutex<HashMap<TaskId, ProjectResult>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let known_peers: Arc<Mutex<HashMap<libp2p::PeerId, NodeDescriptor>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let catalog = match state_handle_opt {
            Some(st) => crate::project_client::Catalog::Raft {
                state: st,
                registry: raft_registry.clone(),
            },
            None => crate::project_client::Catalog::Peers(Arc::clone(&known_peers)),
        };
        let project_client = Arc::new(crate::project_client::ProjectClient::new(
            swarm.commands.clone(),
            catalog,
            Arc::clone(&project_results),
            Arc::clone(&object_store),
            descriptor.node_id,
        ));

        // 7. Start IPC server
        let ipc_handle = if let Some(socket_path) = ipc_socket_path {
            let ipc_handler = crate::ipc::JsonRpcHandler::new(
                proposal_tx,
                Arc::clone(&object_store),
                Some(Arc::clone(&project_client)),
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
            known_peers,
            #[cfg(target_os = "linux")]
            cas: cas_fetcher,
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
                    // Light-client topology: remember peer descriptors so the
                    // project client can resolve a capable executor.
                    if let Ok(mut peers) = self.known_peers.lock() {
                        peers.insert(peer_id, descriptor);
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
                    let len = data.len();
                    #[cfg(target_os = "linux")]
                    let stored = self.cas.on_blob_received(&hash, found, data);
                    #[cfg(not(target_os = "linux"))]
                    let stored = {
                        let _ = data;
                        false
                    };
                    if stored {
                        info!("cas: blob {} fetched from {} ({} bytes)", hash, peer_id, len);
                    } else if !found {
                        warn!("cas: peer {} does not have blob {}", peer_id, hash);
                    }
                }
                Some(Event::ProjectResultReceived { peer_id, result }) => {
                    info!(
                        "project result {} from {} exit={} ({}ms)",
                        result.task_id, peer_id, result.exit_code, result.execution_time_ms
                    );
                    self.project_client.record_result(result);
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
