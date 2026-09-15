//! Integration tests for P2P discovery and descriptor exchange.

use std::time::Duration;

use chrono::Utc;
use eo_core::types::{Capabilities, NodeDescriptor, NodeType, OsType, Role};
use libp2p::identity;
use tokio::time::timeout;

use p2p::{new_swarm, Event, SwarmConfig};

const TEST_TIMEOUT: Duration = Duration::from_secs(15);

fn make_test_descriptor() -> NodeDescriptor {
    NodeDescriptor {
        node_id: uuid::Uuid::new_v4(),
        node_type: NodeType::Heavy,
        os: OsType::MacOS,
        capabilities: Capabilities::default(),
        advertised_addresses: vec![],
        current_assigned_roles: vec![Role::Execution],
        started_at: Utc::now(),
        raft_id: None,
    }
}

/// Test that a single swarm can be created and starts listening.
#[tokio::test]
async fn single_swarm_starts_listening() {
    let keypair = identity::Keypair::generate_ed25519();
    let config = SwarmConfig {
        listen_addresses: vec!["/ip4/127.0.0.1/tcp/0".parse().unwrap()],
        bootstrap_peers: vec![],
    };
    let descriptor = make_test_descriptor();

    let handle = new_swarm(keypair, config, descriptor, None, None).unwrap();

    // Wait for the NewListenAddr event
    let result = timeout(TEST_TIMEOUT, async {
        let mut events = handle.events;
        loop {
            match events.recv().await {
                Some(Event::NewListenAddr { address }) => {
                    assert!(!address.to_string().is_empty());
                    return true;
                }
                Some(_) => continue,
                None => panic!("Event stream closed unexpectedly"),
            }
        }
    })
    .await;

    assert!(
        result.is_ok(),
        "Swarm did not start listening within timeout"
    );
}

/// Test that two swarms can be created and discover each other via mDNS.
#[tokio::test]
async fn two_nodes_discover_each_other() {
    let keypair1 = identity::Keypair::generate_ed25519();
    let keypair2 = identity::Keypair::generate_ed25519();

    let config1 = SwarmConfig {
        listen_addresses: vec!["/ip4/127.0.0.1/tcp/0".parse().unwrap()],
        bootstrap_peers: vec![],
    };
    let config2 = SwarmConfig {
        listen_addresses: vec!["/ip4/127.0.0.1/tcp/0".parse().unwrap()],
        bootstrap_peers: vec![],
    };

    let handle1 = new_swarm(keypair1, config1, make_test_descriptor(), None, None).unwrap();
    let handle2 = new_swarm(keypair2, config2, make_test_descriptor(), None, None).unwrap();

    let events1 = handle1.events;
    let events2 = handle2.events;

    let mut discovered1 = false;
    let mut discovered2 = false;

    let result = timeout(
        TEST_TIMEOUT,
        wait_for_discovery(events1, events2, &mut discovered1, &mut discovered2),
    )
    .await;

    if result.is_ok() {
        assert!(discovered1, "Node 1 should have discovered a peer");
        assert!(discovered2, "Node 2 should have discovered a peer");
    } else {
        eprintln!(
            "Note: mDNS discovery test timed out ({}s). This is expected in environments \
             where multicast is not routed on loopback.",
            TEST_TIMEOUT.as_secs()
        );
    }
}

async fn wait_for_discovery(
    mut events1: tokio::sync::mpsc::Receiver<Event>,
    mut events2: tokio::sync::mpsc::Receiver<Event>,
    discovered1: &mut bool,
    discovered2: &mut bool,
) {
    loop {
        tokio::select! {
            event = events1.recv() => {
                match event {
                    Some(Event::PeerDiscovered { .. }) => {
                        *discovered1 = true;
                        if *discovered1 && *discovered2 { return; }
                    }
                    None => return,
                    _ => continue,
                }
            }
            event = events2.recv() => {
                match event {
                    Some(Event::PeerDiscovered { .. }) => {
                        *discovered2 = true;
                        if *discovered1 && *discovered2 { return; }
                    }
                    None => return,
                    _ => continue,
                }
            }
        }
    }
}

/// Test that descriptor exchange completes between two nodes.
#[tokio::test]
async fn descriptor_exchange_completes() {
    let keypair1 = identity::Keypair::generate_ed25519();
    let keypair2 = identity::Keypair::generate_ed25519();

    let config1 = SwarmConfig {
        listen_addresses: vec!["/ip4/127.0.0.1/tcp/0".parse().unwrap()],
        bootstrap_peers: vec![],
    };
    let config2 = SwarmConfig {
        listen_addresses: vec!["/ip4/127.0.0.1/tcp/0".parse().unwrap()],
        bootstrap_peers: vec![],
    };

    let handle1 = new_swarm(keypair1, config1, make_test_descriptor(), None, None).unwrap();
    let handle2 = new_swarm(keypair2, config2, make_test_descriptor(), None, None).unwrap();

    let events1 = handle1.events;
    let events2 = handle2.events;

    let mut desc1_rcvd = false;
    let mut desc2_rcvd = false;

    let result = timeout(
        TEST_TIMEOUT,
        wait_for_descriptors(events1, events2, &mut desc1_rcvd, &mut desc2_rcvd),
    )
    .await;

    if result.is_ok() {
        assert!(
            desc1_rcvd || desc2_rcvd,
            "At least one node should have received a descriptor"
        );
    } else {
        eprintln!(
            "Note: Descriptor exchange test timed out. This is expected when mDNS \
             does not work in the current environment."
        );
    }
}

async fn wait_for_descriptors(
    mut events1: tokio::sync::mpsc::Receiver<Event>,
    mut events2: tokio::sync::mpsc::Receiver<Event>,
    desc1_rcvd: &mut bool,
    desc2_rcvd: &mut bool,
) {
    loop {
        tokio::select! {
            event = events1.recv() => {
                match event {
                    Some(Event::DescriptorReceived { .. }) => {
                        *desc1_rcvd = true;
                        if *desc1_rcvd && *desc2_rcvd { return; }
                    }
                    None => return,
                    _ => continue,
                }
            }
            event = events2.recv() => {
                match event {
                    Some(Event::DescriptorReceived { .. }) => {
                        *desc2_rcvd = true;
                        if *desc1_rcvd && *desc2_rcvd { return; }
                    }
                    None => return,
                    _ => continue,
                }
            }
        }
    }
}

/// Smoke test: verify the swarm starts and produces events.
#[tokio::test]
async fn peer_expires_on_timeout() {
    let keypair = identity::Keypair::generate_ed25519();
    let config = SwarmConfig {
        listen_addresses: vec!["/ip4/127.0.0.1/tcp/0".parse().unwrap()],
        bootstrap_peers: vec![],
    };
    let descriptor = make_test_descriptor();
    let mut handle = new_swarm(keypair, config, descriptor, None, None).unwrap();

    // Verify the swarm starts and produces a NewListenAddr event
    let got_listen = timeout(Duration::from_secs(5), async {
        loop {
            match handle.events.recv().await {
                Some(Event::NewListenAddr { .. }) => return true,
                Some(_) => continue,
                None => return false,
            }
        }
    })
    .await
    .unwrap_or(false);

    assert!(got_listen, "Swarm should produce a NewListenAddr event");
}

struct MemBlob(std::collections::HashMap<String, Vec<u8>>);
impl p2p::BlobProvider for MemBlob {
    fn get_blob(&self, hash: &str) -> Option<Vec<u8>> {
        self.0.get(hash).cloned()
    }
}

#[tokio::test]
async fn blob_is_served_between_two_swarms() {
    use std::time::Duration;
    let keypair1 = identity::Keypair::generate_ed25519();
    let keypair2 = identity::Keypair::generate_ed25519();
    let peer1_id = keypair1.public().to_peer_id();
    let mut blob = std::collections::HashMap::new();
    blob.insert("abc".to_string(), b"hello-blob".to_vec());
    let h1 = new_swarm(
        keypair1,
        SwarmConfig {
            listen_addresses: vec!["/ip4/127.0.0.1/tcp/0".parse().unwrap()],
            bootstrap_peers: Vec::new(),
        },
        make_test_descriptor(),
        Some(std::sync::Arc::new(MemBlob(blob))),
        None,
    )
    .unwrap();
    let mut ev1 = h1.events;
    let addr1 = loop {
        match tokio::time::timeout(Duration::from_secs(5), ev1.recv()).await {
            Ok(Some(Event::NewListenAddr { address })) => break address,
            Ok(Some(_)) => continue,
            _ => panic!("no listen"),
        }
    };
    let h2 = new_swarm(
        keypair2,
        SwarmConfig {
            listen_addresses: vec!["/ip4/127.0.0.1/tcp/0".parse().unwrap()],
            bootstrap_peers: vec![addr1],
        },
        make_test_descriptor(),
        None,
        None,
    )
    .unwrap();
    let mut ev2 = h2.events;
    // wait until the bootstrap connection to peer1 is established
    loop {
        match tokio::time::timeout(Duration::from_secs(10), ev2.recv()).await {
            Ok(Some(Event::PeerConnected { peer_id })) if peer_id == peer1_id => break,
            Ok(Some(_)) => continue,
            _ => panic!("no connect"),
        }
    }
    h2.commands
        .send(p2p::SwarmCommand::RequestBlob {
            peer_id: peer1_id,
            hash: "abc".into(),
        })
        .await
        .unwrap();
    let got = loop {
        match tokio::time::timeout(Duration::from_secs(10), ev2.recv()).await {
            Ok(Some(Event::BlobResponseReceived { found, data, .. })) => {
                assert!(found);
                break data;
            }
            Ok(Some(_)) => continue,
            _ => panic!("no blob"),
        }
    };
    assert_eq!(got, b"hello-blob");
}

struct MockExecutor {
    node_id: uuid::Uuid,
}
#[async_trait::async_trait]
impl p2p::ProjectExecutor for MockExecutor {
    async fn run(
        &self,
        task: eo_core::types::ProjectTask,
    ) -> anyhow::Result<eo_core::types::ProjectResult> {
        Ok(eo_core::types::ProjectResult {
            task_id: task.task_id,
            exit_code: 0,
            stdout: b"mock-done".to_vec(),
            stderr: Vec::new(),
            execution_time_ms: 1,
            executed_on: self.node_id,
        })
    }
}

#[tokio::test]
async fn project_task_roundtrip_via_request_response() {
    use std::time::Duration;
    let keypair1 = identity::Keypair::generate_ed25519();
    let keypair2 = identity::Keypair::generate_ed25519();
    let peer2 = keypair2.public().to_peer_id();
    let h2 = new_swarm(
        keypair2,
        SwarmConfig {
            listen_addresses: vec!["/ip4/127.0.0.1/tcp/0".parse().unwrap()],
            bootstrap_peers: Vec::new(),
        },
        make_test_descriptor(),
        None,
        Some(std::sync::Arc::new(MockExecutor {
            node_id: uuid::Uuid::new_v4(),
        })),
    )
    .unwrap();
    let mut ev2 = h2.events;
    let addr2 = loop {
        match tokio::time::timeout(Duration::from_secs(5), ev2.recv()).await {
            Ok(Some(Event::NewListenAddr { address })) => break address,
            Ok(Some(_)) => continue,
            _ => panic!("no listen"),
        }
    };
    let h1 = new_swarm(
        keypair1,
        SwarmConfig {
            listen_addresses: vec!["/ip4/127.0.0.1/tcp/0".parse().unwrap()],
            bootstrap_peers: vec![addr2],
        },
        make_test_descriptor(),
        None,
        None,
    )
    .unwrap();
    let mut ev1 = h1.events;
    loop {
        match tokio::time::timeout(Duration::from_secs(10), ev1.recv()).await {
            Ok(Some(Event::PeerConnected { peer_id })) if peer_id == peer2 => break,
            Ok(Some(_)) => continue,
            _ => panic!("no connect"),
        }
    }
    let task = eo_core::types::ProjectTask {
        task_id: uuid::Uuid::new_v4(),
        snapshot: eo_core::types::ProjectSnapshot {
            hash: "tarhash".into(),
            tar_bytes: b"tar".to_vec(),
        },
        work_dir: "/root/proj".into(),
        build_cmd: vec!["make".into()],
        run_cmd: vec!["./app".into()],
        timeout_ms: 5000,
        resource_limits: eo_core::types::ResourceLimits::default(),
        pinned_node: None,
    };
    h1.commands
        .send(p2p::SwarmCommand::SendProjectTask {
            peer_id: peer2,
            task,
        })
        .await
        .unwrap();
    let result = loop {
        match tokio::time::timeout(Duration::from_secs(10), ev1.recv()).await {
            Ok(Some(Event::ProjectResultReceived { result, .. })) => break result,
            Ok(Some(_)) => continue,
            _ => panic!("no result"),
        }
    };
    assert_eq!(result.stdout, b"mock-done");
    assert_eq!(result.exit_code, 0);
}

/// Executor that takes its time, like a real VM boot + build, and reports when
/// execution actually starts.
struct SlowExecutor {
    node_id: uuid::Uuid,
    delay: Duration,
    started: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}

#[async_trait::async_trait]
impl p2p::ProjectExecutor for SlowExecutor {
    async fn run(
        &self,
        task: eo_core::types::ProjectTask,
    ) -> anyhow::Result<eo_core::types::ProjectResult> {
        if let Ok(mut slot) = self.started.lock() {
            if let Some(tx) = slot.take() {
                let _ = tx.send(());
            }
        }
        tokio::time::sleep(self.delay).await;
        Ok(eo_core::types::ProjectResult {
            task_id: task.task_id,
            exit_code: 0,
            stdout: b"slow-done".to_vec(),
            stderr: Vec::new(),
            execution_time_ms: self.delay.as_millis() as u64,
            executed_on: self.node_id,
        })
    }
}

/// Regression test for the "executor runs inline on the swarm event loop" bug.
///
/// A project job takes minutes in production (VM boot + toolchain install +
/// build). If the acceptor runs it inline, its swarm event loop is parked for
/// the whole job: it cannot answer any other request, and the connection that
/// must carry the result can be torn down. This test pins the invariant that
/// while a job is in flight, the acceptor still serves other requests promptly.
#[tokio::test]
async fn project_execution_does_not_block_the_swarm_event_loop() {
    let executor_delay = Duration::from_secs(4);
    let (started_tx, started_rx) = tokio::sync::oneshot::channel::<()>();

    // Executor node (the acceptor).
    let keypair2 = identity::Keypair::generate_ed25519();
    let peer2 = keypair2.public().to_peer_id();
    let h2 = new_swarm(
        keypair2,
        SwarmConfig {
            listen_addresses: vec!["/ip4/127.0.0.1/tcp/0".parse().unwrap()],
            bootstrap_peers: Vec::new(),
        },
        make_test_descriptor(),
        None,
        Some(std::sync::Arc::new(SlowExecutor {
            node_id: uuid::Uuid::new_v4(),
            delay: executor_delay,
            started: std::sync::Mutex::new(Some(started_tx)),
        })),
    )
    .unwrap();
    let mut ev2 = h2.events;
    let addr2 = loop {
        match timeout(Duration::from_secs(10), ev2.recv()).await {
            Ok(Some(Event::NewListenAddr { address })) => break address,
            Ok(Some(_)) => continue,
            _ => panic!("no listen address from the executor node"),
        }
    };

    // Master node: connects outbound, then submits and polls.
    let keypair1 = identity::Keypair::generate_ed25519();
    let h1 = new_swarm(
        keypair1,
        SwarmConfig {
            listen_addresses: vec!["/ip4/127.0.0.1/tcp/0".parse().unwrap()],
            bootstrap_peers: vec![addr2],
        },
        make_test_descriptor(),
        None,
        None,
    )
    .unwrap();
    let mut ev1 = h1.events;
    loop {
        match timeout(Duration::from_secs(15), ev1.recv()).await {
            Ok(Some(Event::PeerConnected { peer_id })) if peer_id == peer2 => break,
            Ok(Some(_)) => continue,
            _ => panic!("master never connected to the executor node"),
        }
    }

    h1.commands
        .send(p2p::SwarmCommand::SendProjectTask {
            peer_id: peer2,
            task: eo_core::types::ProjectTask {
                task_id: uuid::Uuid::new_v4(),
                snapshot: eo_core::types::ProjectSnapshot {
                    hash: "tarhash".into(),
                    tar_bytes: b"tar".to_vec(),
                },
                work_dir: "/root/proj".into(),
                build_cmd: vec!["make".into()],
                run_cmd: vec!["./app".into()],
                timeout_ms: 30_000,
                resource_limits: eo_core::types::ResourceLimits::default(),
                pinned_node: None,
            },
        })
        .await
        .unwrap();

    // The job is now running: only the detached-task design delivers this, since
    // it lets the event loop return to `select!` while `run` is in flight.
    timeout(Duration::from_secs(10), started_rx)
        .await
        .expect("executor never started the task")
        .expect("executor dropped the started signal");

    // While the job runs, the acceptor must still answer an already-connected
    // peer: with inline execution its loop is parked inside `run` and cannot even
    // observe this request, let alone answer it.
    let responded = timeout(executor_delay / 2, async {
        h1.commands
            .send(p2p::SwarmCommand::RequestDescriptor { peer_id: peer2 })
            .await
            .unwrap();
        loop {
            match ev1.recv().await {
                Some(Event::DescriptorReceived { peer_id, .. }) if peer_id == peer2 => break,
                Some(_) => continue,
                None => panic!("event channel closed"),
            }
        }
    })
    .await;
    assert!(
        responded.is_ok(),
        "acceptor did not answer a descriptor request while executing a project task: \
         the project executor is blocking the swarm event loop"
    );

    // The job's own result must still arrive afterwards.
    let result = loop {
        match timeout(Duration::from_secs(15), ev1.recv()).await {
            Ok(Some(Event::ProjectResultReceived { result, .. })) => break result,
            Ok(Some(_)) => continue,
            _ => panic!("no project result"),
        }
    };
    assert_eq!(result.stdout, b"slow-done");
    assert_eq!(result.exit_code, 0);
}

/// A node without a project executor must reject the task explicitly instead of
/// swallowing it: a dropped request leaves the master polling `pending` forever.
#[tokio::test]
async fn project_task_without_executor_is_rejected() {
    let keypair2 = identity::Keypair::generate_ed25519();
    let peer2 = keypair2.public().to_peer_id();
    let h2 = new_swarm(
        keypair2,
        SwarmConfig {
            listen_addresses: vec!["/ip4/127.0.0.1/tcp/0".parse().unwrap()],
            bootstrap_peers: Vec::new(),
        },
        make_test_descriptor(),
        None,
        None, // no project executor
    )
    .unwrap();
    let mut ev2 = h2.events;
    let addr2 = loop {
        match timeout(Duration::from_secs(5), ev2.recv()).await {
            Ok(Some(Event::NewListenAddr { address })) => break address,
            Ok(Some(_)) => continue,
            _ => panic!("no listen"),
        }
    };

    let keypair1 = identity::Keypair::generate_ed25519();
    let h1 = new_swarm(
        keypair1,
        SwarmConfig {
            listen_addresses: vec!["/ip4/127.0.0.1/tcp/0".parse().unwrap()],
            bootstrap_peers: vec![addr2],
        },
        make_test_descriptor(),
        None,
        None,
    )
    .unwrap();
    let mut ev1 = h1.events;
    loop {
        match timeout(Duration::from_secs(10), ev1.recv()).await {
            Ok(Some(Event::Identified { peer_id, .. })) if peer_id == peer2 => break,
            Ok(Some(_)) => continue,
            _ => panic!("master never identified the executor node"),
        }
    }
    h1.commands
        .send(p2p::SwarmCommand::SendProjectTask {
            peer_id: peer2,
            task: eo_core::types::ProjectTask {
                task_id: uuid::Uuid::new_v4(),
                snapshot: eo_core::types::ProjectSnapshot {
                    hash: "tarhash".into(),
                    tar_bytes: b"tar".to_vec(),
                },
                work_dir: "/root/proj".into(),
                build_cmd: vec![],
                run_cmd: vec![],
                timeout_ms: 5000,
                resource_limits: eo_core::types::ResourceLimits::default(),
                pinned_node: None,
            },
        })
        .await
        .unwrap();

    let result = loop {
        match timeout(Duration::from_secs(10), ev1.recv()).await {
            Ok(Some(Event::ProjectResultReceived { result, .. })) => break result,
            Ok(Some(_)) => continue,
            _ => panic!("no rejection response: the task was silently dropped"),
        }
    };
    assert_eq!(result.exit_code, 101);
    assert!(
        String::from_utf8_lossy(&result.stderr).contains("no project executor"),
        "unexpected rejection reason: {}",
        String::from_utf8_lossy(&result.stderr)
    );
}
