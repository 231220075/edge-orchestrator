//! Raft node lifecycle — manages a single Raft participant.

use std::sync::Arc;

use eo_core::error::Result;
use raft::eraftpb::Message as RaftMessage;
use raft::prelude::*;
use raft::{RawNode, StateRole};
use storage::LocalObjectStore;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::raft::network::Libp2pRaftTransport;
use crate::raft::proposal::Proposal;
use crate::raft::state_machine::ClusterState;
use crate::raft::storage::CasRaftStorage;

/// A Raft participant node.
pub struct RaftNode {
    /// The underlying tikv/raft-rs raw node.
    raw_node: RawNode<CasRaftStorage>,

    /// Network transport for sending/receiving Raft messages.
    transport: Libp2pRaftTransport,

    /// The replicated state machine, shared with scheduler/executor loops.
    state: Arc<std::sync::Mutex<ClusterState>>,

    /// Channel for receiving proposals.
    proposal_rx: mpsc::Receiver<Proposal>,

    /// Channel handle for submitting proposals (clone to share).
    proposal_tx: mpsc::Sender<Proposal>,

    /// This node's Raft ID.
    id: u64,
}

impl RaftNode {
    /// Create a new Raft node.
    ///
    /// voters is the static cluster membership (raft ids). Every node must be
    /// started with the SAME voters list so they agree on the initial config.
    pub async fn new(
        id: u64,
        voters: Vec<u64>,
        object_store: Arc<LocalObjectStore>,
        transport: Libp2pRaftTransport,
    ) -> Result<Self> {
        let mut config = Config {
            id,
            ..Default::default()
        };
        config.election_tick = 10;
        config.heartbeat_tick = 3;
        // Pre-vote avoids term inflation on network partitions, which matches
        // the kill-leader demo where a node is forcibly removed.
        config.pre_vote = false;
        config.check_quorum = true;

        let storage = CasRaftStorage::new_empty(object_store, voters);

        let discard_logger = slog::Logger::root(slog::Discard, slog::o!());
        let raw_node = RawNode::new(&config, storage, &discard_logger).map_err(|e| {
            eo_core::error::CoreError::Raft(format!("failed to create RawNode: {e}"))
        })?;

        let (proposal_tx, proposal_rx) = mpsc::channel(256);

        Ok(Self {
            raw_node,
            transport,
            state: Arc::new(std::sync::Mutex::new(ClusterState::default())),
            proposal_rx,
            proposal_tx,
            id,
        })
    }

    /// Get a sender for submitting proposals.
    pub fn proposal_sender(&self) -> mpsc::Sender<Proposal> {
        self.proposal_tx.clone()
    }

    /// Get a cloneable handle to the replicated state machine.
    pub fn state_handle(&self) -> Arc<std::sync::Mutex<ClusterState>> {
        Arc::clone(&self.state)
    }

    /// Run the main Raft event loop.
    pub async fn run(&mut self) -> Result<()> {
        let mut tick = tokio::time::interval(std::time::Duration::from_millis(100));
        info!("Raft node {} starting event loop", self.id);

        loop {
            tokio::select! {
                _ = tick.tick() => {
                    self.raw_node.tick();
                }

                msg = self.transport.recv() => {
                    match msg {
                        Some(envelope) => {
                            if let Ok(raft_msg) = envelope.decode() {
                                if let Err(e) = self.raw_node.step(raft_msg) {
                                    warn!("Raft step error: {}", e);
                                }
                            }
                        }
                        None => {
                            debug!("Raft transport closed, exiting event loop");
                            break;
                        }
                    }
                }

                proposal = self.proposal_rx.recv() => {
                    match proposal {
                        Some(proposal) => {
                            if let Err(e) = self.propose(proposal).await {
                                warn!("Failed to propose: {}", e);
                            }
                        }
                        None => {
                            debug!("Proposal channel closed, exiting event loop");
                            break;
                        }
                    }
                }
            }

            self.process_ready().await?;
        }

        info!("Raft node {} event loop exited", self.id);
        Ok(())
    }

    fn send_message(&self, msg: &RaftMessage) {
        let to = msg.to;
        if let Err(e) = self.transport.send(to, msg) {
            warn!("Failed to send Raft message to {}: {}", to, e);
        }
    }

    /// Submit a proposal to the Raft cluster.
    async fn propose(&mut self, proposal: Proposal) -> Result<()> {
        let data = proposal.encode().map_err(|e| {
            eo_core::error::CoreError::Serialization(format!("encode proposal: {e}"))
        })?;

        self.raw_node
            .propose(vec![], data)
            .map_err(|e| eo_core::error::CoreError::Raft(format!("propose failed: {e}")))?;

        debug!("Proposed to Raft cluster");
        Ok(())
    }

    /// Process the Raft Ready state.
    async fn process_ready(&mut self) -> Result<()> {
        if !self.raw_node.has_ready() {
            return Ok(());
        }

        let mut ready = self.raw_node.ready();

        // raft-rs Ready protocol:
        //  1. messages() (only leader sends them here)
        //  2. persist hard state + entries + snapshot
        //  3. persisted_messages() (vote requests / pre-votes for non-leaders)
        //  4. apply committed entries
        //  5. advance
        let store = &self.raw_node.raft.raft_log.store;
        for msg in ready.messages() {
            self.send_message(msg);
        }
        if let Some(hs) = ready.hs() {
            if let Err(e) = store.set_hard_state(hs.clone()) {
                warn!("Failed to persist hard state: {}", e);
            }
        }
        for entry in ready.entries() {
            if let Err(e) = store.append_entry(entry) {
                warn!("Failed to persist entry {}: {}", entry.index, e);
            }
        }
        for msg in ready.persisted_messages() {
            self.send_message(msg);
        }

        // Apply committed entries
        let committed = ready.take_committed_entries();
        for entry in committed {
            if entry.data.is_empty() {
                continue;
            }

            match Proposal::decode(&entry.data) {
                Ok(proposal) => {
                    debug!("Applying committed proposal at index {}", entry.index);
                    {
                        let mut guard = self.state.lock().expect("state poisoned");
                        guard.apply(proposal);
                        guard.last_applied_index = entry.index;
                    }
                }
                Err(e) => {
                    warn!(
                        "Failed to decode committed entry at index {}: {}",
                        entry.index, e
                    );
                }
            }
        }

        // Advance and get light ready
        let _light_ready = self.raw_node.advance(ready);

        // Log status
        let status = self.raw_node.status();
        // info level so the integration test can grep the stable RAFT_STATUS
        // marker to observe state transitions without restarting the process.
        info!(
            "RAFT_STATUS raft_id={} state={:?} term={} leader={:?}",
            self.id, status.ss.raft_state, status.hs.term, status.ss.leader_id
        );
        if status.ss.raft_state == StateRole::Leader {
            info!("RAFT_LEADER raft_id={}", self.id);
        }

        Ok(())
    }
}
