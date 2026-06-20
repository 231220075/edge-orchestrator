//! Raft node lifecycle — manages a single Raft participant.

use std::sync::Arc;

use eo_core::error::Result;
use raft::prelude::*;
use raft::{RawNode, StateRole};
use storage::LocalObjectStore;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::network::Libp2pRaftTransport;
use crate::proposal::Proposal;
use crate::state_machine::ClusterState;
use crate::storage::CasRaftStorage;

/// A Raft participant node.
pub struct RaftNode {
    /// The underlying tikv/raft-rs raw node.
    raw_node: RawNode<CasRaftStorage>,

    /// Network transport for sending/receiving Raft messages.
    transport: Libp2pRaftTransport,

    /// The replicated state machine.
    state_machine: ClusterState,

    /// Channel for receiving proposals.
    proposal_rx: mpsc::Receiver<Proposal>,

    /// Channel handle for submitting proposals (clone to share).
    proposal_tx: mpsc::Sender<Proposal>,

    /// This node's Raft ID.
    id: u64,
}

impl RaftNode {
    /// Create a new Raft node.
    pub async fn new(id: u64, _peers: Vec<u64>, transport: Libp2pRaftTransport) -> Result<Self> {
        let mut config = Config {
            id,
            ..Default::default()
        };
        config.election_tick = 10;
        config.heartbeat_tick = 3;

        let storage = CasRaftStorage::new_empty(Arc::new(
            LocalObjectStore::new(std::env::temp_dir().join("edge-orch-raft"))
                .map_err(|e| eo_core::error::CoreError::Internal(format!("store: {e}")))?,
        ));

        let discard_logger = slog::Logger::root(slog::Discard, slog::o!());
        let raw_node = RawNode::new(&config, storage, &discard_logger).map_err(|e| {
            eo_core::error::CoreError::Raft(format!("failed to create RawNode: {e}"))
        })?;

        let (proposal_tx, proposal_rx) = mpsc::channel(256);

        Ok(Self {
            raw_node,
            transport,
            state_machine: ClusterState::default(),
            proposal_rx,
            proposal_tx,
            id,
        })
    }

    /// Get a sender for submitting proposals.
    pub fn proposal_sender(&self) -> mpsc::Sender<Proposal> {
        self.proposal_tx.clone()
    }

    /// Get a reference to the state machine.
    pub fn state(&self) -> &ClusterState {
        &self.state_machine
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

        // Send messages to peers
        for msg in ready.messages() {
            let to = msg.to;
            if let Err(e) = self.transport.send(to, msg) {
                warn!("Failed to send Raft message to {}: {}", to, e);
            }
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
                    self.state_machine.apply(proposal);
                    self.state_machine.last_applied_index = entry.index;
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
        debug!(
            "Raft node {}: raft_state={:?}, term={}",
            self.id, status.ss.raft_state, status.hs.term,
        );

        if status.ss.raft_state == StateRole::Leader {
            info!("Raft node {} is now LEADER", self.id);
        }

        Ok(())
    }
}
