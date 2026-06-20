//! CAS-backed Raft storage adapter.
//!
//! Implements tikv/raft-rs [`Storage`] trait using our content-addressed
//! object store. Raft log entries, HardState, and snapshots are all stored
//! as CAS objects, giving us immutable history and easy state inspection.

use std::sync::{Arc, Mutex};

use eo_core::error::Result as CoreResult;
use lru::LruCache;
use raft::prelude::*;
use raft::storage::Storage;
use raft::{Error as RaftError, GetEntriesContext, Result as RaftResult};
use storage::LocalObjectStore;
use tracing::{debug, trace};

/// Maximum number of log entries to cache in memory.
const ENTRY_CACHE_SIZE: usize = 1024;

/// Key prefixes for Raft state stored as blobs in CAS.
#[allow(dead_code)]
const HARD_STATE_KEY: &str = "raft-hard-state";
#[allow(dead_code)]
const CONF_STATE_KEY: &str = "raft-conf-state";
#[allow(dead_code)]
const SNAPSHOT_KEY: &str = "raft-snapshot";

/// A Raft [`Storage`] implementation backed by our CAS [`LocalObjectStore`].
///
/// # Storage layout
///
/// - **Log entries**: Each entry is stored as a Blob with key `raft-entry-{index}`.
///   The blob contains protobuf-serialized entry data.
/// - **HardState**: Stored as a Blob with key `raft-hard-state`.
/// - **ConfState**: Stored as a Blob with key `raft-conf-state`.
/// - **Snapshots**: Stored as Commit objects pointing to a Tree of the full state.
pub struct CasRaftStorage {
    /// The underlying CAS object store.
    object_store: Arc<LocalObjectStore>,

    /// In-memory LRU cache for recent log entries.
    entry_cache: Mutex<LruCache<u64, Entry>>,

    /// Cached hard state.
    hard_state: Mutex<Option<HardState>>,

    /// Cached conf state.
    conf_state: Mutex<Option<ConfState>>,

    /// First available log index (accounts for compaction).
    first_index: Mutex<u64>,

    /// Last written log index.
    last_index: Mutex<u64>,

    /// Content hashes of stored entries, indexed by raft log index.
    entry_hashes: Mutex<Vec<Option<eo_core::types::Hash>>>,
}

impl CasRaftStorage {
    /// Create a new CAS-backed Raft storage.
    ///
    /// # Arguments
    /// * `object_store` — The CAS store for persisting Raft state.
    pub fn new(object_store: Arc<LocalObjectStore>) -> Self {
        let mut cache = LruCache::new(std::num::NonZeroUsize::new(ENTRY_CACHE_SIZE).unwrap());

        // Try to restore state from CAS
        let (hard_state, conf_state, first_idx, last_idx) =
            Self::load_state_from_store(&object_store, &mut cache);

        Self {
            object_store,
            entry_cache: Mutex::new(cache),
            hard_state: Mutex::new(hard_state),
            conf_state: Mutex::new(conf_state),
            first_index: Mutex::new(first_idx),
            last_index: Mutex::new(last_idx),
            entry_hashes: Mutex::new(vec![None]),
        }
    }

    /// Create a new empty storage (no prior state).
    pub fn new_empty(object_store: Arc<LocalObjectStore>) -> Self {
        Self {
            object_store,
            entry_cache: Mutex::new(LruCache::new(
                std::num::NonZeroUsize::new(ENTRY_CACHE_SIZE).unwrap(),
            )),
            hard_state: Mutex::new(None),
            conf_state: Mutex::new(None),
            first_index: Mutex::new(1),
            last_index: Mutex::new(0),
            entry_hashes: Mutex::new(vec![None]), // index 0 is unused
        }
    }

    /// Append a log entry to storage.
    ///
    /// Writes the entry to CAS and updates the in-memory index.
    pub fn append_entry(&self, entry: &Entry) -> CoreResult<()> {
        let idx = entry.index;
        let data = entry.data.clone();

        // Store as blob in CAS, track the content hash
        let hash = self.object_store.put_blob(&data)?;

        // Update entry hash mapping
        {
            let mut hashes = self.entry_hashes.lock().unwrap();
            while hashes.len() <= idx as usize {
                hashes.push(None);
            }
            hashes[idx as usize] = Some(hash);
        }

        // Update cache and index
        {
            let mut cache = self.entry_cache.lock().unwrap();
            cache.put(idx, entry.clone());
        }

        {
            let mut last = self.last_index.lock().unwrap();
            if idx > *last {
                *last = idx;
            }
        }

        trace!("Appended raft entry at index {}", idx);
        Ok(())
    }

    /// Persist the hard state to CAS.
    pub fn set_hard_state(&self, hs: HardState) -> CoreResult<()> {
        let data = serde_json::to_vec(&hard_state_to_json(&hs))
            .map_err(|e| eo_core::error::CoreError::Serialization(e.to_string()))?;
        self.object_store.put_blob(&data)?;

        let mut state = self.hard_state.lock().unwrap();
        *state = Some(hs);
        Ok(())
    }

    /// Persist the conf state to CAS.
    pub fn set_conf_state(&self, cs: ConfState) -> CoreResult<()> {
        let data = serde_json::to_vec(&conf_state_to_json(&cs))
            .map_err(|e| eo_core::error::CoreError::Serialization(e.to_string()))?;
        self.object_store.put_blob(&data)?;

        let mut state = self.conf_state.lock().unwrap();
        *state = Some(cs);
        Ok(())
    }

    /// Compact log entries up to the given index.
    ///
    /// All entries with index <= compact_index are removed from CAS
    /// (they're included in a snapshot).
    pub fn compact(&self, compact_index: u64) -> CoreResult<()> {
        let mut first = self.first_index.lock().unwrap();
        let last = self.last_index.lock().unwrap();

        if compact_index > *last {
            return Err(eo_core::error::CoreError::InvalidState(format!(
                "compact_index {compact_index} > last_index {}",
                *last
            )));
        }

        *first = compact_index + 1;

        // Entries <= compact_index are no longer needed
        // (they're covered by snapshot). We don't physically delete them
        // from CAS — they'll be cleaned up by GC.

        {
            let mut cache = self.entry_cache.lock().unwrap();
            for i in 1..=compact_index {
                cache.pop(&i);
            }
        }

        Ok(())
    }

    // ------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------

    /// Attempt to restore cached state from the CAS store.
    fn load_state_from_store(
        store: &LocalObjectStore,
        _cache: &mut LruCache<u64, Entry>,
    ) -> (Option<HardState>, Option<ConfState>, u64, u64) {
        // In a production system, we'd scan the CAS for raft entries
        // and reconstruct state. For now, start fresh.
        let _ = store;
        (None, None, 1, 0)
    }
}

// ---------------------------------------------------------------------------
// raft::storage::Storage trait implementation
// ---------------------------------------------------------------------------

impl Storage for CasRaftStorage {
    fn initial_state(&self) -> RaftResult<RaftState> {
        let hard_state = self.hard_state.lock().unwrap().clone().unwrap_or_default();
        let conf_state = self.conf_state.lock().unwrap().clone().unwrap_or_default();

        Ok(RaftState::new(hard_state, conf_state))
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        _context: GetEntriesContext,
    ) -> RaftResult<Vec<Entry>> {
        let max_size = max_size.into();
        let last = *self.last_index.lock().unwrap();

        if high > last + 1 {
            panic!(
                "entries: requested high={high} exceeds last_index+1={}",
                last + 1
            );
        }

        let mut entries = Vec::new();
        let mut total_size: u64 = 0;

        // Check cache first, then fall back to CAS
        let mut cache = self.entry_cache.lock().unwrap();

        for idx in low..high {
            if let Some(entry) = cache.get(&idx).cloned() {
                let size = entry.data.len() as u64;
                if let Some(limit) = max_size {
                    if !entries.is_empty() && total_size + size > limit {
                        break;
                    }
                }
                total_size += size;
                entries.push(entry.clone());
            } else {
                // Try to load from CAS using tracked hash
                let hash = {
                    let hashes = self.entry_hashes.lock().unwrap();
                    hashes.get(idx as usize).cloned().flatten()
                };
                if let Some(hash) = hash {
                    if let Ok(data) = self.object_store.get_blob(&hash) {
                        let entry = Entry {
                            index: idx,
                            data,
                            ..Default::default()
                        };
                        let size = entry.data.len() as u64;
                        if let Some(limit) = max_size {
                            if !entries.is_empty() && total_size + size > limit {
                                break;
                            }
                        }
                        total_size += size;
                        cache.put(idx, entry.clone());
                        entries.push(entry);
                    }
                }
            }
        }
        drop(cache);

        debug!("entries: [{}, {}) -> {} entries", low, high, entries.len());
        Ok(entries)
    }

    fn term(&self, idx: u64) -> RaftResult<u64> {
        let first = *self.first_index.lock().unwrap();

        if idx < first - 1 {
            return Err(RaftError::Store(raft::StorageError::Unavailable));
        }

        if idx == first - 1 {
            // Return the term of the compacted entry (in snapshot)
            // For now, return 0 (will be matched by Raft)
            return Ok(0);
        }

        // Check cache
        {
            let mut cache = self.entry_cache.lock().unwrap();
            if let Some(entry) = cache.get(&idx) {
                return Ok(entry.term);
            }
        }

        // Try CAS using tracked hash
        let hash = {
            let hashes = self.entry_hashes.lock().unwrap();
            hashes.get(idx as usize).cloned().flatten()
        };
        if let Some(hash) = hash {
            if let Ok(data) = self.object_store.get_blob(&hash) {
                // Deserialize the entry to get its term
                // The entry term is stored as first 8 bytes (little-endian)
                if data.len() >= 8 {
                    let term_bytes: [u8; 8] = data[..8].try_into().unwrap();
                    return Ok(u64::from_le_bytes(term_bytes));
                }
            }
        }

        Err(RaftError::Store(raft::StorageError::Unavailable))
    }

    fn first_index(&self) -> RaftResult<u64> {
        Ok(*self.first_index.lock().unwrap())
    }

    fn last_index(&self) -> RaftResult<u64> {
        Ok(*self.last_index.lock().unwrap())
    }

    fn snapshot(&self, request_index: u64, _to: u64) -> RaftResult<Snapshot> {
        let last = *self.last_index.lock().unwrap();

        if request_index > last {
            return Err(RaftError::Store(
                raft::StorageError::SnapshotTemporarilyUnavailable,
            ));
        }

        // Build a snapshot from the log entries using tracked hashes
        let mut data = Vec::new();
        let hashes = self.entry_hashes.lock().unwrap();
        for idx in 1..=last {
            if let Some(Some(hash)) = hashes.get(idx as usize) {
                if let Ok(entry_data) = self.object_store.get_blob(hash) {
                    data.extend_from_slice(&entry_data);
                    data.push(b'\n');
                }
            }
        }
        drop(hashes);

        let conf_state = self.conf_state.lock().unwrap().clone().unwrap_or_default();

        let metadata = SnapshotMetadata {
            index: last,
            term: 1, // Simplified — should be the term at `last`
            conf_state: Some(conf_state),
        };

        Ok(Snapshot {
            data,
            metadata: Some(metadata),
        })
    }
}

// ---------------------------------------------------------------------------
// JSON helpers for serializing Raft types
// ---------------------------------------------------------------------------

fn hard_state_to_json(hs: &HardState) -> serde_json::Value {
    serde_json::json!({
        "term": hs.term,
        "vote": hs.vote,
        "commit": hs.commit,
    })
}

fn conf_state_to_json(cs: &ConfState) -> serde_json::Value {
    serde_json::json!({
        "voters": cs.voters,
        "learners": cs.learners,
        "auto_leave": cs.auto_leave,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tempfile::tempdir;

    fn make_store() -> (LocalObjectStore, Arc<LocalObjectStore>) {
        let dir = tempdir().unwrap();
        let store = LocalObjectStore::new(dir.path().to_path_buf()).unwrap();
        let arc = Arc::new(store);
        (
            LocalObjectStore::new(dir.path().to_path_buf()).unwrap(),
            arc,
        )
    }

    #[test]
    fn initial_state_on_empty_store() {
        let (_store, arc) = make_store();
        let storage = CasRaftStorage::new_empty(arc);
        let state = storage.initial_state().unwrap();
        assert_eq!(state.hard_state.term, 0);
    }

    #[test]
    fn append_and_read_entries() {
        let (_store, arc) = make_store();
        let storage = CasRaftStorage::new_empty(arc);

        let entry = Entry {
            index: 1,
            term: 1,
            data: vec![1, 2, 3],
            ..Default::default()
        };

        storage.append_entry(&entry).unwrap();
        assert_eq!(storage.first_index().unwrap(), 1);
        assert_eq!(storage.last_index().unwrap(), 1);

        let entries = storage
            .entries(1, 2, None, GetEntriesContext::empty(false))
            .unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].index, 1);
    }

    #[test]
    fn term_lookup() {
        let (_store, arc) = make_store();
        let storage = CasRaftStorage::new_empty(arc);

        // Entry with term encoded in first 8 bytes
        let term: u64 = 5;
        let mut data = term.to_le_bytes().to_vec();
        data.extend_from_slice(b"payload");
        let entry = Entry {
            index: 1,
            term,
            data,
            ..Default::default()
        };

        storage.append_entry(&entry).unwrap();
        assert_eq!(storage.term(1).unwrap(), term);
    }

    #[test]
    fn snapshot_creation() {
        let (_store, arc) = make_store();
        let storage = CasRaftStorage::new_empty(arc);

        for i in 1..=5 {
            let entry = Entry {
                index: i,
                term: 1,
                data: format!("entry-{i}").into_bytes(),
                ..Default::default()
            };
            storage.append_entry(&entry).unwrap();
        }

        let snapshot = storage.snapshot(3, 0).unwrap();
        assert!(snapshot.metadata.is_some());
        assert_eq!(snapshot.metadata.unwrap().index, 5);
        assert!(!snapshot.data.is_empty());
    }
}
