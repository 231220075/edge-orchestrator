//! Packfile index for efficient batch operations.
//!
//! Provides a `PackWriter` for creating append-only packfiles and
//! a `PackReader` for random access into packfiles by object hash.

use std::collections::HashMap;
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use eo_core::error::Result;
use eo_core::types::Hash;

/// Entry in a packfile index: maps hash → (offset, length) in the packfile.
#[derive(Debug, Clone)]
pub struct IndexEntry {
    /// Byte offset of the object in the packfile.
    pub offset: u64,
    /// Length of the object data in bytes.
    pub length: u64,
    /// The object hash (redundant with map key, but useful for verification).
    pub hash: Hash,
}

/// An index mapping object hashes to their locations in a packfile.
#[derive(Debug, Clone, Default)]
pub struct PackIndex {
    /// Hash → file location mapping.
    entries: HashMap<Hash, IndexEntry>,
}

impl PackIndex {
    /// Create a new empty index.
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    /// Add an entry to the index.
    pub fn insert(&mut self, entry: IndexEntry) {
        self.entries.insert(entry.hash.clone(), entry);
    }

    /// Look up an object by hash.
    pub fn get(&self, hash: &Hash) -> Option<&IndexEntry> {
        self.entries.get(hash)
    }

    /// Number of entries in the index.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the index is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Iterate over all entries.
    pub fn iter(&self) -> impl Iterator<Item = &IndexEntry> {
        self.entries.values()
    }
}

/// Append-only packfile writer.
///
/// Accumulates objects in memory and flushes them to disk as a single
/// packfile with an accompanying index.
pub struct PackWriter {
    /// Accumulated object data (hash → compressed bytes).
    objects: Vec<(Hash, Vec<u8>)>,
    /// The index being built.
    index: PackIndex,
    /// Current write offset.
    offset: u64,
}

impl PackWriter {
    /// Create a new pack writer.
    pub fn new() -> Self {
        Self {
            objects: Vec::new(),
            index: PackIndex::new(),
            offset: 0,
        }
    }

    /// Append an object to the pack.
    pub fn append(&mut self, hash: Hash, data: Vec<u8>) {
        let length = data.len() as u64;
        self.index.insert(IndexEntry {
            offset: self.offset,
            length,
            hash: hash.clone(),
        });
        self.objects.push((hash, data));
        self.offset += length;
    }

    /// Write the packfile and its index to disk.
    pub fn flush(&self, pack_path: &Path, index_path: &Path) -> Result<()> {
        // Write packfile
        let mut pack = fs::File::create(pack_path)
            .map_err(|e| eo_core::error::CoreError::StorageIo(format!("create pack: {e}")))?;

        for (_hash, data) in &self.objects {
            pack.write_all(data)
                .map_err(|e| eo_core::error::CoreError::StorageIo(format!("write pack: {e}")))?;
        }
        pack.flush()
            .map_err(|e| eo_core::error::CoreError::StorageIo(format!("flush pack: {e}")))?;

        // Write index as JSON (simple approach)
        let index_entries: Vec<serde_json::Value> = self
            .index
            .iter()
            .map(|e| {
                serde_json::json!({
                    "hash": e.hash,
                    "offset": e.offset,
                    "length": e.length,
                })
            })
            .collect();

        let index_json = serde_json::to_string_pretty(&index_entries)
            .map_err(|e| eo_core::error::CoreError::Serialization(e.to_string()))?;

        fs::write(index_path, index_json)
            .map_err(|e| eo_core::error::CoreError::StorageIo(format!("write index: {e}")))?;

        Ok(())
    }
}

impl Default for PackWriter {
    fn default() -> Self {
        Self::new()
    }
}

/// Random-access reader for packfiles.
pub struct PackReader {
    /// Path to the packfile.
    pack_path: PathBuf,
    /// The loaded index.
    index: PackIndex,
}

impl PackReader {
    /// Open a packfile with its index.
    pub fn open(pack_path: &Path, index_path: &Path) -> Result<Self> {
        let index_json = fs::read_to_string(index_path)
            .map_err(|e| eo_core::error::CoreError::StorageIo(format!("read index: {e}")))?;

        let entries: Vec<serde_json::Value> = serde_json::from_str(&index_json)
            .map_err(|e| eo_core::error::CoreError::Serialization(format!("parse index: {e}")))?;

        let mut index = PackIndex::new();
        for entry in entries {
            index.insert(IndexEntry {
                offset: entry["offset"].as_u64().unwrap_or(0),
                length: entry["length"].as_u64().unwrap_or(0),
                hash: entry["hash"].as_str().unwrap_or("").to_string(),
            });
        }

        Ok(Self {
            pack_path: pack_path.to_path_buf(),
            index,
        })
    }

    /// Read an object from the packfile by hash.
    pub fn read(&self, hash: &Hash) -> Result<Vec<u8>> {
        let entry = self
            .index
            .get(hash)
            .ok_or_else(|| eo_core::error::CoreError::ObjectNotFound(hash.clone()))?;

        let mut file = fs::File::open(&self.pack_path)
            .map_err(|e| eo_core::error::CoreError::StorageIo(format!("open pack: {e}")))?;

        use std::io::Seek;
        file.seek(std::io::SeekFrom::Start(entry.offset))
            .map_err(|e| eo_core::error::CoreError::StorageIo(format!("seek pack: {e}")))?;

        let mut buf = vec![0u8; entry.length as usize];
        file.read_exact(&mut buf)
            .map_err(|e| eo_core::error::CoreError::StorageIo(format!("read pack: {e}")))?;

        Ok(buf)
    }

    /// Check if an object exists in the packfile.
    pub fn exists(&self, hash: &Hash) -> bool {
        self.index.get(hash).is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::objects::hash_blob;

    #[test]
    fn packfile_write_and_read_1000_objects() {
        let dir = tempfile::tempdir().unwrap();
        let pack_path = dir.path().join("objects.pack");
        let index_path = dir.path().join("objects.idx");

        let mut writer = PackWriter::new();
        let mut hashes = Vec::new();
        let mut data_by_hash = HashMap::new();

        for i in 0..1000 {
            let data = format!("object number {}", i).into_bytes();
            let hash = hash_blob(&data);
            writer.append(hash.clone(), data.clone());
            hashes.push(hash.clone());
            data_by_hash.insert(hash, data);
        }

        assert_eq!(writer.index.len(), 1000);
        writer.flush(&pack_path, &index_path).unwrap();

        let reader = PackReader::open(&pack_path, &index_path).unwrap();

        for hash in &hashes {
            assert!(reader.exists(hash));
            let retrieved = reader.read(hash).unwrap();
            assert_eq!(retrieved, data_by_hash[hash]);
        }
    }
}
