//! Local object store — on-disk CAS storage following Git's layout.
//!
//! Objects are stored at `objects/<prefix>/<rest>` as zlib-compressed
//! JSON-serialized data. The store provides put/get/exists/gc operations.

use std::fs;
use std::io::{Read, Write};
use std::path::PathBuf;

use eo_core::error::Result;
use eo_core::traits::TreeEntry;
use eo_core::types::Hash;
use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;
use flate2::Compression;
use tracing::{debug, info};

use crate::objects::hash_commit;
use crate::objects::{hash_blob, hash_tree, Blob, Commit, Object, Tag, Tree};

/// Local filesystem-backed content-addressed object store.
///
/// Stores objects in the Git-like layout: `{root}/{prefix}/{rest}`
/// where the hash is split into 2-char prefix and the remainder.
pub struct LocalObjectStore {
    /// Root directory for object storage (e.g., `~/.edge-orchestrator/objects`).
    root: PathBuf,
}

impl LocalObjectStore {
    /// Create a new store rooted at the given path.
    ///
    /// Creates the directory if it doesn't exist.
    pub fn new(root: PathBuf) -> Result<Self> {
        fs::create_dir_all(&root).map_err(|e| {
            eo_core::error::CoreError::StorageIo(format!(
                "failed to create object store root {}: {e}",
                root.display()
            ))
        })?;
        Ok(Self { root })
    }

    /// Store a blob and return its content hash.
    pub fn put_blob(&self, data: &[u8]) -> Result<Hash> {
        let hash = hash_blob(data);
        let blob = Blob {
            hash: hash.clone(),
            data: data.to_vec(),
        };
        self.write_object(&Object::Blob(blob), &hash)?;
        debug!("Stored blob: {}", &hash[..12]);
        Ok(hash)
    }

    /// Retrieve a blob by its content hash.
    pub fn get_blob(&self, hash: &Hash) -> Result<Vec<u8>> {
        match self.read_object(hash)? {
            Object::Blob(blob) => Ok(blob.data),
            other => Err(eo_core::error::CoreError::HashMismatch {
                expected: hash.clone(),
                computed: format!("expected Blob, got {other:?}"),
            }),
        }
    }

    /// Store a tree and return its content hash.
    pub fn put_tree(&self, entries: Vec<TreeEntry>) -> Result<Hash> {
        let hash = hash_tree(&entries);
        let tree = Tree {
            hash: hash.clone(),
            entries,
        };
        self.write_object(&Object::Tree(tree), &hash)?;
        debug!("Stored tree: {}", &hash[..12]);
        Ok(hash)
    }

    /// Retrieve a tree by its content hash.
    pub fn get_tree(&self, hash: &Hash) -> Result<Vec<TreeEntry>> {
        match self.read_object(hash)? {
            Object::Tree(tree) => Ok(tree.entries),
            other => Err(eo_core::error::CoreError::HashMismatch {
                expected: hash.clone(),
                computed: format!("expected Tree, got {other:?}"),
            }),
        }
    }

    /// Create a commit and return its hash.
    pub fn commit(
        &self,
        tree_hash: Hash,
        parent_hashes: Vec<Hash>,
        author: &str,
        message: &str,
    ) -> Result<Hash> {
        let timestamp = chrono::Utc::now();
        let hash = hash_commit(&tree_hash, &parent_hashes, author, &timestamp, message);
        let commit = Commit {
            hash: hash.clone(),
            tree_hash,
            parent_hashes,
            author: author.to_string(),
            timestamp,
            message: message.to_string(),
        };
        self.write_object(&Object::Commit(commit), &hash)?;
        debug!("Stored commit: {}", &hash[..12]);
        Ok(hash)
    }

    /// Get a commit by hash.
    pub fn get_commit(&self, hash: &Hash) -> Result<Commit> {
        match self.read_object(hash)? {
            Object::Commit(c) => Ok(c),
            other => Err(eo_core::error::CoreError::HashMismatch {
                expected: hash.clone(),
                computed: format!("expected Commit, got {other:?}"),
            }),
        }
    }

    /// Store a tag and return its hash.
    pub fn put_tag(&self, commit_hash: Hash, name: &str, message: Option<&str>) -> Result<Hash> {
        // Tag hash is just the hash of its serialized content
        let tag = Tag {
            hash: String::new(), // Will be set after hashing
            commit_hash,
            name: name.to_string(),
            message: message.map(|s| s.to_string()),
        };
        let serialized = serde_json::to_vec(&tag)
            .map_err(|e| eo_core::error::CoreError::Serialization(e.to_string()))?;
        let hash = hash_blob(&serialized);
        let tag = Tag {
            hash: hash.clone(),
            ..tag
        };
        self.write_object(&Object::Tag(tag), &hash)?;
        debug!("Stored tag: {} -> {}", name, &hash[..12]);
        Ok(hash)
    }

    /// Check whether an object exists in the store.
    pub fn exists(&self, hash: &Hash) -> bool {
        self.object_path(hash).exists()
    }

    /// Walk all reachable objects starting from a commit hash,
    /// and return unreferenced objects for GC.
    pub fn gc(&self, _keep_roots: &[Hash]) -> Result<Vec<Hash>> {
        // TODO: Full reachability analysis.
        // For now, return empty — GC will be fully implemented later.
        info!("GC: scanning object store at {}", self.root.display());
        Ok(vec![])
    }

    // ------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------

    /// Compute the on-disk path for an object hash.
    fn object_path(&self, hash: &Hash) -> PathBuf {
        let (prefix, rest) = hash.split_at(2);
        self.root.join(prefix).join(rest)
    }

    /// Write a zlib-compressed JSON object to disk.
    fn write_object(&self, object: &Object, hash: &Hash) -> Result<()> {
        let path = self.object_path(hash);
        if path.exists() {
            return Ok(()); // Object already exists, skip
        }

        // Ensure prefix directory exists
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| {
                eo_core::error::CoreError::StorageIo(format!("mkdir {}: {e}", parent.display()))
            })?;
        }

        let serialized = serde_json::to_vec(object)
            .map_err(|e| eo_core::error::CoreError::Serialization(e.to_string()))?;

        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
        encoder
            .write_all(&serialized)
            .map_err(|e| eo_core::error::CoreError::StorageIo(e.to_string()))?;
        let compressed = encoder
            .finish()
            .map_err(|e| eo_core::error::CoreError::StorageIo(e.to_string()))?;

        let mut file = fs::File::create(&path).map_err(|e| {
            eo_core::error::CoreError::StorageIo(format!("create {}: {e}", path.display()))
        })?;
        file.write_all(&compressed).map_err(|e| {
            eo_core::error::CoreError::StorageIo(format!("write {}: {e}", path.display()))
        })?;

        Ok(())
    }

    /// Read and decompress an object from disk.
    fn read_object(&self, hash: &Hash) -> Result<Object> {
        let path = self.object_path(hash);
        if !path.exists() {
            return Err(eo_core::error::CoreError::ObjectNotFound(hash.clone()));
        }

        let compressed = fs::read(&path).map_err(|e| {
            eo_core::error::CoreError::StorageIo(format!("read {}: {e}", path.display()))
        })?;

        let mut decoder = ZlibDecoder::new(&compressed[..]);
        let mut serialized = Vec::new();
        decoder.read_to_end(&mut serialized).map_err(|e| {
            eo_core::error::CoreError::StorageIo(format!("decompress {}: {e}", path.display()))
        })?;

        let object: Object = serde_json::from_slice(&serialized).map_err(|e| {
            eo_core::error::CoreError::Serialization(format!("deserialize {}: {e}", path.display()))
        })?;

        // Verify the stored hash matches the object's own hash
        let stored_hash = object.hash();
        if stored_hash != hash {
            return Err(eo_core::error::CoreError::HashMismatch {
                expected: hash.clone(),
                computed: stored_hash.clone(),
            });
        }

        Ok(object)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use eo_core::traits::ObjectMode;

    fn temp_store() -> LocalObjectStore {
        let dir = tempfile::tempdir().unwrap();
        LocalObjectStore::new(dir.path().to_path_buf()).unwrap()
    }

    #[test]
    fn blob_roundtrip_through_store() {
        let store = temp_store();
        let data = b"hello content-addressed world";
        let hash = store.put_blob(data).unwrap();
        assert!(store.exists(&hash));
        let retrieved = store.get_blob(&hash).unwrap();
        assert_eq!(retrieved, data);
    }

    #[test]
    fn tree_roundtrip() {
        let store = temp_store();
        let entries = vec![TreeEntry {
            name: "hello.txt".into(),
            hash: "abc123".into(),
            mode: ObjectMode::Blob,
        }];
        let hash = store.put_tree(entries.clone()).unwrap();
        assert!(store.exists(&hash));
        let retrieved = store.get_tree(&hash).unwrap();
        assert_eq!(retrieved, entries);
    }

    #[test]
    fn commit_chain_resolution() {
        let store = temp_store();

        // Create a tree
        let tree_hash = store
            .put_tree(vec![TreeEntry {
                name: "data.txt".into(),
                hash: "blobhash123".into(),
                mode: ObjectMode::Blob,
            }])
            .unwrap();

        // Create initial commit
        let c1 = store
            .commit(tree_hash.clone(), vec![], "test", "initial commit")
            .unwrap();
        let commit1 = store.get_commit(&c1).unwrap();
        assert_eq!(commit1.tree_hash, tree_hash);
        assert!(commit1.parent_hashes.is_empty());
        assert_eq!(commit1.message, "initial commit");

        // Create child commit
        let c2 = store
            .commit(tree_hash.clone(), vec![c1.clone()], "test", "second commit")
            .unwrap();
        let commit2 = store.get_commit(&c2).unwrap();
        assert_eq!(commit2.parent_hashes, vec![c1]);
    }

    #[test]
    fn object_not_found_error() {
        let store = temp_store();
        let result = store.get_blob(&"nonexistent_hash".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn store_is_idempotent() {
        let store = temp_store();
        let data = b"idempotent test";
        let h1 = store.put_blob(data).unwrap();
        let h2 = store.put_blob(data).unwrap();
        assert_eq!(h1, h2);
    }
}
