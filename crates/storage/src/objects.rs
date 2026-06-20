//! Git-model objects: Blob, Tree, Commit, Tag.
//!
//! Objects are content-addressed by SHA-256 hash. The hashing follows
//! Git's model: `"<type> <len>\0<content>"` prefixed hashing.

use chrono::{DateTime, Utc};
use eo_core::traits::{ObjectMode, TreeEntry};
use eo_core::types::Hash;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

// ---------------------------------------------------------------------------
// Object types
// ---------------------------------------------------------------------------

/// A Blob — arbitrary bytes content-addressed by SHA-256.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Blob {
    /// The content hash (SHA-256 hex).
    pub hash: Hash,
    /// The raw data.
    pub data: Vec<u8>,
}

/// A Tree — a directory snapshot containing sorted entries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tree {
    /// The tree hash.
    pub hash: Hash,
    /// Sorted entries in this directory.
    pub entries: Vec<TreeEntry>,
}

/// A Commit — a pointer to a Tree with metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Commit {
    /// The commit hash.
    pub hash: Hash,
    /// The tree this commit points to.
    pub tree_hash: Hash,
    /// Parent commit hashes (empty for initial commit).
    pub parent_hashes: Vec<Hash>,
    /// Author identifier.
    pub author: String,
    /// Timestamp of the commit.
    pub timestamp: DateTime<Utc>,
    /// Human-readable commit message.
    pub message: String,
}

/// A Tag — a named reference to a Commit (used for snapshots).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tag {
    /// The tag hash.
    pub hash: Hash,
    /// The commit this tag points to.
    pub commit_hash: Hash,
    /// Tag name (e.g., "snapshot-raft-index-42").
    pub name: String,
    /// Optional annotation.
    pub message: Option<String>,
}

/// Enum over all object types.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Object {
    /// A blob.
    Blob(Blob),
    /// A tree.
    Tree(Tree),
    /// A commit.
    Commit(Commit),
    /// A tag.
    Tag(Tag),
}

impl Object {
    /// Return the hash of this object.
    pub fn hash(&self) -> &Hash {
        match self {
            Object::Blob(b) => &b.hash,
            Object::Tree(t) => &t.hash,
            Object::Commit(c) => &c.hash,
            Object::Tag(t) => &t.hash,
        }
    }
}

// ---------------------------------------------------------------------------
// Hashing functions (Git-compatible)
// ---------------------------------------------------------------------------

/// Compute a SHA-256 hash of data prefixed with type and length.
///
/// Format: `"blob <len>\0<data>"` (following Git's object model).
pub fn hash_blob(data: &[u8]) -> Hash {
    let header = format!("blob {}\0", data.len());
    let mut hasher = Sha256::new();
    hasher.update(header.as_bytes());
    hasher.update(data);
    hex::encode(hasher.finalize())
}

/// Compute a SHA-256 hash of a tree from its sorted entries.
///
/// Format: one line per entry: `"<mode> <hash> <name>\n"`, then hashed
/// with prefix `"tree <len>\0"`.
pub fn hash_tree(entries: &[TreeEntry]) -> Hash {
    let mut content = String::new();
    let mut sorted = entries.to_vec();
    sorted.sort_by(|a, b| a.name.cmp(&b.name));

    for entry in &sorted {
        let mode_str = match entry.mode {
            ObjectMode::Blob => "100644",
            ObjectMode::Executable => "100755",
            ObjectMode::Tree => "040000",
        };
        content.push_str(&format!("{} {} {}\n", mode_str, entry.hash, entry.name));
    }

    let header = format!("tree {}\0", content.len());
    let mut hasher = Sha256::new();
    hasher.update(header.as_bytes());
    hasher.update(content.as_bytes());
    hex::encode(hasher.finalize())
}

/// Compute a SHA-256 hash of a commit.
pub fn hash_commit(
    tree_hash: &str,
    parent_hashes: &[Hash],
    author: &str,
    timestamp: &DateTime<Utc>,
    message: &str,
) -> Hash {
    let mut content = format!("tree {}\n", tree_hash);
    for parent in parent_hashes {
        content.push_str(&format!("parent {}\n", parent));
    }
    content.push_str(&format!(
        "author {} {}\n",
        author,
        timestamp.format("%s %z")
    ));
    content.push_str(&format!(
        "committer {} {}\n",
        author,
        timestamp.format("%s %z")
    ));
    content.push_str(&format!("\n{}\n", message));

    let header = format!("commit {}\0", content.len());
    let mut hasher = Sha256::new();
    hasher.update(header.as_bytes());
    hasher.update(content.as_bytes());
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_blob_is_deterministic() {
        let data = b"hello world";
        let h1 = hash_blob(data);
        let h2 = hash_blob(data);
        assert_eq!(h1, h2);
    }

    #[test]
    fn hash_blob_different_for_different_data() {
        let h1 = hash_blob(b"hello");
        let h2 = hash_blob(b"world");
        assert_ne!(h1, h2);
    }

    #[test]
    fn hash_tree_is_deterministic() {
        let entries = vec![
            TreeEntry {
                name: "b.txt".into(),
                hash: "abc123".into(),
                mode: ObjectMode::Blob,
            },
            TreeEntry {
                name: "a.txt".into(),
                hash: "def456".into(),
                mode: ObjectMode::Blob,
            },
        ];
        let h1 = hash_tree(&entries);
        let h2 = hash_tree(&entries);
        assert_eq!(h1, h2);
    }

    #[test]
    fn hash_tree_sorted_independent_of_input_order() {
        let entries_a = vec![
            TreeEntry {
                name: "b.txt".into(),
                hash: "abc".into(),
                mode: ObjectMode::Blob,
            },
            TreeEntry {
                name: "a.txt".into(),
                hash: "def".into(),
                mode: ObjectMode::Blob,
            },
        ];
        let entries_b = vec![
            TreeEntry {
                name: "a.txt".into(),
                hash: "def".into(),
                mode: ObjectMode::Blob,
            },
            TreeEntry {
                name: "b.txt".into(),
                hash: "abc".into(),
                mode: ObjectMode::Blob,
            },
        ];
        assert_eq!(hash_tree(&entries_a), hash_tree(&entries_b));
    }

    #[test]
    fn hash_commit_is_deterministic() {
        // Same everything → same hash
        let ts = Utc::now();
        let h1 = hash_commit("tree_hash", &[], "test", &ts, "init");
        let h2 = hash_commit("tree_hash", &[], "test", &ts, "init");
        assert_eq!(h1, h2);

        // Different timestamps → different hashes
        let ts2 = ts + chrono::Duration::seconds(1);
        let h3 = hash_commit("tree_hash", &[], "test", &ts2, "init");
        assert_ne!(h1, h3);
    }
}
