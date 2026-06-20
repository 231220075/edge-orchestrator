//! Tree diffing for state synchronization.
//!
//! Computes the difference between two tree snapshots, producing
//! a list of additions, deletions, and modifications.

use eo_core::traits::TreeEntry;

/// The result of diffing two trees.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeDiff {
    /// Entries present in `new` but not in `old` (or with different hash).
    pub added_or_modified: Vec<TreeEntry>,
    /// Entries present in `old` but not in `new`.
    pub removed: Vec<TreeEntry>,
    /// Entries identical in both trees.
    pub unchanged: Vec<TreeEntry>,
}

/// Compute the diff between two trees.
///
/// Returns a [`TreeDiff`] describing what changed from `old` to `new`.
pub fn diff_trees(old: &[TreeEntry], new: &[TreeEntry]) -> TreeDiff {
    use std::collections::BTreeMap;

    let old_map: BTreeMap<&str, &TreeEntry> = old.iter().map(|e| (e.name.as_str(), e)).collect();
    let new_map: BTreeMap<&str, &TreeEntry> = new.iter().map(|e| (e.name.as_str(), e)).collect();

    let mut added_or_modified = Vec::new();
    let mut removed = Vec::new();
    let mut unchanged = Vec::new();

    // Find additions and modifications
    for (name, new_entry) in &new_map {
        match old_map.get(name) {
            Some(old_entry) if old_entry.hash == new_entry.hash => {
                unchanged.push((*new_entry).clone());
            }
            _ => {
                added_or_modified.push((*new_entry).clone());
            }
        }
    }

    // Find deletions
    for (name, old_entry) in &old_map {
        if !new_map.contains_key(name) {
            removed.push((*old_entry).clone());
        }
    }

    TreeDiff {
        added_or_modified,
        removed,
        unchanged,
    }
}

impl TreeDiff {
    /// Whether the diff is empty (no changes).
    pub fn is_empty(&self) -> bool {
        self.added_or_modified.is_empty() && self.removed.is_empty()
    }

    /// Total number of changes (additions + modifications + deletions).
    pub fn change_count(&self) -> usize {
        self.added_or_modified.len() + self.removed.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use eo_core::traits::ObjectMode;

    fn entry(name: &str, hash: &str) -> TreeEntry {
        TreeEntry {
            name: name.into(),
            hash: hash.into(),
            mode: ObjectMode::Blob,
        }
    }

    #[test]
    fn identical_trees_produce_empty_diff() {
        let old = vec![entry("a.txt", "abc"), entry("b.txt", "def")];
        let new = vec![entry("a.txt", "abc"), entry("b.txt", "def")];
        let diff = diff_trees(&old, &new);
        assert!(diff.is_empty());
        assert_eq!(diff.unchanged.len(), 2);
    }

    #[test]
    fn added_file_detected() {
        let old = vec![entry("a.txt", "abc")];
        let new = vec![entry("a.txt", "abc"), entry("b.txt", "def")];
        let diff = diff_trees(&old, &new);
        assert_eq!(diff.added_or_modified.len(), 1);
        assert_eq!(diff.added_or_modified[0].name, "b.txt");
    }

    #[test]
    fn removed_file_detected() {
        let old = vec![entry("a.txt", "abc"), entry("b.txt", "def")];
        let new = vec![entry("a.txt", "abc")];
        let diff = diff_trees(&old, &new);
        assert_eq!(diff.removed.len(), 1);
        assert_eq!(diff.removed[0].name, "b.txt");
    }

    #[test]
    fn modified_file_detected() {
        let old = vec![entry("a.txt", "abc")];
        let new = vec![entry("a.txt", "xyz")];
        let diff = diff_trees(&old, &new);
        assert_eq!(diff.added_or_modified.len(), 1);
        assert_eq!(diff.added_or_modified[0].hash, "xyz");
    }
}
