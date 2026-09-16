// Project snapshot: pack a directory into a tar, and the inverse.

use std::path::Path;

use eo_core::error::{CoreError, Result};
use eo_core::types::ProjectSnapshot;

/// Pack a directory (recursively) into a tar byte stream.
/// Used by master-side project submission and executor extraction (Phase 2).
#[allow(dead_code)]
pub fn pack_directory(dir: &Path) -> Result<Vec<u8>> {
    let mut tar = tar::Builder::new(Vec::new());
    tar.append_dir_all(".", dir)
        .map_err(|e| CoreError::Internal(format!("tar pack {}: {e}", dir.display())))?;
    let inner = tar
        .into_inner()
        .map_err(|e| CoreError::Internal(format!("tar finish: {e}")))?;
    Ok(inner)
}

/// Unpack a tar byte stream into the destination directory.
#[allow(dead_code)]
pub fn unpack_tar(bytes: &[u8], dest: &Path) -> Result<()> {
    std::fs::create_dir_all(dest)
        .map_err(|e| CoreError::Internal(format!("mkdir {}: {e}", dest.display())))?;
    let mut archive = tar::Archive::new(bytes);
    archive
        .unpack(dest)
        .map_err(|e| CoreError::Internal(format!("untar: {e}")))?;
    Ok(())
}

/// Pack a directory and store it in the given CAS (master-side).
///
/// Returns the hash only: the master keeps the bytes locally and serves them to
/// the executor over the blob protocol, so the task message stays small no matter
/// how large the workspace is.
pub fn snapshot_from_dir(dir: &Path, store: &storage::LocalObjectStore) -> Result<ProjectSnapshot> {
    let tar_bytes = pack_directory(dir)?;
    let hash = store.put_blob(&tar_bytes)?;
    Ok(ProjectSnapshot::from_hash(hash))
}

/// Extract a snapshot into the destination dir (executor side, Linux only).
///
/// The bytes come from whichever source the executor has: the inline payload when
/// the sender inlined it, otherwise its local CAS (after a fetch). An empty
/// payload with no CAS entry is a hard error rather than an empty workspace — the
/// earlier "build in an empty directory" failure mode was expensive to diagnose.
#[cfg(target_os = "linux")]
pub fn extract_snapshot(
    snapshot: &ProjectSnapshot,
    store: &storage::LocalObjectStore,
    dest: &Path,
) -> Result<()> {
    let bytes = if !snapshot.tar_bytes.is_empty() {
        snapshot.tar_bytes.clone()
    } else {
        store.get_blob(&snapshot.hash).map_err(|e| {
            CoreError::Internal(format!(
                "snapshot {} is neither inline nor in the local CAS: {e}",
                snapshot.hash
            ))
        })?
    };
    unpack_tar(&bytes, dest)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pack_unpack_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(src.join("main.c"), b"int main(){return 0;}").unwrap();
        std::fs::write(src.join("hello.txt"), b"hi").unwrap();

        let bytes = pack_directory(&src).unwrap();
        let out = dir.path().join("out");
        unpack_tar(&bytes, &out).unwrap();

        assert_eq!(
            std::fs::read(out.join("main.c")).unwrap(),
            b"int main(){return 0;}"
        );
        assert_eq!(std::fs::read(out.join("hello.txt")).unwrap(), b"hi");
    }

    #[test]
    fn snapshot_hash_is_deterministic_and_lands_in_cas() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("p");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(src.join("a.txt"), b"a").unwrap();
        let store =
            storage::LocalObjectStore::new(dir.path().to_path_buf()).expect("temp CAS is usable");

        let s1 = snapshot_from_dir(&src, &store).unwrap();
        let s2 = snapshot_from_dir(&src, &store).unwrap();
        assert_eq!(s1.hash, s2.hash);
        // The master must be able to serve the bytes it advertises.
        assert!(!s1.tar_bytes.is_empty() || store.exists(&s1.hash));
        assert_eq!(
            s1.inline_len(),
            0,
            "snapshots are hash-only on the wire; the bytes live in the CAS"
        );
        assert!(
            store.exists(&s1.hash),
            "hash-only means the CAS must hold it"
        );
    }
}
