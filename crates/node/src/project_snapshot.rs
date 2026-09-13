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

/// Build a ProjectSnapshot from a local directory (Phase 2 master-side path).
#[allow(dead_code)]
pub fn snapshot_from_dir(dir: &Path) -> Result<ProjectSnapshot> {
    let tar_bytes = pack_directory(dir)?;
    let hash = storage::hash_blob(&tar_bytes);
    Ok(ProjectSnapshot { hash, tar_bytes })
}

/// Extract a ProjectSnapshot tar into the destination dir (executor side).
#[allow(dead_code)]
pub fn extract_snapshot(snapshot: &ProjectSnapshot, dest: &Path) -> Result<()> {
    unpack_tar(&snapshot.tar_bytes, dest)
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
    fn snapshot_hash_is_deterministic() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("p");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(src.join("a.txt"), b"a").unwrap();

        let s1 = snapshot_from_dir(&src).unwrap();
        let s2 = snapshot_from_dir(&src).unwrap();
        assert_eq!(s1.hash, s2.hash);
    }
}
