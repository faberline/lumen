//! The private directories pending-change spills and fallback checkpoints write
//! to: one mode-0700 directory per spill under the system temp dir, removed
//! when the last store holding it drops.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use anyhow::{Context, Result};

use crate::persistence::infrastructure::segment_rdb_store::SegmentRdbStore;

static PENDING_SPILL_NONCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// A process-private root. It stays alive for the Engine lifetime because live
/// mmap readers and checkpoint lineage can still name files below it.
pub(in crate::persistence) struct SpillDirectory {
    pub(in crate::persistence) path: std::path::PathBuf,
}

impl SpillDirectory {
    pub(in crate::persistence) fn create() -> Result<Self> {
        let parent = std::env::temp_dir();
        let process = std::process::id();
        for _ in 0..128 {
            let nonce = PENDING_SPILL_NONCE.fetch_add(1, Ordering::Relaxed);
            let path = parent.join(format!("lumen-pending-spill-{process}-{nonce}"));
            let mut builder = std::fs::DirBuilder::new();
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                builder.mode(0o700);
            }
            match builder.create(&path) {
                Ok(()) => return Ok(Self { path }),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error).context("create pending spill root"),
            }
        }
        anyhow::bail!("could not allocate a unique pending spill root")
    }
}

pub(crate) fn temporary_spill_store() -> Result<Arc<SegmentRdbStore>> {
    let root = Arc::new(SpillDirectory::create()?);
    Ok(Arc::new(
        SegmentRdbStore::new(&root.path)?.with_root_guard(root),
    ))
}

impl Drop for SpillDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}
