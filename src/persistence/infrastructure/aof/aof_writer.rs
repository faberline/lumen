//! The AOF writer: appending frames in applied-sequence order, the fsync policy
//! and its off-lock sync plan, and trimming the prefix a checkpoint covers.

use std::path::PathBuf;

use anyhow::Result;
use storage_durable::{FramedLogTrimObserver, FramedLogWriter, FsyncPolicy};

use crate::ingest::domain::wal_record::WalRecord;
use crate::persistence::infrastructure::aof::frame::encode_payload;

#[cfg(unix)]
use storage_durable::FramedLogTrimPlan;

/// Append-only writer keyed by applied seq. Frames are appended in seq order;
/// `open` first truncates any torn tail left by a crash mid-append, so the file
/// always starts in a clean, fully-decodable state.
pub struct AofWriter {
    inner: FramedLogWriter,
    #[cfg(test)]
    pub(super) policy: FsyncPolicy,
    /// #2516: test-only ENOSPC fault injection, armed via
    /// [`AofWriter::set_inject_storage_full`]. Scoped to THIS writer instance
    /// (not a process-global flag) so parallel `cargo test` threads sharing
    /// the same test binary never cross-contaminate each other's AOF writes —
    /// each test opens its own `AofWriter` over its own tempdir.
    #[cfg(test)]
    inject_storage_full: std::sync::atomic::AtomicBool,
    /// Test-only one-shot non-ENOSPC failure. This models a single AOF gap
    /// followed by a healthy filesystem, so coordinator tests can prove that
    /// later records are still rejected after the first uncertain write.
    #[cfg(test)]
    inject_failure_once: Option<std::io::ErrorKind>,
}

pub(crate) struct AofSyncPlan {
    sync: Option<Box<dyn FnOnce() -> Result<()> + Send>>,
    complete: Option<Box<dyn FnOnce(&mut FramedLogWriter) -> Result<()> + Send>>,
}

impl AofSyncPlan {
    pub(crate) fn sync_off_lock(&mut self) -> Result<()> {
        self.sync.take().expect("AOF sync plan already synced")()
    }

    pub(crate) fn complete(mut self, writer: &mut FramedLogWriter) -> Result<()> {
        self.complete
            .take()
            .expect("AOF sync plan already completed")(writer)
    }
}

/// A prepared durable AOF trim. It has no public configuration surface.
#[cfg(unix)]
pub(crate) struct AofTrimPlan {
    inner: FramedLogTrimPlan,
}

#[cfg(unix)]
impl AofTrimPlan {
    /// Copy and durably sync the stable trim prefix without borrowing the AOF
    /// writer, so `SharedAof` can admit normal appends during this work.
    pub(crate) fn copy_stable_prefix(&mut self) -> Result<()> {
        self.inner.copy_stable_prefix()
    }
}

impl AofWriter {
    pub(crate) fn begin_sync(&mut self) -> Result<Option<AofSyncPlan>> {
        let Some(plan) = self.inner.begin_sync()? else {
            return Ok(None);
        };
        let state = std::sync::Arc::new(std::sync::Mutex::new(Some(plan)));
        let sync_state = std::sync::Arc::clone(&state);
        let complete_state = std::sync::Arc::clone(&state);
        Ok(Some(AofSyncPlan {
            sync: Some(Box::new(move || {
                sync_state
                    .lock()
                    .map_err(|_| anyhow::anyhow!("AOF sync plan poisoned"))?
                    .as_ref()
                    .expect("AOF sync plan missing")
                    .sync_off_lock()
            })),
            complete: Some(Box::new(move |writer| {
                let plan = complete_state
                    .lock()
                    .map_err(|_| anyhow::anyhow!("AOF sync plan poisoned"))?
                    .take()
                    .expect("AOF sync plan missing");
                writer.complete_sync(plan)
            })),
        }))
    }

    pub(crate) fn complete_sync(&mut self, plan: AofSyncPlan) -> Result<()> {
        plan.complete(&mut self.inner)
    }

    /// Install an optional in-process observer for covered AOF frames and the
    /// initial temp-sync boundary during trim. This only forwards the shared
    /// framed-log hook; it does not change AOF locking, trimming, or durability.
    #[doc(hidden)]
    pub fn with_trim_observer(
        mut self,
        observer: std::sync::Arc<dyn FramedLogTrimObserver>,
    ) -> Self {
        self.inner = self.inner.with_trim_observer(observer);
        self
    }

    /// Begin a two-phase trim while the caller owns `SharedAof`.
    #[cfg(unix)]
    pub(crate) fn begin_trim(&mut self, through: u64) -> Result<AofTrimPlan> {
        self.inner
            .begin_trim_mapped(through)
            .map(|inner| AofTrimPlan { inner })
    }

    /// Finish a prepared trim while the caller owns `SharedAof`.
    #[cfg(unix)]
    pub(crate) fn finish_trim(&mut self, plan: AofTrimPlan) -> Result<()> {
        self.inner.finish_trim_mapped(plan.inner)
    }

    /// Open `path` for appending with the default [`FsyncPolicy::Always`],
    /// first truncating any torn tail.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        Self::open_with_policy(path, FsyncPolicy::Always)
    }

    /// Open `path` for appending, first truncating any torn tail (a partial
    /// frame from a crash mid-append) so the next append lands after the last
    /// good frame. Creates the file (and parent dirs) if absent.
    pub fn open_with_policy(path: impl Into<PathBuf>, policy: FsyncPolicy) -> Result<Self> {
        let path = path.into();
        Ok(Self {
            inner: FramedLogWriter::open(&path, policy)?,
            #[cfg(test)]
            policy,
            #[cfg(test)]
            inject_storage_full: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            inject_failure_once: None,
        })
    }

    /// #2516: arm/disarm the next typed or raw append call on THIS writer to
    /// fail with a synthetic `io::ErrorKind::StorageFull` error instead of
    /// touching the real file — the fault-injection seam that exercises the
    /// REAL production error-handling path
    /// (`crate::ingest::application::write_coordinator::errors::is_storage_full`
    /// -> `Metrics::mark_storage_degraded`
    /// -> `crate::ingest::application::write_coordinator::errors::StorageFullError`
    /// -> `ApiErr`'s 507 mapping) end to end without needing a genuinely full
    /// disk.
    #[cfg(test)]
    pub fn set_inject_storage_full(&self, on: bool) {
        self.inject_storage_full
            .store(on, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(test)]
    pub fn inject_failure_once(&mut self, kind: std::io::ErrorKind) {
        self.inject_failure_once = Some(kind);
    }

    /// Append one applied `(seq, record)` frame. Buffered; durability follows the
    /// fsync policy (`Always` fsyncs now, `EverySec` defers to `maybe_sync`).
    pub fn append(&mut self, seq: u64, record: &WalRecord) -> Result<()> {
        self.check_injected_append_failure()?;
        let payload = encode_payload(record)?;
        self.append_large_payload(seq, &payload)
    }

    /// Append a payload which the caller already validated as one public WAL
    /// wire record. This keeps a mapped fast-Index source borrowed: it does not
    /// decode or clone its values. The coordinator owns wire validation before
    /// it calls this crate-private persistence primitive.
    pub(crate) fn append_raw_payload(&mut self, seq: u64, payload: &[u8]) -> Result<()> {
        self.check_injected_append_failure()?;
        self.append_large_payload(seq, payload)
    }

    fn check_injected_append_failure(&mut self) -> Result<()> {
        #[cfg(test)]
        if let Some(kind) = self.inject_failure_once.take() {
            return Err(anyhow::Error::new(std::io::Error::from(kind)));
        }
        #[cfg(test)]
        if self
            .inject_storage_full
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(anyhow::Error::new(std::io::Error::from(
                std::io::ErrorKind::StorageFull,
            )));
        }
        Ok(())
    }

    fn append_large_payload(&mut self, seq: u64, payload: &[u8]) -> Result<()> {
        self.inner.append_large_payload(seq, payload)
    }

    /// Flush the buffered writer to the OS (does NOT fsync). Cheap; safe to call
    /// often.
    pub fn flush(&mut self) -> Result<()> {
        self.inner.flush()
    }

    /// Flush + fsync NOW, unconditionally. Resets the everysec timer.
    pub fn sync(&mut self) -> Result<()> {
        self.inner.sync()
    }

    /// Flush and fsync the AOF, then require a successful data-root directory
    /// fsync. Durable restore uses this before it can move `CURRENT`.
    pub fn sync_strict(&mut self) -> Result<()> {
        self.inner.sync_strict()
    }

    /// Call-driven everysec fsync: fsync only if dirty AND ≥ the cadence has
    /// elapsed since the last sync. Under `Always` this is a no-op (already
    /// synced on append). Meant to be called off the apply hot path (a periodic
    /// tick or after a batch), so the apply loop never blocks on fsync.
    pub fn maybe_sync(&mut self) -> Result<()> {
        self.inner.maybe_sync()
    }

    /// Drop every frame with `seq <= through`, keeping only newer frames. Called
    /// at checkpoint: once a checkpoint at seq `C` is durable in the segment RDB,
    /// every AOF frame with `seq <= C` is redundant and can be reclaimed.
    ///
    /// Crash-safe rewrite-survivors-to-temp + atomic rename: surviving frames are
    /// streamed (byte-for-byte, no re-encode) into `<path>.compact.tmp`, fsynced,
    /// then renamed over `path`. A crash before the rename leaves the original
    /// AOF intact (un-checkpointed frames are never lost); a crash after leaves
    /// the compacted AOF. The temp is removed first if a prior attempt left one.
    pub fn truncate_through(&mut self, through: u64) -> Result<()> {
        self.inner.truncate_through_mapped(through)
    }
}
