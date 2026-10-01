//! `StageStore`: staging a committed record through each durability step to a
//! receipt, retrying a failed step onto the same payload, recovering published
//! marker and payload pairs in sequence order, and removing a stage only once a
//! verified watermark covers it.

use std::collections::BTreeSet;
use std::fs;
use std::io::{self, ErrorKind, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::ingest::infrastructure::committed_stage::files::{
    copy_reader_fixed, digest_reader, ensure_directory, ensure_regular_file, open_regular_readonly,
    path_exists, prepare_root, rename_new, sync_directory, sync_regular_file, write_and_hash,
};
use crate::ingest::infrastructure::committed_stage::marker::{read_marker, write_marker};
use crate::ingest::infrastructure::committed_stage::{
    invalid, stage_name, temp_name, CleanupProof, DurableStage, Marker, NoFailures, SourceIdentity,
    StageFailureInjector, StageFailurePoint, StageStore, MARKER_SUFFIX, RECORD_SUFFIX,
};

impl StageStore {
    pub(crate) fn new(root: impl Into<PathBuf>) -> io::Result<Self> {
        Self::with_injector(root, Arc::new(NoFailures))
    }

    pub(crate) fn with_injector(
        root: impl Into<PathBuf>,
        injector: Arc<dyn StageFailureInjector>,
    ) -> io::Result<Self> {
        let root = root.into();
        prepare_root(&root)?;
        let root = fs::canonicalize(root)?;
        Ok(Self { root, injector })
    }

    /// Stream a committed record into a new durable stage.
    ///
    /// The source reader is consumed once.  The implementation owns only a
    /// fixed 64 KiB buffer, the SHA-256 state, and small marker metadata.
    pub(crate) fn stage<R: Read>(
        &self,
        sequence: u64,
        engine_epoch: u64,
        source: SourceIdentity,
        mut reader: R,
    ) -> io::Result<DurableStage> {
        self.stage_with_writer(sequence, engine_epoch, source, |writer| {
            copy_reader_fixed(&mut reader, writer)
        })
    }

    /// Stream a serializer directly into staging. This avoids a whole-record
    /// encoded buffer for owned records such as coordinator `Record`.
    pub(crate) fn stage_with_writer<F>(
        &self,
        sequence: u64,
        engine_epoch: u64,
        source: SourceIdentity,
        mut write: F,
    ) -> io::Result<DurableStage>
    where
        F: FnMut(&mut dyn Write) -> io::Result<()>,
    {
        let name = stage_name(sequence, engine_epoch);
        let records = self.records_dir();
        let markers = self.markers_dir();
        ensure_directory(&records)?;
        ensure_directory(&markers)?;

        let record_path = records.join(format!("{name}{RECORD_SUFFIX}"));
        let marker_path = markers.join(format!("{name}{MARKER_SUFFIX}"));
        let payload_temp = records.join(temp_name("payload"));
        let marker_temp = markers.join(temp_name("marker"));
        let result = (|| {
            let (byte_len, digest) = write_and_hash(&payload_temp, &mut write)?;
            let expected = Marker {
                sequence,
                engine_epoch,
                source: source.clone(),
                byte_len,
                digest,
            };
            if path_exists(&marker_path)? {
                let marker = read_marker(&marker_path)?;
                if marker != expected {
                    return Err(invalid("conflicting committed-stage identity or payload"));
                }
                self.finish_existing(&record_path, &marker_path, &expected)?;
                self.remove_redundant_payload_temp(&payload_temp)?;
                return Ok(self.receipt(expected, record_path, marker_path));
            }
            if path_exists(&record_path)? {
                let (old_len, old_digest) = digest_reader(open_regular_readonly(&record_path)?)?;
                if old_len != byte_len || old_digest != digest {
                    return Err(invalid("conflicting committed-stage payload"));
                }
                self.injector.check(StageFailurePoint::SyncPayload)?;
                sync_regular_file(&record_path)?;
                self.injector
                    .check(StageFailurePoint::SyncPayloadDirectory)?;
                sync_directory(&records)?;
                self.finish_marker(&marker_temp, &marker_path, &expected)?;
                self.remove_redundant_payload_temp(&payload_temp)?;
                return Ok(self.receipt(expected, record_path, marker_path));
            }
            self.injector.check(StageFailurePoint::SyncPayload)?;
            sync_regular_file(&payload_temp)?;
            self.injector.check(StageFailurePoint::RenamePayload)?;
            rename_new(&payload_temp, &record_path)?;
            self.injector
                .check(StageFailurePoint::SyncPayloadDirectory)?;
            sync_directory(&records)?;

            self.finish_marker(&marker_temp, &marker_path, &expected)?;
            Ok(self.receipt(expected, record_path, marker_path))
        })();
        if result.is_err() {
            // Do not delete a payload that may have reached a commit-adjacent
            // state.  It is an orphan until an operator/recovery policy proves
            // it safe.  Temp files get the same conservative treatment.
        }
        result
    }

    fn finish_existing(&self, record: &Path, marker: &Path, expected: &Marker) -> io::Result<()> {
        let (len, digest) = digest_reader(open_regular_readonly(record)?)?;
        if len != expected.byte_len || digest != expected.digest {
            return Err(invalid("committed-stage payload differs from marker"));
        }
        self.injector.check(StageFailurePoint::SyncPayload)?;
        sync_regular_file(record)?;
        self.injector
            .check(StageFailurePoint::SyncPayloadDirectory)?;
        sync_directory(&self.records_dir())?;
        self.injector.check(StageFailurePoint::SyncMarker)?;
        sync_regular_file(marker)?;
        self.injector
            .check(StageFailurePoint::SyncMarkerDirectory)?;
        sync_directory(&self.markers_dir())
    }

    /// An existing matching marker proves this invocation's new temporary
    /// payload is not recovery data. Remove only that known file; uncertain
    /// failed-stage payloads and all other orphans remain untouched.
    fn remove_redundant_payload_temp(&self, payload_temp: &Path) -> io::Result<()> {
        ensure_regular_file(payload_temp)?;
        fs::remove_file(payload_temp)?;
        sync_directory(&self.records_dir())
    }

    fn finish_marker(&self, temp: &Path, marker: &Path, expected: &Marker) -> io::Result<()> {
        write_marker(temp, expected)?;
        self.injector.check(StageFailurePoint::SyncMarker)?;
        sync_regular_file(temp)?;
        self.injector.check(StageFailurePoint::PublishMarker)?;
        rename_new(temp, marker)?;
        self.injector
            .check(StageFailurePoint::SyncMarkerDirectory)?;
        sync_directory(&self.markers_dir())
    }

    fn receipt(&self, marker: Marker, record_path: PathBuf, marker_path: PathBuf) -> DurableStage {
        DurableStage {
            store_root: self.root.clone(),
            sequence: marker.sequence,
            engine_epoch: marker.engine_epoch,
            source: marker.source,
            byte_len: marker.byte_len,
            digest: marker.digest,
            record_path,
            marker_path,
        }
    }

    pub(crate) fn cleanup_proof(
        &self,
        source: SourceIdentity,
        engine_epoch: u64,
        verified_watermark: u64,
    ) -> CleanupProof {
        CleanupProof {
            store_root: self.root.clone(),
            source,
            engine_epoch,
            verified_watermark,
        }
    }

    /// Recover only fully published marker/payload pairs.  Orphan data files
    /// and temporary names are intentionally retained and never cleaned here.
    pub(crate) fn recover(&self) -> io::Result<Vec<DurableStage>> {
        let records = self.records_dir();
        let markers = self.markers_dir();
        ensure_directory(&records)?;
        ensure_directory(&markers)?;
        let mut recovered = Vec::new();
        let mut seen = BTreeSet::new();
        for entry in fs::read_dir(&markers)? {
            let entry = entry?;
            let path = entry.path();
            let file_name = entry.file_name();
            let Some(name) = file_name.to_str() else {
                return Err(invalid("committed-stage marker has non-UTF8 name"));
            };
            if name.starts_with(".marker-") && name.ends_with(".tmp") {
                // A failed marker publication leaves an uncertain orphan.  It
                // is deliberately retained, but it is not a commit marker.
                continue;
            }
            if !name.ends_with(MARKER_SUFFIX) {
                return Err(invalid("committed-stage marker directory has unknown file"));
            }
            ensure_regular_file(&path)?;
            let marker = read_marker(&path)?;
            let expected = format!(
                "{}{}",
                stage_name(marker.sequence, marker.engine_epoch),
                MARKER_SUFFIX
            );
            if name != expected {
                return Err(invalid(
                    "committed-stage marker name does not match content",
                ));
            }
            if !seen.insert((marker.sequence, marker.engine_epoch, marker.source.clone())) {
                return Err(invalid("duplicate committed-stage marker identity"));
            }
            let record_path = records.join(format!(
                "{}{}",
                stage_name(marker.sequence, marker.engine_epoch),
                RECORD_SUFFIX
            ));
            ensure_regular_file(&record_path)?;
            let (actual_len, actual_digest) = digest_reader(open_regular_readonly(&record_path)?)?;
            if actual_len != marker.byte_len || actual_digest != marker.digest {
                return Err(invalid(
                    "committed-stage payload checksum or length mismatch",
                ));
            }
            recovered.push(DurableStage {
                store_root: self.root.clone(),
                sequence: marker.sequence,
                engine_epoch: marker.engine_epoch,
                source: marker.source,
                byte_len: marker.byte_len,
                digest: marker.digest,
                record_path,
                marker_path: path,
            });
        }
        recovered.sort_by(|left, right| {
            (left.sequence, left.engine_epoch, &left.source).cmp(&(
                right.sequence,
                right.engine_epoch,
                &right.source,
            ))
        });
        Ok(recovered)
    }

    /// Delete a receipt only after the caller has verified a durable Engine
    /// watermark that includes this exact sequence.  There is intentionally no
    /// `Drop` cleanup path.
    pub(crate) fn remove_after_verified_watermark(
        &self,
        stage: DurableStage,
        proof: CleanupProof,
    ) -> io::Result<()> {
        if stage.store_root != self.root || proof.store_root != self.root {
            return Err(invalid("committed-stage receipt belongs to another store"));
        }
        if proof.source != stage.source || proof.engine_epoch != stage.engine_epoch {
            return Err(invalid(
                "cleanup proof does not cover stage source and epoch",
            ));
        }
        if proof.verified_watermark < stage.sequence {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "durable watermark is below committed-stage sequence",
            ));
        }
        // Revalidate before removal so a caller cannot remove a path that a
        // corrupt or substituted marker no longer proves.
        let marker = read_marker(&stage.marker_path)?;
        if marker.sequence != stage.sequence
            || marker.engine_epoch != stage.engine_epoch
            || marker.source != stage.source
            || marker.byte_len != stage.byte_len
            || marker.digest != stage.digest
        {
            return Err(invalid("committed-stage receipt no longer matches marker"));
        }
        let (len, digest) = digest_reader(open_regular_readonly(&stage.record_path)?)?;
        if len != stage.byte_len || digest != stage.digest {
            return Err(invalid(
                "committed-stage receipt payload changed before removal",
            ));
        }
        fs::remove_file(&stage.marker_path)?;
        sync_directory(&self.markers_dir())?;
        fs::remove_file(&stage.record_path)?;
        sync_directory(&self.records_dir())?;
        Ok(())
    }

    pub(super) fn records_dir(&self) -> PathBuf {
        self.root.join("records")
    }

    pub(super) fn markers_dir(&self) -> PathBuf {
        self.root.join("markers")
    }
}
