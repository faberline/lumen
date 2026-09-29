//! AOF replay: reading frames back in order past a starting sequence, stopping
//! at a torn tail, and applying them to an engine during cold start.

use std::path::Path;

use anyhow::{Context, Result};
use storage_durable::{FramedLogCursor, LogFrame};

use crate::ingest::domain::wal_record::WalRecord;
use crate::persistence::infrastructure::aof::frame::decode_payload;

/// Replay frames from an AOF, applying each `(seq, WalRecord)` with `seq >
/// from_seq` to a caller closure in order, stopping cleanly at a torn tail.
pub struct AofReader;

impl AofReader {
    /// Iterate every frame in `path` in order, SKIP frames with `seq <=
    /// from_seq` (already covered by the RDB baseline), and call `apply(seq,
    /// record)` for each frame with `seq > from_seq`. On a TORN TAIL (short read,
    /// `len` overruns EOF, or crc mismatch) STOP cleanly at the last good frame —
    /// no panic, no error. Returns the max seq REPLAYED (0 if none applied).
    ///
    /// An absent file replays nothing and returns 0 (a node that crashed before
    /// its first append simply has no AOF).
    pub fn replay(
        path: impl AsRef<Path>,
        from_seq: u64,
        apply: impl FnMut(u64, WalRecord),
    ) -> Result<u64> {
        let mut cursor = FramedLogCursor::open(path)?;
        replay_frames(from_seq, || cursor.next_frame(), apply)
    }
}

/// Replay one validated frame at a time in sequence order.
///
/// The next frame is fetched only after the previous callback returns.  This
/// keeps replay memory bounded by the frame currently being decoded.
pub(super) fn replay_frames(
    from_seq: u64,
    mut next_frame: impl FnMut() -> Result<Option<LogFrame>>,
    mut apply: impl FnMut(u64, WalRecord),
) -> Result<u64> {
    let mut max_seq = 0u64;
    while let Some(frame) = next_frame()? {
        if frame.seq <= from_seq {
            continue;
        }
        let rec = decode_payload(&frame.payload)
            .with_context(|| format!("decode complete AOF frame at sequence {}", frame.seq))?;
        apply(frame.seq, rec);
        max_seq = max_seq.max(frame.seq);
    }
    Ok(max_seq)
}

/// Recovery helper: replay every AOF frame with `seq > from_seq` into `engine`
/// via [`crate::storage::Engine::apply_raft_entry`], returning the max seq
/// replayed. This is step 2 of cold start (RDB → **AOF** → broker); the engine is
/// already seeded to `from_seq` by the segment checkpoint.
pub fn replay_aof_into(
    engine: &std::sync::Arc<crate::storage::Engine>,
    path: impl AsRef<Path>,
    from_seq: u64,
) -> Result<u64> {
    // Initialize the capture cut before a relay can react to a process-global
    // waiter from another Engine. The observed helper repeats this idempotently.
    engine.capture_barrier.apply().initialize_sequence(from_seq);
    // The public replay owns its fallback before any admission can wait. The
    // observed helper below deliberately stays a lower-level manual seam.
    let mut capacity_owner = None;
    crate::segment_capacity::Fallback::ensure(&mut capacity_owner, engine, None)?;
    replay_aof_into_with_capacity_owner(engine, path, from_seq, || {}, &mut capacity_owner)
}

pub(super) fn replay_aof_into_observed(
    engine: &std::sync::Arc<crate::storage::Engine>,
    path: impl AsRef<Path>,
    from_seq: u64,
    before_decode: impl FnMut(),
) -> Result<u64> {
    let mut capacity_owner = None;
    replay_aof_into_with_capacity_owner(engine, path, from_seq, before_decode, &mut capacity_owner)
}

fn replay_aof_into_with_capacity_owner(
    engine: &std::sync::Arc<crate::storage::Engine>,
    path: impl AsRef<Path>,
    from_seq: u64,
    mut before_decode: impl FnMut(),
    capacity_owner: &mut Option<crate::segment_capacity::Fallback>,
) -> Result<u64> {
    engine.capture_barrier.apply().initialize_sequence(from_seq);
    let mut cursor = FramedLogCursor::open(path)?;
    let mut max_seq = 0u64;

    while let Some((start, frame)) = {
        let start = cursor.byte_offset();
        cursor
            .next_large_mapped_frame()?
            .map(|frame| (start, frame))
    } {
        if frame.seq <= from_seq {
            continue;
        }
        let seq = frame.seq;
        let encoded_bytes = frame.payload().len();
        let encoded_crc = crc32fast::hash(frame.payload());
        // A large fast Index frame remains mapped through its scalar projection.
        // Do this before scanner admission or generic decode, so one valid value
        // cannot become an owned record merely because recovery is replaying it.
        if encoded_bytes > crate::ingest::domain::change_budget::HARD_LIMIT / 8 {
            if let Ok(scanner) =
                crate::ingest::infrastructure::wal::fast_index_scanner::FastIndexScanner::parse(
                    frame.payload(),
                )
            {
                if engine.try_apply_committed_index_with_capacity_owner(&scanner, seq, || {
                    crate::segment_capacity::Fallback::ensure(capacity_owner, engine, None)
                }, |apply, outcome| {
                    if let Err(error) = outcome {
                        tracing::warn!(seq, error = %error, "AOF replay apply error (entry no-ops)");
                    }
                    apply.advance_sequence(seq);
                })? {
                    max_seq = max_seq.max(seq);
                    continue;
                }
            }
            if engine.try_apply_committed_replace_with_capacity_owner(
                frame.payload(), seq,
                &mut || crate::segment_capacity::Fallback::ensure(capacity_owner, engine, None),
                |apply, outcome| {
                    if let Err(error) = outcome {
                        tracing::warn!(seq, error = %error, "AOF replay replacement business error");
                    }
                    apply.advance_sequence(seq);
                },
            )? {
                max_seq = max_seq.max(seq);
                continue;
            }
        }
        // This first pass borrows mapped bytes and stores only token counters.
        // The source mapping is excluded from pending heap ownership. Reserve
        // before the typed scanner or decoder can allocate token buffers.
        let workspace =
            crate::ingest::infrastructure::wire_cost::scan_workspace_bound(frame.payload())
                .with_context(|| format!("preflight complete AOF frame at sequence {seq}"))?;
        let request = engine.record_ram_request_from_bound(workspace, 0);
        let mut reservation = match engine.try_reserve_record_ram(&request) {
            Ok(reservation) => reservation,
            Err(crate::storage::RecordAdmissionError::Capacity(
                crate::ingest::domain::change_budget::AdmissionError::Full { .. },
            )) => {
                // A replay admission can hit the hard limit before the
                // decode-growth path runs.  Publish the maintenance request
                // before sleeping so the bootstrap checkpoint owner wakes
                // even when the current frame has no decoded reservation yet.
                engine.request_pending_checkpoint();
                engine
                    .wait_reserve_record_ram(&request)
                    .context("wait for AOF replay scanner admission")?
            }
            Err(error) => {
                return Err(anyhow::Error::new(error).context("reserve AOF replay scanner"))
            }
        };
        let decoded_peak =
            crate::ingest::infrastructure::wire_cost::decoded_peak_bound(frame.payload())
                .with_context(|| format!("price complete AOF frame at sequence {seq}"))?;
        match reservation.try_grow_to(decoded_peak) {
            Ok(()) => (),
            Err(crate::ingest::domain::change_budget::AdmissionError::Full { .. }) => {
                engine.request_pending_checkpoint();
                reservation
                    .wait_grow_to(decoded_peak)
                    .map_err(crate::storage::RecordAdmissionError::Capacity)
                    .context("wait for AOF replay decode admission")?;
            }
            Err(error) => {
                return Err(
                    anyhow::Error::new(crate::storage::RecordAdmissionError::Capacity(error))
                        .context("reserve AOF replay decode"),
                )
            }
        }
        // The same cursor pins the original inode and open-time length across
        // capacity waits. Recheck that exact frame, never resolve a new path.
        let reread = cursor
            .reread_large_mapped_frame_at(start)
            .context("reread pinned AOF frame after admission")?
            .ok_or_else(|| anyhow::anyhow!("AOF frame vanished from pinned cursor"))?;
        anyhow::ensure!(
            reread.seq == seq
                && reread.payload().len() == encoded_bytes
                && crc32fast::hash(reread.payload()) == encoded_crc,
            "pinned AOF frame identity changed while replay waited"
        );
        before_decode();
        let record = decode_payload(reread.payload())
            .with_context(|| format!("decode complete AOF frame at sequence {seq}"))?;
        engine
            .price_decoded_record(&record.entry, &mut reservation, decoded_peak, 0, false)
            .context("retain decoded AOF record charge")?;
        drop(reread);
        drop(frame);
        // Decoder scratch is gone. Release its conservative allowance before
        // normalized changes are prepared; retain the actual decoded owner.
        reservation
            .release_transport_bytes()
            .map_err(crate::storage::RecordAdmissionError::Capacity)
            .context("release completed AOF decoder workspace")?;
        let mut entry = record.entry;
        loop {
            match engine.begin_admitted_record(entry, reservation) {
                Ok(mut prepared) => {
                    // Existing partial-prefix semantics retain a charge before
                    // dispatch and advance ordinary validation errors. Admission,
                    // preparation, decode, and reread failures returned above do
                    // not reach this point and therefore do not advance `seq`.
                    if let Err(error) = engine.apply_prepared_raft_entry(&mut prepared) {
                        tracing::warn!(seq, error = %error, "AOF replay apply error (entry no-ops)");
                    }
                    prepared.apply_lease().advance_sequence(seq);
                    max_seq = max_seq.max(seq);
                    break;
                }
                Err(mut reprice) => {
                    let Some(required) = reprice.required else {
                        return Err(
                            anyhow::Error::new(reprice.error).context("prepare AOF replay record")
                        );
                    };
                    // `begin_admitted_record` dropped its apply lease before it
                    // returned repricing ownership. Only missing normalized or
                    // staged bytes wait here; the decoded entry remains charged.
                    let grown = match reprice.reservation.try_grow_to(required) {
                        Err(crate::ingest::domain::change_budget::AdmissionError::Full {
                            ..
                        }) => {
                            engine.request_pending_checkpoint();
                            reprice.reservation.wait_grow_to(required)
                        }
                        result => result,
                    };
                    grown
                        .map_err(crate::storage::RecordAdmissionError::Capacity)
                        .context("wait for AOF replay repricing")?;
                    entry = reprice.entry;
                    reservation = reprice.reservation;
                }
            }
        }
    }
    Ok(max_seq)
}
