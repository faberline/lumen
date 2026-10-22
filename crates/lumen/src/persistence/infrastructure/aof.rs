//! Local append-only log (Stage 2 Phase 2f-3) — the binary's "AOF".
//!
//! The segment checkpoint ([`crate::segment_rdb`]) is the binary's "RDB": a
//! periodic, atomic snapshot of the materialized index, tagged with the WAL
//! sequence `S` it is current as of. Between checkpoints, this file is the
//! durable record of every APPLIED `(seq, WalRecord)` — the exact Redis
//! RDB+AOF split. Recovery is:
//!
//! 1. **RDB** — reopen the newest segment checkpoint (engine seeded to seq `S`),
//! 2. **AOF** — replay every frame with `seq > S` into the engine (to seq `A`),
//! 3. **Broker** — tail the log from `A + 1`.
//!
//! Because the AOF is durable through `A`, the broker stream only needs retention
//! beyond `A`, not from seq 0 — which is the whole point: broker retention can be
//! TRIMMED instead of kept forever.
//!
//! ## Frame format
//!
//! Each appended record is one self-describing frame:
//!
//! ```text
//! [ seq : u64 LE ][ len : u32 LE ][ crc : u32 LE ][ payload : len bytes ]
//! ```
//!
//! - `seq`     — the global sequence the record was applied at (the order key).
//! - `len`     — payload length in bytes.
//! - `crc`     — `crc32(payload)` (crc32fast), checked on replay.
//! - `payload` — the [`WalRecord`] encoded with ciborium (a compact, stable CBOR
//!   form — the same codec the segment checkpoint sidecars and CBOR RDB use).
//!
//! The 16-byte fixed header lets replay detect a TORN TAIL without parsing the
//! payload: if fewer than 16 header bytes remain, or `len` overruns EOF, or the
//! crc mismatches, the frame is incomplete (a crash landed mid-append) — replay
//! stops cleanly at the last good frame, with no panic and no error. The byte
//! offset of that last good frame's end is recorded so the next
//! [`AofWriter::open`] can truncate the torn tail before appending.
//!
//! ## fsync policy
//!
//! Mirrors Redis `appendfsync`:
//!
//! - [`FsyncPolicy::EverySec`] — append writes to the OS buffer; a
//!   periodic [`AofWriter::maybe_sync`] (call-driven, off the apply hot path)
//!   fsyncs at most once per second. A crash loses at most ~1s of un-fsynced
//!   tail, which replay recovers as a torn tail (the frames are still in the OS
//!   page cache up to the crash point, and any partial frame is discarded).
//! - [`FsyncPolicy::Always`] (default) — fsync after every append.
//!
//! [`WalRecord`]: crate::ingest::domain::wal_record::WalRecord
//! [`AofWriter::open`]: aof_writer::AofWriter::open
//! [`AofWriter::maybe_sync`]: aof_writer::AofWriter::maybe_sync

pub(crate) mod aof_writer;
pub(crate) mod frame;
pub(crate) mod replay;

pub use storage_durable::{FramedLogTrimObserver, FsyncPolicy};

#[cfg(test)]
mod crux_recovery_tests;

#[cfg(test)]
mod tests;
