//! Write-ahead log abstraction — the data-plane backbone.
//!
//! lumen's write path is "turn the database inside out": a write is published
//! to an ordered log and then folded into each serving node's materialized
//! index. The log may be in-process (`MemWal`) or Lumen-owned
//! primary/replica replication.
//!
//! This mirrors Redis's AOF (the op log) + replication stream, with the
//! "master" role dissolved into "the log owner":
//!
//! - **AOF**  → this log (a stream of [`WalRecord`]).
//! - **RDB**  → periodic snapshots to object storage (see `rdb`), tagged
//!   with the log sequence they correspond to, so a fresh node loads a
//!   baseline then tails the log from there.
//!
//! The local WAL backend implements [`WalLog`]:
//!
//! - [`MemWal`] — in-process, in-memory. Unit tests + the simplest
//!   single-node dev runs. Publish applies synchronously from the
//!   caller's perspective (the subscriber sees it immediately).
//!
//! The record payload reuses [`crate::shared_kernel::log_entry::RaftLogEntry`] — it
//! already enumerates every mutation 1:1 with an `Engine` method and is
//! the exact shape a replication record needs.
//!
//! [`WalRecord`]: crate::ingest::domain::wal_record::WalRecord
//! [`WalLog`]: crate::ingest::domain::wal_log::WalLog
//! [`MemWal`]: mem_wal::MemWal

pub(crate) mod borrowed_replace_scanner;
pub(crate) mod borrowed_replace_spool;
pub(crate) mod bounded_generic;
pub(crate) mod delivery;
pub(crate) mod encode;
pub(crate) mod fast_cursor;
pub(crate) mod fast_decode;
pub(crate) mod fast_index_scanner;
pub(crate) mod mem_wal;

const WAL_FAST_MAGIC: &[u8; 4] = b"LWAL";
/// Legacy fast-Index tag: `(external_id, field, value)` triples only, no
/// `IndexItem.version`. Every fast record written before #3952 used this tag.
/// The decode branch for it is frozen byte-for-byte so those records keep
/// replaying — `version` is reconstructed as `None`, matching what the writer
/// actually put on the wire at the time.
const WAL_FAST_INDEX: u8 = 1;
/// #3952: fast-Index tag carrying each item's optional external LWW
/// `version` (#184) on the wire.
///
/// Emitted ONLY when at least one item in the record actually carries a
/// `version`. A record with nothing to say beyond the legacy layout is still
/// written with [`WAL_FAST_INDEX`], byte-for-byte as before #3952, and that
/// asymmetry is deliberate: [`WAL_FORMAT_VERSION`] is still 1, so a pre-#3952
/// binary's version check waves every record through and then fails on the
/// tag. Emitting tag 2 unconditionally therefore made a single appended
/// `Index` record — even one from a deployment that never sets `version` —
/// enough to break replay on a rollback. Choosing the tag by content keeps the
/// downgrade path open for exactly the records an older binary could have
/// read correctly, and closes it, loudly, for the ones it could not.
///
/// [`WAL_FORMAT_VERSION`]: crate::ingest::domain::wal_record::WAL_FORMAT_VERSION
const WAL_FAST_INDEX_VERSIONED: u8 = 2;
/// #3992: `POST .../docs:truncate`.  This has no request body beyond its
/// collection id, so a small control record is both clearer and stricter than
/// a generic CBOR enum payload.  It is always paired with WAL v2.
const WAL_FAST_TRUNCATE_DOCS: u8 = 3;
/// #3994: `POST .../docs:unindex`.  This stays a separate v2 control tag so
/// a reader never mistakes an identifier batch for a collection-wide swap.
const WAL_FAST_UNINDEX_DOCS: u8 = 4;
const WAL_VALUE_STRING: u8 = 1;
const WAL_VALUE_NUMBER: u8 = 2;
const WAL_VALUE_VECTOR: u8 = 3;
const WAL_VALUE_STRING_LIST: u8 = 4;

#[cfg(test)]
mod tests;
