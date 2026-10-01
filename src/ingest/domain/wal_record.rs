//! One record of the write-ahead log: the mutation it carries and the wire
//! format version it was written under.

use serde::{Deserialize, Serialize};

use crate::shared_kernel::log_entry::RaftLogEntry;

/// Legacy on-the-wire record format version.  Existing writable operations
/// keep emitting this byte so a 0.4.30 reader sees their exact old wire form.
pub const WAL_FORMAT_VERSION: u8 = 1;

/// #3992 control-record version.  A pre-0.4.31 reader rejects this byte before
/// it attempts to decode the new command tag, making the downgrade boundary
/// explicit rather than reporting an opaque unknown-enum error.
pub const WAL_CONTROL_FORMAT_VERSION: u8 = 2;

/// One durable, ordered mutation in the log. The sequence number is
/// **not** part of the record — it is assigned by the log on publish
/// and delivered alongside the record on subscribe (`MemWal` uses the append
/// index; external-log or primary/replica backends own sequence assignment).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalRecord {
    pub version: u8,
    pub entry: RaftLogEntry,
}

impl WalRecord {
    pub fn new(entry: RaftLogEntry) -> Self {
        Self {
            version: match entry {
                RaftLogEntry::TruncateDocs { .. } | RaftLogEntry::UnindexDocs { .. } => {
                    WAL_CONTROL_FORMAT_VERSION
                }
                _ => WAL_FORMAT_VERSION,
            },
            entry,
        }
    }
}
