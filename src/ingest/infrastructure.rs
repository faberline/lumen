//! What the write path runs on: the process-wide change budget, the WAL's
//! codecs, delivery handles and in-process log, and the wire-cost bounds a
//! record is checked against before it decodes.

pub(crate) mod process_budget;
pub(crate) mod wal;
pub(crate) mod wire_cost;
