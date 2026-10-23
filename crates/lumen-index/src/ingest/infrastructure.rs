//! What the write path runs on: the process-wide change budget, the WAL's
//! codecs, delivery handles and in-process log, the wire-cost bounds a record
//! is checked against before it decodes, and the durable stage a committed
//! record is copied to before its memory is released.

pub mod committed_record_codec;
pub mod committed_stage;
pub mod process_budget;
pub mod wal;
pub mod wal_source_stage;
pub mod wire_cost;
