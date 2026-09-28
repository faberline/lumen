//! What a pending change costs and who holds it: the change budget and its
//! owners, reservations and charges, the cost models that price a record before
//! it applies, and the journal a checkpoint freezes. It also holds the WAL
//! record and `WalLog`, the log seam writes publish through.

pub(crate) mod change_admission;
pub(crate) mod change_budget;
pub(crate) mod change_journal;
pub(crate) mod change_memory_cost;
pub(crate) mod change_record_cost;
pub(crate) mod wal_log;
pub(crate) mod wal_record;
