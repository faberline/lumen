//! Persistence: the on-disk forms of an engine's state. The columnar segment
//! files and the readers over them, the checkpoint generations that publish
//! them, the AOF, restore, background merge and compaction, and capacity.

pub(crate) mod infrastructure;
