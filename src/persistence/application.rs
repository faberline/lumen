//! Persistence's use case: the background merge, which compacts a field's
//! delta segments outside the checkpoint lock and publishes the result onto the
//! newest complete generation.

pub(crate) mod background_merge;
