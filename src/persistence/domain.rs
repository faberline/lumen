//! The checkpoint catalog model: the generation manifest a checkpoint
//! publishes, and rebasing a manifest onto a background merge's verified
//! output.

pub(crate) mod generation_manifest;
pub(crate) mod merge_rebase;
