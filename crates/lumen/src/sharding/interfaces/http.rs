//! The reshard admin verbs: batch apply, prune, bucket-scoped export, evict and
//! the write fence. Each requires `Role::Admin`.

pub(crate) mod fence;
pub(crate) mod reshard;
