//! What an Engine holds behind its lock: the collections by name, the next
//! collection generation it hands out, and the checkpoint namespace its
//! segments are written under.

use std::collections::BTreeMap;

use anyhow::{anyhow, Result};

use crate::index::domain::collection::Collection;

#[derive(Debug, Default)]
pub(crate) struct EngineState {
    pub(crate) collections: BTreeMap<String, Collection>,
    pub(crate) next_collection_generation: u64,
    pub(crate) checkpoint_namespace: Option<std::path::PathBuf>,
}

impl EngineState {
    pub(crate) fn allocate_collection_generation(&mut self) -> Result<u64> {
        let generation = self.next_collection_generation.max(1);
        self.next_collection_generation = generation
            .checked_add(1)
            .ok_or_else(|| anyhow!("collection generation exhausted"))?;
        Ok(generation)
    }
}
