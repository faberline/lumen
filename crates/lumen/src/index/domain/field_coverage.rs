//! The fields a document wrote. The collection keeps one per doc id, so a
//! delete visits only the field indexes that hold the doc.

use std::collections::BTreeSet;

#[derive(Debug, Clone, Default)]
pub(in crate::index) struct FieldCoverage {
    pub(in crate::index) names: Vec<String>,
}

impl FieldCoverage {
    pub(in crate::index) fn insert(&mut self, field: String) -> bool {
        if self.contains(&field) {
            return false;
        }
        self.insert_absent(field);
        true
    }

    pub(in crate::index) fn insert_absent(&mut self, field: String) {
        debug_assert!(!self.contains(&field));
        self.names.push(field);
    }

    pub(in crate::index) fn contains(&self, field: &str) -> bool {
        self.names.iter().any(|seen| seen == field)
    }

    pub(in crate::index) fn remove(&mut self, field: &str) -> bool {
        let Some(pos) = self.names.iter().position(|name| name == field) else {
            return false;
        };
        self.names.swap_remove(pos);
        true
    }

    pub(in crate::index) fn is_empty(&self) -> bool {
        self.names.is_empty()
    }

    pub(in crate::index) fn iter(&self) -> impl Iterator<Item = &String> {
        self.names.iter()
    }

    pub(in crate::index) fn to_btree_set(&self) -> BTreeSet<String> {
        self.names.iter().cloned().collect()
    }

    pub(in crate::index) fn from_btree_set(set: BTreeSet<String>) -> Self {
        Self {
            names: set.into_iter().collect(),
        }
    }
}
