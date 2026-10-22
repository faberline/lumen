//! The one `ChangeBudget` every engine in the process shares.

use std::sync::{Arc, Mutex, OnceLock, Weak};

use crate::ingest::domain::change_budget::{ChangeBudget, Inner};

impl ChangeBudget {
    /// Process default. Every live caller gets the same 256 MiB accounting
    /// instance; tests use `with_hard_limit` instead.
    pub fn process_shared() -> Self {
        static REGISTRY: OnceLock<Mutex<Weak<Inner>>> = OnceLock::new();
        let registry = REGISTRY.get_or_init(|| Mutex::new(Weak::new()));
        let mut weak = registry.lock().expect("change budget registry poisoned");
        if let Some(inner) = weak.upgrade() {
            return Self(inner);
        }
        let budget = Self::new();
        *weak = Arc::downgrade(&budget.0);
        budget
    }
}
