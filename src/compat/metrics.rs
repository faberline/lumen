//! `crate::metrics` before the DDD split. Re-exports its public items from
//! their new homes so callers outside the crate keep compiling.

pub use crate::app::observability::metrics::labels::{
    apply_item_count, kind_label, ApplyKind, CoordinatorStage, MergeStep, APPLY_KIND_COUNT,
    MERGE_STEP_COUNT,
};
pub use crate::app::observability::metrics::Metrics;
