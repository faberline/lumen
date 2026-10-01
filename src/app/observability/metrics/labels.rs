//! The label values of the labelled families: MergeStep for a merge job's
//! `phase`, ApplyKind for an applied entry's `kind`, with the items each apply
//! charges, and CoordinatorStage for a coordinator request's `stage`.

#[cfg(doc)]
use crate::app::observability::metrics::Metrics;

/// Number of [`MergeStep`] variants — the row count of the
/// `lumen_segment_merge_phase_seconds{phase=...}` family.
pub const MERGE_STEP_COUNT: usize = 10;

/// One timed step of a single background segment merge job.
///
/// A merge job's cost is not one number: the payload work
/// (`Compact`) is proportional to the fields it drains, while the
/// generation-wide hard-link, layout-validation, inheritance, and capture
/// steps are proportional to the *whole* root — every file of every
/// collection, drained or idle. Splitting the job into named steps is what
/// lets a production `/metrics` scrape say which of the two dominates,
/// instead of leaving that to a guess about a job that took seconds.
///
/// Ordering is the order the steps run inside
/// `persistence::application::background_merge::publish` (`SegmentRdbStore::merge_one`), and
/// [`MergeStep::Total`] spans the whole job so
/// `Total - sum(others)` is the unattributed remainder.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum MergeStep {
    /// `engine.capture_background_merge(identities(&prior))` taken under
    /// the first `save_gate` hold, before any file work.
    CaptureBefore,
    /// `link_collection(source -> scratch)`: hard-links the files of the one
    /// collection this job compacts into its job-private scratch stage. The
    /// count is proportional to the job's payload, not to the root.
    LinkScratch,
    /// `compact_staged_delta_windows`: `compact_one_staged_field` for the one
    /// field this job compacts. The only payload-proportional step.
    Compact,
    /// `link_collections_with_paths(latest -> new generation)`: the second
    /// whole-root hard-link pass, taken while `save_gate` is held.
    LinkGeneration,
    /// `validate_generation_layout_with_prior`.
    ValidateLayout,
    /// The `inherit_current_file` loop over every inherited file.
    InheritFiles,
    /// `telemetry::pending_deltas` over the published catalog.
    PendingDeltas,
    /// The publication-guard `capture_background_merge` re-capture.
    CapturePublish,
    /// `write_generation_manifest` for the rebased catalog.
    ManifestWrite,
    /// The whole job, from the first `save_gate` acquisition to the last
    /// `drop(guard)`.
    Total,
}

impl MergeStep {
    /// Every step, in run order. Rendering iterates this, so the published
    /// row set is fixed and a scrape never silently loses a phase.
    pub const ALL: [MergeStep; MERGE_STEP_COUNT] = [
        MergeStep::CaptureBefore,
        MergeStep::LinkScratch,
        MergeStep::Compact,
        MergeStep::LinkGeneration,
        MergeStep::ValidateLayout,
        MergeStep::InheritFiles,
        MergeStep::PendingDeltas,
        MergeStep::CapturePublish,
        MergeStep::ManifestWrite,
        MergeStep::Total,
    ];

    /// The `phase` label value published for this step.
    pub const fn name(self) -> &'static str {
        match self {
            MergeStep::CaptureBefore => "capture_before",
            MergeStep::LinkScratch => "link_scratch",
            MergeStep::Compact => "compact",
            MergeStep::LinkGeneration => "link_generation",
            MergeStep::ValidateLayout => "validate_layout",
            MergeStep::InheritFiles => "inherit_files",
            MergeStep::PendingDeltas => "pending_deltas",
            MergeStep::CapturePublish => "capture_publish",
            MergeStep::ManifestWrite => "manifest_write",
            MergeStep::Total => "total",
        }
    }

    /// This step's slot in the per-step atomic arrays.
    pub const fn index(self) -> usize {
        self as usize
    }
}

/// Number of [`ApplyKind`] variants — the row count of the
/// `lumen_coordinator_apply_seconds{kind=...}` /
/// `lumen_coordinator_apply_items_total{kind=...}` families.
pub const APPLY_KIND_COUNT: usize = 9;

/// #4326: the `kind` label of one applied [`crate::shared_kernel::log_entry::RaftLogEntry`]
/// — every variant maps 1:1, see [`ApplyKind::from_entry`]. A plain enum
/// (not the borrowed `RaftLogEntry` itself) so
/// [`Metrics::observe_coordinator_apply`] can index its per-kind atomic
/// arrays without re-matching after the entry has already been consumed by
/// `Engine::apply_prepared_raft_entry`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyKind {
    CreateCollection,
    Index,
    ReplaceDocs,
    TruncateDocs,
    UnindexDocs,
    Delete,
    DropCollection,
    AddField,
    DropField,
}

/// One bounded segment of an admitted coordinator request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoordinatorStage {
    AdmissionToMutationGate,
    PublishToApplyStart,
    ApplyToWaiter,
}

impl CoordinatorStage {
    pub(super) const ALL: [Self; 3] = [
        Self::AdmissionToMutationGate,
        Self::PublishToApplyStart,
        Self::ApplyToWaiter,
    ];

    pub(super) const fn index(self) -> usize {
        self as usize
    }

    pub(super) const fn label(self) -> &'static str {
        match self {
            Self::AdmissionToMutationGate => "admission_to_mutation_gate",
            Self::PublishToApplyStart => "publish_to_apply_start",
            Self::ApplyToWaiter => "apply_to_waiter",
        }
    }
}

impl ApplyKind {
    /// Every kind, in `RaftLogEntry` declaration order. Rendering iterates
    /// this, so the published row set is fixed and a scrape never silently
    /// loses a kind that has not been observed yet.
    pub const ALL: [ApplyKind; APPLY_KIND_COUNT] = [
        ApplyKind::CreateCollection,
        ApplyKind::Index,
        ApplyKind::ReplaceDocs,
        ApplyKind::TruncateDocs,
        ApplyKind::UnindexDocs,
        ApplyKind::Delete,
        ApplyKind::DropCollection,
        ApplyKind::AddField,
        ApplyKind::DropField,
    ];

    /// The `RaftLogEntry` variant name in lowercase snake_case, matching the
    /// wire vocabulary an operator already knows from the API (`index`,
    /// `replace`, `unindex`, ...) rather than the Rust variant spelling.
    pub const fn label(self) -> &'static str {
        match self {
            ApplyKind::CreateCollection => "create_collection",
            ApplyKind::Index => "index",
            ApplyKind::ReplaceDocs => "replace",
            ApplyKind::TruncateDocs => "truncate_docs",
            ApplyKind::UnindexDocs => "unindex",
            ApplyKind::Delete => "delete",
            ApplyKind::DropCollection => "drop_collection",
            ApplyKind::AddField => "add_field",
            ApplyKind::DropField => "drop_field",
        }
    }

    /// This kind's slot in the per-kind atomic arrays.
    pub(super) const fn index(self) -> usize {
        self as usize
    }

    /// Classify one committed mutation for `lumen_coordinator_apply_seconds`
    /// / `lumen_coordinator_apply_items_total`.
    pub const fn from_entry(entry: &crate::shared_kernel::log_entry::RaftLogEntry) -> ApplyKind {
        use crate::shared_kernel::log_entry::RaftLogEntry;
        match entry {
            RaftLogEntry::CreateCollection { .. } => ApplyKind::CreateCollection,
            RaftLogEntry::Index { .. } => ApplyKind::Index,
            RaftLogEntry::ReplaceDocs { .. } => ApplyKind::ReplaceDocs,
            RaftLogEntry::TruncateDocs { .. } => ApplyKind::TruncateDocs,
            RaftLogEntry::UnindexDocs { .. } => ApplyKind::UnindexDocs,
            RaftLogEntry::Delete { .. } => ApplyKind::Delete,
            RaftLogEntry::DropCollection { .. } => ApplyKind::DropCollection,
            RaftLogEntry::AddField { .. } => ApplyKind::AddField,
            RaftLogEntry::DropField { .. } => ApplyKind::DropField,
        }
    }
}

/// #4326: the `kind` label for one applied `RaftLogEntry` — see
/// [`ApplyKind::from_entry`]. Kept as a standalone `&'static str` helper
/// (in addition to [`ApplyKind`] itself) for call sites that only need the
/// rendered label, not the enum.
pub fn kind_label(entry: &crate::shared_kernel::log_entry::RaftLogEntry) -> &'static str {
    ApplyKind::from_entry(entry).label()
}

/// #4326: items charged to one `lumen_coordinator_apply_items_total{kind}`
/// observation — docs for `index`/`replace`, external ids for `unindex`,
/// `1` for every single-record kind (`create_collection`, `truncate_docs`,
/// `delete`, `drop_collection`, `add_field`, `drop_field`).
pub fn apply_item_count(entry: &crate::shared_kernel::log_entry::RaftLogEntry) -> u64 {
    use crate::shared_kernel::log_entry::RaftLogEntry;
    match entry {
        RaftLogEntry::Index { req, .. } => req.items.len() as u64,
        RaftLogEntry::ReplaceDocs { req, .. } => req.docs.len() as u64,
        RaftLogEntry::UnindexDocs { req, .. } => req.external_ids.len() as u64,
        _ => 1,
    }
}

#[cfg(test)]
mod tests;
