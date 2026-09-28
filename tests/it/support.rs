//! Helpers shared by the `it` cases. Each file is declared once here, so its
//! statics (such as the serve-budget port handoff lock) have one instance for
//! the whole binary.

pub mod indexing_durable_catalog_fixture;
pub mod indexing_durable_fixture;
pub mod perf_cell_receipt;
pub mod perf_workload_ledger;
pub mod serve_budget_support;

/// The libtest name of one case in this binary, for cases that rerun
/// themselves in a child process with `<name> --exact`. `module_path` is the
/// caller's `module_path!()`; `case` is the test path relative to the caller's
/// top-level case module, as it was when each case was its own binary.
pub fn exact_case(module_path: &str, case: &str) -> String {
    let module = module_path
        .split("::")
        .nth(1)
        .expect("exact_case is called from inside a case module");
    format!("{module}::{case}")
}
