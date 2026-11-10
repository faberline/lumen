//! Optional diagnostic record for capacity refusals.

/// Records the numeric fields of a capacity refusal.
pub trait CapacityTrace: Send + Sync {
    fn enabled(&self) -> bool;
    fn refusal(
        &self,
        revision: u64,
        present: bool,
        requested: usize,
        used: usize,
        hard_limit: usize,
    );
}
