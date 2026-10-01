//! A cell's report: the percentiles `summarize` takes over the measured
//! samples, the budget it stamps, and the line `print_report` writes.

pub(super) const SORTED_PAGE_BUDGET_US: u128 = 5_000;

#[derive(Debug)]
pub(super) struct BenchReport {
    cell: &'static str,
    documents: usize,
    pages_walked: usize,
    measured_pages: usize,
    p50_us: u128,
    pub(super) p99_us: u128,
    min_us: u128,
    max_us: u128,
    budget_us: u128,
}

pub(super) fn print_report(report: BenchReport) {
    println!(
        "cell={} documents={} pages_walked={} measured_pages={} min_us={} p50_us={} p99_us={} max_us={} budget_us={} status=pass",
        report.cell,
        report.documents,
        report.pages_walked,
        report.measured_pages,
        report.min_us,
        report.p50_us,
        report.p99_us,
        report.max_us,
        report.budget_us
    );
}

pub(super) fn summarize(
    cell: &'static str,
    documents: usize,
    pages_walked: usize,
    mut samples: Vec<u128>,
) -> BenchReport {
    samples.sort_unstable();
    let percentile = |q: f64| -> u128 {
        let idx = (((samples.len() - 1) as f64) * q).round() as usize;
        samples[idx]
    };
    BenchReport {
        cell,
        documents,
        pages_walked,
        measured_pages: samples.len(),
        min_us: samples[0],
        p50_us: percentile(0.50),
        p99_us: percentile(0.99),
        max_us: *samples.last().expect("non-empty samples"),
        budget_us: SORTED_PAGE_BUDGET_US,
    }
}
