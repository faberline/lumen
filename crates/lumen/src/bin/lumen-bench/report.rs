//! A cell's report: the percentiles `summarize` takes over the measured
//! samples, the budget it stamps, and the line `print_report` writes.

use anyhow::{bail, Result};

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

/// Fails when `report.p99_us` is over the budget, unless `gate` is false.
pub(super) fn check_budget(report: &BenchReport, gate: bool) -> Result<()> {
    if gate && report.p99_us > report.budget_us {
        bail!(
            "{} p99 {}us exceeds budget {}us",
            report.cell,
            report.p99_us,
            report.budget_us
        );
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn summary_sorts_samples_and_retains_depth_and_budget() {
        let report = summarize("sorted_page_deep", 20_000, 100, vec![9, 1, 4, 2]);
        assert_eq!(
            (report.min_us, report.p50_us, report.p99_us, report.max_us),
            (1, 4, 9, 9)
        );
        assert_eq!(
            (report.documents, report.pages_walked, report.measured_pages),
            (20_000, 100, 4)
        );
        assert_eq!(report.cell, "sorted_page_deep");
        assert_eq!(report.budget_us, SORTED_PAGE_BUDGET_US);
    }

    #[test]
    fn one_sample_and_tail_outlier_are_not_discarded() {
        let one = summarize("bool_filter", 1, 1, vec![77]);
        assert_eq!(
            (one.min_us, one.p50_us, one.p99_us, one.max_us),
            (77, 77, 77, 77)
        );
        let mut samples = vec![1; 99];
        samples.push(10_000);
        let tail = summarize("sorted_page_deep", 100, 50, samples);
        assert_eq!(tail.measured_pages, 100);
        assert_eq!(tail.max_us, 10_000);
        assert_eq!(tail.p99_us, 1);
    }

    #[test]
    fn check_budget_fails_over_budget_and_passes_when_not_gated() {
        let over = summarize("sorted_page_deep", 100, 50, vec![9_000]);
        let err = check_budget(&over, true).expect_err("9000us is over 5000us");
        assert_eq!(
            err.to_string(),
            "sorted_page_deep p99 9000us exceeds budget 5000us"
        );
        assert!(check_budget(&over, false).is_ok());

        let under = summarize("sorted_page_deep", 100, 50, vec![4_999]);
        assert!(check_budget(&under, true).is_ok());

        let equal = summarize("sorted_page_deep", 100, 50, vec![5_000]);
        assert!(check_budget(&equal, true).is_ok());
    }
}
