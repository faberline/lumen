//! Reads counters from the Prometheus text of `GET /metrics`. The checkpoint phase uses it
//! to take the checkpoint and merge counts from the counter deltas of one process.

use anyhow::{bail, Result};

use crate::run::Observed;

pub(super) const CHECKPOINT_COUNTER: &str = "lumen_segment_checkpoint_completed_total";
pub(super) const MERGE_COUNTER: &str = "lumen_segment_merge_completed_total";

/// The sum of the values of every sample of the metric `name`. `None` when no sample
/// matches or a value does not parse.
pub(super) fn counter(text: &str, name: &str) -> Option<f64> {
    let mut sum = None;
    for line in text.lines() {
        if line.starts_with('#') {
            continue;
        }
        let is_sample = line
            .strip_prefix(name)
            .is_some_and(|rest| rest.starts_with(' ') || rest.starts_with('{'));
        if !is_sample {
            continue;
        }
        let value: f64 = line.split_whitespace().last()?.parse().ok()?;
        sum = Some(sum.unwrap_or(0.0) + value);
    }
    sum
}

/// The increase of the counter `name` between two scrapes of the same process.
pub(super) fn delta(before: &str, after: &str, name: &str) -> Result<u64> {
    let (Some(before_value), Some(after_value)) = (counter(before, name), counter(after, name))
    else {
        bail!("metric {name} is missing");
    };
    if after_value < before_value {
        bail!("metric {name} went down: {before_value} -> {after_value}");
    }
    Ok((after_value - before_value).round() as u64)
}

/// The checkpoint and merge counts observed between two scrapes.
pub(super) fn observe(before: &str, after: &str) -> Result<Observed> {
    Ok(Observed {
        checkpoint_count: delta(before, after, CHECKPOINT_COUNTER)?,
        merge_count: delta(before, after, MERGE_COUNTER)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counter_sums_all_samples_of_one_name() {
        let text = "# TYPE x counter\nx{a=\"1\"} 2\nx{a=\"2\"} 3\nxy 9\n";
        assert_eq!(counter(text, "x"), Some(5.0));
    }

    #[test]
    fn counter_is_none_for_an_absent_name() {
        assert_eq!(counter("y 1\n", "x"), None);
    }

    #[test]
    fn delta_of_one_checkpoint_is_one() {
        let before = "lumen_segment_checkpoint_completed_total 4\n";
        let after = "lumen_segment_checkpoint_completed_total 5\n";
        assert_eq!(delta(before, after, CHECKPOINT_COUNTER).unwrap(), 1);
    }

    #[test]
    fn delta_of_no_checkpoint_is_zero() {
        let text = "lumen_segment_checkpoint_completed_total 4\n";
        assert_eq!(delta(text, text, CHECKPOINT_COUNTER).unwrap(), 0);
    }

    #[test]
    fn delta_refuses_a_counter_that_went_down() {
        let before = "lumen_segment_checkpoint_completed_total 5\n";
        let after = "lumen_segment_checkpoint_completed_total 4\n";
        assert!(delta(before, after, CHECKPOINT_COUNTER).is_err());
    }

    #[test]
    fn delta_refuses_a_missing_metric() {
        let err = delta("", "", CHECKPOINT_COUNTER).expect_err("missing metric");
        assert!(format!("{err:#}").contains("is missing"), "{err:#}");
    }

    #[test]
    fn observe_reads_checkpoint_and_merge() {
        let before = "lumen_segment_checkpoint_completed_total 1\n\
                      lumen_segment_merge_completed_total 0\n";
        let after = "lumen_segment_checkpoint_completed_total 2\n\
                     lumen_segment_merge_completed_total 3\n";
        assert_eq!(
            observe(before, after).expect("observed"),
            Observed {
                checkpoint_count: 1,
                merge_count: 3,
            }
        );
    }
}
