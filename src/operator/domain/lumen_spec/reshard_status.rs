//! The reshard status a spec reports: its policy thresholds, and with live
//! usage, which of them are crossed.

use std::collections::BTreeMap;

use crate::operator::domain::lumen_spec::status::LumenReshardStatus;
use crate::operator::domain::lumen_spec::LumenSpec;

impl LumenSpec {
    pub fn reshard_status(&self) -> LumenReshardStatus {
        let policy = &self.reshard_policy;
        let recommendation_only = policy.max_shard_bytes.is_none();
        let mut blocking_conditions = Vec::new();
        if recommendation_only {
            blocking_conditions.push("maxShardBytesUnset".to_string());
        }
        if policy.max_shards.is_some_and(|max| self.shard_count >= max) {
            blocking_conditions.push("maxShardsReached".to_string());
        }
        let target = policy.workflow.target_shard_count.or_else(|| {
            policy.max_shards.map(|max| {
                if self.shard_count < max {
                    self.shard_count + 1
                } else {
                    self.shard_count
                }
            })
        });
        let message = if recommendation_only {
            "maxShardBytes unset; operator reports recommendations only and will not auto-split"
                .to_string()
        } else if policy.max_shards.is_some_and(|max| self.shard_count >= max) {
            "maxShards reached; reshard workflow requires an explicit higher limit".to_string()
        } else {
            format!(
                "prepare at {}%, start at {}%, urgent at {}%",
                policy.prepare_at_percent,
                policy.start_at_percent.unwrap_or(policy.prepare_at_percent),
                policy.urgent_at_percent
            )
        };
        LumenReshardStatus {
            phase: policy.workflow.phase.as_str().to_string(),
            recommendation_only,
            progress_percent: policy.workflow.phase.progress_percent(),
            target_shard_count: target,
            migration_bytes_per_sec: policy.migration_bytes_per_sec,
            max_observed_percent: None,
            usage_measured_at_map_version: None,
            blocking_conditions,
            message,
            convergence_remediation_restart_count: policy
                .workflow
                .convergence_remediation_restart_count,
            convergence_remediation_restarted_at: policy
                .workflow
                .convergence_remediation_restarted_at,
        }
    }

    /// Live-usage-aware reshard status (#1319 R1): layers [`Self::reshard_status`]
    /// with real per-shard byte measurements instead of only formatting the
    /// configured percentages into a message. `shard_usage_bytes` maps
    /// `shard_index -> observed bytes`; `measured_at_map_version` is the
    /// `spec.shardMap.version` that was live on this CR when that usage was
    /// scraped (see [`super::reconcile`]'s pod-`/metrics` measurement loop,
    /// the function's only caller).
    ///
    /// Reports whether the busiest shard has crossed `prepareAtPercent` /
    /// `urgentAtPercent` of `maxShardBytes` — but only once
    /// `measured_at_map_version` matches this CR's *current*
    /// `shard_map.version` (#1386 R1/R2). A split's cutover bumps
    /// `shard_map.version` in the very same patch that follows evicting
    /// moved documents from their old shard, so a mismatch means the
    /// measurement predates that cutover and still reflects pre-eviction
    /// usage — most visibly, immediately after a split reaches `Complete`,
    /// when the shard-usage cache has not yet re-scraped (the exact live
    /// #1384 bug: a stale post-migration reading re-crossed the threshold
    /// and cascaded straight into an unwarranted second split). While
    /// stale, this reports `"usageStalePostCutover"` instead of a
    /// threshold-crossed condition, holding until a fresh post-cutover
    /// scrape lands; a genuinely still-hot shard can still trigger the next
    /// split, but only once the measurement itself is proven post-cutover.
    ///
    /// This function itself does **not** drive `workflow.phase` or move any
    /// data — it only computes the status this tick. The autonomous split
    /// executor (#1319 R2, #1381: computing a target topology, invoking
    /// [`crate::reshard::bucket_moves`] / [`crate::reshard::
    /// snapshot_reshard_batches`], and updating `shardMap.assignments`) is a
    /// separate loop ([`crate::operator::reshard_driver::
    /// should_start_split`] / `drive_tick`) that reads the
    /// `blockingConditions` this function writes and acts on them
    /// independently.
    ///
    /// [`super::reconcile`]: crate::operator::application::reconcile
    pub fn reshard_status_with_usage(
        &self,
        shard_usage_bytes: &BTreeMap<u32, u64>,
        measured_at_map_version: u64,
    ) -> LumenReshardStatus {
        let mut status = self.reshard_status();
        let Some(max_shard_bytes) = self.reshard_policy.max_shard_bytes else {
            // recommendation-only: nothing to compare usage against.
            return status;
        };
        if max_shard_bytes == 0 {
            return status;
        }
        let Some((&busiest_shard, &busiest_bytes)) =
            shard_usage_bytes.iter().max_by_key(|(_, bytes)| **bytes)
        else {
            // Usage not measured yet this tick; keep the policy-only status.
            return status;
        };

        let percent = ((busiest_bytes as f64 / max_shard_bytes as f64) * 100.0)
            .round()
            .clamp(0.0, 255.0) as u8;
        status.max_observed_percent = Some(percent);
        status.usage_measured_at_map_version = Some(measured_at_map_version);

        if measured_at_map_version != self.shard_map.version {
            // #1386 R1: this measurement predates the most recent cutover
            // (or, less likely, raced ahead of a status write that hasn't
            // observed it yet) — never let a stale reading drive
            // `should_start_split`, no matter how urgent the stale
            // percentage looks.
            status
                .blocking_conditions
                .push("usageStalePostCutover".to_string());
            status.message = format!(
                "usage measured at shardMap version {measured_at_map_version}, but the CR is \
                 now at version {}; holding for a fresh post-cutover measurement before \
                 evaluating the next split",
                self.shard_map.version
            );
            return status;
        }

        let policy = &self.reshard_policy;
        let prepare_at = policy.start_at_percent.unwrap_or(policy.prepare_at_percent);
        status.message = if percent >= policy.urgent_at_percent {
            status
                .blocking_conditions
                .push("urgentThresholdCrossed".to_string());
            format!(
                "urgent threshold crossed: shard {busiest_shard} at {percent}% of maxShardBytes \
                 (urgent {}%)",
                policy.urgent_at_percent
            )
        } else if percent >= prepare_at {
            status
                .blocking_conditions
                .push("prepareThresholdCrossed".to_string());
            format!(
                "prepare threshold crossed: shard {busiest_shard} at {percent}% of \
                 maxShardBytes (prepare {prepare_at}%)"
            )
        } else {
            format!(
                "shard {busiest_shard} at {percent}% of maxShardBytes; below prepare \
                 threshold ({prepare_at}%)"
            )
        };
        status
    }
}
