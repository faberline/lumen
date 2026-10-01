//! The live shard map, and the fan-in and routed shard topology, read from the
//! environment the operator renders.

use anyhow::{Context, Result};

use crate::sharding::domain::virtual_bucket_shard_map::{
    VirtualBucketShardMap, DEFAULT_VIRTUAL_BUCKET_COUNT,
};

/// Live shard-routing map from the same ConfigMap env the operator renders
/// (`serving_configmap` in `operator/render.rs`): `SHARD_MAP_VERSION` /
/// `SHARD_MAP_ASSIGNMENTS` / `VIRTUAL_BUCKET_COUNT`. Mirrors
/// `ShardMapSpec`'s "empty assignments means balanced" contract exactly —
/// `SHARD_MAP_ASSIGNMENTS` unset (the ConfigMap key is only written once the
/// operator has committed a real split; see `serving_configmap`) falls back
/// to today's `bucket % shard_count` balanced default, so a pod started
/// before any reshard, or with no shard-map env at all (plain `lumen serve`
/// outside k8s), routes exactly as it always has (#1384 AC4). `shard_count`
/// is the caller's already-resolved physical shard count (`ClusterConfig::
/// shard_count` / `ServeArgs::shard_count`), not re-read from env here, so
/// this stays usable without the full raft `ClusterConfig` (e.g. the
/// non-raft `--search-shard-segment-dirs` read-shard-fan-in path).
pub fn shard_map_from_env(shard_count: u32) -> Result<VirtualBucketShardMap> {
    let version = match std::env::var("SHARD_MAP_VERSION") {
        Ok(raw) => raw
            .trim()
            .parse::<u64>()
            .with_context(|| format!("SHARD_MAP_VERSION={raw:?} is not a valid u64"))?,
        Err(_) => 0,
    };
    let virtual_bucket_count = match std::env::var("VIRTUAL_BUCKET_COUNT") {
        Ok(raw) => raw
            .trim()
            .parse::<u32>()
            .with_context(|| format!("VIRTUAL_BUCKET_COUNT={raw:?} is not a valid u32"))?,
        Err(_) => DEFAULT_VIRTUAL_BUCKET_COUNT,
    };
    match std::env::var("SHARD_MAP_ASSIGNMENTS") {
        Ok(raw) if !raw.trim().is_empty() => {
            let assignments = raw
                .split(',')
                .map(|s| {
                    s.trim().parse::<u32>().with_context(|| {
                        format!("SHARD_MAP_ASSIGNMENTS entry {s:?} is not a valid u32")
                    })
                })
                .collect::<Result<Vec<u32>>>()?;
            VirtualBucketShardMap::new(version, assignments, shard_count.max(1))
        }
        _ => VirtualBucketShardMap::balanced(version, virtual_bucket_count, shard_count.max(1)),
    }
}

/// Physical shard count the segment-dirs fan-in path
/// (`lumen serve --search-shard-segment-dirs a,b,c`) should route across.
///
/// `explicit` is `ServeArgs::shard_count`, a clap `Option<u32>` with
/// `env = "SHARD_COUNT"` and **no** `default_value_t` — that shape is
/// deliberate: it's the only way to tell "the operator/user actually set
/// `--shard-count`/`SHARD_COUNT`" (`Some`) from "nobody set anything"
/// (`None`) at the type level, since a `u32` field with a `default_value_t`
/// can't distinguish an explicit `--shard-count 1`/`SHARD_COUNT=1` from
/// clap's own default. When `explicit` is `Some`, it is honored as-is
/// (still fed through `shard_map_from_env` below, so `SHARD_MAP_*` env
/// overrides still apply on top). When it is `None`, the count defaults to
/// `loaded_dirs`, restoring `EngineShardSearch::new`'s original
/// derive-from-loaded-dirs behavior (#1398 R4: the prior call site fed
/// clap's old `default_value_t = 1` straight into `shard_map_from_env`,
/// so `a,b,c` with `SHARD_COUNT` unset silently built a 1-shard map and
/// searched only dir `a` on routed queries).
pub fn fan_in_shard_count(explicit: Option<u32>, loaded_dirs: usize) -> u32 {
    explicit.unwrap_or(loaded_dirs as u32)
}

/// Startup guard for the segment-dirs fan-in path: the shard map's declared
/// `physical_shard_count` must equal the number of loaded
/// `--search-shard-segment-dirs` roots, or routed queries would silently
/// reach only a subset of shards (#1398 R4 — e.g. an explicit
/// `SHARD_COUNT` that doesn't match the actual dir count). Names both
/// numbers in the error instead of continuing to serve with an
/// inconsistent map. The remediation names only `--shard-count` (#1442 R3):
/// this is the segment-dirs fan-in CLI path specifically, not the routed
/// k8s topology, and `--shard-count` is the flag every caller here actually
/// has in hand — naming the `SHARD_COUNT` env var too would suggest the
/// operator-managed routed topology's activation knob, which this code path
/// never reaches (see `routed_shard_count_from_env`).
pub fn check_fan_in_shard_count(map: &VirtualBucketShardMap, loaded_dirs: usize) -> Result<()> {
    let declared = map.physical_shard_count() as usize;
    if declared != loaded_dirs {
        anyhow::bail!(
            "shard map physical_shard_count ({declared}) does not match the number of \
             loaded --search-shard-segment-dirs ({loaded_dirs}); set --shard-count to \
             {loaded_dirs} or fix the loaded dirs"
        );
    }
    Ok(())
}

/// Derives `(StatefulSet name prefix, this pod's shard index)` from
/// `POD_NAME` + a caller-supplied `shard_count`, for the routed
/// (`shardCount > 1`, `replicasPerShard <= 1`) serving topology (#1398 R1).
///
/// `ClusterConfig::from_env` can't be used here: it requires the full raft
/// downward-API quartet (`REPLICAS_PER_SHARD`/`VOTER_COUNT`), which
/// `service_k8s::render::serving_statefulset` deliberately strips at
/// `replicasPerShard <= 1` — there is no raft peer identity to derive in
/// that topology, exactly the one routed mode targets. At
/// `replicasPerShard <= 1` the StatefulSet has exactly `shard_count` pods
/// (ordinals `0..shard_count`), so `shard_index = ordinal % shard_count ==
/// ordinal`; this mirrors `raft_runtime::cluster::ClusterDims::pod_ordinal`/
/// `shard_index`'s exact math (same `rsplit_once('-')` + `% shard_count`)
/// so the two derivations can't drift apart.
pub fn routed_pod_topology(shard_count: u32) -> Result<(String, u32)> {
    if shard_count == 0 {
        anyhow::bail!("shard_count must be > 0");
    }
    let pod_name = std::env::var("POD_NAME").context("POD_NAME not set")?;
    let (prefix, suffix) = pod_name
        .rsplit_once('-')
        .context("POD_NAME has no '-<ordinal>' suffix")?;
    let ordinal: u32 = suffix
        .parse()
        .with_context(|| format!("POD_NAME ordinal '{suffix}' is not a u32"))?;
    Ok((prefix.to_string(), ordinal % shard_count))
}

/// `Some(shard_count)` only when the operator-rendered `SHARD_COUNT` env is
/// present and describes more than one physical shard *and*
/// `REPLICAS_PER_SHARD` describes at most one replica per shard — the
/// routed serving topology's activation condition (#1398 R1, tightened by
/// #1442 R3). `SHARD_COUNT` unset or `<= 1` (including plain non-k8s `lumen
/// serve`) returns `None`, so single-shard deployments never build a
/// [`crate::sharding::infrastructure::routed_router::RoutedRouter`] at all (#1398 AC5: zero
/// forwarding overhead, not just a no-op branch through one).
///
/// `REPLICAS_PER_SHARD` unset is treated as `<= 1`
/// (`service_k8s::render::serving_statefulset` strips this env entirely at
/// `replicasPerShard <= 1`, so "absent" and "explicitly 1" are the same
/// topology) — a `SHARD_COUNT > 1` deployment that also has
/// `REPLICAS_PER_SHARD > 1` is the multi-replica-per-shard raft topology
/// (each shard is itself a raft group, not a single owning pod), not the
/// routed topology this activates: without this check, a multi-replica
/// pod's `POD_NAME` ordinal doesn't identify a shard 1:1
/// (`routed_pod_topology`'s `ordinal % shard_count` math assumes exactly
/// one pod per shard), so activating routing there would silently mis-map
/// pods to shards instead of failing fast.
pub fn routed_shard_count_from_env() -> Result<Option<u32>> {
    let replicas_per_shard: u32 = match std::env::var("REPLICAS_PER_SHARD") {
        Ok(raw) => raw
            .trim()
            .parse()
            .with_context(|| format!("REPLICAS_PER_SHARD={raw:?} is not a valid u32"))?,
        Err(_) => 1,
    };
    if replicas_per_shard > 1 {
        return Ok(None);
    }
    match std::env::var("SHARD_COUNT") {
        Ok(raw) => {
            let n: u32 = raw
                .trim()
                .parse()
                .with_context(|| format!("SHARD_COUNT={raw:?} is not a valid u32"))?;
            Ok((n > 1).then_some(n))
        }
        Err(_) => Ok(None),
    }
}

/// Combines the fan-in mutual-exclusion guard with
/// [`routed_shard_count_from_env`] into the single activation condition
/// `bin/lumen.rs` gates the routed block on (#1442 R3): routed cross-pod
/// routing activates only when the segment-dirs fan-in search backend did
/// NOT already activate (`search_shard_segment_dirs_empty`) AND the env
/// describes the routed topology. Pulled out as its own pure function so the
/// "a fan-in invocation must never reach the routed block at all" guarantee
/// is unit-testable without starting a real server — `SHARD_COUNT` isn't set
/// the same way on today's fan-in CLI path, but this makes the two paths
/// structurally mutually exclusive instead of relying on that coincidence.
pub fn routed_activation_shard_count(search_shard_segment_dirs_empty: bool) -> Result<Option<u32>> {
    if !search_shard_segment_dirs_empty {
        return Ok(None);
    }
    routed_shard_count_from_env()
}

#[cfg(test)]
pub(crate) mod tests;
