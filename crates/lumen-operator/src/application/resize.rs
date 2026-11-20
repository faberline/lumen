//! `lumen k8s operator resize-storage` (#809) support module.
//!
//! StatefulSet `volumeClaimTemplates` are immutable after creation, so
//! bumping `spec.serving.raftStorage` on a live `Lumen` CR and letting the
//! operator reconcile does **not** resize anything — the rendered
//! StatefulSet's `apply` is a silent no-op for that field. The generic
//! detect-and-patch tool (quantity parsing, grow/no-op/shrink decision, PVC
//! listing + `StorageClass`-gated patch) lives in the shared
//! [`service_k8s::resize`] module (#970); this module is the thin Lumen-specific
//! adapter that fetches the CR's declared `spec.serving.raftStorage` and
//! scopes the PVC listing to the instance's `raft-<name>-<n>` volumes.

use anyhow::{Context, Result};
use kube::api::Api;

use crate::domain::lumen_spec::Lumen;

pub use service_k8s::resize::{decide, parse_storage_bytes, PvcResizeOutcome, ResizeAction};

/// List the live `raft-<name>-<n>` (or `raft-<name>-s<k>-<n>` per shard) PVCs
/// for the named `Lumen` instance,
/// compare each to the CR's declared `spec.serving.raftStorage`, and patch
/// `spec.resources.requests.storage` on PVCs that are safe to grow (bound
/// `StorageClass.allowVolumeExpansion == true`, `dry_run == false`).
/// No other PVC field is touched, and the CR itself is never mutated — the
/// deployer edits `spec.serving.raftStorage` separately, the same source of
/// truth `render()` already reads.
pub async fn resize_instance(
    client: kube::Client,
    namespace: &str,
    name: &str,
    dry_run: bool,
) -> Result<Vec<PvcResizeOutcome>> {
    let lumens: Api<Lumen> = Api::namespaced(client.clone(), namespace);
    let cr = lumens
        .get(name)
        .await
        .with_context(|| format!("get Lumen '{namespace}/{name}'"))?;
    let desired = cr.spec.serving.raft_storage.clone();

    let label_selector = format!("app.kubernetes.io/instance={name}");

    service_k8s::resize::resize_instance(
        client,
        namespace,
        &label_selector,
        |pvc_name| pvc_belongs_to_instance(pvc_name, name),
        |_pvc_name| desired.clone(),
        dry_run,
    )
    .await
}

/// True when `pvc_name` is a raft claim of the instance `name`: `raft-<name>-<n>`
/// for a single StatefulSet, or `raft-<name>-s<k>-<n>` for shard `k` when
/// `spec.shardCount > 1`. Any other shape, or another instance, is false.
pub(crate) fn pvc_belongs_to_instance(pvc_name: &str, name: &str) -> bool {
    let prefix = format!("raft-{name}-");
    let Some(rest) = pvc_name.strip_prefix(&prefix) else {
        return false;
    };
    let all_digits = |s: &str| !s.is_empty() && s.chars().all(|c| c.is_ascii_digit());
    if all_digits(rest) {
        return true;
    }
    match rest.strip_prefix('s').and_then(|r| r.split_once('-')) {
        Some((shard, ordinal)) => all_digits(shard) && all_digits(ordinal),
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::pvc_belongs_to_instance;

    #[test]
    fn pvc_matches_single_set_names() {
        assert!(pvc_belongs_to_instance("raft-lumen-0", "lumen"));
        assert!(pvc_belongs_to_instance("raft-lumen-12", "lumen"));
    }

    #[test]
    fn pvc_matches_two_shard_names() {
        assert!(pvc_belongs_to_instance("raft-lumen-s0-0", "lumen"));
        assert!(pvc_belongs_to_instance("raft-lumen-s1-2", "lumen"));
    }

    #[test]
    fn pvc_rejects_other_instance_and_shapes() {
        for pvc in [
            "raft-lumen-extra-0",
            "raft-lumen-s-0",
            "raft-lumen-s1-",
            "raft-lumen-",
            "raft-other-0",
            "data-lumen-0",
        ] {
            assert!(
                !pvc_belongs_to_instance(pvc, "lumen"),
                "{pvc} must not match"
            );
        }
    }
}
