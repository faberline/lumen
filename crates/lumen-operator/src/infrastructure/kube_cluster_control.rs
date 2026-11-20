//! `ClusterControl` over the Kubernetes API and each shard's admin HTTP.

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use async_trait::async_trait;
use kube::api::{Api, ApiResource, DynamicObject, ListParams, Patch, PatchParams};
use kube::Client;
use serde_json::json;
use service_auth::k8s::{ProjectedToken, ProjectedTokenFile};

use crate::access::application::control_plane_token::control_plane_token_file;
use crate::application::reshard_driver::cluster_control::ClusterControl;
use crate::domain::lumen_spec::serving::AuthMode;
use crate::domain::lumen_spec::Lumen;

/// The client-facing port lumen's serving Service/StatefulSet expose. Kept
/// duplicated from `render::CLIENT_PORT`/`reconcile::CLIENT_PORT` the same
/// way those two already duplicate it from each other — the smallest private
/// constant beats a new `pub` cross-module symbol-table row.
const CLIENT_PORT: u16 = 7373;

/// Production [`ClusterControl`]: real `kube::Client` calls.
pub struct KubeClusterControl {
    client: Client,
    /// Where this process finds its own audience-bound ServiceAccount token
    /// (#2877). A field rather than a constant so a test can point it at a
    /// temp file and drive the missing/expired/wrong-audience branches; the
    /// default is the path the operator Deployment actually projects, so
    /// production never depends on the seam being set.
    token_file: ProjectedTokenFile,
}

impl KubeClusterControl {
    pub fn new(client: Client) -> Self {
        Self {
            client,
            token_file: control_plane_token_file(),
        }
    }

    /// Read the operator's credential from somewhere else. Test seam only —
    /// deliberately not an environment variable, because an env var is a
    /// production-reachable way to redirect a control-plane credential at a
    /// file an attacker chose.
    /// The serving StatefulSets of `base`: the single set `base`, or one set
    /// per shard `<base>-s<k>` (see `render::serving_set_names`).
    async fn serving_sets(&self, namespace: &str, base: &str) -> Result<Vec<DynamicObject>> {
        let ar = statefulset_api_resource();
        let api: Api<DynamicObject> = Api::namespaced_with(self.client.clone(), namespace, &ar);
        let sets = api
            .list(&ListParams::default())
            .await
            .context("list serving StatefulSets")?;
        Ok(sets
            .items
            .into_iter()
            .filter(|set| {
                set.metadata
                    .name
                    .as_deref()
                    .is_some_and(|candidate| is_serving_set(base, candidate))
            })
            .collect())
    }

    /// Read the operator's credential from somewhere else. Test seam only —
    /// deliberately not an environment variable, because an env var is a
    /// production-reachable way to redirect a control-plane credential at a
    /// file an attacker chose.
    pub fn with_token_file(mut self, token_file: ProjectedTokenFile) -> Self {
        self.token_file = token_file;
        self
    }
}

fn statefulset_api_resource() -> ApiResource {
    ApiResource {
        group: "apps".to_string(),
        version: "v1".to_string(),
        api_version: "apps/v1".to_string(),
        kind: "StatefulSet".to_string(),
        plural: "statefulsets".to_string(),
    }
}

/// True when `candidate` is the single serving set `base`, or one of its
/// per-shard sets `<base>-s<k>` with `k` one or more ASCII digits.
fn is_serving_set(base: &str, candidate: &str) -> bool {
    if candidate == base {
        return true;
    }
    match candidate
        .strip_prefix(base)
        .and_then(|rest| rest.strip_prefix("-s"))
    {
        Some(digits) => !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()),
        None => false,
    }
}

/// True when every serving set has converged on its update revision and the
/// ready and updated totals reach `desired_replicas`. `sets` are the `status`
/// objects. No set is never converged.
fn sets_converged(sets: &[serde_json::Value], desired_replicas: i64) -> bool {
    if sets.is_empty() {
        return false;
    }
    // A rollout still in flight has distinct current/update revisions; once
    // it completes, k8s converges them onto the same value. Absent fields
    // (any StatefulSet old enough not to report them) fail this check open on
    // the safe side — never assumed identical.
    let revisions_converged = sets.iter().all(|status| {
        matches!(
            (status["currentRevision"].as_str(), status["updateRevision"].as_str()),
            (Some(current), Some(update)) if current == update
        )
    });
    let ready: i64 = sets
        .iter()
        .map(|status| status["readyReplicas"].as_i64().unwrap_or(0))
        .sum();
    let updated: i64 = sets
        .iter()
        .map(|status| status["updatedReplicas"].as_i64().unwrap_or(0))
        .sum();
    revisions_converged && ready >= desired_replicas && updated >= desired_replicas
}

#[async_trait]
impl ClusterControl for KubeClusterControl {
    async fn patch_spec(
        &self,
        namespace: &str,
        name: &str,
        patch: serde_json::Value,
    ) -> Result<()> {
        let api: Api<Lumen> = Api::namespaced(self.client.clone(), namespace);
        api.patch(name, &PatchParams::default(), &Patch::Merge(&patch))
            .await
            .context("patch Lumen spec")?;
        Ok(())
    }

    async fn statefulset_ready_replicas(&self, namespace: &str, name: &str) -> Result<i64> {
        let sets = self.serving_sets(namespace, name).await?;
        Ok(sets
            .iter()
            .map(|set| set.data["status"]["readyReplicas"].as_i64().unwrap_or(0))
            .sum())
    }

    async fn trigger_rolling_restart(&self, namespace: &str, name: &str) -> Result<()> {
        let sets = self.serving_sets(namespace, name).await?;
        if sets.is_empty() {
            anyhow::bail!("no serving StatefulSet found for {name}");
        }
        let ar = statefulset_api_resource();
        let api: Api<DynamicObject> = Api::namespaced_with(self.client.clone(), namespace, &ar);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let patch = json!({
            "spec": {
                "template": {
                    "metadata": {
                        "annotations": {
                            "lumen.dev/reshard-restarted-at": now.to_string(),
                        }
                    }
                }
            }
        });
        for set in sets {
            let set_name = set.metadata.name.clone().unwrap_or_default();
            api.patch(&set_name, &PatchParams::default(), &Patch::Merge(&patch))
                .await
                .context("trigger serving StatefulSet rolling restart")?;
        }
        Ok(())
    }

    async fn admin_token(&self, _namespace: &str, lumen: &Lumen) -> Result<Option<ProjectedToken>> {
        if !matches!(lumen.spec.auth, AuthMode::Required) {
            return Ok(None);
        }
        // Read on every call, never held (#2877 R3). The kubelet replaces this
        // file partway through the token's lifetime; a value cached across
        // reshard ticks would work for minutes and then fail permanently, at
        // an hour nobody is watching.
        //
        // A failure here stops the reshard rather than degrading it: sending
        // the admin calls out unauthenticated against an instance that
        // requires an identity would turn a credential problem into a wall of
        // 401s from four different verbs. The error names the path and the
        // audience and never the material (#2877 R5).
        self.token_file
            .read()
            .map(Some)
            .with_context(|| "the operator cannot authenticate to this Lumen instance".to_string())
    }

    fn shard_base_url(&self, namespace: &str, name: &str, shard: u32) -> String {
        format!("http://{name}-{shard}.{name}-headless.{namespace}.svc.cluster.local:{CLIENT_PORT}")
    }

    async fn serving_topology_converged(
        &self,
        namespace: &str,
        name: &str,
        desired_replicas: i64,
    ) -> Result<bool> {
        // No StatefulSet yet is not "converged" — the caller keeps treating
        // this as pending rather than assuming success (`sets_converged`).
        let statuses: Vec<serde_json::Value> = self
            .serving_sets(namespace, name)
            .await
            .context("read serving StatefulSets for topology convergence")?
            .into_iter()
            .map(|set| set.data["status"].clone())
            .collect();
        Ok(sets_converged(&statuses, desired_replicas))
    }

    async fn serving_pods_report_map_version(
        &self,
        http: &reqwest::Client,
        namespace: &str,
        name: &str,
        shard_count: u32,
        map_version: u64,
    ) -> Result<bool> {
        for shard in 0..shard_count {
            let url = format!("{}/metrics", self.shard_base_url(namespace, name, shard));
            // An unreachable pod (mid-rollout, mid-restart) or a decode
            // failure is "not converged yet", not an error — the caller
            // just keeps the fence armed and retries next tick, exactly
            // like an unready StatefulSet replica.
            let Ok(resp) = http.get(&url).send().await else {
                return Ok(false);
            };
            if !resp.status().is_success() {
                return Ok(false);
            }
            let Ok(body) = resp.text().await else {
                return Ok(false);
            };
            if crate::application::reconcile::shard_usage::parse_metric(
                &body,
                "lumen_shard_map_version",
            ) != Some(map_version)
            {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{is_serving_set, sets_converged};

    #[test]
    fn is_serving_set_matches_base_and_shard_suffix() {
        assert!(is_serving_set("search", "search"));
        assert!(is_serving_set("search", "search-s0"));
        assert!(is_serving_set("search", "search-s12"));
        assert!(!is_serving_set("search", "search-s"));
        assert!(!is_serving_set("search", "search-sx"));
        assert!(!is_serving_set("search", "search-headless"));
        assert!(!is_serving_set("search", "search2"));
        assert!(!is_serving_set("search", "other-s0"));
    }

    #[test]
    fn sets_converged_needs_every_set_converged() {
        // Negative control: the second set is still rolling (revisions differ).
        let rolling = [
            json!({"readyReplicas": 1, "updatedReplicas": 1,
                   "currentRevision": "a", "updateRevision": "a"}),
            json!({"readyReplicas": 1, "updatedReplicas": 1,
                   "currentRevision": "a", "updateRevision": "b"}),
        ];
        assert!(!sets_converged(&rolling, 2));

        let converged = [
            json!({"readyReplicas": 1, "updatedReplicas": 1,
                   "currentRevision": "a", "updateRevision": "a"}),
            json!({"readyReplicas": 1, "updatedReplicas": 1,
                   "currentRevision": "b", "updateRevision": "b"}),
        ];
        assert!(sets_converged(&converged, 2));
    }

    #[test]
    fn sets_converged_is_false_without_sets() {
        assert!(!sets_converged(&[], 1));
    }

    #[test]
    fn sets_converged_needs_the_ready_total() {
        let short = [json!({"readyReplicas": 1, "updatedReplicas": 2,
                            "currentRevision": "a", "updateRevision": "a"})];
        assert!(!sets_converged(&short, 2));
    }
}
