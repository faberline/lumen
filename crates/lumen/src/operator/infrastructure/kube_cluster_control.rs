//! `ClusterControl` over the Kubernetes API and each shard's admin HTTP.

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use async_trait::async_trait;
use kube::api::{Api, ApiResource, DynamicObject, Patch, PatchParams};
use kube::Client;
use serde_json::json;
use service_auth::k8s::{ProjectedToken, ProjectedTokenFile};

use crate::access::application::control_plane_token::control_plane_token_file;
use crate::operator::application::reshard_driver::cluster_control::ClusterControl;
use crate::operator::domain::lumen_spec::serving::AuthMode;
use crate::operator::domain::lumen_spec::Lumen;

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
        let ar = statefulset_api_resource();
        let api: Api<DynamicObject> = Api::namespaced_with(self.client.clone(), namespace, &ar);
        let ready = api
            .get_opt(name)
            .await
            .context("read serving StatefulSet")?
            .and_then(|o| o.data["status"]["readyReplicas"].as_i64())
            .unwrap_or(0);
        Ok(ready)
    }

    async fn trigger_rolling_restart(&self, namespace: &str, name: &str) -> Result<()> {
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
        api.patch(name, &PatchParams::default(), &Patch::Merge(&patch))
            .await
            .context("trigger serving StatefulSet rolling restart")?;
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
        let ar = statefulset_api_resource();
        let api: Api<DynamicObject> = Api::namespaced_with(self.client.clone(), namespace, &ar);
        let Some(sts) = api
            .get_opt(name)
            .await
            .context("read serving StatefulSet for topology convergence")?
        else {
            // No StatefulSet yet is not "converged" — the caller keeps
            // treating this as pending rather than assuming success.
            return Ok(false);
        };
        let status = &sts.data["status"];
        let ready_replicas = status["readyReplicas"].as_i64().unwrap_or(0);
        let updated_replicas = status["updatedReplicas"].as_i64().unwrap_or(0);
        // A rollout still in flight has distinct current/update revisions;
        // once it completes, k8s converges them onto the same value. Absent
        // fields (any StatefulSet old enough not to report them) fail this
        // check open on the safe side — never assumed identical.
        let current_revision = status["currentRevision"].as_str();
        let update_revision = status["updateRevision"].as_str();
        let revisions_converged =
            matches!((current_revision, update_revision), (Some(c), Some(u)) if c == u);
        Ok(revisions_converged
            && ready_replicas >= desired_replicas
            && updated_replicas >= desired_replicas)
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
            if crate::operator::application::reconcile::shard_usage::parse_metric(
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
