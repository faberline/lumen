//! Lumen's `ManagedService` impl: what the shared controller renders, polls and
//! reports.

use kube::ResourceExt;
use serde_json::json;
use service_k8s::{ConditionFact, ManagedService, ReadinessTarget, ReadyFacts};

use crate::operator::application::reconcile::auth_delegator::apply_auth_delegator_binding;
use crate::operator::application::reconcile::plan_verdicts::{
    auth_delegation_error, check_peer_identity, peer_identity_error, AUTH_DELEGATION_CONTEXT_KEY,
    PEER_IDENTITY_CONTEXT_KEY,
};
use crate::operator::application::render;
use crate::operator::domain::lumen_spec::Lumen;
use crate::operator::infrastructure::kube_auth_delegator_control::KubeAuthDelegatorControl;

/// lumen's contribution to the shared operator.
impl ManagedService for Lumen {
    /// Server-side-apply field manager + leader-election Lease name.
    const MANAGER: &'static str = "lumen-operator";

    fn render(&self) -> Vec<serde_json::Value> {
        render::render(self)
    }

    /// #2678 R7: reject a spec that names two credential sources before any
    /// child object is applied.
    ///
    /// `render` is infallible by contract — six services share that signature —
    /// so the refusal lives here, the one hook on the reconcile path that can
    /// return an error. Failing here means the reconcile fails and the CR does
    /// not converge; the alternative, picking one source by precedence, leaves
    /// an operator reading the credentials they deployed while lumen serves the
    /// other ones. The CRD carries the same rule as CEL, so on a current
    /// cluster this never fires — it is the backstop for an older CRD or an
    /// object written before the rule existed.
    fn reconcile_plan(
        &self,
        client: kube::Client,
    ) -> impl std::future::Future<Output = anyhow::Result<service_k8s::service::ReconcilePlan>> + Send
    {
        let validation = self.spec.validate();
        // #2876: the serving ServiceAccount's `system:auth-delegator` binding
        // is cluster-scoped, so it cannot be one of `children` — those are all
        // applied into the CR's namespace. This hook is where it goes: it is
        // the one place on the reconcile path with a client, and applying the
        // grant *before* the workload means the pods do not start serving
        // ahead of their ability to authenticate anyone.
        let lumen = self.clone();
        async move {
            validation.map_err(|why| anyhow::anyhow!(why))?;
            let control = KubeAuthDelegatorControl {
                client: client.clone(),
            };
            let mut context = serde_json::Map::new();
            if let Some(error) = apply_auth_delegator_binding(&control, &lumen).await {
                context.insert(
                    AUTH_DELEGATION_CONTEXT_KEY.to_string(),
                    serde_json::Value::String(error),
                );
            }
            if let Some(error) = check_peer_identity(&lumen) {
                context.insert(
                    PEER_IDENTITY_CONTEXT_KEY.to_string(),
                    serde_json::Value::String(error),
                );
            }

            // A non-empty user selector with the default legacy machine type is
            // the Kubernetes-native compatibility path. It is self-contained:
            // do not read or resolve the GCE capacity catalog, and let render()
            // preserve the exact selector and tolerations supplied by the user.
            let native_placement = !lumen.spec.placement.node_selector.is_empty()
                && lumen.spec.placement.initial_machine_type
                    == crate::operator::domain::capacity::DEFAULT_INITIAL_MACHINE_TYPE;
            let children = if native_placement {
                render::render(&lumen)
            } else {
                // Empty selectors, tolerations-only placement, and an explicit
                // non-default machine type retain legacy catalog behavior.
                let catalog =
                    crate::operator::application::capacity_catalog::fetch_capacity_catalog(
                        &client,
                        crate::operator::domain::capacity::DEFAULT_CATALOG_NAMESPACE,
                        crate::operator::domain::capacity::DEFAULT_CATALOG_CONFIG_MAP_NAME,
                    )
                    .await
                    .map_err(|rejection| anyhow::anyhow!(rejection.message))?;
                let profile = crate::operator::domain::capacity::preflight::resolve_machine_type(
                    &lumen.spec.placement.initial_machine_type,
                    &catalog,
                )
                .map_err(|rejection| anyhow::anyhow!(rejection.message))?;
                render::render_with_profile(&lumen, &profile)
            };

            Ok(service_k8s::service::ReconcilePlan {
                children,
                context: serde_json::Value::Object(context),
            })
        }
    }

    fn prunes(&self) -> Vec<service_k8s::service::PruneTarget> {
        render::prunes(self)
    }

    fn readiness_targets(&self) -> Vec<ReadinessTarget> {
        // The serving fleet is always a StatefulSet (render::render), whether
        // or not raft consensus (`replicasPerShard > 1`) is active.
        let name = self.name_any();
        vec![ReadinessTarget {
            kind: "StatefulSet",
            name,
        }]
    }

    fn status_patch(&self, ready: &ReadyFacts) -> serde_json::Value {
        let obs = self.observe(ready);
        json!({ "status": {
            "phase": obs.phase,
            "observedGeneration": self.metadata.generation.unwrap_or(0),
            "servingReadyReplicas": obs.serving_ready,
            "desiredReplicas": obs.desired,
            "shardCount": self.spec.shard_count,
            "reshard": obs.reshard,
            "message": format!("{}/{} serving pods ready", obs.serving_ready, obs.desired),
        }})
    }

    /// #2601: the Kubernetes-convention convergence surface, derived from the
    /// same [`Observation`] [`Self::status_patch`] projects — so the flat
    /// fields and the conditions can never disagree about whether this CR has
    /// converged.
    ///
    /// Clock-free by construction: the caller stamps `lastTransitionTime`, which
    /// is what keeps this side of the projection deterministic (see the module
    /// doc's no-I/O `status_patch` contract).
    ///
    /// [`Observation`]: crate::operator::application::reconcile::observation::Observation
    fn conditions(&self, ready: &ReadyFacts, context: &serde_json::Value) -> Vec<ConditionFact> {
        let mut observation = self.observe(ready);
        // #2876 AC4: the plan hook's verdict on the cluster-scoped RBAC write
        // reaches the status here. It cannot come from `observe`, which is
        // I/O-free by contract and has no way to know what the apply did.
        observation.auth_delegation = auth_delegation_error(context);
        // #2890 R4: same channel, same reason — the peer-identity verdict is a
        // Secret read, and `observe` does no I/O.
        observation.peer_identity = peer_identity_error(context);
        observation.conditions()
    }

    /// #2601: the conditions already persisted on this object, so the shared
    /// projection can carry each `lastTransitionTime` forward. `Patch::Merge`
    /// replaces arrays wholesale, so nothing survives server-side unless it is
    /// read back off the watched object and re-sent.
    fn observed_conditions(&self) -> Vec<service_k8s::Condition> {
        self.status
            .as_ref()
            .map(|status| status.conditions.clone())
            .unwrap_or_default()
    }
}
