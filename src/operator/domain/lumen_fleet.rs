//! `LumenFleet` (`lumen.dev/v1alpha1`) — one cluster-scoped object, owned by
//! the platform team, that declares every data-plane namespace and the
//! settings each one gets.
//!
//! ```text
//! LumenFleet (cluster-scoped, applied once into the control plane)
//!   ├─ defaults: <a complete LumenSpec>            platform knowledge
//!   └─ instances[]: { namespace, name?, spec }     app-team knowledge
//!            │
//!            └─ materializes ──▶ Lumen/team-a, Lumen/team-b, …
//! ```
//!
//! ## Why cluster-scoped
//!
//! The fleet's whole purpose is to own objects in *other* namespaces, and a
//! namespaced owner cannot legally do that: Kubernetes rejects a cross-namespace
//! `ownerReference` with an `OwnerRefInvalidNamespace` event and its garbage
//! collector treats the owner as absent — which would delete the very
//! dependents the fleet just created. A cluster-scoped object owning namespaced
//! dependents is the supported direction, so the fleet is cluster-scoped. That
//! does not weaken "all configuration lives in the control plane": it is still
//! one object only the platform team has RBAC to touch.
//!
//! ## Why a generator, and not a replacement for `Lumen`
//!
//! Each entry still materializes a real `Lumen` CR rather than being reconciled
//! straight into StatefulSets. That keeps `kubectl get lumen -A`, the
//! per-instance `status.conditions[]` (#2601), independent failure domains, and
//! every existing operator behaviour exactly as they are; the fleet only
//! decides which `Lumen` objects should exist and what their specs say.
//!
//! ## What the fleet deliberately does not manage
//!
//! `spec.shardCount`, `spec.shardMap`, and `spec.reshardPolicy.workflow` are
//! written at runtime by the autonomous reshard driver
//! ([`super::reshard_driver`]). A declarative applier that listed them would
//! revert a completed split on its next pass — resetting `shardMap.version`
//! and re-triggering migration over data that has already moved. So the fleet
//! seeds them **once**, on the create that brings an instance into existence,
//! and never names them again: the steady-state apply omits those paths
//! entirely, which under server-side apply means the fleet never owns them and
//! therefore can never remove them.
//!
//! [`super::reshard_driver`]: crate::operator::application::reshard_driver

pub(crate) mod plan;

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::operator::domain::lumen_spec::LumenSpec;

/// Label every `Lumen` a fleet materializes carries, naming the fleet that
/// owns it. This — not an `ownerReference` — is the ownership link, on
/// purpose: an owner reference would make deleting the fleet cascade-delete
/// every data plane and its PVCs, so a typo in one cluster-scoped object would
/// destroy every tenant's data. Pruning is explicit and policy-driven instead
/// (see [`PrunePolicy`]).
pub const FLEET_LABEL: &str = "lumen.dev/fleet";

/// Server-side-apply field manager for the steady-state apply.
pub const FLEET_MANAGER: &str = "lumen-fleet";

/// Field manager for the one-time create. Deliberately *not* [`FLEET_MANAGER`]:
/// the initial topology fields it writes must stay owned by a manager that
/// never applies again, so the steady-state apply-set — which omits them —
/// cannot cause the API server to prune them.
pub const FLEET_SEED_MANAGER: &str = "lumen-fleet-seed";

/// `lumen.dev/v1alpha1` `LumenFleet`. Cluster-scoped — see the module docs.
#[derive(CustomResource, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "lumen.dev",
    version = "v1alpha1",
    kind = "LumenFleet",
    plural = "lumenfleets",
    shortname = "lfleet",
    status = "LumenFleetStatus",
    printcolumn = r#"{"name":"Desired","type":"integer","jsonPath":".status.desiredInstances"}"#,
    printcolumn = r#"{"name":"Applied","type":"integer","jsonPath":".status.appliedInstances"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct LumenFleetSpec {
    /// The complete `Lumen` spec every instance starts from — the platform
    /// team's knowledge: which image, which node pool, which StorageClass,
    /// which auth mode. Required and fully schema-validated, because a fleet
    /// that cannot produce one valid instance is not a fleet.
    pub defaults: LumenSpec,

    /// One entry per data-plane namespace.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub instances: Vec<FleetInstance>,

    /// What happens to an instance whose entry is removed from
    /// [`Self::instances`].
    #[serde(default)]
    pub prune_policy: PrunePolicy,
}

/// One data plane the fleet declares.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct FleetInstance {
    /// The namespace the `Lumen` is materialized into. The namespace must
    /// already exist: creating namespaces from a CR would make the operator's
    /// ClusterRole a namespace-creation privilege, and namespace lifecycle
    /// (quotas, labels, Workload Identity bindings) belongs to whatever
    /// provisions the cluster.
    pub namespace: String,

    /// The `Lumen` object's name. Defaults to the fleet's own name, so
    /// `kubectl get lumen -A` reads as one fleet spread across namespaces.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    /// A JSON Merge Patch (RFC 7386) applied over
    /// [`LumenFleetSpec::defaults`] — the app team's knowledge: this tenant's
    /// CPU/memory request, its disk size, the name of its credential source.
    /// A `null` value removes an inherited field.
    ///
    /// Free-form rather than an enumerated override struct so it covers every
    /// `Lumen` field, now and after the next one is added. Typos cannot hide
    /// in it: the merged document is deserialized into a `LumenSpec` and any
    /// key the spec does not have is reported as a rejected entry rather than
    /// silently dropped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(schema_with = "free_form_object")]
    pub spec: Option<Value>,
}

/// What happens to a materialized `Lumen` whose entry leaves the fleet.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub enum PrunePolicy {
    /// Leave it running and report it as orphaned. The default, because
    /// removing a line from a list is a plausible edit and deleting a search
    /// index with its PVCs is not a plausible consequence of one.
    #[default]
    Retain,
    /// Delete it, PVCs included via the instance's own garbage collection.
    /// Opt-in only.
    Delete,
}

/// Status subresource for a fleet.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LumenFleetStatus {
    /// The `.metadata.generation` this status reflects.
    #[serde(default)]
    pub observed_generation: i64,
    /// How many instances the spec declares.
    #[serde(default)]
    pub desired_instances: i32,
    /// How many were successfully created or applied this pass.
    #[serde(default)]
    pub applied_instances: i32,
    /// Per-entry outcome, so one bad entry is diagnosable without reading
    /// operator logs.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub entries: Vec<FleetEntryStatus>,
    /// Human-readable summary of the last pass.
    #[serde(default)]
    pub message: String,
}

/// One entry's outcome.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct FleetEntryStatus {
    pub namespace: String,
    pub name: String,
    /// `Created` | `Applied` | `Rejected` | `NamespaceMissing` | `NotAdopted`
    /// | `ApplyFailed` | `Orphaned` | `Pruned`.
    pub state: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub message: String,
}

/// A `Lumen` the fleet wants to exist, or the reason one entry cannot produce
/// one.
#[derive(Clone, Debug, PartialEq)]
pub struct PlannedInstance {
    pub namespace: String,
    pub name: String,
    pub outcome: PlanOutcome,
}

#[derive(Clone, Debug, PartialEq)]
pub enum PlanOutcome {
    /// The merged spec, as JSON. Kept as JSON rather than a `LumenSpec` so the
    /// apply body is exactly what was validated, with no second round-trip
    /// between the check and the write.
    Ready(Value),
    /// Why this entry produced nothing.
    Rejected(String),
}

/// The `x-kubernetes-preserve-unknown-fields` object schema for
/// [`FleetInstance::spec`]. A structural CRD schema has to say *something*
/// about every property, and "an object whose keys are validated later, by
/// deserializing the merge result into a `LumenSpec`" is what this expresses.
fn free_form_object(_: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    let mut schema = schemars::schema::SchemaObject {
        instance_type: Some(schemars::schema::InstanceType::Object.into()),
        ..Default::default()
    };
    schema.extensions.insert(
        "x-kubernetes-preserve-unknown-fields".to_string(),
        json!(true),
    );
    schemars::schema::Schema::Object(schema)
}

#[cfg(test)]
mod tests;
