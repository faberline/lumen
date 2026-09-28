//! The CustomResourceDefinitions this operator owns, as YAML.

use crate::operator::domain::lumen_fleet::LumenFleet;
use crate::operator::domain::lumen_spec::Lumen;

/// The CEL operator no CRD rule here may use, kept as a written rule because
/// its absence is not self-evident from the schema (#2764, #2872).
///
/// A field rendered `nullable: true` reads like it needs an explicit
/// `!= null` guard. It does not, and adding one breaks the CRD outright:
/// Kubernetes types a nullable string as plain `string`, so `!= null` fails CEL
/// compilation at the API server ("found no matching overload for '_!=_'
/// applied to '(string, null)'") — while every local test still passes, because
/// they assert on YAML text and never on the compiled expression. The guard is
/// also unnecessary: Kubernetes prunes an explicitly-null field before CEL runs,
/// so `has()` already reports it absent. Presence tests plus `size()`, nothing
/// else.
///
/// Test-only because there is no rule left to apply it to. It stays at module
/// scope, next to [`lumen_crd_yaml`], so the next author to add one reads the
/// rule before writing the expression rather than after the cluster rejects it.
#[cfg(test)]
const FORBIDDEN_CEL_OPERATOR: &str = "!= null";

/// Every CustomResourceDefinition this operator owns, as one multi-document
/// YAML: the namespaced `Lumen` data plane, then the cluster-scoped
/// [`LumenFleet`] that declares which `Lumen` objects exist.
///
/// One document rather than two files because the two are not independently
/// installable: a fleet whose `Lumen` CRD is absent applies cleanly and then
/// fails every instance, which is a worse failure than not installing.
pub fn crd_yaml() -> String {
    format!("{}---\n{}", lumen_crd_yaml(), fleet_crd_yaml())
}

/// The `Lumen` CustomResourceDefinition as YAML, for `kubectl apply`.
pub fn lumen_crd_yaml() -> String {
    use kube::CustomResourceExt;
    let mut crd = serde_json::to_value(Lumen::crd()).expect("CRD serializes to JSON");
    service_k8s::crd::normalize_unsigned_integer_formats(&mut crd);
    serde_yaml::to_string(&crd).expect("CRD serializes")
}

/// The `LumenFleet` CustomResourceDefinition as YAML.
pub fn fleet_crd_yaml() -> String {
    use kube::CustomResourceExt;
    let mut crd = serde_json::to_value(LumenFleet::crd()).expect("CRD serializes to JSON");
    service_k8s::crd::normalize_unsigned_integer_formats(&mut crd);
    serde_yaml::to_string(&crd).expect("CRD serializes")
}

#[cfg(test)]
mod tests;
