//! The Direct `Lumen` ServiceAccount access list (`spec.access`).

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Exact ServiceAccounts allowed to call the data plane. An explicit empty
/// list denies all; an absent `access` keeps the current behavior.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AccessSpec {
    /// ServiceAccounts, each named by its exact namespace and name.
    #[schemars(schema_with = "subject_list_schema")]
    pub allowed_service_accounts: Vec<ServiceAccountSubject>,
}

/// Schema of the subject list, derived from a mirror that does not deny unknown
/// fields. `deny_unknown_fields` on the subject makes schemars emit
/// `additionalProperties: false` next to `properties`, which the API server
/// rejects in a structural CRD schema. Deserialization still denies unknown
/// fields through the real subject type.
fn subject_list_schema(gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    /// One ServiceAccount, named exactly. Wildcards, groups and raw usernames are
    /// not representable here.
    #[derive(JsonSchema)]
    struct SubjectSchema {
        /// Namespace of the ServiceAccount, an exact DNS-1123 label.
        #[allow(dead_code)]
        namespace: String,
        /// Name of the ServiceAccount, an exact DNS-1123 subdomain.
        #[allow(dead_code)]
        name: String,
    }
    <Vec<SubjectSchema>>::json_schema(gen)
}

/// One ServiceAccount, named exactly. Wildcards, groups and raw usernames are
/// not representable here.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ServiceAccountSubject {
    /// Namespace of the ServiceAccount, an exact DNS-1123 label.
    pub namespace: String,
    /// Name of the ServiceAccount, an exact DNS-1123 subdomain.
    pub name: String,
}

/// Exact-name check for one subject: both fields must be exact DNS-1123 names.
pub fn validate_subject(subject: &ServiceAccountSubject) -> Result<(), String> {
    for (field, value) in [("namespace", &subject.namespace), ("name", &subject.name)] {
        if !is_exact_name(value) {
            return Err(format!(
                "allowedServiceAccounts subject ({}/{}): {field} \"{value}\" must be an exact DNS-1123 name (no wildcard, ':' or '/')",
                subject.namespace, subject.name
            ));
        }
    }
    Ok(())
}

fn is_exact_name(value: &str) -> bool {
    let bytes = value.as_bytes();
    !value.is_empty()
        && value.len() <= 253
        && bytes
            .iter()
            .all(|b| matches!(b, b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.'))
        && matches!(bytes.first(), Some(b'a'..=b'z' | b'0'..=b'9'))
        && matches!(bytes.last(), Some(b'a'..=b'z' | b'0'..=b'9'))
}
