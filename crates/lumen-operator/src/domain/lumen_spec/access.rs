//! The Direct `Lumen` ServiceAccount access list (`spec.access`).

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Exact ServiceAccounts allowed to call the data plane. An explicit empty
/// list denies all; an absent `access` keeps the current behavior.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AccessSpec {
    /// ServiceAccounts, each named by its exact namespace and name.
    pub allowed_service_accounts: Vec<ServiceAccountSubject>,
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
