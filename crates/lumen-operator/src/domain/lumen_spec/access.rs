//! Whole-runtime access: the exact ServiceAccounts allowed to call a Lumen.
//! Only exact names are accepted. There is no wildcard, group or username form.

use std::collections::BTreeSet;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Whole-runtime access. An empty list denies every client.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AccessSpec {
    #[serde(default)]
    pub allowed_service_accounts: Vec<ServiceAccountRef>,
}

/// One exact ServiceAccount, by namespace and name. No wildcard.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ServiceAccountRef {
    pub namespace: String,
    pub name: String,
}

impl AccessSpec {
    /// Check each entry in list order and return the first error.
    pub fn validate(&self) -> Result<(), String> {
        let mut seen = BTreeSet::new();
        for (index, account) in self.allowed_service_accounts.iter().enumerate() {
            if !is_dns_label(&account.namespace) {
                return Err(format!(
                    "access.allowedServiceAccounts[{index}].namespace (\"{}\") must be one exact namespace name (lowercase letters, digits and '-', at most 63 characters; no wildcard, group or username)",
                    account.namespace
                ));
            }
            if !is_dns_subdomain(&account.name) {
                return Err(format!(
                    "access.allowedServiceAccounts[{index}].name (\"{}\") must be one exact ServiceAccount name (lowercase letters, digits, '-' and '.', at most 253 characters; no wildcard, group or username)",
                    account.name
                ));
            }
            if !seen.insert((account.namespace.as_str(), account.name.as_str())) {
                return Err(format!(
                    "access.allowedServiceAccounts[{index}] (\"{}/{}\") is listed twice",
                    account.namespace, account.name
                ));
            }
        }
        Ok(())
    }
}

/// A DNS-1123 label: 1 to 63 characters of `a-z0-9-`, not starting or ending
/// with `-`.
fn is_dns_label(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 63
        && bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-')
        && bytes.first() != Some(&b'-')
        && bytes.last() != Some(&b'-')
}

/// A DNS-1123 subdomain: at most 253 characters, dot-separated DNS labels.
fn is_dns_subdomain(value: &str) -> bool {
    value.len() <= 253 && value.split('.').all(is_dns_label)
}
