//! The config's validation: every name `render` derives must be a valid
//! Kubernetes name, and no required field may still hold a placeholder.

use std::collections::BTreeSet;

use anyhow::{bail, Result};

use crate::standalone::gke::Config;

pub(super) fn valid_dns(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 63
        && s.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && !s.starts_with('-')
        && !s.ends_with('-')
}
fn valid_node_pool(s: &str) -> bool {
    s.len() <= 40 && valid_dns(s) && s.as_bytes().first().is_some_and(u8::is_ascii_lowercase)
}
fn quantity(s: &str) -> bool {
    if s.is_empty()
        || s.len() > 32
        || s.trim() != s
        || matches!(s.as_bytes().first(), Some(b'+' | b'-'))
    {
        return false;
    }
    let suffix = [
        "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "n", "u", "m", "k", "K", "M", "G", "T", "P", "E",
    ]
    .into_iter()
    .find(|suffix| s.ends_with(suffix))
    .unwrap_or("");
    let number = s.strip_suffix(suffix).unwrap_or(s);
    if number.is_empty()
        || number.chars().filter(|ch| *ch == '.').count() > 1
        || !number.chars().all(|ch| ch.is_ascii_digit() || ch == '.')
        || !number.chars().any(|ch| ch.is_ascii_digit())
    {
        return false;
    }
    number
        .parse::<f64>()
        .map(|value| value.is_finite() && value > 0.0)
        .unwrap_or(false)
}
pub(super) fn valid_dns_subdomain(s: &str) -> bool {
    !s.is_empty() && s.len() <= 253 && s.split('.').all(valid_dns)
}
fn placeholder(s: &str) -> bool {
    let lower = s.to_ascii_lowercase();
    s.trim().is_empty()
        || lower == "required"
        || lower == "namespace/name"
        || lower.contains("replace_me")
        || lower.contains("placeholder")
        || s.contains('<')
        || s.contains('>')
        || s.contains('$')
}
pub(super) fn validate(c: &Config) -> Result<()> {
    if placeholder(&c.name)
        || placeholder(&c.namespace)
        || !valid_dns(&c.name)
        || !valid_dns(&c.namespace)
    {
        bail!("invalid DNS name")
    };
    let mut derived = vec![
        format!("{}-data", c.name),
        format!("{}-admin", c.name),
        format!("{}-client", c.name),
    ];
    let mut accounts = c.allowed_service_accounts.clone();
    accounts.sort();
    derived.extend(
        accounts
            .iter()
            .enumerate()
            .map(|(index, _)| format!("{}-client-{index:03}", c.name)),
    );
    for name in derived {
        if !valid_dns(&name) {
            bail!("invalid derived Kubernetes name")
        }
    }
    let binding = format!("lumen.{}.{}.auth-delegator", c.namespace, c.name);
    if !valid_dns_subdomain(&binding) {
        bail!("invalid derived ClusterRoleBinding name")
    }
    if placeholder(&c.node_pool) || !valid_node_pool(&c.node_pool) {
        bail!("invalid node pool")
    };
    for (n, v) in [("cpu", &c.cpu), ("memory", &c.memory)] {
        if placeholder(v) {
            bail!("{n} is required")
        };
    }
    if !quantity(&c.cpu) || !quantity(&c.memory) || !quantity(&c.storage_size) {
        bail!("invalid resource quantity")
    };
    if placeholder(&c.storage_class) || !valid_dns_subdomain(&c.storage_class) {
        bail!("invalid storage class")
    }
    if c.allowed_service_accounts.is_empty() {
        bail!("allowedServiceAccounts must not be empty")
    };
    let mut seen = BTreeSet::new();
    for x in &accounts {
        let p: Vec<_> = x.split('/').collect();
        if placeholder(x) || p.len() != 2 || !valid_dns(p[0]) || !valid_dns(p[1]) || !seen.insert(x)
        {
            bail!("invalid or duplicate service account")
        };
    }
    Ok(())
}
