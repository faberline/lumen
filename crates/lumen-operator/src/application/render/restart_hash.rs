//! Hash of the restart-required serving settings (#42, first slice).

use std::collections::BTreeMap;

use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::application::render::serving_config::{serving_configmap, serving_env};
use crate::application::render::{ctx, instance, namespace};
use crate::domain::lumen_spec::Lumen;

/// Canonical text the hash is computed over.
///
/// Lines, joined with `\n`:
/// - `env:<json>` for each serving env entry, sorted by name then by full JSON;
/// - `cm:<key>=<value>` for each serving ConfigMap data key, sorted by key;
/// - `ver:<name>=<resourceVersion>` for each object version, in map order.
///
/// Only names, keys, versions and non-secret settings go in. A Secret value is
/// never read, so it can never reach the hash input.
pub(super) fn canonical_input(lumen: &Lumen, object_versions: &BTreeMap<String, String>) -> String {
    let mut lines: Vec<String> = Vec::new();

    let mut env: Vec<(String, String)> = serving_env(lumen)
        .into_iter()
        .map(|entry| {
            let name = entry["name"].as_str().unwrap_or_default().to_string();
            (name, entry.to_string())
        })
        .collect();
    env.sort();
    lines.extend(env.into_iter().map(|(_, json)| format!("env:{json}")));

    let name = instance(lumen);
    let ns = namespace(lumen);
    let cx = ctx(lumen, &name, &ns);
    let configmap = serving_configmap(lumen, &cx);
    if let Some(data) = configmap["data"].as_object() {
        let sorted: BTreeMap<&str, String> = data
            .iter()
            .map(|(key, value)| {
                let text = match value {
                    Value::String(text) => text.clone(),
                    other => other.to_string(),
                };
                (key.as_str(), text)
            })
            .collect();
        lines.extend(
            sorted
                .into_iter()
                .map(|(key, value)| format!("cm:{key}={value}")),
        );
    }

    lines.extend(
        object_versions
            .iter()
            .map(|(name, version)| format!("ver:{name}={version}")),
    );

    lines.join("\n")
}

/// SHA-256 hex of the restart-required serving settings and object versions.
///
/// Certificate file content is not an input. A rotated TLS file behind the same
/// Secret reference keeps the hash, so TLS rotation stays hot-reloadable.
pub fn restart_required_hash(lumen: &Lumen, object_versions: &BTreeMap<String, String>) -> String {
    let digest = Sha256::digest(canonical_input(lumen, object_versions).as_bytes());
    format!("{digest:x}")
}
