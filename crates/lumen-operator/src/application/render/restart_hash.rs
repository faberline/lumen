//! Hash of the restart-required serving settings (#42, first slice).

use std::collections::BTreeMap;

use sha2::{Digest, Sha256};

use crate::application::render::serving_config::{serving_configmap, serving_env};
use crate::application::render::{ctx, instance, namespace};
use crate::domain::lumen_spec::Lumen;

/// Canonical text the hash is computed over: the serving env entries, the
/// serving ConfigMap data, and the referenced object versions, each sorted, one
/// per line. It holds names, keys, versions and non-secret settings only. A
/// Secret's content is never an input, so rotating certificate file content
/// does not change it.
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
    let mut data: Vec<(String, String)> = configmap["data"]
        .as_object()
        .map(|object| {
            object
                .iter()
                .map(|(key, value)| {
                    let text = value
                        .as_str()
                        .map_or_else(|| value.to_string(), str::to_owned);
                    (key.clone(), text)
                })
                .collect()
        })
        .unwrap_or_default();
    data.sort();
    lines.extend(
        data.into_iter()
            .map(|(key, value)| format!("cm:{key}={value}")),
    );

    lines.extend(
        object_versions
            .iter()
            .map(|(object, version)| format!("ver:{object}={version}")),
    );
    lines.join("\n")
}

/// SHA-256 hex over the restart-required serving settings and the referenced
/// object versions. A change here rolls the serving pods; a rotated TLS file
/// does not change it.
pub fn restart_required_hash(lumen: &Lumen, object_versions: &BTreeMap<String, String>) -> String {
    let digest = Sha256::digest(canonical_input(lumen, object_versions).as_bytes());
    format!("{digest:x}")
}
