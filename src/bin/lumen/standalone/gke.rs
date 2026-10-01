//! Standalone GKE configuration and the atomic publication of its manifests.

mod manifest;
mod validate;

use crate::cli::standalone::{
    StandaloneGkeArgs, StandaloneGkeCmd, StandaloneGkeInitArgs, StandaloneGkeRenderArgs,
};
use crate::standalone::gke::manifest::build;
use crate::standalone::gke::validate::validate;
use anyhow::{bail, Context, Result};
use serde::Deserialize;
use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use storage_durable::{atomic_write, sync_parent_dir, FsyncPolicy};

#[cfg(test)]
use crate::standalone::gke::validate::{valid_dns, valid_dns_subdomain};

const MARKER: &str = "lumen-standalone-managed/v1\n";
const IMAGE: &str = "ghcr.io/faberline/lumen:0.6.1";

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Config {
    #[serde(default = "default_name")]
    name: String,
    #[serde(default = "default_namespace")]
    namespace: String,
    node_pool: String,
    cpu: String,
    memory: String,
    #[serde(default = "default_storage_size")]
    storage_size: String,
    #[serde(default = "default_storage_class")]
    storage_class: String,
    allowed_service_accounts: Vec<String>,
}

#[cfg(all(feature = "backup", feature = "delegated-auth"))]
pub(super) struct BackupTarget {
    pub(super) name: String,
    pub(super) namespace: String,
}

fn load_config(path: &Path) -> Result<Config> {
    let config: Config =
        serde_yaml::from_slice(&fs::read(path).context("read config")?).context("parse config")?;
    validate(&config)?;
    Ok(config)
}

#[cfg(all(feature = "backup", feature = "delegated-auth"))]
pub(super) fn load_target(path: &Path) -> Result<BackupTarget> {
    let config = load_config(path)?;
    Ok(BackupTarget {
        name: config.name,
        namespace: config.namespace,
    })
}
fn default_name() -> String {
    "lumen".into()
}
fn default_namespace() -> String {
    "lumen".into()
}
fn default_storage_size() -> String {
    "20Gi".into()
}
fn default_storage_class() -> String {
    "premium-rwo".into()
}

pub(super) fn run(args: StandaloneGkeArgs) -> Result<()> {
    match args.cmd {
        StandaloneGkeCmd::Init(a) => init(a),
        StandaloneGkeCmd::Render(a) => render(a),
    }
}
fn init(a: StandaloneGkeInitArgs) -> Result<()> {
    if a.out.exists() {
        bail!("refusing to overwrite existing file")
    }
    let parent = parent_dir(&a.out);
    fs::create_dir_all(parent)?;
    let text = "name: lumen\nnamespace: lumen\nnodePool: REQUIRED\ncpu: REQUIRED\nmemory: REQUIRED\nstorageSize: 20Gi\nstorageClass: premium-rwo\nallowedServiceAccounts:\n  - namespace/name\n";
    write_file(&a.out, text.as_bytes())
}
fn render(a: StandaloneGkeRenderArgs) -> Result<()> {
    let cfg = load_config(&a.file)?;
    let docs = build(&cfg)?;
    validate_output(&a.out)?;
    let parent = parent_dir(&a.out);
    fs::create_dir_all(parent)?;
    let stage = unique_sibling(parent, ".lumen-stage")?;
    fs::create_dir(&stage)?;
    if let Err(error) = write_stage(&stage, docs) {
        let _ = fs::remove_dir_all(&stage);
        return Err(error);
    }

    if !a.out.exists() {
        fs::rename(&stage, &a.out).context("commit rendered output")?;
        sync_parent_dir(&a.out)?;
        return Ok(());
    }

    let old = unique_sibling(parent, ".lumen-old")?;
    fs::rename(&a.out, &old).context("stage previous managed output")?;
    sync_parent_dir(&a.out)?;
    if let Err(commit_error) = fs::rename(&stage, &a.out) {
        let restore = fs::rename(&old, &a.out);
        let _ = sync_parent_dir(&a.out);
        let _ = fs::remove_dir_all(&stage);
        if let Err(restore_error) = restore {
            bail!(
                "could not commit rendered output ({commit_error}) and could not restore the previous managed output ({restore_error})"
            );
        }
        return Err(commit_error).context("commit rendered output");
    }
    sync_parent_dir(&a.out)?;
    fs::remove_dir_all(&old).context("remove previous managed output")?;
    sync_parent_dir(&old)?;
    Ok(())
}

fn write_stage(stage: &Path, docs: (Vec<(String, Value)>, Vec<(String, Value)>)) -> Result<()> {
    let storage = stage.join("storage");
    let runtime = stage.join("runtime");
    fs::create_dir(&storage)?;
    fs::create_dir(&runtime)?;
    write_file(&stage.join(".lumen-standalone-managed"), MARKER.as_bytes())?;
    for (root, files) in [(storage, docs.0), (runtime, docs.1)] {
        for (name, value) in files {
            write_file(&root.join(name), serde_yaml::to_string(&value)?.as_bytes())?;
        }
    }
    Ok(())
}

fn validate_output(out: &Path) -> Result<()> {
    if out.file_name().is_none() {
        bail!("output root must name a directory")
    }
    if fs::symlink_metadata(out)
        .map(|metadata| metadata.file_type().is_symlink())
        .unwrap_or(false)
    {
        bail!("output root must not be a symlink")
    }
    if out.exists() {
        if !out.is_dir() {
            bail!("output root must be a directory")
        }
        let marker = fs::read(out.join(".lumen-standalone-managed")).unwrap_or_default();
        let entries: Vec<_> = fs::read_dir(out)?.collect::<std::io::Result<_>>()?;
        if !entries.is_empty() && marker != MARKER.as_bytes() {
            bail!("refusing unmanaged output")
        }
        for sub in ["storage", "runtime"] {
            let path = out.join(sub);
            if fs::symlink_metadata(&path)
                .map(|metadata| metadata.file_type().is_symlink())
                .unwrap_or(false)
            {
                bail!("generated root must not be a symlink")
            }
        }
    }
    Ok(())
}

fn parent_dir(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

fn unique_sibling(parent: &Path, prefix: &str) -> Result<PathBuf> {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    for attempt in 0..1000 {
        let path = parent.join(format!("{prefix}-{}-{stamp}-{attempt}", std::process::id()));
        if !path.exists() {
            return Ok(path);
        }
    }
    bail!("could not allocate a staging directory")
}
fn write_file(path: &Path, bytes: &[u8]) -> Result<()> {
    atomic_write(path, bytes, FsyncPolicy::Always)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(name: &str, accounts: Vec<String>) -> Config {
        Config {
            name: name.into(),
            namespace: "lumen".into(),
            node_pool: "pool".into(),
            cpu: "1".into(),
            memory: "1Gi".into(),
            storage_size: "20Gi".into(),
            storage_class: "premium-rwo".into(),
            allowed_service_accounts: accounts,
        }
    }

    #[test]
    fn fifty_two_char_name_with_one_account_is_valid() {
        let name = "a".repeat(52);
        assert!(validate(&config(&name, vec!["ns/sa".into()])).is_ok());
    }

    #[test]
    fn fifty_three_char_name_fails_indexed_binding() {
        let name = "a".repeat(53);
        assert!(validate(&config(&name, vec!["ns/sa".into()])).is_err());
    }

    #[test]
    fn fifty_eight_char_name_fails_admin_name() {
        let name = "a".repeat(58);
        assert!(validate(&config(&name, vec!["ns/sa".into()])).is_err());
    }

    #[test]
    fn too_many_accounts_fail_index_one_thousand() {
        let accounts = (0..1001).map(|index| format!("ns-{index}/sa")).collect();
        assert!(validate(&config(&"a".repeat(52), accounts)).is_err());
    }

    #[test]
    fn every_rendered_metadata_name_is_valid() {
        let cfg = config("a".repeat(52).as_str(), vec!["ns/sa".into()]);
        validate(&cfg).unwrap();
        let (storage, runtime) = build(&cfg).unwrap();
        for (filename, document) in storage.into_iter().chain(runtime) {
            let Some(metadata_name) = document
                .get("metadata")
                .and_then(|metadata| metadata.get("name"))
                .and_then(Value::as_str)
            else {
                continue;
            };
            if filename == "clusterrolebinding.yaml" {
                assert!(valid_dns_subdomain(metadata_name), "{metadata_name}");
            } else {
                assert!(valid_dns(metadata_name), "{metadata_name}");
            }
        }
    }
}
