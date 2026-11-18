//! The `fail_closed` functional phase: five damage cases on the durable files of a stopped
//! `lumen serve` child. Each damaged start must fail closed and serve no data. An undamaged
//! start (the negative control) must serve the same hits.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use serde_json::{json, Value};

use crate::http::HttpApi;
use crate::lifecycle::search;
use crate::run::Phase;
use crate::spawn::{LumenChild, SharedChild};

/// A damaged start gets 20 s to answer `/readyz`; the 60 s start is only for the control.
const DAMAGED_START_LIMIT: Duration = Duration::from_secs(20);

pub(super) struct FailClosed {
    child: SharedChild,
}

impl FailClosed {
    pub(super) fn new(child: SharedChild) -> Self {
        Self { child }
    }
}

impl Phase for FailClosed {
    fn name(&self) -> &str {
        "fail_closed"
    }

    fn run(&self, _endpoint: &str, _artifact_dir: &Path) -> Result<()> {
        let mut child = self
            .child
            .lock()
            .map_err(|_| anyhow!("child lock poisoned"))?;
        run_cases(&mut child)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Case {
    MissingCheckpoint,
    CorruptCheckpoint,
    UnknownFormatVersion,
    StaleAfterRestore,
    AmbiguousPublication,
}

impl Case {
    pub(super) const ALL: [Case; 5] = [
        Case::MissingCheckpoint,
        Case::CorruptCheckpoint,
        Case::UnknownFormatVersion,
        Case::StaleAfterRestore,
        Case::AmbiguousPublication,
    ];

    pub(super) fn name(self) -> &'static str {
        match self {
            Case::MissingCheckpoint => "missing_checkpoint",
            Case::CorruptCheckpoint => "corrupt_checkpoint",
            Case::UnknownFormatVersion => "unknown_format_version",
            Case::StaleAfterRestore => "stale_publication_after_restore",
            Case::AmbiguousPublication => "ambiguous_publication",
        }
    }
}

/// The bytes of `CURRENT` after the first checkpoint, and the generation it names. That
/// generation is older than the one the second checkpoint publishes.
pub(super) struct Backup {
    pub(super) current: Vec<u8>,
    pub(super) generation: String,
}

#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Closed(String),
    Served,
}

/// Fails only when at least one case failed. Each failure reads `case: reason`.
pub(super) fn summarize(failures: &[(String, String)]) -> Result<()> {
    if failures.is_empty() {
        return Ok(());
    }
    let list: Vec<String> = failures
        .iter()
        .map(|(name, reason)| format!("{name}: {reason}"))
        .collect();
    bail!("{}", list.join("; "))
}

/// The control first, then each case in `Case::ALL` order. Every failure is kept, so the
/// phase detail names each failing case.
pub(super) fn run_cases(child: &mut LumenChild) -> Result<()> {
    child.stop()?;
    let mut failures: Vec<(String, String)> = Vec::new();
    if let Err(err) = run_control(child) {
        failures.push(("control".to_string(), format!("{err:#}")));
    }
    for case in Case::ALL {
        if let Err(err) = run_case(child, case) {
            failures.push((case.name().to_string(), format!("{err:#}")));
        }
    }
    summarize(&failures)
}

/// A client for the child's current process. It is made after each start: a client kept
/// across a restart reuses a pooled connection to the old process and fails.
fn api_for(child: &LumenChild) -> Result<HttpApi> {
    HttpApi::new(&child.endpoint())
}

/// The negative control: undamaged files must start and serve the three documents.
fn run_control(child: &mut LumenChild) -> Result<()> {
    let collection = "acceptance-failclosed-control";
    let base = format!("/collections/{collection}");
    prepare(child, collection)?;
    child.start()?;
    let api = api_for(child)?;
    let response = search(
        &api,
        &base,
        json!({"term": {"field": "kw", "value": "red"}}),
    )?;
    let mut ids: Vec<String> = response["hits"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|hit| hit["external_id"].as_str().map(str::to_string))
        .collect();
    ids.sort();
    let wanted = ["doc-1", "doc-2", "doc-3"];
    if ids != wanted {
        bail!("undamaged start served {ids:?}, wanted {wanted:?}");
    }
    let _ = api.call("DELETE", &format!("{base}?force=true"), None);
    child.stop()
}

fn run_case(child: &mut LumenChild, case: Case) -> Result<()> {
    let collection = format!("acceptance-failclosed-{}", case.name());
    let older = prepare(child, &collection)?;
    damage(case, child.data_dir(), &older)?;
    match start_outcome(child) {
        Outcome::Closed(reason) => {
            child.stop()?;
            if reason.trim().is_empty() {
                bail!("the damaged start failed without a reason");
            }
            Ok(())
        }
        Outcome::Served => {
            child.stop()?;
            bail!("served data after damage")
        }
    }
}

/// A damaged start must not become ready. An error from `start_within` (the child exited,
/// or `/readyz` never answered 200) is a closed start.
fn start_outcome(child: &mut LumenChild) -> Outcome {
    match child.start_within(DAMAGED_START_LIMIT) {
        Err(err) => Outcome::Closed(format!("{err:#}")),
        Ok(()) => Outcome::Served,
    }
}

/// Leaves the child stopped, with a fresh data dir that holds two generations: the older one
/// is returned in `Backup`, the newer one is the current publication.
fn prepare(child: &mut LumenChild, collection: &str) -> Result<Backup> {
    child.stop()?;
    reset_data_dir(child.data_dir())?;
    child.start()?;
    let api = api_for(child)?;

    let base = format!("/collections/{collection}");
    let index = format!("{base}/index");
    let _ = api.call("DELETE", &format!("{base}?force=true"), None);
    api.expect(
        "create",
        &[200],
        "PUT",
        &base,
        Some(&json!({"fields": {"kw": {"type": "keyword"}}})),
    )?;
    api.expect(
        "index doc-1 and doc-2",
        &[200],
        "POST",
        &index,
        Some(&json!({"items": [
            {"external_id": "doc-1", "field": "kw", "value": "red"},
            {"external_id": "doc-2", "field": "kw", "value": "red"}
        ]})),
    )?;
    api.expect(
        "checkpoint",
        &[200],
        "POST",
        "/admin/checkpoint",
        Some(&json!({})),
    )?;
    let current = std::fs::read(child.data_dir().join("CURRENT")).context("read CURRENT")?;
    let backup = Backup {
        current,
        generation: current_generation_name(child.data_dir())?,
    };
    api.expect(
        "index doc-3",
        &[200],
        "POST",
        &index,
        Some(&json!({"items": [
            {"external_id": "doc-3", "field": "kw", "value": "red"}
        ]})),
    )?;
    api.expect(
        "second checkpoint",
        &[200],
        "POST",
        "/admin/checkpoint",
        Some(&json!({})),
    )?;

    child.stop()?;
    Ok(backup)
}

/// Removes every entry inside `data_dir`, but not the directory itself.
fn reset_data_dir(data_dir: &Path) -> Result<()> {
    for entry in
        std::fs::read_dir(data_dir).with_context(|| format!("read {}", data_dir.display()))?
    {
        let path = entry?.path();
        if path.is_dir() {
            std::fs::remove_dir_all(&path)?;
        } else {
            std::fs::remove_file(&path)?;
        }
    }
    Ok(())
}

/// The name in `CURRENT`, which reads `generation:<name>`.
fn current_generation_name(data_dir: &Path) -> Result<String> {
    let text = std::fs::read_to_string(data_dir.join("CURRENT")).context("read CURRENT")?;
    let name = text
        .trim()
        .strip_prefix("generation:")
        .ok_or_else(|| anyhow!("CURRENT has no generation: {text:?}"))?;
    Ok(name.to_string())
}

fn current_generation_dir(data_dir: &Path) -> Result<PathBuf> {
    Ok(data_dir.join(current_generation_name(data_dir)?))
}

fn manifest_path(data_dir: &Path) -> Result<PathBuf> {
    Ok(current_generation_dir(data_dir)?.join("_generation.json"))
}

fn read_manifest(data_dir: &Path) -> Result<(PathBuf, Value)> {
    let path = manifest_path(data_dir)?;
    let value = serde_json::from_slice(&std::fs::read(&path).context("read manifest")?)
        .context("parse manifest")?;
    Ok((path, value))
}

fn write_manifest(path: &Path, value: &Value) -> Result<()> {
    let mut bytes = serde_json::to_vec_pretty(value)?;
    bytes.push(b'\n');
    std::fs::write(path, bytes).with_context(|| format!("write {}", path.display()))
}

/// Applies one damage to the data dir of a stopped child.
pub(super) fn damage(case: Case, data_dir: &Path, older: &Backup) -> Result<()> {
    match case {
        Case::MissingCheckpoint => {
            let path = manifest_path(data_dir)?;
            std::fs::remove_file(&path).with_context(|| format!("remove {}", path.display()))
        }
        Case::CorruptCheckpoint => {
            let path = manifest_path(data_dir)?;
            std::fs::write(&path, b"{ this is not json")
                .with_context(|| format!("write {}", path.display()))
        }
        Case::UnknownFormatVersion => {
            let (path, mut value) = read_manifest(data_dir)?;
            value["schema_version"] = json!(999);
            write_manifest(&path, &value)
        }
        Case::StaleAfterRestore => {
            if !data_dir.join(&older.generation).is_dir() {
                bail!("older generation {} is gone", older.generation);
            }
            std::fs::write(data_dir.join("CURRENT"), &older.current).context("write CURRENT")
        }
        Case::AmbiguousPublication => {
            let (path, mut value) = read_manifest(data_dir)?;
            let revision = value["revision"]
                .as_u64()
                .ok_or_else(|| anyhow!("manifest has no revision"))?;
            value["revision"] = json!(revision + 100);
            write_manifest(&path, &value)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    /// A fake checkpoint root: `CURRENT` names `gen-5-rev-1`, and `gen-3-rev-1` also exists.
    fn fake_dir() -> tempfile::TempDir {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path();
        fs::write(root.join("CURRENT"), "generation:gen-5-rev-1\n").expect("CURRENT");
        fs::create_dir_all(root.join("gen-5-rev-1")).expect("gen-5");
        fs::write(
            root.join("gen-5-rev-1/_generation.json"),
            r#"{"schema_version":2,"revision":1,"previous":null}"#,
        )
        .expect("manifest");
        fs::create_dir_all(root.join("gen-3-rev-1")).expect("gen-3");
        tmp
    }

    fn no_backup() -> Backup {
        Backup {
            current: Vec::new(),
            generation: String::new(),
        }
    }

    fn manifest_bytes(root: &Path) -> Vec<u8> {
        fs::read(root.join("gen-5-rev-1/_generation.json")).expect("manifest")
    }

    #[test]
    fn missing_checkpoint_removes_the_manifest() {
        let tmp = fake_dir();
        damage(Case::MissingCheckpoint, tmp.path(), &no_backup()).expect("damage");
        assert!(!tmp.path().join("gen-5-rev-1/_generation.json").exists());
        assert!(tmp.path().join("CURRENT").exists(), "CURRENT stays");
    }

    #[test]
    fn corrupt_checkpoint_makes_the_manifest_invalid_json() {
        let tmp = fake_dir();
        damage(Case::CorruptCheckpoint, tmp.path(), &no_backup()).expect("damage");
        assert!(serde_json::from_slice::<Value>(&manifest_bytes(tmp.path())).is_err());
    }

    #[test]
    fn unknown_format_version_sets_schema_999() {
        let tmp = fake_dir();
        damage(Case::UnknownFormatVersion, tmp.path(), &no_backup()).expect("damage");
        let value: Value = serde_json::from_slice(&manifest_bytes(tmp.path())).expect("json");
        assert_eq!(value["schema_version"], 999);
        assert_eq!(value["revision"], 1, "other keys are kept");
    }

    #[test]
    fn stale_after_restore_points_current_at_the_older_generation() {
        let tmp = fake_dir();
        let older = Backup {
            current: b"generation:gen-3-rev-1\n".to_vec(),
            generation: "gen-3-rev-1".into(),
        };
        damage(Case::StaleAfterRestore, tmp.path(), &older).expect("damage");
        assert_eq!(
            fs::read(tmp.path().join("CURRENT")).expect("CURRENT"),
            b"generation:gen-3-rev-1\n"
        );
        assert!(
            tmp.path().join("gen-5-rev-1").is_dir(),
            "newer generation kept"
        );
    }

    #[test]
    fn ambiguous_publication_changes_the_revision() {
        let tmp = fake_dir();
        damage(Case::AmbiguousPublication, tmp.path(), &no_backup()).expect("damage");
        let value: Value = serde_json::from_slice(&manifest_bytes(tmp.path())).expect("json");
        assert_eq!(value["revision"], 101);
    }

    #[test]
    fn damage_fails_when_current_is_missing() {
        let tmp = tempfile::tempdir().expect("tempdir");
        assert!(damage(Case::MissingCheckpoint, tmp.path(), &no_backup()).is_err());
    }

    #[test]
    fn case_names_are_the_five_documented_names() {
        assert_eq!(
            Case::ALL.map(Case::name),
            [
                "missing_checkpoint",
                "corrupt_checkpoint",
                "unknown_format_version",
                "stale_publication_after_restore",
                "ambiguous_publication"
            ]
        );
    }

    #[test]
    fn failures_are_joined_with_case_names() {
        assert!(summarize(&[]).is_ok());
        let err = summarize(&[(
            "ambiguous_publication".into(),
            "served data after damage".into(),
        )])
        .expect_err("one failure");
        assert_eq!(
            format!("{err:#}"),
            "ambiguous_publication: served data after damage"
        );
    }
}
