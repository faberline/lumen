//! `lumen restore-set`: restore a multi-shard backup set into one segment root per
//! shard, restart the engines from disk, verify them and print the report as JSON.

use anyhow::Result;

use crate::cli::client::RestoreSetArgs;

/// Restores the backup set, restarts every shard and prints the verification report.
/// A failed check exits non-zero after the report is printed.
#[cfg(feature = "backup")]
pub(crate) async fn dispatch_restore_set(args: RestoreSetArgs) -> Result<()> {
    use std::path::PathBuf;
    use std::sync::Arc;

    use anyhow::{anyhow, Context};
    use lumen::backup::{
        read_backup_manifest, restore_backup_set, run_queries, verify_restored_set,
        BackupSetSource, DirSource, EngineSet, RestoreOptions, ShardArtifact, BACKUP_MANIFEST_KEY,
    };
    use lumen::segment_rdb::SegmentRdbStore;
    use lumen::storage::Engine;

    let queries = load_queries(args.queries.as_deref())?;
    let source = DirSource(args.from.clone());
    let bytes = match source.read(BACKUP_MANIFEST_KEY) {
        Ok(Some(bytes)) => bytes,
        Ok(None) => anyhow::bail!(
            "restore refused: backup manifest lumen-backup-manifest.json is missing from the backup set"
        ),
        Err(err) => anyhow::bail!("restore refused: cannot read the backup set: {err:#}"),
    };
    let manifest = read_backup_manifest(&bytes).map_err(|err| anyhow!("restore refused: {err}"))?;
    let shard_count = manifest.physical_shard_count;

    // The data root must not hold data already: refuse before any directory is created.
    let roots: Vec<PathBuf> = (0..shard_count)
        .map(|i| args.data_root.join(format!("shard-{i}")))
        .collect();
    for (i, root) in roots.iter().enumerate() {
        let empty = root.is_dir() && std::fs::read_dir(root)?.next().is_none();
        if root.exists() && !empty {
            anyhow::bail!(
                "data root {} is not empty: shard-{i} already exists",
                args.data_root.display()
            );
        }
    }
    for root in &roots {
        std::fs::create_dir_all(root).with_context(|| format!("create {}", root.display()))?;
    }

    // Restore into fresh engines, then answer the queries before the restart.
    let engines = EngineSet((0..shard_count).map(|_| Arc::new(Engine::new())).collect());
    restore_backup_set(&source, &engines, &RestoreOptions { replace: false })
        .map_err(|err| anyhow!("restore refused: {err}"))?;
    let before = run_queries(&engines, &queries);

    // Save each shard to its root, drop the engines, then reopen every root from disk.
    for (i, root) in roots.iter().enumerate() {
        SegmentRdbStore::new(root.clone())?.save(&engines.0[i], 1)?;
    }
    drop(engines);
    let mut reopened = Vec::with_capacity(roots.len());
    for (i, root) in roots.iter().enumerate() {
        match SegmentRdbStore::new(root.clone())?.load_current_generation()? {
            Some(loaded) => reopened.push(loaded.engine),
            None => anyhow::bail!("shard {i}: no CURRENT generation after restart"),
        }
    }
    let restarted = EngineSet(reopened);

    // The artifacts are read again, so the report can check each digest.
    let mut artifacts = Vec::new();
    for entry in &manifest.shards {
        if let Some(bytes) = source.read(&entry.key)? {
            artifacts.push(ShardArtifact {
                shard: entry.shard,
                key: entry.key.clone(),
                bytes,
            });
        }
    }

    let report = verify_restored_set(&manifest, &artifacts, &restarted, &queries, &before);
    println!("{}", serde_json::to_string_pretty(&report)?);
    if report.status == "failed" {
        anyhow::bail!(
            "restore verification failed: {}",
            report.failed_check.clone().unwrap_or_default()
        );
    }
    Ok(())
}

/// Reads `--queries`: a JSON object from collection id to its SearchRequest list.
#[cfg(feature = "backup")]
fn load_queries(
    path: Option<&std::path::Path>,
) -> Result<std::collections::BTreeMap<String, Vec<lumen::types::SearchRequest>>> {
    use anyhow::anyhow;

    let Some(path) = path else {
        return Ok(std::collections::BTreeMap::new());
    };
    let text = std::fs::read_to_string(path).map_err(|err| anyhow!("read --queries: {err:#}"))?;
    serde_json::from_str(&text).map_err(|err| anyhow!("parse --queries: {err}"))
}

#[cfg(not(feature = "backup"))]
pub(crate) async fn dispatch_restore_set(_args: RestoreSetArgs) -> Result<()> {
    anyhow::bail!(
        "this lumen build was compiled without backup support; rebuild with \
         `--features backup` (or `operator`, which pulls it in — the published \
         image includes both)"
    )
}
