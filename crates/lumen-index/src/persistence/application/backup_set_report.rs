//! The verification report of a restored multi-shard backup set. The report is
//! evidence: it carries no timestamp, no path and no host name, so the same
//! inputs give the same bytes. Each check runs even when an earlier one fails.

use std::collections::BTreeMap;

use crate::index::application::engine::Engine;
use crate::persistence::application::backup_set_restore::EngineSet;
use crate::persistence::domain::backup_manifest::{verify_artifact, BackupManifest, ShardArtifact};
use crate::shared_kernel::types::search::SearchRequest;

/// The version of the report format. Field names and order follow the structs below.
pub const REPORT_VERSION: u32 = 1;

/// One answer of a fixed query on one shard.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct QueryAnswer {
    pub collection: String,
    pub index: usize,
    pub shard: u32,
    pub total: u64,
    pub ids: Vec<String>,
    pub error: Option<String>,
}

/// One named check of the report.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct Check {
    pub name: String,
    pub ok: bool,
    pub detail: String,
}

/// The result of one shard: its digest and its document count.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ShardReport {
    pub shard: u32,
    pub key: String,
    pub sha256: String,
    pub bytes: u64,
    pub expected_documents: u64,
    pub restored_documents: u64,
    pub digest_ok: bool,
    pub documents_ok: bool,
}

/// The verification report of one restored backup set.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct VerificationReport {
    pub report_version: u32,
    pub backup_id: String,
    pub status: String,
    pub shards: Vec<ShardReport>,
    pub collections: Vec<String>,
    pub documents_total: u64,
    pub queries_compared: usize,
    pub answers: Vec<QueryAnswer>,
    pub checks: Vec<Check>,
    pub failed_check: Option<String>,
}

/// Runs every fixed query on every shard, in the order collection, request, shard.
pub fn run_queries(
    set: &EngineSet,
    queries: &BTreeMap<String, Vec<SearchRequest>>,
) -> Vec<QueryAnswer> {
    let mut answers = Vec::new();
    for (collection, requests) in queries {
        for (index, request) in requests.iter().enumerate() {
            for (shard, engine) in set.0.iter().enumerate() {
                let shard = shard as u32;
                match engine.search(collection, request.clone()) {
                    Ok(response) => answers.push(QueryAnswer {
                        collection: collection.clone(),
                        index,
                        shard,
                        total: response.total,
                        ids: response
                            .hits
                            .iter()
                            .map(|hit| hit.external_id.clone())
                            .collect(),
                        error: None,
                    }),
                    Err(err) => answers.push(QueryAnswer {
                        collection: collection.clone(),
                        index,
                        shard,
                        total: 0,
                        ids: Vec::new(),
                        error: Some(format!("{err:#}")),
                    }),
                }
            }
        }
    }
    answers
}

/// The documents of one engine, counted as the manifest counts them.
fn engine_documents(engine: &Engine) -> u64 {
    match engine.snapshot() {
        Ok(snapshot) => snapshot
            .collections
            .values()
            .map(|collection| collection.eid_fields.len() as u64)
            .sum(),
        Err(_) => 0,
    }
}

/// The sorted collection ids of one engine; empty when the engine cannot be read.
fn engine_collections(engine: Option<&Engine>) -> Vec<String> {
    match engine.map(|engine| engine.snapshot()) {
        Some(Ok(snapshot)) => snapshot.collections.keys().cloned().collect(),
        _ => Vec::new(),
    }
}

/// Verifies a restored set. `restarted` holds the engines reopened from disk, and
/// `before` holds the answers of the same queries before the restart.
pub fn verify_restored_set(
    manifest: &BackupManifest,
    artifacts: &[ShardArtifact],
    restarted: &EngineSet,
    queries: &BTreeMap<String, Vec<SearchRequest>>,
    before: &[QueryAnswer],
) -> VerificationReport {
    let mut entries: Vec<_> = manifest.shards.iter().collect();
    entries.sort_by_key(|entry| entry.shard);

    // 1. Every shard artifact matches its digest.
    let mut digest_ok = Vec::with_capacity(entries.len());
    let mut digest_detail = None;
    for entry in &entries {
        let artifact = artifacts
            .iter()
            .find(|artifact| artifact.shard == entry.shard);
        let ok = match artifact {
            Some(artifact) => verify_artifact(entry, &artifact.bytes).is_ok(),
            None => false,
        };
        if !ok && digest_detail.is_none() {
            digest_detail = Some(match artifact {
                Some(_) => format!(
                    "shard {} artifact {} does not match the manifest digest",
                    entry.shard, entry.key
                ),
                None => format!("shard {} artifact is missing", entry.shard),
            });
        }
        digest_ok.push(ok);
    }
    let digests_check = Check {
        name: "shard_digests".into(),
        ok: digest_detail.is_none(),
        detail: digest_detail.unwrap_or_else(|| {
            format!(
                "{n} of {n} shard artifacts match their digests",
                n = entries.len()
            )
        }),
    };

    // 2. Every restarted shard holds the documents that the manifest names.
    let mut shards = Vec::with_capacity(entries.len());
    let mut documents_detail = None;
    for (position, entry) in entries.iter().enumerate() {
        let restored_documents = match restarted.0.get(entry.shard as usize) {
            Some(engine) => engine_documents(engine),
            None => 0,
        };
        let documents_ok = restored_documents == entry.documents;
        if !documents_ok && documents_detail.is_none() {
            documents_detail = Some(format!(
                "shard {} holds {} documents, manifest says {}",
                entry.shard, restored_documents, entry.documents
            ));
        }
        shards.push(ShardReport {
            shard: entry.shard,
            key: entry.key.clone(),
            sha256: entry.sha256.clone(),
            bytes: entry.bytes,
            expected_documents: entry.documents,
            restored_documents,
            digest_ok: digest_ok[position],
            documents_ok,
        });
    }
    let documents_total: u64 = shards.iter().map(|shard| shard.restored_documents).sum();
    let documents_check = Check {
        name: "shard_documents".into(),
        ok: documents_detail.is_none(),
        detail: documents_detail.unwrap_or_else(|| {
            format!(
                "{documents_total} documents across {} shards",
                entries.len()
            )
        }),
    };

    // 3. Every restarted shard holds the collections of the manifest.
    let mut manifest_collections: Vec<String> = manifest.collections.keys().cloned().collect();
    manifest_collections.sort();
    let mut collections_detail = None;
    for entry in &entries {
        let got = engine_collections(restarted.0.get(entry.shard as usize).map(|e| e.as_ref()));
        if got != manifest_collections && collections_detail.is_none() {
            collections_detail = Some(format!(
                "shard {} collections [{}] differ from the manifest [{}]",
                entry.shard,
                got.join(", "),
                manifest_collections.join(", ")
            ));
        }
    }
    let collections_check = Check {
        name: "collections".into(),
        ok: collections_detail.is_none(),
        detail: collections_detail.unwrap_or_else(|| {
            format!(
                "{} collection(s) on every shard",
                manifest_collections.len()
            )
        }),
    };

    // 4. The same queries give the same answers after the restart.
    let after = run_queries(restarted, queries);
    let queries_detail = if queries.is_empty() {
        (true, "no queries given".to_string())
    } else if after.as_slice() == before {
        (
            true,
            format!("{} answers equal before and after restart", after.len()),
        )
    } else {
        let first = (0..after.len().max(before.len())).find(|&i| after.get(i) != before.get(i));
        let detail = match first.and_then(|i| after.get(i).or_else(|| before.get(i))) {
            Some(answer) => format!(
                "collection {} query {} on shard {} answered differently after restart",
                answer.collection, answer.index, answer.shard
            ),
            None => "answers differ after restart".to_string(),
        };
        (false, detail)
    };
    let queries_check = Check {
        name: "queries".into(),
        ok: queries_detail.0,
        detail: queries_detail.1,
    };

    let checks = vec![
        digests_check,
        documents_check,
        collections_check,
        queries_check,
    ];
    let failed_check = checks
        .iter()
        .find(|check| !check.ok)
        .map(|check| check.name.clone());
    let queries_compared = queries.values().map(Vec::len).sum();

    VerificationReport {
        report_version: REPORT_VERSION,
        backup_id: manifest.backup_id.clone(),
        status: if failed_check.is_none() {
            "passed".into()
        } else {
            "failed".into()
        },
        shards,
        collections: manifest_collections,
        documents_total,
        queries_compared,
        answers: after,
        checks,
        failed_check,
    }
}

#[cfg(test)]
mod tests;
