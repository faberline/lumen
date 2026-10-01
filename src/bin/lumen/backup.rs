//! `lumen backup` and the snapshot commands: dump or export, load or import,
//! and the offline inspect.

use anyhow::{Context, Result};

use crate::cli::client::{
    BackupArgs, InspectArgs, InspectFormat, SnapshotExportArgs, SnapshotImportArgs,
};

#[cfg(feature = "backup")]
use std::path::Path;

/// Whatever eventually authenticates `lumen backup` to the admin API, it will
/// not be a string on the command line. #2871 took away the metadata-server
/// fallback; #2873 takes away the bearer flag that was left, because a
/// credential passed as an argument is a credential in `ps`, in shell history,
/// and in the CronJob's own `kubectl describe` — the exact exposure R5 rules
/// out. The `token` parameter on the `lumen::backup` calls below is the seam
/// #2877 fills with a projected, audience-bound ServiceAccount token read from
/// a file; until then it is `None` and the request carries no `Authorization`
/// header at all.
///
/// `lumen backup` (#808): fetch `{url}/admin/backup` and ship the bytes to
/// `dest` via `libs/service-backup`, printing the resulting
/// `BackupRunResult` as JSON. This is what the operator's optional backup
/// CronJob (`spec.serving.backup`) invokes on a schedule; it works equally
/// well ad hoc against any running serving node.
#[cfg(feature = "backup")]
pub(crate) async fn dispatch_backup(args: BackupArgs) -> Result<()> {
    let dest = service_backup::BackupDestination::from_uri(&args.dest)?;
    let retention = match args.retention_secs {
        Some(secs) => service_backup::RetentionPolicy::max_age_seconds(secs),
        None => service_backup::RetentionPolicy::default(),
    };
    // Read here, not at parse time: one backup run is one read of the
    // projected file, so a CronJob pod that starts minutes after the kubelet
    // last rotated the token still presents the current one (#2877 R3).
    // Missing, empty, expired, or minted for the wrong audience all fail the
    // run before any bytes move, with a message naming the path rather than
    // the material (#2877 R5).
    let token = match args.token_file.as_deref() {
        Some(path) => Some(
            service_auth::k8s::ProjectedTokenFile::new(path, lumen::auth::AUDIENCE)
                .read()
                .with_context(|| "the backup runner cannot authenticate to this Lumen fleet")?,
        ),
        None => None,
    };
    let result = lumen::backup::run_backup(
        &args.url,
        token
            .as_ref()
            .map(service_auth::k8s::ProjectedToken::expose),
        &dest,
        &retention,
    )
    .await?;
    // Chainable output (#963): `lumen backup` always emits a single JSON
    // object, so the contract's "next" is a top-level field, not a text tail
    // line. `service_backup::BackupRunResult` stays untouched (shared type) —
    // this widens only the ad hoc `Value` this CLI prints.
    let mut out = serde_json::to_value(&result)?;
    if let serde_json::Value::Object(ref mut map) = out {
        map.insert(
            "next".to_string(),
            serde_json::Value::String(restore_next_command(&args, &result)),
        );
    }
    println!("{}", serde_json::to_string_pretty(&out)?);
    Ok(())
}

/// The matching restore step for a `lumen backup` run (#963): POST the
/// just-written snapshot bytes back to `/admin/restore` on the same fleet.
/// Only `file://` destinations resolve to a concrete local path for a copyable
/// restore command; cloud sinks remain shared `service-backup` behavior and
/// fall back to a generic note here instead of guessing a wrong object-fetch
/// command. The command carries no `Authorization` header: there is no
/// credential for it to carry (#2873), and a placeholder one would read as an
/// instruction to go find a token that does not exist.
#[cfg(feature = "backup")]
fn restore_next_command(args: &BackupArgs, result: &service_backup::BackupRunResult) -> String {
    let url = args.url.trim_end_matches('/');
    match result.object.sink.strip_prefix("local:") {
        Some(root) => format!(
            "curl -sS -X POST {url}/admin/restore -H 'Content-Type: application/json' --data-binary @{}/{}",
            root.trim_end_matches('/'),
            result.object.key
        ),
        None => format!(
            "fetch {} from {} then: curl -sS -X POST {url}/admin/restore -H 'Content-Type: application/json' --data-binary @<downloaded-file>",
            result.object.key, result.object.sink
        ),
    }
}

#[cfg(not(feature = "backup"))]
pub(crate) async fn dispatch_backup(_args: BackupArgs) -> Result<()> {
    anyhow::bail!(
        "this lumen build was compiled without backup support; rebuild with \
         `--features backup` (or `operator`, which pulls it in — the published \
         image includes both)"
    )
}

/// `lumen dump|export` (#1095): fetch `{url}/admin/backup` and write exact
/// SnapshotV1 JSON bytes to stdout or `--out`.
#[cfg(feature = "backup")]
pub(crate) async fn dispatch_snapshot_export(args: SnapshotExportArgs) -> Result<()> {
    let payload = lumen::backup::fetch_snapshot_bytes(&args.url, None).await?;
    if let Some(out) = args.out {
        if let Some(parent) = out.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create {}", parent.display()))?;
        }
        std::fs::write(&out, &payload).with_context(|| format!("write {}", out.display()))?;
        let next = restore_file_next_command(args.url.trim_end_matches('/'), &out);
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "status": "exported",
                "path": out,
                "bytes": payload.len(),
                "next": next,
            }))?
        );
    } else {
        let mut stdout = std::io::stdout().lock();
        std::io::Write::write_all(&mut stdout, &payload)?;
        std::io::Write::flush(&mut stdout)?;
    }
    Ok(())
}

#[cfg(not(feature = "backup"))]
pub(crate) async fn dispatch_snapshot_export(_args: SnapshotExportArgs) -> Result<()> {
    anyhow::bail!(
        "this lumen build was compiled without backup support; rebuild with \
         `--features backup` (or `operator`, which pulls it in — the published \
         image includes both)"
    )
}

/// `lumen load|import` (#1095): read SnapshotV1 JSON bytes from `--file` or
/// stdin and POST them to `{url}/admin/restore`.
#[cfg(feature = "backup")]
pub(crate) async fn dispatch_snapshot_import(args: SnapshotImportArgs) -> Result<()> {
    let payload = match &args.file {
        Some(path) => std::fs::read(path).with_context(|| format!("read {}", path.display()))?,
        None => {
            let mut buf = Vec::new();
            let mut stdin = std::io::stdin();
            std::io::Read::read_to_end(&mut stdin, &mut buf)?;
            buf
        }
    };
    lumen::backup::restore_snapshot_bytes(&args.url, None, &payload).await?;
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "status": "restored",
            "url": args.url.trim_end_matches('/'),
            "bytes": payload.len(),
            "next": "done",
        }))?
    );
    Ok(())
}

#[cfg(not(feature = "backup"))]
pub(crate) async fn dispatch_snapshot_import(_args: SnapshotImportArgs) -> Result<()> {
    anyhow::bail!(
        "this lumen build was compiled without backup support; rebuild with \
         `--features backup` (or `operator`, which pulls it in — the published \
         image includes both)"
    )
}

/// `lumen inspect`: ask a snapshot document, offline, which of its fields will
/// not come back.
///
/// This is the same audit `Engine::reindex_needed` runs — and the serving node
/// logs — at the end of every `restore` and every segment reopen. It is
/// duplicated here rather than queried from a node because the audience is an
/// operator holding a backup file, deciding whether to import it at all: by
/// the time a node can answer, the decision has already been made.
///
/// It asks the parsed document, not a restored engine. Restoring first would
/// build every interner, roaring bitmap and forward map of the whole backup in
/// RAM — a multiple of the file size, on the machine that most needs the
/// answer, and against a file already suspected of being damaged — to compute
/// something out of fields the parsed document already carries.
/// `SnapshotV1::reindex_needed` and `Engine::reindex_needed` are held to one
/// verdict by `tests/it/reopen_names_the_fields_that_need_reindexing.rs`, which runs
/// both over the same bytes and requires the same rows.
///
/// Deliberately not behind the `backup` feature. Reading a local file and
/// auditing its fields needs no HTTP client, and an operator diagnosing a
/// suspect backup should not first have to work out which build of the binary
/// they are holding.
pub(crate) fn dispatch_snapshot_inspect(args: InspectArgs) -> Result<()> {
    let payload = match &args.file {
        Some(path) => std::fs::read(path).with_context(|| format!("read {}", path.display()))?,
        None => {
            let mut buf = Vec::new();
            let mut stdin = std::io::stdin();
            std::io::Read::read_to_end(&mut stdin, &mut buf)?;
            buf
        }
    };
    let snapshot: lumen::storage::SnapshotV1 = serde_json::from_slice(&payload).context(
        "decode SnapshotV1 JSON — this reads the document `lumen export` writes, \
         not an on-disk segment directory",
    )?;

    let needed = snapshot.reindex_needed();
    let not_audited = snapshot.fields_not_audited();
    let documents_scanned = snapshot.documents_scanned();

    match args.format {
        InspectFormat::Json => println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "reindex_needed": needed,
                "fields_not_audited": not_audited,
                "documents_scanned": documents_scanned,
            }))?
        ),
        InspectFormat::Text => {
            // The census travels with the verdict in both formats: "nothing is
            // damaged" and "nothing was read" print differently.
            for (id, indexed) in &documents_scanned {
                println!("scanned {id}: {indexed} documents");
            }
            // And so does what the audit could not look at. An empty
            // `reindex_needed` over a collection whose every field is here is a
            // clean verdict about nothing.
            for row in &not_audited {
                println!(
                    "NOT AUDITED {}.{}: {}",
                    row.collection, row.field, row.reason
                );
            }
            if needed.is_empty() {
                println!("no audited field needs re-indexing");
                println!("next: done");
            } else {
                for row in &needed {
                    println!(
                        "REINDEX {}.{}: the census covers {} documents, this document holds \
                         a value for none of them",
                        row.collection, row.field, row.documents_covered
                    );
                }
                println!(
                    "next: re-index the {} field(s) above from their source of record — their \
                     contents are not in this document",
                    needed.len()
                );
            }
        }
    }
    Ok(())
}

#[cfg(feature = "backup")]
fn restore_file_next_command(url: &str, path: &Path) -> String {
    format!("lumen import --url {url} --file {}", path.display())
}
