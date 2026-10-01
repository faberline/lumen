//! `lumen query`: build the index, search and duplicates request bodies from
//! the command line and post them to a running node.

use anyhow::Result;

#[cfg(any(feature = "backup", test))]
use anyhow::Context;

use crate::cli::client::QueryArgs;

#[cfg(all(feature = "backup", feature = "delegated-auth"))]
use crate::connect::proxy::{client_token_source, first_mint};

#[cfg(any(test, feature = "backup"))]
use crate::cli::client::{QueryDuplicatesArgs, QuerySearchArgs, QueryTarget};

#[cfg(feature = "backup")]
use crate::cli::client::{QueryCollectionsCommand, QueryCommand};

// ---------------------------------------------------------------------------
// `lumen connect` / `lumen query` (#1321) — thin adapter over
// `cli_std::connect` (#1376): the `kubectl port-forward` process lifecycle
// (`ChildGuard`, `free_local_port`, `wait_for_local_port_ready`) lives in
// `libs/cli-std/src/connect.rs`, reusable by any k8s-native service CLI.
// This file keeps only its own flag surface (`ConnectArgs`/`QueryTarget`) and
// the `Lumen` CRD-name lookup convention (`"lumen"` passed as
// `resource_kind`).
//
// #2873 cut the credential half away entirely. The shared module's resolver
// chain — kubectl-get the Secret named by the CR, base64-decode the registry
// key inside it, pick an entry whose role covers the request — is still there
// for the services that have not migrated, but lumen no longer calls any of
// it: the registry it decoded stopped existing in #2871, and the CR field
// naming the Secret stopped existing in #2872. What remains here is a
// port-forward, and nothing that reads, derives, prints, or passes on a
// credential.
// ---------------------------------------------------------------------------

// The body-builder / URL-resolution helpers below are exercised directly by
// `dispatch_query`'s `backup`-gated real implementation and by this file's
// unit tests; `cfg(any(test, feature = "backup"))` keeps them from tripping
// dead-code warnings in a plain default (non-`backup`) build while staying
// available to `cargo test -p lumen` without requiring `--features backup`.
#[cfg(any(test, feature = "backup"))]
fn resolve_base_url(target: &QueryTarget) -> Result<String> {
    target
        .url
        .clone()
        .context("--url is required (or set LUMEN_URL, or run inside `lumen connect ... -- ...`)")
}

/// Parse a CLI-supplied value into a `FieldValue`: JSON first (so
/// `--item p1:price=79` and `--item p1:embedding=[0.1,0.2,0.9]` work
/// unquoted), else the raw string.
#[cfg(any(test, feature = "backup"))]
fn parse_field_value(raw: &str) -> lumen::types::FieldValue {
    serde_json::from_str::<lumen::types::FieldValue>(raw)
        .unwrap_or_else(|_| lumen::types::FieldValue::String(raw.to_string()))
}

/// Parse one `--item EXTERNAL_ID:FIELD=VALUE` flag into an `IndexItem`.
#[cfg(any(test, feature = "backup"))]
fn parse_index_item(spec: &str) -> Result<lumen::types::IndexItem> {
    let (external_id, rest) = spec
        .split_once(':')
        .with_context(|| format!("--item `{spec}` must be EXTERNAL_ID:FIELD=VALUE"))?;
    let (field, value) = rest
        .split_once('=')
        .with_context(|| format!("--item `{spec}` must be EXTERNAL_ID:FIELD=VALUE"))?;
    Ok(lumen::types::IndexItem {
        external_id: external_id.to_string(),
        field: field.to_string(),
        value: parse_field_value(value),
        version: None,
    })
}

/// AC3: build the exact flat `POST /collections/{id}/index` body —
/// `{"items":[{"external_id","field","value"}]}` — matching the shape
/// `lumen::spec::query_shapes()`'s "index" entry publishes, NOT a nested
/// `{id, fields:{...}}` shape.
#[cfg(any(test, feature = "backup"))]
fn build_index_body(collection: &str, items: &[String]) -> Result<(String, serde_json::Value)> {
    let parsed: Result<Vec<_>> = items.iter().map(|s| parse_index_item(s)).collect();
    let request = lumen::types::IndexRequest {
        items: parsed?,
        request_id: None,
    };
    let body = serde_json::to_value(&request).context("serialize IndexRequest")?;
    Ok((format!("/collections/{collection}/index"), body))
}

/// Build the `QueryNode` for `lumen query search` from exactly one of
/// `--term`/`--match`/`--query-json`.
#[cfg(any(test, feature = "backup"))]
fn build_search_query_node(args: &QuerySearchArgs) -> Result<lumen::types::QueryNode> {
    let set_count = [
        args.term.is_some(),
        args.match_.is_some(),
        args.query_json.is_some(),
    ]
    .into_iter()
    .filter(|set| *set)
    .count();
    if set_count != 1 {
        anyhow::bail!("exactly one of --term, --match, --query-json is required");
    }
    if let Some(term) = &args.term {
        let (field, value) = term.split_once('=').context("--term must be FIELD=VALUE")?;
        return Ok(lumen::types::QueryNode::Term(lumen::types::TermQuery {
            field: field.to_string(),
            value: parse_field_value(value),
        }));
    }
    if let Some(m) = &args.match_ {
        let (field, text) = m.split_once('=').context("--match must be FIELD=TEXT")?;
        return Ok(lumen::types::QueryNode::Match(lumen::types::MatchQuery {
            field: field.to_string(),
            text: text.to_string(),
            op: lumen::types::MatchOp::And,
        }));
    }
    let raw = args
        .query_json
        .as_deref()
        .expect("exactly one branch checked above");
    serde_json::from_str(raw).context("--query-json is not a valid QueryNode")
}

/// Build the `POST /collections/{id}/search` body for `lumen query search`.
#[cfg(any(test, feature = "backup"))]
fn build_search_body(args: &QuerySearchArgs) -> Result<(String, serde_json::Value)> {
    let query = build_search_query_node(args)?;
    let request = lumen::types::SearchRequest {
        query,
        limit: args.limit,
        offset: 0,
        cursor: None,
        routing_key: None,
        sort: None,
        track_total: true,
        collapse: None,
    };
    let body = serde_json::to_value(&request).context("serialize SearchRequest")?;
    Ok((format!("/collections/{}/search", args.collection), body))
}

/// Build the `POST /collections/{id}/duplicates` body for `lumen query duplicates`.
#[cfg(any(test, feature = "backup"))]
fn build_duplicates_body(args: &QueryDuplicatesArgs) -> Result<(String, serde_json::Value)> {
    let request = lumen::types::DuplicatesRequest {
        field: args.field.clone(),
        min_group_size: args.min_group_size,
        limit: args.limit,
        offset: args.offset,
    };
    let body = serde_json::to_value(&request).context("serialize DuplicatesRequest")?;
    Ok((format!("/collections/{}/duplicates", args.collection), body))
}

/// The `token` parameter is the whole of `lumen query`'s credential handling
/// (#2878). It is a value passed in, minted moments earlier for this one
/// command: there is no environment variable, no file, and no Secret lookup
/// behind it. #2873 removed all three, and the parameter is deliberately
/// explicit rather than ambient so that a future change which starts sending a
/// credential from somewhere else has to say so in this signature.
#[cfg(feature = "backup")]
async fn http_post_json(
    base_url: &str,
    path: &str,
    body: serde_json::Value,
    token: Option<&service_auth::k8s::ProjectedToken>,
) -> Result<serde_json::Value> {
    let url = format!("{}{path}", base_url.trim_end_matches('/'));
    let client = reqwest::Client::new();
    let mut req = client.post(&url).json(&body);
    if let Some(token) = token {
        req = req.bearer_auth(token.expose());
    }
    let resp = req.send().await.with_context(|| format!("POST {url}"))?;
    let status = resp.status();
    let payload: serde_json::Value = resp.json().await.unwrap_or(serde_json::Value::Null);
    if !status.is_success() {
        anyhow::bail!("POST {url} returned {status}: {payload}");
    }
    Ok(payload)
}

#[cfg(feature = "backup")]
async fn http_get_json(
    base_url: &str,
    path: &str,
    token: Option<&service_auth::k8s::ProjectedToken>,
) -> Result<serde_json::Value> {
    let url = format!("{}{path}", base_url.trim_end_matches('/'));
    let client = reqwest::Client::new();
    let mut req = client.get(&url);
    if let Some(token) = token {
        req = req.bearer_auth(token.expose());
    }
    let resp = req.send().await.with_context(|| format!("GET {url}"))?;
    let status = resp.status();
    let payload: serde_json::Value = resp.json().await.unwrap_or(serde_json::Value::Null);
    if !status.is_success() {
        anyhow::bail!("GET {url} returned {status}: {payload}");
    }
    Ok(payload)
}

/// Mint the token `lumen query` will carry, if the caller named an account to
/// mint it for (#2878, R3/R4).
///
/// The token is returned by value and lives only as long as the command that
/// asked for it. It is never written anywhere: not to a cache file, not to an
/// environment variable, not to stdout.
#[cfg(all(feature = "backup", feature = "delegated-auth"))]
async fn query_token(target: &QueryTarget) -> Result<Option<service_auth::k8s::ProjectedToken>> {
    let Some(client_sa) = target.client_sa.as_deref() else {
        return Ok(None);
    };
    // clap's `requires = "namespace"` already guarantees this pair arrives
    // together; the message is for the code path, not for the user.
    let namespace = target
        .namespace
        .as_deref()
        .context("--client-sa requires --namespace")?;
    let tokens = client_token_source(target.context.as_deref(), namespace, client_sa).await?;
    Ok(Some(first_mint(&tokens, namespace, client_sa).await?))
}

/// Without `delegated-auth` there is no TokenRequest client to mint with, so
/// `--client-sa` is refused rather than silently ignored. Sending an
/// unauthenticated request in response to an explicit request to authenticate
/// is the one behaviour that must not happen.
#[cfg(all(feature = "backup", not(feature = "delegated-auth")))]
async fn query_token(target: &QueryTarget) -> Result<Option<service_auth::k8s::ProjectedToken>> {
    if target.client_sa.is_some() {
        anyhow::bail!(
            "--client-sa needs the `delegated-auth` feature (rebuild with \
             `cargo build -p lumen --features delegated-auth`); this binary cannot mint a \
             ServiceAccount token"
        );
    }
    Ok(None)
}

/// `lumen query` dispatch (#1321, R3): resolves `--url` via `QueryTarget`,
/// assembles the exact wire body, and POSTs/GETs it. No REPL, no new HTTP
/// endpoint.
///
/// Credential resolution is a single line and it is not a lookup (#2878): with
/// `--client-sa`, a token is minted for the named account, carried in memory
/// for this one request, and dropped with the command. Without it the request
/// goes out as whoever the network says it is and a serving instance under
/// `auth: required` answers 401 — the honest failure (AC4). What #2873 removed
/// and did not come back is the silent path: quietly reaching into a Secret
/// for a shared token nobody named.
#[cfg(feature = "backup")]
pub(crate) async fn dispatch_query(args: QueryArgs) -> Result<()> {
    match args.command {
        QueryCommand::Index(args) => {
            let base = resolve_base_url(&args.target)?;
            let token = query_token(&args.target).await?;
            let (path, body) = build_index_body(&args.collection, &args.items)?;
            let resp = http_post_json(&base, &path, body, token.as_ref()).await?;
            println!("{}", serde_json::to_string_pretty(&resp)?);
            Ok(())
        }
        QueryCommand::Search(args) => {
            let base = resolve_base_url(&args.target)?;
            let token = query_token(&args.target).await?;
            let (path, body) = build_search_body(&args)?;
            let resp = http_post_json(&base, &path, body, token.as_ref()).await?;
            println!("{}", serde_json::to_string_pretty(&resp)?);
            Ok(())
        }
        QueryCommand::Duplicates(args) => {
            let base = resolve_base_url(&args.target)?;
            let token = query_token(&args.target).await?;
            let (path, body) = build_duplicates_body(&args)?;
            let resp = http_post_json(&base, &path, body, token.as_ref()).await?;
            println!("{}", serde_json::to_string_pretty(&resp)?);
            Ok(())
        }
        QueryCommand::Collections(args) => match args.command {
            QueryCollectionsCommand::List(args) => {
                let base = resolve_base_url(&args.target)?;
                let token = query_token(&args.target).await?;
                let resp = http_get_json(&base, "/collections", token.as_ref()).await?;
                println!("{}", serde_json::to_string_pretty(&resp)?);
                Ok(())
            }
        },
    }
}

#[cfg(not(feature = "backup"))]
pub(crate) async fn dispatch_query(_args: QueryArgs) -> Result<()> {
    anyhow::bail!(
        "this lumen build was compiled without backup support; rebuild with \
         `--features backup` (or `operator`, which pulls it in — the published \
         image includes both)"
    )
}

#[cfg(test)]
mod tests;
