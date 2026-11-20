//! `lumen migrate`: offline Search v2 migration tools. Each command reads JSON
//! from stdin and writes JSON to stdout. None of them contacts a runtime.

use std::io::{Read, Write};

use anyhow::{bail, Context, Result};
use serde_json::{json, Map, Value};

use crate::cli::{MigrateArgs, MigrateCmd, MigrateSearchRequestArgs};

mod search_request;
use search_request::convert_search_request;

const SHAPE_REFUSAL: &str = r#"schema must be an object with a "fields" object"#;
const FACETABLE_NOTE: &str = "facetable is not set; enabling it later needs a shadow rebuild";

/// `lumen migrate collection-schema`: stdin to stdout, or an error with
/// nothing on stdout when the schema is refused.
pub(crate) fn dispatch(args: MigrateArgs) -> Result<()> {
    match args.cmd {
        MigrateCmd::CollectionSchema => {
            let mut input = String::new();
            std::io::stdin()
                .read_to_string(&mut input)
                .context("read stdin")?;
            let value: Value = serde_json::from_str(&input).context("stdin is not valid JSON")?;
            match convert_collection_schema(&value) {
                Ok(output) => {
                    let mut text = serde_json::to_string_pretty(&output)?;
                    text.push('\n');
                    std::io::stdout().write_all(text.as_bytes())?;
                    Ok(())
                }
                Err(refusals) => bail!("refused: {}", refusals.join("; ")),
            }
        }
        MigrateCmd::SearchRequest(args) => run_search_request(args),
    }
}

/// `lumen migrate search-request`: a refused request writes nothing to stdout,
/// `--out` or `--report`.
fn run_search_request(args: MigrateSearchRequestArgs) -> Result<()> {
    let input = match &args.input {
        Some(path) => std::fs::read_to_string(path).context("cannot read --in file")?,
        None => {
            let mut text = String::new();
            std::io::stdin()
                .read_to_string(&mut text)
                .context("read stdin")?;
            text
        }
    };
    let value: Value = serde_json::from_str(&input).context("input is not valid JSON")?;
    let (changes, refusals, converted) = match convert_search_request(&value) {
        Ok((request, changes)) => (changes, Vec::new(), Some(request)),
        Err(refusals) => (Vec::new(), refusals, None),
    };

    let change_rows: Vec<Value> = changes
        .iter()
        .map(|c| json!({"path": c.path, "from": c.from, "to": c.to}))
        .collect();
    // A refused request still writes its report, with the refusals named by path.
    let mut report = serde_json::to_string_pretty(&json!({
        "changes": change_rows,
        "refusals": refusals,
    }))?;
    report.push('\n');
    match &args.report {
        Some(path) => std::fs::write(path, report).context("cannot write --report file")?,
        None => eprint!("{report}"),
    }

    // A refusal fails with nothing on stdout and no --out file.
    let Some(request) = converted else {
        bail!("refused: {}", refusals.join("; "));
    };
    let mut text = serde_json::to_string_pretty(&request)?;
    text.push('\n');
    match &args.out {
        Some(path) => std::fs::write(path, text).context("cannot write --out file")?,
        None => std::io::stdout().write_all(text.as_bytes())?,
    }
    Ok(())
}

/// Pure transform from a 0.4 collection schema to the 0.5 schema plus report.
/// `Err` holds one message per refused field, or one for a bad top-level shape.
pub(crate) fn convert_collection_schema(input: &Value) -> Result<Value, Vec<String>> {
    let top = input
        .as_object()
        .ok_or_else(|| vec![SHAPE_REFUSAL.to_string()])?;
    let fields = top
        .get("fields")
        .and_then(Value::as_object)
        .ok_or_else(|| vec![SHAPE_REFUSAL.to_string()])?;

    // Sorted names make refusals, conversions and report rows stable.
    let mut names: Vec<&String> = fields.keys().collect();
    names.sort();

    let mut refusals = Vec::new();
    let mut out_fields = Map::new();
    let mut conversions = Vec::new();
    let mut rebuild_required = Vec::new();
    let mut facetable_notes = Vec::new();

    for name in names {
        let Some(obj) = fields[name].as_object() else {
            refusals.push(format!("field \"{name}\": not an object"));
            continue;
        };
        let Some(ty) = obj.get("type").and_then(Value::as_str) else {
            refusals.push(format!("field \"{name}\": type is missing or not a string"));
            continue;
        };
        match ty {
            "text" | "vector" | "hash" => {
                out_fields.insert(name.clone(), fields[name].clone());
            }
            "keyword" => {
                out_fields.insert(name.clone(), fields[name].clone());
                facetable_notes.push(json!({"field": name, "note": FACETABLE_NOTE}));
            }
            "number" | "set" => {
                let mut ambiguous = false;
                for key in ["multi", "facetable"] {
                    if obj.contains_key(key) {
                        refusals.push(format!(
                            "field \"{name}\": ambiguous: {key} is a 0.5 option on a legacy {ty} field"
                        ));
                        ambiguous = true;
                    }
                }
                if ambiguous {
                    continue;
                }
                let mut out = obj.clone();
                // `insert` on an existing key keeps its position; `multi` is new and goes last.
                let (to_type, to_label) = if ty == "number" {
                    ("float64", "float64")
                } else {
                    ("keyword", "keyword+multi")
                };
                out.insert("type".to_string(), json!(to_type));
                if ty == "set" {
                    out.insert("multi".to_string(), json!(true));
                }
                out_fields.insert(name.clone(), Value::Object(out));
                conversions.push(json!({"field": name, "from": ty, "to": to_label}));
                rebuild_required.push(json!(name));
                facetable_notes.push(json!({"field": name, "note": FACETABLE_NOTE}));
            }
            other => refusals.push(format!(
                "field \"{name}\": type \"{other}\" is not a 0.4 type"
            )),
        }
    }

    if !refusals.is_empty() {
        return Err(refusals);
    }

    // `insert` on the existing "fields" key keeps its place among the other top-level keys.
    let mut schema = top.clone();
    schema.insert("fields".to_string(), Value::Object(out_fields));
    Ok(json!({
        "report": {
            "conversions": conversions,
            "facetable_notes": facetable_notes,
            "rebuild_required": rebuild_required,
            "refusals": [],
        },
        "schema": schema,
    }))
}

#[cfg(test)]
mod tests {
    use super::convert_collection_schema;
    use serde_json::{json, Value};

    const SHAPE: &str = r#"schema must be an object with a "fields" object"#;

    fn ok(input: Value) -> Value {
        convert_collection_schema(&input).expect("conversion should succeed")
    }

    fn refused(input: Value) -> Vec<String> {
        convert_collection_schema(&input).expect_err("conversion should be refused")
    }

    #[test]
    fn number_becomes_float64() {
        let out = ok(json!({"fields": {"age": {"type": "number"}}}));
        assert_eq!(out["schema"]["fields"]["age"], json!({"type": "float64"}));
        assert_eq!(
            out["report"]["conversions"],
            json!([{"field": "age", "from": "number", "to": "float64"}])
        );
        assert_eq!(out["report"]["rebuild_required"], json!(["age"]));
    }

    #[test]
    fn set_becomes_keyword_multi() {
        let out = ok(json!({"fields": {"tags": {"type": "set"}}}));
        assert_eq!(
            out["schema"]["fields"]["tags"],
            json!({"type": "keyword", "multi": true})
        );
        assert_eq!(
            out["report"]["conversions"][0]["to"],
            json!("keyword+multi")
        );
    }

    #[test]
    fn other_types_are_copied_unchanged() {
        let fields = json!({
            "bio": {"type": "text", "analyzer": "whitespace_lower"},
            "email": {"type": "keyword"},
            "embedding": {"type": "vector", "dim": 768, "metric": "cosine", "backend": "hnsw-cpu"},
            "avatar_phash": {"type": "hash"}
        });
        let out = ok(json!({"fields": fields.clone()}));
        assert_eq!(out["schema"]["fields"], fields);
        assert_eq!(out["report"]["conversions"], json!([]));
        assert_eq!(out["report"]["rebuild_required"], json!([]));
        assert_eq!(
            out["report"]["facetable_notes"],
            json!([{"field": "email", "note": "facetable is not set; enabling it later needs a shadow rebuild"}])
        );
    }

    #[test]
    fn facetable_notes_cover_keyword_and_float64_and_converted_set() {
        let out = ok(json!({"fields": {
            "email": {"type": "keyword"},
            "age": {"type": "number"},
            "tags": {"type": "set"}
        }}));
        let notes: Vec<&str> = out["report"]["facetable_notes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|n| n["field"].as_str().unwrap())
            .collect();
        assert_eq!(notes, ["age", "email", "tags"]);
        for (_, field) in out["schema"]["fields"].as_object().unwrap() {
            assert!(field.get("facetable").is_none(), "facetable key in {field}");
        }
    }

    #[test]
    fn unknown_and_already_0_5_types_are_refused() {
        let errs = refused(json!({"fields": {
            "a": {"type": "int64"},
            "b": {"type": "blob"}
        }}));
        assert!(
            errs.contains(&r#"field "a": type "int64" is not a 0.4 type"#.to_string()),
            "{errs:?}"
        );
        assert!(
            errs.contains(&r#"field "b": type "blob" is not a 0.4 type"#.to_string()),
            "{errs:?}"
        );
    }

    #[test]
    fn missing_type_and_non_object_field_are_refused() {
        let errs = refused(json!({"fields": {"a": {}, "b": 5}}));
        assert!(
            errs.contains(&r#"field "a": type is missing or not a string"#.to_string()),
            "{errs:?}"
        );
        assert!(
            errs.contains(&r#"field "b": not an object"#.to_string()),
            "{errs:?}"
        );
    }

    #[test]
    fn ambiguous_multi_or_facetable_on_legacy_field_is_refused() {
        // Negative control: without the ambiguity check these two fields would
        // convert silently and the test would fail.
        let errs = refused(json!({"fields": {
            "tags": {"type": "set", "multi": false},
            "age": {"type": "number", "facetable": true}
        }}));
        assert!(
            errs.contains(
                &r#"field "age": ambiguous: facetable is a 0.5 option on a legacy number field"#
                    .to_string()
            ),
            "{errs:?}"
        );
        assert!(
            errs.contains(
                &r#"field "tags": ambiguous: multi is a 0.5 option on a legacy set field"#
                    .to_string()
            ),
            "{errs:?}"
        );
    }

    #[test]
    fn bad_top_level_shape_is_refused() {
        for input in [json!([]), json!({}), json!({"fields": 3})] {
            assert_eq!(refused(input), vec![SHAPE.to_string()]);
        }
    }

    #[test]
    fn output_is_stable_for_any_input_key_order() {
        let a: Value =
            serde_json::from_str(r#"{"fields":{"b":{"type":"text"},"a":{"type":"number"}}}"#)
                .unwrap();
        let b: Value =
            serde_json::from_str(r#"{"fields":{"a":{"type":"number"},"b":{"type":"text"}}}"#)
                .unwrap();
        assert_eq!(
            serde_json::to_string_pretty(&ok(a)).unwrap(),
            serde_json::to_string_pretty(&ok(b)).unwrap()
        );
    }
}
