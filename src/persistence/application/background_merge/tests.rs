use crate::index::application::engine::Engine;
use crate::persistence::domain::generation_manifest::{
    SegmentGenerationManifest, SegmentKind, SegmentRole,
};
use crate::persistence::infrastructure::segment_rdb_store::manifest_io::read_generation_manifest;
use crate::persistence::infrastructure::segment_rdb_store::SegmentRdbStore;
use crate::shared_kernel::types::{
    document::{FieldValue, IndexItem, IndexRequest},
    query::{QueryNode, TermQuery},
    schema::{CreateCollectionRequest, FieldSpec, FieldType},
    search::SearchRequest,
};
use std::collections::BTreeMap;
use std::path::Path;
use storage_durable::CurrentTarget;

fn capacity_schema() -> CreateCollectionRequest {
    let mut fields = BTreeMap::new();
    for field in ["a_capacity", "z_unrelated"] {
        fields.insert(
            field.to_owned(),
            FieldSpec {
                field_type: FieldType::Keyword,
                analyzer: None,
                multi: None,
                dim: None,
                metric: None,
                backend: None,
                quantize: None,
            },
        );
    }
    CreateCollectionRequest { fields }
}

fn index_fields(engine: &Engine, a_value: &str, z_value: Option<&str>) {
    let mut items = vec![IndexItem {
        external_id: "row".into(),
        field: "a_capacity".into(),
        value: FieldValue::String(a_value.into()),
        version: None,
    }];
    if let Some(z_value) = z_value {
        items.push(IndexItem {
            external_id: "row".into(),
            field: "z_unrelated".into(),
            value: FieldValue::String(z_value.into()),
            version: None,
        });
    }
    engine
        .index(
            "u",
            IndexRequest {
                items,
                request_id: None,
            },
        )
        .unwrap();
}

fn current_manifest(store: &SegmentRdbStore) -> SegmentGenerationManifest {
    let CurrentTarget::Generation(name) = store.generations.read_current().unwrap() else {
        panic!("capacity fixture must publish CURRENT")
    };
    read_generation_manifest(&store.root.join(name.as_str())).unwrap()
}

fn field_delta_count(manifest: &SegmentGenerationManifest, field: &str) -> usize {
    manifest.collections[0]
        .segments
        .iter()
        .filter(|segment| {
            segment.role == SegmentRole::Field
                && segment.kind == SegmentKind::Delta
                && segment.field.as_deref() == Some(field)
        })
        .count()
}

fn has_capacity_value(engine: &Engine, value: &str) -> bool {
    !engine
        .search(
            "u",
            SearchRequest {
                query: QueryNode::Term(TermQuery {
                    field: "a_capacity".into(),
                    value: FieldValue::String(value.into()),
                }),
                limit: 10,
                offset: 0,
                cursor: None,
                routing_key: None,
                sort: None,
                track_total: true,
                collapse: None,
            },
        )
        .unwrap()
        .hits
        .is_empty()
}

fn keyword_schema(fields: &[&str]) -> CreateCollectionRequest {
    let mut map = BTreeMap::new();
    for field in fields {
        map.insert(
            (*field).to_owned(),
            FieldSpec {
                field_type: FieldType::Keyword,
                analyzer: None,
                multi: None,
                dim: None,
                metric: None,
                backend: None,
                quantize: None,
            },
        );
    }
    CreateCollectionRequest { fields: map }
}

fn put_row(engine: &Engine, collection: &str, field: &str, id: &str, value: &str) {
    engine
        .index(
            collection,
            IndexRequest {
                items: vec![IndexItem {
                    external_id: id.into(),
                    field: field.into(),
                    value: FieldValue::String(value.into()),
                    version: None,
                }],
                request_id: None,
            },
        )
        .unwrap();
}

fn published_merge_jobs(store: &SegmentRdbStore) -> u64 {
    store
        .background
        .state
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .published_revision
}

/// Walk one generation directory, returning `(files, directories)` under
/// the subtree rooted at `path`.
fn count_tree(path: &Path) -> (usize, usize) {
    let mut files = 0;
    let mut dirs = 0;
    let Ok(entries) = std::fs::read_dir(path) else {
        return (0, 0);
    };
    for entry in entries.flatten() {
        let metadata = std::fs::symlink_metadata(entry.path()).unwrap();
        if metadata.is_dir() {
            dirs += 1;
            let (nested_files, nested_dirs) = count_tree(&entry.path());
            files += nested_files;
            dirs += nested_dirs;
        } else {
            files += 1;
        }
    }
    (files, dirs)
}

mod capacity_progress;

mod capacity_wait;

mod link_cost;

mod phase_metrics;

mod scratch_cleanup;

mod whole_delta_stack;

mod window_selection;
