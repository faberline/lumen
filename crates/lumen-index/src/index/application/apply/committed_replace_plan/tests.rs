use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

use crate::index::application::apply::committed_index_plan::PlanView;
use crate::index::application::apply::committed_replace_plan::{
    OldFieldsBound, ParsedValue, ParsedValues, ReplacePlanView,
};
use crate::ingest::domain::wal_record::WalRecord;
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::shared_kernel::types::document::{FieldValue, IndexItem, IndexRequest};
use crate::shared_kernel::types::schema::FieldType;

#[derive(Default)]
struct View {
    fields: BTreeMap<String, FieldType>,
    ids: BTreeMap<String, u32>,
    coverage: BTreeMap<u32, Vec<String>>,
    versions: BTreeMap<u32, u64>,
    unchanged: BTreeSet<(u32, String)>,
}
impl PlanView for View {
    fn engine_epoch(&self) -> u64 {
        1
    }
    fn collection_generation(&self) -> u64 {
        2
    }
    fn schema_version(&self) -> u32 {
        3
    }
    fn data_version(&self) -> u64 {
        4
    }
    fn revision(&self) -> u64 {
        5
    }
    fn interner_len(&self) -> usize {
        10
    }
    fn is_live(&self) -> bool {
        true
    }
    fn field_type(&self, f: &str) -> Option<FieldType> {
        self.fields.get(f).copied()
    }
    fn vector_dimension(&self, f: &str) -> Option<u32> {
        (f == "vec").then_some(2)
    }
    fn id(&self, e: &str) -> Option<u32> {
        self.ids.get(e).copied()
    }
    fn has_cell(&self, id: u32, f: &str) -> bool {
        self.coverage
            .get(&id)
            .is_some_and(|v| v.iter().any(|x| x == f))
    }
    fn cell_version(&self, _: u32, _: &str) -> Option<u64> {
        None
    }
    fn request_deadline(&self, _: &str) -> Option<Instant> {
        None
    }
}
impl ReplacePlanView for View {
    fn doc_version(&self, id: u32) -> Option<u64> {
        self.versions.get(&id).copied()
    }
    fn old_fields(&self, id: u32) -> Option<&[String]> {
        self.coverage.get(&id).map(Vec::as_slice)
    }
    fn old_fields_bound(&self, id: u32) -> OldFieldsBound {
        let fields = self.old_fields(id).unwrap_or_default();
        OldFieldsBound {
            count: fields.len(),
            copied_bytes: fields.iter().map(String::len).sum(),
        }
    }
    fn existing_unchanged(&self, id: u32, f: &str, _: FieldType, _: usize, _: Option<u64>) -> bool {
        self.unchanged.contains(&(id, f.into()))
    }
}
fn wire(items: Vec<IndexItem>) -> Vec<u8> {
    WalRecord::new(RaftLogEntry::Index {
        collection_id: "docs".into(),
        req: IndexRequest {
            request_id: None,
            items,
        },
    })
    .encode()
    .unwrap()
}
fn item(id: &str, field: &str, value: FieldValue) -> IndexItem {
    IndexItem {
        external_id: id.into(),
        field: field.into(),
        value,
        version: None,
    }
}
fn view() -> View {
    View {
        fields: BTreeMap::from([
            ("kw".into(), FieldType::Keyword),
            ("text".into(), FieldType::Text),
            ("vec".into(), FieldType::Vector),
        ]),
        ids: BTreeMap::from([("old".into(), 2)]),
        coverage: BTreeMap::from([(2, vec!["kw".into(), "text".into(), "vec".into()])]),
        ..View::default()
    }
}
fn parsed() -> ParsedValues {
    BTreeMap::from([
        (
            0,
            ParsedValue {
                hash: None,
                checksum: Some(11),
            },
        ),
        (
            1,
            ParsedValue {
                hash: None,
                checksum: Some(12),
            },
        ),
        (
            2,
            ParsedValue {
                hash: None,
                checksum: Some(13),
            },
        ),
    ])
}

mod bounds;
mod items;
