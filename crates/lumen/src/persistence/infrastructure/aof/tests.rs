use std::collections::BTreeMap;
use std::path::Path;

use crate::ingest::domain::wal_record::WalRecord;
use crate::persistence::infrastructure::aof::replay::AofReader;
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::shared_kernel::types::document::{FieldValue, IndexItem, IndexRequest};
use crate::shared_kernel::types::query::{MatchOp, MatchQuery, QueryNode, TermQuery};
use crate::shared_kernel::types::schema::{CreateCollectionRequest, FieldSpec, FieldType};
use crate::shared_kernel::types::search::SearchRequest;

fn create_entry(coll: &str) -> RaftLogEntry {
    RaftLogEntry::CreateCollection {
        collection_id: coll.into(),
        req: CreateCollectionRequest {
            fields: {
                let mut f = BTreeMap::new();
                f.insert(
                    "email".to_string(),
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
                f
            },
        },
    }
}

fn index_entry(coll: &str, eid: &str, val: &str) -> RaftLogEntry {
    RaftLogEntry::Index {
        collection_id: coll.into(),
        req: IndexRequest {
            items: vec![IndexItem {
                external_id: eid.into(),
                field: "email".into(),
                value: FieldValue::String(val.into()),
                version: None,
            }],
            request_id: None,
        },
    }
}

fn rec(entry: RaftLogEntry) -> WalRecord {
    WalRecord::new(entry)
}

/// Collect (seq, record-debug) by replaying with from_seq = 0.
fn replay_seqs(path: &Path, from: u64) -> Vec<u64> {
    let mut out = Vec::new();
    AofReader::replay(path, from, |seq, _rec| out.push(seq)).unwrap();
    out
}

fn term_query(field: &str, value: &str) -> SearchRequest {
    SearchRequest {
        query: QueryNode::Term(TermQuery {
            field: field.into(),
            value: FieldValue::String(value.into()),
        }),
        limit: 10,
        offset: 0,
        cursor: None,
        routing_key: None,
        sort: None,
        track_total: true,
        collapse: None,
    }
}

fn text_match_query(field: &str, text: &str) -> SearchRequest {
    SearchRequest {
        query: QueryNode::Match(MatchQuery {
            field: field.into(),
            text: text.into(),
            op: MatchOp::And,
        }),
        limit: 10,
        offset: 0,
        cursor: None,
        routing_key: None,
        sort: None,
        track_total: true,
        collapse: None,
    }
}

mod capacity_maintainer;

mod reader;

mod replay_admission;

mod writer;

// Candidate AOF red tests to place in `aof::tests` after the cursor seam lands:
//
// 1. replay_admission_waits_before_mutating_or_advancing:
//    create a budget-limited Engine, retain a checkpointable record charge to
//    fill its budget, append the next create/index frame, and start replay on a
//    thread. Assert the query and capture sequence remain at the prefix while
//    the replay thread blocks. Drive the independent checkpoint, join replay,
//    then assert the new document and its exact sequence are present after cold
//    reopen.
//
// 2. replay_preparation_error_keeps_frame_and_watermark:
//    arrange a record whose admitted prepared-text staging fails through the
//    existing test failure seam. Assert replay returns Err, its sequence stays
//    at the prefix, and a second AOF reader still returns that same frame.
