// ---------------------------------------------------------------------------
// THE CRUX: RDB + AOF recovery WITHOUT broker replay (Stage 2 Phase 2f-3).
//
// The whole durability story end-to-end, with the log out of the picture:
//
//   1. A "live" engine applies ops 1..=A via `apply_raft_entry`, with every op
//      ALSO appended to an AOF.
//   2. At seq S (< A) the segment checkpoint is taken (`flush_to_segments`) and
//      the AOF is `truncate_through(S)`d — so on disk the RDB covers 1..=S and
//      the AOF covers S+1..=A.
//   3. "Restart": a FRESH engine reopens the segment dir (recovers to S), then
//      `replay_aof_into` replays S+1..=A (recovers to A).
//
// The restarted engine's query results — result sets, byte-identical f32 scores,
// retrieved field values, and ordered kNN — must equal the live engine at A. If
// the frame crc/len decode, the seq-skip boundary, or `truncate_through` is
// wrong, recovery diverges (or the torn-tail path panics) and this test fails.
// ---------------------------------------------------------------------------

use std::collections::BTreeMap;
use std::sync::Arc;

use storage_durable::FsyncPolicy;

use crate::index::application::engine::Engine;
use crate::ingest::domain::wal_record::WalRecord;
use crate::persistence::infrastructure::aof::aof_writer::AofWriter;
use crate::persistence::infrastructure::aof::crux_recovery_tests::battery::{battery, knn};
use crate::persistence::infrastructure::aof::frame::HEADER_LEN;
use crate::persistence::infrastructure::aof::replay::{replay_aof_into, AofReader};
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::shared_kernel::types::document::{FieldValue, IndexItem, IndexRequest};
use crate::shared_kernel::types::schema::{
    Analyzer, CreateCollectionRequest, FieldSpec, FieldType, VectorBackend, VectorMetric,
};

const DIM: usize = 4;

fn fieldspec(t: FieldType, analyzer: Option<Analyzer>) -> FieldSpec {
    FieldSpec {
        field_type: t,
        analyzer,
        multi: None,
        dim: None,
        metric: None,
        backend: None,
        quantize: None,
    }
}

fn vec_fieldspec() -> FieldSpec {
    FieldSpec {
        field_type: FieldType::Vector,
        analyzer: None,
        multi: None,
        dim: Some(DIM as u32),
        metric: Some(VectorMetric::L2),
        backend: Some(VectorBackend::FlatCpu),
        quantize: None,
    }
}

fn schema() -> CreateCollectionRequest {
    let mut fields = BTreeMap::new();
    fields.insert("num".into(), fieldspec(FieldType::Number, None));
    fields.insert("kw".into(), fieldspec(FieldType::Keyword, None));
    fields.insert("tags".into(), fieldspec(FieldType::Set, None));
    fields.insert(
        "body".into(),
        fieldspec(FieldType::Text, Some(Analyzer::WhitespaceLower)),
    );
    fields.insert("sig".into(), fieldspec(FieldType::Hash, None));
    fields.insert("emb".into(), vec_fieldspec());
    CreateCollectionRequest { fields }
}

/// Build an `Index` entry for one doc across all six fields.
fn index_entry(
    coll: &str,
    eid: &str,
    n: f64,
    kw: &str,
    tag: &str,
    tok: bool,
    sig: u64,
    emb: &[f32],
) -> RaftLogEntry {
    RaftLogEntry::Index {
        collection_id: coll.into(),
        req: IndexRequest {
            items: vec![
                IndexItem {
                    external_id: eid.into(),
                    field: "num".into(),
                    value: FieldValue::Number(n),
                    version: None,
                },
                IndexItem {
                    external_id: eid.into(),
                    field: "kw".into(),
                    value: FieldValue::String(kw.into()),
                    version: None,
                },
                IndexItem {
                    external_id: eid.into(),
                    field: "tags".into(),
                    value: FieldValue::StringList(vec![tag.into()]),
                    version: None,
                },
                IndexItem {
                    external_id: eid.into(),
                    field: "body".into(),
                    value: FieldValue::String(if tok {
                        "tok filler".into()
                    } else {
                        "filler".into()
                    }),
                    version: None,
                },
                IndexItem {
                    external_id: eid.into(),
                    field: "sig".into(),
                    value: FieldValue::String(format!("{sig:016x}")),
                    version: None,
                },
                IndexItem {
                    external_id: eid.into(),
                    field: "emb".into(),
                    value: FieldValue::Vector(emb.to_vec()),
                    version: None,
                },
            ],
            request_id: None,
        },
    }
}

/// The full op TRANSCRIPT, in apply order. Two collections, all field types;
/// base docs (1..=S region) then tail docs (S+1..=A region). Returns the
/// ordered RaftLogEntry list — applied with seq = index+1.
fn transcript() -> (Vec<RaftLogEntry>, usize) {
    let mut ops = Vec::new();
    ops.push(RaftLogEntry::CreateCollection {
        collection_id: "alpha".into(),
        req: schema(),
    });
    ops.push(RaftLogEntry::CreateCollection {
        collection_id: "beta".into(),
        req: schema(),
    });
    // Base docs (these end up under the segment checkpoint at S).
    let base = [
        ("d0", 1.0, "a", "red", true, 0u64, [0.1f32, 0.2, 0.3, 0.4]),
        ("d1", 3.0, "b", "blue", true, 3, [0.9, 0.8, 0.7, 0.6]),
        ("d2", 5.0, "a", "red", false, 7, [0.5, 0.5, 0.5, 0.5]),
        ("d3", 7.0, "c", "green", true, 1, [0.2, 0.4, 0.6, 0.8]),
    ];
    for (eid, n, kw, tag, tok, sig, emb) in base {
        ops.push(index_entry("alpha", eid, n, kw, tag, tok, sig, &emb));
        ops.push(index_entry(
            "beta",
            &format!("b{eid}"),
            n + 1.0,
            kw,
            tag,
            tok,
            sig + 1,
            &emb,
        ));
    }
    // S = number of ops so far (the checkpoint boundary).
    let s = ops.len();
    // Tail docs (these end up only in the AOF, S+1..=A).
    let tail = [
        (
            "d4",
            2.5,
            "b",
            "red",
            true,
            0u64,
            [0.11f32, 0.22, 0.33, 0.44],
        ),
        ("d5", 6.5, "a", "blue", true, 7, [0.6, 0.6, 0.6, 0.6]),
        ("d6", 8.5, "c", "green", false, 2, [0.3, 0.3, 0.3, 0.3]),
    ];
    for (eid, n, kw, tag, tok, sig, emb) in tail {
        ops.push(index_entry("alpha", eid, n, kw, tag, tok, sig, &emb));
        ops.push(index_entry(
            "beta",
            &format!("b{eid}"),
            n + 1.0,
            kw,
            tag,
            tok,
            sig + 1,
            &emb,
        ));
    }
    (ops, s)
}

#[test]
fn rdb_plus_aof_recovery_matches_live_without_nats() {
    let (ops, s) = transcript();
    let a = ops.len(); // every op applied; A = total.
    let qa = [0.15f32, 0.25, 0.35, 0.45];

    let dir = tempfile::tempdir().unwrap();
    let seg_dir = dir.path().join("segments");
    std::fs::create_dir_all(&seg_dir).unwrap();
    let aof_path = dir.path().join("aof.log");

    // --- LIVE: apply 1..=A, append every op to the AOF, checkpoint at S. ---
    let live = Arc::new(Engine::new());
    let mut aof = AofWriter::open_with_policy(&aof_path, FsyncPolicy::Always).unwrap();
    for (i, op) in ops.iter().enumerate() {
        let seq = (i + 1) as u64;
        live.apply_raft_entry(op.clone()).unwrap();
        aof.append(seq, &WalRecord::new(op.clone())).unwrap();
        if seq == s as u64 {
            // RDB checkpoint at S, then trim the AOF through S — so on disk the
            // segment covers 1..=S and the AOF covers S+1..=A only.
            live.flush_to_segments(&seg_dir, s as u64).unwrap();
            aof.truncate_through(s as u64).unwrap();
        }
    }
    aof.sync().unwrap();

    // On-disk shape sanity: the AOF now holds exactly S+1..=A.
    let mut remaining = Vec::new();
    AofReader::replay(&aof_path, 0, |seq, _| remaining.push(seq)).unwrap();
    assert_eq!(
        remaining,
        ((s as u64 + 1)..=(a as u64)).collect::<Vec<_>>(),
        "AOF must hold exactly the post-checkpoint tail"
    );

    let live_alpha = battery(&live, "alpha");
    let live_beta = battery(&live, "beta");
    let live_knn_alpha = knn(&live, "alpha", &qa);
    let live_knn_beta = knn(&live, "beta", &qa);

    // --- RESTART: fresh engine, RDB reopen → AOF replay (no broker tail). ---
    let restarted = Arc::new(Engine::new());
    let s_recovered = restarted.reopen_from_segment_dir(&seg_dir).unwrap();
    assert_eq!(
        s_recovered, s as u64,
        "RDB must restore to the checkpoint seq S"
    );
    let a_recovered = replay_aof_into(&restarted, &aof_path, s_recovered).unwrap();
    assert_eq!(a_recovered, a as u64, "AOF replay must advance to A");

    // The restarted engine must be byte-identical to the live engine at A.
    assert_eq!(
        battery(&restarted, "alpha"),
        live_alpha,
        "alpha legs diverged after RDB+AOF recovery"
    );
    assert_eq!(
        battery(&restarted, "beta"),
        live_beta,
        "beta legs diverged after RDB+AOF recovery"
    );
    assert_eq!(
        knn(&restarted, "alpha", &qa),
        live_knn_alpha,
        "alpha kNN diverged after RDB+AOF recovery"
    );
    assert_eq!(
        knn(&restarted, "beta", &qa),
        live_knn_beta,
        "beta kNN diverged after RDB+AOF recovery"
    );
    assert_eq!(
        restarted.stats("alpha").unwrap().documents_indexed,
        live.stats("alpha").unwrap().documents_indexed
    );
    assert_eq!(
        restarted.stats("beta").unwrap().documents_indexed,
        live.stats("beta").unwrap().documents_indexed
    );
}

/// Recovery is robust to a torn AOF tail: a crash mid-append leaves a partial
/// frame; recovery replays the good prefix and the engine still converges to
/// the last DURABLE op (no panic, no divergence on the good prefix).
#[test]
fn recovery_tolerates_torn_aof_tail() {
    let (ops, s) = transcript();
    let qa = [0.15f32, 0.25, 0.35, 0.45];
    let dir = tempfile::tempdir().unwrap();
    let seg_dir = dir.path().join("segments");
    std::fs::create_dir_all(&seg_dir).unwrap();
    let aof_path = dir.path().join("aof.log");

    // Apply + append all but the LAST op durably; checkpoint at S.
    let live = Arc::new(Engine::new());
    let mut aof = AofWriter::open_with_policy(&aof_path, FsyncPolicy::Always).unwrap();
    let last = ops.len() - 1;
    for (i, op) in ops.iter().enumerate().take(last) {
        let seq = (i + 1) as u64;
        live.apply_raft_entry(op.clone()).unwrap();
        aof.append(seq, &WalRecord::new(op.clone())).unwrap();
        if seq == s as u64 {
            live.flush_to_segments(&seg_dir, s as u64).unwrap();
            aof.truncate_through(s as u64).unwrap();
        }
    }
    aof.sync().unwrap();
    let good_len = std::fs::metadata(&aof_path).unwrap().len();

    // Simulate a crash mid-append of the FINAL op: a header whose length
    // overruns EOF, plus a stray byte.
    {
        use std::io::Write as _;
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&aof_path)
            .unwrap();
        let mut hdr = [0u8; HEADER_LEN];
        hdr[0..8].copy_from_slice(&((ops.len()) as u64).to_le_bytes());
        hdr[8..12].copy_from_slice(&999_999u32.to_le_bytes());
        f.write_all(&hdr).unwrap();
        f.write_all(&[0x42]).unwrap();
        f.sync_all().unwrap();
    }

    // The live oracle: exactly the durable prefix (NOT the torn final op).
    let live_alpha = battery(&live, "alpha");
    let live_beta = battery(&live, "beta");
    let live_knn = knn(&live, "alpha", &qa);

    // Recovery: RDB reopen → AOF replay. The torn tail is skipped cleanly.
    let restarted = Arc::new(Engine::new());
    let s_rec = restarted.reopen_from_segment_dir(&seg_dir).unwrap();
    assert_eq!(s_rec, s as u64);
    let a_rec = replay_aof_into(&restarted, &aof_path, s_rec).unwrap();
    assert_eq!(
        a_rec,
        (ops.len() - 1) as u64,
        "torn final frame must not be replayed"
    );

    assert_eq!(
        battery(&restarted, "alpha"),
        live_alpha,
        "alpha diverged after torn-tail recovery"
    );
    assert_eq!(
        battery(&restarted, "beta"),
        live_beta,
        "beta diverged after torn-tail recovery"
    );
    assert_eq!(
        knn(&restarted, "alpha", &qa),
        live_knn,
        "kNN diverged after torn-tail recovery"
    );

    // And the next writer open truncates the torn tail back to the prefix.
    let _w = AofWriter::open(&aof_path).unwrap();
    assert_eq!(std::fs::metadata(&aof_path).unwrap().len(), good_len);
}

mod battery;
