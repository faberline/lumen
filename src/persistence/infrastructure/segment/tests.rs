use crate::persistence::infrastructure::segment::format::sortable_bits;

/// Unique temp path per test, cleaned up by the OS temp dir convention.
fn tmp_path(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("lumen-segment-{}-{}", std::process::id(), tag));
    std::fs::create_dir_all(&dir).unwrap();
    dir.join("col.lseg")
}

/// Same `sortable_bits` transform `storage::SortableF64::new` applies, so the
/// segment tests build probe keys identically to the runtime.
fn bits(x: f64) -> u64 {
    sortable_bits(x)
}

// -----------------------------------------------------------------------
// Variable-width column machinery (Phase 2e-A)
// -----------------------------------------------------------------------

/// Derive the inverted `terms` posting index from a dense `values` column —
/// the same fold the seal does, so the keyword writer's two arguments stay
/// consistent in the unit tests. Term `t` posts every docid whose value is `t`.
fn kw_terms(values: &[Option<&str>]) -> std::collections::BTreeMap<String, roaring::RoaringBitmap> {
    let mut terms: std::collections::BTreeMap<String, roaring::RoaringBitmap> =
        std::collections::BTreeMap::new();
    for (id, v) in values.iter().enumerate() {
        if let Some(s) = v {
            terms.entry((*s).to_string()).or_default().insert(id as u32);
        }
    }
    terms
}

/// Derive the inverted `elements` posting index from a dense `values` column
/// — the same fold the Set seal does, so the set writer's two arguments stay
/// consistent in the unit tests. Element `e` posts every docid whose member
/// set contains `e` (Phase 2h-2).
fn set_elems(
    values: &[Option<&[String]>],
) -> std::collections::BTreeMap<String, roaring::RoaringBitmap> {
    let mut elements: std::collections::BTreeMap<String, roaring::RoaringBitmap> =
        std::collections::BTreeMap::new();
    for (id, v) in values.iter().enumerate() {
        if let Some(members) = v {
            for m in members.iter() {
                elements.entry(m.clone()).or_default().insert(id as u32);
            }
        }
    }
    elements
}

mod codecs;

mod eid;

mod framing;

mod hash_vector;

mod keyword_set;

mod number;

mod text;
