//! A field's snapshot after a seal: a hash field keeps its sealed base values.

use crate::index::domain::field_index::FieldIndex;
use crate::index::domain::hash_index::HashIndex;
use crate::index::domain::interner::Interner;
use crate::index::infrastructure::snapshot_v1::FieldIndexSnapshot;

#[test]
fn hash_snapshot_keeps_sealed_base_values() {
    let dir = tempfile::tempdir().unwrap();
    let mut interner = Interner::default();
    let id = interner.intern("document");
    let mut hash = HashIndex::default();
    hash.forward.insert(id, 42);
    let mut field = FieldIndex::Hash(hash);
    field
        .seal_to_segment("sig", dir.path(), 1, 7, &|_| true)
        .unwrap();
    let FieldIndexSnapshot::Hash { forward, .. } = field.to_snapshot(&interner, &[id]).unwrap()
    else {
        panic!("hash snapshot")
    };
    assert_eq!(
        forward.get("document"),
        Some(&42),
        "sealed hash value missing from backup"
    );
}
