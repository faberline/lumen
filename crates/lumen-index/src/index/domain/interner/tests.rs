use crate::index::domain::fast_hash::FastHashMap;
use crate::index::domain::interner::{Interner, InternerBucket};

#[test]
fn interner_retirement_compacts_a_collision_bucket() {
    let mut interner = Interner {
        to_hash: FastHashMap::default(),
        to_eid: vec!["first".into(), "second".into()],
    };
    interner.to_hash.insert(7, InternerBucket::Many(vec![0, 1]));

    interner.remove_hash_id(7, 1);
    assert!(matches!(
        interner.to_hash.get(&7),
        Some(InternerBucket::One(0))
    ));
    interner.remove_hash_id(7, 0);
    assert!(interner.to_hash.is_empty());
}
