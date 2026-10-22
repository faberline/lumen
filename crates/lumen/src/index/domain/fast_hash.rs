//! The hash maps and sets the in-memory indexes key by ids and terms, over the
//! non-cryptographic FxHash.

use rustc_hash::FxHashMap;

pub(in crate::index) type FastHashMap<K, V> = FxHashMap<K, V>;

pub(super) type FastHashSet<K> = rustc_hash::FxHashSet<K>;
