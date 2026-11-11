//! Seeded dataset of the performance mode (#263): 500,000 documents in 182 collections, 14
//! fields each (three of them ngram text), plus separate Flat and HNSW vector cases. Pure: no
//! HTTP.

use serde_json::{json, Map, Value};

pub(super) const COLLECTIONS: usize = 182;
pub(super) const DOCUMENTS: usize = 500_000;
pub(super) const FIELDS_PER_DOC: usize = 14;
pub(super) const VECTOR_DIM: usize = 16;
pub(super) const VECTOR_DOCS: usize = 20_000;

/// The splitmix64 generator. Own implementation: no new dependency.
pub(super) struct SplitMix64(u64);

impl SplitMix64 {
    pub(super) fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub(super) fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

#[derive(Clone, Debug)]
pub(super) struct DatasetSpec {
    pub(super) seed: u64,
    pub(super) collections: usize,
    pub(super) documents: usize,
}

impl DatasetSpec {
    pub(super) fn full(seed: u64) -> Self {
        Self {
            seed,
            collections: COLLECTIONS,
            documents: DOCUMENTS,
        }
    }

    /// Documents of collection `c`: `documents / collections`, plus 1 for `c < documents % collections`.
    pub(super) fn docs_in_collection(&self, c: usize) -> usize {
        let base = self.documents / self.collections;
        let extra = usize::from(c < self.documents % self.collections);
        base + extra
    }

    pub(super) fn collection_id(c: usize) -> String {
        format!("perf-c{c:03}")
    }

    pub(super) fn external_id(c: usize, i: usize) -> String {
        format!("c{c:03}-d{i:07}")
    }

    pub(super) fn document(&self, c: usize, i: usize) -> Map<String, Value> {
        let mut rng = SplitMix64::new(self.seed ^ ((c as u64) << 40) ^ (i as u64));
        let mut doc = Map::new();
        for k in 0..5u64 {
            let r = rng.next_u64();
            doc.insert(
                format!("kw{k}"),
                json!(format!("k{k}-{}", r % (10 * (k + 1)))),
            );
        }
        for k in 0..4 {
            let r = rng.next_u64();
            doc.insert(format!("num{k}"), json!((r % 1_000_000) as f64));
        }
        let tags: Vec<String> = (0..3)
            .map(|_| format!("tag{}", rng.next_u64() % 100))
            .collect();
        doc.insert("tags".into(), json!(tags));
        let r = rng.next_u64();
        doc.insert("fp".into(), json!(format!("{r:016x}")));
        for k in 0..3 {
            let words: Vec<String> = (0..8)
                .map(|_| format!("w{}", rng.next_u64() % 500))
                .collect();
            doc.insert(format!("t{k}"), json!(words.join(" ")));
        }
        doc
    }
}

/// The schema of the 500,000-document collections: 14 fields, three ngram text fields.
pub(super) fn schema() -> Value {
    json!({"fields": {
        "kw0": {"type": "keyword"},
        "kw1": {"type": "keyword"},
        "kw2": {"type": "keyword"},
        "kw3": {"type": "keyword"},
        "kw4": {"type": "keyword"},
        "num0": {"type": "number"},
        "num1": {"type": "number"},
        "num2": {"type": "number"},
        "num3": {"type": "number"},
        "tags": {"type": "set"},
        "fp": {"type": "hash"},
        "t0": {"type": "text", "analyzer": "ngram"},
        "t1": {"type": "text", "analyzer": "ngram"},
        "t2": {"type": "text", "analyzer": "ngram"}
    }})
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum VectorCase {
    Flat,
    Hnsw,
}

impl VectorCase {
    pub(super) fn collection_id(self) -> &'static str {
        match self {
            VectorCase::Flat => "perf-vec-flat",
            VectorCase::Hnsw => "perf-vec-hnsw",
        }
    }

    pub(super) fn schema(self) -> Value {
        let backend = match self {
            VectorCase::Flat => "flat-cpu",
            VectorCase::Hnsw => "hnsw-cpu",
        };
        json!({"fields": {
            "vec": {"type": "vector", "dim": VECTOR_DIM, "metric": "l2", "backend": backend}
        }})
    }

    /// Both cases draw the same vectors; only the collection and the schema differ.
    pub(super) fn document(self, seed: u64, i: usize) -> Map<String, Value> {
        let mut rng = SplitMix64::new(seed ^ 0xA5A5_0000_0000_0000 ^ (i as u64));
        let values: Vec<f32> = (0..VECTOR_DIM)
            .map(|_| ((rng.next_u64() % 2001) as f32 - 1000.0) / 1000.0)
            .collect();
        let mut doc = Map::new();
        doc.insert("vec".into(), json!(values));
        doc
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_spec_has_the_issue_numbers() {
        let spec = DatasetSpec::full(7);
        assert_eq!(spec.collections, 182);
        assert_eq!(spec.documents, 500_000);
        let counts: Vec<usize> = (0..182).map(|c| spec.docs_in_collection(c)).collect();
        assert_eq!(counts.iter().sum::<usize>(), 500_000);
        assert_eq!(*counts.iter().min().expect("min"), 2747);
        assert_eq!(*counts.iter().max().expect("max"), 2748);
    }

    #[test]
    fn schema_has_14_fields_and_three_ngram_text_fields() {
        let fields = schema()["fields"].as_object().expect("fields").clone();
        assert_eq!(fields.len(), 14);
        let text: Vec<&Value> = fields.values().filter(|f| f["type"] == "text").collect();
        assert_eq!(text.len(), 3);
        for field in text {
            assert_eq!(field["analyzer"], "ngram");
        }
    }

    #[test]
    fn same_seed_gives_the_same_document() {
        assert_eq!(
            DatasetSpec::full(7).document(3, 5),
            DatasetSpec::full(7).document(3, 5)
        );
        assert_ne!(
            DatasetSpec::full(8).document(3, 5),
            DatasetSpec::full(7).document(3, 5)
        );
        assert_ne!(
            DatasetSpec::full(7).document(3, 6),
            DatasetSpec::full(7).document(3, 5)
        );
    }

    #[test]
    fn document_has_exactly_the_schema_fields() {
        let doc = DatasetSpec::full(7).document(0, 0);
        let schema = schema();
        let mut want: Vec<String> = schema["fields"]
            .as_object()
            .expect("fields")
            .keys()
            .cloned()
            .collect();
        let mut got: Vec<String> = doc.keys().cloned().collect();
        want.sort();
        got.sort();
        assert_eq!(got, want);
        assert_eq!(doc.len(), FIELDS_PER_DOC);
    }

    #[test]
    fn vector_cases_are_separate() {
        assert_ne!(
            VectorCase::Flat.collection_id(),
            VectorCase::Hnsw.collection_id()
        );
        assert!(!VectorCase::Flat.collection_id().starts_with("perf-c"));
        assert!(!VectorCase::Hnsw.collection_id().starts_with("perf-c"));
        let flat = VectorCase::Flat.schema();
        let hnsw = VectorCase::Hnsw.schema();
        assert_eq!(flat["fields"]["vec"]["backend"], "flat-cpu");
        assert_eq!(hnsw["fields"]["vec"]["backend"], "hnsw-cpu");
        assert_eq!(flat["fields"]["vec"]["dim"], 16);
        assert_eq!(hnsw["fields"]["vec"]["dim"], 16);
        let vec = VectorCase::Flat.document(7, 3)["vec"].clone();
        assert_eq!(vec.as_array().expect("array").len(), 16);
        assert_eq!(vec, VectorCase::Hnsw.document(7, 3)["vec"]);
    }
}
