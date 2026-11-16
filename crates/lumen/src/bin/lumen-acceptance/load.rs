//! Batch shapes and the seeded load generator of the performance mode (#263). Pure: it builds
//! requests and counts them, but sends nothing.

use anyhow::{bail, Result};
use serde_json::{json, Value};

use crate::dataset::{DatasetSpec, SplitMix64};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Shape {
    Index(usize),
    Replace(usize),
    Unindex(usize),
}

/// The batch shapes of the performance mode, in this fixed order.
pub(super) const SHAPES: [Shape; 8] = [
    Shape::Index(1),
    Shape::Index(100),
    Shape::Index(1000),
    Shape::Replace(1),
    Shape::Replace(32),
    Shape::Unindex(1),
    Shape::Unindex(100),
    Shape::Unindex(1000),
];

/// Three separate counters: requests, field items and complete document operations.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct Counters {
    pub(super) requests: u64,
    pub(super) field_items: u64,
    pub(super) document_ops: u64,
}

impl Counters {
    pub(super) fn add(&mut self, other: Counters) {
        self.requests += other.requests;
        self.field_items += other.field_items;
        self.document_ops += other.document_ops;
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(super) struct Request {
    pub(super) method: &'static str,
    pub(super) path: String,
    pub(super) body: Value,
}

/// The request of one batch shape on collection `c`, for documents `start`, `start + 1`, ...
/// A document index past `docs_in_collection(c)` is allowed.
pub(super) fn build_request(spec: &DatasetSpec, shape: Shape, c: usize, start: usize) -> Request {
    let collection = DatasetSpec::collection_id(c);
    match shape {
        Shape::Index(n) => {
            let items: Vec<Value> = (0..n)
                .map(|j| {
                    let i = start + j;
                    let doc = spec.document(c, i);
                    json!({
                        "external_id": DatasetSpec::external_id(c, i),
                        "field": "kw0",
                        "value": doc["kw0"],
                    })
                })
                .collect();
            Request {
                method: "POST",
                path: format!("/collections/{collection}/index"),
                body: json!({ "items": items }),
            }
        }
        Shape::Replace(n) => {
            let docs: Vec<Value> = (0..n)
                .map(|j| {
                    let i = start + j;
                    json!({
                        "external_id": DatasetSpec::external_id(c, i),
                        "fields": spec.document(c, i),
                    })
                })
                .collect();
            Request {
                method: "PUT",
                path: format!("/collections/{collection}/docs:replace"),
                body: json!({ "docs": docs }),
            }
        }
        Shape::Unindex(n) => {
            let ids: Vec<String> = (0..n)
                .map(|j| DatasetSpec::external_id(c, start + j))
                .collect();
            Request {
                method: "POST",
                path: format!("/collections/{collection}/docs:unindex"),
                body: json!({ "external_ids": ids }),
            }
        }
    }
}

/// Counts a request from its body and its path, never from the shape.
pub(super) fn counters_of(request: &Request) -> Result<Counters> {
    let body = &request.body;
    if request.method == "POST" && request.path.ends_with("/index") {
        let items = body["items"].as_array().map_or(0, Vec::len) as u64;
        return Ok(Counters {
            requests: 1,
            field_items: items,
            document_ops: 0,
        });
    }
    if request.method == "PUT" && request.path.ends_with("/docs:replace") {
        let docs = body["docs"].as_array().map(Vec::as_slice).unwrap_or(&[]);
        let field_items: usize = docs
            .iter()
            .map(|doc| doc["fields"].as_object().map_or(0, |f| f.len()))
            .sum();
        return Ok(Counters {
            requests: 1,
            field_items: field_items as u64,
            document_ops: docs.len() as u64,
        });
    }
    if request.method == "POST" && request.path.ends_with("/docs:unindex") {
        let ids = body["external_ids"].as_array().map_or(0, Vec::len) as u64;
        return Ok(Counters {
            requests: 1,
            field_items: 0,
            document_ops: ids,
        });
    }
    bail!("not a load request: {} {}", request.method, request.path)
}

pub(super) struct LoadGenerator {
    spec: DatasetSpec,
    rng: SplitMix64,
    next_shape: usize,
}

impl LoadGenerator {
    pub(super) fn new(spec: DatasetSpec) -> Self {
        let rng = SplitMix64::new(spec.seed ^ 0x10AD);
        Self {
            spec,
            rng,
            next_shape: 0,
        }
    }

    /// Cycles through `SHAPES`; the collection and the start document come from the generator's rng.
    pub(super) fn next_request(&mut self) -> Request {
        let shape = SHAPES[self.next_shape % SHAPES.len()];
        self.next_shape += 1;
        let c = (self.rng.next_u64() % self.spec.collections as u64) as usize;
        let start = (self.rng.next_u64() % self.spec.docs_in_collection(c).max(1) as u64) as usize;
        build_request(&self.spec, shape, c, start)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataset::FIELDS_PER_DOC;

    fn short_spec(seed: u64) -> DatasetSpec {
        DatasetSpec {
            seed,
            collections: 6,
            documents: 200,
        }
    }

    fn expected(shape: Shape) -> Counters {
        match shape {
            Shape::Index(n) => Counters {
                requests: 1,
                field_items: n as u64,
                document_ops: 0,
            },
            Shape::Replace(n) => Counters {
                requests: 1,
                field_items: (FIELDS_PER_DOC * n) as u64,
                document_ops: n as u64,
            },
            Shape::Unindex(n) => Counters {
                requests: 1,
                field_items: 0,
                document_ops: n as u64,
            },
        }
    }

    #[test]
    fn counters_match_each_shape() {
        let spec = short_spec(7);
        for shape in SHAPES {
            let got = counters_of(&build_request(&spec, shape, 2, 4)).expect("counted");
            assert_eq!(got, expected(shape), "{shape:?}");
        }
    }

    #[test]
    fn partial_index_fields_are_never_a_document() {
        let spec = short_spec(7);
        for n in [1usize, 100, 1000] {
            let request = build_request(&spec, Shape::Index(n), 2, 4);
            let counted = counters_of(&request).expect("counted");
            assert_eq!(counted.document_ops, 0, "Index({n})");
            let items = request.body["items"].as_array().expect("items");
            assert_eq!(items.len(), n);
            let mut ids: Vec<&str> = items
                .iter()
                .map(|item| item["external_id"].as_str().expect("id"))
                .collect();
            for item in items {
                assert_eq!(item["field"], "kw0");
            }
            ids.sort();
            ids.dedup();
            assert_eq!(ids.len(), n, "each item is a different document");
        }
    }

    #[test]
    fn replace_requests_carry_complete_documents() {
        let spec = short_spec(7);
        let request = build_request(&spec, Shape::Replace(32), 2, 4);
        let docs = request.body["docs"].as_array().expect("docs");
        assert_eq!(docs.len(), 32);
        for doc in docs {
            assert_eq!(doc["fields"].as_object().expect("fields").len(), 14);
        }
        assert_eq!(counters_of(&request).expect("counted").document_ops, 32);
    }

    #[test]
    fn unindex_ids_are_unique_and_bounded() {
        let spec = short_spec(7);
        let request = build_request(&spec, Shape::Unindex(1000), 2, 4);
        let ids = request.body["external_ids"].as_array().expect("ids");
        assert_eq!(ids.len(), 1000);
        let mut unique: Vec<&Value> = ids.iter().collect();
        unique.sort_by_key(|v| v.to_string());
        unique.dedup();
        assert_eq!(unique.len(), 1000);
    }

    #[test]
    fn same_seed_gives_the_same_load() {
        let spec = short_spec(7);
        let take = |seed: u64| -> Vec<Request> {
            let mut gen = LoadGenerator::new(DatasetSpec {
                seed,
                ..spec.clone()
            });
            (0..16).map(|_| gen.next_request()).collect()
        };
        assert_eq!(take(7), take(7));
        assert_ne!(take(7), take(8));
    }

    #[test]
    fn short_run_counters_match_the_generated_load() {
        let mut gen = LoadGenerator::new(short_spec(7));
        let mut total = Counters::default();
        for _ in 0..16 {
            total.add(counters_of(&gen.next_request()).expect("counted"));
        }
        // Two full rounds of SHAPES, so the sum is the per-shape expectation times two.
        let mut want = Counters::default();
        for _ in 0..2 {
            for shape in SHAPES {
                want.add(expected(shape));
            }
        }
        assert_eq!(total, want);
        assert_eq!(total.requests, 16);
        assert_eq!(total.field_items, 3126);
        assert_eq!(total.document_ops, 2268);
    }

    #[test]
    fn counters_of_rejects_an_unknown_path() {
        let request = Request {
            method: "POST",
            path: "/collections/x/search".to_string(),
            body: json!({}),
        };
        assert!(counters_of(&request).is_err());
    }
}
