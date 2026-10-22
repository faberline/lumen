//! The query-shape cookbook `lumen spec --shapes` emits: a ready-to-POST
//! request for every `QueryNode` variant, sort and collapse.

use serde_json::{json, Value};

/// A cookbook of canonical query shapes. Each entry is a ready-to-POST
/// `{name, description, request}` for `POST /collections/{id}/search` (or
/// `/duplicates` where noted) using the exact wire form of every `QueryNode`
/// variant plus sort / collapse.
pub fn query_shapes() -> Value {
    json!({
        "search_endpoint": "POST /collections/{collection}/search",
        "search_all_endpoint": "POST /collections/{collection}/search:all",
        "note": "lumen returns ranked/sorted external_id hits only — never documents.",
        "shapes": [
            { "name": "term", "description": "exact keyword/number/bool match",
              "request": { "query": { "term": { "field": "status", "value": "active" } }, "limit": 20 } },
            { "name": "terms", "description": "keyword in a set (IN)",
              "request": { "query": { "terms": { "field": "status", "values": ["active", "trial"] } }, "limit": 20 } },
            { "name": "prefix", "description": "case-sensitive UTF-8 starts-with match on a keyword field",
              "request": { "query": { "prefix": { "field": "path", "value": "台北市/" } }, "limit": 20 } },
            { "name": "ids", "description": "filter by a set of external_ids (row_id_in); unknown ids skipped",
              "request": { "query": { "ids": { "values": ["row-42", "row-91"] } }, "limit": 20 } },
            { "name": "range", "description": "numeric range on a `number` field (e.g. 1000 <= price < 5000)",
              "request": { "query": { "range": { "field": "price", "gte": 1000, "lt": 5000 } }, "limit": 20 } },
            { "name": "range_keyword", "description": "byte/lexicographic range on a `keyword` field — string bounds are valid only against `keyword` fields (rejected with 400 against `number` or `text`), and compare the same way ISO-8601 date/datetime strings sort chronologically",
              "request": { "query": { "range": { "field": "created_at", "gte": "2026-01-01", "lt": "2026-02-01" } }, "limit": 20 } },
            { "name": "match_bm25", "description": "lexical BM25 ranking over a text field",
              "request": { "query": { "match": { "field": "bio", "text": "rust search engineer" } }, "limit": 20 } },
            { "name": "autocomplete_ngram", "description": "autocomplete/suggest recipe: declare a text field with analyzer=ngram, index the searchable label, then run match on the prefix/substring; lumen returns external_ids, not suggestion payloads",
              "request": { "query": { "match": { "field": "title_suggest", "text": "wire" } }, "limit": 10 } },
            { "name": "boolean_and", "description": "conjunction; planner drives from the most selective clause",
              "request": { "query": { "and": [
                  { "match": { "field": "name", "text": "手機殼" } },
                  { "range": { "field": "price", "gte": 1000, "lt": 5000 } }
              ] }, "limit": 20 } },
            { "name": "boolean_or", "description": "disjunction",
              "request": { "query": { "or": [
                  { "term": { "field": "brand", "value": "apple" } },
                  { "term": { "field": "brand", "value": "samsung" } }
              ] }, "limit": 20 } },
            { "name": "boolean_not", "description": "AND with a negated filter clause",
              "request": { "query": { "and": [
                  { "term": { "field": "category", "value": "phone" } },
                  { "not": { "term": { "field": "refurbished", "value": "true" } } }
              ] }, "limit": 20 } },
            { "name": "knn", "description": "vector kNN (caller supplies the embedding)",
              "request": { "query": { "knn": { "field": "embedding", "vector": [0.12, -0.03, 0.88], "k": 10 } }, "limit": 10 } },
            { "name": "rrf_hybrid", "description": "hybrid lexical+semantic: fuse a BM25 match and a vector kNN by rank (Reciprocal Rank Fusion)",
              "request": { "query": { "rrf": { "k": 60, "queries": [
                  { "match": { "field": "title", "text": "wireless earbuds" } },
                  { "knn": { "field": "embedding", "vector": [0.12, -0.03, 0.88], "k": 50 } }
              ] } }, "limit": 10 } },
            { "name": "rrf_hybrid_filtered", "description": "filter-correct hybrid: put the filter INSIDE each leg so the kNN leg stays filter-correct (no recall collapse)",
              "request": { "query": { "rrf": { "k": 60, "queries": [
                  { "and": [ { "match": { "field": "title", "text": "wireless earbuds" } }, { "term": { "field": "brand", "value": "acme" } } ] },
                  { "and": [ { "knn": { "field": "embedding", "vector": [0.12, -0.03, 0.88], "k": 50 } }, { "term": { "field": "brand", "value": "acme" } } ] }
              ] } }, "limit": 10 } },
            { "name": "hamming_near_dup", "description": "perceptual near-duplicate: hashes within N Hamming bits",
              "request": { "query": { "hamming": { "field": "phash", "hash": "f0e1d2c3b4a59687", "max_distance": 8 } }, "limit": 20 } },
            { "name": "has_child_nested_group", "description": "rows whose nested group has an element matching a sub-query; may be combined with parent-field sort",
              "request": { "query": { "has_child": {
                  "collection": "orders_items", "field": "parent_row_id",
                  "query": { "and": [
                      { "term": { "field": "sku", "value": "S0" } },
                      { "range": { "field": "qty", "gte": 5 } }
                  ] } } },
                  "sort": [ { "field": "score", "order": "asc" } ],
                  "track_total": true,
                  "limit": 20 } },
            { "name": "collapse_group_by", "description": "one hit per distinct keyword value (group-by), scored by the max member",
              "request": { "query": { "term": { "field": "in_stock", "value": "true" } }, "collapse": "brand", "limit": 20 } },
            { "name": "filter_then_sort", "description": "filter, then sort by a field instead of relevance",
              "request": { "query": { "range": { "field": "price", "gte": 100 } },
                           "sort": [ { "field": "price", "order": "asc" } ], "track_total": false, "limit": 20 } },
            { "name": "native_offset", "description": "jump directly after global filtering and ordering without client over-fetch; offset and cursor are mutually exclusive",
              "request": { "query": { "range": { "field": "price", "gte": 100 } },
                           "sort": [ { "field": "price", "order": "asc" } ], "offset": 1000, "limit": 20 } },
            { "name": "search_all", "description": "explicitly expensive full materialization of every matching external_id (POST /collections/{id}/search:all); local consistency is one read-lock snapshot, routed consistency is one snapshot per shard",
              "request": { "query": { "term": { "field": "status", "value": "active" } },
                           "sort": [ { "field": "created_at", "order": "asc" } ] } },
            { "name": "duplicates", "description": "find external_ids sharing a value (POST /collections/{id}/duplicates)",
              "request": { "field": "email", "min_group_size": 2, "limit": 100 } },
            { "name": "index", "description": "index one or more field values (POST /collections/{id}/index); the wire shape is FLAT — {items:[{external_id,field,value}]} — not the nested {id, fields:{...}} shape a caller might assume",
              "request": { "items": [
                  { "external_id": "row-42", "field": "email", "value": "person@example.com" },
                  { "external_id": "row-42", "field": "price", "value": 79 }
              ] } }
        ]
    })
}
