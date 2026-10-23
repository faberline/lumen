//! The copy-paste end-to-end `lumen llm --topic quickstart` serves.

/// A copy-paste end-to-end (`lumen llm --topic quickstart`) as Markdown:
/// create → index → search against a local `lumen serve` on `:7373`.
pub fn llm_quickstart_md() -> String {
    r#"# lumen quickstart (copy-paste)

Assumes a local node at `http://localhost:7373` (`lumen serve`), which is h2c
and `LUMEN_AUTH=disabled` — every reachable node serves everyone, so keep it
off shared networks. Clients need `LUMEN_URL` and nothing else.

A production fleet is not reached this way. It answers only at
`https://<instance>.<namespace>.svc:7373` inside the cluster, verified against
the public CA distributed separately by the deployment administrator, and each request carries a short-lived
Kubernetes ServiceAccount token that lumen resolves through
TokenReview/SubjectAccessReview. The bodies below are unchanged; the URL and
the `Authorization` header are what differ. See `lumen llm --topic auth` and
`lumen connect`.

## 1. Declare a collection
```bash
curl -sS -XPUT localhost:7373/collections/products \
  -H 'content-type: application/json' -d '{
    "fields": {
      "title":     { "type": "text", "analyzer": "whitespace_lower" },
      "brand":     { "type": "keyword" },
      "price":     { "type": "number" },
      "embedding": { "type": "vector", "dim": 3, "metric": "cosine" }
    }
  }'
```

## 2. Index items (your pub/sub does this in production)
```bash
curl -sS -XPOST localhost:7373/collections/products/index \
  -H 'content-type: application/json' -d '{
    "items": [
      { "external_id": "p1", "field": "title", "value": "wireless earbuds" },
      { "external_id": "p1", "field": "brand", "value": "acme" },
      { "external_id": "p1", "field": "price", "value": 79 },
      { "external_id": "p1", "field": "embedding", "value": [0.1, 0.2, 0.9] }
    ]
  }'
```

## 3. Search (filters + relevance)
```bash
curl -sS -XPOST localhost:7373/collections/products/search \
  -H 'content-type: application/json' -d '{
    "query": { "and": [
      { "match": { "field": "title", "text": "earbuds" } },
      { "range": { "field": "price", "lte": 100 } }
    ] },
    "limit": 10
  }'
```

## 4. Hydrate
The response is `{ "hits": [ { "external_id", "score" } ], ... }`. Fetch the full
records from YOUR store by those `external_id`s — lumen never stored them.

More shapes: `lumen llm --topic recipes`. Full schema: `lumen spec`.

## Agent-friendly one-shot wrappers
No need to hand-build curl bodies or track a port-forward yourself:

```bash
lumen connect --namespace prod --cr search -- \
  lumen query index --collection products --item 'p1:title=wireless earbuds'
lumen query search --collection products --match 'title=earbuds' --limit 10
lumen query duplicates --collection products --field email
lumen query collections list
```

`lumen connect` manages the `kubectl port-forward` and sets `LUMEN_URL` — and
only `LUMEN_URL` — for the wrapped command; `lumen query *` assembles the exact
wire body (same shapes as `lumen spec --shapes`). Neither carries a credential:
see `lumen llm --topic auth`.
"#
    .to_string()
}
