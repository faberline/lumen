//! The bench cells: `run` runs each cell `--types` names against a synthetic
//! in-process corpus.

use std::collections::BTreeMap;
use std::time::Instant;

use anyhow::{bail, Context, Result};
use lumen::storage::{Engine, MAX_INDEX_ITEMS};
use lumen::types::{
    CreateCollectionRequest, FieldSpec, FieldType, FieldValue, IndexItem, IndexRequest, QueryNode,
    RangeBound, RangeQuery, SearchRequest, SortMissing, SortOrder, SortSpec, TermQuery,
};

use crate::cli::{parse_types, RunArgs};
use crate::report::{print_report, summarize, BenchReport, SORTED_PAGE_BUDGET_US};

pub(super) fn run(args: RunArgs) -> Result<()> {
    if args.documents == 0 {
        bail!("--documents must be > 0");
    }
    if args.page_size == 0 {
        bail!("--page-size must be > 0");
    }
    let _tiers = &args.tiers;
    for cell in parse_types(&args.types)? {
        match cell {
            "sorted_page_deep" => print_report(run_sorted_page_deep(&args)?),
            "bool_filter" => print_report(run_bool_filter(&args)?),
            _ => unreachable!("parse_types only returns supported cells"),
        }
    }
    Ok(())
}

fn run_sorted_page_deep(args: &RunArgs) -> Result<BenchReport> {
    let engine = build_corpus(args.documents)?;
    let depth = args.documents / 2;
    let page_size = args.page_size as usize;
    let target_page = (depth / page_size).max(1);
    let measure_window = args.queries.max(1).min(target_page + 1);
    let measure_from = target_page + 1 - measure_window;
    let mut cursor = None;
    let mut samples = Vec::with_capacity(measure_window);

    for page_idx in 0..=target_page {
        let mut req = sorted_page_request(args.page_size);
        req.cursor = cursor.take();
        let started = Instant::now();
        let resp = engine
            .search("docs", req)
            .with_context(|| format!("sorted_page_deep page {page_idx}"))?;
        let elapsed = started.elapsed().as_micros();
        if page_idx >= measure_from {
            samples.push(elapsed);
        }
        if resp.hits.is_empty() {
            bail!("sorted_page_deep exhausted before target depth {depth}");
        }
        cursor = resp.cursor;
    }

    let report = summarize("sorted_page_deep", args.documents, target_page + 1, samples);
    if report.p99_us > SORTED_PAGE_BUDGET_US {
        bail!(
            "sorted_page_deep p99 {}us exceeds budget {}us",
            report.p99_us,
            SORTED_PAGE_BUDGET_US
        );
    }
    Ok(report)
}

fn run_bool_filter(args: &RunArgs) -> Result<BenchReport> {
    let engine = build_corpus(args.documents)?;
    let reps = args.queries.max(1);
    let mut samples = Vec::with_capacity(reps);
    for _ in 0..reps {
        let started = Instant::now();
        let resp = engine
            .search(
                "docs",
                SearchRequest {
                    query: QueryNode::And(vec![
                        QueryNode::Term(TermQuery {
                            field: "city".into(),
                            value: FieldValue::String("taipei".into()),
                        }),
                        QueryNode::Range(RangeQuery {
                            field: "age".into(),
                            gt: None,
                            gte: Some(RangeBound::Number(30.0)),
                            lt: Some(RangeBound::Number(40.0)),
                            lte: None,
                        }),
                    ]),
                    limit: 20,
                    offset: 0,
                    cursor: None,
                    routing_key: None,
                    sort: None,
                    track_total: false,
                    collapse: None,
                },
            )
            .context("bool_filter search")?;
        if resp.hits.is_empty() {
            bail!("bool_filter returned no hits");
        }
        samples.push(started.elapsed().as_micros());
    }
    Ok(summarize("bool_filter", args.documents, reps, samples))
}

fn sorted_page_request(limit: u32) -> SearchRequest {
    SearchRequest {
        query: QueryNode::Range(RangeQuery {
            field: "age".into(),
            gt: None,
            gte: Some(RangeBound::Number(0.0)),
            lt: None,
            lte: None,
        }),
        limit,
        offset: 0,
        cursor: None,
        routing_key: None,
        sort: Some(vec![SortSpec {
            field: "age".into(),
            order: SortOrder::Asc,
            missing: SortMissing::Exclude,
        }]),
        track_total: false,
        collapse: None,
    }
}

fn build_corpus(n: usize) -> Result<Engine> {
    let engine = Engine::new();
    let mut fields = BTreeMap::new();
    fields.insert("city".into(), spec(FieldType::Keyword));
    fields.insert("age".into(), spec(FieldType::Number));
    engine.create_collection("docs", CreateCollectionRequest { fields })?;

    let max_docs_per_batch = (MAX_INDEX_ITEMS / 2).max(1);
    let mut start = 0usize;
    while start < n {
        let end = (start + max_docs_per_batch).min(n);
        let mut items = Vec::with_capacity((end - start) * 2);
        for i in start..end {
            let city = if i % 3 == 0 { "taipei" } else { "tokyo" };
            items.push(IndexItem {
                external_id: format!("doc-{i:06}"),
                field: "city".into(),
                value: FieldValue::String(city.into()),
                version: None,
            });
            items.push(IndexItem {
                external_id: format!("doc-{i:06}"),
                field: "age".into(),
                value: FieldValue::Number((i % 1_000_000) as f64),
                version: None,
            });
        }
        engine.index(
            "docs",
            IndexRequest {
                items,
                request_id: None,
            },
        )?;
        start = end;
    }
    Ok(engine)
}

fn spec(field_type: FieldType) -> FieldSpec {
    FieldSpec {
        field_type,
        analyzer: None,
        multi: None,
        dim: None,
        metric: None,
        backend: None,
        quantize: None,
    }
}
