# Ignored tests

An ignored test is not a pass. This file lists every `#[ignore]` test under `crates/` with its kind and the CI job that runs it; a guard test (`crates/lumen/tests/it/ignored_inventory.rs`) keeps the list equal to the source.

| File | Test | Kind | Reason | CI job |
|---|---|---|---|---|
| crates/lumen/tests/it/behavior_lumen_claim_cli_connect_query_workflow.rs | `lumen_claim_cli_connect_query_workflow` | contract owner | native hosted contract owner; exact roster is required by development CI | ci.yml / e2e-native |
| crates/lumen/tests/it/behavior_lumen_claim_developer_interactive_tooling.rs | `lumen_claim_developer_interactive_tooling` | contract owner | native hosted contract owner; exact roster is required by development CI | ci.yml / e2e-native |
| crates/lumen/tests/it/behavior_lumen_claim_k8s_kustomize_base.rs | `lumen_claim_k8s_kustomize_base` | contract owner | native hosted contract owner; exact roster is required by development CI | ci.yml / e2e-native |
| crates/lumen/tests/it/behavior_lumen_claim_long_running_kustomize_base.rs | `lumen_claim_long_running_kustomize_base` | contract owner | native hosted contract owner; exact roster is required by development CI | ci.yml / e2e-native |
| crates/lumen/tests/it/behavior_lumen_claim_observability_servicemonitor_rule.rs | `lumen_claim_observability_servicemonitor_rule` | contract owner | native hosted contract owner; exact roster is required by development CI | ci.yml / e2e-native |
| crates/lumen/tests/it/benchmark_lumen_competitor_performance_competitive.rs | `lumen_competitor_performance_competitive` | contract owner | native hosted contract owner; exact roster is required by development CI | lumen-release-candidate.yml / perf-coarse |
| crates/lumen/tests/it/efficiency_lumen_claim_competitor_performance_external_comparison.rs | `lumen_claim_competitor_performance_external_comparison` | contract owner | native hosted contract owner; exact roster is required by development CI | lumen-release-candidate.yml / perf-coarse |
| crates/lumen/tests/it/efficiency_lumen_claim_elastic_disk_tier.rs | `lumen_claim_elastic_disk_tier` | contract owner | native hosted contract owner; exact roster is required by development CI | lumen-release-candidate.yml / perf-coarse |
| crates/lumen/tests/it/stability_lumen_long_running_stability_latency_drift.rs | `lumen_long_running_stability_latency_drift` | contract owner | native hosted contract owner; exact roster is required by development CI | lumen-release-candidate.yml / perf-coarse |
| crates/lumen/tests/it/stability_lumen_long_running_stability_overload_backpressure.rs | `lumen_long_running_stability_overload_backpressure` | contract owner | native hosted contract owner; exact roster is required by development CI | lumen-release-candidate.yml / perf-coarse |
| crates/lumen/tests/it/stability_lumen_long_running_stability_query_resilience.rs | `lumen_long_running_stability_query_resilience` | contract owner | native hosted contract owner; exact roster is required by development CI | lumen-release-candidate.yml / perf-coarse |
| crates/lumen/tests/it/stability_lumen_long_running_stability_resilience_survival.rs | `lumen_long_running_stability_resilience_survival` | contract owner | native hosted contract owner; exact roster is required by development CI | lumen-release-candidate.yml / perf-coarse |
| crates/lumen/tests/it/stability_lumen_long_running_stability_resource_leak.rs | `lumen_long_running_stability_resource_leak` | contract owner | native hosted contract owner; exact roster is required by development CI | lumen-release-candidate.yml / perf-coarse |
| crates/lumen/tests/it/e2e/standalone_container.rs | `bind` | needs environment | e2e: needs E2E_STANDALONE_IMAGE | ci.yml / e2e-native; gke-test.yml / standalone-container; lumen-release-candidate.yml / container-smoke, container-smoke-arm64 |
| crates/lumen/tests/it/e2e/standalone_container.rs | `durable` | needs environment | e2e: needs E2E_STANDALONE_IMAGE | ci.yml / e2e-native; gke-test.yml / standalone-container; lumen-release-candidate.yml / container-smoke, container-smoke-arm64 |
| crates/lumen/tests/it/e2e/overlay.rs | `journey` | needs environment | e2e: needs E2E_ENDPOINT | ci.yml / e2e-gke (calls faberline/aw gke-test.yml); lumen-release-candidate.yml / GKE e2e |
| crates/lumen/tests/it/e2e/operator.rs | `journey` | needs environment | e2e: needs E2E_ENDPOINT | ci.yml / e2e-gke (calls faberline/aw gke-test.yml); lumen-release-candidate.yml / GKE e2e |
| crates/lumen/tests/it/docker_named_volume.rs | `digest_bound_compose_down_keeps_volume_and_down_v_deletes_it` | needs environment | no reason given (`#[ignore]` without text) | ci.yml / e2e-native |
| crates/lumen/tests/it/docker_named_volume.rs | `digest_bound_volume_survives_replacement_and_hard_kill_but_anonymous_volume_does_not` | needs environment | no reason given (`#[ignore]` without text) | ci.yml / e2e-native |
| crates/lumen/tests/it/spec_gen_e2e.rs | `generated_client_live_h2c_public_api_journey` | needs environment | AW EC gate: opens a local h2c listener; run explicitly with --ignored | ci.yml / e2e-native |
| crates/lumen/tests/perf_gate_vs_db.rs | `competitive_perf_gate_disk` | needs environment | DISK-tier explicit peer calibration gate — requires Postgres + OpenSearch | none |
| crates/lumen/tests/it/perf_gate.rs | `index_throughput_floor` | performance gate | coarse performance gate runs in the release candidate workflow | lumen-release-candidate.yml / perf-coarse |
| crates/lumen/tests/it/perf_gate.rs | `match_query_latency_floor` | performance gate | coarse performance gate runs in the release candidate workflow | lumen-release-candidate.yml / perf-coarse |
| crates/lumen/tests/it/perf_gate.rs | `number_read_costs_on_100k_documents` | performance gate | coarse performance gate runs in the release candidate workflow | lumen-release-candidate.yml / perf-coarse |
| crates/lumen/tests/it/perf_gate.rs | `term_query_latency_floor` | performance gate | coarse performance gate runs in the release candidate workflow | lumen-release-candidate.yml / perf-coarse |
| crates/lumen/tests/it/perf_gate.rs | `truncate_docs_cost_is_constant_from_10_to_100k_documents` | performance gate | coarse performance gate runs in the release candidate workflow | lumen-release-candidate.yml / perf-coarse |
| crates/lumen/tests/it/perf_gate.rs | `approved_30_minute_durable_workload` | performance gate | 30-minute Docker release workload; default execution runs all sixteen matrix cells serially | none |
| crates/lumen/tests/perf_gate_vs_db.rs | `competitive_perf_gate` | performance gate | competitive perf gate — release-mode Lumen regression; peer compare is explicit | lumen-release-candidate.yml / perf-coarse |
| crates/lumen/tests/disk_scale_proof.rs | `scale_proof_reopen_rss_is_bounded` | performance gate | heavy scale proof: run explicitly with --ignored (like perf_gate_vs_db) | lumen-release-candidate.yml / perf-coarse |
| crates/lumen-index/src/index/domain/query/zip_cursor/tests.rs | `timing_and_match_500k_all_docs_match` | performance gate | timing harness, not a correctness gate — run explicitly in release | lumen-release-candidate.yml / perf-coarse |
| crates/lumen-index/src/index/application/engine/search/tests/pagination/cursors.rs | `deep_pagination_depth_invariance` | performance gate | no reason given (`#[ignore]` without text) | lumen-release-candidate.yml / perf-coarse |
| crates/lumen/tests/it/write_qps.rs | `write_qps_bench` | report only | report-only write-path QPS bench | none |
| crates/lumen/tests/it/write_qps.rs | `engine_index_qps_probe` | report only | report-only direct Engine::index QPS probe; isolates engine apply from HTTP/WAL | none |
| crates/lumen/tests/it/write_qps.rs | `wal_codec_probe` | report only | WAL codec micro-probe; report-only | none |
| crates/lumen/tests/perf_gate_vs_db.rs | `lumen_scale_bench` | report only | LUMEN-ONLY disk scale bench (no pg/OS) — report, run --ignored --nocapture lumen_scale_bench | none |
| crates/lumen/tests/it/hnsw_ef_recall.rs | `hnsw_ef_recall_latency_sweep` | measurement harness | bench/validation harness — run with --release --ignored --nocapture | none |
| crates/lumen/tests/it/disk_format_bench.rs | `disk_format_size_and_decode_speed` | measurement harness | measurement harness — run explicitly with --ignored --nocapture | none |
| crates/lumen-index/src/persistence/application/background_merge/tests/phase_metrics.rs | `measure_background_merge_job_cost_by_collection_count` | measurement harness | measurement: prints a per-phase cost breakdown, asserts no wall-clock budget | none |

## Empty stubs

No ignored test is an empty stub: every file named behavior_, stability_, efficiency_ or benchmark_ has a body that CI runs with --include-ignored.
