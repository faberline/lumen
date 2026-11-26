# Coverage and ignored-test exceptions

The data comes from the `coverage-list` check run of commit `f2cda48` on `main` (Coverage 87.48%, 405 files below 100%). `line` in `aw.toml` is unchanged (87.34). #296 removes ignored tests; this file does not duplicate that work.

## Least covered files

| File | Line % | Uncovered lines | Plan or reason |
|---|---:|---:|---|
| `crates/lumen-index/src/index/domain/query/filter_page.rs` | 0.0 | 271 | add tests, tracked by a coverage issue |
| `crates/lumen/src/bin/lumen/connect/proxy.rs` | 0.0 | 98 | add tests, tracked by a coverage issue |
| `crates/lumen/src/bin/lumen/connect.rs` | 0.0 | 91 | add tests, tracked by a coverage issue |
| `crates/lumen-operator/src/application/reconcile/loops.rs` | 0.0 | 69 | add tests, tracked by a coverage issue |
| `crates/lumen-operator/src/application/reshard_driver/driver_loop.rs` | 0.0 | 46 | add tests, tracked by a coverage issue |
| `crates/lumen-operator/src/infrastructure/kube_access_policy_control.rs` | 0.0 | 23 | add tests, tracked by a coverage issue |
| `crates/lumen-operator/src/application/reconcile.rs` | 0.0 | 17 | add tests, tracked by a coverage issue |
| `crates/lumen-operator/src/infrastructure/kube_hpa_control.rs` | 0.0 | 13 | add tests, tracked by a coverage issue |
| `crates/lumen-access/src/application/control_plane_token.rs` | 0.0 | 3 | add tests, tracked by a coverage issue |
| `crates/lumen/src/bin/lumen/cli/k8s.rs` | 0.0 | 3 | add tests, tracked by a coverage issue |
| `crates/lumen-index/src/persistence/infrastructure/segment_rdb_store/flat_layout.rs` | 10.82 | 173 | add tests, tracked by a coverage issue |
| `crates/lumen-index/src/index/application/apply/committed_text_apply.rs` | 21.7 | 101 | add tests, tracked by a coverage issue |
| `crates/lumen-index/src/index/domain/vector.rs` | 23.07 | 80 | add tests, tracked by a coverage issue |
| `crates/lumen-index/src/sharding/application/engine_shard_write/backend.rs` | 23.46 | 75 | add tests, tracked by a coverage issue |
| `crates/lumen/src/bin/lumen/serve/telemetry.rs` | 26.78 | 41 | add tests, tracked by a coverage issue |
| `crates/lumen-access/src/interfaces/http.rs` | 27.58 | 21 | add tests, tracked by a coverage issue |
| `crates/lumen-index/src/index/interfaces/native_wire/server.rs` | 28.46 | 93 | add tests, tracked by a coverage issue |
| `crates/lumen-index/src/index/domain/query/range_conjunction.rs` | 32.09 | 165 | add tests, tracked by a coverage issue |
| `crates/lumen-index/src/ingest/infrastructure/wire_cost/bounded_serde/deserializer.rs` | 36.64 | 121 | add tests, tracked by a coverage issue |
| `crates/lumen-storage/src/infrastructure/segment.rs` | 46.15 | 7 | add tests, tracked by a coverage issue |

## Ignored tests

| Test | File | Command | CI job |
|---|---|---|---|
| `scale_proof_reopen_rss_is_bounded` | tests/disk_scale_proof.rs | `cargo test --release --locked -p lumen --features release --test disk_scale_proof -- --exact --include-ignored --test-threads 1 scale_proof_reopen_rss_is_bounded` | perf-coarse (lumen-release-candidate.yml) |
| `competitive_perf_gate` | tests/perf_gate_vs_db.rs | `cargo test --release --locked -p lumen --features release --test perf_gate_vs_db -- --exact --include-ignored --test-threads 1 competitive_perf_gate` | perf-coarse (lumen-release-candidate.yml) |
| `competitive_perf_gate_disk` | tests/perf_gate_vs_db.rs | `cargo test --release -p lumen --test perf_gate_vs_db competitive_perf_gate_disk -- --ignored --exact --nocapture` | none (run by hand) |
| `lumen_scale_bench` | tests/perf_gate_vs_db.rs | `cargo test --release -p lumen --test perf_gate_vs_db lumen_scale_bench -- --ignored --exact --nocapture` | none (run by hand) |
| `lumen_claim_cli_connect_query_workflow` | tests/it/behavior_lumen_claim_cli_connect_query_workflow.rs | `cargo test --release --locked -p lumen --features release --test it -- --exact --include-ignored --test-threads 1 behavior_lumen_claim_cli_connect_query_workflow::lumen_claim_cli_connect_query_workflow` | e2e-native (ci.yml) |
| `lumen_claim_developer_interactive_tooling` | tests/it/behavior_lumen_claim_developer_interactive_tooling.rs | `cargo test --release --locked -p lumen --features release --test it -- --exact --include-ignored --test-threads 1 behavior_lumen_claim_developer_interactive_tooling::lumen_claim_developer_interactive_tooling` | e2e-native (ci.yml) |
| `lumen_claim_k8s_kustomize_base` | tests/it/behavior_lumen_claim_k8s_kustomize_base.rs | `cargo test --release --locked -p lumen --features release --test it -- --exact --include-ignored --test-threads 1 behavior_lumen_claim_k8s_kustomize_base::lumen_claim_k8s_kustomize_base` | e2e-native (ci.yml) |
| `lumen_claim_long_running_kustomize_base` | tests/it/behavior_lumen_claim_long_running_kustomize_base.rs | `cargo test --release --locked -p lumen --features release --test it -- --exact --include-ignored --test-threads 1 behavior_lumen_claim_long_running_kustomize_base::lumen_claim_long_running_kustomize_base` | e2e-native (ci.yml) |
| `lumen_claim_observability_servicemonitor_rule` | tests/it/behavior_lumen_claim_observability_servicemonitor_rule.rs | `cargo test --release --locked -p lumen --features release --test it -- --exact --include-ignored --test-threads 1 behavior_lumen_claim_observability_servicemonitor_rule::lumen_claim_observability_servicemonitor_rule` | e2e-native (ci.yml) |
| `lumen_competitor_performance_competitive` | tests/it/benchmark_lumen_competitor_performance_competitive.rs | `cargo test --release --locked -p lumen --features release --test it -- --exact --include-ignored --test-threads 1 benchmark_lumen_competitor_performance_competitive::lumen_competitor_performance_competitive` | perf-coarse (lumen-release-candidate.yml) |
| `disk_format_size_and_decode_speed` | tests/it/disk_format_bench.rs | `cargo test -p lumen --test it -- disk_format_bench:: --ignored --nocapture` | none (run by hand) |
| `digest_bound_volume_survives_replacement_and_hard_kill_but_anonymous_volume_does_not` | tests/it/docker_named_volume.rs | `cargo test --release --locked -p lumen --features release --test it -- --exact --include-ignored --test-threads 1 docker_named_volume::digest_bound_volume_survives_replacement_and_hard_kill_but_anonymous_volume_does_not` | e2e-native (ci.yml) |
| `digest_bound_compose_down_keeps_volume_and_down_v_deletes_it` | tests/it/docker_named_volume.rs | `cargo test --release --locked -p lumen --features release --test it -- --exact --include-ignored --test-threads 1 docker_named_volume::digest_bound_compose_down_keeps_volume_and_down_v_deletes_it` | e2e-native (ci.yml) |
| `journey` | tests/it/e2e/operator.rs | `lumen-it --ignored --test-threads=1 e2e::operator::` in the GKE test Job | gke-e2e (lumen-release-candidate.yml) |
| `journey` | tests/it/e2e/overlay.rs | `lumen-it --ignored --test-threads=1 e2e::overlay::` in the GKE test Job | gke-e2e (lumen-release-candidate.yml) |
| `bind` | tests/it/e2e/standalone_container.rs | `cargo test --locked -p lumen --test it -- e2e::standalone_container:: --ignored --test-threads=1` | e2e-native (ci.yml), standalone-container (gke-test.yml), container-smoke and container-smoke-arm64 (lumen-release-candidate.yml) |
| `durable` | tests/it/e2e/standalone_container.rs | `cargo test --locked -p lumen --test it -- e2e::standalone_container:: --ignored --test-threads=1` | e2e-native (ci.yml), standalone-container (gke-test.yml), container-smoke and container-smoke-arm64 (lumen-release-candidate.yml) |
| `lumen_claim_competitor_performance_external_comparison` | tests/it/efficiency_lumen_claim_competitor_performance_external_comparison.rs | `cargo test --release --locked -p lumen --features release --test it -- --exact --include-ignored --test-threads 1 efficiency_lumen_claim_competitor_performance_external_comparison::lumen_claim_competitor_performance_external_comparison` | perf-coarse (lumen-release-candidate.yml) |
| `lumen_claim_elastic_disk_tier` | tests/it/efficiency_lumen_claim_elastic_disk_tier.rs | `cargo test --release --locked -p lumen --features release --test it -- --exact --include-ignored --test-threads 1 efficiency_lumen_claim_elastic_disk_tier::lumen_claim_elastic_disk_tier` | perf-coarse (lumen-release-candidate.yml) |
| `hnsw_ef_recall_latency_sweep` | tests/it/hnsw_ef_recall.rs | `cargo test --release -p lumen --test it -- hnsw_ef_recall:: --ignored --nocapture` | none (run by hand) |
| `index_throughput_floor` | tests/it/perf_gate.rs | `cargo test --release --locked -p lumen --test it -- perf_gate:: --ignored --skip durable_workload --test-threads=1 --nocapture` | perf-coarse (lumen-release-candidate.yml) |
| `match_query_latency_floor` | tests/it/perf_gate.rs | `cargo test --release --locked -p lumen --test it -- perf_gate:: --ignored --skip durable_workload --test-threads=1 --nocapture` | perf-coarse (lumen-release-candidate.yml) |
| `term_query_latency_floor` | tests/it/perf_gate.rs | `cargo test --release --locked -p lumen --test it -- perf_gate:: --ignored --skip durable_workload --test-threads=1 --nocapture` | perf-coarse (lumen-release-candidate.yml) |
| `truncate_docs_cost_is_constant_from_10_to_100k_documents` | tests/it/perf_gate.rs | `cargo test --release --locked -p lumen --test it -- perf_gate:: --ignored --skip durable_workload --test-threads=1 --nocapture` | perf-coarse (lumen-release-candidate.yml) |
| `number_read_costs_on_100k_documents` | tests/it/perf_gate.rs | `cargo test --release --locked -p lumen --test it -- perf_gate:: --ignored --skip durable_workload --test-threads=1 --nocapture` | perf-coarse (lumen-release-candidate.yml) |
| `approved_30_minute_durable_workload` | tests/it/perf_gate.rs | `cargo test --release --locked -p lumen --test it -- perf_gate::approved_30_minute_durable_workload --ignored --nocapture` | none (run by hand) |
| `generated_client_live_h2c_public_api_journey` | tests/it/spec_gen_e2e.rs | `cargo test --release --locked -p lumen --features release --test it -- --exact --include-ignored --test-threads 1 spec_gen_e2e::generated_client_live_h2c_public_api_journey` | e2e-native (ci.yml) |
| `lumen_long_running_stability_latency_drift` | tests/it/stability_lumen_long_running_stability_latency_drift.rs | `cargo test --release --locked -p lumen --features release --test it -- --exact --include-ignored --test-threads 1 stability_lumen_long_running_stability_latency_drift::lumen_long_running_stability_latency_drift` | perf-coarse (lumen-release-candidate.yml) |
| `lumen_long_running_stability_overload_backpressure` | tests/it/stability_lumen_long_running_stability_overload_backpressure.rs | `cargo test --release --locked -p lumen --features release --test it -- --exact --include-ignored --test-threads 1 stability_lumen_long_running_stability_overload_backpressure::lumen_long_running_stability_overload_backpressure` | perf-coarse (lumen-release-candidate.yml) |
| `lumen_long_running_stability_query_resilience` | tests/it/stability_lumen_long_running_stability_query_resilience.rs | `cargo test --release --locked -p lumen --features release --test it -- --exact --include-ignored --test-threads 1 stability_lumen_long_running_stability_query_resilience::lumen_long_running_stability_query_resilience` | perf-coarse (lumen-release-candidate.yml) |
| `lumen_long_running_stability_resilience_survival` | tests/it/stability_lumen_long_running_stability_resilience_survival.rs | `cargo test --release --locked -p lumen --features release --test it -- --exact --include-ignored --test-threads 1 stability_lumen_long_running_stability_resilience_survival::lumen_long_running_stability_resilience_survival` | perf-coarse (lumen-release-candidate.yml) |
| `lumen_long_running_stability_resource_leak` | tests/it/stability_lumen_long_running_stability_resource_leak.rs | `cargo test --release --locked -p lumen --features release --test it -- --exact --include-ignored --test-threads 1 stability_lumen_long_running_stability_resource_leak::lumen_long_running_stability_resource_leak` | perf-coarse (lumen-release-candidate.yml) |
| `write_qps_bench` | tests/it/write_qps.rs | `cargo test --release -p lumen --test it -- write_qps:: --ignored --nocapture` | none (run by hand) |
| `engine_index_qps_probe` | tests/it/write_qps.rs | `cargo test --release -p lumen --test it -- write_qps:: --ignored --nocapture` | none (run by hand) |
| `wal_codec_probe` | tests/it/write_qps.rs | `cargo test --release -p lumen --test it -- write_qps:: --ignored --nocapture` | none (run by hand) |
