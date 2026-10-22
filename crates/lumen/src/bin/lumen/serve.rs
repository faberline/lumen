//! `lumen serve`: build the engine, the write log and the router, and run the
//! serving node until shutdown.

mod bootstrap;
#[cfg(feature = "raft-wal")]
mod raft;
mod restore;
mod telemetry;

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use lumen::auth::{AuthConfig, AuthProfile};
use lumen::coordinator::WriteCoordinator;
use lumen::rdb::{LocalFsRdbStore, RdbSnapshot, RdbStore};
use lumen::segment_checkpoint::SegmentCheckpointSink;
use lumen::storage::Engine;
use lumen::wal::{MemWal, SharedWal};

use crate::cli::serve::{ServeArgs, WalBackend};
use crate::serve::bootstrap::{
    apply_bootstrap_seed, cbor_cold_start, load_search_shard_segment_roots, recovery_phase_start,
    resolve_wal_backend, use_segment_persistence,
};
use crate::serve::restore::segment_restore_sink;
use crate::serve::telemetry::init_tracing;

#[cfg(feature = "otel")]
use crate::serve::telemetry::init_otel_meter;

#[cfg(feature = "raft-wal")]
use crate::serve::raft::{shutdown_raft_within, spawn_cluster_state_poller, RaftPeerServer};

pub(crate) async fn serve(args: ServeArgs) -> Result<()> {
    init_tracing(
        &args.log_level,
        args.log_format,
        args.otlp_endpoint.as_deref(),
    )?;

    let engine = Arc::new(Engine::new());

    if apply_bootstrap_seed(&engine, args.bootstrap_seed_uri.as_deref())? {
        if let Some(limit) = args.bootstrap_max_bytes_per_sec {
            tracing::info!(
                max_bytes_per_sec = limit,
                "bootstrap seed applied; read throttle reserved for object-store fetchers"
            );
        }
    }

    // OTLP metrics push (opt-in, same endpoint as traces): observable
    // instruments read the engine's atomic counters and push to the collector.
    #[cfg(feature = "otel")]
    if let Some(endpoint) = args.otlp_endpoint.as_deref() {
        match init_otel_meter(endpoint, engine.clone()) {
            Ok(()) => tracing::info!(otlp_endpoint = endpoint, "OTLP metrics push enabled"),
            Err(e) => {
                tracing::error!(error = %e, "OTLP metrics init failed; /metrics pull still works")
            }
        }
    }

    // Select the write log. `--wal raft` also yields a driver whose router is
    // merged into the serve app below (peer RPCs ride the h2c port).
    #[cfg(feature = "raft-wal")]
    let mut raft_host: Option<Arc<raft_runtime::RaftHost>> = None;
    #[cfg(feature = "raft-wal")]
    let mut raft_peer_transport: Option<raft_runtime::PeerTransport> = None;
    #[cfg(feature = "raft-wal")]
    let mut raft_writer: Option<Arc<dyn lumen::coordinator::WriteSink>> = None;
    // Live `ClusterState` for `AppState::with_cluster` (#1349): populated only
    // in raft mode, kept current for the process lifetime by
    // `spawn_cluster_state_poller` below. `None` here (standalone/legacy-log
    // backends) is correct — `enforce_read_consistency` no-ops when
    // `state.cluster` is `None`.
    #[cfg(feature = "raft-wal")]
    let mut raft_cluster: Option<Arc<lumen::raft::ClusterState>> = None;
    // k8s-native auto-detect: `--wal auto` (the default) picks raft when the
    // StatefulSet runs >1 replica per shard, else embedded — so single-node /
    // local dev needs no flags or cluster env.
    let backend = resolve_wal_backend(args.wal);
    let segment_mode = use_segment_persistence(&args);
    // The segment-checkpoint store — built only in segment mode.
    let segment_store: Option<Arc<lumen::segment_rdb::SegmentRdbStore>> = if segment_mode {
        match &args.data_dir {
            Some(dir) => Some(Arc::new(
                lumen::segment_rdb::SegmentRdbStore::new(dir)
                    .context("open segment-checkpoint store")?,
            )),
            None => None,
        }
    } else {
        None
    };

    // CBOR, ephemeral serving, and Raft keep the same public persistence path.
    // A private segment root only bounds their pending in-process changes.
    // Start before RaftHost restores and replays committed entries.
    #[cfg(feature = "raft-wal")]
    let raft_bootstrap = matches!(backend, WalBackend::Raft);
    #[cfg(not(feature = "raft-wal"))]
    let raft_bootstrap = false;
    // Raft replay can start below the configured CURRENT sequence. Keep its
    // startup capacity spill independent until the durable log has caught up.
    let mut pending_spill = if raft_bootstrap || !segment_mode || args.data_dir.is_none() {
        Some(
            lumen::segment_checkpoint::PendingChangeSpill::temporary(
                engine.clone(),
                Duration::from_secs(args.snapshot_secs.max(1)),
            )
            .context("start private pending-change spill")?,
        )
    } else {
        None
    };
    let wal: Option<SharedWal> = match backend {
        WalBackend::Auto => unreachable!("auto is resolved by resolve_wal_backend"),
        WalBackend::Embedded => {
            tracing::info!("wal=embedded (in-process; single-node)");
            // Constructed below (#1486), once the final restore watermark
            // (`start_seq`, after any checkpoint + AOF-tail replay) is
            // known — an embedded `MemWal` must start its sequence domain
            // above that watermark, not at 0, or the apply loop's
            // redelivery-dedup guard silently strands the first N
            // post-restart writes.
            None
        }
        #[cfg(feature = "raft-wal")]
        WalBackend::Raft => {
            // Topology from the StatefulSet downward API via the shared helper
            // (node id + membership + peers — no hand-rolled ordinal/DNS math).
            // Peers are always addressed on the dedicated authenticated Raft
            // port over `https`.
            let headless = std::env::var("LUMEN_HEADLESS_SERVICE")
                .unwrap_or_else(|_| "lumen-headless".to_string());
            let peer_transport = lumen::tls::PeerTlsConfig::from_env()
                .context("raft: load peer TLS configuration")?
                .map(|config| config.peer_transport())
                .transpose()
                .context("raft: build shared peer mTLS transport")?;
            // #2890 R3/R4: no plaintext fallback. This used to route peer RPCs
            // at the *client* port over h2c whenever TLS material was absent —
            // a replicated group replicating committed index mutations between
            // pods with nothing on the wire saying who either end is, reachable
            // by anything that can open a TCP connection to the Service. The
            // failure it replaced (a pod that will not start) is loud, local,
            // and names the field to set; the one it created was silent.
            let Some(peer_transport) = peer_transport else {
                anyhow::bail!(
                    "raft: replicated mode needs peer mTLS material — set \
                     LUMEN_PEER_TLS_CERT / LUMEN_PEER_TLS_KEY / LUMEN_PEER_TLS_CA \
                     (+ LUMEN_PEER_MTLS=on), or under Kubernetes name a Secret with \
                     tls.crt/tls.key/ca.crt in the Lumen CR's spec.peerTlsSecret. \
                     Raft peer traffic has no plaintext path"
                );
            };
            let topo = raft_runtime::ClusterTopology::from_env_with_scheme(
                "lumen",
                &headless,
                args.raft_port,
                "LUMEN_PEERS",
                "https",
            )
            .context("raft: cluster topology from env")?;
            tracing::info!(
                node_id = topo.node_id,
                voters = ?topo.membership.voters,
                peers = ?topo.peers.keys().collect::<Vec<_>>(),
                data_dir = %args.raft_data_dir,
                "wal=raft (raft_core; multi-pod)"
            );
            let store = raft_runtime::RaftStore::open(
                &args.raft_data_dir,
                topo.node_id,
                raft_runtime::FsyncPolicy::Always,
            )
            .context("open raft store")?;
            // The host is the sole applier: committed entries fold straight into
            // the engine (via `EngineSm`), so there is no `WalLog`/coordinator
            // seam for the raft path. Cold-start (restore + replay) happens in
            // `RaftHost::spawn`; snapshot/compaction is driven externally below.
            let sm = match &segment_store {
                Some(store) => lumen::raft_sm::EngineSm::new_with_segment_store(
                    engine.clone(),
                    0,
                    store.clone(),
                ),
                None => lumen::raft_sm::EngineSm::new(engine.clone(), 0),
            };
            let host_config = raft_runtime::HostConfig {
                snapshot: raft_runtime::SnapshotPolicy::External,
                ..Default::default()
            };
            let host = Arc::new(raft_runtime::RaftHost::spawn_with_peer_transport(
                topo.node_id,
                topo.membership,
                topo.peers,
                store,
                sm.clone() as Arc<dyn raft_runtime::RaftStateMachine>,
                host_config,
                peer_transport.clone(),
            ));
            raft_peer_transport = Some(peer_transport);

            // Live cluster state (#1349): the same `ClusterConfig`/`RaftGroup`
            // shape `AppState::with_cluster`'s consumer (`enforce_read_consistency`,
            // `GET /debug/cluster`) already expects, seeded with the same
            // topology math as `topo` above (#1002 delegation keeps them from
            // drifting) so `group.peers` names line up with raft `NodeId`s
            // 1:1 by replica index.
            let cluster_cfg =
                lumen::config::ClusterConfig::from_env().context("raft: cluster config")?;
            let group = lumen::raft::RaftGroup::from_config(
                &cluster_cfg,
                "lumen",
                &headless,
                args.port,
                args.port,
            )
            .context("raft: build raft group")?;
            let cluster_state = Arc::new(
                lumen::raft::ClusterState::new(&cluster_cfg, group)
                    .context("raft: build cluster state")?,
            );
            spawn_cluster_state_poller(
                host.clone(),
                cluster_state.clone(),
                cluster_cfg.is_voter()?,
                engine.clone(),
            );
            raft_cluster = Some(cluster_state);

            raft_host = Some(Arc::clone(&host));
            raft_writer = Some(Arc::new(lumen::raft_sm::RaftWriteSink::new(host, sm)));
            None
        }
    };

    // The raft path is the sole applier (no WalLog/coordinator seam): it
    // cold-starts inside `RaftHost::spawn` and uses the host as its `WriteSink`.
    #[cfg(feature = "raft-wal")]
    let is_raft = raft_writer.is_some();
    #[cfg(not(feature = "raft-wal"))]
    let is_raft = false;

    // Persistence bootstrap: load the latest checkpoint (if any) so we tail from
    // its sequence instead of replaying the whole log. Two modes share the
    // `--data-dir`: the default CBOR RDB and (opt-in) the columnar segment
    // checkpoint. `segment_mode` is `false` unless `--persistence=segment` is
    // passed, so the block below is byte-identical to today in the default mode.

    // The CBOR RDB store — built unless segment persistence is selected.
    let rdb_store = if segment_mode {
        None
    } else {
        match &args.data_dir {
            Some(dir) => Some(Arc::new(
                LocalFsRdbStore::new(dir).context("open RDB store")?,
            )),
            None => None,
        }
    };

    // Cold-start sequence: the WAL position the checkpoint is current as of, so
    // the apply loop tails from `start_seq + 1`.
    let mut deferred_graph_restore = false;
    let mut start_seq = {
        if is_raft {
            // Raft cold-starts inside `RaftHost::spawn` (snapshot restore + replay
            // of committed entries); the engine here is fresh and the host owns
            // the applied seq, so there is nothing to load from `--data-dir`.
            0
        } else if let Some(store) = &segment_store {
            // Segment mode: reopen every collection from the newest checkpoint
            // INTO `engine` (no whole-collection load), replacing the CBOR restore.
            let checkpoint_reopen_started = std::time::Instant::now();
            let (outcome, deferred) = store
                .reopen_for_aof_replay(&engine)
                .map_err(|error| {
                    tracing::error!(
                        checkpoint_reopen_ms = checkpoint_reopen_started.elapsed().as_millis() as u64,
                        %error,
                        "segment checkpoint startup decision"
                    );
                    error
                })
                .context("load latest segment checkpoint")?;
            deferred_graph_restore = deferred;
            let generation = outcome.generation.as_ref().map(|name| name.as_str());
            tracing::info!(
                decision = outcome.decision.as_str(),
                generation = ?generation,
                checkpoint_seq = ?outcome.checkpoint_sequence,
                recovered_legacy_aside = outcome.recovered_legacy_aside,
                staging_cleaned = outcome.staging_cleaned,
                checkpoint_reopen_ms = checkpoint_reopen_started.elapsed().as_millis() as u64,
                "segment checkpoint startup decision"
            );
            outcome.checkpoint_sequence.unwrap_or(0)
        } else {
            cbor_cold_start(&rdb_store, &engine).await?
        }
    };

    // Recovery may need to wait for pending capacity before the coordinator
    // exists. This driver uses the same configured checkpoint store, samples
    // the Engine watermark, and never trims the AOF being replayed.
    let mut replay_checkpoint_driver = if !is_raft {
        segment_store.as_ref().map(|store| {
            lumen::segment_checkpoint::PendingChangeSpill::configured_replay(
                engine.clone(),
                store.clone(),
                Duration::from_secs(args.snapshot_secs.max(1)),
            )
        })
    } else {
        None
    };

    // Local AOF (segment mode only): RDB (segment checkpoint, up to `start_seq`)
    // → AOF replay (`start_seq+1 .. A`) → broker tail (`A+1 ..`). After replay the
    // apply loop keeps appending to this same writer, and the checkpoint
    // snapshotter trims it. The default CBOR path never builds one.
    let aof_writer: Option<lumen::coordinator::SharedAof> = if segment_mode && !is_raft {
        match &args.data_dir {
            Some(dir) => {
                let aof_path = std::path::Path::new(dir).join("aof.log");
                // (b) Replay the AOF over the RDB baseline. `replay_aof_into`
                // returns only the highest replayed tail sequence (zero when
                // there is no tail), so retain the checkpoint watermark when
                // that return value is lower. The loop must always tail from
                // the first sequence strictly above the durable baseline.
                let replay_started = std::time::Instant::now();
                let checkpoints_before = engine.metrics().segment_checkpoint_completed_total.get();
                let replay_engine = engine.clone();
                let replay_path = aof_path.clone();
                let replayed = recovery_phase_start("aof_replay", || {
                    tokio::task::spawn_blocking(move || {
                        lumen::aof::replay_aof_into(&replay_engine, &replay_path, start_seq)
                    })
                })
                .await
                .context("AOF replay worker failed")?
                .context("replay AOF over segment baseline")?;
                let replay_checkpoints = engine
                    .metrics()
                    .segment_checkpoint_completed_total
                    .get()
                    .saturating_sub(checkpoints_before);
                let recovered_head = start_seq.max(replayed);
                let aof_decision = if replayed > start_seq {
                    "tail_replayed"
                } else {
                    "no_tail"
                };
                tracing::info!(
                    aof_decision,
                    from_seq = start_seq,
                    to_seq = recovered_head,
                    replay_checkpoints,
                    replay_ms = replay_started.elapsed().as_millis() as u64,
                    "AOF startup decision"
                );
                start_seq = recovered_head;
                // Open the same AOF for continued appends (truncates any torn tail).
                let w = lumen::aof::AofWriter::open_with_policy(
                    &aof_path,
                    lumen::aof::FsyncPolicy::EverySec,
                )
                .context("open AOF")?;
                Some(std::sync::Arc::new(std::sync::Mutex::new(w)))
            }
            None => None,
        }
    } else {
        None
    };

    if deferred_graph_restore {
        // The shutdown cache can be newer than CURRENT. Match it only after
        // replaying durable AOF records, while no request can observe staging.
        let store = segment_store
            .as_ref()
            .expect("deferred graph restore needs a segment store")
            .clone();
        let recovered = engine.clone();
        tokio::task::spawn_blocking(move || store.finish_aof_graph_restore(&recovered))
            .await
            .context("graph restore worker failed")?
            .context("finish graph restore after AOF replay")?;
    }

    // Embedded backend: build the `MemWal` now that `start_seq` reflects the
    // final restore watermark (checkpoint restore, then AOF-tail replay if
    // any — whichever is higher). Raft bypasses `wal` entirely (#1486).
    let wal: Option<SharedWal> = match backend {
        WalBackend::Embedded => Some(Arc::new(MemWal::starting_at(start_seq))),
        _ => wal,
    };

    // (c) Start the apply loop. In segment mode with an AOF, the loop appends
    // every applied record to it; otherwise the default loop runs unchanged.
    // The raft path uses the `RaftHost` as its `WriteSink`; every other backend
    // uses the `WriteCoordinator` (sole applier over a `WalLog`). Both are erased
    // to `Arc<dyn WriteSink>` so the API binds to a single write seam.
    #[cfg(feature = "raft-wal")]
    let raft_writer = raft_writer.take();
    #[cfg(not(feature = "raft-wal"))]
    let raft_writer: Option<Arc<dyn lumen::coordinator::WriteSink>> = None;
    let writer: Arc<dyn lumen::coordinator::WriteSink> = if let Some(rw) = raft_writer {
        rw
    } else {
        let wal = wal.expect("non-raft backend yields a WAL");
        match aof_writer.clone() {
            Some(aof) => WriteCoordinator::start_from_with_aof(wal, engine.clone(), start_seq, aof),
            None => WriteCoordinator::start_from(wal, engine.clone(), start_seq),
        }
    };

    // `LUMEN_AUTH=required|in-cluster` delegates both halves of the decision to the
    // apiserver: TokenReview says who is calling, SubjectAccessReview says
    // whether they may (#2869). Building the verifier proves both grants before
    // the listener opens, so a missing `system:auth-delegator` binding is a
    // startup failure rather than a fleet that serves 503s while looking ready.
    //
    // The transport lives behind the `delegated-auth` feature. A build without
    // it can still parse either required mode — and must refuse to start
    // rather than quietly serve unauthenticated traffic under that setting.
    #[cfg(feature = "delegated-auth")]
    async fn serving_verifier(
        auth: &AuthConfig,
    ) -> anyhow::Result<Arc<lumen::auth::LumenVerifier>> {
        Ok(Arc::new(lumen::auth::LumenVerifier::connect(auth).await?))
    }
    #[cfg(not(feature = "delegated-auth"))]
    async fn serving_verifier(
        auth: &AuthConfig,
    ) -> anyhow::Result<Arc<lumen::auth::LumenVerifier>> {
        anyhow::bail!(
            "LUMEN_AUTH={} needs the Kubernetes TokenReview/SubjectAccessReview transport, \
             which this binary was built without. Rebuild with `--features delegated-auth`, or \
             unset LUMEN_AUTH to serve without authentication. Refusing to start.",
            auth.profile().env_value()
        )
    }

    let auth = Arc::new(AuthConfig::from_env()?);
    let verifier = if auth.required {
        let verifier = serving_verifier(&auth).await?;
        match auth.profile() {
            AuthProfile::ManagedAudience => tracing::info!(
                namespace = %auth.namespace,
                audience = lumen::auth::AUDIENCE,
                "auth=required — every request is authenticated by Kubernetes TokenReview and \
                 authorized by SubjectAccessReview; only audience-bound ServiceAccount tokens are \
                 accepted"
            ),
            AuthProfile::KubernetesDefault => tracing::info!(
                namespace = %auth.namespace,
                "auth=in-cluster — every request is authenticated by Kubernetes TokenReview \
                 against the apiserver's configured audiences and authorized by \
                 SubjectAccessReview; only Kubernetes ServiceAccount identities are accepted"
            ),
            AuthProfile::Off => unreachable!("required auth cannot use the off profile"),
        }
        Some(verifier)
    } else {
        tracing::warn!(
            "auth=off — requests are not authenticated. Set LUMEN_AUTH=required for Managed or \
             LUMEN_AUTH=in-cluster for Standalone (plus LUMEN_AUTH_NAMESPACE when needed) to \
             delegate to Kubernetes"
        );
        None
    };
    let admission = service_http::AdmissionConfig::from_env("LUMEN")?.controller(
        "lumen.read",
        "lumen.write",
        "lumen.admin",
    );
    if admission.is_some() {
        tracing::info!("request admission enabled (LUMEN_ADMISSION_*; probes stay exempt)");
    }

    let mut state = lumen::api::AppState::with_components(engine.clone(), auth, writer.clone());
    if let Some(verifier) = verifier {
        state = state.with_verifier(verifier);
    }
    if let Some(restore_sink) = segment_restore_sink(
        segment_mode,
        backend,
        engine.clone(),
        segment_store.clone(),
        writer.clone(),
        aof_writer.clone(),
    )? {
        state = state.with_restore_sink(restore_sink);
    }
    // #1389: wire a real on-demand checkpoint (`POST /admin/checkpoint`) only
    // when segment persistence is actually configured — the raft path has
    // its own snapshot mechanism and is out of the reshard driver's scope
    // (single-member only), and the default CBOR/no-data-dir path stays
    // `NoopCheckpoint` (nothing durable to force). Cloned from `segment_store`
    // before the periodic-snapshotter block below consumes it.
    if let Some(store) = segment_store.clone() {
        state = state.with_checkpoint(Arc::new(SegmentCheckpointSink {
            engine: engine.clone(),
            store,
            writer: writer.clone(),
            aof: aof_writer.clone(),
        }));
    }
    // Populate #1310's read-consistency enforcement seam with live cluster
    // state (#1349) — only in raft mode; standalone/legacy-log backends
    // correctly leave `state.cluster` `None` (single authoritative copy).
    #[cfg(feature = "raft-wal")]
    if let Some(cluster) = raft_cluster {
        state = state.with_cluster(cluster);
    }
    if !args.search_shard_segment_dirs.is_empty() {
        let shards = load_search_shard_segment_roots(&args.search_shard_segment_dirs)?;
        // #1384: route by the operator/reshard-driver-committed shard map
        // (SHARD_MAP_VERSION/SHARD_MAP_ASSIGNMENTS/VIRTUAL_BUCKET_COUNT env)
        // instead of always assuming the balanced default — queries for
        // buckets moved by a completed autonomous split must land on their
        // new physical shard.
        //
        // #1398 R4: the physical shard count that map is built for defaults
        // to the number of loaded dirs (matching `EngineShardSearch::new`'s
        // original behavior) unless `--shard-count`/`SHARD_COUNT` was
        // explicitly set — see `fan_in_shard_count`'s doc comment for why
        // `args.shard_count` has to be `Option<u32>` to make that
        // distinction. A mismatch between the resolved count and the
        // actual loaded-dir count fails startup instead of silently
        // under-routing.
        let fan_in_shard_count = lumen::config::fan_in_shard_count(args.shard_count, shards.len());
        let shard_map = lumen::config::shard_map_from_env(fan_in_shard_count).context(
            "shard map from env (SHARD_MAP_VERSION/SHARD_MAP_ASSIGNMENTS/VIRTUAL_BUCKET_COUNT)",
        )?;
        lumen::config::check_fan_in_shard_count(&shard_map, shards.len())?;
        tracing::info!(
            shard_count = shards.len(),
            shard_map_version = shard_map.version(),
            "search backend=segment-sharded"
        );
        state = state.with_search_backend(Arc::new(
            lumen::routing::EngineShardSearch::new_with_shard_map(shards, shard_map),
        ));
    }
    // #1398 R1: activate cross-pod routing only in the operator/k8s routed
    // serving topology (`SHARD_COUNT` env > 1 at `replicasPerShard <= 1`) —
    // `routed_activation_shard_count` returns `None` for every other
    // deployment shape, so `shardCount:1` serving never even constructs a
    // `RoutedRouter` (AC5). It also folds in the fan-in mutual-exclusion
    // guard (#1442 R3): the fan-in path above already built its own local
    // `EngineShardSearch`/`state.search_backend` when
    // `args.search_shard_segment_dirs` is non-empty, so a fan-in invocation
    // must never also reach the routed block — pulled into
    // `config::routed_activation_shard_count` so that guarantee is
    // unit-tested directly instead of only by inline control flow here.
    #[cfg(feature = "operator")]
    if let Some(shard_count) =
        lumen::config::routed_activation_shard_count(args.search_shard_segment_dirs.is_empty())
            .context("routed shard count from env (SHARD_COUNT)")?
    {
        let shard_map = lumen::config::shard_map_from_env(shard_count).context(
            "shard map from env (SHARD_MAP_VERSION/SHARD_MAP_ASSIGNMENTS/VIRTUAL_BUCKET_COUNT)",
        )?;
        let (prefix, local_shard) = lumen::config::routed_pod_topology(shard_count)
            .context("routed pod topology from env (POD_NAME)")?;
        let headless = std::env::var("LUMEN_HEADLESS_SERVICE")
            .unwrap_or_else(|_| "lumen-headless".to_string());
        let shard_urls: Vec<String> = (0..shard_count)
            .map(|shard| {
                format!(
                    "http://{}:{}",
                    lumen::routing::shard_host(&prefix, shard, &headless),
                    args.port
                )
            })
            .collect();
        tracing::info!(
            shard_count,
            local_shard,
            shard_map_version = shard_map.version(),
            "cross-pod shard routing active"
        );
        // #1467 R5: publish this pod's live shard-map version on `/metrics`
        // so the reshard driver's `advance_convergence` can require every
        // serving pod to actually report the new map, not just that its
        // StatefulSet rollout finished.
        engine.metrics().set_shard_map_version(shard_map.version());
        let router = lumen::routing_remote::RoutedRouter::new(
            engine.clone(),
            state.write_backend.clone(),
            shard_map,
            local_shard,
            shard_urls,
        )
        .context("construct routed shard router")?;
        state = state.with_routed(Arc::new(router));
    }
    #[cfg_attr(not(feature = "raft-wal"), allow(unused_mut))]
    let mut app = lumen::api::router_with_admission(state, admission);
    // Plain local/backward-compatible Raft RPCs share the public h2c port.
    // Configured mTLS peers are served only by the dedicated listener below.
    #[cfg(feature = "raft-wal")]
    if raft_peer_transport.is_none() {
        if let Some(host) = &raft_host {
            app = app.merge(host.router());
        }
    }

    // Raft compaction uses the state machine's snapshot backend. Segment mode
    // prepares and exports immutable files outside the host apply boundary.
    // Otherwise the RDB snapshotter writes the `--data-dir` checkpoints the apply
    // loop tails from on restart.
    #[cfg(feature = "raft-wal")]
    if let Some(host) = raft_host.clone() {
        let period = Duration::from_secs(args.snapshot_secs.max(1));
        let snap_engine = engine.clone();
        tokio::spawn(async move {
            loop {
                // Start each interval after the previous attempt completes.
                tokio::time::sleep(period).await;
                match host.snapshot_and_compact().await {
                    Ok(idx) if idx > 0 => {
                        tracing::info!(snapshot_index = idx, "raft snapshot taken + log compacted")
                    }
                    Ok(_) => {}
                    Err(e) => {
                        // #2516: the raft snapshot is itself a durable
                        // checkpoint write — same ENOSPC treatment as the
                        // non-raft RDB/segment snapshotters below.
                        if lumen::coordinator::is_storage_full(&e) {
                            tracing::error!(error = %e, "raft snapshot/compact hit ENOSPC — entering degraded read-only mode");
                            snap_engine.metrics().mark_storage_degraded();
                        } else {
                            tracing::warn!(error = %e, "raft snapshot/compact failed");
                        }
                    }
                }
            }
        });
    }
    if let (false, Some(store)) = (is_raft, rdb_store) {
        let snap_engine = engine.clone();
        let snap_writer = writer.clone();
        let period = Duration::from_secs(args.snapshot_secs.max(1));
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(period);
            ticker.tick().await; // skip immediate fire
            loop {
                ticker.tick().await;
                let _checkpoint_permit = match snap_writer.mutation_gate() {
                    Some(gate) => match gate.shared().await {
                        Ok(permit) => Some(permit),
                        Err(e) => {
                            tracing::error!(error = %e, "RDB checkpoint blocked by durability state");
                            continue;
                        }
                    },
                    None => None,
                };
                let seq = snap_writer.applied_seq();
                match RdbSnapshot::capture(&snap_engine, seq) {
                    Ok(rdb) => {
                        if let Err(e) = store.save(&rdb).await {
                            // #2516: a checkpoint write is a durable write
                            // path too — ENOSPC here must enter the same
                            // sticky degraded read-only mode as an AOF
                            // ENOSPC, not just a one-off warn.
                            if lumen::coordinator::is_storage_full(&e) {
                                tracing::error!(error = %e, "RDB snapshot save hit ENOSPC — entering degraded read-only mode");
                                snap_engine.metrics().mark_storage_degraded();
                            } else {
                                tracing::warn!(error = %e, "RDB snapshot save failed");
                            }
                        } else {
                            tracing::info!(up_to_seq = seq, "RDB snapshot written");
                            let _ = store.prune(3).await;
                        }
                    }
                    Err(e) => tracing::warn!(error = %e, "RDB capture failed"),
                }
            }
        });
    }

    // Finish any bootstrap save before the driver that can trim AOF starts.
    if let Some(driver) = &mut replay_checkpoint_driver {
        driver
            .stop_bootstrap()
            .await
            .context("finish recovery checkpoint driver")?;
    }
    if segment_store.is_some() {
        if let Some(driver) = &mut pending_spill {
            driver
                .stop_bootstrap()
                .await
                .context("finish private bootstrap spill")?;
        }
    }
    // Manual and periodic saves share checkpoint_now. Retain the handle so
    // its waiter thread receives shutdown with this serving scope.
    let segment_checkpoint_sink = segment_store.map(|store| {
        Arc::new(SegmentCheckpointSink {
            engine: engine.clone(),
            store,
            writer: writer.clone(),
            aof: aof_writer.clone(),
        })
    });
    let mut segment_checkpoint_driver = segment_checkpoint_sink.as_ref().map(|sink| {
        sink.clone()
            .spawn_periodic_driver(Duration::from_secs(args.snapshot_secs.max(1)))
    });
    // Raft retains its existing shutdown/snapshot contract. This optional cache
    // belongs only to the standalone segment persistence path.
    let shutdown_checkpoint_sink = if is_raft {
        None
    } else {
        segment_checkpoint_sink
    };

    // #2516: periodic ENOSPC re-probe. While this node is in degraded
    // read-only mode (`Metrics::storage_degraded`), attempt a small write
    // into `--data-dir` every `LUMEN_STORAGE_FULL_REPROBE_SECS` (default 30s)
    // and clear the sticky flag once one succeeds — the automatic recovery
    // path for a PVC that was resized or freed up without a pod restart.
    // (Operators can also just restart the pod: the flag is process-local
    // and starts clear on a fresh process.) Only probes while degraded, so a
    // healthy node pays nothing extra.
    if let Some(dir) = args.data_dir.clone() {
        let probe_engine = engine.clone();
        let reprobe_secs: u64 = std::env::var("LUMEN_STORAGE_FULL_REPROBE_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .filter(|&v| v > 0)
            .unwrap_or(30);
        let probe_path = std::path::Path::new(&dir).join(".storage_full_probe");
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(reprobe_secs));
            ticker.tick().await; // skip immediate fire
            loop {
                ticker.tick().await;
                if !probe_engine.metrics().is_storage_degraded() {
                    continue;
                }
                match tokio::fs::write(&probe_path, b"ok").await {
                    Ok(()) => {
                        probe_engine.metrics().clear_storage_degraded();
                        tracing::info!(
                            "storage re-probe write succeeded; leaving degraded read-only mode"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "storage re-probe still failing");
                    }
                }
            }
        });
    }

    // #3113 R1: the client port's own identity, loaded before the socket binds
    // so a pod with unusable material fails startup instead of accepting
    // connections it cannot serve. `None` is the h2c posture — local and kind
    // development, and the only path to cleartext on this port.
    let serving_tls = lumen::tls::ServingTlsConfig::from_env()
        .context("load serving TLS configuration")?
        .map(|config| {
            let names = config.dns_names.clone();
            config.reloadable().map(|tls| (tls, names))
        })
        .transpose()
        .context("activate the serving certificate")?;

    let bind = format!("{}:{}", args.host, args.port);
    let listener = tokio::net::TcpListener::bind(&bind)
        .await
        .with_context(|| format!("bind {bind}"))?;
    match &serving_tls {
        Some((tls, names)) => tracing::info!(
            addr = %bind,
            shard_count = args.shard_count.unwrap_or(1),
            tls_generation = tls.generation(),
            server_names = %names.join(","),
            "lumen serve listening over TLS"
        ),
        None => tracing::info!(
            addr = %bind,
            shard_count = args.shard_count.unwrap_or(1),
            "lumen serve listening"
        ),
    }

    #[cfg(feature = "raft-wal")]
    let peer_server = if let (Some(host), Some(transport)) =
        (raft_host.as_ref(), raft_peer_transport.as_ref())
    {
        let peer_bind = format!("{}:{}", args.host, args.raft_port);
        let peer_listener = tokio::net::TcpListener::bind(&peer_bind)
            .await
            .with_context(|| format!("bind authenticated raft peer listener {peer_bind}"))?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let peer_router = host.router();
        let transport = transport.clone();
        tracing::info!(addr = %peer_bind, tls_generation = transport.generation(), "lumen raft peer mTLS listening");
        let task = tokio::spawn(async move {
            transport
                .serve(peer_listener, peer_router, async {
                    let _ = shutdown_rx.await;
                })
                .await
        });
        Some(RaftPeerServer { shutdown_tx, task })
    } else {
        None
    };

    let grace = Duration::from_secs(args.grace_secs);
    let shutdown_engine = engine.clone();
    let shutdown_aof = aof_writer.clone();
    #[cfg(feature = "raft-wal")]
    let shutdown_raft_host = raft_host.clone();
    #[cfg(feature = "raft-wal")]
    let shutdown_peer_server = peer_server;
    let shutdown_error = Arc::new(std::sync::Mutex::new(None::<String>));
    #[cfg(feature = "raft-wal")]
    let shutdown_error_slot = shutdown_error.clone();
    // The legacy HTTP adapters begin their drain only after this future
    // resolves.  Keep that edge separate from the Raft work below: it lets
    // the public listener drain while Raft consumes the same signal-time
    // deadline, rather than adding the adapter's default drain timeout after
    // the deadline has already expired.
    let (http_shutdown_tx, http_shutdown_rx) = tokio::sync::oneshot::channel();
    let mut http_server = match serving_tls {
        // #3113 R1/R9: the configuration is read per accepted connection, so
        // a renewed leaf reaches connection N+1 with no rebind and no restart.
        // `None` from the source refuses the connection — there is deliberately
        // no branch here that answers it in cleartext instead.
        Some((tls, _)) => tokio::spawn(service_http::serve_tls(
            listener,
            app,
            service_http::config_source(move || tls.server_config()),
            async move {
                let _ = http_shutdown_rx.await;
            },
        )),
        None => tokio::spawn(service_http::serve(listener, app, async move {
            let _ = http_shutdown_rx.await;
        })),
    };
    // Serve HTTP/1.1 + h2c on one port through the shared service HTTP shell,
    // with the standard SIGTERM drain sequence flipping `/readyz` to 503
    // before the listener closes. The single-replica segment AOF is synced at
    // the *start* of termination, not after the full drain window: the pod's
    // termination grace can equal that window, so a post-drain sync may lose
    // the race with Kubernetes' SIGKILL.
    let shutdown = async move {
        service_http::wait_shutdown_signal().await;
        let deadline = server_lifecycle::ShutdownDeadline::from_now(grace, Duration::ZERO)
            .expect("zero reserve must fit the Lumen shutdown grace");
        let _ = http_shutdown_tx.send(());
        #[cfg(feature = "raft-wal")]
        if let Some(host) = shutdown_raft_host.as_ref() {
            host.quiesce_proposals();
        }
        {
            shutdown_engine.start_drain();
            if let Some(aof) = shutdown_aof {
                match aof.lock() {
                    Ok(mut writer) => {
                        if let Err(e) = writer.sync() {
                            tracing::warn!(error = %e, "sync local AOF during shutdown failed");
                        }
                    }
                    Err(_) => tracing::warn!("local AOF writer poisoned during shutdown"),
                }
            }
        }

        if let Some(sink) = shutdown_checkpoint_sink {
            if let Some(driver) = &mut segment_checkpoint_driver {
                driver.request_stop();
            }
            if deadline.remaining().is_zero() {
                tracing::warn!("optional HNSW shutdown cache skipped after shutdown deadline");
            } else {
                match tokio::time::timeout_at(deadline.expires_at, sink.save_shutdown_graph_cache())
                    .await
                {
                    Ok(Ok(0)) => tracing::debug!("no HNSW shutdown cache needed"),
                    Ok(Ok(fields)) => tracing::info!(fields, "HNSW shutdown cache saved"),
                    Ok(Err(error)) => {
                        tracing::warn!(%error, "optional HNSW shutdown cache unavailable")
                    }
                    Err(_) => {
                        tracing::warn!("optional HNSW shutdown cache reached the shutdown deadline")
                    }
                }
            }
        }
        #[cfg(feature = "raft-wal")]
        if let Some(host) = shutdown_raft_host {
            if let Err(error) = shutdown_raft_within(host, shutdown_peer_server, deadline).await {
                *shutdown_error_slot
                    .lock()
                    .expect("shutdown error slot poisoned") = Some(error.to_string());
            }
        }
        tracing::info!(grace_secs = grace.as_secs(), "draining");
        match tokio::time::timeout_at(deadline.expires_at, &mut http_server).await {
            Ok(Ok(())) => tracing::info!("http server drained; shutting down"),
            Ok(Err(error)) => tracing::warn!(%error, "http server task failed during drain"),
            Err(_) => {
                http_server.abort();
                let _ = http_server.await;
                tracing::info!("grace expired; shutting down");
            }
        }
    };
    shutdown.await;
    if let Some(error) = shutdown_error
        .lock()
        .expect("shutdown error slot poisoned")
        .take()
    {
        anyhow::bail!(error);
    }
    // Flush any batched spans before exit (no-op when OTLP was never enabled).
    #[cfg(feature = "otel")]
    opentelemetry::global::shutdown_tracer_provider();
    Ok(())
}
