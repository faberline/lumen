//! The operator storage and ops contract `lumen llm --topic storage` serves.

/// Operator storage/ops contract (`lumen llm --topic storage`) as Markdown: the
/// serving fleet's workload kind and PVC durability guarantee, independent of
/// `replicasPerShard`.
pub fn llm_storage_md() -> String {
    let mut out = r#"# lumen storage

## The serving fleet is always a StatefulSet
The operator (`lumen::operator::render`) renders the serving fleet as a
Kubernetes `StatefulSet` unconditionally — never a `Deployment` — regardless
of `spec.replicasPerShard`. Every serving pod mounts a durable
`volumeClaimTemplates`-backed PVC named `raft` at `/var/lib/lumen`, sized by
`spec.serving.raftStorage` (default `10Gi`) and optionally pinned to
`spec.serving.raftStorageClass`.

This means a pod reschedule, eviction, or node loss never wipes the WAL —
including for a `replicasPerShard: 1` deployer who doesn't want or need raft
consensus. `replicasPerShard` only changes whether the fleet runs raft
consensus; it never changes whether the WAL is durable — but the PVC being
mounted is not by itself sufficient; see the next section for what actually
makes the single-member WAL durable.

## `replicasPerShard: 1` (default) — single member, no raft consensus
- One StatefulSet member per shard, with the durable `raft` PVC.
- No raft peer-identity env — the pod runs a local WAL with no consensus
  overhead.
- The legacy single-shard HPA path is serving-capacity only. It is not a
  primary/follower data-replica mode, and extra pods do not continuously catch
  up a shared shard from a primary.

## Embedded-mode persistence and crash durability (#1387)
`LUMEN_WAL=auto` resolves to `Embedded` — an in-process `MemWal` — whenever
there is no raft cluster context, i.e. exactly the `replicasPerShard: 1`
regime above. `Embedded` alone is RAM-only: mounting the `raft` PVC is not
sufficient by itself, because nothing writes to it unless `LUMEN_DATA_DIR` is
also set. Prior to #1387 the operator never set it, so a pod restart —
including the reshard cutover's own rolling restart — silently wiped all
data despite the PVC being durably attached.

The operator now renders, only at `replicasPerShard <= 1`:

```
LUMEN_DATA_DIR=/var/lib/lumen/data
LUMEN_PERSISTENCE=segment
```

`/var/lib/lumen/data` is disjoint from the raft backend's own
`/var/lib/lumen/raft` subtree (`LUMEN_RAFT_DATA_DIR`'s default) on the same
`raft` PVC mount, so both can coexist safely across a `replicasPerShard`
change without colliding. `LUMEN_PERSISTENCE=segment` (rather than the CBOR
default) activates the local AOF (`src/persistence/infrastructure/aof.rs`)
alongside the periodic segment checkpoint
(`src/persistence/infrastructure/segment_rdb_store.rs`): every applied write
is appended to the AOF and fsynced under the `everysec` policy (at most ~1s
of un-fsynced tail on a crash — a torn tail that replay discards cleanly, not
corruption),
so crash durability (kill -9 / OOM, not just a clean SIGTERM drain) is bounded
by roughly a 1-second recovery point, not by `LUMEN_SNAPSHOT_SECS` (default
300s, the periodic checkpoint interval used only to bound cold-start replay
and trim the AOF — not the durability window itself). Cold start reopens the
newest segment checkpoint, replays the AOF tail past it, then tails the
broker from there — the existing `serve()` bootstrap path, unchanged by this
render wiring.

### Dev mode: bare `lumen serve` stays in-memory
Running `lumen serve` directly (outside the operator, with no `LUMEN_DATA_DIR`
set) is unaffected and keeps today's behavior: `--wal auto` still resolves to
`Embedded`, and with no data dir configured the engine is purely in-memory —
any restart loses all data. This is intentional dev-mode behavior, not a bug:
set `--data-dir`/`LUMEN_DATA_DIR` (and optionally `--persistence=segment`)
explicitly to get the same durability the operator now wires by default.

The shipped image supplies `LUMEN_DATA_DIR=/var/lib/lumen/data`,
`LUMEN_PERSISTENCE=segment`, `LUMEN_WAL=embedded`, and
`VOLUME ["/var/lib/lumen/data"]`. A named volume or a caller-managed bind mount
at that exact path lets container data survive replacement.

Standalone GKE uses the shared `StatefulInstancePlan` boundary to render one
StatefulSet plus a separately owned PVC instance. Its public config has no image
field; the renderer fixes the published version. The Standalone GKE live
acceptance is manual, controller-run, and paid. It is separate from
Managed/operator GCP acceptance, and candidate CI makes no GKE or `gcloud`
claim.

## `replicasPerShard > 1` — raft-HA
- Fixed replica count `shardCount * replicasPerShard` (raft needs a known,
  stable peer set) — no HPA is attached.
- Each pod additionally gets the downward-API env quartet
  `raft_runtime::cluster::ClusterTopology::from_env` reads (`POD_NAME`,
  `POD_NAMESPACE`, `REPLICAS_PER_SHARD`, `VOTER_COUNT`,
  `LUMEN_HEADLESS_SERVICE`) and a stable DNS identity via the serving
  headless Service (`<name>-headless`), required for the StatefulSet's
  `serviceName`.
- This regime, including the PVC, is unchanged from before `replicasPerShard:
  1` also started getting a StatefulSet.

## Shards, replicas, and HPA
Storage topology has two independent knobs:

- `spec.shardCount` controls how many physical storage shards own the corpus.
- `spec.replicasPerShard` controls HA inside each shard group.

The serving pod count is `shardCount * replicasPerShard`. StatefulSet pod
ordinals map to topology slots with `shardIndex = ordinal % shardCount` and
`replicaIndex = ordinal / shardCount`. HPA does not change storage ownership:
it must never be used as the mechanism for increasing `shardCount` or changing
raft membership. Dynamic shard growth is an operator workflow driven by
storage pressure and a versioned virtual-bucket map, with bounded snapshot-batch
movement between physical shards.

When an operator does not know the max shard size or max shard count, it should
report the pressure condition instead of auto-splitting.

## Empty-PVC replica bootstrap
Existing pods restart from their PVC-local raft state, snapshots, and WAL. A
new replacement pod with an empty PVC should seed from an exact `SnapshotV1`
object before WAL/raft delta catch-up:

```
LUMEN_BOOTSTRAP_SEED_URI=file:///snapshots/shard-0.json
LUMEN_BOOTSTRAP_SEED_URI=s3://bucket/path/shard-0.json
```

External backup is the cold disaster-recovery and bootstrap seed surface; it is
not the normal live replica synchronization mechanism.

## Upgrading `<=0.4.9` Deployment-backed instances to `>=0.4.10` (#834)
Lumen `<=0.4.9` rendered `spec.replicasPerShard: 1` serving fleets as an
`apps/v1` `Deployment` named `<name>`. Lumen `>=0.4.10` renders the serving
fleet as an `apps/v1` `StatefulSet` with the same `<name>`. Kubernetes treats
those as different resources, and the shared operator only server-side-applies
the currently rendered child objects; it does not prune a stale child object
whose API kind changed. Applying the new operator/image alone can therefore
leave the old `Deployment/<name>` beside the new `StatefulSet/<name>`.

Use an explicit handoff for any cluster that already reconciled the CR with
`<=0.4.9`:

1. Apply the new CRD first if needed. This only updates the schema and is safe
   before the workload handoff.
2. Schedule write downtime and take an admin backup (`GET /admin/backup`) if
   you need to carry data into the new PVC-backed StatefulSet.
3. Pause the old `<=0.4.9` operator reconciliation, for example by scaling the
   operator `Deployment/lumen-operator` to zero or pausing the GitOps rollout
   that runs it. Otherwise the old operator or old HPA can recreate/scale the
   serving Deployment while you are migrating.
4. Stop the old serving workload before the `>=0.4.10` operator reconciles:

   ```
   kubectl -n <ns> scale deployment/<name> --replicas=0
   kubectl -n <ns> delete deployment/<name> --wait=true
   ```

   Scaling first is reversible; deleting with `--wait=true` makes the handoff
   boundary explicit.
5. Deploy or unpause the `>=0.4.10` operator/image and let it create
   `StatefulSet/<name>` plus the `raft-<name>-<ordinal>` PVCs.
6. Wait for the new fleet before resuming traffic or writes:

   ```
   kubectl -n <ns> rollout status statefulset/<name>
   ```

Do not run both the old `Deployment/<name>` pods and the new
`StatefulSet/<name>` pods behind the same Service. They have independent WAL /
engine storage, and the operator does not copy a Deployment pod's filesystem
or local WAL into the new StatefulSet PVC. If you must preserve data from the
old Deployment-backed pod, restore an admin backup into the new pod or rebuild
from your upstream source-of-truth before reopening writes.

## Snapshot / backup (#808)
The durable `raft` PVC protects against pod reschedule/eviction/node loss,
but it is not an off-node backup: it does not protect against a bad write, a
namespace deletion, or a lost PVC/PV. Lumen already exposes a safe,
consistent, manual snapshot-restore procedure over its admin API; production
CRs schedule the same snapshot bytes to object storage.

### Manual admin API (always available)
Every serving node — regardless of `replicasPerShard` — answers three admin
routes, each requiring `Role::Admin` on `*` (the wildcard subject, not a
per-collection grant) when `spec.auth: required`:

- `GET /admin/backup` — snapshots the live engine (`Engine::snapshot()`, the
  same quiesce-free call the raft snapshotter itself uses — no separate
  flush/quiesce step needed) and returns it as a `SnapshotV1` JSON document.
  Safe to call against any replica at any time; it does not pause writes.
- `POST /admin/backup/local` — same snapshot, written directly to a path on
  the pod's own filesystem via a `LocalFsSink` (`{"path": "...", "prefix":
  "lumen-backup"}` request body). Useful when the pod already has a mounted
  destination volume.
- `POST /admin/restore` — replaces *all* engine state with a `SnapshotV1`
  document (the same shape `/admin/backup` returns). Destructive; there is no
  merge or partial-restore mode.

These three routes are the safe procedure for ad hoc or scripted
snapshot/restore — pull with `GET /admin/backup`, keep the bytes wherever you
like, push back with `POST /admin/restore` to recover.

### Reshard admin verbs (#1380, #1389, #1396, #1457)
Six more `Role::Admin`-gated routes support moving a bounded set of
documents between shards during an operator-driven reshard, without a
full-engine restore:

- `POST /admin/backup:scoped` — like `GET /admin/backup`, but restricted to
  documents routed to a requested set of virtual buckets:
  `{"virtual_bucket_count": N, "buckets": [0, 3, ...]}`. Bucket membership is
  computed with the same hash the engine's own routing uses, so an export
  and a batch computed against the same map can never disagree about which
  documents belong to which bucket.
- `POST /admin/reshard:apply` — additively merges one `ReshardBatch`'s
  snapshot into the live engine: upsert semantics for the batch's documents,
  never a full replace, so a target shard's pre-existing data outside the
  batch is untouched. Safe to retry — replaying the same batch (operator
  resume after a checkpoint) converges to the same query-visible state.
- `POST /admin/reshard:evict` — source-side post-cutover cleanup. Given a
  newer virtual-bucket map (`{"shard": N, "map_version": V, "assignments":
  [...], "physical_shard_count": N}`) and this shard's own index within it,
  removes exactly the documents whose bucket no longer routes to this
  shard — nothing else. A separate, explicitly-invoked step; never implicit
  in `/admin/reshard:apply` or the backup routes above.
- `POST /admin/reshard:fence` (#1396 R2) — arms or clears a bounded write
  pause on a set of virtual buckets: `{"virtual_bucket_count": N, "buckets":
  [0, 3, ...], "ttl_secs": 300}`; an empty `buckets` array clears the fence
  instead of arming it. A write routed to a currently-fenced bucket is
  rejected with a retryable `503 bucket_write_paused` rather than being
  silently dropped or applied against a map that is about to change. `ttl_secs`
  defaults to 300 and is capped at 3600 (a request outside `1..=3600` is
  rejected with `400 invalid_ttl_secs`); expiry is enforced on the *serving*
  pod independent of the caller, so a caller that dies between arming and
  clearing can never leave a bucket permanently unwritable. This is a
  **driver-owned** verb: the reshard driver (`service_k8s::reshard_driver::
  advance_catching_up`) arms it over exactly the buckets its final
  `CatchingUp` migration pass is about to copy, immediately before that pass,
  and always clears it (`buckets: []`) on every exit path of that tick —
  success or `Blocked` — re-arming a fresh deadline every tick it still needs
  one (`reshard_driver::WRITE_FENCE_TTL_SECS`, 120s). Calling it manually
  outside driver-orchestrated cutover risks a real write outage: an operator
  who arms it and forgets to clear it (or races the driver's own arm/clear
  cycle) pauses writes to those buckets until the TTL lapses.
- `POST /admin/reshard:prune` (#1457 R1) — accumulates one byte-capped
  `ReshardPruneChunk` of a final migration pass's authoritative "keep" id
  set for one `(bucket, collection_id)` pair, keyed by `(to_map_version,
  bucket, collection_id, total_chunks)`, and prunes any document this shard
  holds that routes to that bucket but is absent from the accumulated set
  once every chunk has arrived. Unlike `/admin/reshard:apply` (always
  purely additive), this is what makes the final, fenced `CatchingUp` pass
  authoritative: a document deleted on the source during the split is
  absent from the accumulated keep set and is pruned here instead of
  surviving as a stale copy from an earlier additive pass. Independently
  byte-capped from `/admin/reshard:apply`'s own batches, so a bucket whose
  id set alone would exceed the body limit still converges via multiple
  chunks rather than ever producing an over-limit request. Idempotent both
  per chunk (safe to retry after a 413) and as a whole group (safe to
  re-send every chunk after a driver restart — re-running an
  already-completed group's accumulate-then-apply sequence is a no-op
  against already-pruned state).
- `POST /admin/checkpoint` — forces a synchronous, awaited durability
  checkpoint of the live engine state, bypassing the periodic
  `LUMEN_SNAPSHOT_SECS` cadence. `/admin/reshard:apply` and
  `/admin/reshard:evict` mutate engine state directly rather than through
  `WriteCoordinator`/the AOF, so without this verb their effects are only
  captured by the next periodic segment checkpoint — a window a pod restart
  can land inside and silently lose (target: the whole batch; source: the
  eviction, i.e. `documents_indexed` reverting upward). The reshard phase
  driver (`advance_catching_up`,
  `src/operator/application/reshard_driver/`) calls this on every shard touched by a
  split — every old shard plus the new one — and awaits success on all of
  them before patching `spec.shardMap` and triggering the cutover rolling
  restart, so a batch or eviction is only ever counted "migrated" once it
  can survive that restart. Returns `{"persisted": bool}`: `true` when a
  real durable store was actually written, `false` when no durable store is
  configured (e.g. tests, or a deployment running without segment
  persistence) — a vacuous success, not an error, so the verb is always safe
  to call.

  Two designs were considered for this durability gap: (a) route
  `apply`/`evict` through the AOF/`WriteCoordinator` as new log-entry types,
  or (b) the explicit synchronous checkpoint step described above, invoked
  and awaited by the driver per touched shard before cutover. (b) was
  chosen: it reuses `SegmentRdbStore::save` exactly as the periodic
  snapshotter already does — a full atomic re-seal of the current engine
  state, independent of which code path produced that state — with no new
  WAL record shape, apply-loop branch, or distinct idempotency reasoning.
  (a) would require a new `ReshardBatch`-shaped log entry that doesn't fit
  the existing single-mutation entry variants, plus a second, different
  notion of "already applied" alongside `merge_snapshot_delta`'s own
  idempotent merge semantics.

These six verbs are the data-plane building blocks for a reshard; only
`/admin/checkpoint`'s ordering relative to cutover is sequenced by the
operator phase driver — the rest do not sequence a migration end to end or
decide *when* to cut over. `/admin/reshard:fence` is armed/cleared by that
same driver around the `CatchingUp` pass — it is not an independent step an
operator sequences by hand.

### Direct CLI data movement: `dump` / `export` / `load` / `import`
For ad hoc SnapshotV1 movement from a shell, use the direct CLI wrappers:

```
lumen export --url http://localhost:7373 --out snapshot.json
lumen import --url http://localhost:7373 --file snapshot.json
```

`lumen dump` is an alias of `export`, and `lumen load` is an alias of
`import`. With no `--out`, dump/export write the exact SnapshotV1 JSON bytes to
stdout; with no `--file`, load/import read SnapshotV1 JSON from stdin. These
verbs do not add a new format, merge mode, or partial import semantics:
load/import still replace all engine state via `/admin/restore`. Neither verb
acquires a credential of its own: the metadata-server ID token they used to
mint is gone (#2871), because a Google-issued token is not something the
Kubernetes-native verifier can ever accept.

### Required production scheduled backup: `spec.serving.backup`
Production Lumen CRs set `spec.serving.backup` so the operator renders a
`<name>-backup` `batch/v1` CronJob that runs `lumen backup` on a schedule and
writes the snapshot to object storage. This adds no new snapshot mechanism — it
only *schedules and transports* the same `GET /admin/backup` bytes above to a
destination:

```yaml
spec:
  serving:
    backup:
      schedule: "0 * * * *"        # CronJob.spec.schedule
      destination: "gs://my-bucket/lumen-backups"  # file:// | s3:// | gs://
      retentionSecs: 604800        # optional; drop objects older than this
      adminTokenSecret: lumen-backup-token  # deprecated no-op; kept so pre-#2764 CRs still apply
```

Use `s3://` or `gs://` for production object-storage snapshots. GCS accepts an
explicit `GOOGLE_OAUTH_ACCESS_TOKEN`/`GCS_ACCESS_TOKEN` and otherwise resolves
the GCE/GKE metadata-server token used by Workload Identity. `file://` remains
a local-dev, migration, or break-glass sink and does not satisfy the production
service archetype.

`adminTokenSecret` (the Secret name above) is deprecated and has no effect as of
#2764; setting it generates no error but no bearer token is injected. The
backup runner authenticates as its own ServiceAccount, with a projected
audience-bound token the cluster mints for it — there is no Secret, and no
Google-issued token, anywhere on that path.

Omitting `spec.serving.backup` renders no CronJob. That is acceptable for local
development or manual recovery exercises, but a production Lumen instance is not
service-archetype complete until the scheduled object snapshot is configured.

### `lumen backup` CLI verb
The CronJob (and any ad hoc invocation) drives the same verb:

```
lumen backup --url http://<name>.<namespace>.svc.cluster.local:7373 \
  --dest s3://my-bucket/lumen-backups \
  [--retention-secs 604800]
```

`--url` points at the serving Service (not a specific pod). There is no
credential flag: #2871 removed the metadata-server ID token this used to mint,
and #2873 removed the bearer flag that was left, because a credential passed as
an argument is a credential in `ps` and in `kubectl describe`. The request
carries no `Authorization` header, which is correct against today's only
startable mode (`auth: disabled`); the projected, audience-bound ServiceAccount
token the backup runner will present instead is #2877. The verb GETs
`/admin/backup`, hands the
bytes to the `libs/service-backup` destination sink named by `--dest`, prunes
by `--retention-secs` if given, and prints the resulting `BackupRunResult` as
JSON. It needs the `backup` Cargo feature (pulled in transitively by
`operator`; the published image includes both).

## Resizing `raftStorage` (#809)
`spec.serving.raftStorage` is baked into the StatefulSet's
`volumeClaimTemplates` at first apply. Kubernetes treats
`volumeClaimTemplates` as **immutable** after creation, so editing
`spec.serving.raftStorage` on a live CR and letting the operator reconcile
does **not** resize anything — the StatefulSet `apply` is a silent no-op for
that field, and the pods' existing PVCs stay at their original size. This is
true for every `replicasPerShard` value, including the default
`replicasPerShard: 1` single-member topology.

Growing storage requires patching each per-pod PVC directly:

```
kubectl patch pvc raft-<name>-<n> --type merge \
  -p '{"spec":{"resources":{"requests":{"storage":"<new size>"}}}}'
```

This only succeeds if the PVC's bound `StorageClass` has
`allowVolumeExpansion: true`; otherwise the API server rejects the patch.
Kubernetes does not support shrinking a bound PVC — a smaller
`raftStorage` value only affects newly created PVCs (a fresh instance or a
recreated pod), never an existing one.

### `lumen k8s operator resize-storage` CLI verb
Rather than patching PVCs by hand, run the automated form of the same
procedure:

```
lumen k8s operator resize-storage --namespace <ns> --name <name> [--dry-run]
```

This fetches the named `Lumen` CR's declared `spec.serving.raftStorage`,
lists that instance's live `raft-<name>-<n>` PVCs, and for each PVC whose
current size is smaller: checks the bound `StorageClass.allowVolumeExpansion`
and, when it's `true`, patches only `spec.resources.requests.storage`
(`Patch::Merge`, no other PVC field touched) — unless `--dry-run` is given,
in which case it reports what it would do without patching anything. PVCs
already at the desired size, PVCs whose `StorageClass` does not allow
expansion, and shrink requests are reported but never mutated. It needs the
`operator` Cargo feature (`--features operator`), the same feature gate as
`lumen k8s operator run`.

## Choosing an SSD-backed StorageClass for `raftStorage` (#810)
`spec.serving.raftStorageClass` (`ServingSpec.raft_storage_class` in
`crd.rs`) is a free-text Kubernetes StorageClass name. Leaving it unset does
not mean "no StorageClass" — it means "cluster default," and on most managed
Kubernetes offerings **the cluster default is not SSD-backed**. Raft/WAL
write latency is sensitive to disk performance, so a deployer who cares
about that latency should set `raftStorageClass` explicitly rather than
relying on whatever the cluster's default happens to be.

There is no `serving.ssd` boolean toggle and no operator-side
cloud-provider detection — `raftStorageClass` is the sole mechanism, by
design (see Non-goals below). The table below is informational reference
only; verify the actual StorageClass names available on your cluster
(`kubectl get storageclass`) before setting this field, since names and
defaults vary by provider, region, and cluster version.

| Provider | Common default (usually NOT SSD) | Example SSD-backed class(es) |
|----------|-----------------------------------|-------------------------------|
| GKE | `standard-rwo` (pd-balanced) | `premium-rwo`, `pd-ssd` |
| EKS | `gp2` (older clusters) | `gp3` (tune `iops`/`throughput` parameters) |
| AKS | `default`/`managed-csi` (Standard SSD tier) | `managed-csi-premium` |
| Self-hosted / on-prem | varies by CSI driver — no universal default | ask your cluster operator; there is no cross-cluster naming convention |

```yaml
spec:
  serving:
    raftStorageClass: premium-rwo   # example: GKE SSD-backed class
```

### Non-goals: no `serving.ssd` toggle, no provider-detection
A `serving.ssd: true` boolean that maps to a hard-coded per-provider
StorageClass name was considered and explicitly rejected: cloud-provider
SSD class names change and vary across regions/versions, a hard-coded
mapping cannot know a given cluster's actual class names, it would not
cover on-prem/self-hosted Kubernetes at all, and a silently-wrong guess is
worse for a raft/WAL workload than no guess. A second toggle field
competing with the existing free-text `raftStorageClass` would also add
CRD validation ambiguity (which one wins if both are set?) for no real
gain. `raftStorageClass` already lets a deployer set any StorageClass name
they want — the fix here is this guidance, not new API surface.
"#
    .to_string();
    out.push_str("\n## Shared backup primitive\n");
    out.push_str(service_backup::llm::topic().body);
    out.push_str("\n## Shared raft-runtime primitive\n");
    out.push_str(raft_runtime::llm::topic().body);
    out
}
