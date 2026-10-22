//! The Kubernetes-native deployment topology `lumen llm --topic deployment`
//! serves.

/// Kubernetes-native deployment topology (`lumen llm --topic deployment`) as
/// Markdown.
pub fn llm_deployment_md() -> String {
    let mut out = r#"# lumen deployment

## Artifact layers
Use the layered service CLI surface so image, cluster API, operator, and
instance ownership stay separate:

```
lumen dockerfile render --variant release --version lumen@<version> --out Dockerfile
lumen k8s crd render --out lumen-crd.yaml
lumen k8s operator render --namespace lumen-system --out operator/
lumen k8s instance render --profile prod --out lumen.yaml
```

`dockerfile render` is intentionally outside `k8s`: compose, kind, and
registries all consume the same image artifact. `k8s crd render` is the
cluster-scoped API layer, `k8s operator render|run` is the control plane, and
`k8s instance render` is the app-namespace custom resource.

## Externally Provisioned TLS Secrets
Deployment administrators or an external platform provision the serving and peer TLS Secrets named by each Lumen instance. The operator only consumes those Secrets and does not resolve issuers or perform CAS automation.

The deployment administrator or an external platform provisions the named
`servingTlsSecret` and `peerTlsSecret` in the instance namespace. The operator
consumes those Secrets through the existing TLS loaders and never resolves an
issuer, contacts CAS, selects a trust domain, or obtains certificate tokens.

## Serving transport: private ClusterIP TLS
Production traffic is **not** published. A Lumen instance is reached at its
Service DNS name inside the cluster and nowhere else:

```
LUMEN_URL=https://<instance>.<namespace>.svc:7373
```

There is no Ingress, no Gateway, no LoadBalancer, no NodePort, and no service
mesh terminating TLS on lumen's behalf. The serving pod holds the private key
itself, so the connection a caller authenticates is the connection lumen
serves. An edge that terminated TLS and re-originated plaintext would leave the
last hop unauthenticated while every client-side check still passed — and the
KSA token in `Authorization` would cross that hop in the clear.

Set `spec.servingTlsSecret` to a Secret holding `tls.crt`, `tls.key`, and
`ca.crt`. The operator projects it into every serving pod and switches the
client port from h2c to TLS with ALPN `h2, http/1.1`:

```env
LUMEN_TLS=on
LUMEN_TLS_CERT=/var/run/secrets/lumen-serving/tls.crt
LUMEN_TLS_KEY=/var/run/secrets/lumen-serving/tls.key
LUMEN_TLS_CA=/var/run/secrets/lumen-serving/ca.crt
LUMEN_TLS_SERVER_NAMES=<instance>.<namespace>.svc,<instance>.<namespace>.svc.cluster.local
```

The leaf asserts the Service's own two DNS spellings and nothing else. A name
in the certificate is a name this instance can impersonate, so no node name and
no external name belongs there. While no valid leaf is active the port refuses
connections rather than falling back to plaintext.

Callers verify against the anchor alone. The deployment administrator or
external certificate platform distributes the public CA separately from the
private-key-bearing serving Secret. Pass it to `lumen connect --ca-file`, or
as `PrivateTrust` in a generated client; it replaces the public roots rather
than joining them.

`spec.peerTlsSecret` is a separate field for a separate decision — mutual,
instance-scoped Raft identity on `:7374`. Sharing one Secret between the two
would let either listener's material authenticate on the other's port.

Omit `spec.servingTlsSecret` only for local and kind development, where the
client port stays h2c and `spec.auth` is `disabled`.

## Storage topology knobs
The operator-owned storage topology has two independent knobs:

- `spec.shardCount`: the number of physical storage shards that own the corpus.
- `spec.replicasPerShard`: how many pods belong to each shard group.

The serving StatefulSet replica count is always:

```
totalPods = shardCount * replicasPerShard
```

Pod ordinals map deterministically to topology slots:

```
shardIndex = ordinal % shardCount
replicaIndex = ordinal / shardCount
```

That means `shardCount = 3, replicasPerShard = 2` creates six pods: shard 0
has ordinals 0 and 3, shard 1 has ordinals 1 and 4, and shard 2 has ordinals
2 and 5.

## Replica modes
- `replicasPerShard: 1`: one durable member per shard. It uses the local WAL,
  no raft consensus, and is the simplest topology for dev, small prod, or
  sharded-but-not-HA deployments. It is not a primary/follower replication
  mode: there is no background follower catching up from a primary.
- `replicasPerShard: 2`: failover-oriented shape. It adds a second member per
  shard; use it only when the operator/raft policy for the environment is
  intentionally configured for that failover mode.
- `replicasPerShard: 3`: normal raft quorum shape. Set `voterCount: 3` for a
  three-voter shard group.

`voterCount` is per shard group, not cluster-wide. Extra replicas beyond
`voterCount` are learners.

## HPA boundary
HPA is for stateless or near-stateless serving capacity, not for changing
storage ownership. Do not use HPA to change `shardCount` or to add/remove raft
members. Lumen attaches HPA only where the rendered topology can tolerate it;
raft-HA shard groups use a fixed `shardCount * replicasPerShard` peer set.
HPA-created pods in a single-member topology must not be treated as synced data
replicas; production data fan-out is `shardCount`, and production HA is
`replicasPerShard > 1` raft.

## Dynamic shard growth
Shard growth is an operator workflow, not a direct response to request load.
The normal trigger is storage pressure: for example, prepare a split around
50% of the configured shard ceiling, then move virtual buckets in bounded
snapshot batches. The versioned virtual-bucket map decides ownership:

```
bucket = hash(collection_id, routing_key || external_id) % virtualBucketCount
```

Search without a routing key scatters/gathers across shards. Search with a
routing key can target the owning shard. Do not auto-split when the max shard
size or max shard count is unknown; surface the condition to the operator
instead.

## Reshard/convergence observability (#1467)
Beyond the `blockingConditions`/`message` fields on `status.reshard` covered
above, watch these signals during and after a split:

- `lumen_shard_map_version` (gauge) — each serving pod's own live routed
  shard-map version, `0` outside routed deployments. The reshard driver's
  `advance_convergence` scrapes this over every serving pod's `/metrics`
  (the same admin-reachable surface its usage-polling loop already uses) to
  require every pod to actually report the new map version before clearing
  the post-cutover write-pause fence — a rollout that completed but whose
  ConfigMap write has not yet propagated to every pod is not treated as
  converged.
- `lumen_scatter_map_version_mismatches_total` (counter) — count of scatter
  (routing-key-less) search sub-requests where the responding pod's live
  shard-map version disagreed with the scattering pod's own declared
  version. This is an expected, non-fatal signal during a mixed-map rolling
  restart window, not an error: lumen accepts availability over
  completeness for a scatter search mid-flight during a rollout rather than
  fail the whole search. A sustained non-zero rate outside an active reshard
  points at a stuck rollout.
- `awaitingTopologyConvergence` (`status.reshard.blockingConditions`) — the
  post-cutover write-pause fence is armed and waiting for every serving pod
  to confirm the new `shardMap.version`; expected and bounded during a
  cutover, not itself an error.
- `topologyConvergenceStalled` (`status.reshard.blockingConditions`) —
  layered on top of `awaitingTopologyConvergence` once convergence has been
  pending for an extended, bounded number of driver ticks
  (`CONVERGENCE_STALL_TICKS`) without clearing. The write-pause fence is
  never silently dropped when this budget is exceeded — the driver keeps
  re-arming it — so this condition is the operator-visible signal that the
  wait is abnormally long and needs investigation, not proof writes have
  resumed.

## Empty-PVC bootstrap
Existing pods restart from their PVC: local raft state, snapshots, and WAL are
authoritative. A replacement pod with an empty PVC should seed from an exact
`SnapshotV1` object first, then catch up the WAL/raft delta:

```
LUMEN_BOOTSTRAP_SEED_URI=file:///snapshots/shard-0.json
LUMEN_BOOTSTRAP_SEED_URI=s3://bucket/path/shard-0.json
```

Backup is the cold disaster-recovery and seed surface; it is not the normal
live replica synchronization mechanism.
"#
    .to_string();
    out.push_str("\n## Shared raft-runtime topology primitive\n");
    out.push_str(raft_runtime::llm::topic().body);
    out
}
