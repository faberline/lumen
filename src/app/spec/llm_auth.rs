//! The request-authentication contract `lumen llm --topic auth` serves.

/// Request-authentication contract (`lumen llm --topic auth`) as Markdown.
pub fn llm_auth_md() -> String {
    let mut out = String::from(
        r#"# lumen auth

## Runtime contract
Kubernetes deployments use a private ClusterIP and a short-lived Kubernetes
ServiceAccount token in `Authorization: Bearer`. The cluster answers both the
identity and authorization questions.

Managed keeps two independent checks. Its serving port uses private TLS, and
its request token has the private `lumen.axiom.dev` audience. Standalone
in-cluster is the simple one-Pod profile. It uses cleartext only inside the
cluster network and accepts the mounted token for the calling Pod's configured
KSA. The server checks that KSA against `allowedServiceAccounts`.

Server modes:

```env
LUMEN_AUTH=required        # Managed private-audience token
LUMEN_AUTH=in-cluster      # Standalone mounted configured-KSA token
LUMEN_AUTH=off             # Compose and local development
```

`required` and `in-cluster` prove both delegation grants at startup. The
process must reach kube-apiserver. Its ServiceAccount must be allowed to create
`TokenReview` and `SubjectAccessReview`. If either check fails, the process
does not start. The error names the missing `system:auth-delegator` binding.

There is no degradation. An authenticated process that loses its verifier
rejects requests. It never falls back to open. `disabled` remains an alias for
`off`.

Probe/spec/scrape routes stay auth-exempt regardless: `/healthz`, `/readyz`,
`/metrics`, `/openapi.json`, and `/docs`.

## Standalone in-cluster transport
Standalone GKE uses one cleartext ClusterIP Service:

```env
LUMEN_URL=http://lumen.lumen.svc.cluster.local:7373
```

The renderer also creates a NetworkPolicy. It does not create an Ingress,
Gateway, LoadBalancer, or NodePort. Compose stays on
`http://127.0.0.1:7373` with `LUMEN_AUTH=off`.

## Managed transport: private ClusterIP TLS, terminated by lumen
Managed traffic is **not** published. There is no Ingress, no Gateway, no
LoadBalancer, no NodePort, and no service mesh terminating TLS on lumen's
behalf. The listener holds the private key itself, so the connection a caller
authenticates is the connection lumen serves — an edge that terminates TLS and
re-originates plaintext would leave the last hop unauthenticated while the
client's own check still passed.

```env
LUMEN_URL=https://<instance>.<namespace>.svc:7373
```

Set `spec.servingTlsSecret` on the `Lumen` CR and the operator projects the
leaf, key, and CA into the pods and turns the listener on:

```env
LUMEN_TLS=on
LUMEN_TLS_CERT=/var/run/secrets/lumen-serving/tls.crt
LUMEN_TLS_KEY=/var/run/secrets/lumen-serving/tls.key
LUMEN_TLS_CA=/var/run/secrets/lumen-serving/ca.crt
LUMEN_TLS_SERVER_NAMES=<instance>.<namespace>.svc,<instance>.<namespace>.svc.cluster.local
```

The names in `LUMEN_TLS_SERVER_NAMES` are the Service's own two spellings and
nothing else. No node name, no external name: a name in the leaf is a name this
instance can impersonate.

Callers verify against the public CA distributed separately by the deployment
administrator or external certificate platform. Pass that PEM file with
`lumen connect --ca-file`; it replaces the public roots and is never read from
the private-key-bearing serving Secret.

Peer traffic on `:7374` is a separate trust decision with its own material
(`spec.peerTlsSecret`) — mutual, instance-scoped, and never interchangeable
with the serving anchor.

## Kubernetes: who a caller is, is the cluster's answer
The CRD configures **no** credential source. `spec.tokensSecret`,
`spec.identities` and `spec.identityAudiences` are gone (#2872): a Lumen CR
that sets any of them is rejected by the API server's strict decoding rather
than applied and quietly ignored.

Every request under `auth: required` or `auth: in-cluster` carries a short-lived
ServiceAccount token. Lumen answers two questions with the cluster rather than
with a file it was handed:

- **Who is this?** Managed `required` sends `lumen.axiom.dev` as the requested
  TokenReview audience. Standalone `in-cluster` omits the requested audience,
  so kube-apiserver checks its configured default audiences. The principal
  must be exactly
  `system:serviceaccount:<namespace>:<name>`; nothing else is accepted as an
  identity.
- **May they do this?** `SubjectAccessReview` against the virtual resources
  `lumencollections` and `lumenadmin` in API group `lumen.axiom.dev`, scoped to
  the serving instance's own namespace. RBAC in the cluster is the
  authorization source of truth, so a grant is a Role a reviewer can read,
  `kubectl auth can-i` can answer, and the audit log records.

| Action | Resource | Name | Verb |
|--------|----------|------|------|
| read a collection | `lumencollections` | collection id | `get` |
| write a collection | `lumencollections` | collection id | `update` |
| administer a collection | `lumencollections` | collection id | `delete` |
| instance admin | `lumenadmin` | — | per role |

There is no role precedence. `delete` does not imply `get` unless the
RoleBinding says so.

Nothing is a long-lived secret: tokens are minted per use and expire, so there
is no registry to rotate, no Secret to sync from Secret Manager, and no CSI
mount whose refresh behaviour has to be reasoned about. Google user and GSA
credentials authenticate to the kube-apiserver only — lumen never sees one, and
never verifies a Google-issued token itself.

## The CLI: kubeconfig authenticates you to Kubernetes, not to Lumen
Two boundaries, two credentials, and they are never the same string.

- **To the cluster:** your kubeconfig — a Google user, a GSA, or the GKE
  credential plugin. This is what `kubectl` and `lumen connect` already use.
- **To Lumen:** a short-lived, audience-bound ServiceAccount token that the
  cluster mints. Never a Google access token, a Google ID token, an ADC
  credential, a GSA key, or a metadata-server token. Those authenticate to
  kube-apiserver and stop there; sent to Lumen they are rejected, and submitted
  to `TokenReview` a Google ID token comes back `{"user": {}, "error":
  "invalid bearer token"}`.

The bridge between the two is the TokenRequest API: your kubeconfig identity is
RBAC-authorized to request a token for one explicitly named client
ServiceAccount, and that token — not your own credential — is what reaches
Lumen.

`lumen connect` does exactly that (#2878): it mints the token through your
kubeconfig, holds it in memory, and attaches it on a loopback proxy so the
child process is handed `LUMEN_URL` and never the token itself. There is no
token flag, no token environment variable, and no Kubernetes Secret standing
behind either.

The port-forward it opens is transport only. The upstream URL keeps the
Service's real DNS name, so SNI, hostname verification, and `:authority` all
target the name the certificate asserts while only address resolution points at
the forwarded loopback socket — `--ca-file` supplies the anchor:

```sh
lumen connect --namespace search --cr lumen \
  --client-sa agent --ca-file ./ca.crt -- ./my-app
```

Verification is never the thing to switch off. There is no insecure flag: a
server that cannot be verified is a wrong anchor or a wrong name, and both are
fixable.

## Generated clients
`lumen spec gen --lang rust|py|ts` opts the generated source into the mounted
token for the calling Pod's configured KSA, at
`/var/run/secrets/kubernetes.io/serviceaccount/token`. The client rereads it
before each request to an exact non-empty `*.svc.cluster.local` HTTP or HTTPS
host. An explicit Authorization header wins and prevents a file read. Local,
Docker, IP, and external URLs do not read or send the token. An eligible
browser request fails before transport.

Managed callers keep the private-audience contract. They supply the
audience-bound Authorization value explicitly and configure the CA bundle and
the name that the server must assert. Private trust replaces the public roots:

```rust
Client::with_private_ca(
    "https://lumen.search.svc:7373",
    TransportPolicy::default(),
    PrivateTrust { ca_bundle: "/etc/lumen/ca.crt".into(), server_name: "lumen.search.svc".into() },
)?
```

```python
Client("https://lumen.search.svc:7373",
       trust=PrivateTrust(ca_bundle="/etc/lumen/ca.crt", server_name="lumen.search.svc"),
       auth_token=token)
```

```ts
const trust = { caBundle: "/etc/lumen/ca.crt", serverName: "lumen.search.svc" };
const config = { baseUrl: "https://lumen.search.svc:7373",
                 trust, fetch: await privateCaFetch(trust) };
```

Construction fails if `server_name` is not the host the base URL addresses: a
client that verified one name while addressing another would be checking a
certificate it never actually relies on. No generated client offers a way to
skip verification.

For Managed, pass `auth_token="<token>"` or
`default_headers={"Authorization": "Bearer <token>"}` on `Client` and
`AsyncClient`. This explicit value takes precedence over the Standalone default
token provider.
"#,
    );
    out.push_str("\n## Shared auth primitive\n");
    out.push_str(service_auth::llm::topic().body);
    out
}
