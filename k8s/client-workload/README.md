# Client workload template

A copy-to-customize Kustomize component. It patches an existing application
Deployment to use the Managed identity contract: an existing ServiceAccount,
no default token mount, a projected token for audience `lumen.axiom.dev`, and
the runtime CA mounted without `subPath`.

This template is separate from `k8s/overlays/template`, which is the Standalone
runtime template.

The component creates no Namespace, ServiceAccount or Secret.

## Customize

Replace every placeholder before you apply:

| Placeholder | Replace with |
|---|---|
| `REPLACE_ME__CLIENT_KSA` | the existing ServiceAccount of your workload |
| `REPLACE_ME__RUNTIME_CA_CONFIGMAP` | the ConfigMap that holds the runtime CA |
| `REPLACE_ME__CONTAINER` | the container name in your Deployment |
| `REPLACE_ME__RUNTIME` | the Lumen runtime Service name |
| `REPLACE_ME__NAMESPACE` | the namespace of the Lumen runtime |
| `REPLACE_ME__CLIENT_APP` | the `app` label of your pods (egress component only) |

Check that no placeholder remains before apply:

```sh
grep -rn REPLACE_ME__ <your dir>
```

The command must print nothing.

## Optional egress policy

`egress/` is an optional component. It adds a NetworkPolicy that allows egress
only to DNS and to Lumen pods on TCP 7373. Add it to your kustomization only
when your cluster's CNI enforces NetworkPolicy.

## Render check

`example/` renders the component with a stub Deployment:

```sh
kustomize build k8s/client-workload/example
```

The output contains exactly a `Deployment` and a `NetworkPolicy`.
