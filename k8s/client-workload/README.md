# Client-workload template

This template patches an existing application Deployment so that it can call
Lumen with a Managed KSA identity. It is separate from `k8s/overlays/template`,
which is the Standalone runtime template. The template version is `0.6.2`.

It creates no Namespace, ServiceAccount or Secret. Your application must
already have its own ServiceAccount and runtime CA ConfigMap.

## Steps

1. Copy this directory next to your application's kustomization.
2. Replace every placeholder:
   - `REPLACE_ME__CLIENT_DEPLOYMENT`: the name of your Deployment.
   - `REPLACE_ME__CLIENT_KSA`: the existing ServiceAccount.
   - `REPLACE_ME__RUNTIME_CA_CONFIGMAP`: the public CA ConfigMap of the runtime.
   - `REPLACE_ME__CONTAINER`: the container name in your Deployment.
   - `REPLACE_ME__RUNTIME`: the Lumen runtime Service name.
   - `REPLACE_ME__NAMESPACE`: the namespace of the runtime.
   - `REPLACE_ME__CLIENT_APP`: the label value of your client pods (egress
     component only).
3. Check that no placeholder is left. This command must print nothing:

   ```sh
   grep -rn REPLACE_ME__ <your dir>
   ```

4. Add the optional egress component (`egress/`) only where your cluster
   enforces NetworkPolicy.
5. Apply the result to your cluster and verify it there.

The CA mount has no `subPath`, so an in-place update of the ConfigMap reaches
the container.
