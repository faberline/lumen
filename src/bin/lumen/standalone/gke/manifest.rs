//! The manifests `render` writes: the storage and runtime objects built from
//! the config, each labelled with the instance's identity.

use anyhow::Result;
use serde_json::{json, Value};
use service_k8s::render::common::{network_policy, NetworkPolicy, ServicePodTemplate};
use service_k8s::render::stateful_instance::{
    stateful_instance, ExistingClaim, StatefulInstancePlan, StatefulStorageAttachment,
};
use service_k8s::render::{
    client_service, requested_resources, restricted_container_security_context,
    restricted_pod_security_context, RenderCtx,
};

use crate::standalone::gke::{Config, IMAGE};

fn meta(c: &Config, name: &str, component: &str) -> Value {
    json!({"name":name,"namespace":c.namespace,"labels":{"app.kubernetes.io/name":"lumen","app.kubernetes.io/instance":c.name,"app.kubernetes.io/component":component,"app.kubernetes.io/managed-by":"lumen-standalone","lumen.axiom.dev/instance":c.name,"lumen.axiom.dev/profile":"gke","lumen.axiom.dev/storage":format!("{}-data",c.name)}})
}
fn attach_identity(value: &mut Value, c: &Config) {
    value["metadata"]["labels"]["lumen.axiom.dev/instance"] = json!(c.name);
    value["metadata"]["labels"]["lumen.axiom.dev/profile"] = json!("gke");
    value["metadata"]["labels"]["lumen.axiom.dev/storage"] = json!(format!("{}-data", c.name));
    value["metadata"]["annotations"]["lumen.axiom.dev/instance-identity"] =
        json!(format!("{}/{}", c.namespace, c.name));
}
pub(super) fn build(c: &Config) -> Result<(Vec<(String, Value)>, Vec<(String, Value)>)> {
    let cx = RenderCtx {
        app: "lumen",
        manager: "lumen-standalone",
        api_version: "v1",
        kind: "Standalone",
        name: &c.name,
        ns: &c.namespace,
        owner: None,
    };
    let labels = cx
        .selector("serving")
        .as_object()
        .unwrap()
        .iter()
        .map(|(k, v)| (k.clone(), v.as_str().unwrap().into()))
        .collect();
    let pvc_template = json!({"spec":{"accessModes":["ReadWriteOnce"],"resources":{"requests":{"storage":c.storage_size}},"storageClassName":c.storage_class}});
    let claim = ExistingClaim::new(
        "data",
        format!("{}-data", c.name),
        pvc_template,
        "/var/lib/lumen/data",
    );
    let pod = ServicePodTemplate {
        cx: &cx,
        component: "serving",
        image: IMAGE,
        image_pull_policy: "IfNotPresent",
        command: vec!["lumen".into(), "serve".into()],
        args: vec![],
        ports: vec![json!({"name":"http","containerPort":7373,"protocol":"TCP"})],
        env: vec![
            json!({"name":"LUMEN_AUTH","value":"in-cluster"}),
            json!({"name":"LUMEN_AUTH_NAMESPACE","value":c.namespace}),
        ],
        env_from: vec![],
        resources: requested_resources(&c.cpu, &c.memory),
        readiness_probe: Some(json!({
            "httpGet":{"path":"/readyz","port":"http","scheme":"HTTP"},
            "initialDelaySeconds":5,"periodSeconds":10,"timeoutSeconds":3,"failureThreshold":60,
        })),
        liveness_probe: Some(json!({
            "httpGet":{"path":"/healthz","port":"http","scheme":"HTTP"},
            "initialDelaySeconds":15,"periodSeconds":30,"timeoutSeconds":5,"failureThreshold":3,
        })),
        startup_probe: Some(json!({
            "httpGet":{"path":"/healthz","port":"http","scheme":"HTTP"},
            "periodSeconds":5,"timeoutSeconds":3,"failureThreshold":120,
        })),
        lifecycle: None,
        container_security_context: Some(restricted_container_security_context()),
        pod_security_context: Some(restricted_pod_security_context()),
        service_account_name: Some(&c.name),
        termination_grace_period_seconds: Some(30),
        volumes: vec![json!({"name":"tmp","emptyDir":{}})],
        volume_mounts: vec![json!({"name":"tmp","mountPath":"/tmp"})],
        pod_annotations: Some(
            json!({"prometheus.io/scrape":"true","prometheus.io/port":"7373","prometheus.io/path":"/metrics"}),
        ),
        topology_spread_constraints: vec![],
    };
    let mut plan = StatefulInstancePlan::new(
        &cx,
        c.name.clone(),
        1,
        pod,
        StatefulStorageAttachment::ExistingClaim(claim),
    );
    plan.labels = labels;
    plan.labels
        .insert("lumen.axiom.dev/instance".into(), c.name.clone());
    plan.labels
        .insert("lumen.axiom.dev/profile".into(), "gke".into());
    plan.labels
        .insert("lumen.axiom.dev/storage".into(), format!("{}-data", c.name));
    plan.node_selector = Some(json!({"cloud.google.com/gke-nodepool":c.node_pool}));
    // A Service named `lumen` would otherwise inject `LUMEN_PORT=tcp://...`,
    // which collides with Lumen's numeric `LUMEN_PORT` serving option.
    plan.enable_service_links = Some(false);
    let mut rendered = stateful_instance(plan).map_err(|e| anyhow::anyhow!(e.to_string()))?;
    attach_identity(&mut rendered.workload, c);
    let mut pvc = rendered
        .storage
        .take()
        .expect("existing claim renders a PVC");
    attach_identity(&mut pvc, c);
    let mut service = client_service(&cx, &c.name, "serving", 7373);
    attach_identity(&mut service, c);
    let storage = vec![
        (
            "namespace.yaml".into(),
            json!({"apiVersion":"v1","kind":"Namespace","metadata":{"name":c.namespace,"labels":{"pod-security.kubernetes.io/enforce":"restricted","pod-security.kubernetes.io/audit":"restricted","pod-security.kubernetes.io/warn":"restricted"}}}),
        ),
        ("pvc.yaml".into(), pvc),
        (
            "kustomization.yaml".into(),
            json!({"apiVersion":"kustomize.config.k8s.io/v1beta1","kind":"Kustomization","resources":["namespace.yaml","pvc.yaml"]}),
        ),
    ];
    let mut runtime = vec![
        ("statefulset.yaml".into(), rendered.workload),
        ("service.yaml".into(), service),
        (
            "serviceaccount.yaml".into(),
            json!({"apiVersion":"v1","kind":"ServiceAccount","automountServiceAccountToken":true,"metadata":meta(c,&c.name,"serving")}),
        ),
        (
            "admin-serviceaccount.yaml".into(),
            json!({"apiVersion":"v1","kind":"ServiceAccount","automountServiceAccountToken":true,"metadata":meta(c,&format!("{}-admin",c.name),"admin")}),
        ),
        (
            "clusterrolebinding.yaml".into(),
            json!({"apiVersion":"rbac.authorization.k8s.io/v1","kind":"ClusterRoleBinding","metadata":{"name":format!("lumen.{}.{}.auth-delegator",c.namespace,c.name),"labels":{"app.kubernetes.io/name":"lumen","app.kubernetes.io/instance":c.name,"app.kubernetes.io/component":"auth-delegation","app.kubernetes.io/managed-by":"lumen-standalone","lumen.axiom.dev/owner-namespace":c.namespace,"lumen.axiom.dev/profile":"gke"}},"roleRef":{"apiGroup":"rbac.authorization.k8s.io","kind":"ClusterRole","name":"system:auth-delegator"},"subjects":[{"kind":"ServiceAccount","name":c.name,"namespace":c.namespace}]}),
        ),
    ];
    let resources = json!(["lumencollections"]);
    runtime.push(("client-role.yaml".into(),json!({"apiVersion":"rbac.authorization.k8s.io/v1","kind":"Role","metadata":meta(c,&format!("{}-client",c.name),"rbac"),"rules":[{"apiGroups":["lumen.axiom.dev"],"resources":resources,"verbs":["get","update","delete"]}]})));
    runtime.push(("admin-role.yaml".into(),json!({"apiVersion":"rbac.authorization.k8s.io/v1","kind":"Role","metadata":meta(c,&format!("{}-admin",c.name),"rbac"),"rules":[{"apiGroups":["lumen.axiom.dev"],"resources":["lumencollections","lumenadmin"],"verbs":["get","update","delete"]}]})));
    let mut accounts = c.allowed_service_accounts.clone();
    accounts.sort();
    for (index, x) in accounts.iter().enumerate() {
        let p: Vec<_> = x.split('/').collect();
        runtime.push((format!("client-rolebinding-{index:03}.yaml"),json!({"apiVersion":"rbac.authorization.k8s.io/v1","kind":"RoleBinding","metadata":meta(c,&format!("{}-client-{index:03}",c.name),"rbac"),"roleRef":{"apiGroup":"rbac.authorization.k8s.io","kind":"Role","name":format!("{}-client",c.name)},"subjects":[{"kind":"ServiceAccount","name":p[1],"namespace":p[0]}]})));
    }
    runtime.push(("admin-rolebinding.yaml".into(),json!({"apiVersion":"rbac.authorization.k8s.io/v1","kind":"RoleBinding","metadata":meta(c,&format!("{}-admin",c.name),"rbac"),"roleRef":{"apiGroup":"rbac.authorization.k8s.io","kind":"Role","name":format!("{}-admin",c.name)},"subjects":[{"kind":"ServiceAccount","name":format!("{}-admin",c.name),"namespace":c.namespace}]})));
    runtime.push((
        "networkpolicy.yaml".into(),
        network_policy(NetworkPolicy {
            cx: &cx,
            name: &c.name,
            component: "serving",
            client_ports: vec![7373],
            peer_ports: vec![],
            extra_egress: vec![],
        }),
    ));
    let mut names: Vec<_> = runtime.iter().map(|x| x.0.clone()).collect();
    names.sort();
    runtime.push(("kustomization.yaml".into(),json!({"apiVersion":"kustomize.config.k8s.io/v1beta1","kind":"Kustomization","resources":names})));
    Ok((storage, runtime))
}
