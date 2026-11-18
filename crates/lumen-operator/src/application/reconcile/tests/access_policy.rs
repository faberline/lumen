use std::collections::BTreeMap;
use std::sync::Mutex;

use serde_json::{json, Value};

use crate::application::reconcile::access_policy::{
    apply_access_policy, sweep_stale_access_policy, AccessKind, AccessPolicyControl,
    ManagedAccessObject,
};
use crate::application::reconcile::tests::hpa::hpa_test_lumen;
use crate::application::render;
use crate::domain::lumen_spec::access::{AccessSpec, ServiceAccountSubject};
use crate::domain::lumen_spec::Lumen;

// ---- namespaced access Role and RoleBinding (#365) ---------------------

type ObjectKey = (AccessKind, String, String);

/// In-memory [`AccessPolicyControl`]: a `(kind, namespace, name) -> labels`
/// map of the objects the cluster holds, plus switches to make the apply or
/// the list fail the way a 403 or an apiserver outage would.
#[derive(Default)]
struct FakeAccessPolicyControl {
    objects: Mutex<BTreeMap<ObjectKey, BTreeMap<String, String>>>,
    applied: Mutex<Vec<Value>>,
    deletes: Mutex<Vec<ObjectKey>>,
    apply_fails: bool,
    list_fails: bool,
}

impl FakeAccessPolicyControl {
    fn with(objects: Vec<(ObjectKey, BTreeMap<String, String>)>) -> Self {
        let control = Self::default();
        for (key, labels) in objects {
            control.objects.lock().unwrap().insert(key, labels);
        }
        control
    }

    fn applied_of(&self, kind: &str) -> Vec<Value> {
        self.applied
            .lock()
            .unwrap()
            .iter()
            .filter(|v| v["kind"] == kind)
            .cloned()
            .collect()
    }
}

#[async_trait::async_trait]
impl AccessPolicyControl for FakeAccessPolicyControl {
    async fn apply(&self, object: &Value) -> anyhow::Result<()> {
        if self.apply_fails {
            anyhow::bail!("roles.rbac.authorization.k8s.io is forbidden");
        }
        self.applied.lock().unwrap().push(object.clone());
        let kind = match object["kind"].as_str().unwrap() {
            "Role" => AccessKind::Role,
            _ => AccessKind::RoleBinding,
        };
        let namespace = object["metadata"]["namespace"]
            .as_str()
            .unwrap()
            .to_string();
        let name = object["metadata"]["name"].as_str().unwrap().to_string();
        let labels = serde_json::from_value(object["metadata"]["labels"].clone())?;
        self.objects
            .lock()
            .unwrap()
            .insert((kind, namespace, name), labels);
        Ok(())
    }

    async fn get_labels(
        &self,
        kind: AccessKind,
        namespace: &str,
        name: &str,
    ) -> anyhow::Result<Option<BTreeMap<String, String>>> {
        Ok(self
            .objects
            .lock()
            .unwrap()
            .get(&(kind, namespace.to_string(), name.to_string()))
            .cloned())
    }

    async fn managed(&self) -> anyhow::Result<Vec<ManagedAccessObject>> {
        if self.list_fails {
            anyhow::bail!("the server was unable to return a response");
        }
        Ok(self
            .objects
            .lock()
            .unwrap()
            .iter()
            .map(|((kind, namespace, name), labels)| ManagedAccessObject {
                kind: *kind,
                namespace: namespace.clone(),
                name: name.clone(),
                labels: labels.clone(),
            })
            .collect())
    }

    async fn delete(&self, kind: AccessKind, namespace: &str, name: &str) -> anyhow::Result<()> {
        let key = (kind, namespace.to_string(), name.to_string());
        self.objects.lock().unwrap().remove(&key);
        self.deletes.lock().unwrap().push(key);
        Ok(())
    }
}

fn access(subjects: &[(&str, &str)]) -> AccessSpec {
    AccessSpec {
        allowed_service_accounts: subjects
            .iter()
            .map(|(namespace, name)| ServiceAccountSubject {
                namespace: (*namespace).to_string(),
                name: (*name).to_string(),
            })
            .collect(),
    }
}

fn runtime(name: &str, subjects: Option<&[(&str, &str)]>) -> Lumen {
    let mut lumen = hpa_test_lumen(name, "acme", 1, 1);
    lumen.spec.access = subjects.map(access);
    lumen
}

fn key(kind: AccessKind, name: &str) -> ObjectKey {
    (kind, "acme".to_string(), name.to_string())
}

#[test]
fn access_role_grants_use_on_exactly_this_runtime() {
    let lumen = runtime("search", Some(&[("client-ns", "web")]));
    let role = render::access::access_role(&lumen);
    assert_eq!(
        role["rules"],
        json!([{
            "apiGroups": ["lumen.axiom.dev"],
            "resources": ["lumenruntimes"],
            "resourceNames": ["search"],
            "verbs": ["use"]
        }])
    );
    assert_eq!(role["metadata"]["name"], json!("search-access"));
    assert_eq!(role["metadata"]["namespace"], json!("acme"));
    assert_eq!(
        role["metadata"]["labels"],
        json!(render::access::access_policy_labels(&lumen))
    );
    assert_eq!(
        role["metadata"]["labels"]["app.kubernetes.io/component"],
        json!("access-policy")
    );
}

#[test]
fn access_role_binding_names_exactly_the_declared_subjects() {
    let lumen = runtime("search", Some(&[("client-ns", "web"), ("acme", "worker")]));
    let binding = render::access::access_role_binding(&lumen);
    assert_eq!(
        binding["subjects"],
        json!([
            {"kind": "ServiceAccount", "name": "web", "namespace": "client-ns"},
            {"kind": "ServiceAccount", "name": "worker", "namespace": "acme"}
        ])
    );
    assert_eq!(
        binding["roleRef"],
        json!({"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": "search-access"})
    );
}

#[tokio::test]
async fn empty_access_list_renders_deny_all() {
    let lumen = runtime("search", Some(&[]));
    assert_eq!(
        render::access::access_role_binding(&lumen)["subjects"],
        json!([])
    );
    let control = FakeAccessPolicyControl::default();
    assert_eq!(apply_access_policy(&control, &lumen).await, None);
    assert_eq!(control.objects.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn absent_access_applies_nothing() {
    let lumen = runtime("search", None);
    let control = FakeAccessPolicyControl::default();
    assert_eq!(apply_access_policy(&control, &lumen).await, None);
    assert!(control.applied.lock().unwrap().is_empty());
    assert!(control.objects.lock().unwrap().is_empty());
}

#[tokio::test]
async fn apply_converges_and_is_idempotent() {
    let lumen = runtime("search", Some(&[("client-ns", "web")]));
    let control = FakeAccessPolicyControl::default();
    assert_eq!(apply_access_policy(&control, &lumen).await, None);
    assert_eq!(apply_access_policy(&control, &lumen).await, None);
    assert_eq!(control.objects.lock().unwrap().len(), 2);
    assert_eq!(control.applied.lock().unwrap().len(), 4);
}

#[tokio::test]
async fn an_unlabelled_role_is_an_owner_conflict_and_is_not_applied() {
    // Negative control: without the ownership check the Role is overwritten
    // and the message is absent.
    let lumen = runtime("search", Some(&[("client-ns", "web")]));
    let control = FakeAccessPolicyControl::with(vec![(
        key(AccessKind::Role, "search-access"),
        BTreeMap::new(),
    )]);
    let message = apply_access_policy(&control, &lumen)
        .await
        .expect("an owner conflict must be reported");
    assert!(message.contains("Role acme/search-access"), "{message}");
    assert!(message.contains("not managed"), "{message}");
    assert!(control.applied_of("Role").is_empty());
    assert_eq!(
        control.objects.lock().unwrap()[&key(AccessKind::Role, "search-access")],
        BTreeMap::new(),
        "the foreign Role keeps its labels"
    );
    let bindings = control.applied_of("RoleBinding");
    assert_eq!(bindings.len(), 1, "the RoleBinding is still applied");
}

#[tokio::test]
async fn a_foreign_instance_label_is_an_owner_conflict() {
    let lumen = runtime("search", Some(&[("client-ns", "web")]));
    let mut labels = render::access::access_policy_labels(&lumen);
    labels.insert(
        "app.kubernetes.io/instance".to_string(),
        "other".to_string(),
    );
    let control = FakeAccessPolicyControl::with(vec![(
        key(AccessKind::RoleBinding, "search-access"),
        labels,
    )]);
    let message = apply_access_policy(&control, &lumen)
        .await
        .expect("an owner conflict must be reported");
    assert!(
        message.contains("RoleBinding acme/search-access"),
        "{message}"
    );
}

#[tokio::test]
async fn a_failed_apply_is_reported_and_drops_no_subject() {
    // Negative control: a refused write reports and leaves nothing half-done.
    let lumen = runtime("search", Some(&[("client-ns", "web")]));
    let refused = FakeAccessPolicyControl {
        apply_fails: true,
        ..Default::default()
    };
    let message = apply_access_policy(&refused, &lumen)
        .await
        .expect("a refused write must be reported");
    assert!(
        message.contains("apply Role acme/search-access"),
        "{message}"
    );
    assert!(message.contains("forbidden"), "{message}");
    assert!(refused.objects.lock().unwrap().is_empty());

    let accepted = FakeAccessPolicyControl::default();
    assert_eq!(apply_access_policy(&accepted, &lumen).await, None);
    let binding = accepted
        .applied_of("RoleBinding")
        .pop()
        .expect("the RoleBinding is applied once the failure clears");
    assert_eq!(
        binding["subjects"],
        json!([{"kind": "ServiceAccount", "name": "web", "namespace": "client-ns"}])
    );
}

#[tokio::test]
async fn a_removed_subject_disappears_from_the_binding() {
    let control = FakeAccessPolicyControl::default();
    let two = runtime(
        "search",
        Some(&[("client-ns", "web"), ("client-ns", "old")]),
    );
    assert_eq!(apply_access_policy(&control, &two).await, None);
    let one = runtime("search", Some(&[("client-ns", "web")]));
    assert_eq!(apply_access_policy(&control, &one).await, None);
    let last = control
        .applied_of("RoleBinding")
        .pop()
        .expect("a RoleBinding was applied");
    assert_eq!(
        last["subjects"],
        json!([{"kind": "ServiceAccount", "name": "web", "namespace": "client-ns"}])
    );
}

#[tokio::test]
async fn sweep_deletes_managed_objects_whose_lumen_is_gone() {
    let live = runtime("live", Some(&[("client-ns", "web")]));
    let gone = runtime("gone", Some(&[("client-ns", "web")]));
    let control = FakeAccessPolicyControl::with(vec![
        (
            key(AccessKind::Role, "gone-access"),
            render::access::access_policy_labels(&gone),
        ),
        (
            key(AccessKind::RoleBinding, "gone-access"),
            render::access::access_policy_labels(&gone),
        ),
        (
            key(AccessKind::Role, "live-access"),
            render::access::access_policy_labels(&live),
        ),
        (
            key(AccessKind::RoleBinding, "live-access"),
            render::access::access_policy_labels(&live),
        ),
    ]);
    sweep_stale_access_policy(&control, std::slice::from_ref(&live)).await;
    let deletes = control.deletes.lock().unwrap().clone();
    assert_eq!(deletes.len(), 2, "{deletes:?}");
    assert!(deletes.contains(&key(AccessKind::Role, "gone-access")));
    assert!(deletes.contains(&key(AccessKind::RoleBinding, "gone-access")));
    let objects = control.objects.lock().unwrap();
    assert!(objects.contains_key(&key(AccessKind::Role, "live-access")));
    assert!(objects.contains_key(&key(AccessKind::RoleBinding, "live-access")));
}

#[tokio::test]
async fn sweep_deletes_objects_of_a_lumen_whose_access_was_removed() {
    let live = runtime("search", None);
    let control = FakeAccessPolicyControl::with(vec![
        (
            key(AccessKind::Role, "search-access"),
            render::access::access_policy_labels(&live),
        ),
        (
            key(AccessKind::RoleBinding, "search-access"),
            render::access::access_policy_labels(&live),
        ),
    ]);
    sweep_stale_access_policy(&control, std::slice::from_ref(&live)).await;
    assert_eq!(control.deletes.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn sweep_leaves_a_differently_labelled_object_at_a_live_name() {
    let live = runtime("search", Some(&[("client-ns", "web")]));
    let mut labels = render::access::access_policy_labels(&live);
    labels.insert("team".to_string(), "other".to_string());
    let control = FakeAccessPolicyControl::with(vec![(
        key(AccessKind::RoleBinding, "search-access"),
        labels,
    )]);
    sweep_stale_access_policy(&control, std::slice::from_ref(&live)).await;
    assert!(control.deletes.lock().unwrap().is_empty());
}

#[tokio::test]
async fn sweep_deletes_nothing_when_the_list_fails() {
    let live = runtime("search", None);
    let control = FakeAccessPolicyControl {
        list_fails: true,
        ..FakeAccessPolicyControl::with(vec![(
            key(AccessKind::Role, "search-access"),
            render::access::access_policy_labels(&live),
        )])
    };
    sweep_stale_access_policy(&control, std::slice::from_ref(&live)).await;
    assert!(control.deletes.lock().unwrap().is_empty());
}
