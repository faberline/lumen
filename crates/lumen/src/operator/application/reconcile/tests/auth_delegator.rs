use std::collections::BTreeMap;
use std::sync::Mutex;

use service_k8s::{ConditionStatus, ManagedService};

use crate::operator::application::reconcile::auth_delegator::{
    apply_auth_delegator_binding, sweep_stale_auth_delegator_bindings, AuthDelegatorControl,
};
use crate::operator::application::reconcile::plan_verdicts::AUTH_DELEGATION_CONTEXT_KEY;
use crate::operator::application::reconcile::tests::hpa::hpa_test_lumen;
use crate::operator::application::reconcile::tests::{condition, ready_facts};
use crate::operator::application::render;

// ---- auth-delegator ClusterRoleBinding (#2876) -------------------------

/// In-memory [`AuthDelegatorControl`]: a `name -> labels` map of the
/// bindings the cluster currently holds, plus switches to make either the
/// apply or the list fail the way a 403 or an apiserver outage would.
#[derive(Default)]
struct FakeAuthDelegatorControl {
    objects: Mutex<BTreeMap<String, BTreeMap<String, String>>>,
    applied: Mutex<Vec<serde_json::Value>>,
    deletes: Mutex<Vec<String>>,
    apply_fails: bool,
    list_fails: bool,
}

impl FakeAuthDelegatorControl {
    fn with(bindings: &[(&str, BTreeMap<String, String>)]) -> Self {
        let control = Self::default();
        for (name, labels) in bindings {
            control
                .objects
                .lock()
                .unwrap()
                .insert((*name).to_string(), labels.clone());
        }
        control
    }
}

#[async_trait::async_trait]
impl AuthDelegatorControl for FakeAuthDelegatorControl {
    async fn apply_binding(&self, binding: &serde_json::Value) -> anyhow::Result<()> {
        if self.apply_fails {
            anyhow::bail!("clusterrolebindings.rbac.authorization.k8s.io is forbidden");
        }
        self.applied.lock().unwrap().push(binding.clone());
        let name = binding["metadata"]["name"].as_str().unwrap().to_string();
        let labels = serde_json::from_value(binding["metadata"]["labels"].clone())?;
        self.objects.lock().unwrap().insert(name, labels);
        Ok(())
    }

    async fn managed_bindings(&self) -> anyhow::Result<Vec<(String, BTreeMap<String, String>)>> {
        if self.list_fails {
            anyhow::bail!("the server was unable to return a response");
        }
        Ok(self
            .objects
            .lock()
            .unwrap()
            .iter()
            .map(|(name, labels)| (name.clone(), labels.clone()))
            .collect())
    }

    async fn delete_binding(&self, name: &str) -> anyhow::Result<()> {
        self.objects.lock().unwrap().remove(name);
        self.deletes.lock().unwrap().push(name.to_string());
        Ok(())
    }
}

#[tokio::test]
async fn apply_auth_delegator_binding_applies_the_instance_binding() {
    let lumen = hpa_test_lumen("search", "acme", 1, 1);
    let control = FakeAuthDelegatorControl::default();

    assert_eq!(apply_auth_delegator_binding(&control, &lumen).await, None);

    let applied = control.applied.lock().unwrap();
    assert_eq!(applied.len(), 1);
    assert_eq!(applied[0]["kind"], "ClusterRoleBinding");
    assert_eq!(applied[0]["roleRef"]["name"], "system:auth-delegator");
    assert_eq!(
        applied[0]["subjects"],
        serde_json::json!([{
            "kind": "ServiceAccount",
            "name": "search",
            "namespace": "acme",
        }]),
        "exactly one subject: this instance's own serving ServiceAccount"
    );
}

/// A second reconcile of an unchanged CR must not produce a second object:
/// the apply is server-side and keyed by a name derived from the CR, so it
/// converges on the same binding (#2876 AC3).
#[tokio::test]
async fn apply_auth_delegator_binding_is_idempotent_across_reconciles() {
    let lumen = hpa_test_lumen("search", "acme", 1, 1);
    let control = FakeAuthDelegatorControl::default();

    apply_auth_delegator_binding(&control, &lumen).await;
    apply_auth_delegator_binding(&control, &lumen).await;

    assert_eq!(
        control.objects.lock().unwrap().len(),
        1,
        "the binding name is a function of the CR, so re-applying overwrites rather than adds"
    );
}

/// AC4: a refused write does not vanish into a log line, and does not fail
/// the reconcile before the status is written either — it comes back as the
/// message the CR will publish.
#[tokio::test]
async fn apply_auth_delegator_binding_reports_a_refused_write() {
    let lumen = hpa_test_lumen("search", "acme", 1, 1);
    let control = FakeAuthDelegatorControl {
        apply_fails: true,
        ..Default::default()
    };

    let error = apply_auth_delegator_binding(&control, &lumen)
        .await
        .expect("a refused apply must produce a message");

    assert!(
        error.contains("ClusterRoleBinding") && error.contains("system:auth-delegator"),
        "the message must name the operation that was refused, got: {error}"
    );
    assert!(
        error.contains(&render::identity::auth_delegator_binding_name(&lumen)),
        "the message must name the object, got: {error}"
    );
}

/// AC4, the other half: that message reaches `status.conditions` as a
/// not-Ready CR. Reporting Ready while the serving pods cannot authenticate
/// anyone is the failure this exists to prevent.
#[test]
fn a_refused_binding_makes_the_cr_not_ready_and_says_why() {
    // Every replica is up: without the refused binding this CR would report
    // `Ready=True`, which is exactly the lie AC4 forbids.
    let lumen = hpa_test_lumen("search", "acme-cond-delegation", 1, 2);
    let context = serde_json::json!({
        AUTH_DELEGATION_CONTEXT_KEY: "apply ClusterRoleBinding lumen.acme-cond-delegation.search.auth-delegator (system:auth-delegator): forbidden",
    });

    let facts = lumen.conditions(&ready_facts("search", 2), &context);

    let ready = condition(&facts, "Ready");
    assert_eq!(ready.status, ConditionStatus::False, "got: {facts:?}");
    assert_eq!(ready.reason, "AuthDelegationNotGranted");
    assert!(
        ready.message.contains("ClusterRoleBinding"),
        "the Ready message must name the refused operation, got: {facts:?}"
    );
    let delegation = condition(&facts, "AuthDelegationReady");
    assert_eq!(delegation.status, ConditionStatus::False);
    assert_eq!(delegation.reason, "ClusterRoleBindingFailed");
}

#[test]
fn a_granted_binding_leaves_readiness_to_the_workload() {
    let lumen = hpa_test_lumen("search", "acme-cond-delegation-ok", 1, 2);

    let facts = lumen.conditions(&ready_facts("search", 2), &serde_json::json!({}));

    let delegation = condition(&facts, "AuthDelegationReady");
    assert_eq!(delegation.status, ConditionStatus::True);
    assert_eq!(delegation.reason, "AuthDelegatorBound");
    assert_eq!(
        condition(&facts, "Ready").status,
        ConditionStatus::True,
        "got: {facts:?}"
    );
}

/// AC3: the binding a deleted CR left behind is the whole reason this loop
/// exists — nothing else can reach it, since a cluster-scoped object cannot
/// name a namespaced owner.
#[tokio::test]
async fn sweep_deletes_a_binding_whose_instance_is_gone() {
    let gone = hpa_test_lumen("retired", "acme", 1, 1);
    let live = hpa_test_lumen("search", "acme", 1, 1);
    let control = FakeAuthDelegatorControl::with(&[
        (
            &render::identity::auth_delegator_binding_name(&gone),
            render::identity::auth_delegator_labels(&gone),
        ),
        (
            &render::identity::auth_delegator_binding_name(&live),
            render::identity::auth_delegator_labels(&live),
        ),
    ]);

    sweep_stale_auth_delegator_bindings(&control, &[live.clone()]).await;

    assert_eq!(
        control.deletes.lock().unwrap().as_slice(),
        &[render::identity::auth_delegator_binding_name(&gone)]
    );
    assert!(control
        .objects
        .lock()
        .unwrap()
        .contains_key(&render::identity::auth_delegator_binding_name(&live)));
}

/// A rename is a delete plus a create as far as the CR is concerned, and
/// the old name is not derivable from the new object — only from the
/// cluster-wide diff this sweep computes.
#[tokio::test]
async fn sweep_deletes_the_binding_left_by_a_renamed_instance() {
    let old = hpa_test_lumen("old-name", "acme", 1, 1);
    let new = hpa_test_lumen("new-name", "acme", 1, 1);
    let control = FakeAuthDelegatorControl::with(&[(
        &render::identity::auth_delegator_binding_name(&old),
        render::identity::auth_delegator_labels(&old),
    )]);

    sweep_stale_auth_delegator_bindings(&control, &[new]).await;

    assert_eq!(
        control.deletes.lock().unwrap().as_slice(),
        &[render::identity::auth_delegator_binding_name(&old)]
    );
}

/// Full label-set equality, not name equality, is what proves authorship —
/// the same guard `prune_stale_hpa` uses, and this one deletes an RBAC
/// object.
#[tokio::test]
async fn sweep_leaves_a_foreign_labeled_binding_at_a_live_name_untouched() {
    let lumen = hpa_test_lumen("search", "acme", 1, 1);
    let mut foreign = render::identity::auth_delegator_labels(&lumen);
    foreign.insert(
        "app.kubernetes.io/managed-by".to_string(),
        "some-other-operator".to_string(),
    );
    let control = FakeAuthDelegatorControl::with(&[(
        &render::identity::auth_delegator_binding_name(&lumen),
        foreign,
    )]);

    sweep_stale_auth_delegator_bindings(&control, &[lumen]).await;

    assert!(control.deletes.lock().unwrap().is_empty());
}

/// An unreadable list is not an empty cluster. Treating the two alike would
/// make one apiserver blip revoke delegated review for every Lumen at once.
#[tokio::test]
async fn sweep_deletes_nothing_when_it_cannot_list() {
    let orphan = hpa_test_lumen("retired", "acme", 1, 1);
    let mut control = FakeAuthDelegatorControl::with(&[(
        &render::identity::auth_delegator_binding_name(&orphan),
        render::identity::auth_delegator_labels(&orphan),
    )]);
    control.list_fails = true;

    sweep_stale_auth_delegator_bindings(&control, &[]).await;

    assert!(control.deletes.lock().unwrap().is_empty());
}
