use std::collections::BTreeMap;
use std::sync::Mutex;

use crate::operator::application::reconcile::hpa::{prune_stale_hpa, HpaControl};
use crate::operator::application::render;
use crate::operator::domain::lumen_spec::Lumen;

// ---- HPA topology-transition handoff (#1385, AC1) ----------------------

/// In-memory [`HpaControl`]: a `(namespace, name) -> labels` map plus a
/// record of every `delete_hpa` call, mirroring
/// `reshard_driver::tests::FakeControl`'s role for that module's
/// `ClusterControl` seam.
#[derive(Default)]
struct FakeHpaControl {
    pub(super) objects: Mutex<BTreeMap<(String, String), BTreeMap<String, String>>>,
    pub(super) deletes: Mutex<Vec<(String, String)>>,
}

impl FakeHpaControl {
    fn with(ns: &str, name: &str, labels: BTreeMap<String, String>) -> Self {
        let control = Self::default();
        control
            .objects
            .lock()
            .unwrap()
            .insert((ns.to_string(), name.to_string()), labels);
        control
    }
}

#[async_trait::async_trait]
impl HpaControl for FakeHpaControl {
    async fn hpa_labels(
        &self,
        namespace: &str,
        name: &str,
    ) -> anyhow::Result<Option<BTreeMap<String, String>>> {
        Ok(self
            .objects
            .lock()
            .unwrap()
            .get(&(namespace.to_string(), name.to_string()))
            .cloned())
    }

    async fn delete_hpa(&self, namespace: &str, name: &str) -> anyhow::Result<()> {
        let key = (namespace.to_string(), name.to_string());
        self.objects.lock().unwrap().remove(&key);
        self.deletes.lock().unwrap().push(key);
        Ok(())
    }
}

fn hpa_test_spec(
    shard_count: u32,
    replicas_per_shard: u32,
) -> crate::operator::domain::lumen_spec::LumenSpec {
    use crate::operator::domain::lumen_spec::{
        serving::ServingSpec, topology::ShardMapSpec, LumenSpec,
    };
    LumenSpec {
        image: "lumen:latest".into(),
        image_pull_policy: None,
        placement: Default::default(),
        shard_count,
        shard_map: ShardMapSpec::default(),
        replicas_per_shard,
        voter_count: replicas_per_shard,
        log_format: Default::default(),
        log_level: None,
        auth: Default::default(),
        serving: ServingSpec::default(),
        reshard_policy: Default::default(),
        observability: false,
        network_policy: false,
        admission: None,
        service_account_name: None,
        service_account_annotations: std::collections::BTreeMap::new(),
        peer_tls_secret: None,
        serving_tls_secret: None,
        body_limit_bytes: None,
    }
}

pub(super) fn hpa_test_lumen(
    name: &str,
    ns: &str,
    shard_count: u32,
    replicas_per_shard: u32,
) -> Lumen {
    let mut lumen = Lumen::new(name, hpa_test_spec(shard_count, replicas_per_shard));
    lumen.metadata.namespace = Some(ns.to_string());
    lumen
}

#[tokio::test]
async fn prune_stale_hpa_deletes_operator_rendered_hpa_on_multi_shard() {
    let lumen = hpa_test_lumen("search", "acme", 3, 1);
    let control = FakeHpaControl::with("acme", "search", render::hpa_labels(&lumen));

    prune_stale_hpa(&control, &lumen).await;

    assert_eq!(
        control.deletes.lock().unwrap().as_slice(),
        &[("acme".to_string(), "search".to_string())]
    );
    assert!(control
        .objects
        .lock()
        .unwrap()
        .get(&("acme".to_string(), "search".to_string()))
        .is_none());
}

#[tokio::test]
async fn prune_stale_hpa_deletes_legacy_hpa_on_single_member() {
    let lumen = hpa_test_lumen("search", "acme", 1, 1);
    let control = FakeHpaControl::with("acme", "search", render::hpa_labels(&lumen));

    prune_stale_hpa(&control, &lumen).await;

    assert_eq!(
        control.deletes.lock().unwrap().as_slice(),
        &[("acme".to_string(), "search".to_string())]
    );
    assert!(control
        .objects
        .lock()
        .unwrap()
        .get(&("acme".to_string(), "search".to_string()))
        .is_none());
}

#[tokio::test]
async fn prune_stale_hpa_leaves_missing_hpa_as_noop() {
    let lumen = hpa_test_lumen("search", "acme", 3, 1);
    let control = FakeHpaControl::default();

    prune_stale_hpa(&control, &lumen).await;

    assert!(control.deletes.lock().unwrap().is_empty());
}

#[tokio::test]
async fn prune_stale_hpa_leaves_unrelated_hpa_name_untouched() {
    let lumen = hpa_test_lumen("search", "acme", 3, 1);
    // A user's own, differently-named HPA lives in the same namespace —
    // the handoff loop only ever looks up the CR's own name, so it must
    // never be inspected or deleted.
    let control = FakeHpaControl::with("acme", "my-other-hpa", render::hpa_labels(&lumen));

    prune_stale_hpa(&control, &lumen).await;

    assert!(control.deletes.lock().unwrap().is_empty());
    assert!(control
        .objects
        .lock()
        .unwrap()
        .contains_key(&("acme".to_string(), "my-other-hpa".to_string())));
}

#[tokio::test]
async fn prune_stale_hpa_leaves_foreign_labeled_hpa_at_same_name_untouched() {
    let lumen = hpa_test_lumen("search", "acme", 3, 1);
    // Same namespace/name as the CR would render, but labels that don't
    // match lumen's stamp (e.g. a different `managed-by`) — R2's scope
    // guard, not just a name check.
    let mut foreign_labels = render::hpa_labels(&lumen);
    foreign_labels.insert(
        "app.kubernetes.io/managed-by".to_string(),
        "some-other-operator".to_string(),
    );
    let control = FakeHpaControl::with("acme", "search", foreign_labels);

    prune_stale_hpa(&control, &lumen).await;

    assert!(control.deletes.lock().unwrap().is_empty());
    assert!(control
        .objects
        .lock()
        .unwrap()
        .contains_key(&("acme".to_string(), "search".to_string())));
}
