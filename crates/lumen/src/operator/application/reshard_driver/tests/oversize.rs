use std::collections::{BTreeMap, BTreeSet};

use serde_json::json;

use crate::index::infrastructure::snapshot_v1::SnapshotV1;
use crate::operator::application::reshard_driver::oversize::{
    clear_oversize_block, oversize_block_condition, record_oversize_block,
    should_skip_for_oversize, OversizedDocumentBlock, OVERSIZE_RECHECK_TICKS,
};
use crate::operator::application::reshard_driver::phases::advance_catching_up;
use crate::operator::application::reshard_driver::tests::fence::TwoShardFenceControl;
use crate::operator::application::reshard_driver::tests::{http_client, lumen_with, spec};
use crate::operator::application::reshard_driver::transfer::{
    apply_reshard_batch, detect_oversized_batch,
};
use crate::operator::application::reshard_driver::DriveOutcome;
use crate::sharding::domain::reshard_batch::ReshardBatch;

// ---- #1444 R2: oversized-doc reshard remediation --------------------

/// A minimal, otherwise-empty [`ReshardBatch`] whose `external_ids` holds
/// one collection/id pair with an `external_id` long enough on its own to
/// push the batch's serialized size over
/// [`crate::sharding::domain::reshard_batch::ADMIN_ROUTE_BODY_LIMIT_BYTES`] — the exact shape
/// `byte_cap_chunk` produces when it floors at a single oversized id.
fn oversized_batch(collection: &str, external_id_len: usize) -> ReshardBatch {
    let mut external_ids = BTreeMap::new();
    let mut ids = BTreeSet::new();
    ids.insert("x".repeat(external_id_len));
    external_ids.insert(collection.to_string(), ids);
    ReshardBatch {
        from_map_version: 1,
        to_map_version: 2,
        bucket: 0,
        from_shard: 0,
        to_shard: 1,
        external_ids,
        snapshot: SnapshotV1 {
            version: 1,
            collections: BTreeMap::new(),
        },
    }
}

#[test]
fn detect_oversized_batch_none_when_under_limit() {
    let batch = oversized_batch("widgets", 64);
    assert!(
        detect_oversized_batch(&batch).is_none(),
        "a small batch must not be classified as oversized"
    );
}

#[test]
fn detect_oversized_batch_some_when_over_limit_names_first_id() {
    let batch = oversized_batch(
        "widgets",
        crate::sharding::domain::reshard_batch::ADMIN_ROUTE_BODY_LIMIT_BYTES + 1024,
    );
    let block = detect_oversized_batch(&batch)
        .expect("a batch over ADMIN_ROUTE_BODY_LIMIT_BYTES must be classified as oversized");
    assert_eq!(block.collection, "widgets");
    assert_eq!(
        block.external_id.len(),
        crate::sharding::domain::reshard_batch::ADMIN_ROUTE_BODY_LIMIT_BYTES + 1024
    );
    assert!(block.bytes > crate::sharding::domain::reshard_batch::ADMIN_ROUTE_BODY_LIMIT_BYTES);
}

#[tokio::test]
async fn apply_reshard_batch_rejects_oversized_batch_without_sending_request() {
    // #1444 R2 AC2: the pre-flight check in `apply_reshard_batch` must
    // reject an oversized batch itself — no HTTP round trip at all, let
    // alone one that could 413. `.expect(0)` on the mount makes wiremock
    // panic if the driver ever calls out.
    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("POST"))
        .and(wiremock::matchers::path("/admin/reshard:apply"))
        .respond_with(wiremock::ResponseTemplate::new(200))
        .expect(0)
        .mount(&server)
        .await;
    let batch = oversized_batch(
        "widgets",
        crate::sharding::domain::reshard_batch::ADMIN_ROUTE_BODY_LIMIT_BYTES + 1024,
    );
    let result = apply_reshard_batch(&http_client(), &server.uri(), None, &batch).await;
    let err = result.expect_err("an oversized batch must be rejected pre-flight");
    assert!(
        err.downcast_ref::<OversizedDocumentBlock>().is_some(),
        "the error must downcast to OversizedDocumentBlock, got: {err:?}"
    );
}

#[test]
fn oversize_block_cache_records_skips_then_exhausts_recheck_budget() {
    // Each test in this crate shares the process-global oversize cache,
    // so use a namespace/name unique to this test to avoid cross-test
    // interference under parallel execution.
    let namespace = "ac2-cache-ns";
    let name = "ac2-cache-name";
    let uid = "ac2-cache-uid";
    assert!(
        oversize_block_condition(namespace, name, uid).is_none(),
        "no wedge recorded yet"
    );
    assert!(
        should_skip_for_oversize(namespace, name, uid).is_none(),
        "nothing to skip before a wedge is ever recorded"
    );

    let block = OversizedDocumentBlock {
        collection: "widgets".to_string(),
        external_id: "abc".to_string(),
        bytes: crate::sharding::domain::reshard_batch::ADMIN_ROUTE_BODY_LIMIT_BYTES + 1,
    };
    record_oversize_block(namespace, name, uid, block.clone());
    assert_eq!(
        oversize_block_condition(namespace, name, uid),
        Some(block.clone()),
        "the recorded wedge must be readable without affecting the skip budget"
    );

    // The recheck budget is consumed by `should_skip_for_oversize`, not
    // by the read-only `oversize_block_condition` above.
    for _ in 0..OVERSIZE_RECHECK_TICKS {
        assert_eq!(
            should_skip_for_oversize(namespace, name, uid),
            Some(block.clone()),
            "every tick within the recheck budget must skip on the same wedge"
        );
    }
    assert!(
        should_skip_for_oversize(namespace, name, uid).is_none(),
        "once the recheck budget is exhausted, the next tick must be let through"
    );

    clear_oversize_block(namespace, name);
    assert!(
        oversize_block_condition(namespace, name, uid).is_none(),
        "clearing must remove the wedge entirely"
    );
}

#[tokio::test]
async fn advance_catching_up_skips_fence_arm_when_oversize_wedge_recorded() {
    // #1444 R2 AC2: a tick already known-wedged on an oversized document
    // must short-circuit to `Blocked` before arming the write fence —
    // `.expect(0)` on the fence-route mount makes wiremock panic if the
    // driver ever arms it.
    let namespace = "ac2-fence-ns";
    let name = "ac2-fence-name";
    record_oversize_block(
        namespace,
        name,
        "",
        OversizedDocumentBlock {
            collection: "widgets".to_string(),
            external_id: "abc".to_string(),
            bytes: crate::sharding::domain::reshard_batch::ADMIN_ROUTE_BODY_LIMIT_BYTES + 1,
        },
    );

    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("POST"))
        .and(wiremock::matchers::path("/admin/reshard:fence"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(json!({})))
        .expect(0)
        .mount(&server)
        .await;
    let control = TwoShardFenceControl {
        shard_urls: vec![server.uri()],
    };
    let lumen = lumen_with(spec(2, 1, None), None);

    let outcome = advance_catching_up(&control, &http_client(), namespace, name, &lumen).await;
    assert!(
        matches!(outcome, DriveOutcome::Blocked(_)),
        "a known-wedged tick must report Blocked, got: {outcome:?}"
    );

    clear_oversize_block(namespace, name);
}
