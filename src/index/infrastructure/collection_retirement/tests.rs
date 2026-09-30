use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::sync::{mpsc, Arc, Mutex};

use crate::index::application::engine::tests::{build_users_schema, item};
use crate::index::application::engine::Engine;
use crate::index::domain::collection::Collection;
use crate::index::domain::field_index::delta::DROP_EID_CALLS;
use crate::index::infrastructure::collection_retirement::{
    collection_retirement_worker_count, finish_retirement_task, receive_retirement_task,
    retire_collection_with, run_retire_task, send_retirement_task, try_receive_retirement_task,
    CollectionRetirementWorker, RetireTask, RetireTaskProgress, RetiredGeneration,
    MAX_COLLECTION_RETIREMENT_WORKERS, RETIRED_DOCUMENTS_PER_TASK, RETIREMENT_FAILSAFE_RETAINS,
};
use crate::shared_kernel::types::document::{FieldValue, IndexRequest};

#[test]
fn unavailable_retirement_worker_fails_safe_without_changing_apply_semantics() {
    let before = RETIREMENT_FAILSAFE_RETAINS.load(Ordering::Relaxed);
    let detached = Collection::new(BTreeMap::new()).unwrap();
    retire_collection_with(&CollectionRetirementWorker::Unavailable, detached);
    assert_eq!(
        RETIREMENT_FAILSAFE_RETAINS.load(Ordering::Relaxed),
        before + 1,
        "a failed worker handoff must retain the old generation instead of dropping it on the apply thread"
    );
}

#[test]
fn retired_generation_splits_document_reclaim_into_fixed_size_tasks() {
    let e = Engine::new();
    e.create_collection("users", build_users_schema()).unwrap();
    let documents = RETIRED_DOCUMENTS_PER_TASK * 2 + 1;
    let mut items = Vec::with_capacity(documents);
    for document in 0..documents {
        items.push(item(
            &format!("u{document}"),
            "email",
            FieldValue::String(format!("u{document}@example.com")),
        ));
    }
    e.index(
        "users",
        IndexRequest {
            items,
            request_id: None,
        },
    )
    .unwrap();
    assert_eq!(
        e.stats("users").unwrap().documents_indexed,
        documents as u64
    );

    let detached = e
        .state
        .write()
        .unwrap()
        .collections
        .remove("users")
        .expect("test collection exists");
    DROP_EID_CALLS.with(|calls| calls.set(0));
    let generation = RetiredGeneration::new(0, detached);
    let mut task_sizes = Vec::new();
    let final_collection = loop {
        match generation.drain_document_task() {
            RetireTaskProgress::More { retired_documents } => {
                task_sizes.push(retired_documents);
            }
            RetireTaskProgress::Complete {
                retired_documents,
                collection,
            } => {
                task_sizes.push(retired_documents);
                break collection;
            }
        }
    };

    assert_eq!(
        task_sizes,
        vec![RETIRED_DOCUMENTS_PER_TASK, RETIRED_DOCUMENTS_PER_TASK, 1],
        "each token owns one bounded document slice"
    );
    DROP_EID_CALLS.with(|calls| {
        assert_eq!(
            calls.get(),
            documents as u64,
            "the detached worker removed each indexed field exactly once"
        );
    });
    assert!(final_collection.interner.to_eid.is_empty());
    assert!(final_collection.interner.to_hash.is_empty());
    assert!(final_collection.eid_fields.is_empty());
    assert!(final_collection.cell_versions.is_empty());
    assert!(final_collection.doc_versions.is_empty());
    assert!(final_collection.field_checksums.is_empty());
    assert_eq!(final_collection.fields["email"].bytes(), 0);
    drop(final_collection);
}

#[test]
fn retired_generation_requeues_exactly_one_token_until_completion() {
    let e = Engine::new();
    e.create_collection("users", build_users_schema()).unwrap();
    let documents = RETIRED_DOCUMENTS_PER_TASK + 1;
    let items = (0..documents)
        .map(|document| {
            item(
                &format!("u{document}"),
                "email",
                FieldValue::String(format!("u{document}@example.com")),
            )
        })
        .collect();
    e.index(
        "users",
        IndexRequest {
            items,
            request_id: None,
        },
    )
    .unwrap();
    let detached = e
        .state
        .write()
        .unwrap()
        .collections
        .remove("users")
        .expect("test collection exists");
    let generation = Arc::new(RetiredGeneration::new(0, detached));
    let (sender, receiver) = mpsc::channel();

    run_retire_task(
        RetireTask {
            generation: Arc::clone(&generation),
        },
        &sender,
    );
    let completion = try_receive_retirement_task(&receiver)
        .expect("one incomplete generation must requeue one token");
    assert!(
        receiver.try_recv().is_err(),
        "one generation may not enqueue multiple concurrent tokens"
    );

    run_retire_task(completion, &sender);
    finish_retirement_task();
    assert!(
        receiver.try_recv().is_err(),
        "the final token must acknowledge completion without requeueing"
    );
    assert!(
        generation.collection.lock().unwrap().is_none(),
        "completion consumes the detached collection exactly once"
    );
}

#[test]
fn shared_retirement_receiver_assigns_distinct_generation_tokens_to_workers() {
    let (sender, receiver) = mpsc::channel();
    let receiver = Arc::new(Mutex::new(receiver));
    let first = Arc::new(RetiredGeneration::new(
        41,
        Collection::new(BTreeMap::new()).unwrap(),
    ));
    let second = Arc::new(RetiredGeneration::new(
        42,
        Collection::new(BTreeMap::new()).unwrap(),
    ));
    send_retirement_task(
        &sender,
        RetireTask {
            generation: Arc::clone(&first),
        },
    )
    .unwrap();
    send_retirement_task(
        &sender,
        RetireTask {
            generation: Arc::clone(&second),
        },
    )
    .unwrap();
    drop(sender);

    let barrier = Arc::new(std::sync::Barrier::new(3));
    let (observed_sender, observed_receiver) = mpsc::channel();
    std::thread::scope(|scope| {
        for _ in 0..2 {
            let receiver = Arc::clone(&receiver);
            let barrier = Arc::clone(&barrier);
            let observed_sender = observed_sender.clone();
            scope.spawn(move || {
                barrier.wait();
                let task = receive_retirement_task(receiver.as_ref())
                    .expect("each worker receives one pre-enqueued token");
                observed_sender.send(task.generation.id).unwrap();
                finish_retirement_task();
            });
        }
        barrier.wait();
    });

    let mut observed = vec![
        observed_receiver.recv().unwrap(),
        observed_receiver.recv().unwrap(),
    ];
    observed.sort_unstable();
    assert_eq!(observed, vec![41, 42]);
    assert!(
        receiver.lock().unwrap().try_recv().is_err(),
        "the two workers consumed both tokens exactly once"
    );
}

#[test]
fn collection_reclaimer_worker_count_defaults_and_caps() {
    assert_eq!(collection_retirement_worker_count(None), 1);
    assert_eq!(collection_retirement_worker_count(Some("0")), 1);
    assert_eq!(collection_retirement_worker_count(Some("invalid")), 1);
    assert_eq!(collection_retirement_worker_count(Some("2")), 2);
    assert_eq!(
        collection_retirement_worker_count(Some("99")),
        MAX_COLLECTION_RETIREMENT_WORKERS
    );
}
