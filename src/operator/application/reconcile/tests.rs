use service_k8s::{ConditionFact, ReadyFacts};

// ---- #2601: metav1.Condition convergence surface -----------------------

/// `ReadyFacts` reporting `count` ready pods for `name`'s StatefulSet.
fn ready_facts(name: &str, count: i64) -> ReadyFacts {
    let mut ready = std::collections::HashMap::new();
    ready.insert(name.to_string(), count);
    ReadyFacts { ready }
}

fn condition<'a>(facts: &'a [ConditionFact], type_: &str) -> &'a ConditionFact {
    facts
        .iter()
        .find(|c| c.type_ == type_)
        .unwrap_or_else(|| panic!("expected a `{type_}` condition, got: {facts:?}"))
}

mod auth_delegator;

mod conditions;

mod hpa;

mod peer_identity;

mod shard_usage;

mod status_patch;
