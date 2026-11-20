use service_k8s::{ConditionFact, ReadyFacts};

// ---- #2601: metav1.Condition convergence surface -----------------------

/// `ReadyFacts` reporting `count` ready pods for `name`'s StatefulSet.
fn ready_facts(name: &str, count: i64) -> ReadyFacts {
    let mut ready = std::collections::HashMap::new();
    ready.insert(name.to_string(), count);
    ReadyFacts::new(ready)
}

/// `ReadyFacts` for the per-shard sets of `name`: the set `<name>-s<k>` reports `counts[k]` ready pods.
fn sharded_ready_facts(name: &str, counts: &[i64]) -> ReadyFacts {
    let mut ready = std::collections::HashMap::new();
    for (k, count) in counts.iter().enumerate() {
        ready.insert(format!("{name}-s{k}"), *count);
    }
    ReadyFacts::new(ready)
}

fn condition<'a>(facts: &'a [ConditionFact], type_: &str) -> &'a ConditionFact {
    facts
        .iter()
        .find(|c| c.type_ == type_)
        .unwrap_or_else(|| panic!("expected a `{type_}` condition, got: {facts:?}"))
}

mod access_policy;

mod auth_delegator;

mod conditions;

mod google_credential;

mod handover;

mod hpa;

mod peer_identity;
mod readiness_targets;

mod shard_usage;

mod status_patch;
