//! Pure gate: the next write of a fleet pass waits for the previous child.
use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChildProgress {
    pub generation: i64,
    pub observed_generation: i64,
    pub ready: bool,
}

impl ChildProgress {
    /// A child that this pass has just created or applied: generation 1, observed 0, not Ready.
    pub fn just_written() -> Self {
        Self {
            generation: 1,
            observed_generation: 0,
            ready: false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GateDecision {
    Proceed,
    Hold(String),
}

/// `None` (no earlier child) proceeds. Rules, in this order:
/// `generation > observed_generation` -> `Hold(format!("stale generation (observed {observed_generation} of {generation})"))`;
/// else `!ready` -> `Hold("not Ready".to_string())`; else `Proceed`.
pub fn gate(previous: Option<&ChildProgress>) -> GateDecision {
    let Some(p) = previous else {
        return GateDecision::Proceed;
    };
    if p.generation > p.observed_generation {
        return GateDecision::Hold(format!(
            "stale generation (observed {} of {})",
            p.observed_generation, p.generation
        ));
    }
    if !p.ready {
        return GateDecision::Hold("not Ready".to_string());
    }
    GateDecision::Proceed
}

/// `lumen` is the whole live `Lumen` as JSON. `generation` = `metadata.generation`,
/// `observed_generation` = `status.observedGeneration`; each is 0 when missing or not an integer.
/// `ready` is true only when `status.conditions` has an object whose `type` is `Ready` and whose
/// `status` is `True` (copy the check from `live_child_from_json` in `domain/lumen_fleet/aggregate.rs`).
pub fn child_progress(lumen: &Value) -> ChildProgress {
    let int = |value: Option<&Value>| value.and_then(Value::as_i64).unwrap_or(0);
    let generation = int(lumen.get("metadata").and_then(|m| m.get("generation")));
    let observed_generation = int(lumen
        .get("status")
        .and_then(|s| s.get("observedGeneration")));
    let ready = lumen
        .get("status")
        .and_then(|s| s.get("conditions"))
        .and_then(Value::as_array)
        .is_some_and(|conditions| {
            conditions.iter().any(|c| {
                c.get("type").and_then(Value::as_str) == Some("Ready")
                    && c.get("status").and_then(Value::as_str) == Some("True")
            })
        });
    ChildProgress {
        generation,
        observed_generation,
        ready,
    }
}

/// Per-pass state. `converge` makes a new one for every pass; nothing is stored between passes.
#[derive(Debug, Default)]
pub struct RolloutGate {
    held: Option<String>,
    previous: Option<(String, ChildProgress)>,
}

impl RolloutGate {
    pub fn new() -> Self {
        Self::default()
    }

    /// `changing == false` always returns `Proceed` and never sets the hold.
    /// `changing == true`: when `held` is `Some(message)`, return `Hold(message.clone())`.
    /// Otherwise run `gate(previous progress)`. On `Hold(reason)` store and return
    /// `Hold(format!("waiting for {id}: {reason}"))`, where `id` is the id of the previous child.
    /// With no previous child, return `Proceed`.
    pub fn check(&mut self, changing: bool) -> GateDecision {
        if !changing {
            return GateDecision::Proceed;
        }
        if let Some(message) = &self.held {
            return GateDecision::Hold(message.clone());
        }
        let Some((id, progress)) = &self.previous else {
            return GateDecision::Proceed;
        };
        match gate(Some(progress)) {
            GateDecision::Proceed => GateDecision::Proceed,
            GateDecision::Hold(reason) => {
                let message = format!("waiting for {id}: {reason}");
                self.held = Some(message.clone());
                GateDecision::Hold(message)
            }
        }
    }

    /// Remember the progress of an earlier child under `id` = `"<namespace>/<name>"`.
    /// A later call replaces the earlier one. It does not clear a stored hold.
    pub fn record(&mut self, id: &str, progress: ChildProgress) {
        self.previous = Some((id.to_string(), progress));
    }
}

#[cfg(test)]
#[path = "gate_tests.rs"]
mod tests;
