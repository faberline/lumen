use super::*;
use serde_json::json;

fn progress(generation: i64, observed: i64, ready: bool) -> ChildProgress {
    ChildProgress {
        generation,
        observed_generation: observed,
        ready,
    }
}

fn hold_text(decision: &GateDecision) -> &str {
    match decision {
        GateDecision::Hold(text) => text,
        GateDecision::Proceed => panic!("expected Hold, got Proceed"),
    }
}

#[test]
fn gate_proceeds_without_a_previous_child() {
    assert_eq!(gate(None), GateDecision::Proceed);
}

#[test]
fn gate_holds_on_a_stale_generation() {
    let decision = gate(Some(&progress(4, 3, true)));
    let text = hold_text(&decision);
    assert!(text.contains("stale generation"), "{text}");
    assert!(text.contains("observed 3 of 4"), "{text}");
}

#[test]
fn gate_holds_when_not_ready() {
    assert_eq!(
        gate(Some(&progress(4, 4, false))),
        GateDecision::Hold("not Ready".to_string())
    );
}

#[test]
fn gate_proceeds_when_caught_up_and_ready() {
    assert_eq!(gate(Some(&progress(4, 4, true))), GateDecision::Proceed);
    assert_eq!(gate(Some(&progress(4, 5, true))), GateDecision::Proceed);
}

#[test]
fn gate_reports_stale_generation_before_not_ready() {
    let decision = gate(Some(&progress(4, 3, false)));
    assert!(hold_text(&decision).contains("stale generation"));
}

#[test]
fn child_progress_reads_the_live_lumen_json() {
    let ready = json!({
        "metadata": { "generation": 4 },
        "status": { "observedGeneration": 3, "conditions": [{ "type": "Ready", "status": "True" }] }
    });
    assert_eq!(child_progress(&ready), progress(4, 3, true));

    let not_ready = json!({
        "metadata": { "generation": 4 },
        "status": { "observedGeneration": 3, "conditions": [{ "type": "Ready", "status": "False" }] }
    });
    assert_eq!(child_progress(&not_ready), progress(4, 3, false));

    let only_progressing = json!({
        "metadata": { "generation": 4 },
        "status": { "observedGeneration": 4, "conditions": [{ "type": "Progressing", "status": "True" }] }
    });
    assert_eq!(child_progress(&only_progressing), progress(4, 4, false));

    assert_eq!(child_progress(&json!({})), progress(0, 0, false));
    assert_eq!(
        child_progress(&json!({ "metadata": { "generation": 2 } })),
        progress(2, 0, false)
    );
}

#[test]
fn just_written_is_never_caught_up() {
    let decision = gate(Some(&ChildProgress::just_written()));
    assert!(hold_text(&decision).contains("stale generation"));
}

#[test]
fn a_pass_stops_at_a_stale_generation_then_continues_after_recovery() {
    // Pass 1: A is written; B and C wait for it.
    let mut pass = RolloutGate::new();
    assert_eq!(pass.check(true), GateDecision::Proceed);
    pass.record("ns/A", ChildProgress::just_written());
    let b = pass.check(true);
    assert!(hold_text(&b).starts_with("waiting for ns/A:"));
    assert!(hold_text(&b).contains("stale generation"));
    let c = pass.check(true);
    assert_eq!(hold_text(&c), hold_text(&b));

    // Pass 2: A is caught up but not Ready; it is only read, not written.
    let mut pass = RolloutGate::new();
    assert_eq!(pass.check(false), GateDecision::Proceed);
    pass.record("ns/A", progress(2, 2, false));
    assert_eq!(
        pass.check(true),
        GateDecision::Hold("waiting for ns/A: not Ready".to_string())
    );
    assert_eq!(
        pass.check(true),
        GateDecision::Hold("waiting for ns/A: not Ready".to_string())
    );

    // Pass 3: A is caught up and Ready; B is written, C waits for B.
    let mut pass = RolloutGate::new();
    assert_eq!(pass.check(false), GateDecision::Proceed);
    pass.record("ns/A", progress(2, 2, true));
    assert_eq!(pass.check(true), GateDecision::Proceed);
    pass.record("ns/B", ChildProgress::just_written());
    let c = pass.check(true);
    assert!(hold_text(&c).starts_with("waiting for ns/B:"));

    // Pass 4: A and B are caught up and Ready; C proceeds.
    let mut pass = RolloutGate::new();
    assert_eq!(pass.check(false), GateDecision::Proceed);
    pass.record("ns/A", progress(2, 2, true));
    assert_eq!(pass.check(false), GateDecision::Proceed);
    pass.record("ns/B", progress(5, 5, true));
    assert_eq!(pass.check(true), GateDecision::Proceed);
}

#[test]
fn a_non_changing_entry_never_holds_and_never_clears_the_record() {
    let mut pass = RolloutGate::new();
    pass.record("ns/A", progress(2, 1, false));
    assert_eq!(pass.check(false), GateDecision::Proceed);
    assert!(matches!(pass.check(true), GateDecision::Hold(_)));
    assert_eq!(pass.check(false), GateDecision::Proceed);
}

#[test]
fn a_held_entry_that_has_a_live_child_is_recorded_for_later_entries() {
    let mut pass = RolloutGate::new();
    pass.record("ns/A", progress(2, 2, true));
    assert_eq!(pass.check(true), GateDecision::Proceed);
    // B was held by a rollout slot and is only recorded.
    pass.record("ns/B", progress(3, 2, true));
    let c = pass.check(true);
    assert!(hold_text(&c).starts_with("waiting for ns/B:"));
}

#[test]
fn a_new_gate_has_no_memory_of_an_earlier_pass() {
    let mut first = RolloutGate::new();
    first.record("ns/A", progress(2, 1, true));
    assert!(matches!(first.check(true), GateDecision::Hold(_)));
    assert_eq!(RolloutGate::new().check(true), GateDecision::Proceed);
}

/// Negative control: a gate that always proceeds fails the tests above.
#[test]
fn the_gate_must_not_be_a_no_op() {
    assert_ne!(gate(Some(&progress(2, 1, true))), GateDecision::Proceed);
    assert_ne!(gate(Some(&progress(2, 2, false))), GateDecision::Proceed);
}
