//! Preserve the pinned Rig gate, including its explicit missing-baseline result.

use rig::pins::{BaselineStore, GateOutcome, Pin};
use serde_json::{json, Value};

pub struct PinCheck {
    pub permitted: bool,
    pub measurement: Value,
}

pub fn evaluate(
    pin: &Pin,
    scenario: &str,
    value: f64,
    baselines: &BaselineStore,
    strict: bool,
) -> PinCheck {
    assert!(value.is_finite(), "non-finite load metric");
    let (outcome, severity, permitted, ratchet_evaluated) =
        match rig::pins::gate(pin, scenario, value, baselines) {
            GateOutcome::Pass => ("pass", "none", true, pin.ratchet.is_some()),
            GateOutcome::FloorBreach { .. } => ("floor_breach", "high", false, false),
            GateOutcome::RatchetBreach { .. } => ("ratchet_breach", "high", false, true),
            GateOutcome::NoBaseline { .. } => (
                "no_baseline",
                if strict { "high" } else { "info" },
                !strict,
                false,
            ),
        };
    PinCheck {
        permitted,
        measurement: json!({
            "scenario":scenario,"issue":pin.issue,"pin_scenario":pin.scenario,
            "metric":pin.metric,"value":value,
            "floor":pin.floor,"ratchet":pin.ratchet,"outcome":outcome,
            "severity":severity,"strict":strict,"permitted":permitted,
            "ratchet_evaluated":ratchet_evaluated,
            "baseline":baselines.get(scenario,&pin.metric).map(|entry|entry.value),
            "host_fingerprint":rig::pins::baseline::host_fingerprint(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pin() -> Pin {
        Pin {
            issue: "axiom#5".into(),
            scenario: "load/search_qps".into(),
            metric: "p99_ms".into(),
            floor: Some(25.0),
            ratchet: Some(0.8),
        }
    }

    #[test]
    fn original_floor_remains_absolute_before_any_ratchet() {
        let directory = tempfile::tempdir().unwrap();
        let store = BaselineStore::load(directory.path());
        let rejected = evaluate(&pin(), "lumen/load/search_qps", 25.001, &store, false);
        assert!(!rejected.permitted);
        assert_eq!(rejected.measurement["outcome"], "floor_breach");
        let boundary = evaluate(&pin(), "lumen/load/search_qps", 25.0, &store, false);
        assert!(boundary.permitted);
        assert_eq!(boundary.measurement["outcome"], "no_baseline");
    }

    #[test]
    fn missing_baseline_is_explicit_and_original_strict_mode_gates_it() {
        let directory = tempfile::tempdir().unwrap();
        let store = BaselineStore::load(directory.path());
        for strict in [false, true] {
            let result = evaluate(&pin(), "lumen/load/search_qps", 12.0, &store, strict);
            assert_eq!(result.permitted, !strict);
            assert_eq!(result.measurement["outcome"], "no_baseline");
            assert_eq!(
                result.measurement["severity"],
                if strict { "high" } else { "info" }
            );
            assert_eq!(result.measurement["ratchet_evaluated"], false);
            assert!(result.measurement["baseline"].is_null());
        }
    }

    #[test]
    fn recorded_same_host_baseline_keeps_the_original_eighty_percent_ratchet() {
        let directory = tempfile::tempdir().unwrap();
        let mut store = BaselineStore::load(directory.path());
        store.record("lumen/load/search_qps", "p99_ms", 10.0);
        let boundary = evaluate(&pin(), "lumen/load/search_qps", 12.5, &store, true);
        assert!(boundary.permitted);
        assert_eq!(boundary.measurement["outcome"], "pass");
        assert_eq!(boundary.measurement["ratchet_evaluated"], true);
        let rejected = evaluate(&pin(), "lumen/load/search_qps", 12.501, &store, false);
        assert!(!rejected.permitted);
        assert_eq!(rejected.measurement["outcome"], "ratchet_breach");
    }

    #[test]
    fn a_foreign_platform_baseline_cannot_gate_this_host() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join(".rig");
        std::fs::create_dir(&path).unwrap();
        std::fs::write(
            path.join("baselines.json"),
            serde_json::to_vec(&json!({
                "entries":{"lumen/load/search_qps::p99_ms::foreign-platform":{
                    "value":0.001,"recorded_at":1,"tool_version":"original-fixture"
                }}
            }))
            .unwrap(),
        )
        .unwrap();
        let store = BaselineStore::load(directory.path());
        let result = evaluate(&pin(), "lumen/load/search_qps", 12.0, &store, true);
        assert!(!result.permitted);
        assert_eq!(result.measurement["outcome"], "no_baseline");
    }
}
