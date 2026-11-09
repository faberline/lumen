use crate::app::observability::metrics::process::parse_linux_vmhwm_bytes;
use crate::app::observability::metrics::Metrics;
use crate::ingest::domain::change_budget::ChangeBudget;

#[test]
fn rss_peak_keeps_the_largest_observed_sample_when_a_later_value_is_lower() {
    let mut state = super::RssHighWaterState::default();
    assert_eq!(state.observe(Some(8192)).bytes, 8192);
    assert_eq!(state.observe(Some(4096)).bytes, 8192);
    assert_eq!(state.observe(Some(12288)).bytes, 12288);
}

#[test]
fn rss_peak_marks_unavailable_without_erasing_observed_history() {
    let mut state = super::RssHighWaterState::default();
    state.observe(Some(8192));
    let unavailable = state.observe(None);
    assert_eq!(unavailable.bytes, 0);
    assert!(!unavailable.available);
    let recovered = state.observe(Some(4096));
    assert_eq!(recovered.bytes, 8192);
    assert!(recovered.available);
}

#[test]
fn rss_render_uses_one_captured_pair_after_compatibility_gauges_change() {
    let mut state = super::RssHighWaterState::default();
    let metrics = Metrics::new();
    let budget = ChangeBudget::with_hard_limit(64);
    let pending = metrics.read_pending_change_accounting(&budget);
    let available = state.observe(Some(8192));
    metrics.process_rss_high_water_bytes.set(0);
    metrics.process_rss_high_water_available.set(0);
    let out = metrics.render_segment_telemetry(pending, available);
    assert_eq!(
        rendered_pending_gauge(&out, "lumen_process_rss_high_water_bytes"),
        8192
    );
    assert_eq!(
        rendered_pending_gauge(&out, "lumen_process_rss_high_water_available"),
        1
    );

    let unavailable = state.observe(None);
    metrics.process_rss_high_water_bytes.set(12288);
    metrics.process_rss_high_water_available.set(1);
    let out = metrics.render_segment_telemetry(pending, unavailable);
    assert_eq!(
        rendered_pending_gauge(&out, "lumen_process_rss_high_water_bytes"),
        0
    );
    assert_eq!(
        rendered_pending_gauge(&out, "lumen_process_rss_high_water_available"),
        0
    );
}

fn rendered_pending_gauge(rendered: &str, name: &str) -> u64 {
    rendered
        .lines()
        .find_map(|line| line.strip_prefix(&format!("{name} ")))
        .unwrap_or_else(|| panic!("missing {name} in:\n{rendered}"))
        .parse()
        .unwrap_or_else(|_| panic!("non-integer {name} in:\n{rendered}"))
}

fn assert_coherent_pending_change_gauges(rendered: &str, held_bytes: u64) {
    let reserved = rendered_pending_gauge(rendered, "lumen_pending_change_reserved_bytes");
    let active = rendered_pending_gauge(rendered, "lumen_pending_change_active_bytes");
    let frozen = rendered_pending_gauge(rendered, "lumen_pending_change_frozen_bytes");
    let total = rendered_pending_gauge(rendered, "lumen_pending_change_total_bytes");
    let high_water = rendered_pending_gauge(rendered, "lumen_pending_change_high_water_bytes");
    assert_eq!(
        reserved + active + frozen,
        total,
        "mixed scrape:\n{rendered}"
    );
    assert!(high_water >= total, "peak below live total:\n{rendered}");
    assert!(
        total >= held_bytes,
        "the held test reservation is absent from the scrape:\n{rendered}"
    );
}

#[test]
fn pending_change_render_stays_coherent_during_concurrent_writes_and_scrapes() {
    let budget = ChangeBudget::process_shared();
    let held_owner = budget.owner();
    let held = held_owner.try_reserve(7).unwrap();
    let metrics = std::sync::Arc::new(Metrics::new());
    let gate = std::sync::Arc::new(std::sync::Barrier::new(3));
    let successes = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

    std::thread::scope(|scope| {
        let writer_budget = budget.clone();
        let writer_gate = gate.clone();
        let writer_successes = successes.clone();
        scope.spawn(move || {
            let owner = writer_budget.owner();
            writer_gate.wait();
            for _ in 0..256 {
                if let Ok(reservation) = owner.try_reserve(1) {
                    writer_successes.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    drop(reservation);
                }
            }
        });
        for _ in 0..2 {
            let metrics = metrics.clone();
            let reader_gate = gate.clone();
            scope.spawn(move || {
                reader_gate.wait();
                for _ in 0..256 {
                    assert_coherent_pending_change_gauges(&metrics.render(), 7);
                }
            });
        }
    });

    assert!(
        successes.load(std::sync::atomic::Ordering::Relaxed) > 0,
        "the writer must make real process-shared reservations"
    );
    drop(held);
}

#[test]
fn pending_change_accounting_helper_maps_one_isolated_budget_state() {
    let budget = ChangeBudget::with_hard_limit(64);
    let owner = budget.owner();
    let active = owner.try_reserve(11).unwrap().commit().unwrap();
    let frozen = owner.freeze().unwrap();
    let reserved = owner.try_reserve(7).unwrap();
    let metrics = Metrics::new();

    let values = metrics.read_pending_change_accounting(&budget);
    assert_eq!(values.reserved, 7);
    assert_eq!(values.active, 0);
    assert_eq!(values.frozen, 11);
    assert_eq!(values.total, 18);
    assert_eq!(values.high_water, 18);

    drop(reserved);
    drop(active);
    assert_eq!(frozen.publish().unwrap(), 11);
}

#[test]
fn parse_linux_vmhwm_requires_one_exact_kilobyte_row() {
    assert_eq!(
        parse_linux_vmhwm_bytes("Name:\tlumen\nVmHWM:\t 123 kB\n"),
        Some(123 * 1024)
    );
    assert_eq!(parse_linux_vmhwm_bytes("Name:\tlumen\n"), None);
    assert_eq!(parse_linux_vmhwm_bytes("VmHWM:\tnope kB\n"), None);
    assert_eq!(parse_linux_vmhwm_bytes("VmHWM:\t123 KB\n"), None);
    assert_eq!(parse_linux_vmhwm_bytes("VmHWM:\t123 kB extra\n"), None);
    assert_eq!(
        parse_linux_vmhwm_bytes("VmHWM:\t18446744073709551615 kB\n"),
        None
    );
    assert_eq!(
        parse_linux_vmhwm_bytes("VmHWM:\t1 kB\nVmHWM:\t2 kB\n"),
        None
    );
    assert_eq!(
        parse_linux_vmhwm_bytes("VmHWM:\t18446744073709551615 kB\nVmHWM:\t2 kB\n"),
        None
    );
}

#[cfg(target_os = "linux")]
#[test]
fn linux_render_reads_this_process_vmhwm_and_marks_it_available() {
    // VmHWM belongs to the whole process. Run the real assertions in a fresh
    // process so unrelated parallel tests cannot change its memory accounting.
    const CASE: &str = "app::observability::metrics::process::tests::linux_render_reads_this_process_vmhwm_and_marks_it_available";
    const CHILD: &str = "LUMEN_VMHWM_TEST_CHILD";
    if std::env::var(CHILD).as_deref() != Ok(CASE) {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                CASE,
                "--exact",
                "--format",
                "pretty",
                "--color",
                "never",
                "--test-threads=1",
            ])
            .env(CHILD, CASE)
            .output()
            .expect("run the original VmHWM assertions in an isolated process");
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            output.status.success(),
            "isolated VmHWM test failed:\n{stdout}\n{stderr}"
        );
        assert!(
            stdout.contains(&format!("test {CASE} ... ok")),
            "original case did not run:\n{stdout}"
        );
        assert!(
            stdout.contains("test result: ok. 1 passed; 0 failed; 0 ignored;"),
            "original assertions did not pass exactly once:\n{stdout}"
        );
        return;
    }
    let expected = std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| parse_linux_vmhwm_bytes(&status))
        .expect("Linux /proc/self/status must expose a valid VmHWM row");
    let m = Metrics::new();
    let out = m.render();
    assert!(
        out.contains("lumen_process_rss_high_water_available 1"),
        "VmHWM was available but render did not mark it available:\n{out}"
    );
    let rendered = m.process_rss_high_water_bytes.get();
    assert!(
        rendered >= expected,
        "rendered VmHWM {rendered} fell below pre-render /proc value {expected}"
    );
    assert!(
        rendered > 0,
        "a running test process must have nonzero VmHWM"
    );
}

#[cfg(not(target_os = "linux"))]
#[test]
fn non_linux_render_marks_rss_high_water_unavailable() {
    let m = Metrics::new();
    let out = m.render();
    assert!(
        out.contains("lumen_process_rss_high_water_available 0"),
        "non-Linux must not qualify an unavailable VmHWM value:\n{out}"
    );
    assert!(out.contains("lumen_process_rss_high_water_bytes 0"));
}
