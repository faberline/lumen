//! The workspace pins every faberline/core crate to one tag (#400).

use std::path::Path;

const CORE_URL: &str = "https://github.com/faberline/core";

fn repo_file(relative: &str) -> String {
    let path = Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/../../")).join(relative);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()))
}

#[test]
fn workspace_manifest_pins_every_core_crate_to_v0_5_0() {
    let manifest = repo_file("Cargo.toml");
    let lines: Vec<&str> = manifest
        .lines()
        .filter(|line| line.contains(CORE_URL) && !line.trim().starts_with('#'))
        .collect();

    assert_eq!(
        lines.len(),
        16,
        "expected 16 faberline/core dependency lines in Cargo.toml, found {}:\n{}",
        lines.len(),
        lines.join("\n")
    );
    for line in &lines {
        assert!(
            line.contains(r#"tag = "v0.5.0""#),
            "core dependency is not pinned to v0.5.0: {line}"
        );
    }
    for (number, line) in manifest.lines().enumerate() {
        assert!(
            !line.contains("v0.4.14"),
            "Cargo.toml line {} still pins v0.4.14: {line}",
            number + 1
        );
    }
}

#[test]
fn lock_file_has_no_v0_4_14_core_source() {
    let lock = repo_file("Cargo.lock");
    let lines: Vec<&str> = lock
        .lines()
        .filter(|line| line.contains("git+https://github.com/faberline/core?tag="))
        .collect();

    assert!(
        !lines.is_empty(),
        "Cargo.lock has no faberline/core git source; the test does not know what to check"
    );
    for line in &lines {
        assert!(
            !line.contains("?tag=v0.4.14#"),
            "Cargo.lock still resolves a core crate from v0.4.14: {line}"
        );
        assert!(
            line.contains("?tag=v0.5.0#"),
            "Cargo.lock resolves a core crate from another tag: {line}"
        );
    }
}
