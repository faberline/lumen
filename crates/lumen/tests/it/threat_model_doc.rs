//! The threat model in `docs/security/threat-model.md` stays complete, and the
//! supply-chain steps it names stay in the release workflows.
//!
//! The document is checked for shape only: four sections in order, three
//! trust-boundary surfaces with enough threats, and every threat pointing at a
//! mitigation that exists. The release workflows must keep the keyless cosign
//! signature, the SPDX SBOM attestations and the verification steps. A removed
//! step fails the test, not a reader's memory of the workflow.

use std::collections::HashSet;
use std::fs;
use std::path::PathBuf;

fn repo_root() -> PathBuf {
    PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../.."))
}

fn read(relative: &str) -> String {
    let path = repo_root().join(relative);
    fs::read_to_string(&path).unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()))
}

fn read_threat_model() -> String {
    read("docs/security/threat-model.md")
}

/// The text under `heading`, up to the next `## ` heading or the end of the file.
fn section<'a>(doc: &'a str, heading: &str) -> Option<&'a str> {
    let start = if doc.starts_with(&format!("{heading}\n")) {
        0
    } else {
        doc.find(&format!("\n{heading}\n"))? + 1
    };
    let body = &doc[start + heading.len()..];
    let end = body.find("\n## ").unwrap_or(body.len());
    Some(&body[..end])
}

/// The non-empty cells of a markdown table row, trimmed.
fn cells(row: &str) -> Vec<&str> {
    row.split('|')
        .map(str::trim)
        .filter(|c| !c.is_empty())
        .collect()
}

/// The needles that `text` does not contain.
fn missing(text: &str, needles: &[&str]) -> Vec<String> {
    needles
        .iter()
        .filter(|n| !text.contains(**n))
        .map(|n| n.to_string())
        .collect()
}

#[test]
fn threat_model_has_the_four_sections_in_order() {
    let doc = read_threat_model();
    let lines: Vec<&str> = doc.lines().collect();
    let headings = [
        "## Assets",
        "## Trust boundaries",
        "## Threats",
        "## Mitigations",
    ];
    let positions: Vec<usize> = headings
        .iter()
        .map(|h| {
            lines
                .iter()
                .position(|l| l == h)
                .unwrap_or_else(|| panic!("missing heading `{h}`"))
        })
        .collect();
    assert!(
        positions.windows(2).all(|w| w[0] < w[1]),
        "headings out of order: {positions:?}"
    );
}

#[test]
fn threat_model_covers_the_three_surfaces() {
    let doc = read_threat_model();

    let boundaries = section(&doc, "## Trust boundaries").expect("Trust boundaries section");
    for surface in ["### HTTP API", "### Operator", "### Image"] {
        assert!(
            boundaries.lines().any(|l| l == surface),
            "`{surface}` is missing under Trust boundaries"
        );
    }

    let threats = section(&doc, "## Threats").expect("Threats section");
    let rows: Vec<&str> = threats.lines().filter(|l| l.starts_with("| T")).collect();
    assert!(
        rows.len() >= 9,
        "expected at least 9 threats, found {}",
        rows.len()
    );
    for surface in ["HTTP API", "Operator", "Image"] {
        let count = rows
            .iter()
            .filter(|row| cells(row).get(1) == Some(&surface))
            .count();
        assert!(
            count >= 3,
            "`{surface}` has {count} threats, needs at least 3"
        );
    }
}

#[test]
fn every_threat_names_an_existing_mitigation() {
    let doc = read_threat_model();
    let threats = section(&doc, "## Threats").expect("Threats section");
    let mitigations = section(&doc, "## Mitigations").expect("Mitigations section");

    let mitigation_ids: HashSet<&str> = mitigations
        .lines()
        .filter(|l| l.starts_with("| M"))
        .filter_map(|l| cells(l).first().copied())
        .collect();
    assert!(!mitigation_ids.is_empty(), "no mitigation rows found");

    for row in threats.lines().filter(|l| l.starts_with("| T")) {
        let cells = cells(row);
        let id = cells.first().copied().unwrap_or("?");
        let mitigation = cells.last().copied().unwrap_or("");
        assert!(
            mitigation_ids.contains(mitigation),
            "threat {id} names mitigation `{mitigation}`, which is not in ## Mitigations"
        );
    }
}

#[test]
fn release_workflows_keep_sbom_signing_and_verification() {
    let candidate = read(".github/workflows/lumen-release-candidate.yml");
    let release = read(".github/workflows/lumen-release.yml");
    let verify = read("scripts/verify-release-candidate.sh");

    let missing_candidate = missing(
        &candidate,
        &[
            "cosign sign --yes",
            "anchore/sbom-action@",
            "sbom-path: spdx-amd64.json",
            "sbom-path: spdx-arm64.json",
            "actions/attest@",
            "scripts/verify-release-candidate.sh",
        ],
    );
    let missing_release = missing(&release, &["scripts/verify-release-artifacts.sh"]);
    let missing_verify = missing(&verify, &["cosign verify", "gh attestation verify"]);

    assert!(
        missing_candidate.is_empty() && missing_release.is_empty() && missing_verify.is_empty(),
        "supply-chain steps removed: candidate {missing_candidate:?}, \
         release {missing_release:?}, verify script {missing_verify:?}"
    );
}

#[test]
fn a_removed_signing_step_is_detected() {
    // The helper must report a missing needle and accept a present one, or the
    // test above could pass for any text.
    assert_eq!(
        missing("run: echo hi", &["cosign sign --yes"]),
        vec!["cosign sign --yes".to_string()]
    );
    assert!(missing("run: cosign sign --yes img", &["cosign sign --yes"]).is_empty());
}
