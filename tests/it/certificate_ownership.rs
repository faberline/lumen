//! Where certificate code is allowed to live (#3110 AC6).
//!
//! R1 says the generic lifecycle belongs in shared libraries and that Lumen
//! supplies profiles. That is an architectural claim, and architectural claims
//! decay silently: the first copy of an issuance loop into `apps/lumen` compiles,
//! passes every behavioural test, and is only visible to someone who happens to
//! read the right file. So it is checked here, mechanically, against the source
//! tree itself.
//!
//! The check is deliberately a *type* allowlist rather than a keyword denylist.
//! A denylist of "no `rcgen`, no CSR" is a guess about which name the next
//! duplicate will be spelled with; an allowlist of what Lumen's certificate
//! module may contain refuses the one nobody predicted.
//!
//! The shared-library half of this check lives with `service-k8s` in
//! faberline/core.

use std::path::{Path, PathBuf};

/// Lumen's crate root is the repository root.
fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn read(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|err| panic!("read {}: {err}", path.display()))
}

/// Every `.rs` file under `dir`.
fn rust_files(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(current) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&current) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                out.push(path);
            }
        }
    }
    out.sort();
    out
}

#[test]
fn lumen_owns_certificate_profiles_and_nothing_else() {
    let lumen_cert = repo_root().join("src/operator/certificate.rs");
    assert!(
        lumen_cert.is_file(),
        "expected Lumen's certificate profiles at {}",
        lumen_cert.display()
    );
    let source = read(&lumen_cert);

    // The allowlist: what a profile module legitimately does is construct
    // `CertificateProfile`s and name the identities this service answers to.
    // Anything that generates a key, speaks to a CA, writes a Secret, or decides
    // when to renew is generic and has a home already.
    let forbidden: [(&str, &str); 8] = [
        ("KeyPair", "generating key material is the shared issuer's job"),
        ("rcgen", "no service links a CSR builder directly"),
        (
            "IssuanceRequest",
            "requesting issuance is the shared reconciler's job",
        ),
        (
            "next_action",
            "renewal and rotation timing is the shared state machine's job",
        ),
        (
            "material_secret",
            "Secret projection is the shared projector's job",
        ),
        (
            "trust_bundle_secret",
            "trust-bundle layout is the shared projector's job",
        ),
        (
            "privateca.googleapis.com",
            "no service talks to CA Service directly",
        ),
        (
            "impl Issuer",
            "a service-local issuer is exactly the duplication R1 forbids",
        ),
    ];
    for (needle, why) in forbidden {
        assert!(
            !source.contains(needle),
            "src/operator/certificate.rs mentions `{needle}`: {why}"
        );
    }
}

#[test]
fn no_other_lumen_source_file_implements_a_certificate_lifecycle() {
    let root = repo_root();
    let lumen_src = root.join("src");
    let profiles = root.join("src/operator/certificate.rs");

    // These names are how a lifecycle gets built, whatever the file is called.
    let lifecycle_markers = [
        "CertificateSigningRequestParams",
        "IssuanceRequest",
        "trust_anchor_pem",
        "next_action(",
    ];

    let mut offenders = Vec::new();
    for path in rust_files(&lumen_src) {
        if path == profiles {
            continue;
        }
        let source = read(&path);
        for marker in lifecycle_markers {
            if source.contains(marker) {
                offenders.push(format!("{} uses `{marker}`", path.display()));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "certificate lifecycle machinery appeared under src/ outside the profile module: \
         {offenders:#?}"
    );
}

#[test]
fn terraform_provisions_trust_but_never_issues_a_leaf() {
    // R9, checked where it can actually be checked. A `google_privateca_certificate`
    // resource would make `terraform apply` the renewal mechanism, and it would
    // work — for exactly one lifetime.
    let terraform = repo_root().join("terraform");
    if !terraform.is_dir() {
        return;
    }
    let mut offenders = Vec::new();
    let mut stack = vec![terraform];
    while let Some(current) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&current) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            let is_tf = path
                .extension()
                .is_some_and(|ext| ext == "tf" || ext == "hcl");
            if !is_tf {
                continue;
            }
            let source = read(&path);
            if source.contains("resource \"google_privateca_certificate\"") {
                offenders.push(path.display().to_string());
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "Terraform declares leaf certificates, so renewal needs an apply: {offenders:#?}"
    );
}
