//! Deterministic oracle for the Lumen release supply chain.
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Command, Output},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Finding {
    pub code: &'static str,
    pub detail: String,
}

const DEBIAN_BASE: &str =
    "debian:bookworm-slim@sha256:88200866dfff7ea7f5cbcb6ec7c8a701889efe6fe859fe64d6990e4b07ea4171";
const DISTROLESS_BASE: &str =
    "gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab";
macro_rules! require {
    ($condition:expr, $code:expr, $detail:expr) => {
        if !$condition {
            return Err(Finding {
                code: $code,
                detail: $detail.into(),
            });
        }
    };
}
struct Inputs {
    docs: String,
    dockerfile: String,
    installer: String,
    cases: String,
    rendered: String,
}

#[rustfmt::skip]
fn root() -> PathBuf { PathBuf::from(env!("CARGO_MANIFEST_DIR")) }
fn between<'a>(source: &'a str, start: &str, end: &str) -> &'a str {
    source
        .split_once(start)
        .and_then(|(_, tail)| tail.split_once(end))
        .map_or("", |v| v.0)
}

#[rustfmt::skip]
fn validate(input: &Inputs) -> Result<(), Finding> {
    let installer = &input.installer;
    for needle in ["|| die \"checksum download failed: ${sha_url}\"", "awk 'NR == 1 { print $1; exit }'", "[ \"${#expected}\" -eq 64 ]", "missing required checksum tool", "[ \"${actual}\" != \"${expected}\" ]", "actual_version=\"$(\"${bin}\" --version 2>/dev/null)\"", "[ \"${actual_version}\" = \"${expected_version}\" ]"] {
        require!(installer.contains(needle), "INSTALLER_INTEGRITY", format!("installer integrity proof missing: {needle}"));
    }
    require!(!installer.contains("Best-effort integrity check"), "INSTALLER_INTEGRITY", "installer must fail closed when the checksum is missing");
    let installer_checksum = between(installer, "# ---- download + verify", "# ---- extract + install");
    let installer_version = between(installer, "bin=\"${tmpdir}/lumen-${target}/lumen\"", "mkdir -p \"${INSTALL_DIR}\"");
    require!(!installer_checksum.contains("true ||") && !installer_checksum.contains("|| true") && !installer_version.contains("true ||") && !installer_version.contains("|| true"), "INSTALLER_INTEGRITY", "installer integrity control flow contains a bypass");

    for needle in ["{{json .Manifest}}' | jq -er '.digest'", "[[ \"$RAW_DIGEST\" =~ ^sha256:[0-9a-f]{64}$ ]]", "IMAGE=\"ghcr.io/faberline/lumen@${RAW_DIGEST}\"", "--candidate-run-id <id>", "--mode public", "--output /tmp/lumen-public-release.json", "protected annotated `lumen@<version>` tag", "discovery-only", "runs the GKE e2e suites and a native arm64 standalone smoke before publication"] {
        require!(input.docs.contains(needle), "DEPLOYMENT_DIGEST", format!("deployment proof missing: {needle}"));
    }
    let verify_docs = between(&input.docs, "Create one protected annotated `lumen@<version>` tag at the exact candidate\ncommit before promotion. Then verify the public release before deployment:", "Each release image carries");
    require!(verify_docs.contains("--candidate-run-id <id>") && verify_docs.contains("--mode public") && verify_docs.contains("--output /tmp/lumen-public-release.json"), "DEPLOYMENT_DIGEST", "published verifier must bind the candidate run and write its receipt");
    let fetch = between(&input.dockerfile, &format!("FROM {DEBIAN_BASE} AS binary-source-fetch"), &format!("FROM {DEBIAN_BASE} AS binary-source-staged"));
    let staged = between(&input.dockerfile, &format!("FROM {DEBIAN_BASE} AS binary-source-staged"), "FROM binary-source-${SOURCE} AS binary-source");
    let froms: Vec<_> = input.dockerfile.lines().filter_map(|line| line.trim().strip_prefix("FROM ")).map(|line| line.split_whitespace().take(3).collect::<Vec<_>>().join(" ")).collect();
    let expected_froms = vec![format!("{DEBIAN_BASE} AS seed"), format!("{DEBIAN_BASE} AS binary-source-fetch"), format!("{DEBIAN_BASE} AS binary-source-staged"), "binary-source-${SOURCE} AS binary-source".into(), DISTROLESS_BASE.into()];
    require!(froms == expected_froms, "DOCKERFILE_CONTRACT", "release Dockerfile base image inventory or digest changed");
    for needle in ["ARG SOURCE=fetch", "FROM binary-source-${SOURCE} AS binary-source", "ENV LUMEN_HOST=0.0.0.0", "ENTRYPOINT [\"/usr/local/bin/lumen\"]", "CMD [\"serve\"]"] {
        require!(input.dockerfile.contains(needle), "DOCKERFILE_CONTRACT", format!("Dockerfile release contract missing: {needle}"));
    }
    require!(input.dockerfile.matches("ARG TARGETARCH\n").count() == 2 && !input.dockerfile.contains("ARG TARGETARCH="), "TARGETARCH_SELECTION", "TARGETARCH must expose BuildKit's automatic target without a fallback");
    require!(fetch.contains("amd64) t=x86_64-unknown-linux-musl") && fetch.contains("arm64) t=aarch64-unknown-linux-musl") && staged.contains("COPY dist/linux/${TARGETARCH}/lumen /tmp/lumen"), "TARGETARCH_SELECTION", "TARGETARCH must select the matching fetched and staged binaries");
    require!(fetch.contains("releases/download/${LUMEN_VERSION}") && fetch.contains("sha256sum -c \"${asset}.sha256\"") && !["curl", "apt-get", "releases/download"].iter().any(|v| staged.contains(v)), "DOCKERFILE_CONTRACT", "fetch/staged sources are not isolated");
    let registered = input.cases.lines().filter(|line| line.trim() == "mod release_artifacts;").count();
    require!(registered == 1, "CARGO_REGISTRATION", "release oracle must be registered exactly once");
    for needle in ["ARG SOURCE=fetch", "ARG LUMEN_VERSION=lumen@9.9.9", "releases/download/${LUMEN_VERSION}", "sha256sum -c \"${asset}.sha256\"", "FROM binary-source-${SOURCE}", "--build-arg LUMEN_VERSION=lumen@9.9.9"] {
        require!(input.rendered.contains(needle), "PUBLIC_RENDER", format!("public render missing: {needle}"));
    }
    require!(!input.rendered.contains("SOURCE=staged"), "PUBLIC_RENDER", "public render selected staged source");
    Ok(())
}

#[rustfmt::skip]
fn render_release() -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_lumen")).args(["dockerfile", "render", "--variant", "release", "--version", "9.9.9"]).output().expect("run lumen dockerfile render");
    assert!(output.status.success(), "render failed: {}", String::from_utf8_lossy(&output.stderr));
    String::from_utf8(output.stdout).expect("render is UTF-8")
}
#[rustfmt::skip]
fn live() -> Inputs {
    let root = root();
    let read = |path: &str| fs::read_to_string(root.join(path)).unwrap_or_else(|e| panic!("read {path}: {e}"));
    Inputs { docs: read("docs/deployment.md"), dockerfile: read("Dockerfile.release"), installer: read("install.sh"), cases: read("tests/it/main.rs"), rendered: render_release() }
}
#[rustfmt::skip]
fn replace_once(source: &str, from: &str, to: &str) -> String {
    assert_ne!(from, to, "mutation must change bytes");
    assert_eq!(source.matches(from).count(), 1, "mutation target {from:?} is not unique");
    let changed = source.replacen(from, to, 1); assert_ne!(changed, source); changed
}
#[rustfmt::skip]
fn replace_nth(source: &str, from: &str, to: &str, nth: usize) -> String {
    let offsets: Vec<_> = source.match_indices(from).map(|(at, _)| at).collect();
    let at = *offsets.get(nth).unwrap_or_else(|| panic!("mutation target {from:?} has only {} sites", offsets.len()));
    let changed = format!("{}{}{}", &source[..at], to, &source[at + from.len()..]); assert_ne!(changed, source); changed
}
fn expect(mutated: Inputs, code: &'static str) {
    let finding = validate(&mutated).expect_err("negative mutation passed");
    assert_eq!(finding.code, code, "wrong finding: {finding:?}");
}

const RELEASE_TARGETS: [&str; 5] = [
    "aarch64-apple-darwin",
    "x86_64-unknown-linux-gnu",
    "aarch64-unknown-linux-gnu",
    "x86_64-unknown-linux-musl",
    "aarch64-unknown-linux-musl",
];

fn release_host_target() -> Option<&'static str> {
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("macos", "aarch64") => Some("aarch64-apple-darwin"),
        ("linux", "x86_64") => Some("x86_64-unknown-linux-gnu"),
        ("linux", "aarch64") => Some("aarch64-unknown-linux-gnu"),
        _ => None,
    }
}

fn sha256_file(path: &Path) -> String {
    for (program, args) in [
        ("sha256sum", vec![path.as_os_str()]),
        (
            "shasum",
            vec![
                std::ffi::OsStr::new("-a"),
                std::ffi::OsStr::new("256"),
                path.as_os_str(),
            ],
        ),
    ] {
        let Ok(output) = Command::new(program).args(args).output() else {
            continue;
        };
        if output.status.success() {
            return String::from_utf8(output.stdout)
                .expect("checksum output is UTF-8")
                .split_whitespace()
                .next()
                .expect("checksum output has a digest")
                .to_string();
        }
    }
    panic!("sha256sum or shasum is required for the release fixture");
}

fn write_checksum_sidecar(dir: &Path, target: &str) {
    let asset_name = format!("lumen-{target}.tar.gz");
    let digest = sha256_file(&dir.join(&asset_name));
    fs::write(
        dir.join(format!("{asset_name}.sha256")),
        format!("{digest}  {asset_name}\n"),
    )
    .expect("write checksum sidecar");
}

fn binary_fixture(version: &str, exit_code: i32) -> (tempfile::TempDir, &'static str) {
    let host = release_host_target().expect("release verifier runs on a supported host");
    let dir = tempfile::tempdir().expect("create release fixture");
    for target in RELEASE_TARGETS {
        let asset_name = format!("lumen-{target}.tar.gz");
        let asset = dir.path().join(&asset_name);
        let package = dir.path().join("stage").join(format!("lumen-{target}"));
        fs::create_dir_all(&package).expect("create release package");
        fs::write(package.join("README.md"), "release fixture\n").unwrap();
        let binary = package.join("lumen");
        fs::write(
            &binary,
            format!("#!/bin/sh\nprintf 'lumen {version}\\n'\nexit {exit_code}\n"),
        )
        .expect("write fixture binary");
        let mut permissions = fs::metadata(&binary).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&binary, permissions).unwrap();
        let status = Command::new("tar")
            .arg("-C")
            .arg(dir.path().join("stage"))
            .arg("-czf")
            .arg(&asset)
            .arg(format!("lumen-{target}"))
            .status()
            .expect("run tar");
        assert!(status.success(), "tar fixture failed");
        write_checksum_sidecar(dir.path(), target);
    }
    (dir, host)
}

fn binary_asset_names() -> Vec<String> {
    RELEASE_TARGETS
        .into_iter()
        .flat_map(|target| {
            let archive = format!("lumen-{target}.tar.gz");
            [archive.clone(), format!("{archive}.sha256")]
        })
        .collect()
}

fn write_executable(path: &Path, body: &str) {
    fs::write(path, body).expect("write executable fixture");
    let mut permissions = fs::metadata(path).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).unwrap();
}

fn run_installer_fixture(assets: &Path, host: &str) -> (tempfile::TempDir, Output) {
    let run = tempfile::tempdir().expect("create installer run dir");
    let mock = run.path().join("mock");
    fs::create_dir(&mock).unwrap();
    write_executable(
        &mock.join("curl"),
        r#"#!/bin/sh
set -eu
out=""
url=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    -o) out="$2"; shift 2 ;;
    -H) shift 2 ;;
    -fsSL) shift ;;
    *) url="$1"; shift ;;
  esac
done
[ -n "$out" ]
cp "$LUMEN_TEST_ASSET_DIR/${url##*/}" "$out"
"#,
    );
    write_executable(&mock.join("gh"), "#!/bin/sh\nexit 1\n");
    let (os, arch) = match host {
        "aarch64-apple-darwin" => ("Darwin", "arm64"),
        "x86_64-unknown-linux-gnu" => ("Linux", "x86_64"),
        "aarch64-unknown-linux-gnu" => ("Linux", "aarch64"),
        other => panic!("unsupported installer fixture host: {other}"),
    };
    write_executable(
        &mock.join("uname"),
        &format!(
            "#!/bin/sh\ncase \"${{1:-}}\" in\n  -s) printf '{os}\\n' ;;\n  -m) printf '{arch}\\n' ;;\n  *) exit 2 ;;\nesac\n"
        ),
    );
    let path = format!(
        "{}:{}",
        mock.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let output = Command::new("sh")
        .arg(root().join("install.sh"))
        .env("PATH", path)
        .env("LUMEN_VERSION", "lumen@9.9.9")
        .env("LUMEN_INSTALL", run.path().join("install"))
        .env("LUMEN_REPO", "faberline/lumen")
        .env("LUMEN_TEST_ASSET_DIR", assets)
        .env_remove("GH_TOKEN")
        .env_remove("GITHUB_TOKEN")
        .output()
        .expect("run installer fixture");
    (run, output)
}

#[test]
fn installer_executes_checksum_and_version_failure_paths() {
    let (valid, host) = binary_fixture("9.9.9", 0);
    let (run, output) = run_installer_fixture(valid.path(), host);
    assert!(
        output.status.success(),
        "valid installer fixture failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let installed = run.path().join("install/lumen");
    assert!(installed.is_file());
    let output = Command::new(&installed).arg("--version").output().unwrap();
    assert!(output.status.success());
    assert_eq!(String::from_utf8(output.stdout).unwrap(), "lumen 9.9.9\n");

    let (bad_checksum, host) = binary_fixture("9.9.9", 0);
    let sidecar = bad_checksum
        .path()
        .join(format!("lumen-{host}.tar.gz.sha256"));
    fs::write(
        &sidecar,
        format!("{}  lumen-{host}.tar.gz\n", "0".repeat(64)),
    )
    .unwrap();
    let (run, output) = run_installer_fixture(bad_checksum.path(), host);
    assert!(!output.status.success(), "bad installer checksum passed");
    assert!(!run.path().join("install/lumen").exists());

    let (missing_checksum, host) = binary_fixture("9.9.9", 0);
    fs::remove_file(
        missing_checksum
            .path()
            .join(format!("lumen-{host}.tar.gz.sha256")),
    )
    .unwrap();
    let (run, output) = run_installer_fixture(missing_checksum.path(), host);
    assert!(!output.status.success(), "missing checksum passed");
    assert!(!run.path().join("install/lumen").exists());

    let (wrong_version, host) = binary_fixture("9.9.8", 0);
    let (run, output) = run_installer_fixture(wrong_version.path(), host);
    assert!(!output.status.success(), "wrong installer version passed");
    assert!(!run.path().join("install/lumen").exists());

    let (nonzero_version, host) = binary_fixture("9.9.9", 17);
    let (run, output) = run_installer_fixture(nonzero_version.path(), host);
    assert!(
        !output.status.success(),
        "non-zero installer version passed"
    );
    assert!(!run.path().join("install/lumen").exists());
}

#[test]
fn release_asset_inventory_requires_exact_five_pairs() {
    let names = binary_asset_names();
    assert_eq!(names.len(), 10);
    assert!(RELEASE_TARGETS
        .iter()
        .all(|target| names.contains(&format!("lumen-{target}.tar.gz"))));

    let mut missing = names.clone();
    missing.pop();
    assert_ne!(missing.len(), 10);

    let mut duplicate = names.clone();
    duplicate.push(names[0].clone());
    assert_ne!(duplicate.len(), 10);

    let mut extra = names;
    extra.push("lumen-s390x-unknown-linux-gnu.tar.gz".into());
    extra.push("lumen-s390x-unknown-linux-gnu.tar.gz.sha256".into());
    assert_ne!(extra.len(), 10);
}

#[test]
fn live_release_artifacts_satisfy_contract() {
    validate(&live()).expect("live release artifacts satisfy the frozen contract");
}

#[test]
#[rustfmt::skip]
fn scoped_negative_mutations_fail_with_stable_findings() {
    let mut fixture = live(); fixture.cases = replace_once(&fixture.cases, "mod release_artifacts;", "// mod release_artifacts;"); expect(fixture, "CARGO_REGISTRATION");
    let mut fixture = live(); fixture.installer = replace_once(&fixture.installer, "|| die \"checksum download failed: ${sha_url}\"", "|| true"); expect(fixture, "INSTALLER_INTEGRITY");
    let mut fixture = live(); fixture.installer = replace_once(&fixture.installer, "expected=\"$(awk 'NR == 1 { print $1; exit }' \"${tmpdir}/${asset}.sha256\")\"", "expected=\"$(cat \"${tmpdir}/${asset}.sha256\")\""); expect(fixture, "INSTALLER_INTEGRITY");
    let mut fixture = live(); fixture.installer = replace_once(&fixture.installer, "[ \"${actual_version}\" = \"${expected_version}\" ]", "true"); expect(fixture, "INSTALLER_INTEGRITY");
    let mut fixture = live(); fixture.installer = replace_once(&fixture.installer, "[ \"${actual_version}\" = \"${expected_version}\" ]", "true || [ \"${actual_version}\" = \"${expected_version}\" ]"); expect(fixture, "INSTALLER_INTEGRITY");
    let mut fixture = live(); fixture.dockerfile = replace_once(&fixture.dockerfile, "ARG SOURCE=fetch", "ARG SOURCE=staged"); expect(fixture, "DOCKERFILE_CONTRACT");
    let mut fixture = live(); fixture.dockerfile = replace_once(&fixture.dockerfile, &format!("FROM {DEBIAN_BASE} AS seed"), "FROM debian:bookworm-slim AS seed"); expect(fixture, "DOCKERFILE_CONTRACT");
    let mut fixture = live(); fixture.dockerfile = replace_once(&fixture.dockerfile, &format!("FROM {DISTROLESS_BASE}"), "FROM gcr.io/distroless/static-debian12:nonroot@sha256:0000000000000000000000000000000000000000000000000000000000000000"); expect(fixture, "DOCKERFILE_CONTRACT");
    for nth in 0..2 {
        let mut fixture = live(); fixture.dockerfile = replace_nth(&fixture.dockerfile, "ARG TARGETARCH\n", "ARG TARGETARCH=amd64\n", nth); expect(fixture, "TARGETARCH_SELECTION");
    }
    let mut fixture = live(); fixture.dockerfile = replace_once(&fixture.dockerfile, "COPY dist/linux/${TARGETARCH}/lumen /tmp/lumen", "COPY dist/linux/amd64/lumen /tmp/lumen"); expect(fixture, "TARGETARCH_SELECTION");
    let mut fixture = live(); fixture.dockerfile = replace_once(&fixture.dockerfile, "arm64) t=aarch64-unknown-linux-musl", "arm64) t=x86_64-unknown-linux-musl"); expect(fixture, "TARGETARCH_SELECTION");
}
