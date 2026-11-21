//! Contract tests for the opaque projected-token source (#53).

use service_auth::k8s::{
    OpaqueProjectedTokenError, OpaqueProjectedTokenSource, ProjectedTokenMode,
};
use std::path::{Path, PathBuf};

const SECRET: &str = "eyJ-lumen-contract-secret-0000";

fn token_path(dir: &Path) -> PathBuf {
    dir.join("token")
}

fn write_token(path: &Path, contents: impl AsRef<[u8]>) {
    std::fs::write(path, contents).expect("write token fixture");
}

#[test]
fn disabled_mode_reads_nothing() {
    let source = OpaqueProjectedTokenSource::disabled();
    assert!(matches!(source.mode(), ProjectedTokenMode::Disabled));
    assert!(source.path().is_none());
    assert!(matches!(source.read(), Ok(None)));
}

#[test]
fn required_mode_returns_the_file_text_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let path = token_path(dir.path());
    write_token(&path, format!("{SECRET}\n"));

    let source = OpaqueProjectedTokenSource::required(path.clone());
    assert_eq!(source.path(), Some(path.as_path()));

    let token = source
        .read()
        .expect("read succeeds")
        .expect("token present");
    assert_eq!(token.expose(), SECRET);
}

#[test]
fn missing_file_is_missing() {
    let dir = tempfile::tempdir().unwrap();
    let path = token_path(dir.path());

    match OpaqueProjectedTokenSource::required(path.clone()).read() {
        Err(OpaqueProjectedTokenError::Missing { path: reported }) => {
            assert_eq!(reported, path);
        }
        other => panic!("expected Missing, got {other:?}"),
    }
}

#[test]
fn unreadable_path_is_unreadable() {
    let dir = tempfile::tempdir().unwrap();

    // The directory itself is not a readable token file.
    let result = OpaqueProjectedTokenSource::required(dir.path()).read();
    assert!(
        matches!(result, Err(OpaqueProjectedTokenError::Unreadable { .. })),
        "expected Unreadable, got {result:?}"
    );
}

#[test]
fn empty_and_blank_files_are_empty() {
    for contents in ["", "\n", "  \t\n"] {
        let dir = tempfile::tempdir().unwrap();
        let path = token_path(dir.path());
        write_token(&path, contents);

        let result = OpaqueProjectedTokenSource::required(path).read();
        assert!(
            matches!(result, Err(OpaqueProjectedTokenError::Empty { .. })),
            "contents {contents:?}: expected Empty, got {result:?}"
        );
    }
}

#[test]
fn malformed_files_are_malformed() {
    let cases: [&[u8]; 5] = [
        b"abc def",
        b"abc\tdef",
        b"abc\ndef",
        b"ab\x07cd",
        &[0xff, 0xfe, 0xfd],
    ];
    for contents in cases {
        let dir = tempfile::tempdir().unwrap();
        let path = token_path(dir.path());
        write_token(&path, contents);

        let result = OpaqueProjectedTokenSource::required(path).read();
        assert!(
            matches!(result, Err(OpaqueProjectedTokenError::Malformed { .. })),
            "contents {contents:?}: expected Malformed, got {result:?}"
        );
    }
}

#[test]
fn rotation_is_visible_on_the_next_read() {
    let dir = tempfile::tempdir().unwrap();
    let path = token_path(dir.path());
    let tmp = dir.path().join("token.tmp");
    write_token(&path, "old-token-value");

    let source = OpaqueProjectedTokenSource::required(path.clone());
    let first = source.read().unwrap().unwrap();
    assert_eq!(first.expose(), "old-token-value");

    // Rotate the way the kubelet does: write a temporary file, then rename it.
    write_token(&tmp, "new-token-value");
    std::fs::rename(&tmp, &path).unwrap();
    let second = source.read().unwrap().unwrap();
    assert_eq!(second.expose(), "new-token-value");

    // An empty rotation is refused; no earlier value is served from a cache.
    write_token(&tmp, "");
    std::fs::rename(&tmp, &path).unwrap();
    let third = source.read();
    assert!(
        matches!(third, Err(OpaqueProjectedTokenError::Empty { .. })),
        "expected Empty after rotation, got {third:?}"
    );
}

#[test]
fn errors_never_contain_the_secret() {
    let dir = tempfile::tempdir().unwrap();
    let path = token_path(dir.path());
    // The interior space makes the content malformed.
    write_token(&path, format!("{SECRET} {SECRET}"));

    let err = OpaqueProjectedTokenSource::required(path.clone())
        .read()
        .expect_err("malformed content is refused");
    assert!(matches!(err, OpaqueProjectedTokenError::Malformed { .. }));

    let display = format!("{err}");
    let debug = format!("{err:?}");
    assert!(!display.contains(SECRET), "Display leaked the secret");
    assert!(!debug.contains(SECRET), "Debug leaked the secret");
    assert!(
        display.contains(&path.display().to_string()),
        "Display must name the file path"
    );
}

#[test]
fn token_debug_and_display_are_redacted() {
    let dir = tempfile::tempdir().unwrap();
    let path = token_path(dir.path());
    write_token(&path, SECRET);

    let token = OpaqueProjectedTokenSource::required(path)
        .read()
        .unwrap()
        .unwrap();
    assert!(
        !format!("{token:?}").contains(SECRET),
        "Debug leaked the token"
    );
    assert!(
        !format!("{token}").contains(SECRET),
        "Display leaked the token"
    );
}
