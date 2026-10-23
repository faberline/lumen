use crate::persistence::infrastructure::segment_rdb_store::SegmentRdbStore;

#[test]
fn failed_owned_scratch_cleanup_retains_both_pins() {
    let directory = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(directory.path()).unwrap();
    let (_, scratch) = store.begin_next_generation(1).unwrap();
    let scratch_path = scratch.path().to_path_buf();
    let scratch_name = scratch_path
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    let source_name = "gen-0-rev-0";
    store.background.pin(scratch_name.clone());
    store.background.pin(source_name.to_owned());

    let error = store
        .cleanup_owned_merge_staging_with(
            &scratch_path,
            &scratch_name,
            source_name,
            scratch,
            |_| Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied)),
        )
        .expect_err("a failed owned scratch removal must remain an error");

    assert!(format!("{error:#}").contains("remove owned background merge staging"));
    assert!(
        store.background.protects(&scratch_name),
        "cleanup failure must retain the scratch pin"
    );
    assert!(
        store.background.protects(source_name),
        "cleanup failure must retain the source pin"
    );
}

#[test]
fn missing_owned_scratch_cleanup_releases_both_pins() {
    let directory = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(directory.path()).unwrap();
    let (_, scratch) = store.begin_next_generation(1).unwrap();
    let scratch_path = scratch.path().to_path_buf();
    let scratch_name = scratch_path
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    let source_name = "gen-0-rev-0";
    store.background.pin(scratch_name.clone());
    store.background.pin(source_name.to_owned());

    std::fs::remove_dir_all(&scratch_path).unwrap();
    store
        .cleanup_owned_merge_staging(&scratch_path, &scratch_name, source_name, scratch)
        .expect("an already-absent owned scratch is equivalent to prior absence");

    assert!(
        !store.background.protects(&scratch_name),
        "successful cleanup must release the scratch pin"
    );
    assert!(
        !store.background.protects(source_name),
        "successful cleanup must release the source pin"
    );
}
