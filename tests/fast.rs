use cdu::{
    ListOptions,
    cache::Cache,
    estimate::AuditOptions,
    fast,
    model::ROOT,
    scan::{Options, Progress},
};
use std::{fs, os::unix::fs::symlink, process::Command, sync::Arc};

fn quick() -> Options {
    Options {
        exact: false,
        audit: AuditOptions {
            sample: 0,
            priority: 0,
            ..AuditOptions::default()
        },
    }
}

#[test]
fn foreground_tree_matches_du_and_reuses_checkpoints() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("tree");
    fs::create_dir_all(root.join("a/deep")).unwrap();
    fs::create_dir(root.join("b")).unwrap();
    fs::write(root.join("a/deep/data"), b"shared data").unwrap();
    fs::hard_link(root.join("a/deep/data"), root.join("b/link")).unwrap();
    symlink("a", root.join("symlink")).unwrap();
    let cache = Cache::new(&root, Some(&temp.path().join("cache")), false).unwrap();
    let first = fast::scan(
        cache.clone(),
        &quick(),
        2,
        Arc::new(Progress::default()),
        None,
    )
    .unwrap();
    assert!(first.report.complete, "{:?}", first.report);
    assert!(first.report.settled);
    for (apparent, value) in [
        (false, first.report.totals.disk_bytes),
        (true, first.report.totals.apparent_bytes),
    ] {
        let mut command = Command::new("du");
        command.args(["--summarize", "--block-size=1"]);
        if apparent {
            command.arg("--apparent-size");
        }
        let output = command.arg(&root).output().unwrap();
        let expected: u64 = String::from_utf8(output.stdout)
            .unwrap()
            .split_whitespace()
            .next()
            .unwrap()
            .parse()
            .unwrap();
        assert_eq!(value, expected);
    }
    let total = first.report.totals;
    drop(first);
    let second = fast::scan(
        cache.clone(),
        &quick(),
        2,
        Arc::new(Progress::default()),
        None,
    )
    .unwrap();
    assert_eq!(second.report.counters.directories_enumerated, 0);
    assert_eq!(second.report.counters.files_statted, 0);
    assert_eq!(second.report.totals, total);
    assert_eq!(
        second
            .tree
            .snapshot(ROOT, &ListOptions::default())
            .unwrap()
            .children,
        3
    );
    drop(second);
    assert_eq!(
        fast::cached(&cache)
            .unwrap()
            .snapshot(ROOT, &ListOptions::default())
            .unwrap()
            .totals,
        total
    );
}

#[test]
fn foreground_changes_only_enumerate_affected_directories() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("tree");
    fs::create_dir_all(root.join("a")).unwrap();
    fs::create_dir(root.join("b")).unwrap();
    fs::write(root.join("a/first"), b"one").unwrap();
    let cache = Cache::new(&root, Some(&temp.path().join("cache")), false).unwrap();
    drop(
        fast::scan(
            cache.clone(),
            &quick(),
            2,
            Arc::new(Progress::default()),
            None,
        )
        .unwrap(),
    );
    std::thread::sleep(std::time::Duration::from_millis(5));
    fs::write(root.join("a/second"), b"two").unwrap();
    let changed = fast::scan(
        cache.clone(),
        &quick(),
        2,
        Arc::new(Progress::default()),
        None,
    )
    .unwrap();
    assert!(changed.report.complete);
    assert_eq!(changed.report.counters.directories_enumerated, 1);
    assert_eq!(changed.report.counters.files_statted, 2);
}

fn assert_usage(tree: &fast::Tree) {
    let snapshot = tree.snapshot(ROOT, &ListOptions::default()).unwrap();
    for (apparent, size) in [
        (false, snapshot.totals.disk_bytes),
        (true, snapshot.totals.apparent_bytes),
    ] {
        let mut command = Command::new("du");
        command.args(["--summarize", "--block-size=1"]);
        if apparent {
            command.arg("--apparent-size");
        }
        let result = command.arg(&tree.root).output().unwrap();
        assert!(result.status.success());
        let reference: u64 = String::from_utf8(result.stdout)
            .unwrap()
            .split_whitespace()
            .next()
            .unwrap()
            .parse()
            .unwrap();
        assert_eq!(size, reference);
    }
}

#[test]
fn checkpoint_refresh_handles_moves_deletion_sparse_files_and_raw_names() {
    use std::{ffi::OsString, os::unix::ffi::OsStringExt};
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("tree");
    fs::create_dir_all(root.join("a/deep")).unwrap();
    fs::create_dir(root.join("b")).unwrap();
    fs::write(root.join("a/deep/file"), b"shared").unwrap();
    fs::hard_link(root.join("a/deep/file"), root.join("b/alias")).unwrap();
    fs::File::create(root.join("sparse"))
        .unwrap()
        .set_len(1024 * 1024)
        .unwrap();
    let raw = b"raw-\xff-\x1b[2J".to_vec();
    fs::write(root.join(OsString::from_vec(raw.clone())), b"raw").unwrap();
    let cache = Cache::new(&root, Some(&temp.path().join("cache")), false).unwrap();
    let initial = fast::scan(
        cache.clone(),
        &quick(),
        2,
        Arc::new(Progress::default()),
        None,
    )
    .unwrap();
    assert_usage(&initial.tree);
    let raw_entry = initial
        .tree
        .snapshot(ROOT, &ListOptions::default())
        .unwrap()
        .entries
        .into_iter()
        .find(|entry| entry.name_bytes == raw)
        .unwrap();
    assert!(!raw_entry.name.contains('\x1b'));
    drop(initial);
    std::thread::sleep(std::time::Duration::from_millis(5));
    fs::rename(root.join("a"), root.join("b/moved")).unwrap();
    let moved = fast::scan(
        cache.clone(),
        &quick(),
        2,
        Arc::new(Progress::default()),
        None,
    )
    .unwrap();
    assert!(moved.report.complete);
    assert_usage(&moved.tree);
    drop(moved);
    std::thread::sleep(std::time::Duration::from_millis(5));
    fs::remove_dir_all(root.join("b/moved")).unwrap();
    let removed = fast::scan(cache, &quick(), 2, Arc::new(Progress::default()), None).unwrap();
    assert!(removed.report.complete);
    assert_usage(&removed.tree);
}

#[test]
fn checkpoint_audits_detect_existing_file_growth() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("tree");
    fs::create_dir(&root).unwrap();
    fs::write(root.join("file"), b"old").unwrap();
    let cache = Cache::new(&root, Some(&temp.path().join("cache")), false).unwrap();
    drop(
        fast::scan(
            cache.clone(),
            &quick(),
            2,
            Arc::new(Progress::default()),
            None,
        )
        .unwrap(),
    );
    fs::write(root.join("file"), [5; 20000]).unwrap();
    let audited = fast::scan(
        cache,
        &Options::default(),
        2,
        Arc::new(Progress::default()),
        None,
    )
    .unwrap();
    assert_usage(&audited.tree);
    assert_eq!(audited.report.counters.directories_enumerated, 0);
}

#[test]
fn lazy_path_checks_still_exclude_an_in_tree_cache() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("tree");
    fs::create_dir_all(root.join("nested")).unwrap();
    fs::write(root.join("nested/file"), b"contents").unwrap();
    let cache = Cache::new(&root, Some(&root.join("cache")), false).unwrap();
    let first = fast::scan(
        cache.clone(),
        &quick(),
        2,
        Arc::new(Progress::default()),
        None,
    )
    .unwrap();
    assert!(first.report.complete);
    assert_eq!(first.report.totals.excluded, 1);
    drop(first);
    let again = fast::scan(cache, &quick(), 2, Arc::new(Progress::default()), None).unwrap();
    assert!(again.report.complete);
    assert_eq!(again.report.counters.directories_enumerated, 0);
    assert_eq!(again.report.totals.excluded, 1);
}
