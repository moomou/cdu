use super::*;
use crate::{fast, scan};
use std::fs;

#[test]
fn interrupted_indexing_resumes_after_its_last_committed_batch() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("root");
    fs::create_dir(&root).unwrap();
    for at in 0..700 {
        fs::write(root.join(format!("file-{at}")), b"contents").unwrap();
    }
    let cache = Cache::new(&root, Some(&temp.path().join("cache")), false).unwrap();
    let scanned = fast::scan(
        cache.clone(),
        &scan::Options::default(),
        2,
        Arc::new(scan::Progress::default()),
        None,
    )
    .unwrap();
    let mut index_cache = cache.clone();
    index_cache.database = cache.directory.join("lookup.sqlite3");
    let mut store = Store::shared(&index_cache, scanned.lock.clone()).unwrap();
    store
        .conn
        .execute_batch(
            "CREATE TABLE directory_index(id INTEGER PRIMARY KEY, signature BLOB NOT NULL,
         phase INTEGER NOT NULL, next_entry INTEGER NOT NULL);",
        )
        .unwrap();
    let cancel = Arc::new(AtomicBool::new(false));
    let cancellation = cancel.clone();
    let mut commits = 0;
    store
        .conn
        .commit_hook(Some(move || {
            commits += 1;
            if commits == 1 {
                cancellation.store(true, Ordering::Relaxed);
            }
            false
        }))
        .unwrap();
    let first = build(&store, &cache, &scanned.tree, &cancel, &Progress::default()).unwrap();
    assert!(!first.complete);
    assert_eq!(first.rows_written, BATCH as u64);
    drop(store);
    let resumed = run(
        &cache,
        &scanned.tree,
        scanned.lock.clone(),
        Arc::new(AtomicBool::new(false)),
        Arc::new(Progress::default()),
    )
    .unwrap();
    assert!(resumed.complete);
    assert_eq!(resumed.resumed_rows, BATCH as u64);
    assert_eq!(resumed.rows_written, 700 - BATCH as u64);
    let unchanged = run(
        &cache,
        &scanned.tree,
        scanned.lock,
        Arc::new(AtomicBool::new(false)),
        Arc::new(Progress::default()),
    )
    .unwrap();
    assert!(unchanged.complete);
    assert_eq!(unchanged.rows_written, 0);
    assert_eq!(unchanged.resumed_rows, 700);
}
