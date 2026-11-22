use super::*;

fn quick() -> ScanOptions {
    ScanOptions {
        exact: false,
        audit: AuditOptions {
            sample: 0,
            priority: 0,
            ..AuditOptions::default()
        },
    }
}

#[test]
fn partial_directory_checkpoints_resume_without_restatting_saved_files() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("root");
    fs::create_dir(&root).unwrap();
    for i in 0..300 {
        fs::write(root.join(format!("file-{i:04}")), b"data").unwrap();
    }
    let cache = Cache::new(&root, Some(&temp.path().join("cache")), false).unwrap();
    let lock = cache.lock().unwrap();
    let loaded = journal::load(&cache).unwrap();
    let mut log = Journal::open(&cache, &loaded).unwrap();
    let mut tree = loaded.tree;
    let info = Info::from(&fs::symlink_metadata(&root).unwrap());
    append(
        &mut tree,
        &mut log,
        Event::Start {
            root: root.as_os_str().as_bytes().to_vec(),
            info,
        },
    )
    .unwrap();
    append(&mut tree, &mut log, Event::Begin { directory: 1, info }).unwrap();
    let entries = (0..256)
        .map(|i| {
            let name = format!("file-{i:04}").into_bytes().into_boxed_slice();
            let info =
                Info::from(&fs::symlink_metadata(root.join(OsStr::from_bytes(&name))).unwrap());
            RawEntry {
                name,
                info,
                directory: 0,
                flags: 0,
            }
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    append(
        &mut tree,
        &mut log,
        Event::Entries {
            directory: 1,
            chunk: Arc::new(Chunk { start: 0, entries }),
        },
    )
    .unwrap();
    drop(log);
    drop(lock);
    let mut options = quick();
    options.exact = true;
    let resumed = scan(
        cache.clone(),
        &options,
        2,
        Arc::new(Progress::default()),
        None,
    )
    .unwrap();
    assert!(resumed.resumed);
    assert!(resumed.report.complete);
    assert_eq!(resumed.report.counters.files_statted, 44);
    assert_eq!(resumed.report.counters.reused_files, 256);
    assert_eq!(resumed.report.totals.entries, 301);
    let sizes = resumed.report.totals;
    drop(resumed);
    assert_eq!(cached(&cache).unwrap().dir(1).unwrap().totals, sizes);
}

#[test]
fn truncated_checkpoint_tail_keeps_the_committed_prefix() {
    use std::io::{Seek, SeekFrom, Write};
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("root");
    fs::create_dir(&root).unwrap();
    fs::write(root.join("file"), b"contents").unwrap();
    let cache = Cache::new(&root, Some(&temp.path().join("cache")), false).unwrap();
    let original = scan(
        cache.clone(),
        &quick(),
        1,
        Arc::new(Progress::default()),
        None,
    )
    .unwrap();
    let expected = original.report.totals;
    drop(original);
    let loaded = journal::load(&cache).unwrap();
    let mut file = OpenOptions::new()
        .write(true)
        .open(
            cache
                .directory
                .join(format!("scan-{:08}.bin", loaded.segment)),
        )
        .unwrap();
    file.seek(SeekFrom::Start(loaded.offset as u64)).unwrap();
    file.write_all(&100000u32.to_le_bytes()).unwrap();
    file.write_all(b"incomplete checkpoint").unwrap();
    drop(file);
    assert_eq!(cached(&cache).unwrap().dir(1).unwrap().totals, expected);
    let resumed = scan(cache, &quick(), 1, Arc::new(Progress::default()), None).unwrap();
    assert_eq!(resumed.report.totals, expected);
    assert_eq!(resumed.report.counters.files_statted, 0);
}

#[test]
fn an_unwritten_tail_segment_can_be_resumed() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("root");
    fs::create_dir(&root).unwrap();
    fs::write(root.join("file"), b"contents").unwrap();
    let cache = Cache::new(&root, Some(&temp.path().join("cache")), false).unwrap();
    let original = scan(
        cache.clone(),
        &quick(),
        1,
        Arc::new(Progress::default()),
        None,
    )
    .unwrap();
    let expected = original.report.totals;
    drop(original);
    File::create(cache.directory.join("scan-00000001.bin"))
        .unwrap()
        .set_len(4 * 1024 * 1024)
        .unwrap();
    let resumed = scan(cache, &quick(), 1, Arc::new(Progress::default()), None).unwrap();
    assert_eq!(resumed.report.totals, expected);
    assert_eq!(resumed.report.counters.files_statted, 0);
}
