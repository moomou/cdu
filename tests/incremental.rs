use cdu::{
    ListOptions, Snapshot,
    cache::Cache,
    estimate::AuditOptions,
    model::{ROOT, Report},
    read_snapshot,
    scan::{Options, Progress, Scanner, delete_entry},
};
use std::{
    ffi::OsString,
    fs::{self, File, OpenOptions},
    io::Write,
    os::unix::{ffi::OsStringExt, fs::symlink},
    path::PathBuf,
    process::Command,
    sync::{Arc, atomic::Ordering},
    thread,
    time::Duration,
};
use tempfile::TempDir;

struct Fixture {
    temp: TempDir,
    root: PathBuf,
    cache: Cache,
}

impl Fixture {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("tree");
        fs::create_dir(&root).unwrap();
        let cache = Cache::new(&root, Some(&temp.path().join("cache")), false).unwrap();
        Self { temp, root, cache }
    }

    fn write(&self, path: &str, bytes: &[u8]) {
        let path = self.root.join(path);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, bytes).unwrap();
    }

    fn refresh(&self, options: Options) -> Report {
        let progress = Arc::new(Progress::default());
        let report = Scanner::with_progress(self.cache.clone(), progress.clone())
            .unwrap()
            .refresh(&options)
            .unwrap();
        assert!(progress.size_ready.load(Ordering::Acquire));
        assert_eq!(
            progress.disk_bytes.load(Ordering::Relaxed),
            report.totals.disk_bytes
        );
        assert_eq!(
            progress.apparent_bytes.load(Ordering::Relaxed),
            report.totals.apparent_bytes
        );
        report
    }

    fn snapshot(&self) -> Snapshot {
        read_snapshot(&self.cache, ROOT, &ListOptions::default()).unwrap()
    }

    fn assert_matches_du(&self, report: &Report) {
        assert!(report.complete, "{:?}", report.warning);
        assert!(report.settled, "{:?}", report.warning);
        assert_eq!(
            self.snapshot().children,
            fs::read_dir(&self.root).unwrap().count()
        );
        for (apparent, actual) in [
            (false, report.totals.disk_bytes),
            (true, report.totals.apparent_bytes),
        ] {
            let mut command = Command::new("du");
            command.args(["--summarize", "--block-size=1"]);
            if apparent {
                command.arg("--apparent-size");
            }
            let output = command.arg("--").arg(&self.root).output().unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            let expected: u64 = String::from_utf8_lossy(&output.stdout)
                .split_whitespace()
                .next()
                .unwrap()
                .parse()
                .unwrap();
            assert_eq!(actual, expected, "apparent={apparent}");
        }
    }
}

fn quick() -> Options {
    Options {
        audit: AuditOptions {
            sample: 0,
            priority: 0,
            ..AuditOptions::default()
        },
        ..Options::default()
    }
}

fn census() -> Options {
    Options {
        audit: AuditOptions {
            sample: usize::MAX,
            priority: 0,
            ..AuditOptions::default()
        },
        ..Options::default()
    }
}

fn tick() {
    thread::sleep(Duration::from_millis(3));
}

#[test]
fn cold_scan_counts_allocated_sparse_symlink_and_hardlink_sizes_like_du() {
    let fixture = Fixture::new();
    fixture.write("a/data", b"hello");
    fixture.write("b/other", b"some more bytes");
    fs::hard_link(fixture.root.join("a/data"), fixture.root.join("b/link")).unwrap();
    File::create(fixture.root.join("sparse"))
        .unwrap()
        .set_len(64 * 1024 * 1024)
        .unwrap();
    symlink("a/data", fixture.root.join("symlink")).unwrap();
    symlink("missing", fixture.root.join("broken")).unwrap();
    symlink(&fixture.root, fixture.root.join("cycle")).unwrap();
    let report = fixture.refresh(quick());
    assert_eq!(report.mode, "full");
    fixture.assert_matches_du(&report);
    assert_eq!(report.totals.entries, 10);
}

#[test]
fn reopening_the_cache_does_not_enumerate_or_stat_unchanged_files() {
    let fixture = Fixture::new();
    fixture.write("a/deep/file", b"one");
    fixture.write("b/file", b"two");
    fixture.refresh(quick());
    let modified = fs::metadata(&fixture.cache.database)
        .unwrap()
        .modified()
        .unwrap();
    let report = fixture.refresh(quick());
    assert_eq!(
        fs::metadata(&fixture.cache.database)
            .unwrap()
            .modified()
            .unwrap(),
        modified
    );
    assert_eq!(report.mode, "incremental");
    assert_eq!(report.counters.directories_checked, 4);
    assert_eq!(report.counters.directories_enumerated, 0);
    assert_eq!(report.counters.files_statted, 0);
    assert_eq!(report.counters.rows_written, 0);
    assert_eq!(report.audit.as_ref().unwrap().status, "unbounded");
    fixture.assert_matches_du(&report);
}

#[test]
fn adding_a_file_enumerates_only_its_parent() {
    let fixture = Fixture::new();
    fixture.write("a/deep/file", b"one");
    fixture.write("b/file", b"two");
    fixture.refresh(quick());
    tick();
    fixture.write("a/deep/new", b"three");
    let report = fixture.refresh(quick());
    assert_eq!(report.counters.directories_enumerated, 1);
    assert_eq!(report.counters.files_statted, 2);
    fixture.assert_matches_du(&report);
}

#[test]
fn quick_mode_does_not_pretend_to_detect_in_place_growth() {
    let fixture = Fixture::new();
    fixture.write("file", b"small");
    let before = fixture.refresh(quick());
    tick();
    fixture.write("file", &[7; 20000]);
    let quick = fixture.refresh(quick());
    assert_eq!(quick.totals, before.totals);
    assert!(quick.audit.unwrap().apparent.interval.is_none());
    let report = fixture.refresh(census());
    assert_eq!(report.counters.directories_enumerated, 0);
    assert_eq!(report.counters.files_statted, 1);
    assert_eq!(report.audit.as_ref().unwrap().status, "file_census");
    fixture.assert_matches_du(&report);
}

#[test]
fn a_uniform_delta_sample_estimates_unchecked_growth_without_double_counting() {
    let fixture = Fixture::new();
    for index in 0..20 {
        fixture.write(&format!("file-{index}"), &[1; 20]);
    }
    let before = fixture.refresh(quick());
    tick();
    for index in 0..20 {
        OpenOptions::new()
            .append(true)
            .open(fixture.root.join(format!("file-{index}")))
            .unwrap()
            .write_all(&[2; 10])
            .unwrap();
    }
    let report = fixture.refresh(Options {
        audit: AuditOptions {
            sample: 5,
            priority: 0,
            max_delta_bytes: Some(10),
            ..AuditOptions::default()
        },
        ..Options::default()
    });
    let audit = report.audit.unwrap();
    assert_eq!(audit.random_samples, 5);
    assert_eq!(audit.changed_samples, 5);
    assert_eq!(
        report.totals.apparent_bytes,
        before.totals.apparent_bytes + 50
    );
    let truth = before.totals.apparent_bytes as f64 + 200.0;
    assert_eq!(audit.apparent.bytes, truth);
    let interval = audit.apparent.interval.unwrap();
    assert!(interval.lower_bytes <= truth && interval.upper_bytes >= truth);
    assert!(interval.conditional);
}

#[test]
fn a_violated_growth_assumption_does_not_produce_a_byte_bound() {
    let fixture = Fixture::new();
    for index in 0..10 {
        fixture.write(&format!("file-{index}"), b"a");
    }
    fixture.refresh(quick());
    for index in 0..10 {
        fixture.write(&format!("file-{index}"), &[1; 500]);
    }
    let report = fixture.refresh(Options {
        audit: AuditOptions {
            sample: 3,
            priority: 0,
            max_delta_bytes: Some(5),
            ..AuditOptions::default()
        },
        ..Options::default()
    });
    let audit = report.audit.unwrap();
    assert_eq!(audit.status, "bound_violated");
    assert!(audit.apparent.interval.is_none());
    assert!(audit.disk.interval.is_none());
}

#[test]
fn moving_a_directory_preserves_its_descendant_index() {
    let fixture = Fixture::new();
    fixture.write("a/deep/file", b"data");
    fixture.write("b/other", b"other");
    fixture.refresh(quick());
    let before = fixture.snapshot();
    let old_a = before
        .entries
        .iter()
        .find(|entry| entry.name == "a")
        .unwrap()
        .id;
    tick();
    fs::rename(fixture.root.join("a"), fixture.root.join("b/moved")).unwrap();
    let report = fixture.refresh(quick());
    assert!(report.counters.directories_enumerated <= 3);
    fixture.assert_matches_du(&report);
    let moved = read_snapshot(&fixture.cache, old_a, &ListOptions::default()).unwrap();
    assert!(moved.path.ends_with("/b/moved"));
    assert_eq!(moved.entries[0].name, "deep");
}

#[test]
fn exchanging_directory_names_and_replacing_a_directory_do_not_leave_orphans() {
    let fixture = Fixture::new();
    fixture.write("a/deep/file", b"one");
    fixture.write("b/other", b"two-two");
    fixture.refresh(quick());
    tick();
    fs::rename(fixture.root.join("a"), fixture.root.join("temporary")).unwrap();
    fs::rename(fixture.root.join("b"), fixture.root.join("a")).unwrap();
    fs::rename(fixture.root.join("temporary"), fixture.root.join("b")).unwrap();
    fixture.assert_matches_du(&fixture.refresh(quick()));
    assert_eq!(fixture.snapshot().children, 2);
    tick();
    fs::rename(
        fixture.root.join("a"),
        fixture.temp.path().join("removed-a"),
    )
    .unwrap();
    fixture.write("a/new/deep", b"replacement");
    fixture.assert_matches_du(&fixture.refresh(quick()));
    assert_eq!(fixture.snapshot().children, 2);
}

#[test]
fn hardlink_owner_deletion_and_external_writes_keep_one_contribution() {
    let fixture = Fixture::new();
    fixture.write("a", b"old");
    fs::hard_link(fixture.root.join("a"), fixture.root.join("b")).unwrap();
    let outside = fixture.temp.path().join("external-link");
    fs::hard_link(fixture.root.join("a"), &outside).unwrap();
    fixture.refresh(quick());
    let owner = fixture
        .snapshot()
        .entries
        .into_iter()
        .find(|entry| !entry.hard_link)
        .unwrap();
    tick();
    fs::remove_file(fixture.root.join(OsString::from_vec(owner.name_bytes))).unwrap();
    fixture.assert_matches_du(&fixture.refresh(census()));
    fs::write(&outside, [4; 16000]).unwrap();
    let report = fixture.refresh(census());
    assert_eq!(report.audit.as_ref().unwrap().population_files, 1);
    fixture.assert_matches_du(&report);
}

#[test]
fn adding_a_hardlink_to_a_previously_single_link_file_does_not_duplicate_size() {
    let fixture = Fixture::new();
    fixture.write("a/file", b"initial");
    fixture.write("b/other", b"separate");
    fixture.refresh(quick());
    tick();
    fs::hard_link(fixture.root.join("a/file"), fixture.root.join("b/link")).unwrap();
    fixture.assert_matches_du(&fixture.refresh(census()));
    tick();
    fs::remove_dir_all(fixture.root.join("a")).unwrap();
    let report = fixture.refresh(census());
    fixture.assert_matches_du(&report);
    assert_eq!(report.audit.unwrap().population_files, 2);
}

#[test]
fn filenames_roundtrip_as_bytes_and_do_not_inject_terminal_controls() {
    let fixture = Fixture::new();
    let name = b"file-\xff-\x1b[2J-\n".to_vec();
    fs::write(
        fixture.root.join(OsString::from_vec(name.clone())),
        b"hello",
    )
    .unwrap();
    fixture.refresh(quick());
    let snapshot = fixture.snapshot();
    assert_eq!(snapshot.entries[0].name_bytes, name);
    assert!(!snapshot.entries[0].name.contains('\x1b'));
    assert!(!snapshot.entries[0].name.contains('\n'));
    fixture.assert_matches_du(&fixture.refresh(census()));
}

#[test]
fn root_replacement_triggers_a_fresh_index() {
    let fixture = Fixture::new();
    fixture.write("old", b"old");
    fixture.refresh(quick());
    fs::rename(&fixture.root, fixture.temp.path().join("previous-root")).unwrap();
    fs::create_dir(&fixture.root).unwrap();
    fixture.write("new", b"new contents");
    let report = fixture.refresh(quick());
    assert_eq!(report.mode, "full");
    assert_eq!(report.reason, "root identity changed");
    assert_eq!(fixture.snapshot().entries[0].name, "new");
    fixture.assert_matches_du(&report);
}

#[test]
fn cancellation_preserves_the_committed_cache_and_releases_the_writer_lock() {
    let fixture = Fixture::new();
    fixture.write("file", b"before");
    let before = fixture.refresh(quick());
    fixture.write("new", b"after");
    let progress = Arc::new(Progress::default());
    progress.cancel.store(true, Ordering::Relaxed);
    let mut scanner = Scanner::with_progress(fixture.cache.clone(), progress).unwrap();
    assert!(scanner.refresh(&quick()).is_err());
    assert_eq!(fixture.snapshot().totals, before.totals);
    assert!(Scanner::open(fixture.cache.clone()).is_err());
    drop(scanner);
    fixture.assert_matches_du(&fixture.refresh(quick()));
}

#[test]
fn deletion_refuses_the_root_and_a_replaced_selection() {
    let fixture = Fixture::new();
    fixture.write("file", b"before");
    fixture.refresh(quick());
    let selected = fixture.snapshot().entries.remove(0);
    assert!(delete_entry(&fixture.cache, ROOT, selected.identity).is_err());
    fs::rename(
        fixture.root.join("file"),
        fixture.temp.path().join("old-file"),
    )
    .unwrap();
    fixture.write("file", b"replacement");
    assert!(delete_entry(&fixture.cache, selected.id, selected.identity).is_err());
    assert!(fixture.root.join("file").exists());
}

#[test]
fn the_cache_excludes_itself_without_creating_perpetual_rescans() {
    let fixture = Fixture::new();
    fixture.write("file", b"data");
    let cache = Cache::new(&fixture.root, Some(&fixture.root.join("metadata")), false).unwrap();
    let first = Scanner::open(cache.clone())
        .unwrap()
        .refresh(&quick())
        .unwrap();
    assert_eq!(first.totals.excluded, 1);
    let second = Scanner::open(cache).unwrap().refresh(&quick()).unwrap();
    assert_eq!(second.counters.directories_enumerated, 0);
    assert_eq!(second.counters.files_statted, 0);
}

#[test]
fn repeated_add_remove_and_audit_operations_keep_the_sampling_pool_consistent() {
    let fixture = Fixture::new();
    fixture.write("keep/file", b"keep");
    fixture.refresh(quick());
    for round in 0..12 {
        tick();
        fixture.write(&format!("changes/file-{round}"), &[round; 17]);
        if round > 2 {
            fs::remove_file(fixture.root.join(format!("changes/file-{}", round - 3))).unwrap();
        }
        if round % 3 == 0 {
            fs::write(
                fixture.root.join("keep/file"),
                vec![round; usize::from(round) + 10],
            )
            .unwrap();
        }
        fixture.assert_matches_du(&fixture.refresh(census()));
    }
}

#[test]
fn explicit_exact_mode_ignores_directory_hints() {
    let fixture = Fixture::new();
    fixture.write("a/file", b"before");
    fixture.refresh(quick());
    fixture.write("a/file", &[2; 12000]);
    let report = fixture.refresh(Options {
        exact: true,
        ..quick()
    });
    assert_eq!(report.mode, "full");
    assert_eq!(report.counters.directories_enumerated, 2);
    fixture.assert_matches_du(&report);
}

#[test]
fn cache_paths_with_parent_components_are_excluded_by_their_real_location() {
    let fixture = Fixture::new();
    fs::create_dir(fixture.root.join("anchor")).unwrap();
    let base = fixture.root.join("anchor/../metadata");
    let cache = Cache::new(&fixture.root, Some(&base), false).unwrap();
    let report = Scanner::open(cache).unwrap().refresh(&quick()).unwrap();
    assert_eq!(report.totals.excluded, 1);
}

#[test]
fn reads_remain_available_while_a_writer_holds_the_cache_lock() {
    let fixture = Fixture::new();
    fixture.write("file", b"contents");
    let mut scanner = Scanner::open(fixture.cache.clone()).unwrap();
    scanner.refresh(&quick()).unwrap();
    assert_eq!(fixture.snapshot().entries[0].name, "file");
    assert!(Scanner::open(fixture.cache.clone()).is_err());
}

#[test]
fn unreadable_subtrees_are_marked_incomplete_and_retried() {
    use std::os::unix::fs::PermissionsExt;
    if unsafe { libc::geteuid() } == 0 {
        return;
    }
    struct Restore(PathBuf);
    impl Drop for Restore {
        fn drop(&mut self) {
            let _ = fs::set_permissions(&self.0, fs::Permissions::from_mode(0o700));
        }
    }
    let fixture = Fixture::new();
    fixture.write("private/file", b"contents");
    fixture.refresh(quick());
    let directory = fixture.root.join("private");
    let restore = Restore(directory.clone());
    fs::set_permissions(&directory, fs::Permissions::from_mode(0o000)).unwrap();
    let report = fixture.refresh(quick());
    drop(restore);
    assert!(!report.complete);
    assert!(report.totals.errors > 0);
    assert_eq!(report.audit.unwrap().status, "incomplete");
    fixture.assert_matches_du(&fixture.refresh(census()));
}

#[test]
fn an_older_index_is_upgraded_without_rescanning_files() {
    let fixture = Fixture::new();
    fixture.write("a/deep/file", b"contents");
    fixture.write("b", b"another");
    fixture.refresh(quick());
    let connection = rusqlite::Connection::open(&fixture.cache.database).unwrap();
    connection
        .execute_batch("ALTER TABLE nodes DROP COLUMN direct_children; PRAGMA user_version=1;")
        .unwrap();
    drop(connection);
    let report = fixture.refresh(quick());
    assert_eq!(report.counters.files_statted, 0);
    assert_eq!(report.counters.directories_enumerated, 0);
    fixture.assert_matches_du(&report);
}

#[test]
fn cold_bottom_up_totals_and_bulk_sampling_include_deep_hardlinks() {
    let fixture = Fixture::new();
    let mut directory = String::new();
    for depth in 0..60 {
        directory.push_str("level/");
        fixture.write(&format!("{directory}file-{depth}"), &[depth; 9]);
    }
    fs::hard_link(
        fixture.root.join(format!("{directory}file-59")),
        fixture.root.join("root-link"),
    )
    .unwrap();
    let cold = fixture.refresh(quick());
    fixture.assert_matches_du(&cold);
    let warm = fixture.refresh(census());
    fixture.assert_matches_du(&warm);
    assert_eq!(warm.audit.unwrap().population_files, 60);
    assert_eq!(warm.counters.directories_enumerated, 0);
}
