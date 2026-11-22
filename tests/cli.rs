use serde_json::Value;
use std::{fs, path::Path, process::Command};

fn command(root: &Path, cache: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_cdu"));
    command.arg(root).arg("--cache-dir").arg(cache);
    command
}

#[test]
fn separate_processes_reuse_the_index_without_a_daemon() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("root");
    let cache = temp.path().join("cache");
    fs::create_dir(&root).unwrap();
    fs::write(root.join("file"), "one").unwrap();
    for mode in ["full", "incremental"] {
        let output = command(&root, &cache)
            .args(["--quick", "--json", "--summary"])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let snapshot: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(snapshot["report"]["mode"], mode);
        if mode == "incremental" {
            assert_eq!(snapshot["report"]["counters"]["directories_enumerated"], 0);
            assert_eq!(snapshot["report"]["counters"]["files_statted"], 0);
        }
    }
    let output = command(&root, &cache)
        .args(["--cached", "--json"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let snapshot: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(snapshot["report"]["mode"], "cached");
    assert!(snapshot["report"]["audit"].is_null());
    assert_eq!(snapshot["entries"][0]["name"], "file");
}

#[test]
fn an_unestablished_error_target_has_a_distinct_exit_status() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("root");
    let cache = temp.path().join("cache");
    fs::create_dir(&root).unwrap();
    for i in 0..10 {
        fs::write(root.join(format!("file-{i}")), "one").unwrap();
    }
    assert!(
        command(&root, &cache)
            .args(["--quick", "--summary"])
            .output()
            .unwrap()
            .status
            .success()
    );
    let output = command(&root, &cache)
        .args([
            "--json",
            "--summary",
            "--priority",
            "0",
            "--sample",
            "0",
            "--max-delta-bytes",
            "1MiB",
            "--error-bytes",
            "1KiB",
        ])
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(2),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let snapshot: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(snapshot["report"]["audit"]["disk"]["interval"].is_null());
}

#[test]
fn invalid_confidence_and_byte_limits_are_rejected() {
    for args in [
        vec!["--confidence", "NaN", "--summary"],
        vec!["--confidence", "1", "--summary"],
        vec!["--max-delta-bytes", "18446744073709551615TiB"],
        vec!["--error-bytes", "1KiB"],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_cdu"))
            .args(args)
            .output()
            .unwrap();
        assert!(!output.status.success());
    }
}

#[test]
fn terminal_navigation_and_cancellation_restore_the_terminal() {
    let output = Command::new("python3")
        .arg(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/terminal.py"))
        .env("CDU_TEST_BINARY", env!("CARGO_BIN_EXE_cdu"))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
