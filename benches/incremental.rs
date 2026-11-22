use anyhow::{Result, ensure};
use cdu::{
    ListOptions,
    cache::Cache,
    estimate::AuditOptions,
    model::{ROOT, Report},
    read_snapshot,
    scan::{Options, Scanner},
};
use rand::seq::SliceRandom;
use std::{
    ffi::OsString,
    fs,
    path::Path,
    process::{Command, Stdio},
    time::Instant,
};

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

fn invocation(cache: &Cache, options: &Options) -> Result<(u64, Report)> {
    let start = Instant::now();
    let report = Scanner::open(cache.clone())?.refresh(options)?;
    read_snapshot(
        cache,
        ROOT,
        &ListOptions {
            limit: 0,
            ..ListOptions::default()
        },
    )?;
    Ok((start.elapsed().as_micros() as u64, report))
}

fn repeated(label: &str, cache: &Cache, options: &Options, rounds: usize) -> Result<()> {
    let mut samples = Vec::new();
    let mut last = None;
    for _ in 0..rounds {
        let (elapsed, report) = invocation(cache, options)?;
        samples.push(elapsed);
        last = Some(report);
    }
    samples.sort_unstable();
    let last = last.unwrap();
    println!(
        "{}",
        serde_json::json!({
            "case": label,
            "rounds": rounds,
            "p50_us": samples[samples.len() / 2],
            "p95_us": samples[(samples.len() * 95 / 100).min(samples.len() - 1)],
            "directories_checked": last.counters.directories_checked,
            "directories_enumerated": last.counters.directories_enumerated,
            "files_statted": last.counters.files_statted,
            "rows_written": last.counters.rows_written,
        })
    );
    Ok(())
}

fn tree(path: &Path, files: usize, directories: usize) -> Result<()> {
    fs::create_dir(path)?;
    for dir in 0..directories {
        fs::create_dir(path.join(format!("d{dir:04}")))?;
    }
    for file in 0..files {
        let parent = if directories == 0 {
            path.to_path_buf()
        } else {
            path.join(format!("d{:04}", file % directories))
        };
        fs::write(
            parent.join(format!("f{file:08}")),
            b"synthetic metadata benchmark",
        )?;
    }
    Ok(())
}

fn comparisons(cache: &Cache, root: &Path, rounds: usize) -> Result<()> {
    let mut commands: Vec<(&str, OsString, Vec<OsString>)> = vec![
        (
            "du",
            "du".into(),
            vec!["--summarize".into(), "--block-size=1".into(), root.into()],
        ),
        (
            "ncdu_scan_export",
            "ncdu".into(),
            vec!["-0".into(), "-o".into(), "/dev/null".into(), root.into()],
        ),
    ];
    if let Some(binary) = std::env::var_os("CDU_BENCH_BINARY")
        .or_else(|| option_env!("CARGO_BIN_EXE_cdu").map(Into::into))
    {
        for (name, flag) in [
            ("cdu_cached", Some("--cached")),
            ("cdu_quick", Some("--quick")),
            ("cdu_default", None),
        ] {
            let mut args: Vec<OsString> = vec![
                root.into(),
                "--cache-dir".into(),
                cache.base.as_os_str().into(),
                "--summary".into(),
                "--json".into(),
            ];
            if let Some(flag) = flag {
                args.push(flag.into());
            }
            commands.push((name, binary.clone(), args));
        }
    }
    let mut samples = vec![Vec::new(); commands.len()];
    let mut missing = vec![false; commands.len()];
    let mut order: Vec<_> = (0..commands.len()).collect();
    let mut random = rand::rng();
    for _ in 0..rounds {
        order.shuffle(&mut random);
        for &index in &order {
            if missing[index] {
                continue;
            }
            let (name, program, args) = &commands[index];
            let start = Instant::now();
            let status = match Command::new(program)
                .args(args)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
            {
                Ok(status) => status,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    missing[index] = true;
                    continue;
                }
                Err(error) => return Err(error.into()),
            };
            ensure!(status.success(), "{name} benchmark failed");
            samples[index].push(start.elapsed().as_micros() as u64);
        }
    }
    for ((name, _, _), mut samples) in commands.into_iter().zip(samples) {
        if samples.is_empty() {
            continue;
        }
        samples.sort_unstable();
        println!(
            "{}",
            serde_json::json!({
                "case": format!("{name}_cli_interleaved"),
                "p50_us": samples[rounds / 2],
                "p95_us": samples[(rounds * 95 / 100).min(rounds - 1)],
                "rounds": rounds,
            })
        );
    }
    Ok(())
}

fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    let number = |name: &str, default: usize| -> Result<usize> {
        args.windows(2)
            .find(|pair| pair[0] == name)
            .map(|pair| pair[1].parse().map_err(Into::into))
            .unwrap_or(Ok(default))
    };
    let files = number("--files", 20_000)?;
    let directories = number("--dirs", 200)?;
    let rounds = number("--rounds", 20)?;
    ensure!(
        files > 0 && directories > 0 && rounds > 0,
        "counts must be positive"
    );
    let temp = tempfile::tempdir()?;
    let root = temp.path().join("tree");
    tree(&root, files, directories)?;
    let cache = Cache::new(&root, Some(&temp.path().join("cache")), false)?;
    let (cold, report) = invocation(&cache, &quick())?;
    ensure!(
        report.counters.files_statted == files as u64,
        "cold scan missed files"
    );
    println!(
        "{}",
        serde_json::json!({
            "case": "first_index_os_cache_warm", "files": files, "directories": directories + 1,
            "elapsed_us": cold, "database_bytes": fs::metadata(&cache.database)?.len(),
        })
    );
    repeated("warm_directory_only", &cache, &quick(), rounds)?;
    repeated("warm_default_audit", &cache, &Options::default(), rounds)?;
    let mut cached = Vec::new();
    for _ in 0..rounds {
        let start = Instant::now();
        read_snapshot(
            &cache,
            ROOT,
            &ListOptions {
                limit: 0,
                ..ListOptions::default()
            },
        )?;
        cached.push(start.elapsed().as_micros() as u64);
    }
    cached.sort_unstable();
    println!(
        "{}",
        serde_json::json!({
            "case": "cached_snapshot_only",
            "p50_us": cached[rounds / 2],
            "p95_us": cached[(rounds * 95 / 100).min(rounds - 1)],
            "rounds": rounds,
        })
    );
    comparisons(&cache, &root, rounds)?;
    fs::write(root.join("d0000/new-file"), b"one directory changes")?;
    let (elapsed, report) = invocation(&cache, &quick())?;
    ensure!(
        report.counters.directories_enumerated == 1,
        "unrelated directories were enumerated"
    );
    println!(
        "{}",
        serde_json::json!({
            "case": "one_directory_changed", "elapsed_us": elapsed,
            "directories_enumerated": report.counters.directories_enumerated,
            "files_statted": report.counters.files_statted,
        })
    );
    let (elapsed, _) = invocation(
        &cache,
        &Options {
            exact: true,
            ..quick()
        },
    )?;
    println!(
        "{}",
        serde_json::json!({"case": "full_verification", "elapsed_us": elapsed})
    );
    let wide_root = temp.path().join("wide");
    tree(&wide_root, files, 0)?;
    let wide_cache = Cache::new(&wide_root, Some(&temp.path().join("cache")), false)?;
    invocation(&wide_cache, &quick())?;
    repeated("warm_wide_directory_only", &wide_cache, &quick(), rounds)?;
    Ok(())
}
