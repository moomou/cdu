use anyhow::Result;
use cdu::{
    ListOptions, Sort,
    cache::Cache,
    estimate::AuditOptions,
    fast,
    model::{ROOT, display_name},
    read_snapshot,
    scan::{Options, Progress},
    ui::human_bytes,
};
use clap::{Parser, ValueEnum};
use std::{
    ffi::OsStr,
    io::{self, IsTerminal, Write},
    path::PathBuf,
    process::ExitCode,
    sync::Arc,
    time::Instant,
};

#[derive(Parser)]
#[command(
    version,
    about = "Cached disk usage: an ncdu-style browser without a daemon or watcher",
    after_help = "Default refreshes check directory signatures and sample file metadata. Unchecked file sizes remain cached. Use --exact for a full metadata scan. No file contents are read."
)]
struct Cli {
    #[arg(default_value = ".")]
    path: PathBuf,
    /// Start a full metadata scan, or resume an interrupted scan
    #[arg(long, alias = "rescan", conflicts_with = "cached")]
    exact: bool,
    /// Browse only the last committed cache; perform no scan
    #[arg(long)]
    cached: bool,
    /// Directory checks only; skip both prioritized and random file audits
    #[arg(long, conflicts_with_all = ["exact", "cached", "error_bytes"])]
    quick: bool,
    /// Maximum number of uniformly sampled file identities
    #[arg(long, default_value_t = 512)]
    sample: usize,
    /// Check this many largest cached files before random sampling
    #[arg(long, default_value_t = 32)]
    priority: usize,
    /// Confidence level for each reported statistical bound
    #[arg(long, default_value_t = 0.95)]
    confidence: f64,
    /// ASSUME every audit-population inode changes by at most this many bytes in either metric
    #[arg(long, value_parser = parse_bytes)]
    max_delta_bytes: Option<u64>,
    /// Required absolute error; exit 2 if the selected metric's conditional bound is not met
    #[arg(long, value_parser = parse_bytes, requires = "max_delta_bytes", conflicts_with = "cached")]
    error_bytes: Option<u64>,
    /// Do not cross filesystem boundaries
    #[arg(short = 'x', long)]
    one_file_system: bool,
    /// Show logical size instead of allocated disk blocks
    #[arg(short = 'a', long)]
    apparent_size: bool,
    /// Emit a machine-readable snapshot and scan/audit counters
    #[arg(long)]
    json: bool,
    /// Omit child entries from non-interactive output
    #[arg(long)]
    summary: bool,
    /// Print a table instead of opening the terminal browser
    #[arg(long)]
    no_ui: bool,
    /// Maximum child entries in non-interactive output
    #[arg(long, default_value_t = 200)]
    limit: usize,
    #[arg(long, value_enum, default_value = "size")]
    sort: SortArg,
    #[arg(long)]
    hide_hidden: bool,
    /// Enable permanent deletion in the UI, with an explicit confirmation
    #[arg(long, conflicts_with_all = ["read_only", "cached"])]
    enable_delete: bool,
    /// Read-only browser (the default; accepted for ncdu compatibility)
    #[arg(short = 'r', long)]
    read_only: bool,
    /// Private cache base (also configurable with CDU_CACHE_DIR)
    #[arg(long)]
    cache_dir: Option<PathBuf>,
    /// Print the cache directory's absolute path and exit
    #[arg(long)]
    cache_path: bool,
    /// Bounded filesystem-discovery parallelism
    #[arg(long, default_value_t = 2)]
    threads: usize,
    /// Write first full-size browser frame, input response, and indexing timings on exit
    #[arg(long)]
    timings_json: Option<PathBuf>,
}

#[derive(Clone, Copy, ValueEnum)]
enum SortArg {
    Size,
    Name,
    Count,
}

fn main() -> ExitCode {
    let started = Instant::now();
    let cli = Cli::parse();
    match run(cli, started) {
        Ok(code) => ExitCode::from(code),
        Err(error) => {
            if error
                .downcast_ref::<io::Error>()
                .is_some_and(|error| error.kind() == io::ErrorKind::BrokenPipe)
            {
                return ExitCode::SUCCESS;
            }
            eprintln!("cdu: {}", display_name(OsStr::new(&format!("{error:#}"))));
            ExitCode::FAILURE
        }
    }
}

fn run(cli: Cli, started: Instant) -> Result<u8> {
    let cache = Cache::new(&cli.path, cli.cache_dir.as_deref(), cli.one_file_system)?;
    if cli.cache_path {
        println!("{}", cache.directory.display());
        return Ok(0);
    }
    let options = Options {
        exact: cli.exact,
        audit: AuditOptions {
            sample: if cli.quick { 0 } else { cli.sample },
            priority: if cli.quick { 0 } else { cli.priority },
            confidence: cli.confidence,
            max_delta_bytes: cli.max_delta_bytes,
            error_bytes: cli.error_bytes,
        },
    };
    options.audit.validate()?;
    let listing = ListOptions {
        apparent: cli.apparent_size,
        hidden: !cli.hide_hidden,
        limit: if cli.summary { 0 } else { cli.limit },
        sort: match cli.sort {
            SortArg::Size => Sort::Size,
            SortArg::Name => Sort::Name,
            SortArg::Count => Sort::Count,
        },
        descending: !matches!(cli.sort, SortArg::Name),
        ..ListOptions::default()
    };
    if !cli.json
        && !cli.summary
        && !cli.no_ui
        && cli.error_bytes.is_none()
        && io::stdin().is_terminal()
        && io::stdout().is_terminal()
    {
        cdu::ui::run(
            cache,
            options,
            listing,
            cli.cached,
            cli.enable_delete,
            cdu::ui::Runtime {
                threads: cli.threads,
                started,
                timing_path: cli.timings_json,
            },
        )?;
        return Ok(0);
    }
    let mut snapshot = if cli.cached {
        if fast::has_checkpoint(&cache) {
            fast::cached(&cache)?.snapshot(ROOT, &listing)?
        } else {
            read_snapshot(&cache, ROOT, &listing)?
        }
    } else {
        let result = fast::scan(
            cache.clone(),
            &options,
            cli.threads,
            Arc::new(Progress::default()),
            None,
        )?;
        let snapshot = result.tree.snapshot(ROOT, &listing)?;
        if let Some(path) = cli.timings_json {
            let timings = cdu::ui::Timings {
                scan_complete_us: Some(started.elapsed().as_micros() as u64),
                complete: result.report.complete,
                resumed_scan: result.resumed,
                totals: Some(result.report.totals),
                scan_counters: Some(result.report.counters),
                ..cdu::ui::Timings::default()
            };
            std::fs::write(path, serde_json::to_vec_pretty(&timings)?)?;
        }
        snapshot
    };
    if cli.cached
        && let Some(report) = &mut snapshot.report
    {
        report.mode = "cached".to_owned();
        report.reason = "no filesystem validation requested".to_owned();
        report.audit = None;
        report.warning = Some(
            "Unverified cached snapshot; sizes may have changed since this index was saved."
                .to_owned(),
        );
    }
    let mut stdout = io::stdout().lock();
    if cli.json {
        serde_json::to_writer(&mut stdout, &snapshot)?;
        writeln!(stdout)?;
    } else {
        let size = if cli.apparent_size {
            snapshot.totals.apparent_bytes
        } else {
            snapshot.totals.disk_bytes
        };
        let state = snapshot
            .report
            .as_ref()
            .map_or("cached", |report| report.mode.as_str());
        writeln!(
            stdout,
            "{}  {}  [{state}; recorded total]",
            human_bytes(size),
            snapshot.path
        )?;
        for entry in &snapshot.entries {
            let size = if cli.apparent_size {
                entry.totals.apparent_bytes
            } else {
                entry.totals.disk_bytes
            };
            let flag = if entry.error.is_some() {
                "!"
            } else if entry.excluded.is_some() {
                "<"
            } else if entry.hard_link {
                "H"
            } else {
                " "
            };
            let suffix = if entry.kind == cdu::model::Kind::Directory {
                "/"
            } else {
                ""
            };
            writeln!(
                stdout,
                "{flag} {:>10}  {}{suffix}",
                human_bytes(size),
                entry.name
            )?;
        }
        if snapshot.children > snapshot.entries.len() && !cli.summary {
            writeln!(
                stdout,
                "... {} more entries (use --limit)",
                snapshot.children - snapshot.entries.len()
            )?;
        }
        if let Some(report) = &snapshot.report {
            if let Some(audit) = &report.audit {
                let estimate = if cli.apparent_size {
                    &audit.apparent
                } else {
                    &audit.disk
                };
                write!(
                    stdout,
                    "Estimated total: {}",
                    human_bytes(estimate.bytes as u64)
                )?;
                if let Some(interval) = &estimate.interval {
                    writeln!(
                        stdout,
                        "  [{}, {}], conditional {:.1}% interval",
                        human_bytes(interval.lower_bytes as u64),
                        human_bytes(interval.upper_bytes as u64),
                        interval.confidence * 100.0
                    )?;
                } else {
                    writeln!(
                        stdout,
                        "  (byte-error bound not established; {})",
                        audit.status
                    )?;
                }
            }
            if let Some(warning) = &report.warning {
                writeln!(stdout, "{warning}")?;
            }
        }
    }
    if cli.error_bytes.is_some() {
        let met = snapshot.report.as_ref().is_some_and(|report| {
            if report.mode == "full" {
                return report.complete && report.settled;
            }
            report.audit.as_ref().is_some_and(|audit| {
                let estimate = if cli.apparent_size {
                    &audit.apparent
                } else {
                    &audit.disk
                };
                estimate
                    .interval
                    .as_ref()
                    .is_some_and(|interval| interval.target_met == Some(true))
            })
        });
        if !met {
            return Ok(2);
        }
    }
    Ok(0)
}

fn parse_bytes(value: &str) -> Result<u64, String> {
    let split = value
        .find(|character: char| !character.is_ascii_digit())
        .unwrap_or(value.len());
    let number: u64 = value[..split]
        .parse()
        .map_err(|_| "expected a nonnegative integer byte count")?;
    let multiplier: u64 = match value[split..].to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "kib" | "k" => 1024,
        "mib" | "m" => 1024 * 1024,
        "gib" | "g" => 1024 * 1024 * 1024,
        "tib" | "t" => 1024_u64.pow(4),
        "kb" => 1000,
        "mb" => 1000 * 1000,
        "gb" => 1000 * 1000 * 1000,
        "tb" => 1000_u64.pow(4),
        _ => return Err("supported suffixes: B, KiB, MiB, GiB, TiB, KB, MB, GB, TB".to_owned()),
    };
    number
        .checked_mul(multiplier)
        .ok_or_else(|| "byte count is too large".to_owned())
}
