use crate::{
    ListOptions, Snapshot, Sort,
    cache::Cache,
    db::read_errors,
    fast::{self, Tree},
    indexing,
    model::{Entry, Id, Kind, ROOT, RecordedError, Report, display_name},
    read_snapshot,
    scan::{Options, Progress, delete_entry},
};
use anyhow::{Result, anyhow};
use crossterm::{
    cursor::{Hide, MoveTo, Show},
    event::{self, Event, KeyCode, KeyEventKind, KeyModifiers},
    execute, queue,
    style::{Attribute, Print, SetAttribute},
    terminal::{self, Clear, ClearType, EnterAlternateScreen, LeaveAlternateScreen},
};
use serde::Serialize;
use std::{
    ffi::{OsStr, OsString},
    io::{self, Write},
    os::unix::ffi::{OsStrExt, OsStringExt},
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
use unicode_width::UnicodeWidthChar;

struct Terminal;

impl Terminal {
    fn enter() -> Result<Self> {
        terminal::enable_raw_mode()?;
        let guard = Self;
        execute!(io::stdout(), EnterAlternateScreen, Hide)?;
        Ok(guard)
    }
}

impl Drop for Terminal {
    fn drop(&mut self) {
        let _ = execute!(
            io::stdout(),
            SetAttribute(Attribute::Reset),
            Show,
            LeaveAlternateScreen
        );
        let _ = terminal::disable_raw_mode();
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct Timings {
    pub scan_complete_us: Option<u64>,
    pub full_size_interactive_us: Option<u64>,
    pub first_input_response_us: Option<u64>,
    pub indexing_started_us: Option<u64>,
    pub indexing_finished_us: Option<u64>,
    pub complete: bool,
    pub resumed_scan: bool,
    pub totals: Option<crate::model::Totals>,
    pub scan_counters: Option<crate::model::Counters>,
    pub indexing: Option<indexing::Outcome>,
}

pub struct Runtime {
    pub threads: usize,
    pub started: Instant,
    pub timing_path: Option<PathBuf>,
}

struct Job {
    scope: JobScope,
    options: Options,
    progress: Arc<Progress>,
    index_progress: Arc<indexing::Progress>,
    index_cancel: Arc<AtomicBool>,
    preview: crossbeam_channel::Receiver<Arc<Tree>>,
    ready_result: crossbeam_channel::Receiver<Ready>,
    acknowledge: crossbeam_channel::Sender<()>,
    ready: bool,
    acknowledged: bool,
    thread: Option<JoinHandle<Result<indexing::Outcome>>>,
}

#[derive(Clone)]
enum JobScope {
    Main,
    Target(PathBuf),
}

struct Ready {
    tree: Arc<Tree>,
    report: Report,
    scope: Id,
}

struct ErrorReview {
    entries: Vec<RecordedError>,
    selected: usize,
}

impl ErrorReview {
    fn load(cache: &Cache, tree: Option<&Tree>) -> Result<Self> {
        let entries = if let Some(tree) = tree {
            tree.recorded_errors()?
        } else {
            read_errors(cache)?
        };
        Ok(Self {
            entries,
            selected: 0,
        })
    }

    fn reload(&mut self, cache: &Cache, tree: Option<&Tree>) -> Result<()> {
        self.entries = if let Some(tree) = tree {
            tree.recorded_errors()?
        } else {
            read_errors(cache)?
        };
        self.selected = self.selected.min(self.entries.len().saturating_sub(1));
        Ok(())
    }
}

impl Job {
    fn start(
        cache: Cache,
        options: Options,
        threads: usize,
        started: Instant,
        timings: Arc<Mutex<Timings>>,
        scope: JobScope,
        index_after_scan: bool,
    ) -> Self {
        let progress = Arc::new(Progress::default());
        let worker_progress = progress.clone();
        let index_progress = Arc::new(indexing::Progress::default());
        let worker_index = index_progress.clone();
        let index_cancel = Arc::new(AtomicBool::new(false));
        let cancellation = index_cancel.clone();
        let (preview_tx, preview) = crossbeam_channel::unbounded();
        let (ready_tx, ready_result) = crossbeam_channel::bounded(1);
        let (acknowledge, ack) = crossbeam_channel::bounded(1);
        let worker_scope = scope.clone();
        let worker_options = options.clone();
        let thread = thread::spawn(move || {
            let result = match worker_scope {
                JobScope::Main => fast::scan(
                    cache.clone(),
                    &worker_options,
                    threads,
                    worker_progress.clone(),
                    Some(preview_tx),
                )?,
                JobScope::Target(path) => fast::scan_subtree(
                    cache.clone(),
                    &path,
                    &worker_options,
                    threads,
                    worker_progress.clone(),
                )?,
            };
            let fast::ScanResult {
                tree,
                report,
                lock,
                resumed,
                scope,
            } = result;
            {
                let mut timing = timings.lock().unwrap();
                if timing.full_size_interactive_us.is_none() {
                    timing.scan_complete_us = Some(started.elapsed().as_micros() as u64);
                    timing.complete = report.complete && report.settled;
                    timing.resumed_scan = resumed;
                    timing.totals = Some(report.totals);
                    timing.scan_counters = Some(report.counters.clone());
                }
            }
            if worker_progress.cancel.load(Ordering::Relaxed) {
                return Ok(indexing::Outcome::default());
            }
            ready_tx.send(Ready {
                tree: tree.clone(),
                report,
                scope,
            })?;
            if !index_after_scan {
                return Ok(indexing::Outcome::default());
            }
            loop {
                if cancellation.load(Ordering::Relaxed) {
                    return Ok(indexing::Outcome::default());
                }
                if ack.recv_timeout(Duration::from_millis(10)).is_ok() {
                    break;
                }
            }
            timings.lock().unwrap().indexing_started_us =
                Some(started.elapsed().as_micros() as u64);
            let outcome = indexing::run(&cache, &tree, lock, cancellation, worker_index)?;
            {
                let mut timing = timings.lock().unwrap();
                timing.indexing_finished_us = Some(started.elapsed().as_micros() as u64);
                timing.indexing = Some(outcome.clone());
            }
            Ok(outcome)
        });
        Self {
            scope,
            options,
            progress,
            index_progress,
            index_cancel,
            preview,
            ready_result,
            acknowledge,
            ready: false,
            acknowledged: false,
            thread: Some(thread),
        }
    }

    fn interrupted_main(&self) -> Option<Options> {
        (matches!(&self.scope, JobScope::Main) && !self.ready).then(|| self.options.clone())
    }

    fn finished(&self) -> bool {
        self.thread.as_ref().is_some_and(JoinHandle::is_finished)
    }

    fn result(mut self) -> Result<indexing::Outcome> {
        self.thread
            .take()
            .unwrap()
            .join()
            .map_err(|_| anyhow!("scan worker panicked"))?
    }
}

impl Drop for Job {
    fn drop(&mut self) {
        self.progress.cancel.store(true, Ordering::Relaxed);
        self.index_cancel.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn progress_text(progress: &Progress, apparent: bool) -> String {
    let phase = if progress.finalizing.load(Ordering::Relaxed) {
        "Indexing"
    } else {
        "Scanning"
    };
    let size = if progress.size_ready.load(Ordering::Acquire) {
        let (bytes, metric) = if apparent {
            (progress.apparent_bytes.load(Ordering::Relaxed), "apparent")
        } else {
            (progress.disk_bytes.load(Ordering::Relaxed), "allocated")
        };
        let qualifier = if progress.partial.load(Ordering::Relaxed) {
            "partial"
        } else {
            "cached + checked"
        };
        format!("~{} {metric} ({qualifier})", human_bytes(bytes))
    } else {
        "size pending".to_owned()
    };
    format!(
        "{phase}: {} dirs, {} files | {size}",
        progress.directories.load(Ordering::Relaxed),
        progress.files.load(Ordering::Relaxed),
    )
}

pub fn run(
    cache: Cache,
    scan_options: Options,
    mut listing: ListOptions,
    cached_only: bool,
    enable_delete: bool,
    runtime: Runtime,
) -> Result<()> {
    let Runtime {
        threads,
        started,
        timing_path,
    } = runtime;
    let mut tree = if cached_only && crate::journal::exists(&cache) {
        Some(fast::cached(&cache)?)
    } else {
        None
    };
    let initial = if let Some(tree) = &tree {
        Some(tree.snapshot(ROOT, &listing)?)
    } else if cache.database.exists() {
        match read_snapshot(&cache, ROOT, &listing) {
            Ok(snapshot) => Some(snapshot),
            Err(_) if !cached_only => None,
            Err(error) => return Err(error),
        }
    } else {
        None
    };
    if cached_only && initial.is_none() {
        return Err(anyhow!("no cache exists; run without --cached first"));
    }
    let _terminal = Terminal::enter()?;
    let timings = Arc::new(Mutex::new(Timings::default()));
    let mut snapshot = initial;
    let mut live_report = snapshot.as_ref().and_then(|value| value.report.clone());
    let mut job = (!cached_only).then(|| {
        Job::start(
            cache.clone(),
            scan_options.clone(),
            threads,
            started,
            timings.clone(),
            JobScope::Main,
            true,
        )
    });
    let mut resume_main = None;
    let mut directory = ROOT;
    let mut selected = 0usize;
    let mut history = Vec::new();
    let mut help = false;
    let mut error_review: Option<ErrorReview> = None;
    let mut graph = true;
    let mut counts = false;
    let mut message = String::new();
    let mut confirmation: Option<Entry> = None;
    let mut redraw = true;
    let mut input_pending = false;
    let mut last_draw = Instant::now();
    loop {
        let (width, height) = terminal::size()?;
        let page = usize::from(height.saturating_sub(7)).max(1);
        if listing.limit != page {
            listing.limit = page;
            redraw = true;
        }
        if let Some(job) = &mut job {
            while let Ok(restored) = job.preview.try_recv() {
                live_report = restored.report.clone().map(|mut report| {
                    report.mode = "cached".to_owned();
                    report.audit = None;
                    report
                });
                install_tree(
                    &mut tree,
                    snapshot.as_ref(),
                    &mut directory,
                    &mut selected,
                    &mut history,
                    restored,
                    None,
                );
                if let Some(review) = &mut error_review {
                    review.reload(&cache, tree.as_deref())?;
                }
                redraw = true;
            }
            if let Ok(Ready {
                tree: ready,
                report,
                scope,
            }) = job.ready_result.try_recv()
            {
                let targeted = matches!(&job.scope, JobScope::Target(_));
                let preferred_scope = match (&job.scope, snapshot.as_ref()) {
                    (JobScope::Target(path), Some(snapshot))
                        if path.as_os_str().as_bytes() == snapshot.path_bytes.as_slice() =>
                    {
                        Some(scope)
                    }
                    _ => None,
                };
                message = if targeted {
                    if resume_main.is_some() {
                        format!(
                            "{} dirs checked, {} files checked; selected subtree ready, resuming root refresh",
                            report.counters.directories_checked, report.counters.files_statted,
                        )
                    } else {
                        format!(
                            "{} dirs checked, {} files checked; selected subtree refreshed",
                            report.counters.directories_checked, report.counters.files_statted,
                        )
                    }
                } else {
                    format!(
                        "{} dirs checked, {} enumerated, {} files checked; full tree ready",
                        report.counters.directories_checked,
                        report.counters.directories_enumerated,
                        report.counters.files_statted,
                    )
                };
                live_report = Some(report);
                install_tree(
                    &mut tree,
                    snapshot.as_ref(),
                    &mut directory,
                    &mut selected,
                    &mut history,
                    ready,
                    preferred_scope,
                );
                if let Some(review) = &mut error_review {
                    review.reload(&cache, tree.as_deref())?;
                }
                help = false;
                job.ready = true;
                redraw = true;
            }
        }
        if job.as_ref().is_some_and(Job::finished) {
            let finished = job.take().unwrap();
            let targeted = matches!(&finished.scope, JobScope::Target(_));
            let result = finished.result();
            if targeted && let Some(options) = resume_main.take() {
                if let Err(error) = &result {
                    message = format!("Targeted refresh error: {error:#}; resuming root refresh.");
                }
                job = Some(Job::start(
                    cache.clone(),
                    options,
                    threads,
                    started,
                    timings.clone(),
                    JobScope::Main,
                    true,
                ));
            } else {
                match result {
                    Ok(outcome) => {
                        message = if outcome.complete {
                            "Lookup index complete; browsing remains in memory.".to_owned()
                        } else {
                            "Lookup indexing paused; completed batches are saved.".to_owned()
                        }
                    }
                    Err(error) => message = format!("Scan/index error: {error:#}"),
                }
            }
            redraw = true;
        }
        if redraw {
            listing.offset = selected / page * page;
            if tree.is_some() || cache.database.exists() {
                match read_view(&cache, tree.as_deref(), directory, &listing) {
                    Ok(mut value) => {
                        let same_generation =
                            value.report.as_ref().zip(live_report.as_ref()).is_some_and(
                                |(cached, live)| cached.index_generation == live.index_generation,
                            );
                        if same_generation {
                            value.report = live_report.clone();
                        } else {
                            live_report = value.report.clone();
                            if directory != ROOT {
                                directory = ROOT;
                                selected = 0;
                                history.clear();
                                continue;
                            }
                        }
                        if selected >= value.children && selected != 0 {
                            selected = value.children.saturating_sub(1);
                            listing.offset = selected / page * page;
                            let mut value =
                                read_view(&cache, tree.as_deref(), directory, &listing)?;
                            value.report = live_report.clone();
                            snapshot = Some(value);
                        } else {
                            snapshot = Some(value);
                        }
                    }
                    Err(_) if directory != ROOT => {
                        directory = ROOT;
                        selected = 0;
                        history.clear();
                        continue;
                    }
                    Err(error) if job.is_none() => message = error.to_string(),
                    Err(_) => {}
                }
            }
            draw(
                snapshot.as_ref(),
                selected,
                &listing,
                (width, height),
                help,
                error_review.as_ref(),
                graph,
                counts,
                &message,
                confirmation.as_ref(),
                job.as_ref(),
                cached_only,
            )?;
            last_draw = Instant::now();
            redraw = false;
        } else if job.is_some() && last_draw.elapsed() >= Duration::from_millis(100) {
            draw(
                snapshot.as_ref(),
                selected,
                &listing,
                (width, height),
                help,
                error_review.as_ref(),
                graph,
                counts,
                &message,
                confirmation.as_ref(),
                job.as_ref(),
                cached_only,
            )?;
            last_draw = Instant::now();
        }
        if let Some(job) = &mut job
            && job.ready
            && !job.acknowledged
        {
            let mut timing = timings.lock().unwrap();
            if timing.full_size_interactive_us.is_none() && timing.complete {
                timing.full_size_interactive_us = Some(started.elapsed().as_micros() as u64);
            }
            job.acknowledge.try_send(()).ok();
            job.acknowledged = true;
        }
        if input_pending {
            let mut timing = timings.lock().unwrap();
            if timing.full_size_interactive_us.is_some() && timing.first_input_response_us.is_none()
            {
                timing.first_input_response_us = Some(started.elapsed().as_micros() as u64);
            }
            input_pending = false;
        }
        if !event::poll(Duration::from_millis(
            if job.as_ref().is_some_and(|job| !job.ready) {
                10
            } else {
                100
            },
        ))? {
            continue;
        }
        let key = match event::read()? {
            Event::Resize(..) => {
                redraw = true;
                continue;
            }
            Event::Key(key) if key.kind != KeyEventKind::Release => key,
            _ => continue,
        };
        input_pending = true;
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
            break;
        }
        if let Some(entry) = confirmation.take() {
            if key.code == KeyCode::Char('y') {
                if resume_main.is_none() {
                    resume_main = job.as_ref().and_then(Job::interrupted_main);
                }
                let refresh_path = snapshot
                    .as_ref()
                    .map(|snapshot| PathBuf::from(OsString::from_vec(snapshot.path_bytes.clone())));
                drop(job.take());
                let deletion = if let Some(tree) = &tree {
                    tree.delete(entry.id, entry.identity)
                } else {
                    delete_entry(&cache, entry.id, entry.identity)
                };
                match deletion {
                    Ok(()) => {
                        if directory == ROOT {
                            resume_main = None;
                            job = Some(Job::start(
                                cache.clone(),
                                scan_options.clone(),
                                threads,
                                started,
                                timings.clone(),
                                JobScope::Main,
                                true,
                            ));
                        } else if let Some(path) = refresh_path {
                            let index_after_scan = resume_main.is_none();
                            job = Some(Job::start(
                                cache.clone(),
                                scan_options.clone(),
                                threads,
                                started,
                                timings.clone(),
                                JobScope::Target(path),
                                index_after_scan,
                            ));
                        }
                        message = "Deleted; refreshing the current directory.".to_owned();
                    }
                    Err(error) => {
                        message = format!("Delete refused: {error:#}");
                        let options = resume_main.take().unwrap_or_else(|| scan_options.clone());
                        job = Some(Job::start(
                            cache.clone(),
                            options,
                            threads,
                            started,
                            timings.clone(),
                            JobScope::Main,
                            true,
                        ));
                    }
                }
            }
            redraw = true;
            continue;
        }
        if help {
            help = false;
            redraw = true;
            continue;
        }
        if let Some(mut review) = error_review.take() {
            let mut keep = true;
            match key.code {
                KeyCode::Char('q') => break,
                KeyCode::Esc | KeyCode::Char('E') => keep = false,
                KeyCode::Char('?') => {
                    help = true;
                    keep = false;
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    review.selected =
                        (review.selected + 1).min(review.entries.len().saturating_sub(1));
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    review.selected = review.selected.saturating_sub(1);
                }
                KeyCode::PageDown => {
                    review.selected =
                        (review.selected + page).min(review.entries.len().saturating_sub(1));
                }
                KeyCode::PageUp => review.selected = review.selected.saturating_sub(page),
                KeyCode::Home => review.selected = 0,
                KeyCode::End => review.selected = review.entries.len().saturating_sub(1),
                KeyCode::Enter | KeyCode::Right | KeyCode::Char('l') => {
                    if let Some(error) = review.entries.get(review.selected) {
                        directory = error.directory;
                        selected = 0;
                        history.clear();
                        message = format!("{}: {}", error.path, error.message);
                    }
                    keep = false;
                }
                _ => {}
            }
            if keep {
                error_review = Some(review);
            }
            redraw = true;
            continue;
        }
        let children = snapshot.as_ref().map_or(0, |value| value.children);
        let current = snapshot
            .as_ref()
            .and_then(|value| value.entries.get(selected.saturating_sub(value.offset)));
        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => break,
            KeyCode::Down | KeyCode::Char('j') => {
                selected = (selected + 1).min(children.saturating_sub(1))
            }
            KeyCode::Up | KeyCode::Char('k') => selected = selected.saturating_sub(1),
            KeyCode::PageDown => selected = (selected + page).min(children.saturating_sub(1)),
            KeyCode::PageUp => selected = selected.saturating_sub(page),
            KeyCode::Home => selected = 0,
            KeyCode::End => selected = children.saturating_sub(1),
            KeyCode::Right | KeyCode::Enter | KeyCode::Char('l') => {
                if let Some(entry) = current
                    && entry.kind == Kind::Directory
                    && entry.excluded.is_none()
                {
                    history.push((directory, selected));
                    directory = entry.id;
                    selected = 0;
                    message.clear();
                }
            }
            KeyCode::Left | KeyCode::Backspace | KeyCode::Char('h' | '<') => {
                if let Some((parent, cursor)) = history.pop() {
                    directory = parent;
                    selected = cursor;
                } else if let Some(parent) = snapshot.as_ref().and_then(|value| value.parent) {
                    directory = parent;
                    selected = 0;
                }
            }
            KeyCode::Char('?') => help = true,
            KeyCode::Char('E') => match ErrorReview::load(&cache, tree.as_deref()) {
                Ok(review) => {
                    message = format!("{} recorded errors.", review.entries.len());
                    error_review = Some(review);
                }
                Err(error) => message = format!("Could not load recorded errors: {error:#}"),
            },
            KeyCode::Char('a') => {
                listing.apparent = !listing.apparent;
                selected = 0;
            }
            KeyCode::Char('e') => {
                listing.hidden = !listing.hidden;
                selected = 0;
            }
            KeyCode::Char('t') => {
                listing.directories_first = !listing.directories_first;
                selected = 0;
            }
            KeyCode::Char('g') => graph = !graph,
            KeyCode::Char('c') => counts = !counts,
            KeyCode::Char('s' | 'n' | 'C') => {
                let sort = match key.code {
                    KeyCode::Char('n') => Sort::Name,
                    KeyCode::Char('C') => Sort::Count,
                    _ => Sort::Size,
                };
                listing.descending =
                    if std::mem::discriminant(&listing.sort) == std::mem::discriminant(&sort) {
                        !listing.descending
                    } else {
                        !matches!(sort, Sort::Name)
                    };
                listing.sort = sort;
                selected = 0;
            }
            KeyCode::Char('r' | 'R') if cached_only => {
                message = "Cached-only mode: restart without --cached to refresh.".to_owned();
            }
            KeyCode::Char('r' | 'R') => {
                let mut options = scan_options.clone();
                options.exact = key.code == KeyCode::Char('R');
                if directory == ROOT {
                    resume_main = None;
                    drop(job.take());
                    job = Some(Job::start(
                        cache.clone(),
                        options,
                        threads,
                        started,
                        timings.clone(),
                        JobScope::Main,
                        true,
                    ));
                    message = "Refreshing the scan root.".to_owned();
                } else if let Some(snapshot) = &snapshot {
                    if resume_main.is_none() {
                        resume_main = job.as_ref().and_then(Job::interrupted_main);
                    }
                    let path = PathBuf::from(OsString::from_vec(snapshot.path_bytes.clone()));
                    let display = snapshot.path.clone();
                    drop(job.take());
                    let index_after_scan = resume_main.is_none();
                    job = Some(Job::start(
                        cache.clone(),
                        options,
                        threads,
                        started,
                        timings.clone(),
                        JobScope::Target(path),
                        index_after_scan,
                    ));
                    message = format!("Refreshing {display} only.");
                }
            }
            KeyCode::Char('d') => {
                if !enable_delete || cached_only {
                    message =
                        "Read-only. Restart with --enable-delete to permit confirmed deletion."
                            .to_owned();
                } else if job.as_ref().is_some_and(|job| !job.ready) {
                    message = "Wait for validation to finish before deleting.".to_owned();
                } else if let Some(entry) = current {
                    confirmation = Some(entry.clone());
                }
            }
            KeyCode::Char('i') => {
                if let Some(entry) = current {
                    message = format!(
                        "{} | allocated {} B | apparent {} B | inode {}:{}{}",
                        entry.name,
                        entry.totals.disk_bytes,
                        entry.totals.apparent_bytes,
                        entry.identity.device,
                        entry.identity.inode,
                        entry
                            .error
                            .as_ref()
                            .map(|error| format!(" | {error}"))
                            .unwrap_or_default(),
                    );
                }
            }
            _ => {}
        }
        redraw = true;
    }
    drop(job);
    if let Some(path) = timing_path {
        use std::os::unix::fs::OpenOptionsExt;
        let mut output = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)?;
        serde_json::to_writer_pretty(&mut output, &*timings.lock().unwrap())?;
        output.write_all(b"\n")?;
    }
    Ok(())
}

fn install_tree(
    current: &mut Option<Arc<Tree>>,
    snapshot: Option<&Snapshot>,
    directory: &mut Id,
    selected: &mut usize,
    history: &mut Vec<(Id, usize)>,
    next: Arc<Tree>,
    preferred: Option<Id>,
) {
    let previous_epoch = current.as_ref().map(|tree| tree.epoch);
    let path =
        snapshot.map(|snapshot| PathBuf::from(OsString::from_vec(snapshot.path_bytes.clone())));
    let next_directory = preferred
        .or_else(|| {
            path.as_deref()
                .and_then(|path| next.directory_at_path(path).ok())
        })
        .unwrap_or(ROOT);
    if previous_epoch != Some(next.epoch) || next_directory != *directory {
        history.clear();
        *selected = 0;
    }
    *directory = next_directory;
    *current = Some(next);
}

fn read_view(
    cache: &Cache,
    tree: Option<&Tree>,
    directory: Id,
    listing: &ListOptions,
) -> Result<Snapshot> {
    if let Some(tree) = tree {
        tree.snapshot(directory, listing)
    } else {
        read_snapshot(cache, directory, listing)
    }
}

#[allow(clippy::too_many_arguments)]
fn draw(
    snapshot: Option<&Snapshot>,
    selected: usize,
    options: &ListOptions,
    (width, height): (u16, u16),
    help: bool,
    error_review: Option<&ErrorReview>,
    graph: bool,
    counts: bool,
    message: &str,
    confirmation: Option<&Entry>,
    job: Option<&Job>,
    cached_only: bool,
) -> Result<()> {
    let mut buffer = Vec::new();
    queue!(buffer, MoveTo(0, 0), Clear(ClearType::All))?;
    let width = usize::from(width);
    if height < 8 || width < 35 {
        line(&mut buffer, 0, width, "cdu: terminal too small; q to quit")?;
        io::stdout().write_all(&buffer)?;
        return Ok(());
    }
    let state = if job.is_some_and(|job| job.ready) {
        if snapshot
            .and_then(|snapshot| snapshot.report.as_ref())
            .is_some_and(|report| report.complete && report.settled)
        {
            "ready"
        } else {
            "incomplete"
        }
    } else if job.is_some() {
        "scanning"
    } else if cached_only {
        "cached / unverified"
    } else {
        snapshot
            .and_then(|s| s.report.as_ref())
            .map_or("no index", |r| r.mode.as_str())
    };
    line(
        &mut buffer,
        0,
        width,
        &format!(
            "cdu {} - cached disk usage [{state}]",
            env!("CARGO_PKG_VERSION")
        ),
    )?;
    line(
        &mut buffer,
        1,
        width,
        if error_review.is_some() {
            "Recorded filesystem errors"
        } else {
            snapshot.map_or("Building the first metadata index...", |s| s.path.as_str())
        },
    )?;
    if help {
        let lines = [
            "Navigation: arrows / hjkl, Enter, Backspace, PgUp/PgDn, Home/End",
            "s size sort   n name sort   C item-count sort (repeat to reverse)",
            "a apparent/allocated bytes   c counts   g graph   t directories first",
            "e hidden files   i selected item details   E review errors",
            "r refresh current directory   R exact current-directory verification",
            "d delete (requires --enable-delete, then explicit y confirmation)",
            "? help   q quit / cancel active scan   Ctrl-C quit",
            "",
            "Recorded totals mix cached and freshly checked metadata.",
            "Directory timestamps cannot reveal growth in existing files.",
            "Root estimates are NOT confidence intervals for individual rows.",
            "H = hard link already counted elsewhere; ! = error; < = excluded.",
            "No daemon, watcher, scanned-file content reads, or telemetry.",
            "Press any key to return.",
        ];
        for (index, text) in lines
            .iter()
            .enumerate()
            .take(usize::from(height.saturating_sub(3)))
        {
            line(&mut buffer, index as u16 + 3, width, text)?;
        }
    } else if let Some(review) = error_review {
        line(
            &mut buffer,
            2,
            width,
            &format!(" {} recorded errors", review.entries.len()),
        )?;
        let page = usize::from(height - 7).max(1);
        let offset = review.selected / page * page;
        if review.entries.is_empty() {
            line(
                &mut buffer,
                3,
                width,
                "No filesystem errors are recorded in the current index.",
            )?;
        } else {
            for (index, error) in review.entries.iter().skip(offset).take(page).enumerate() {
                let active = offset + index == review.selected;
                if active {
                    queue!(buffer, SetAttribute(Attribute::Reverse))?;
                }
                let message = display_name(OsStr::new(&error.message));
                line(
                    &mut buffer,
                    index as u16 + 3,
                    width,
                    &format!("! {} | {message}", error.path),
                )?;
                if active {
                    queue!(buffer, SetAttribute(Attribute::Reset))?;
                }
            }
        }
        line(
            &mut buffer,
            height - 4,
            width,
            "Recorded paths and messages from the current metadata index.",
        )?;
    } else if let Some(snapshot) = snapshot {
        let metric = |entry: &Entry| {
            if options.apparent {
                entry.totals.apparent_bytes
            } else {
                entry.totals.disk_bytes
            }
        };
        let total = if options.apparent {
            snapshot.totals.apparent_bytes
        } else {
            snapshot.totals.disk_bytes
        };
        let max = total.max(1);
        line(
            &mut buffer,
            2,
            width,
            &format!(
                " Recorded {} | {} items | {} errors | {} excluded",
                human_bytes(total),
                snapshot.totals.entries.saturating_sub(1),
                snapshot.totals.errors,
                snapshot.totals.excluded,
            ),
        )?;
        for (index, entry) in snapshot
            .entries
            .iter()
            .enumerate()
            .take(usize::from(height - 7))
        {
            let active = snapshot.offset + index == selected;
            if active {
                queue!(buffer, SetAttribute(Attribute::Reverse))?;
            }
            let size = metric(entry);
            let flag = if entry.error.is_some() {
                '!'
            } else if entry.excluded.is_some() {
                '<'
            } else if entry.hard_link {
                'H'
            } else {
                ' '
            };
            let prefix = if entry.kind == Kind::Directory {
                "/"
            } else if entry.kind == Kind::Symlink {
                "@"
            } else {
                ""
            };
            let bar = if graph {
                let filled = (size as f64 / max as f64 * 10.0).round() as usize;
                format!(" [{}{}]", "#".repeat(filled), " ".repeat(10 - filled))
            } else {
                String::new()
            };
            let count = if counts {
                format!(" {:>7}", entry.totals.entries.saturating_sub(1))
            } else {
                String::new()
            };
            let text = format!(
                "{flag} {:>10}{bar}{count} {prefix}{}",
                human_bytes(size),
                entry.name
            );
            line(&mut buffer, index as u16 + 3, width, &text)?;
            if active {
                queue!(buffer, SetAttribute(Attribute::Reset))?;
            }
        }
        let summary = if let Some(job) = job {
            if job.ready {
                format!(
                    "Full size ready | lookup index: {}/{} dirs, {} rows (q pauses indexing)",
                    job.index_progress.directories.load(Ordering::Relaxed),
                    job.index_progress.total_directories.load(Ordering::Relaxed),
                    job.index_progress.rows.load(Ordering::Relaxed),
                )
            } else {
                progress_text(&job.progress, options.apparent)
            }
        } else if snapshot.directory == ROOT && !cached_only {
            snapshot
                .report
                .as_ref()
                .and_then(|report| report.audit.as_ref())
                .map(|audit| {
                    let estimate = if options.apparent {
                        &audit.apparent
                    } else {
                        &audit.disk
                    };
                    let bounds = estimate
                        .interval
                        .as_ref()
                        .map(|interval| {
                            format!(
                                " [{}, {}] conditional {:.0}% interval",
                                human_bytes(interval.lower_bytes.max(0.0) as u64),
                                human_bytes(interval.upper_bytes.max(0.0) as u64),
                                interval.confidence * 100.0,
                            )
                        })
                        .unwrap_or_else(|| " (byte-error bound not established)".to_owned());
                    format!(
                        "Estimated root: {}{bounds}",
                        human_bytes(estimate.bytes as u64)
                    )
                })
                .unwrap_or_else(|| {
                    "Full metadata scan; not an atomic filesystem snapshot.".to_owned()
                })
        } else {
            "Cached row totals; root sampling confidence does not apply to this directory."
                .to_owned()
        };
        line(&mut buffer, height - 4, width, &summary)?;
    } else if let Some(job) = job {
        line(
            &mut buffer,
            3,
            width,
            &progress_text(&job.progress, options.apparent),
        )?;
    }
    if !help {
        if let Some(review) = error_review {
            let detail = review
                .entries
                .get(review.selected)
                .map(|error| {
                    format!(
                        "{} | {}",
                        error.path,
                        display_name(OsStr::new(&error.message))
                    )
                })
                .unwrap_or_else(|| "No recorded errors.".to_owned());
            line(&mut buffer, height - 3, width, &detail)?;
        } else if let Some(entry) = confirmation {
            line(
                &mut buffer,
                height - 3,
                width,
                &format!(
                    "Permanently delete {}? Press y; any other key cancels.",
                    entry.name
                ),
            )?;
        } else {
            line(
                &mut buffer,
                height - 3,
                width,
                &display_name(OsStr::new(message)),
            )?;
        }
        line(
            &mut buffer,
            height - 2,
            width,
            if error_review.is_some() {
                " j/k navigate | Enter open directory | E/Esc close | ? help | q quit"
            } else {
                " arrows/hjkl navigate | E errors | r refresh here | R exact | ? help | q quit"
            },
        )?;
    }
    let mut stdout = io::stdout().lock();
    stdout.write_all(&buffer)?;
    stdout.flush()?;
    Ok(())
}

fn line(buffer: &mut Vec<u8>, row: u16, width: usize, text: &str) -> io::Result<()> {
    let mut used = 0;
    let mut cropped = String::new();
    for character in text.chars() {
        let size = character.width().unwrap_or(0);
        if used + size > width.saturating_sub(1) {
            break;
        }
        cropped.push(character);
        used += size;
    }
    queue!(buffer, MoveTo(0, row), Print(cropped))
}

pub fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 7] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }

    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn running_job() -> Arc<Progress> {
        let progress = Arc::new(Progress::default());
        progress.directories.store(12, Ordering::Relaxed);
        progress.files.store(345, Ordering::Relaxed);
        progress
            .disk_bytes
            .store(2 * 1024 * 1024 * 1024, Ordering::Relaxed);
        progress
            .apparent_bytes
            .store(6 * 1024 * 1024 * 1024, Ordering::Relaxed);
        progress.partial.store(true, Ordering::Relaxed);
        progress.size_ready.store(true, Ordering::Release);
        progress
    }

    #[test]
    fn scanning_shows_partial_size_in_the_selected_metric() {
        let job = running_job();
        let disk = progress_text(&job, false);
        assert!(disk.contains("12 dirs, 345 files"));
        assert!(disk.contains("~2.0 GiB allocated (partial)"));
        assert!(progress_text(&job, true).contains("~6.0 GiB apparent (partial)"));
    }

    #[test]
    fn finalization_keeps_counts_and_live_estimates_visible() {
        let job = running_job();
        job.finalizing.store(true, Ordering::Relaxed);
        job.partial.store(false, Ordering::Relaxed);
        let text = progress_text(&job, false);
        assert!(text.starts_with("Indexing: 12 dirs, 345 files"));
        assert!(text.contains("~2.0 GiB allocated (cached + checked)"));
    }
}
