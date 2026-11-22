use crate::{
    ListOptions, Snapshot, Sort,
    cache::Cache,
    estimate::{self, AuditOptions, Samples},
    journal::{self, Journal},
    model::{
        Counters, Delta, Entry, Id, Identity, Info, Kind, ROOT, Record, Report, Totals,
        display_name,
    },
    scan::{Options as ScanOptions, Progress},
};
use anyhow::{Context, Result, ensure};
use crossbeam_channel::{Receiver, Sender};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BinaryHeap, HashMap, HashSet, VecDeque},
    ffi::OsStr,
    fs::{self, File, OpenOptions},
    os::unix::{ffi::OsStrExt, fs::OpenOptionsExt},
    path::{Path, PathBuf},
    sync::{Arc, atomic::Ordering},
    thread,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

const CHUNK: usize = 256;
const EXCLUDED: u8 = 1;
const UNREADABLE: u8 = 2;

#[cfg(test)]
#[path = "fast_tests.rs"]
mod tests;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct RawEntry {
    pub name: Box<[u8]>,
    pub info: Info,
    pub directory: u32,
    pub flags: u8,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct Chunk {
    pub start: u32,
    pub entries: Box<[RawEntry]>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) enum Event {
    Start {
        root: Vec<u8>,
        info: Info,
    },
    Begin {
        directory: u32,
        info: Info,
    },
    Entries {
        directory: u32,
        chunk: Arc<Chunk>,
    },
    End {
        directory: u32,
        info: Info,
        complete: bool,
        error: Option<String>,
    },
    Patch {
        directory: u32,
        entry: u32,
        info: Info,
        flags: u8,
    },
    Report(Box<Report>),
}

#[derive(Clone, Debug)]
pub(crate) struct Directory {
    pub parent: u32,
    pub name: Box<[u8]>,
    pub info: Info,
    pub chunks: Vec<Arc<Chunk>>,
    pub complete: bool,
    pub error: Option<String>,
    pub revision: u64,
    pub totals: Totals,
    pub counted: Vec<u8>,
    pub entries_count: usize,
}

impl Directory {
    fn new(parent: u32, name: Box<[u8]>, info: Info) -> Self {
        Self {
            parent,
            name,
            info,
            chunks: Vec::new(),
            complete: false,
            error: None,
            revision: 0,
            totals: Totals::default(),
            counted: Vec::new(),
            entries_count: 0,
        }
    }
    pub fn entries(&self) -> impl Iterator<Item = &RawEntry> {
        self.chunks.iter().flat_map(|chunk| chunk.entries.iter())
    }
    pub fn count(&self) -> usize {
        self.entries_count
    }
    pub fn entry(&self, index: usize) -> Option<&RawEntry> {
        let chunk = self
            .chunks
            .partition_point(|chunk| chunk.start as usize <= index)
            .checked_sub(1)?;
        let chunk = &self.chunks[chunk];
        chunk.entries.get(index - chunk.start as usize)
    }
}

#[derive(Clone, Debug)]
pub struct Tree {
    pub root: PathBuf,
    pub(crate) directories: Vec<Option<Directory>>,
    pub(crate) active: Vec<bool>,
    pub report: Option<Report>,
    pub epoch: u64,
}

impl Tree {
    pub(crate) fn empty(root: PathBuf) -> Self {
        Self {
            root,
            directories: vec![None],
            active: Vec::new(),
            report: None,
            epoch: 0,
        }
    }

    pub(crate) fn dir(&self, id: u32) -> Result<&Directory> {
        self.directories
            .get(id as usize)
            .and_then(Option::as_ref)
            .context("directory is no longer in the scan")
    }

    fn dir_mut(&mut self, id: u32) -> Result<&mut Directory> {
        self.directories
            .get_mut(id as usize)
            .and_then(Option::as_mut)
            .context("invalid checkpoint directory")
    }

    pub(crate) fn apply(&mut self, event: &Event, sequence: u64) -> Result<()> {
        match event {
            Event::Start { root, info } => {
                ensure!(
                    root.as_slice() == self.root.as_os_str().as_bytes(),
                    "checkpoint root mismatch"
                );
                self.directories = vec![None, Some(Directory::new(0, Box::default(), *info))];
                self.report = None;
                self.epoch = sequence;
            }
            Event::Begin { directory, info } => {
                let dir = self.dir_mut(*directory)?;
                dir.info = *info;
                dir.chunks.clear();
                dir.entries_count = 0;
                dir.complete = false;
                dir.error = None;
                dir.revision = sequence;
            }
            Event::Entries { directory, chunk } => {
                ensure!(chunk.entries.len() <= CHUNK, "invalid checkpoint chunk");
                for entry in chunk.entries.iter() {
                    ensure!(
                        !entry.name.is_empty()
                            && entry.name.as_ref() != b"."
                            && entry.name.as_ref() != b".."
                            && !entry.name.contains(&b'/')
                            && !entry.name.contains(&0),
                        "invalid checkpoint filename"
                    );
                    if entry.directory != 0 {
                        ensure!(
                            entry.directory as usize <= self.directories.len(),
                            "nonsequential checkpoint directory"
                        );
                        if entry.directory as usize == self.directories.len() {
                            self.directories.push(Some(Directory::new(
                                *directory,
                                entry.name.clone(),
                                entry.info,
                            )));
                        } else {
                            let child = self.dir_mut(entry.directory)?;
                            child.parent = *directory;
                            child.name = entry.name.clone();
                        }
                    }
                }
                let dir = self.dir_mut(*directory)?;
                ensure!(
                    chunk.start as usize == dir.entries_count,
                    "noncontiguous directory checkpoint"
                );
                dir.chunks.push(chunk.clone());
                dir.entries_count += chunk.entries.len();
                dir.revision = sequence;
            }
            Event::End {
                directory,
                info,
                complete,
                error,
            } => {
                let dir = self.dir_mut(*directory)?;
                dir.info = *info;
                dir.complete = *complete;
                dir.error = error.clone();
                dir.revision = sequence;
            }
            Event::Patch {
                directory,
                entry,
                info,
                flags,
            } => {
                let dir = self.dir_mut(*directory)?;
                let at = *entry as usize;
                let mut found = false;
                for chunk in &mut dir.chunks {
                    if at >= chunk.start as usize && at < chunk.start as usize + chunk.entries.len()
                    {
                        let within = at - chunk.start as usize;
                        let item = &mut Arc::make_mut(chunk).entries[within];
                        item.info = *info;
                        item.flags = *flags;
                        found = true;
                        break;
                    }
                }
                ensure!(found, "invalid checkpoint entry reference");
                dir.revision = sequence;
            }
            Event::Report(report) => self.report = Some((**report).clone()),
        }
        Ok(())
    }

    pub(crate) fn path(&self, mut id: u32) -> Result<PathBuf> {
        let mut parts = Vec::new();
        while id != ROOT as u32 {
            ensure!(parts.len() < 4096, "invalid directory ancestry");
            let dir = self.dir(id)?;
            parts.push(dir.name.as_ref());
            id = dir.parent;
        }
        let mut path = self.root.clone();
        for part in parts.into_iter().rev() {
            path.push(OsStr::from_bytes(part));
        }
        Ok(path)
    }

    pub fn is_complete(&self) -> bool {
        self.active
            .iter()
            .enumerate()
            .filter(|(_, active)| **active)
            .all(|(id, _)| {
                self.directories[id]
                    .as_ref()
                    .is_some_and(|dir| dir.complete)
            })
    }

    pub(crate) fn aggregate(&mut self) -> Result<()> {
        self.active = vec![false; self.directories.len()];
        if self.directories.len() < 2 {
            return Ok(());
        }
        let mut stack = vec![(ROOT as u32, 0usize, false)];
        let mut links = HashSet::new();
        while let Some((id, at, entered)) = stack.pop() {
            if !entered {
                ensure!(!self.active[id as usize], "directory alias cycle");
                self.active[id as usize] = true;
                let dir = self.dir_mut(id)?;
                dir.totals = Totals {
                    disk_bytes: dir.info.disk(),
                    apparent_bytes: dir.info.size,
                    entries: 1,
                    errors: u64::from(dir.error.is_some()),
                    excluded: 0,
                };
                dir.counted = vec![0; dir.count()];
            }
            let entry = self.dir(id)?.entry(at);
            if let Some(entry) = entry {
                let child = entry.directory;
                let info = entry.info;
                let flags = entry.flags;
                stack.push((id, at + 1, true));
                if child != 0 && flags & EXCLUDED == 0 {
                    if self.dir(child)?.parent == id {
                        stack.push((child, 0, false));
                    }
                } else {
                    let counted =
                        flags & EXCLUDED == 0 && (info.links <= 1 || links.insert(info.identity));
                    let own = Totals {
                        disk_bytes: if counted { info.disk() } else { 0 },
                        apparent_bytes: if counted { info.size } else { 0 },
                        entries: 1,
                        errors: u64::from(flags & UNREADABLE != 0),
                        excluded: u64::from(flags & EXCLUDED != 0),
                    };
                    let dir = self.dir_mut(id)?;
                    dir.counted[at] = u8::from(counted);
                    dir.totals = Delta::between(Totals::default(), own).apply(dir.totals)?;
                }
            } else {
                let dir = self.dir(id)?;
                let parent = dir.parent;
                let total = dir.totals;
                if parent != 0 {
                    let parent = self.dir_mut(parent)?;
                    parent.totals =
                        Delta::between(Totals::default(), total).apply(parent.totals)?;
                }
            }
        }
        Ok(())
    }

    pub fn snapshot(&self, id: Id, options: &ListOptions) -> Result<Snapshot> {
        ensure!(id > 0, "not a directory");
        let id = u32::try_from(id)?;
        let dir = self.dir(id)?;
        let count = if options.hidden {
            dir.count()
        } else {
            dir.entries()
                .filter(|entry| entry.name.first() != Some(&b'.'))
                .count()
        };
        let mut indexes: Vec<_> = if options.limit == 0 {
            Vec::new()
        } else {
            dir.entries()
                .enumerate()
                .filter(|(_, entry)| options.hidden || entry.name.first() != Some(&b'.'))
                .map(|(at, _)| at)
                .collect()
        };
        if options.limit > 0 {
            let metric = |at: usize| -> u64 {
                let entry = dir.entry(at).unwrap();
                let totals = self.entry_totals(id, at);
                match options.sort {
                    Sort::Size if options.apparent => totals.apparent_bytes,
                    Sort::Size => totals.disk_bytes,
                    Sort::Count => totals.entries,
                    Sort::Name => u64::from(entry.info.kind == Kind::Directory),
                }
            };
            indexes.sort_unstable_by(|&a, &b| {
                let left = dir.entry(a).unwrap();
                let right = dir.entry(b).unwrap();
                let group = if options.directories_first {
                    (right.info.kind == Kind::Directory).cmp(&(left.info.kind == Kind::Directory))
                } else {
                    std::cmp::Ordering::Equal
                };
                group
                    .then_with(|| {
                        let cmp = if matches!(options.sort, Sort::Name) {
                            left.name.cmp(&right.name)
                        } else {
                            metric(a).cmp(&metric(b))
                        };
                        if options.descending {
                            cmp.reverse()
                        } else {
                            cmp
                        }
                    })
                    .then_with(|| left.name.cmp(&right.name))
            });
        }
        let entries = indexes
            .into_iter()
            .skip(options.offset)
            .take(options.limit)
            .map(|at| self.entry_view(id, at))
            .collect();
        let path = self.path(id)?;
        Ok(Snapshot {
            root: display_name(self.root.as_os_str()),
            path: display_name(path.as_os_str()),
            path_bytes: path.as_os_str().as_bytes().to_vec(),
            directory: id as Id,
            parent: (dir.parent != 0).then_some(dir.parent as Id),
            totals: dir.totals,
            children: count,
            offset: options.offset,
            entries,
            report: self.report.clone(),
        })
    }

    fn entry_totals(&self, parent: u32, at: usize) -> Totals {
        let dir = self.dir(parent).unwrap();
        let entry = dir.entry(at).unwrap();
        if entry.directory != 0 && entry.flags & EXCLUDED == 0 {
            return self
                .dir(entry.directory)
                .map(|dir| dir.totals)
                .unwrap_or_default();
        }
        let counted = dir.counted.get(at) == Some(&1);
        Totals {
            disk_bytes: if counted { entry.info.disk() } else { 0 },
            apparent_bytes: if counted { entry.info.size } else { 0 },
            entries: 1,
            errors: u64::from(entry.flags & UNREADABLE != 0),
            excluded: u64::from(entry.flags & EXCLUDED != 0),
        }
    }

    fn entry_view(&self, parent: u32, at: usize) -> Entry {
        let dir = self.dir(parent).unwrap();
        let entry = dir.entry(at).unwrap();
        Entry {
            id: if entry.directory != 0 {
                entry.directory as Id
            } else {
                file_id(parent, at)
            },
            name: display_name(OsStr::from_bytes(&entry.name)),
            name_bytes: entry.name.to_vec(),
            kind: entry.info.kind,
            identity: entry.info.identity,
            totals: self.entry_totals(parent, at),
            hard_link: entry.directory == 0
                && entry.flags & EXCLUDED == 0
                && dir.counted.get(at) != Some(&1),
            excluded: (entry.flags & EXCLUDED != 0)
                .then(|| "excluded filesystem, cache, or directory alias".to_owned()),
            error: (entry.flags & UNREADABLE != 0).then(|| "metadata could not be read".to_owned()),
        }
    }
    pub(crate) fn directory_record(&self, id: u32) -> Result<Record> {
        let dir = self.dir(id)?;
        Ok(Record {
            id: id as Id,
            parent: (dir.parent != 0).then_some(dir.parent as Id),
            name: dir.name.to_vec(),
            info: dir.info,
            counted: true,
            excluded: None,
            error: dir.error.clone(),
            totals: dir.totals,
            direct_children: dir.count() as u64,
        })
    }

    pub(crate) fn child_record(&self, parent: u32, at: usize) -> Result<Record> {
        let dir = self.dir(parent)?;
        let entry = dir.entry(at).context("missing indexed entry")?;
        if entry.directory != 0 {
            return self.directory_record(entry.directory);
        }
        let view = self.entry_view(parent, at);
        Ok(Record {
            id: view.id,
            parent: Some(parent as Id),
            name: entry.name.to_vec(),
            info: entry.info,
            counted: !view.hard_link,
            excluded: view.excluded,
            error: view.error,
            totals: view.totals,
            direct_children: 0,
        })
    }

    pub(crate) fn fingerprint(&self, id: u32) -> Result<Vec<u8>> {
        let dir = self.dir(id)?;
        let mut hash = blake3::Hasher::new();
        hash.update(&self.epoch.to_le_bytes());
        hash.update(&dir.revision.to_le_bytes());
        hash.update(&dir.parent.to_le_bytes());
        hash.update(&dir.name);
        hash.update(&dir.counted);
        for value in [
            dir.totals.disk_bytes,
            dir.totals.apparent_bytes,
            dir.totals.entries,
            dir.totals.errors,
            dir.totals.excluded,
        ] {
            hash.update(&value.to_le_bytes());
        }
        Ok(hash.finalize().as_bytes().to_vec())
    }

    pub fn delete(&self, id: Id, identity: Identity) -> Result<()> {
        ensure!(id != ROOT, "refusing to delete the scan root");
        let (path, info, excluded) = if id > 0 {
            let dir = self.dir(u32::try_from(id)?)?;
            (self.path(id as u32)?, dir.info, false)
        } else {
            let key = id.checked_neg().context("invalid entry id")? as u64;
            let parent = (key >> 32) as u32;
            let at = (key as u32).checked_sub(1).context("invalid entry id")? as usize;
            let entry = self
                .dir(parent)?
                .entry(at)
                .context("entry is no longer present")?;
            (
                self.path(parent)?.join(OsStr::from_bytes(&entry.name)),
                entry.info,
                entry.flags & EXCLUDED != 0,
            )
        };
        ensure!(!excluded, "cannot delete an excluded entry");
        ensure!(
            identity == info.identity,
            "selection changed; refresh first"
        );
        ensure!(
            Info::from(&fs::symlink_metadata(&self.root)?).identity
                == self.dir(ROOT as u32)?.info.identity,
            "scan root was replaced"
        );
        ensure!(
            fs::canonicalize(path.parent().context("missing parent")?)?.starts_with(&self.root),
            "entry escaped the scan root"
        );
        let current = fs::symlink_metadata(&path)?;
        ensure!(
            Info::from(&current) == info,
            "entry changed; refresh before deleting"
        );
        if current.is_dir() {
            fs::remove_dir_all(path)?;
        } else {
            fs::remove_file(path)?;
        }
        Ok(())
    }
}

fn file_id(parent: u32, index: usize) -> Id {
    -(((parent as i64) << 32) | (index as i64 + 1))
}

#[derive(Clone)]
struct Work {
    directory: u32,
    path: PathBuf,
    info: Info,
    cached: Option<Directory>,
    force: bool,
}

enum Found {
    Begin(u32, Info, bool),
    Entries(u32, Vec<RawEntry>),
    End(u32, Info, bool, Option<String>),
    Reused(u32),
    Failed(u32, String),
    Cancelled(u32),
}

pub struct ScanResult {
    pub tree: Arc<Tree>,
    pub report: Report,
    pub lock: Arc<File>,
    pub resumed: bool,
}

pub fn cached(cache: &Cache) -> Result<Arc<Tree>> {
    ensure!(journal::exists(cache), "no resumable scan cache exists");
    let mut loaded = journal::load(cache)?;
    loaded.tree.aggregate()?;
    let totals = loaded.tree.dir(ROOT as u32)?.totals;
    let complete = loaded.tree.is_complete() && totals.errors == 0;
    let report = loaded.tree.report.get_or_insert(Report {
        index_generation: loaded.sequence,
        mode: "cached".to_owned(),
        reason: "restored scan checkpoints".to_owned(),
        completed_at_unix_ms: 0,
        complete,
        settled: false,
        totals,
        counters: Counters::default(),
        audit: None,
        warning: None,
    });
    report.mode = "cached".to_owned();
    report.totals = totals;
    report.complete = complete;
    report.audit = None;
    report.warning = Some(
        if complete {
            "Unverified cached tree."
        } else {
            "Partial scan checkpoint; scanning will resume on the next ordinary invocation."
        }
        .to_owned(),
    );
    Ok(Arc::new(loaded.tree))
}

pub fn has_checkpoint(cache: &Cache) -> bool {
    journal::exists(cache)
}

fn send(sender: &Sender<Found>, value: Found) -> bool {
    sender.send(value).is_ok()
}

fn worker(receiver: Receiver<Work>, sender: Sender<Found>, progress: Arc<Progress>) {
    while let Ok(work) = receiver.recv() {
        if progress.cancel.load(Ordering::Relaxed) {
            send(&sender, Found::Cancelled(work.directory));
            continue;
        }
        if let Err(error) = scan_directory(&work, &sender, &progress)
            && !send(&sender, Found::Failed(work.directory, format!("{error:#}")))
        {
            break;
        }
    }
}

fn scan_directory(work: &Work, sender: &Sender<Found>, progress: &Progress) -> Result<()> {
    let before = if work.cached.is_some() {
        Info::from(&fs::symlink_metadata(&work.path)?)
    } else {
        work.info
    };
    ensure!(before.kind == Kind::Directory, "directory was replaced");
    if !work.force
        && let Some(cached) = &work.cached
        && cached.complete
        && cached.error.is_none()
        && cached.info == before
    {
        send(sender, Found::Reused(work.directory));
        return Ok(());
    }
    let continuing = !work.force
        && work.cached.as_ref().is_some_and(|cached| {
            !cached.complete && cached.error.is_none() && cached.info == before
        });
    let seen: HashSet<&[u8]> = if continuing {
        work.cached
            .as_ref()
            .unwrap()
            .entries()
            .map(|entry| entry.name.as_ref())
            .collect()
    } else {
        HashSet::new()
    };
    let previous_dirs: HashMap<&[u8], (Identity, u32)> = work
        .cached
        .as_ref()
        .map(|dir| {
            dir.entries()
                .filter(|entry| entry.directory != 0)
                .map(|entry| (entry.name.as_ref(), (entry.info.identity, entry.directory)))
                .collect()
        })
        .unwrap_or_default();
    if !send(sender, Found::Begin(work.directory, before, !continuing)) {
        return Ok(());
    }
    let fd = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&work.path)?;
    let mut directory_stream = rustix::fs::Dir::new(fd)?;
    let mut entries = Vec::new();
    let mut finished = true;
    while let Some(entry) = directory_stream.read() {
        if progress.cancel.load(Ordering::Relaxed) {
            finished = false;
            break;
        }
        let entry = entry?;
        let name = entry.file_name();
        if name.to_bytes() == b"." || name.to_bytes() == b".." {
            continue;
        }
        if continuing && seen.contains(name.to_bytes()) {
            continue;
        }
        let (info, flags) = match rustix::fs::statat(
            directory_stream.fd()?,
            name,
            rustix::fs::AtFlags::SYMLINK_NOFOLLOW,
        ) {
            Ok(metadata) => (Info::from(&metadata), 0),
            Err(rustix::io::Errno::NOENT) => continue,
            Err(_) => (
                Info {
                    identity: Identity {
                        device: before.identity.device,
                        inode: entry.ino(),
                    },
                    kind: Kind::Other,
                    size: 0,
                    blocks: 0,
                    links: 1,
                    mtime: 0,
                    mtime_ns: 0,
                    ctime: 0,
                    ctime_ns: 0,
                },
                UNREADABLE,
            ),
        };
        let name: Box<[u8]> = name.to_bytes().into();
        let directory = if info.kind == Kind::Directory {
            previous_dirs
                .get(name.as_ref())
                .filter(|(identity, _)| *identity == info.identity)
                .map_or(0, |(_, id)| *id)
        } else {
            0
        };
        entries.push(RawEntry {
            name,
            info,
            directory,
            flags,
        });
        if entries.len() == CHUNK {
            if !send(
                sender,
                Found::Entries(work.directory, std::mem::take(&mut entries)),
            ) {
                return Ok(());
            }
            entries = Vec::with_capacity(CHUNK);
        }
    }
    if !entries.is_empty() && !send(sender, Found::Entries(work.directory, entries)) {
        return Ok(());
    }
    let after = Info::from(&directory_stream.stat()?);
    let changed = before != after;
    send(
        sender,
        Found::End(
            work.directory,
            after,
            finished && !changed,
            changed.then(|| "directory changed during enumeration".to_owned()),
        ),
    );
    Ok(())
}

fn append(tree: &mut Tree, journal: &mut Journal, event: Event) -> Result<()> {
    let sequence = journal.append(&event)?;
    tree.apply(&event, sequence)
}

fn children(tree: &Tree, id: u32) -> Vec<u32> {
    tree.dir(id)
        .map(|dir| {
            dir.entries()
                .filter(|entry| entry.directory != 0 && entry.flags & EXCLUDED == 0)
                .map(|entry| entry.directory)
                .collect()
        })
        .unwrap_or_default()
}

pub fn scan(
    mut cache: Cache,
    options: &ScanOptions,
    threads: usize,
    progress: Arc<Progress>,
    preview: Option<Sender<Arc<Tree>>>,
) -> Result<ScanResult> {
    ensure!(
        (1..=8).contains(&threads),
        "scanner threads must be between 1 and 8"
    );
    options.audit.validate()?;
    let started = Instant::now();
    let lock = Arc::new(cache.lock()?);
    cache.base = fs::canonicalize(&cache.base)?;
    let mut loaded = journal::load(&cache)?;
    if loaded.tree.directories.len() > 1 {
        loaded.tree.aggregate()?;
        if let Some(preview) = preview {
            let _ = preview.send(Arc::new(loaded.tree.clone()));
        }
    }
    let had_cache = loaded.tree.directories.len() > 1;
    let was_complete = had_cache && loaded.tree.is_complete();
    let root_info = Info::from(&fs::symlink_metadata(&cache.root)?);
    ensure!(
        root_info.kind == Kind::Directory,
        "scan root is not a directory"
    );
    ensure!(
        !cache.root.starts_with(&cache.base),
        "cannot scan cdu's cache"
    );
    let full = (options.exact && was_complete)
        || !had_cache
        || loaded.tree.dir(ROOT as u32)?.info.identity != root_info.identity;
    let resumed = had_cache && !was_complete && !full;
    let mut journal = Journal::open(&cache, &loaded)?;
    let mut tree = loaded.tree;
    if full {
        append(
            &mut tree,
            &mut journal,
            Event::Start {
                root: cache.root.as_os_str().as_bytes().to_vec(),
                info: root_info,
            },
        )?;
    }
    let mut counters = Counters {
        metadata_calls: 1,
        ..Counters::default()
    };
    progress.partial.store(full || resumed, Ordering::Relaxed);
    progress.size_ready.store(true, Ordering::Release);
    let mut live_disk = if full {
        root_info.disk()
    } else {
        tree.dir(ROOT as u32)?.totals.disk_bytes
    };
    let mut live_apparent = if full {
        root_info.size
    } else {
        tree.dir(ROOT as u32)?.totals.apparent_bytes
    };
    let mut live_links = HashSet::new();
    let mut known_dirs: HashMap<Identity, u32> = tree
        .directories
        .iter()
        .enumerate()
        .filter_map(|(id, dir)| dir.as_ref().map(|dir| (dir.info.identity, id as u32)))
        .collect();
    let mut filesystem_types = HashMap::new();
    let cache_inside_root = cache.base.starts_with(&cache.root);
    let (work_send, work_receive) = crossbeam_channel::bounded::<Work>(threads * 2);
    let (result_send, result_receive) = crossbeam_channel::bounded::<Found>(threads * 8);
    let mut todo = VecDeque::from([ROOT as u32]);
    let mut status = vec![0u8; tree.directories.len()];
    let mut attempts = vec![0u8; tree.directories.len()];
    status[ROOT as usize] = 1;
    let mut active = 0usize;
    let mut settled = true;
    let before_sequence = journal.sequence;
    thread::scope(|scope| -> Result<()> {
        for _ in 0..threads {
            let receiver = work_receive.clone();
            let sender = result_send.clone();
            let progress = progress.clone();
            scope.spawn(move || worker(receiver, sender, progress));
        }
        drop(work_receive);
        drop(result_send);
        let mut sender = Some(work_send);
        let schedule = |id: u32, todo: &mut VecDeque<u32>, status: &mut Vec<u8>| {
            if status.len() <= id as usize {
                status.resize(id as usize + 1, 0);
            }
            if status[id as usize] == 0 {
                status[id as usize] = 1;
                todo.push_back(id);
            }
        };
        let result = (|| -> Result<()> {
            loop {
                if progress.cancel.load(Ordering::Relaxed) {
                    todo.clear();
                    sender.take();
                }
                while sender.as_ref().is_some_and(|sender| !sender.is_full()) && !todo.is_empty() {
                    let id = todo.pop_front().unwrap();
                    let dir = tree.dir(id)?;
                    let cached = (dir.revision != 0 || dir.complete || dir.count() != 0)
                        .then(|| dir.clone());
                    let work = Work {
                        directory: id,
                        path: tree.path(id)?,
                        info: dir.info,
                        cached,
                        force: options.exact && !resumed,
                    };
                    counters.metadata_calls += u64::from(work.cached.is_some());
                    sender.as_ref().unwrap().send(work)?;
                    status[id as usize] = 2;
                    if attempts.len() <= id as usize {
                        attempts.resize(id as usize + 1, 0);
                    }
                    attempts[id as usize] += 1;
                    active += 1;
                }
                if active == 0 {
                    sender.take();
                    break;
                }
                let found = result_receive
                    .recv()
                    .context("metadata workers stopped unexpectedly")?;
                let mut finished = None;
                match found {
                    Found::Begin(id, info, clear) => {
                        counters.directories_enumerated += 1;
                        if clear {
                            append(
                                &mut tree,
                                &mut journal,
                                Event::Begin {
                                    directory: id,
                                    info,
                                },
                            )?;
                        } else {
                            counters.reused_files +=
                                tree.dir(id)?
                                    .entries()
                                    .filter(|entry| entry.info.kind != Kind::Directory)
                                    .count() as u64;
                        }
                    }
                    Found::Entries(id, mut entries) => {
                        let mut parent_path = None;
                        let mut next = tree.directories.len() as u32;
                        for entry in &mut entries {
                            counters.metadata_calls += 1;
                            if entry.info.kind != Kind::Directory {
                                counters.files_statted += 1;
                            }
                            if cache.one_file_system
                                && entry.info.identity.device != root_info.identity.device
                            {
                                entry.flags |= EXCLUDED;
                                entry.directory = 0;
                            }
                            if entry.info.kind == Kind::Directory && entry.flags & EXCLUDED == 0 {
                                let device = entry.info.identity.device;
                                let excluded = if cache_inside_root
                                    || (device != root_info.identity.device
                                        && !filesystem_types.contains_key(&device))
                                {
                                    if parent_path.is_none() {
                                        parent_path = Some(tree.path(id)?);
                                    }
                                    let path = parent_path
                                        .as_ref()
                                        .unwrap()
                                        .join(OsStr::from_bytes(&entry.name));
                                    path.starts_with(&cache.base)
                                        || excluded_filesystem(&path, device, &mut filesystem_types)
                                } else if let Some(&excluded) = filesystem_types.get(&device) {
                                    excluded
                                } else {
                                    excluded_filesystem(&cache.root, device, &mut filesystem_types)
                                };
                                if excluded {
                                    entry.flags |= EXCLUDED;
                                    entry.directory = 0;
                                } else if entry.directory == 0 {
                                    if let Some(&old) = known_dirs.get(&entry.info.identity) {
                                        let same_object = tree
                                            .path(old)
                                            .ok()
                                            .and_then(|path| fs::symlink_metadata(path).ok())
                                            .is_some_and(|meta| {
                                                Info::from(&meta).identity == entry.info.identity
                                            });
                                        if same_object
                                            || old == id
                                            || old >= tree.directories.len() as u32
                                        {
                                            entry.flags |= EXCLUDED;
                                        } else {
                                            entry.directory = old;
                                        }
                                    } else {
                                        entry.directory = next;
                                        next += 1;
                                        known_dirs.insert(entry.info.identity, entry.directory);
                                    }
                                }
                            }
                            if full && entry.flags & EXCLUDED == 0 {
                                let count = entry.info.kind == Kind::Directory
                                    || entry.info.links <= 1
                                    || live_links.insert(entry.info.identity);
                                if count {
                                    live_disk = live_disk.saturating_add(entry.info.disk());
                                    live_apparent = live_apparent.saturating_add(entry.info.size);
                                }
                            }
                        }
                        let chunk = Arc::new(Chunk {
                            start: tree.dir(id)?.count() as u32,
                            entries: entries.into_boxed_slice(),
                        });
                        let child_ids: Vec<_> = chunk
                            .entries
                            .iter()
                            .filter(|entry| entry.directory != 0)
                            .map(|entry| entry.directory)
                            .collect();
                        append(
                            &mut tree,
                            &mut journal,
                            Event::Entries {
                                directory: id,
                                chunk,
                            },
                        )?;
                        for child in child_ids {
                            schedule(child, &mut todo, &mut status);
                        }
                    }
                    Found::End(id, info, complete, error) => {
                        counters.metadata_calls += 1;
                        append(
                            &mut tree,
                            &mut journal,
                            Event::End {
                                directory: id,
                                info,
                                complete,
                                error: error.clone(),
                            },
                        )?;
                        if !complete
                            && !progress.cancel.load(Ordering::Relaxed)
                            && attempts[id as usize] < 3
                        {
                            status[id as usize] = 0;
                            schedule(id, &mut todo, &mut status);
                        } else {
                            if !complete && !progress.cancel.load(Ordering::Relaxed) {
                                settled = false;
                            }
                            status[id as usize] = 3;
                        }
                        finished = Some(id);
                    }
                    Found::Reused(id) => {
                        counters.reused_directories += 1;
                        counters.reused_files += tree
                            .dir(id)?
                            .entries()
                            .filter(|entry| entry.info.kind != Kind::Directory)
                            .count() as u64;
                        status[id as usize] = 3;
                        finished = Some(id);
                    }
                    Found::Failed(id, error) => {
                        let info = tree.dir(id)?.info;
                        append(
                            &mut tree,
                            &mut journal,
                            Event::End {
                                directory: id,
                                info,
                                complete: true,
                                error: Some(error),
                            },
                        )?;
                        status[id as usize] = 3;
                        finished = Some(id);
                    }
                    Found::Cancelled(id) => {
                        status[id as usize] = 3;
                        finished = Some(id);
                    }
                }
                if let Some(id) = finished {
                    counters.directories_checked += 1;
                    active -= 1;
                    if !progress.cancel.load(Ordering::Relaxed) {
                        for child in children(&tree, id) {
                            schedule(child, &mut todo, &mut status);
                        }
                    }
                }
                progress
                    .directories
                    .store(counters.directories_checked, Ordering::Relaxed);
                progress
                    .files
                    .store(counters.files_statted, Ordering::Relaxed);
                progress.disk_bytes.store(live_disk, Ordering::Relaxed);
                progress
                    .apparent_bytes
                    .store(live_apparent, Ordering::Relaxed);
            }
            Ok(())
        })();
        if result.is_err() {
            progress.cancel.store(true, Ordering::Relaxed);
            sender.take();
            drop(result_receive);
        }
        result
    })?;
    tree.aggregate()?;
    let cancelled = progress.cancel.load(Ordering::Relaxed);
    let mut audit = None;
    if !full && !cancelled {
        audit = Some(audit_files(
            &mut tree,
            &mut journal,
            &options.audit,
            &mut counters,
            &progress,
        )?);
    }
    let total = tree.dir(ROOT as u32)?.totals;
    let valid_audit = audit
        .as_ref()
        .is_none_or(|audit| audit.status != "incomplete");
    counters.walk_us = started.elapsed().as_micros() as u64;
    counters.elapsed_us = counters.walk_us;
    counters.checkpoint_records = journal.sequence - before_sequence;
    let report = Report {
        index_generation: journal.sequence,
        mode: if full { "full" } else { "incremental" }.to_owned(),
        reason: if resumed {
            "resuming checkpointed scan"
        } else if full {
            "parallel full metadata scan"
        } else {
            "directory validation and file audit"
        }
        .to_owned(),
        completed_at_unix_ms: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64,
        complete: !cancelled && tree.is_complete() && total.errors == 0 && valid_audit,
        settled: settled && !cancelled && valid_audit,
        totals: total,
        counters,
        audit,
        warning: if cancelled {
            Some("Paused; scan checkpoints will resume next time.".to_owned())
        } else if !full {
            Some("Cached file sizes may be stale; file-audit bounds remain conditional.".to_owned())
        } else if total.errors > 0 {
            Some("Some metadata was unreadable; totals are incomplete.".to_owned())
        } else {
            None
        },
    };
    append(
        &mut tree,
        &mut journal,
        Event::Report(Box::new(report.clone())),
    )?;
    progress
        .disk_bytes
        .store(total.disk_bytes, Ordering::Relaxed);
    progress
        .apparent_bytes
        .store(total.apparent_bytes, Ordering::Relaxed);
    Ok(ScanResult {
        tree: Arc::new(tree),
        report,
        lock,
        resumed,
    })
}

fn excluded_filesystem(path: &Path, device: u64, known: &mut HashMap<u64, bool>) -> bool {
    if let Some(excluded) = known.get(&device) {
        return *excluded;
    }
    let Ok(path) = std::ffi::CString::new(path.as_os_str().as_bytes()) else {
        return false;
    };
    let mut value = std::mem::MaybeUninit::<libc::statfs>::uninit();
    // The path is NUL-terminated and statfs initializes value on success.
    let excluded = if unsafe { libc::statfs(path.as_ptr(), value.as_mut_ptr()) } == 0 {
        matches!(
            unsafe { value.assume_init() }.f_type as u64,
            0x9fa0
                | 0x62656572
                | 0x1cd1
                | 0x27e0eb
                | 0x63677270
                | 0x64626720
                | 0xcafe4a11
                | 0x74726163
                | 0x67596969
                | 0x19800202
                | 0x6e736673
        )
    } else {
        false
    };
    known.insert(device, excluded);
    excluded
}

fn audit_files(
    tree: &mut Tree,
    journal: &mut Journal,
    options: &AuditOptions,
    counters: &mut Counters,
    progress: &Progress,
) -> Result<estimate::Audit> {
    let mut population = Vec::new();
    let mut largest = BinaryHeap::new();
    for (id, active) in tree.active.iter().enumerate() {
        if !active {
            continue;
        }

        let dir = tree.dir(id as u32)?;
        for (at, entry) in dir.entries().enumerate() {
            if entry.directory == 0 && entry.flags & EXCLUDED == 0 && dir.counted[at] == 1 {
                let key = (id as u32, at as u32);
                population.push(key);
                if options.priority > 0 {
                    largest.push(std::cmp::Reverse((entry.info.size, key)));
                    if largest.len() > options.priority {
                        largest.pop();
                    }
                }
            }
        }
    }
    let mut verified = HashSet::new();
    let mut valid = true;
    let refresh = |tree: &mut Tree,
                   journal: &mut Journal,
                   key: (u32, u32),
                   counters: &mut Counters|
     -> Result<bool> {
        let entry = tree
            .dir(key.0)?
            .entry(key.1 as usize)
            .context("missing audit entry")?
            .clone();
        let path = tree.path(key.0)?.join(OsStr::from_bytes(&entry.name));
        counters.files_statted += 1;
        counters.metadata_calls += 1;
        match fs::symlink_metadata(path) {
            Ok(metadata) => {
                let info = Info::from(&metadata);
                if info.identity != entry.info.identity || info.kind != entry.info.kind {
                    return Ok(false);
                }
                if info != entry.info {
                    append(
                        tree,
                        journal,
                        Event::Patch {
                            directory: key.0,
                            entry: key.1,
                            info,
                            flags: entry.flags & !UNREADABLE,
                        },
                    )?;
                }
                Ok(true)
            }
            Err(_) => Ok(false),
        }
    };
    for std::cmp::Reverse((_, key)) in largest {
        if progress.cancel.load(Ordering::Relaxed) {
            valid = false;
            break;
        }
        valid &= refresh(tree, journal, key, counters)?;
        verified.insert(key);
    }
    tree.aggregate()?;
    let baseline = tree.dir(ROOT as u32)?.totals;
    let mut samples = Samples::default();
    let selected = rand::seq::index::sample(
        &mut rand::rng(),
        population.len(),
        options.sample.min(population.len()),
    );
    for at in selected.iter() {
        if progress.cancel.load(Ordering::Relaxed) {
            valid = false;
            break;
        }
        let key = population[at];
        let old = tree.dir(key.0)?.entry(key.1 as usize).unwrap().info;
        if !verified.contains(&key) {
            valid &= refresh(tree, journal, key, counters)?;
            verified.insert(key);
        }
        let new = tree.dir(key.0)?.entry(key.1 as usize).unwrap().info;
        samples.observe(
            new.disk() as i128 - old.disk() as i128,
            new.size as i128 - old.size as i128,
        );
    }
    tree.aggregate()?;
    let recorded = tree.dir(ROOT as u32)?.totals;
    progress
        .files
        .store(counters.files_statted, Ordering::Relaxed);
    Ok(estimate::calculate(
        baseline,
        recorded,
        population.len(),
        verified.len(),
        &samples,
        options,
        valid && recorded.errors == 0,
    ))
}
