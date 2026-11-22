use crate::{
    cache::Cache,
    db::{FILE_BATCH_SIZE, Store, indexed_path, integer, record},
    estimate::{self, AuditOptions, Samples},
    model::{Counters, Delta, Id, Identity, Info, Kind, ROOT, Record, Report, Totals},
};
use anyhow::{Context, Result, bail, ensure};
use rusqlite::params;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    ffi::{CString, OsStr},
    fs, io,
    os::unix::{
        ffi::{OsStrExt, OsStringExt},
        fs::DirEntryExt,
    },
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};

#[derive(Clone, Debug, Default)]
pub struct Options {
    pub exact: bool,
    pub audit: AuditOptions,
}

#[derive(Default)]
pub struct Progress {
    pub directories: AtomicU64,
    pub files: AtomicU64,
    pub disk_bytes: AtomicU64,
    pub apparent_bytes: AtomicU64,
    pub size_ready: AtomicBool,
    pub partial: AtomicBool,
    pub cancel: AtomicBool,
    pub finalizing: AtomicBool,
}

pub struct Scanner {
    cache: Cache,
    store: Store,
    directories: HashMap<Id, Record>,
    children: HashMap<Id, Vec<Id>>,
    identities: HashMap<Identity, Id>,
    deltas: HashMap<Id, Delta>,
    fresh: HashSet<Id>,
    new_directories: HashSet<Id>,
    retry_parents: HashSet<Id>,
    pending: Vec<(Id, Id, Vec<u8>)>,
    pending_files: Vec<Record>,
    orphans: HashSet<Id>,
    filesystems: HashMap<u64, Option<String>>,
    next_id: Id,
    counters: Counters,
    live_totals: Totals,
    full: bool,
    settled: bool,
    progress: Arc<Progress>,
}

impl Scanner {
    pub fn open(cache: Cache) -> Result<Self> {
        Self::with_progress(cache, Arc::new(Progress::default()))
    }

    pub fn with_progress(mut cache: Cache, progress: Arc<Progress>) -> Result<Self> {
        let store = Store::open(&cache)?;
        cache.base = fs::canonicalize(&cache.base)?;
        Ok(Self {
            cache,
            store,
            directories: HashMap::new(),
            children: HashMap::new(),
            identities: HashMap::new(),
            deltas: HashMap::new(),
            fresh: HashSet::new(),
            new_directories: HashSet::new(),
            retry_parents: HashSet::new(),
            pending: Vec::new(),
            pending_files: Vec::new(),
            orphans: HashSet::new(),
            filesystems: HashMap::new(),
            next_id: ROOT,
            counters: Counters::default(),
            live_totals: Totals::default(),
            full: false,
            settled: true,
            progress,
        })
    }

    pub fn refresh(&mut self, options: &Options) -> Result<Report> {
        options.audit.validate()?;
        let start = Instant::now();
        let progress = self.progress.clone();
        self.store.conn.progress_handler(
            10_000,
            Some(move || progress.cancel.load(Ordering::Relaxed)),
        )?;
        self.store.conn.execute_batch("BEGIN IMMEDIATE")?;
        let result = self.refresh_inner(options);
        if let Err(error) = self.store.conn.progress_handler(0, None::<fn() -> bool>) {
            let _ = self.store.conn.execute_batch("ROLLBACK");
            return Err(error.into());
        }
        match result {
            Ok(mut report) => {
                report.counters.elapsed_us = start.elapsed().as_micros() as u64;
                let commit = (|| -> Result<()> {
                    self.check_cancel()?;
                    let generation: u64 = self
                        .store
                        .meta("generation")?
                        .map(|bytes| serde_json::from_slice(&bytes))
                        .transpose()?
                        .unwrap_or(0);
                    let changed =
                        report.counters.rows_written > 0 || self.store.meta("report")?.is_none();
                    report.index_generation = if changed {
                        generation
                            .checked_add(1)
                            .context("cache generation overflow")?
                    } else {
                        generation
                    };
                    if changed {
                        self.store
                            .set_meta("root", self.cache.root.as_os_str().as_bytes())?;
                        self.store.set_meta(
                            "generation",
                            &serde_json::to_vec(&report.index_generation)?,
                        )?;
                        self.store
                            .set_meta("report", &serde_json::to_vec(&report)?)?;
                    }
                    self.store.conn.execute_batch("COMMIT")?;
                    Ok(())
                })();
                if let Err(error) = commit {
                    let _ = self.store.conn.execute_batch("ROLLBACK");
                    return Err(error);
                }
                report.counters.elapsed_us = start.elapsed().as_micros() as u64;
                Ok(report)
            }
            Err(error) => {
                let _ = self.store.conn.execute_batch("ROLLBACK");
                Err(error)
            }
        }
    }

    fn refresh_inner(&mut self, options: &Options) -> Result<Report> {
        self.directories.clear();
        self.children.clear();
        self.identities.clear();
        self.deltas.clear();
        self.fresh.clear();
        self.new_directories.clear();
        self.retry_parents.clear();
        self.pending.clear();
        self.pending_files.clear();
        self.orphans.clear();
        self.filesystems.clear();
        self.counters = Counters::default();
        self.live_totals = Totals::default();
        self.settled = true;
        self.progress.size_ready.store(false, Ordering::Release);
        self.progress.directories.store(0, Ordering::Relaxed);
        self.progress.files.store(0, Ordering::Relaxed);
        self.progress.finalizing.store(false, Ordering::Relaxed);
        self.check_cancel()?;
        let metadata = fs::symlink_metadata(&self.cache.root).context("reading scan root")?;
        self.counters.metadata_calls += 1;
        ensure!(metadata.is_dir(), "scan root is not a directory");
        ensure!(
            !self.cache.root.starts_with(&self.cache.base),
            "cannot index cdu's own cache directory"
        );
        let info = Info::from(&metadata);
        let old_root = self.store.get(ROOT)?;
        let reason = if options.exact {
            "explicit full metadata scan"
        } else if old_root.is_none() {
            "first index"
        } else if old_root.as_ref().unwrap().info.identity != info.identity {
            "root identity changed"
        } else {
            "directory signatures and bounded file audit"
        };
        self.full = options.exact
            || old_root
                .as_ref()
                .is_none_or(|old| old.info.identity != info.identity);
        if self.full {
            self.store.begin_bulk()?;
            self.next_id = ROOT;
            self.insert(None, Vec::new(), info, None, None)?;
            self.publish_progress();
        } else {
            self.live_totals = old_root.as_ref().unwrap().totals;
            self.publish_progress();
            let records = self
                .store
                .conn
                .prepare("SELECT * FROM nodes WHERE kind=1")?
                .query_map([], record)?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            for node in records {
                self.remember_directory(node);
            }
            self.next_id = self.store.conn.query_row(
                "SELECT COALESCE(MAX(id),0)+1 FROM nodes",
                [],
                |row| row.get(0),
            )?;
            self.retry_parents = self.store.conn.prepare(
                "SELECT DISTINCT parent FROM nodes WHERE error IS NOT NULL AND parent IS NOT NULL"
            )?.query_map([], |row| row.get(0))?.collect::<rusqlite::Result<_>>()?;
        }
        let walk_started = Instant::now();
        self.walk()?;
        self.flush_file_rows()?;
        let removals = std::mem::take(&mut self.pending);
        for (id, parent, name) in removals {
            if self
                .directories
                .get(&id)
                .is_some_and(|node| node.parent == Some(parent) && node.name == name)
            {
                self.remove_tree(id)?;
            }
        }
        if self.full {
            self.propagate_full_totals()?;
        }
        self.flush_totals()?;
        self.counters.walk_us = walk_started.elapsed().as_micros() as u64;
        if self.full {
            self.progress.finalizing.store(true, Ordering::Relaxed);
            self.publish_progress();
            let index_started = Instant::now();
            self.store.finish_bulk()?;
            self.counters.index_us = index_started.elapsed().as_micros() as u64;
        }
        let audit = if self.full {
            None
        } else {
            Some(self.audit(&options.audit)?)
        };
        self.flush_totals()?;
        let totals = self
            .store
            .get(ROOT)?
            .context("missing root after scan")?
            .totals;
        let warning = if totals.errors > 0 {
            Some("Some entries could not be read. Totals are incomplete; no error bound is certified.".to_owned())
        } else if !self.settled {
            Some(
                "The tree changed during validation. Rerun before relying on an error bound."
                    .to_owned(),
            )
        } else if !self.full {
            Some("Directory timestamps do not reveal in-place file growth. Unchecked file sizes remain cached; statistical bounds assume complete membership and a stable tree.".to_owned())
        } else {
            None
        };
        debug_assert_eq!(self.live_totals, totals);
        self.publish_progress();
        Ok(Report {
            index_generation: 0,
            mode: if self.full { "full" } else { "incremental" }.to_owned(),
            reason: reason.to_owned(),
            completed_at_unix_ms: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64,
            complete: totals.errors == 0,
            settled: self.settled,
            totals,
            counters: self.counters.clone(),
            audit,
            warning,
        })
    }

    fn check_cancel(&self) -> Result<()> {
        ensure!(
            !self.progress.cancel.load(Ordering::Relaxed),
            "scan cancelled"
        );
        Ok(())
    }

    fn update_live_totals(&mut self, old: Totals, new: Totals) -> Result<()> {
        self.live_totals = Delta::between(old, new).apply(self.live_totals)?;
        Ok(())
    }

    fn publish_progress(&self) {
        self.progress
            .directories
            .store(self.counters.directories_checked, Ordering::Relaxed);
        self.progress
            .files
            .store(self.counters.files_statted, Ordering::Relaxed);
        self.progress
            .disk_bytes
            .store(self.live_totals.disk_bytes, Ordering::Relaxed);
        self.progress
            .apparent_bytes
            .store(self.live_totals.apparent_bytes, Ordering::Relaxed);
        self.progress.partial.store(self.full, Ordering::Relaxed);
        self.progress.size_ready.store(true, Ordering::Release);
    }

    fn remember_directory(&mut self, node: Record) {
        if let Some(parent) = node.parent {
            self.children.entry(parent).or_default().push(node.id);
        }
        if node.excluded.is_none() {
            self.identities.insert(node.info.identity, node.id);
        }
        self.directories.insert(node.id, node);
    }

    fn directory_path(&self, mut id: Id) -> Result<PathBuf> {
        let mut parts = Vec::new();
        while id != ROOT {
            ensure!(parts.len() < 4096, "invalid directory ancestry");
            let node = self
                .directories
                .get(&id)
                .context("directory disappeared from cache")?;
            parts.push(node.name.as_slice());
            id = node.parent.context("detached directory")?;
        }
        let mut path = self.cache.root.clone();
        for name in parts.into_iter().rev() {
            path.push(OsStr::from_bytes(name));
        }
        Ok(path)
    }

    fn add_delta(&mut self, mut id: Option<Id>, delta: Delta) -> Result<()> {
        while let Some(current) = id {
            self.deltas.entry(current).or_default().add(delta);
            if self.full {
                break;
            }
            id = self
                .directories
                .get(&current)
                .context("missing aggregate ancestor")?
                .parent;
        }
        Ok(())
    }

    fn current_totals(&self, id: Id) -> Result<Totals> {
        let node = self.directories.get(&id).context("missing directory")?;
        self.deltas
            .get(&id)
            .copied()
            .unwrap_or_default()
            .apply(node.totals)
    }

    fn change_child_count(&mut self, parent: Option<Id>, delta: i64) -> Result<()> {
        if let Some(parent) = parent {
            let node = self
                .directories
                .get_mut(&parent)
                .context("missing parent")?;
            node.direct_children = node
                .direct_children
                .checked_add_signed(delta)
                .context("invalid direct child count")?;
            self.deltas.entry(parent).or_default();
        }
        Ok(())
    }

    fn insert(
        &mut self,
        parent: Option<Id>,
        name: Vec<u8>,
        info: Info,
        excluded: Option<String>,
        error: Option<String>,
    ) -> Result<Id> {
        let mut counted = true;
        let mut refresh_links = false;
        if info.kind != Kind::Directory && info.links > 1 && excluded.is_none() {
            self.flush_file_rows()?;
            if let Some(owner) = self.store.owner(info.identity)? {
                counted = false;
                refresh_links = owner.info != info || owner.error.is_some();
            }
        }
        let mut node = Record {
            id: self.next_id,
            parent,
            name,
            info,
            counted,
            excluded,
            error,
            totals: Totals::default(),
            direct_children: 0,
        };
        self.next_id += 1;
        node.totals = node.own();
        let batch = self.full
            && info.kind != Kind::Directory
            && info.links == 1
            && node.counted
            && node.excluded.is_none()
            && node.error.is_none();
        if !batch {
            self.store.insert(&node)?;
        }
        self.counters.rows_written += 1;
        self.add_delta(parent, Delta::between(Totals::default(), node.totals))?;
        self.update_live_totals(Totals::default(), node.totals)?;
        self.change_child_count(parent, 1)?;
        if batch {
            let id = node.id;
            self.pending_files.push(node);
            if self.pending_files.len() == FILE_BATCH_SIZE {
                self.flush_file_rows()?;
            }
            return Ok(id);
        }
        if info.kind == Kind::Directory {
            self.new_directories.insert(node.id);
            self.remember_directory(node.clone());
        } else if node.excluded.is_none() && node.counted && !self.full {
            self.store.add_slot(node.id)?;
            if node.error.is_none() {
                self.fresh.insert(node.id);
            }
        }
        if refresh_links && node.error.is_none() {
            self.update_file(node.clone(), info, None)?;
        }
        Ok(node.id)
    }

    fn flush_file_rows(&mut self) -> Result<()> {
        let mut start = 0;
        while start < self.pending_files.len() {
            // Power-of-two tails keep the prepared-statement set bounded.
            let count = 1 << (self.pending_files.len() - start).ilog2();
            self.store
                .insert_file_batch(&self.pending_files[start..start + count])?;
            start += count;
        }
        self.pending_files.clear();
        Ok(())
    }

    fn update_directory(
        &mut self,
        mut node: Record,
        info: Info,
        error: Option<String>,
    ) -> Result<()> {
        let old = node.own();
        if node.info == info && node.error == error {
            return Ok(());
        }
        node.info = info;
        node.error = error;
        self.add_delta(Some(node.id), Delta::between(old, node.own()))?;
        self.update_live_totals(old, node.own())?;
        self.store.update(&node)?;
        self.counters.rows_written += 1;
        self.directories.insert(node.id, node);
        Ok(())
    }

    fn update_file(&mut self, node: Record, info: Info, error: Option<String>) -> Result<()> {
        let members = if node.info.links > 1 || info.links > 1 {
            self.flush_file_rows()?;
            self.store.members(node.info.identity)?
        } else {
            vec![node]
        };
        for mut member in members {
            if member.info == info && member.error == error {
                if !self.full && member.counted && member.excluded.is_none() && error.is_none() {
                    self.fresh.insert(member.id);
                }
                continue;
            }
            let old = member.own();
            member.info = info;
            member.error = error.clone();
            member.totals = member.own();
            self.add_delta(member.parent, Delta::between(old, member.totals))?;
            self.update_live_totals(old, member.totals)?;
            self.store.update(&member)?;
            self.counters.rows_written += 1;
            if !self.full && member.counted && member.excluded.is_none() && error.is_none() {
                self.fresh.insert(member.id);
            }
        }
        Ok(())
    }

    fn walk(&mut self) -> Result<()> {
        let mut work = VecDeque::from([(ROOT, self.full)]);
        let mut visited: HashMap<Id, usize> = HashMap::new();
        while let Some((id, force)) = work.pop_front() {
            self.check_cancel()?;
            let Some(node) = self.directories.get(&id).cloned() else {
                continue;
            };
            if node.excluded.is_some()
                || self.orphans.contains(&id)
                || (!force && visited.contains_key(&id))
            {
                continue;
            }
            let attempts = visited.entry(id).or_default();
            *attempts += 1;
            if *attempts > 3 {
                self.settled = false;
                continue;
            }
            let path = self.directory_path(id)?;
            self.counters.directories_checked += 1;
            self.counters.metadata_calls += 1;
            self.publish_progress();
            let info = match fs::symlink_metadata(&path) {
                Ok(metadata) => Info::from(&metadata),
                Err(error) if error.kind() == io::ErrorKind::NotFound && id != ROOT => {
                    self.pending.push((id, node.parent.unwrap(), node.name));
                    continue;
                }
                Err(error) => {
                    self.update_directory(node.clone(), node.info, Some(error.to_string()))?;
                    continue;
                }
            };
            if info.identity != node.info.identity || info.kind != Kind::Directory {
                if let Some(parent) = node.parent {
                    work.push_back((parent, true));
                    continue;
                }
                bail!("scan root changed identity during validation");
            }
            let new_directory = self.new_directories.remove(&id);
            let enumerate = force
                || new_directory
                || node.info != info
                || node.error.is_some()
                || self.retry_parents.contains(&id);
            self.update_directory(node, info, None)?;
            if enumerate {
                self.enumerate(id, &path, new_directory)?;
                self.counters.metadata_calls += 1;
                match fs::symlink_metadata(&path) {
                    Ok(after) if Info::from(&after) != info => work.push_back((id, true)),
                    Err(_) => work.push_back((id, true)),
                    _ => {}
                }
            }
            if let Some(children) = self.children.get(&id) {
                work.extend(children.iter().map(|&child| (child, false)));
            }
        }
        Ok(())
    }

    fn enumerate(&mut self, id: Id, path: &Path, new_directory: bool) -> Result<()> {
        self.counters.directories_enumerated += 1;
        let entries = match fs::read_dir(path) {
            Ok(entries) => entries,
            Err(error) => {
                let node = self.directories[&id].clone();
                self.update_directory(node.clone(), node.info, Some(error.to_string()))?;
                return Ok(());
            }
        };
        let mut old: HashMap<_, _> = if new_directory {
            HashMap::new()
        } else {
            self.flush_file_rows()?;
            self.store
                .children(id)?
                .into_iter()
                .map(|node| (node.name.clone(), node))
                .collect()
        };
        for entry in entries {
            self.check_cancel()?;
            if self.counters.files_statted & 127 == 0 {
                self.publish_progress();
            }
            let entry = match entry {
                Ok(entry) => entry,
                Err(error) => {
                    let node = self.directories[&id].clone();
                    self.update_directory(node.clone(), node.info, Some(error.to_string()))?;
                    return Ok(());
                }
            };
            let name = entry.file_name().into_vec();
            let previous = old.remove(&name).filter(|previous| {
                previous.info.kind != Kind::Directory
                    || self.directories.get(&previous.id).is_some_and(|current| {
                        current.parent == previous.parent && current.name == previous.name
                    })
            });
            self.counters.metadata_calls += 1;
            // DirEntry::metadata does not follow symlinks.
            let info = match entry.metadata() {
                Ok(metadata) => Info::from(&metadata),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    if let Some(previous) = previous {
                        self.remove_or_defer(previous)?;
                    }
                    self.settled = false;
                    continue;
                }
                Err(error) => {
                    if let Some(previous) = previous {
                        if previous.info.kind == Kind::Directory {
                            self.orphans.insert(previous.id);
                            self.update_directory(
                                previous.clone(),
                                previous.info,
                                Some(error.to_string()),
                            )?;
                        } else {
                            self.update_file(
                                previous.clone(),
                                previous.info,
                                Some(error.to_string()),
                            )?;
                        }
                    } else {
                        let info = Info {
                            identity: Identity {
                                device: self.directories[&id].info.identity.device,
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
                        };
                        self.insert(Some(id), name, info, None, Some(error.to_string()))?;
                    }
                    continue;
                }
            };
            if info.kind != Kind::Directory {
                self.counters.files_statted += 1;
            }
            if let Some(previous) = previous {
                if previous.info.identity == info.identity && previous.info.kind == info.kind {
                    if info.kind == Kind::Directory {
                        // Leave the previous directory signature intact until its own validation.
                        continue;
                    }
                    self.update_file(previous, info, None)?;
                    continue;
                }
                if previous.info.kind == Kind::Directory {
                    self.pending.push((previous.id, id, previous.name.clone()));
                    self.store
                        .conn
                        .execute("UPDATE nodes SET parent=NULL WHERE id=?1", [previous.id])?;
                } else {
                    self.remove_tree(previous.id)?;
                }
            }
            let mut excluded = if self.cache.one_file_system
                && info.identity.device != self.directories[&ROOT].info.identity.device
            {
                Some("another filesystem (-x)".to_owned())
            } else {
                None
            };
            if info.kind == Kind::Directory {
                let child_path = path.join(OsStr::from_bytes(&name));
                if child_path.starts_with(&self.cache.base) {
                    excluded = Some("cdu metadata cache".to_owned());
                }
                if excluded.is_none() {
                    excluded = self.filesystem_exclusion(info.identity.device, &child_path)?;
                }
                if excluded.is_none()
                    && let Some(&existing) = self.identities.get(&info.identity)
                {
                    if self.can_move(existing, id)? {
                        self.move_directory(existing, id, name)?;
                        continue;
                    }
                    excluded = Some("directory alias already counted".to_owned());
                }
            }
            self.insert(Some(id), name, info, excluded, None)?;
        }
        for previous in old.into_values() {
            self.remove_or_defer(previous)?;
        }
        Ok(())
    }

    fn remove_or_defer(&mut self, node: Record) -> Result<()> {
        if node.info.kind == Kind::Directory {
            self.pending
                .push((node.id, node.parent.unwrap(), node.name));
        } else {
            self.remove_tree(node.id)?;
        }
        Ok(())
    }

    fn can_move(&mut self, id: Id, mut parent: Id) -> Result<bool> {
        loop {
            if id == parent {
                return Ok(false);
            }
            match self.directories[&parent].parent {
                Some(next) => parent = next,
                None => break,
            }
        }
        self.counters.metadata_calls += 1;
        let old_path = self.directory_path(id)?;
        Ok(match fs::symlink_metadata(old_path) {
            Ok(metadata) => Info::from(&metadata).identity != self.directories[&id].info.identity,
            Err(error) => error.kind() == io::ErrorKind::NotFound,
        })
    }

    fn move_directory(&mut self, id: Id, parent: Id, name: Vec<u8>) -> Result<()> {
        self.orphans.remove(&id);
        let totals = if self.full {
            self.directories[&id].totals
        } else {
            self.current_totals(id)?
        };
        let mut node = self.directories[&id].clone();
        let old_parent = node.parent.context("cannot move the root")?;
        self.add_delta(Some(old_parent), Delta::between(totals, Totals::default()))?;
        self.add_delta(Some(parent), Delta::between(Totals::default(), totals))?;
        self.change_child_count(Some(old_parent), -1)?;
        self.change_child_count(Some(parent), 1)?;
        self.children
            .entry(old_parent)
            .or_default()
            .retain(|&child| child != id);
        self.children.entry(parent).or_default().push(id);
        node.parent = Some(parent);
        node.name = name;
        self.store.update(&node)?;
        self.counters.rows_written += 1;
        self.directories.insert(id, node);
        Ok(())
    }

    fn remove_tree(&mut self, id: Id) -> Result<()> {
        ensure!(id != ROOT, "cannot remove the scan root");
        self.flush_file_rows()?;
        let node = if let Some(node) = self.directories.get(&id) {
            node.clone()
        } else {
            let Some(node) = self.store.get(id)? else {
                return Ok(());
            };
            node
        };
        let totals = if node.info.kind == Kind::Directory && !self.full {
            self.current_totals(id)?
        } else {
            node.totals
        };
        self.add_delta(node.parent, Delta::between(totals, Totals::default()))?;
        self.change_child_count(node.parent, -1)?;
        let records = self.store.conn.prepare(
            "WITH RECURSIVE subtree(id) AS (
                SELECT ?1 UNION ALL SELECT nodes.id FROM nodes JOIN subtree ON nodes.parent=subtree.id
             ) SELECT nodes.* FROM nodes JOIN subtree ON nodes.id=subtree.id"
        )?.query_map([id], record)?.collect::<rusqlite::Result<Vec<_>>>()?;
        let mut transfers = HashSet::new();
        for item in &records {
            self.update_live_totals(item.own(), Totals::default())?;
            if !self.full {
                self.store.remove_slot(item.id)?;
            }
            self.fresh.remove(&item.id);
            if item.info.kind != Kind::Directory && item.counted && item.info.links > 1 {
                transfers.insert(item.info.identity);
            }
        }
        self.store.conn.execute(
            "WITH RECURSIVE subtree(id) AS (
                SELECT ?1 UNION ALL SELECT nodes.id FROM nodes JOIN subtree ON nodes.parent=subtree.id
             ) DELETE FROM nodes WHERE id IN (SELECT id FROM subtree)", [id],
        )?;
        self.counters.rows_written += records.len() as u64;
        for item in records {
            self.deltas.remove(&item.id);
            self.directories.remove(&item.id);
            self.children.remove(&item.id);
            if self.identities.get(&item.info.identity) == Some(&item.id) {
                self.identities.remove(&item.info.identity);
            }
            if let Some(parent) = item.parent {
                self.children
                    .entry(parent)
                    .or_default()
                    .retain(|&child| child != item.id);
            }
        }
        for identity in transfers {
            if let Some(mut replacement) = self
                .store
                .members(identity)?
                .into_iter()
                .find(|node| node.excluded.is_none())
            {
                if replacement.counted {
                    continue;
                }
                let old = replacement.totals;
                replacement.counted = true;
                replacement.totals = replacement.own();
                self.add_delta(replacement.parent, Delta::between(old, replacement.totals))?;
                self.update_live_totals(old, replacement.totals)?;
                self.store.update(&replacement)?;
                if !self.full {
                    self.store.add_slot(replacement.id)?;
                }
                self.counters.rows_written += 1;
            }
        }
        Ok(())
    }

    fn propagate_full_totals(&mut self) -> Result<()> {
        let mut order = Vec::with_capacity(self.directories.len());
        order.push(ROOT);
        let mut cursor = 0;
        while cursor < order.len() {
            let parent = order[cursor];
            if let Some(children) = self.children.get(&parent) {
                order.extend(children.iter().copied().filter(|child| {
                    self.directories
                        .get(child)
                        .is_some_and(|node| node.parent == Some(parent))
                }));
            }
            ensure!(
                order.len() <= self.directories.len(),
                "invalid directory graph"
            );
            cursor += 1;
        }
        ensure!(
            order.len() == self.directories.len(),
            "disconnected directory graph"
        );
        // Cold scans accumulate only direct contributions, then propagate once per directory.
        for id in order.into_iter().rev() {
            if let Some(parent) = self.directories[&id].parent {
                let delta = self.deltas.get(&id).copied().unwrap_or_default();
                self.deltas.entry(parent).or_default().add(delta);
            }
        }
        Ok(())
    }

    fn flush_totals(&mut self) -> Result<()> {
        let mut deltas = std::mem::take(&mut self.deltas);
        let mut ids: Vec<_> = deltas.keys().copied().collect();
        ids.sort_unstable();
        for id in ids {
            let delta = deltas.remove(&id).unwrap();
            let Some(node) = self.directories.get_mut(&id) else {
                continue;
            };
            node.totals = delta.apply(node.totals)?;
            self.store
                .conn
                .prepare_cached(
                    "UPDATE nodes SET disk_total=?2,apparent_total=?3,entries_total=?4,
                 errors_total=?5,excluded_total=?6,direct_children=?7 WHERE id=?1",
                )?
                .execute(params![
                    id,
                    integer(node.totals.disk_bytes)?,
                    integer(node.totals.apparent_bytes)?,
                    integer(node.totals.entries)?,
                    integer(node.totals.errors)?,
                    integer(node.totals.excluded)?,
                    integer(node.direct_children)?
                ])?;
            self.counters.rows_written += 1;
        }
        Ok(())
    }

    fn audit(&mut self, options: &AuditOptions) -> Result<estimate::Audit> {
        if options.priority > 0 {
            let ids = self
                .store
                .conn
                .prepare(
                    "SELECT id FROM nodes WHERE kind<>1 AND counted=1 AND excluded IS NULL
                 ORDER BY size DESC,id LIMIT ?1",
                )?
                .query_map([options.priority as i64], |row| row.get(0))?
                .collect::<rusqlite::Result<Vec<Id>>>()?;
            for id in ids {
                self.audit_file(id)?;
            }
        }
        self.flush_totals()?;
        let baseline = self.store.get(ROOT)?.unwrap().totals;
        let population = self.store.population()?;
        let count = options.sample.min(population);
        let slots = rand::seq::index::sample(&mut rand::rng(), population, count);
        let mut ids = Vec::with_capacity(count);
        let mut query = self
            .store
            .conn
            .prepare_cached("SELECT node FROM sample_slots WHERE slot=?1")?;
        for slot in slots.iter() {
            ids.push(query.query_row([slot as i64 + 1], |row| row.get::<_, Id>(0))?);
        }
        drop(query);
        let mut samples = Samples::default();
        for id in ids {
            self.check_cancel()?;
            let Some(old) = self.store.get(id)? else {
                self.settled = false;
                continue;
            };
            self.audit_file(id)?;
            if let Some(new) = self.store.get(id)? {
                samples.observe(
                    new.totals.disk_bytes as i128 - old.totals.disk_bytes as i128,
                    new.totals.apparent_bytes as i128 - old.totals.apparent_bytes as i128,
                );
            } else {
                self.settled = false;
            }
        }
        self.flush_totals()?;
        let recorded = self.store.get(ROOT)?.unwrap().totals;
        let valid = self.settled && recorded.errors == 0 && self.store.population()? == population;
        Ok(estimate::calculate(
            baseline,
            recorded,
            population,
            self.fresh.len(),
            &samples,
            options,
            valid,
        ))
    }

    fn audit_file(&mut self, id: Id) -> Result<()> {
        if self.counters.files_statted & 127 == 0 {
            self.publish_progress();
        }
        if self.fresh.contains(&id) {
            return Ok(());
        }
        self.check_cancel()?;
        let Some(node) = self.store.get(id)? else {
            return Ok(());
        };
        let parent = node.parent.context("file without a parent")?;
        let path = self
            .directory_path(parent)?
            .join(OsStr::from_bytes(&node.name));
        self.counters.metadata_calls += 1;
        self.counters.files_statted += 1;
        match fs::symlink_metadata(path) {
            Ok(metadata) => {
                let info = Info::from(&metadata);
                if info.identity != node.info.identity || info.kind != node.info.kind {
                    self.settled = false;
                    self.update_file(
                        node.clone(),
                        node.info,
                        Some("identity changed during audit; rerun".to_owned()),
                    )?;
                } else {
                    self.update_file(node, info, None)?;
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                self.settled = false;
                self.remove_tree(id)?;
            }
            Err(error) => self.update_file(node.clone(), node.info, Some(error.to_string()))?,
        }
        Ok(())
    }

    fn filesystem_exclusion(&mut self, device: u64, path: &Path) -> Result<Option<String>> {
        if let Some(reason) = self.filesystems.get(&device) {
            return Ok(reason.clone());
        }
        let path = CString::new(path.as_os_str().as_bytes())?;
        let mut stat = std::mem::MaybeUninit::<libc::statfs>::uninit();
        // statfs initializes stat on success; CString remains alive for the syscall.
        let result = unsafe { libc::statfs(path.as_ptr(), stat.as_mut_ptr()) };
        if result != 0 {
            return Ok(None);
        }
        let kind = unsafe { stat.assume_init() }.f_type as u64;
        let reason = match kind {
            0x9fa0 | 0x62656572 | 0x1cd1 | 0x27e0eb | 0x63677270 | 0x64626720 | 0xcafe4a11
            | 0x74726163 | 0x67596969 | 0x19800202 | 0x6e736673 => {
                Some("virtual filesystem".to_owned())
            }
            _ => None,
        };
        self.filesystems.insert(device, reason.clone());
        Ok(reason)
    }
}

pub fn delete_entry(cache: &Cache, id: Id, identity: Identity) -> Result<()> {
    ensure!(id != ROOT, "refusing to delete the scan root");
    let store = Store::open(cache)?;
    let root = store.get(ROOT)?.context("missing cached root")?;
    ensure!(
        Info::from(&fs::symlink_metadata(&cache.root)?).identity == root.info.identity,
        "scan root was replaced; refresh before deleting"
    );
    let node = store
        .get(id)?
        .context("entry no longer exists in the cache")?;
    ensure!(
        node.info.identity == identity,
        "selection changed; refresh before deleting"
    );
    ensure!(
        node.excluded.is_none(),
        "refusing to delete an excluded entry"
    );
    let path = indexed_path(&store.conn, cache, id)?;
    let parent = fs::canonicalize(path.parent().context("entry has no parent")?)?;
    ensure!(
        parent.starts_with(&cache.root),
        "entry escaped the scan root"
    );
    let actual = fs::symlink_metadata(&path)?;
    ensure!(
        Info::from(&actual) == node.info,
        "entry changed; perform a full refresh before deleting"
    );
    if actual.is_dir() {
        fs::remove_dir_all(&path)?;
    } else {
        fs::remove_file(&path)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ListOptions, read_snapshot};
    use rusqlite::hooks::{AuthAction, AuthContext, Authorization};

    #[test]
    fn partial_sizes_include_deep_files_and_surviving_hardlinks() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("root");
        fs::create_dir_all(root.join("nested")).unwrap();
        fs::write(root.join("nested/file"), b"shared bytes").unwrap();
        fs::hard_link(root.join("nested/file"), root.join("alias")).unwrap();
        let root_info = Info::from(&fs::symlink_metadata(&root).unwrap());
        let nested_info = Info::from(&fs::symlink_metadata(root.join("nested")).unwrap());
        let file_info = Info::from(&fs::symlink_metadata(root.join("nested/file")).unwrap());
        let cache = Cache::new(&root, Some(&temp.path().join("cache")), false).unwrap();
        let progress = Arc::new(Progress::default());
        let mut scanner = Scanner::with_progress(cache, progress.clone()).unwrap();
        scanner.full = true;
        scanner.store.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        scanner.store.begin_bulk().unwrap();
        scanner
            .insert(None, Vec::new(), root_info, None, None)
            .unwrap();
        let nested = scanner
            .insert(Some(ROOT), b"nested".to_vec(), nested_info, None, None)
            .unwrap();
        scanner
            .insert(Some(nested), b"file".to_vec(), file_info, None, None)
            .unwrap();
        scanner
            .insert(Some(ROOT), b"alias".to_vec(), file_info, None, None)
            .unwrap();
        scanner.publish_progress();
        assert!(progress.size_ready.load(Ordering::Acquire));
        assert!(progress.partial.load(Ordering::Relaxed));
        assert_eq!(
            progress.disk_bytes.load(Ordering::Relaxed),
            root_info.disk() + nested_info.disk() + file_info.disk()
        );
        assert_eq!(
            progress.apparent_bytes.load(Ordering::Relaxed),
            root_info.size + nested_info.size + file_info.size
        );

        fs::remove_dir_all(root.join("nested")).unwrap();
        let current_root = Info::from(&fs::symlink_metadata(&root).unwrap());
        scanner
            .update_directory(scanner.directories[&ROOT].clone(), current_root, None)
            .unwrap();
        scanner.remove_tree(nested).unwrap();
        scanner.publish_progress();
        assert_eq!(
            progress.disk_bytes.load(Ordering::Relaxed),
            current_root.disk() + file_info.disk()
        );
        assert_eq!(
            progress.apparent_bytes.load(Ordering::Relaxed),
            current_root.size + file_info.size
        );
    }

    #[test]
    fn a_new_hardlink_finds_an_owner_still_in_the_cold_insert_buffer() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("root");
        fs::create_dir(&root).unwrap();
        fs::write(root.join("first"), b"shared bytes").unwrap();
        let root_info = Info::from(&fs::symlink_metadata(&root).unwrap());
        let first_info = Info::from(&fs::symlink_metadata(root.join("first")).unwrap());
        let cache = Cache::new(&root, Some(&temp.path().join("cache")), false).unwrap();
        let mut scanner = Scanner::open(cache).unwrap();
        scanner.full = true;
        scanner.store.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        scanner.store.begin_bulk().unwrap();
        scanner
            .insert(None, Vec::new(), root_info, None, None)
            .unwrap();
        scanner
            .insert(Some(ROOT), b"first".to_vec(), first_info, None, None)
            .unwrap();
        assert_eq!(scanner.pending_files.len(), 1);
        fs::hard_link(root.join("first"), root.join("second")).unwrap();
        let linked_info = Info::from(&fs::symlink_metadata(root.join("second")).unwrap());
        scanner
            .insert(Some(ROOT), b"second".to_vec(), linked_info, None, None)
            .unwrap();
        scanner.flush_file_rows().unwrap();
        scanner.propagate_full_totals().unwrap();
        scanner.flush_totals().unwrap();
        scanner.store.finish_bulk().unwrap();
        scanner.store.conn.execute_batch("COMMIT").unwrap();
        let children = scanner.store.children(ROOT).unwrap();
        assert_eq!(children.iter().filter(|node| node.counted).count(), 1);
        assert_eq!(
            children
                .iter()
                .map(|node| node.totals.apparent_bytes)
                .sum::<u64>(),
            linked_info.size
        );
        assert_eq!(scanner.store.population().unwrap(), 1);
    }

    #[test]
    fn cancelling_index_build_restores_the_previous_data_and_indexes() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("root");
        fs::create_dir(&root).unwrap();
        for id in 0..3000 {
            fs::write(root.join(format!("file-{id}")), b"x").unwrap();
        }
        let cache = Cache::new(&root, Some(&temp.path().join("cache")), false).unwrap();
        let before = Scanner::open(cache.clone())
            .unwrap()
            .refresh(&Options::default())
            .unwrap();
        let old_root_info = Info::from(&fs::symlink_metadata(&root).unwrap());
        fs::write(
            root.join("new-file"),
            b"must not be committed by a cancelled scan",
        )
        .unwrap();
        let new_root_info = Info::from(&fs::symlink_metadata(&root).unwrap());
        let new_file_info = Info::from(&fs::symlink_metadata(root.join("new-file")).unwrap());
        let progress = Arc::new(Progress::default());
        let mut scanner = Scanner::with_progress(cache.clone(), progress.clone()).unwrap();
        let cancellation = progress.clone();
        scanner
            .store
            .conn
            .authorizer(Some(move |context: AuthContext<'_>| {
                if matches!(context.action, AuthAction::CreateIndex { .. }) {
                    cancellation.cancel.store(true, Ordering::Relaxed);
                }
                Authorization::Allow
            }))
            .unwrap();
        let error = scanner
            .refresh(&Options {
                exact: true,
                ..Options::default()
            })
            .unwrap_err();
        assert!(error.to_string().contains("interrupted"), "{error:#}");
        assert!(progress.finalizing.load(Ordering::Relaxed));
        assert!(progress.size_ready.load(Ordering::Acquire));
        assert_eq!(progress.files.load(Ordering::Relaxed), 3001);
        assert_eq!(
            progress.disk_bytes.load(Ordering::Relaxed),
            before.totals.disk_bytes + new_file_info.disk() + new_root_info.disk()
                - old_root_info.disk()
        );
        assert_eq!(
            progress.apparent_bytes.load(Ordering::Relaxed),
            before.totals.apparent_bytes + new_file_info.size + new_root_info.size
                - old_root_info.size
        );
        let after = read_snapshot(&cache, ROOT, &ListOptions::default()).unwrap();
        assert_eq!(after.totals, before.totals);
        assert_eq!(
            after.report.unwrap().index_generation,
            before.index_generation
        );
        let restored: i64 = scanner
            .store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='index'
             AND name IN ('disk_order','apparent_order','priority')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(restored, 3);
    }
}
