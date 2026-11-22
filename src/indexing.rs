use crate::{
    cache::Cache,
    db::Store,
    fast::Tree,
    model::{Id, Kind},
};
use anyhow::{Context, Result};
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

const BATCH: usize = 256;

#[cfg(test)]
#[path = "indexing_tests.rs"]
mod tests;

#[derive(Default)]
pub struct Progress {
    pub directories: AtomicU64,
    pub total_directories: AtomicU64,
    pub rows: AtomicU64,
    pub resumed_rows: AtomicU64,
    pub done: AtomicBool,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Outcome {
    pub complete: bool,
    pub rows_written: u64,
    pub resumed_rows: u64,
    pub directories_completed: u64,
}

#[derive(Clone)]
struct State {
    signature: Vec<u8>,
    phase: i64,
    next: usize,
}

pub fn run(
    cache: &Cache,
    tree: &Tree,
    lock: Arc<File>,
    cancel: Arc<AtomicBool>,
    progress: Arc<Progress>,
) -> Result<Outcome> {
    let mut destination = cache.clone();
    destination.database = destination.directory.join("lookup.sqlite3");
    let mut store = Store::shared(&destination, lock)?;
    store.conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS directory_index (
            id INTEGER PRIMARY KEY, signature BLOB NOT NULL, phase INTEGER NOT NULL, next_entry INTEGER NOT NULL
         );",
    )?;
    let cancellation = cancel.clone();
    store
        .conn
        .progress_handler(2000, Some(move || cancellation.load(Ordering::Relaxed)))?;
    let result = build(&store, cache, tree, &cancel, &progress);
    store.conn.progress_handler(0, None::<fn() -> bool>)?;
    if !store.conn.is_autocommit() {
        store.conn.execute_batch("ROLLBACK")?;
    }
    match result {
        Err(_) if cancel.load(Ordering::Relaxed) => Ok(Outcome {
            complete: false,
            rows_written: progress.rows.load(Ordering::Relaxed),
            resumed_rows: progress.resumed_rows.load(Ordering::Relaxed),
            directories_completed: progress.directories.load(Ordering::Relaxed),
        }),
        result => result,
    }
}

fn build(
    store: &Store,
    cache: &Cache,
    tree: &Tree,
    cancel: &AtomicBool,
    progress: &Progress,
) -> Result<Outcome> {
    let mut states: HashMap<u32, State> = store
        .conn
        .prepare("SELECT id,signature,phase,next_entry FROM directory_index")?
        .query_map([], |row| {
            Ok((
                row.get::<_, u32>(0)?,
                State {
                    signature: row.get(1)?,
                    phase: row.get(2)?,
                    next: row.get::<_, i64>(3)? as usize,
                },
            ))
        })?
        .collect::<rusqlite::Result<_>>()?;
    let active: Vec<u32> = tree
        .active
        .iter()
        .enumerate()
        .filter(|(_, active)| **active)
        .map(|(id, _)| id as u32)
        .collect();
    let live: HashSet<_> = active.iter().copied().collect();
    progress
        .total_directories
        .store(active.len() as u64, Ordering::Relaxed);
    let mut changed = Vec::new();
    for &id in &active {
        if cancel.load(Ordering::Relaxed) {
            return outcome(progress, false);
        }
        let signature = tree.fingerprint(id)?;
        match states.get(&id) {
            Some(state) if state.signature == signature => {
                progress
                    .resumed_rows
                    .fetch_add(state.next as u64, Ordering::Relaxed);
                if state.phase == 2 {
                    progress.directories.fetch_add(1, Ordering::Relaxed);
                } else {
                    changed.push(id);
                }
            }
            _ => {
                let phase = if states.contains_key(&id) { 0 } else { 1 };
                states.insert(
                    id,
                    State {
                        signature,
                        phase,
                        next: 0,
                    },
                );
                changed.push(id);
            }
        }
    }
    let obsolete: Vec<_> = states
        .keys()
        .copied()
        .filter(|id| !live.contains(id))
        .collect();
    for id in obsolete {
        if cancel.load(Ordering::Relaxed) {
            return outcome(progress, false);
        }
        clear_children(store, id, cancel)?;
        if cancel.load(Ordering::Relaxed) {
            return outcome(progress, false);
        }
        transaction(store, || {
            store.conn.execute("DELETE FROM nodes WHERE id=?1", [id])?;
            store
                .conn
                .execute("DELETE FROM directory_index WHERE id=?1", [id])?;
            Ok(())
        })?;
        states.remove(&id);
    }
    // Clear changed parents before filling any of them, so moved names cannot collide.
    for &id in &changed {
        if cancel.load(Ordering::Relaxed) {
            return outcome(progress, false);
        }
        let state = states.get_mut(&id).unwrap();
        if state.phase != 0 {
            continue;
        }
        transaction(store, || {
            store.conn.execute(
                "INSERT INTO directory_index(id,signature,phase,next_entry) VALUES(?1,?2,0,0)
                 ON CONFLICT(id) DO UPDATE SET signature=excluded.signature,phase=0,next_entry=0",
                params![id, state.signature],
            )?;
            Ok(())
        })?;
        clear_children(store, id, cancel)?;
        if cancel.load(Ordering::Relaxed) {
            return outcome(progress, false);
        }
        transaction(store, || {
            store
                .conn
                .execute("UPDATE directory_index SET phase=1 WHERE id=?1", [id])?;
            Ok(())
        })?;
        state.phase = 1;
    }
    for &id in &changed {
        let state = states
            .get_mut(&id)
            .context("missing directory index state")?;
        while state.next < tree.dir(id)?.count() {
            if cancel.load(Ordering::Relaxed) {
                return outcome(progress, false);
            }
            let end = (state.next + BATCH).min(tree.dir(id)?.count());
            transaction(store, || {
                upsert(store, &tree.directory_record(id)?)?;
                for at in state.next..end {
                    let node = tree.child_record(id, at)?;
                    upsert(store, &node)?;
                    if node.info.kind != Kind::Directory && node.counted && node.excluded.is_none()
                    {
                        let present = store
                            .conn
                            .query_row(
                                "SELECT 1 FROM sample_slots WHERE node=?1",
                                [node.id],
                                |_| Ok(()),
                            )
                            .optional()?
                            .is_some();
                        if !present {
                            store.add_slot(node.id)?;
                        }
                    }
                }
                store.conn.execute(
                    "INSERT INTO directory_index(id,signature,phase,next_entry) VALUES(?1,?2,1,?3)
                     ON CONFLICT(id) DO UPDATE SET signature=excluded.signature,phase=1,next_entry=excluded.next_entry",
                    params![id, state.signature, end as i64],
                )?;
                Ok(())
            })?;
            progress
                .rows
                .fetch_add((end - state.next) as u64, Ordering::Relaxed);
            state.next = end;
        }
        if cancel.load(Ordering::Relaxed) {
            return outcome(progress, false);
        }
        transaction(store, || {
            upsert(store, &tree.directory_record(id)?)?;
            store.conn.execute(
                "INSERT INTO directory_index(id,signature,phase,next_entry) VALUES(?1,?2,2,?3)
                 ON CONFLICT(id) DO UPDATE SET signature=excluded.signature,phase=2,next_entry=excluded.next_entry",
                params![id, state.signature, state.next as i64],
            )?;
            Ok(())
        })?;
        progress.directories.fetch_add(1, Ordering::Relaxed);
    }
    if cancel.load(Ordering::Relaxed) {
        return outcome(progress, false);
    }
    transaction(store, || {
        use std::os::unix::ffi::OsStrExt;
        store.set_meta("root", cache.root.as_os_str().as_bytes())?;
        if let Some(report) = &tree.report {
            store.set_meta("report", &serde_json::to_vec(report)?)?;
        }
        Ok(())
    })?;
    progress.done.store(true, Ordering::Release);
    outcome(progress, true)
}

fn outcome(progress: &Progress, complete: bool) -> Result<Outcome> {
    Ok(Outcome {
        complete,
        rows_written: progress.rows.load(Ordering::Relaxed),
        resumed_rows: progress.resumed_rows.load(Ordering::Relaxed),
        directories_completed: progress.directories.load(Ordering::Relaxed),
    })
}

fn transaction(store: &Store, operation: impl FnOnce() -> Result<()>) -> Result<()> {
    store.conn.execute_batch("BEGIN IMMEDIATE")?;
    match operation() {
        Ok(()) => {
            store.conn.execute_batch("COMMIT")?;
            Ok(())
        }
        Err(error) => {
            let _ = store.conn.execute_batch("ROLLBACK");
            Err(error)
        }
    }
}

fn upsert(store: &Store, node: &crate::model::Record) -> Result<()> {
    if store.get(node.id)?.is_some() {
        store.update(node)?;
    } else {
        store.insert(node)?;
    }
    Ok(())
}

fn clear_children(store: &Store, directory: u32, cancel: &AtomicBool) -> Result<()> {
    loop {
        if cancel.load(Ordering::Relaxed) {
            return Ok(());
        }

        let ids: Vec<Id> = store
            .conn
            .prepare("SELECT id FROM nodes WHERE parent=?1 LIMIT ?2")?
            .query_map(params![directory, BATCH as i64], |row| row.get(0))?
            .collect::<rusqlite::Result<_>>()?;
        if ids.is_empty() {
            return Ok(());
        }
        transaction(store, || {
            for id in &ids {
                store.remove_slot(*id)?;
                store.conn.execute("DELETE FROM nodes WHERE id=?1", [id])?;
            }
            Ok(())
        })?;
    }
}
