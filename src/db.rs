use crate::{
    cache::{Cache, CacheLock},
    model::{Entry, Id, Identity, Info, Kind, ROOT, Record, Report, Totals, display_name},
};
use anyhow::{Context, Result, ensure};
use rusqlite::{
    Connection, OpenFlags, OptionalExtension, Row, ToSql, params, params_from_iter,
    types::{ToSqlOutput, ValueRef},
};
use serde::{Deserialize, Serialize};
use std::{
    ffi::OsStr,
    fmt::Write as _,
    os::unix::ffi::OsStrExt,
    path::PathBuf,
    sync::{Arc, LazyLock},
    time::Duration,
};

pub(crate) const FILE_BATCH_SIZE: usize = 256;

struct Parameter<'a>(ValueRef<'a>);

impl ToSql for Parameter<'_> {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Borrowed(self.0))
    }
}

static FILE_INSERT_SQL: LazyLock<[String; FILE_BATCH_SIZE.ilog2() as usize + 1]> = LazyLock::new(
    || {
        std::array::from_fn(|power| {
            let mut sql = String::from(
            "INSERT INTO nodes(id,parent,name,kind,dev,ino,size,blocks,mtime,mtime_ns,ctime,ctime_ns,
             nlink,counted,excluded,error,disk_total,apparent_total,entries_total,errors_total,
             excluded_total,direct_children) VALUES",
        );
            for row in 0..1 << power {
                if row != 0 {
                    sql.push(',');
                }
                sql.push('(');
                for column in 1..=12 {
                    if column != 1 {
                        sql.push(',');
                    }
                    write!(sql, "?{}", row * 12 + column).unwrap();
                }
                write!(
                    sql,
                    ",1,1,NULL,NULL,?{}*512,?{},1,0,0,0)",
                    row * 12 + 8,
                    row * 12 + 7
                )
                .unwrap();
            }
            sql
        })
    },
);

const SCHEMA: &str = "
CREATE TABLE nodes (
    id INTEGER PRIMARY KEY,
    parent INTEGER,
    name BLOB NOT NULL,
    kind INTEGER NOT NULL,
    dev INTEGER NOT NULL,
    ino INTEGER NOT NULL,
    size INTEGER NOT NULL,
    blocks INTEGER NOT NULL,
    nlink INTEGER NOT NULL,
    mtime INTEGER NOT NULL,
    mtime_ns INTEGER NOT NULL,
    ctime INTEGER NOT NULL,
    ctime_ns INTEGER NOT NULL,
    counted INTEGER NOT NULL,
    excluded TEXT,
    error TEXT,
    disk_total INTEGER NOT NULL,
    apparent_total INTEGER NOT NULL,
    entries_total INTEGER NOT NULL,
    errors_total INTEGER NOT NULL,
    excluded_total INTEGER NOT NULL,
    direct_children INTEGER NOT NULL
);
CREATE UNIQUE INDEX names ON nodes(parent,name);
CREATE INDEX directories ON nodes(id) WHERE kind=1;
CREATE INDEX identities ON nodes(dev,ino) WHERE kind<>1;
CREATE INDEX unreadable ON nodes(parent) WHERE error IS NOT NULL;
CREATE TABLE sample_slots (slot INTEGER PRIMARY KEY, node INTEGER NOT NULL UNIQUE);
CREATE TABLE meta (key TEXT PRIMARY KEY, value BLOB NOT NULL);
PRAGMA user_version=2;
";

const BROWSE_INDEXES: &str = "
CREATE INDEX disk_order ON nodes(parent,disk_total DESC,name);
CREATE INDEX apparent_order ON nodes(parent,apparent_total DESC,name);
CREATE INDEX priority ON nodes(size DESC,id)
    WHERE kind<>1 AND counted=1 AND excluded IS NULL;
";

pub(crate) struct Store {
    pub conn: Connection,
    _lock: Arc<CacheLock>,
}

impl Store {
    pub fn open(cache: &Cache) -> Result<Self> {
        let lock = Arc::new(cache.lock()?);
        Self::shared(cache, lock)
    }

    pub(crate) fn shared(cache: &Cache, lock: Arc<CacheLock>) -> Result<Self> {
        let conn = Connection::open_with_flags(
            &cache.database,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX
                | OpenFlags::SQLITE_OPEN_NOFOLLOW,
        )
        .context("opening metadata cache")?;
        conn.busy_timeout(Duration::from_secs(2))?;
        conn.set_prepared_statement_cache_capacity(32);
        conn.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;
             PRAGMA temp_store=FILE; PRAGMA cache_size=-8192;",
        )?;
        let version: i64 = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        ensure!(version <= 2, "cache was created by a newer cdu version");
        if version == 0 {
            conn.execute_batch("BEGIN IMMEDIATE")?;
            conn.execute_batch(SCHEMA)?;
            conn.execute_batch(BROWSE_INDEXES)?;
            conn.execute_batch("COMMIT")?;
        } else if version == 1 {
            conn.execute_batch(
                "BEGIN IMMEDIATE;
                 ALTER TABLE nodes ADD COLUMN direct_children INTEGER NOT NULL DEFAULT 0;
                 UPDATE nodes SET direct_children=(SELECT COUNT(*) FROM nodes AS child WHERE child.parent=nodes.id)
                   WHERE kind=1;
                 PRAGMA user_version=2;
                 COMMIT;",
            )?;
        }
        let store = Self { conn, _lock: lock };
        if let Some(root) = store.meta("root")? {
            ensure!(
                root == cache.root.as_os_str().as_bytes(),
                "cache root mismatch; refusing to reuse it"
            );
        }
        Ok(store)
    }

    pub fn meta(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(self
            .conn
            .query_row("SELECT value FROM meta WHERE key=?1", [key], |row| {
                row.get(0)
            })
            .optional()?)
    }

    pub fn set_meta(&self, key: &str, value: &[u8]) -> Result<()> {
        self.conn.execute(
            "INSERT INTO meta(key,value) VALUES(?1,?2)
             ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            params![key, value],
        )?;
        Ok(())
    }

    pub fn get(&self, id: Id) -> Result<Option<Record>> {
        Ok(self
            .conn
            .prepare_cached("SELECT * FROM nodes WHERE id=?1")?
            .query_row([id], record)
            .optional()?)
    }

    pub fn children(&self, id: Id) -> Result<Vec<Record>> {
        Ok(self
            .conn
            .prepare_cached("SELECT * FROM nodes WHERE parent=?1")?
            .query_map([id], record)?
            .collect::<rusqlite::Result<_>>()?)
    }

    pub fn members(&self, identity: Identity) -> Result<Vec<Record>> {
        Ok(self
            .conn
            .prepare_cached("SELECT * FROM nodes WHERE dev=?1 AND ino=?2 AND kind<>1")?
            .query_map(
                params![identity.device as i64, identity.inode as i64],
                record,
            )?
            .collect::<rusqlite::Result<_>>()?)
    }

    pub fn owner(&self, identity: Identity) -> Result<Option<Record>> {
        Ok(self
            .conn
            .prepare_cached(
                "SELECT * FROM nodes WHERE dev=?1 AND ino=?2 AND kind<>1
             AND counted=1 AND excluded IS NULL LIMIT 1",
            )?
            .query_row(
                params![identity.device as i64, identity.inode as i64],
                record,
            )
            .optional()?)
    }

    pub fn begin_bulk(&self) -> Result<()> {
        // DDL stays in the scan transaction: rollback restores the previous indexes.
        self.conn.execute_batch(
            "PRAGMA cache_size=-32768;
             DROP INDEX disk_order; DROP INDEX apparent_order; DROP INDEX priority;
             DELETE FROM nodes; DELETE FROM sample_slots;",
        )?;
        Ok(())
    }

    pub fn finish_bulk(&self) -> Result<()> {
        self.conn.execute_batch(
            "INSERT INTO sample_slots(node)
             SELECT id FROM nodes WHERE kind<>1 AND counted=1 AND excluded IS NULL ORDER BY id;",
        )?;
        self.conn.execute_batch(BROWSE_INDEXES)?;
        self.conn.execute_batch("PRAGMA cache_size=-8192;")?;
        Ok(())
    }

    pub fn insert(&self, node: &Record) -> Result<()> {
        if node.info.kind != Kind::Directory
            && node.info.links == 1
            && node.counted
            && node.excluded.is_none()
            && node.error.is_none()
        {
            return self.insert_file_batch(std::slice::from_ref(node));
        }
        self.conn
            .prepare_cached(
                "INSERT INTO nodes(parent,name,kind,dev,ino,size,blocks,nlink,mtime,mtime_ns,
              ctime,ctime_ns,counted,excluded,error,disk_total,apparent_total,entries_total,
              errors_total,excluded_total,id,direct_children)
             VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22)",
            )?
            .execute(params![
                node.parent,
                node.name,
                node.info.kind as i64,
                node.info.identity.device as i64,
                node.info.identity.inode as i64,
                integer(node.info.size)?,
                integer(node.info.blocks)?,
                integer(node.info.links)?,
                node.info.mtime,
                node.info.mtime_ns,
                node.info.ctime,
                node.info.ctime_ns,
                node.counted,
                node.excluded,
                node.error,
                integer(node.totals.disk_bytes)?,
                integer(node.totals.apparent_bytes)?,
                integer(node.totals.entries)?,
                integer(node.totals.errors)?,
                integer(node.totals.excluded)?,
                node.id,
                integer(node.direct_children)?,
            ])?;
        Ok(())
    }

    pub fn insert_file_batch(&self, nodes: &[Record]) -> Result<()> {
        debug_assert!(nodes.len().is_power_of_two() && nodes.len() <= FILE_BATCH_SIZE);
        for node in nodes {
            debug_assert!(
                node.info.kind != Kind::Directory
                    && node.info.links == 1
                    && node.counted
                    && node.excluded.is_none()
                    && node.error.is_none()
            );
            integer(node.info.size)?;
            integer(node.totals.disk_bytes)?;
        }
        let values = nodes.iter().flat_map(|node| {
            [
                ValueRef::Integer(node.id),
                node.parent.map(ValueRef::Integer).unwrap_or(ValueRef::Null),
                ValueRef::Blob(&node.name),
                ValueRef::Integer(node.info.kind as i64),
                ValueRef::Integer(node.info.identity.device as i64),
                ValueRef::Integer(node.info.identity.inode as i64),
                ValueRef::Integer(node.info.size as i64),
                ValueRef::Integer(node.info.blocks as i64),
                ValueRef::Integer(node.info.mtime),
                ValueRef::Integer(node.info.mtime_ns),
                ValueRef::Integer(node.info.ctime),
                ValueRef::Integer(node.info.ctime_ns),
            ]
        });
        self.conn
            .prepare_cached(&FILE_INSERT_SQL[nodes.len().ilog2() as usize])?
            .execute(params_from_iter(values.map(Parameter)))?;
        Ok(())
    }

    pub fn update(&self, node: &Record) -> Result<()> {
        self.conn
            .prepare_cached(
                "UPDATE nodes SET parent=?2,name=?3,kind=?4,dev=?5,ino=?6,size=?7,blocks=?8,
             nlink=?9,mtime=?10,mtime_ns=?11,ctime=?12,ctime_ns=?13,counted=?14,
             excluded=?15,error=?16,disk_total=?17,apparent_total=?18,entries_total=?19,
             errors_total=?20,excluded_total=?21,direct_children=?22 WHERE id=?1",
            )?
            .execute(params![
                node.id,
                node.parent,
                node.name,
                node.info.kind as i64,
                node.info.identity.device as i64,
                node.info.identity.inode as i64,
                integer(node.info.size)?,
                integer(node.info.blocks)?,
                integer(node.info.links)?,
                node.info.mtime,
                node.info.mtime_ns,
                node.info.ctime,
                node.info.ctime_ns,
                node.counted,
                node.excluded,
                node.error,
                integer(node.totals.disk_bytes)?,
                integer(node.totals.apparent_bytes)?,
                integer(node.totals.entries)?,
                integer(node.totals.errors)?,
                integer(node.totals.excluded)?,
                integer(node.direct_children)?,
            ])?;
        Ok(())
    }

    pub fn population(&self) -> Result<usize> {
        Ok(self.conn.query_row(
            "SELECT COALESCE(MAX(slot),0) FROM sample_slots",
            [],
            |row| row.get::<_, i64>(0).map(|value| value as usize),
        )?)
    }

    pub fn add_slot(&self, id: Id) -> Result<()> {
        self.conn
            .prepare_cached(
                "INSERT INTO sample_slots(slot,node)
             VALUES((SELECT COALESCE(MAX(slot),0)+1 FROM sample_slots),?1)",
            )?
            .execute([id])?;
        Ok(())
    }

    pub fn remove_slot(&self, id: Id) -> Result<()> {
        let slot: Option<i64> = self
            .conn
            .prepare_cached("SELECT slot FROM sample_slots WHERE node=?1")?
            .query_row([id], |row| row.get(0))
            .optional()?;
        if let Some(slot) = slot {
            let last = self.population()? as i64;
            self.conn
                .prepare_cached("DELETE FROM sample_slots WHERE slot=?1")?
                .execute([slot])?;
            if slot != last {
                self.conn
                    .prepare_cached("UPDATE sample_slots SET slot=?1 WHERE slot=?2")?
                    .execute(params![slot, last])?;
            }
        }
        Ok(())
    }
}

pub(crate) fn integer(value: u64) -> Result<i64> {
    i64::try_from(value).context("metadata value exceeds the cache's signed 64-bit range")
}

fn unsigned(row: &Row<'_>, column: usize) -> rusqlite::Result<u64> {
    let value: i64 = row.get(column)?;
    u64::try_from(value).map_err(|_| rusqlite::Error::IntegralValueOutOfRange(column, value))
}

pub(crate) fn record(row: &Row<'_>) -> rusqlite::Result<Record> {
    let kind = match row.get::<_, i64>(3)? {
        0 => Kind::File,
        1 => Kind::Directory,
        2 => Kind::Symlink,
        3 => Kind::Other,
        other => return Err(rusqlite::Error::IntegralValueOutOfRange(3, other)),
    };
    Ok(Record {
        id: row.get(0)?,
        parent: row.get(1)?,
        name: row.get(2)?,
        info: Info {
            kind,
            identity: Identity {
                device: row.get::<_, i64>(4)? as u64,
                inode: row.get::<_, i64>(5)? as u64,
            },
            size: unsigned(row, 6)?,
            blocks: unsigned(row, 7)?,
            links: unsigned(row, 8)?,
            mtime: row.get(9)?,
            mtime_ns: row.get(10)?,
            ctime: row.get(11)?,
            ctime_ns: row.get(12)?,
        },
        counted: row.get(13)?,
        excluded: row.get(14)?,
        error: row.get(15)?,
        totals: Totals {
            disk_bytes: unsigned(row, 16)?,
            apparent_bytes: unsigned(row, 17)?,
            entries: unsigned(row, 18)?,
            errors: unsigned(row, 19)?,
            excluded: unsigned(row, 20)?,
        },
        direct_children: unsigned(row, 21)?,
    })
}

#[derive(Clone, Copy, Debug, Default)]
pub enum Sort {
    #[default]
    Size,
    Name,
    Count,
}

#[derive(Clone, Debug)]
pub struct ListOptions {
    pub sort: Sort,
    pub apparent: bool,
    pub descending: bool,
    pub directories_first: bool,
    pub hidden: bool,
    pub offset: usize,
    pub limit: usize,
}

impl Default for ListOptions {
    fn default() -> Self {
        Self {
            sort: Sort::Size,
            apparent: false,
            descending: true,
            directories_first: false,
            hidden: true,
            offset: 0,
            limit: 200,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Snapshot {
    pub root: String,
    pub path: String,
    pub path_bytes: Vec<u8>,
    pub directory: Id,
    pub parent: Option<Id>,
    pub totals: Totals,
    pub children: usize,
    pub offset: usize,
    pub entries: Vec<Entry>,
    pub report: Option<Report>,
}

pub fn read_snapshot(cache: &Cache, directory: Id, options: &ListOptions) -> Result<Snapshot> {
    let conn = Connection::open_with_flags(
        &cache.database,
        OpenFlags::SQLITE_OPEN_READ_ONLY
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_NOFOLLOW,
    )
    .context("no readable index; run cdu without --cached first")?;
    conn.busy_timeout(Duration::from_secs(2))?;
    conn.execute_batch("BEGIN")?;
    let version: i64 = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    ensure!(
        version == 2,
        "unsupported cache format; run without --cached to upgrade an older index"
    );
    let node = conn
        .query_row("SELECT * FROM nodes WHERE id=?1", [directory], record)
        .context("directory is no longer in the index")?;
    ensure!(
        node.info.kind == Kind::Directory,
        "not an indexed directory"
    );
    let path = indexed_path(&conn, cache, directory)?;
    let filter = if options.hidden {
        ""
    } else {
        " AND substr(name,1,1)<>x'2e'"
    };
    let order = match options.sort {
        Sort::Name => "name",
        Sort::Count => "entries_total",
        Sort::Size if options.apparent => "apparent_total",
        Sort::Size => "disk_total",
    };
    let direction = if options.descending { "DESC" } else { "ASC" };
    let groups = if options.directories_first {
        "(kind=1) DESC,"
    } else {
        ""
    };
    let sql = format!(
        "SELECT * FROM nodes WHERE parent=?1{filter}
         ORDER BY {groups}{order} {direction},name ASC,id ASC LIMIT ?2 OFFSET ?3"
    );
    let entries = conn
        .prepare(&sql)?
        .query_map(
            params![
                directory,
                i64::try_from(options.limit)?,
                i64::try_from(options.offset)?
            ],
            |row| Ok(record(row)?.entry()),
        )?
        .collect::<rusqlite::Result<_>>()?;
    let children = if options.hidden {
        usize::try_from(node.direct_children)?
    } else {
        conn.query_row(
            &format!("SELECT COUNT(*) FROM nodes WHERE parent=?1{filter}"),
            [directory],
            |row| row.get::<_, i64>(0).map(|value| value as usize),
        )?
    };
    let report: Option<Vec<u8>> = conn
        .query_row("SELECT value FROM meta WHERE key='report'", [], |row| {
            row.get(0)
        })
        .optional()?;
    let report = report
        .map(|data| serde_json::from_slice(&data))
        .transpose()?;
    Ok(Snapshot {
        root: display_name(cache.root.as_os_str()),
        path: display_name(path.as_os_str()),
        path_bytes: path.as_os_str().as_bytes().to_vec(),
        directory,
        parent: node.parent,
        totals: node.totals,
        children,
        offset: options.offset,
        entries,
        report,
    })
}

pub(crate) fn indexed_path(conn: &Connection, cache: &Cache, mut id: Id) -> Result<PathBuf> {
    let mut parts = Vec::new();
    let mut statement = conn.prepare_cached("SELECT parent,name FROM nodes WHERE id=?1")?;
    while id != ROOT {
        ensure!(parts.len() < 4096, "invalid directory graph in cache");
        let (parent, name): (Option<Id>, Vec<u8>) =
            statement.query_row([id], |row| Ok((row.get(0)?, row.get(1)?)))?;
        ensure!(
            !name.is_empty()
                && name != b"."
                && name != b".."
                && !name.contains(&b'/')
                && !name.contains(&0),
            "invalid filename in cache"
        );
        parts.push(name);
        id = parent.context("detached entry in cache")?;
    }
    let mut path = cache.root.clone();
    for part in parts.into_iter().rev() {
        path.push(OsStr::from_bytes(&part));
    }
    Ok(path)
}
