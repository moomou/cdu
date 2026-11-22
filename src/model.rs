use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use std::{
    ffi::OsStr,
    fs::Metadata,
    os::unix::{ffi::OsStrExt, fs::MetadataExt},
};

pub type Id = i64;
pub const ROOT: Id = 1;

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct Identity {
    pub device: u64,
    pub inode: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
#[repr(i64)]
pub enum Kind {
    File = 0,
    Directory = 1,
    Symlink = 2,
    Other = 3,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct Info {
    pub identity: Identity,
    pub kind: Kind,
    pub size: u64,
    pub blocks: u64,
    pub links: u64,
    pub mtime: i64,
    pub mtime_ns: i64,
    pub ctime: i64,
    pub ctime_ns: i64,
}

impl From<&Metadata> for Info {
    fn from(meta: &Metadata) -> Self {
        Self {
            identity: Identity {
                device: meta.dev(),
                inode: meta.ino(),
            },
            kind: if meta.is_dir() {
                Kind::Directory
            } else if meta.is_file() {
                Kind::File
            } else if meta.is_symlink() {
                Kind::Symlink
            } else {
                Kind::Other
            },
            size: meta.len(),
            blocks: meta.blocks(),
            links: meta.nlink(),
            mtime: meta.mtime(),
            mtime_ns: meta.mtime_nsec(),
            ctime: meta.ctime(),
            ctime_ns: meta.ctime_nsec(),
        }
    }
}

// Linux stat field widths vary across supported architectures.
#[allow(clippy::unnecessary_cast)]
impl From<&rustix::fs::Stat> for Info {
    fn from(meta: &rustix::fs::Stat) -> Self {
        Self {
            identity: Identity {
                device: meta.st_dev as u64,
                inode: meta.st_ino as u64,
            },
            kind: match meta.st_mode as u32 & libc::S_IFMT {
                libc::S_IFDIR => Kind::Directory,
                libc::S_IFREG => Kind::File,
                libc::S_IFLNK => Kind::Symlink,
                _ => Kind::Other,
            },
            size: meta.st_size as u64,
            blocks: meta.st_blocks as u64,
            links: meta.st_nlink as u64,
            mtime: meta.st_mtime as i64,
            mtime_ns: meta.st_mtime_nsec as i64,
            ctime: meta.st_ctime as i64,
            ctime_ns: meta.st_ctime_nsec as i64,
        }
    }
}

impl Info {
    pub fn disk(self) -> u64 {
        self.blocks.saturating_mul(512)
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct Totals {
    pub disk_bytes: u64,
    pub apparent_bytes: u64,
    pub entries: u64,
    pub errors: u64,
    pub excluded: u64,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Delta {
    pub disk: i128,
    pub apparent: i128,
    pub entries: i128,
    pub errors: i128,
    pub excluded: i128,
}

impl Delta {
    pub fn between(old: Totals, new: Totals) -> Self {
        Self {
            disk: new.disk_bytes as i128 - old.disk_bytes as i128,
            apparent: new.apparent_bytes as i128 - old.apparent_bytes as i128,
            entries: new.entries as i128 - old.entries as i128,
            errors: new.errors as i128 - old.errors as i128,
            excluded: new.excluded as i128 - old.excluded as i128,
        }
    }

    pub fn add(&mut self, other: Self) {
        self.disk += other.disk;
        self.apparent += other.apparent;
        self.entries += other.entries;
        self.errors += other.errors;
        self.excluded += other.excluded;
    }

    pub fn apply(self, totals: Totals) -> Result<Totals> {
        fn checked(value: u64, delta: i128) -> Result<u64> {
            let result = value as i128 + delta;
            ensure!(
                (0..=i64::MAX as i128).contains(&result),
                "disk usage exceeds cache limits or an accounting invariant failed"
            );
            Ok(result as u64)
        }
        Ok(Totals {
            disk_bytes: checked(totals.disk_bytes, self.disk)?,
            apparent_bytes: checked(totals.apparent_bytes, self.apparent)?,
            entries: checked(totals.entries, self.entries)?,
            errors: checked(totals.errors, self.errors)?,
            excluded: checked(totals.excluded, self.excluded)?,
        })
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Record {
    pub id: Id,
    pub parent: Option<Id>,
    pub name: Vec<u8>,
    pub info: Info,
    pub counted: bool,
    pub excluded: Option<String>,
    pub error: Option<String>,
    pub totals: Totals,
    pub direct_children: u64,
}

impl Record {
    pub fn own(&self) -> Totals {
        let counted = self.counted && self.excluded.is_none();
        Totals {
            disk_bytes: if counted { self.info.disk() } else { 0 },
            apparent_bytes: if counted { self.info.size } else { 0 },
            entries: 1,
            errors: u64::from(self.error.is_some()),
            excluded: u64::from(self.excluded.is_some()),
        }
    }

    pub fn entry(&self) -> Entry {
        Entry {
            id: self.id,
            name: display_name(OsStr::from_bytes(&self.name)),
            name_bytes: self.name.clone(),
            kind: self.info.kind,
            identity: self.info.identity,
            totals: self.totals,
            hard_link: !self.counted,
            excluded: self.excluded.clone(),
            error: self.error.clone(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Entry {
    pub id: Id,
    pub name: String,
    pub name_bytes: Vec<u8>,
    pub kind: Kind,
    pub identity: Identity,
    pub totals: Totals,
    pub hard_link: bool,
    pub excluded: Option<String>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Counters {
    pub directories_checked: u64,
    pub directories_enumerated: u64,
    pub files_statted: u64,
    pub metadata_calls: u64,
    pub rows_written: u64,
    pub elapsed_us: u64,
    #[serde(default)]
    pub walk_us: u64,
    #[serde(default)]
    pub index_us: u64,
    #[serde(default)]
    pub reused_directories: u64,
    #[serde(default)]
    pub reused_files: u64,
    #[serde(default)]
    pub checkpoint_records: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Report {
    #[serde(default)]
    pub index_generation: u64,
    pub mode: String,
    pub reason: String,
    pub completed_at_unix_ms: u64,
    pub complete: bool,
    pub settled: bool,
    pub totals: Totals,
    pub counters: Counters,
    pub audit: Option<crate::estimate::Audit>,
    pub warning: Option<String>,
}

pub fn display_name(name: &OsStr) -> String {
    let mut output = String::new();
    for character in name.to_string_lossy().chars() {
        if character.is_control()
            || character == '\\'
            || matches!(character, '\u{200e}' | '\u{200f}' | '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}')
        {
            output.extend(character.escape_default());
        } else {
            output.push(character);
        }
    }
    output
}
