use crate::{
    cache::Cache,
    fast::{Event, Tree},
};
use anyhow::{Context, Result, ensure};
use bincode::Options;
use memmap2::MmapMut;
use std::{
    fs::{self, File, OpenOptions},
    io::Read,
    os::fd::AsRawFd,
    os::unix::fs::OpenOptionsExt,
    path::PathBuf,
    sync::atomic::{Ordering, fence},
};

const SEGMENT_BYTES: usize = 4 * 1024 * 1024;
const HEADER: usize = 16;
const FRAME_HEADER: usize = 28;
const MAGIC: &[u8; 8] = b"CDUSCAN2";

#[cfg(test)]
#[path = "journal_tests.rs"]
mod tests;

fn codec() -> impl Options {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(SEGMENT_BYTES as u64)
}

fn path(cache: &Cache, segment: usize) -> PathBuf {
    cache.directory.join(format!("scan-{segment:08}.bin"))
}

pub(crate) struct Loaded {
    pub tree: Tree,
    pub segment: usize,
    pub offset: usize,
    pub sequence: u64,
}

pub(crate) fn exists(cache: &Cache) -> bool {
    path(cache, 0).is_file()
}

pub(crate) fn load(cache: &Cache) -> Result<Loaded> {
    let mut tree = Tree::empty(cache.root.clone());
    let mut sequence = 0;
    let mut segment = 0;
    loop {
        let filename = path(cache, segment);
        if !filename.exists() {
            return Ok(Loaded {
                tree,
                segment,
                offset: HEADER,
                sequence,
            });
        }
        let mut file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&filename)?;
        ensure!(
            file.metadata()?.len() <= SEGMENT_BYTES as u64,
            "invalid checkpoint segment size"
        );
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        if bytes.len() < HEADER || bytes.iter().all(|&byte| byte == 0) {
            ensure!(
                !path(cache, segment + 1).exists(),
                "incomplete checkpoint segment in the middle of a log"
            );
            return Ok(Loaded {
                tree,
                segment,
                offset: HEADER,
                sequence,
            });
        }
        ensure!(&bytes[..8] == MAGIC, "invalid checkpoint header");
        let mut offset = HEADER;
        let mut damaged = false;
        while offset + FRAME_HEADER <= bytes.len() {
            let size = u32::from_le_bytes(bytes[offset..offset + 4].try_into()?) as usize;
            if size == 0 {
                break;
            }
            let end = offset
                .checked_add(FRAME_HEADER + size)
                .context("checkpoint size overflow")?;
            if end > bytes.len() {
                damaged = true;
                break;
            }
            let next = u64::from_le_bytes(bytes[offset + 20..offset + 28].try_into()?);
            let payload = &bytes[offset + FRAME_HEADER..end];
            if next != sequence + 1
                || bytes[offset + 4..offset + 20] != blake3::hash(payload).as_bytes()[..16]
            {
                damaged = true;
                break;
            }
            let event: Event = codec()
                .deserialize(payload)
                .context("decoding scan checkpoint")?;
            tree.apply(&event, next)?;
            sequence = next;
            offset = (end + 7) & !7;
        }
        if !damaged && (offset + FRAME_HEADER > SEGMENT_BYTES || path(cache, segment + 1).exists())
        {
            // A segment can end early when the next complete frame would not fit.
            if path(cache, segment + 1).exists() {
                segment += 1;
                continue;
            }
        }
        return Ok(Loaded {
            tree,
            segment,
            offset,
            sequence,
        });
    }
}

pub(crate) struct Journal {
    cache: Cache,
    file: File,
    map: MmapMut,
    segment: usize,
    offset: usize,
    payload: Vec<u8>,
    pub sequence: u64,
}

impl Journal {
    pub fn open(cache: &Cache, loaded: &Loaded) -> Result<Self> {
        let (file, mut map) = Self::map_segment(cache, loaded.segment)?;
        if loaded.offset == HEADER {
            map[..HEADER].fill(0);
            map[..8].copy_from_slice(MAGIC);
        }
        if loaded.offset + 4 <= SEGMENT_BYTES {
            map[loaded.offset..loaded.offset + 4].fill(0);
        }
        let mut obsolete = loaded.segment + 1;
        while path(cache, obsolete).exists() {
            fs::remove_file(path(cache, obsolete))?;
            obsolete += 1;
        }
        Ok(Self {
            cache: cache.clone(),
            file,
            map,
            segment: loaded.segment,
            offset: loaded.offset,
            payload: Vec::with_capacity(4096),
            sequence: loaded.sequence,
        })
    }

    fn map_segment(cache: &Cache, segment: usize) -> Result<(File, MmapMut)> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(path(cache, segment))?;
        let fresh = file.metadata()?.len() == 0;
        file.set_len(SEGMENT_BYTES as u64)?;
        // Reserve space before mapped writes, so a full cache filesystem yields an error, not SIGBUS.
        let allocation =
            unsafe { libc::posix_fallocate(file.as_raw_fd(), 0, SEGMENT_BYTES as libc::off_t) };
        if allocation != 0 {
            return Err(std::io::Error::from_raw_os_error(allocation))
                .context("reserving checkpoint space");
        }
        // The root's exclusive lock is held throughout the mapping lifetime.
        // Existing frames are immutable; readers copy bytes through read(), not shared Rust references.
        let mut map = unsafe { MmapMut::map_mut(&file)? };
        if fresh {
            map[..HEADER].fill(0);
            map[..8].copy_from_slice(MAGIC);
        }
        Ok((file, map))
    }

    pub fn append(&mut self, event: &Event) -> Result<u64> {
        self.payload.clear();
        // Encoding bounded internal events without a size limit avoids bincode's sizing pass.
        // The frame is checked before publication; decoding still enforces its input limit.
        bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .serialize_into(&mut self.payload, event)?;
        ensure!(
            self.payload.len() + FRAME_HEADER + HEADER <= SEGMENT_BYTES,
            "checkpoint frame is too large"
        );
        let length = (FRAME_HEADER + self.payload.len() + 7) & !7;
        if self.offset + length > SEGMENT_BYTES {
            self.segment += 1;
            let (file, map) = Self::map_segment(&self.cache, self.segment)?;
            self.file = file;
            self.map = map;
            self.offset = HEADER;
        }
        let at = self.offset;
        self.map[at..at + 4].fill(0);
        self.map[at + FRAME_HEADER..at + FRAME_HEADER + self.payload.len()]
            .copy_from_slice(&self.payload);
        self.map[at + FRAME_HEADER + self.payload.len()..at + length].fill(0);
        self.map[at + 4..at + 20].copy_from_slice(&blake3::hash(&self.payload).as_bytes()[..16]);
        self.sequence += 1;
        self.map[at + 20..at + 28].copy_from_slice(&self.sequence.to_le_bytes());
        fence(Ordering::Release);
        self.map[at..at + 4].copy_from_slice(&(self.payload.len() as u32).to_le_bytes());
        self.offset += length;
        if self.offset + 4 <= SEGMENT_BYTES {
            self.map[self.offset..self.offset + 4].fill(0);
        }
        Ok(self.sequence)
    }
}
