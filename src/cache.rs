use anyhow::{Context, Result, ensure};
use std::{
    env,
    fs::{self, DirBuilder, File, OpenOptions},
    os::unix::{
        ffi::OsStrExt,
        fs::{DirBuilderExt, MetadataExt, OpenOptionsExt},
    },
    path::{Path, PathBuf},
};

#[derive(Clone, Debug)]
pub struct Cache {
    pub root: PathBuf,
    pub base: PathBuf,
    pub directory: PathBuf,
    pub database: PathBuf,
    pub one_file_system: bool,
}

impl Cache {
    pub fn new(root: &Path, base: Option<&Path>, one_file_system: bool) -> Result<Self> {
        let root = fs::canonicalize(root).or_else(|_| std::path::absolute(root))?;
        let base = match base {
            Some(base) => std::path::absolute(base)?,
            None => {
                if let Some(base) = env::var_os("CDU_CACHE_DIR") {
                    std::path::absolute(base)?
                } else if let Some(base) =
                    env::var_os("XDG_CACHE_HOME").filter(|p| Path::new(p).is_absolute())
                {
                    PathBuf::from(base).join("cdu")
                } else {
                    PathBuf::from(env::var_os("HOME").context("HOME is unset; pass --cache-dir")?)
                        .join(".cache/cdu")
                }
            }
        };
        let base = fs::canonicalize(&base).unwrap_or(base);
        let mut key = blake3::Hasher::new();
        key.update(b"cdu-cache-v1\0");
        key.update(root.as_os_str().as_bytes());
        key.update(&[u8::from(one_file_system)]);
        let directory = base.join(&key.finalize().to_hex()[..24]);
        Ok(Self {
            root,
            base,
            database: directory.join("index.sqlite3"),
            directory,
            one_file_system,
        })
    }

    pub(crate) fn lock(&self) -> Result<File> {
        private_directory(&self.base)?;
        private_directory(&self.directory)?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(self.directory.join("index.lock"))?;
        lock.try_lock().context(
            "another cdu process is refreshing this cache; use --cached to browse its last committed index",
        )?;
        Ok(lock)
    }
}

fn private_directory(path: &Path) -> Result<()> {
    DirBuilder::new().recursive(true).mode(0o700).create(path)?;
    let metadata = fs::symlink_metadata(path)?;
    // getuid has no pointer arguments or other safety preconditions.
    let uid = unsafe { libc::geteuid() };
    ensure!(
        metadata.is_dir() && !metadata.file_type().is_symlink() && metadata.uid() == uid,
        "cache directory must be a real directory owned by the current user: {}",
        path.display()
    );
    ensure!(
        metadata.mode() & 0o077 == 0,
        "cache directory must be private (chmod 700): {}",
        path.display()
    );
    Ok(())
}
