use anyhow::Result;
use cdu::{
    cache::Cache,
    fast,
    scan::{Options, Progress},
};
use std::{path::PathBuf, sync::Arc, time::Instant};

fn main() -> Result<()> {
    let root = std::env::args_os()
        .skip(1)
        .find(|argument| argument != "--bench")
        .map(PathBuf::from)
        .expect("pass a tree to scan");
    let temp = tempfile::tempdir()?;
    let cache = Cache::new(&root, Some(&temp.path().join("cache")), false)?;
    let started = Instant::now();
    let result = fast::scan(
        cache,
        &Options::default(),
        2,
        Arc::new(Progress::default()),
        None,
    )?;
    println!(
        "{}",
        serde_json::json!({
            "foreground_ms": started.elapsed().as_secs_f64() * 1000.0,
            "report": result.report,
        })
    );
    Ok(())
}
