# cdu

**Cached disk usage**, an ncdu-style Rust terminal browser for Linux.
No daemon, filesystem watcher, scheduled task, telemetry, or scanned-file
content reads.

Use it to find large directories, quit midway through a scan without losing
checkpointed work, and return later without starting over. The browser is
read-only by default; deletion requires an explicit opt-in.

## Full sizes first; indexing afterward

Version 0.2 separates filesystem discovery from persistent lookup indexing:

1. Two bounded metadata workers build the browser's in-memory tree.
2. Scan results are checkpointed in small append-only records as they arrive.
3. The complete directory sizes and browser are rendered immediately after
   discovery and aggregation.
4. Only **after that frame is rendered** may the SQLite lookup indexer start.
   Navigation, sorting, help, and size display use the in-memory tree, not it.

Quitting stops work instead of waiting for a complete index. Reopening the same
root resumes saved directory/file checkpoints and committed index batches.
Neither worker survives the application.

## Install

Requires Linux, Rust 1.92+, and a C compiler for bundled SQLite. No separately
installed SQLite library or administrator privileges are needed.

Clone and install:

```sh
git clone https://github.com/moomou/cdu.git
cd cdu
cargo install --path . --locked --jobs 2
cdu /path/to/tree
```

Or, from the repository, build without installing:

```sh
cargo build --release --locked
./target/release/cdu /path/to/tree
```

### Quick start

```sh
cdu .                       # Discover sizes and open the terminal browser
# Press q to pause; run the same command again to resume.
cdu --quick .               # Validate directory changes without file sampling
cdu --cached .              # Browse saved metadata without scanning
cdu --json --summary .      # Machine-readable totals, estimates, and counters
```

An initial scan reads all included metadata. Later scans can reuse checkpoints,
but unchanged directory timestamps do not prove that file sizes are unchanged.
See [incremental accuracy](#incremental-accuracy-and-statistical-bounds) before
using cached totals as exact measurements.

## Measured time to interactivity

### Focused optimization in 0.2.1

Three paired, fresh-cache runs on the same approximately 1.06-million-entry
monorepo measured complete size display **and a keyboard response**:

| Binary | Median interactivity |
| --- | ---: |
| cdu 0.2.0, measured alongside the candidate | 1,081 ms |
| cdu 0.2.1 | **939 ms** |
| ncdu 1.15.1 | 1,640 ms |

The improvement over cdu's prior version is approximately **142 ms / 13%**.
Old/new cdu byte totals and item counts matched exactly in each pair.
Indexing still starts only after the complete frame is rendered.

The decisive change is owning and reading each directory descriptor directly,
instead of opening it a second time through `/proc/self/fd`. Metadata queries
remain descriptor-relative and do not follow symlinks. The round also reuses a
single-pass checkpoint encoder buffer and constructs paths only when an
exclusion/filesystem check needs one. Those latter changes alone were within
timing noise; no standalone latency improvement is claimed for them.

The measured old/new pairs were 1,116/908, 1,060/950, and 1,081/939 ms.
ncdu measured 8,023, 1,620, and 1,640 ms; its first run was an outlier.
Workers, accuracy checks, memory limits, and the interactivity definition were
unchanged.

To compare a candidate with an older saved binary:

```sh
python3 benches/interactive.py /path/to/tree \
  --baseline /path/to/older/cdu --runs 3 --require-faster
```

With `--baseline`, the gate requires the candidate to beat both the previous
cdu and ncdu medians. The checkpoint format remains compatible with 0.2.0.

### Initial 0.2.0 redesign

On a local monorepo with **1,061,939 entries / 108,685 directories**:

| Cold application launch | Median complete-browser interactivity |
| --- | ---: |
| ncdu 1.15.1 | 1.792 s |
| cdu 0.2.0 | 1.273 s |

This is about **29% less time**, or **1.41x faster**, on that measured workload.
It is not a comparison between a cached partial screen and a complete ncdu scan.

The benchmark starts both real terminal UIs, waits for the complete root-size
screen, sends `?`, and waits for the help screen to respond. It checks both
displayed size metrics and the complete item count. cdu's instrumentation also
asserts that indexing starts after the full-size frame.

The final three measured cdu runs were 1.285, 1.261, and 1.273 seconds. ncdu's
runs were 1.800, 1.792, and 1.772 seconds. Runs alternate order on a shared host.
Every cdu run starts with a fresh application cache; OS caches are not flushed.
The harness applies a 768-MiB address-space limit to each process.

Reproduce the actual interactivity comparison:

```sh
cargo build --release --locked
python3 benches/interactive.py /path/to/tree --runs 3 --require-faster
```

`--require-faster` fails unless cdu's median **complete, keyboard-responsive**
browser time beats ncdu. It does not time export, a progress screen, a cached
answer, or indexing completion.

For an individual run:

```sh
cdu PATH --timings-json /tmp/cdu-timings.json
```

The file is written when the application exits. It records:

- `scan_complete_us`
- `full_size_interactive_us`
- `first_input_response_us`
- `indexing_started_us` / `indexing_finished_us`
- complete root totals, scan/reuse counters, and index resume counters

Times are measured from CLI startup. Headless runs have no interactive-frame
timestamp. Measurements depend on the filesystem, workload, and host load;
they are not universal latency guarantees.

## Pause and resume

### Scanning

Checksummed `scan-*.bin` segments store directory starts, chunks of at most
256 entries, directory completion, and metadata updates. Files are preallocated
and mapped so checkpoints do not require a foreground SQLite transaction.

On a normal `q` or Ctrl-C exit from the browser, workers stop, their completed
metadata batches are recorded, and the application exits. On restart:

- Completed directories with unchanged metadata are reused.
- An unfinished directory can reuse its captured names and metadata and scan
  only the remaining entries.
- Changed directories are reconciled rather than blindly trusted.
- A torn trailing record is ignored; the valid checkpoint prefix remains.

While browsing, `r` and `R` refresh only the current directory subtree. A
targeted refresh preempts an active root refresh, publishes the selected
subtree first, then resumes the checkpointed root work. Stable directory IDs,
parent links, and per-directory lookup signatures let the derived index update
the changed subtree and affected ancestors without rebuilding every row.

These are resumable metadata observations, not atomic filesystem snapshots.
Changes made while the application is stopped still require validation.
Directory timestamps cannot reveal all in-place file growth; the same
incremental-accuracy limitations described below apply.

Normal process exit preserves the mapped records in the OS file cache. An
abrupt crash may require repeating an in-flight batch. Power loss may lose
recent unsynced records; checksums detect incomplete records. No promise of
power-loss durability is made without a completed storage flush.

### Lookup indexing

`lookup.sqlite3` is an optional derived index, never the authority needed to
display the complete tree. Its directory signatures, cleanup phase, and entry
cursor are committed with bounded batches of at most 256 entries. Quitting
interrupts the current operation and resumes after the last committed batch
next time; it does not roll back the entire index.

An actual pause/resume check preserved 270 scanned files and inspected only the
remaining 330 on restart. A separate check paused indexing after 512 committed
rows, then reused those rows and wrote only the remaining 2,488. The browser
remained responsive while indexing was deliberately slowed.

Headless output returns as soon as the tree is ready; it does not stay alive
to build a lookup index. A later interactive visit can finish that work.
`--cached` is read-only and does not start new work.

## Modes and controls

| Command | Behavior |
| --- | --- |
| `cdu PATH` | Scan or resume, then browse; later visits validate cached directories and audit files |
| `cdu --quick PATH` | Directory validation without prioritized/random file audits |
| `cdu --cached PATH` | Browse recorded checkpoints without scanning |
| `cdu --exact PATH` | Start a full metadata scan after a completed scan, or resume an unfinished scan |
| `cdu --json --summary PATH` | Return root totals and counters without waiting for lookup indexing |
| `cdu -a PATH` | Apparent size instead of allocated bytes |
| `cdu -x PATH` | Stay on the root filesystem |
| `cdu --threads 2 PATH` | Set bounded metadata parallelism; supported range 1-8 |

Without a terminal, cdu prints a table. `--no-ui` selects that mode explicitly.
JSON is a directory snapshot, not an ncdu-compatible whole-tree export.
`--limit` defaults to 200 entries; `--summary` omits entries.

| Key | Action |
| --- | --- |
| Arrows / `hjkl`, Enter, Backspace | Navigate |
| Page Up / Down, Home / End | Move through listings |
| `s`, `n`, `C` | Sort by size, name, or item count; repeat to reverse |
| `a` | Toggle allocated / apparent size |
| `e`, `t` | Toggle hidden entries / directories first |
| `g`, `c` | Toggle size-share bars / counts |
| `i`, `?` | Item details / help |
| `r`, `R` | Incremental / exact refresh of the current directory subtree |
| `q`, Ctrl-C | Pause work and quit |
| `d` | Delete only with `--enable-delete` and explicit confirmation |

Live scan counts and size estimates remain visible while discovering the tree.
Initial sizes are labeled **partial**, not presented as the complete answer.
After discovery, the full-size browser stays available while indexing proceeds.
Bars use the current directory's recorded total, not the current screenful.

The browser is read-only by default. Deletion checks identity and metadata but
is not an atomic transaction against concurrent filesystem writers. The scan
root and excluded entries cannot be deleted. There is no shell-spawning feature.

## Incremental accuracy and statistical bounds

A fresh, uninterrupted scan obtains every included entry's metadata. Like
ncdu/du, it is still not an atomic snapshot of a changing filesystem.

Cached and resumed results are not automatically fresh. cdu validates directory
identity and mtime/ctime, but existing files can grow without changing their
parent's timestamps. Timestamp granularity/restoration and mount changes can
also defeat membership hints.

By default, later visits prioritize 32 large files and sample up to 512 counted
file identities uniformly without replacement. Hard-link aliases do not gain
extra sampling weight. The remaining discrepancy is estimated using:

```text
estimated total = baseline total + N * mean(sampled byte deltas)
```

Extrapolated estimates never replace observed file metadata or recorded
aggregates. Per-directory rows do not inherit the root's sampling confidence.

Without an asserted growth bound, cdu reports **byte-error bound not
established**. No changes in a small sample do not prove byte accuracy: one
missed file can dominate the total.

An explicit conditional bound is available:

```sh
cdu PATH --json --summary -a \
  --sample 4096 --priority 32 --confidence 0.99 \
  --max-delta-bytes 1MiB --error-bytes 64MiB
```

This assumes every audit-population inode differs from its retained metadata
by at most the specified bytes **in either metric**, including shrinkage and
block-allocation changes. cdu cannot verify that assumption for unchecked files.
An observed violation invalidates the conditional intervals unless a file
census was completed.

For fixed sample size `n`, the conservative Hoeffding radius is:

```text
N * max_delta * sqrt(2 * ln(2 / (1 - confidence)) / n)
```

Bounds assume complete membership and a stable tree, are per metric, and are
not simultaneous guarantees for every directory, both metrics, or repeated
runs. Read failures and detected instability invalidate them. There is no
adaptive stopping or zero-variance shortcut.

`--error-bytes` selects non-interactive mode and returns exit status **2** when
the requested target cannot be established within budget.

## Storage and privacy

Cache base precedence:

```text
--cache-dir DIR
CDU_CACHE_DIR
$XDG_CACHE_HOME/cdu
$HOME/.cache/cdu
```

`cdu --cache-path PATH` prints the cache directory. It must be owned by the
current user and have mode 0700. Checkpoints contain filenames and metadata;
they are private local data, not encrypted data.

Filenames remain lossless bytes. Terminal control/bidi characters are escaped.
Allocated bytes use Linux `st_blocks * 512`; apparent bytes use `st_size`.
Sparse files and symlinks are handled without reading target contents.
Hard links count once and other aliases are marked `H`. Shared reflink extents
are not physically deduplicated.

The tool's cache, known virtual subtrees, duplicate directory aliases, and
cross-device entries under `-x` are excluded. These are not ncdu 2's
shared/unique subtree metrics.

Legacy 0.1 SQLite caches remain readable in cached mode. The new checkpoint
format is built on the next ordinary scan without deleting the old cache.
Checkpoint history is append-only in this release; repeated full rescans can
grow it. There is no background garbage-collection service.

There are no application network requests. Kernel access to a network-mounted
filesystem can still contact its server.

## Development

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --locked -- --test-threads=1
```

Tests use synthetic trees. Python 3 is required for PTY tests; GNU du provides
an independent accounting reference. Coverage includes full and incremental
accounting, sparse/hard links, byte-preserving names, partial-directory resume,
torn checkpoint recovery, exact index-batch resume, and terminal restoration.

`benches/interactive.py` is the acceptance benchmark for current behavior.
The older backend/export benchmarks are retained for historical comparison;
they do not measure time to a complete interactive browser.
