"""Compare first-index performance on an existing tree using disposable private caches."""

import argparse
import json
import os
import pathlib
import resource
import shutil
import signal
import statistics
import subprocess
import tempfile
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=pathlib.Path)
    parser.add_argument("--binary", type=pathlib.Path, default=pathlib.Path("target/release/cdu"))
    parser.add_argument("--baseline", type=pathlib.Path)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--memory-mib", type=int, default=768)
    args = parser.parse_args()
    if args.runs < 1 or args.timeout <= 0 or args.memory_mib < 64:
        parser.error("runs and timeout must be positive; memory-mib must be at least 64")
    root = args.root.resolve(strict=True)
    if not root.is_dir():
        parser.error("root must be a directory")
    programs = [("cdu", str(args.binary.resolve(strict=True)))]
    if args.baseline:
        programs.insert(0, ("cdu_before", str(args.baseline.resolve(strict=True))))
    if shutil.which("ncdu"):
        programs.insert(0, ("ncdu", shutil.which("ncdu")))
    results = {name: [] for name, _ in programs}

    def limits():
        limit = args.memory_mib * 1024 * 1024
        resource.setrlimit(resource.RLIMIT_AS, (limit, limit))

    for trial in range(args.runs):
        order = programs if trial % 2 == 0 else list(reversed(programs))
        for name, binary in order:
            with tempfile.TemporaryDirectory(prefix="cdu-cold-bench-") as scratch:
                base = pathlib.Path(scratch).resolve()
                if root == base or root in base.parents:
                    parser.error("set TMPDIR outside the scanned tree for a fair comparison")
                timing = base / "timing.json"
                command = [
                    "/usr/bin/time", "-f",
                    '{"user_s":%U,"system_s":%S,"rss_kib":%M}',
                    "-o", str(timing), binary,
                ]
                if name == "ncdu":
                    command += ["-0", "-o", "/dev/null", str(root)]
                else:
                    command += [str(root), "--cache-dir", str(base / "cache"),
                                "--json", "--summary"]
                started = time.perf_counter()
                process = subprocess.Popen(
                    command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                    start_new_session=True, preexec_fn=limits,
                )
                try:
                    output, errors = process.communicate(timeout=args.timeout)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.communicate()
                    raise RuntimeError(f"{name} exceeded the {args.timeout}s bound") from None
                if process.returncode:
                    raise RuntimeError(f"{name} failed: {errors.decode(errors='replace')[-1000:]}")
                row = {
                    "candidate": name, "trial": trial + 1,
                    "wall_s": time.perf_counter() - started,
                    **json.loads(timing.read_text()),
                }
                if name != "ncdu":
                    report = json.loads(output)["report"]
                    row.update(
                        entries=report["totals"]["entries"],
                        disk_bytes=report["totals"]["disk_bytes"],
                        apparent_bytes=report["totals"]["apparent_bytes"],
                        complete=report["complete"], settled=report["settled"],
                        walk_us=report["counters"].get("walk_us"),
                        index_us=report["counters"].get("index_us"),
                        database_bytes=sum(p.stat().st_size for p in (base / "cache").glob("*/index.sqlite3*")),
                    )
                print(json.dumps(row), flush=True)
                results[name].append(row)
    for name, rows in results.items():
        print(json.dumps({
            "candidate": name, "summary": True, "runs": len(rows),
            "median_wall_s": statistics.median(row["wall_s"] for row in rows),
            "median_cpu_s": statistics.median(row["user_s"] + row["system_s"] for row in rows),
            "peak_rss_kib": max(row["rss_kib"] for row in rows),
        }), flush=True)


if __name__ == "__main__":
    main()
