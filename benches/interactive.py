"""Measure cold full-size browser interactivity, including a real keyboard response."""

import argparse
import fcntl
import json
import os
import pathlib
import pty
import re
import resource
import select
import signal
import statistics
import struct
import subprocess
import tempfile
import termios
import time


def measure(name, command, timing_path, timeout, memory_mib):
    is_cdu = name.startswith("cdu")
    master, slave = pty.openpty()
    fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 40, 120, 0, 0))

    def limits():
        limit = memory_mib * 1024 * 1024
        resource.setrlimit(resource.RLIMIT_AS, (limit, limit))
        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))

    start = time.perf_counter_ns()
    process = subprocess.Popen(
        command, stdin=slave, stdout=slave, stderr=slave, start_new_session=True,
        env={**os.environ, "TERM": "xterm-256color"}, preexec_fn=limits,
    )
    data = bytearray()
    ready_at = None
    input_at = None
    last_question = 0.0
    footer = None
    try:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if select.select([master], [], [], 0.005)[0]:
                data.extend(os.read(master, 65536))
            if process.poll() is not None:
                raise RuntimeError(f"{name} exited before becoming interactive")
            if is_cdu and b"[incomplete]" in data:
                raise AssertionError("cdu reported incomplete metadata, not a full-size browser")
            if ready_at is None:
                ready = b"[ready]" in data if is_cdu else (
                    b"Total disk usage:" in data and b"Apparent size:" in data and b"Items:" in data
                )
                if ready:
                    ready_at = (time.perf_counter_ns() - start) / 1e6
                    if name == "ncdu":
                        clean = re.sub(rb"\x1b(?:\[[0-?]*[ -/]*[@-~]|\([A-Za-z0-9])", b"", data)
                        matches = re.findall(
                            rb"Total disk usage:\s*([0-9.]+)\s*([KMGTPE]?i?B)\s+Apparent size:\s*([0-9.]+)\s*([KMGTPE]?i?B)\s+Items:\s*([0-9]+)",
                            clean,
                        )
                        if not matches:
                            raise AssertionError("Could not verify ncdu's full-size footer")
                        disk, disk_unit, apparent, apparent_unit, items = matches[-1]
                        footer = {
                            "disk": float(disk), "disk_unit": disk_unit.decode(),
                            "apparent": float(apparent), "apparent_unit": apparent_unit.decode(),
                            "items": int(items),
                        }
                    data.clear()
                    os.write(master, b"?")
                    last_question = time.monotonic()
            else:
                help_visible = b"Navigation:" in data if is_cdu else b"ncdu help" in data
                if help_visible:
                    input_at = (time.perf_counter_ns() - start) / 1e6
                    break
                if name == "ncdu" and time.monotonic() - last_question > 0.1:
                    os.write(master, b"?")
                    last_question = time.monotonic()
            if len(data) > 1_000_000:
                del data[:-100_000]
        if input_at is None:
            raise RuntimeError(f"{name} did not display a full-size responsive browser")
        stop_at = time.perf_counter_ns()
        os.write(master, b"qq")
        process.wait(timeout=15)
        if process.returncode != 0:
            raise RuntimeError(f"{name} did not exit cleanly")
        row = {"candidate": name, "full_frame_ms": ready_at, "interactive_ms": input_at,
               "stop_ms": (time.perf_counter_ns() - stop_at) / 1e6}
        if footer is not None:
            row["displayed_totals"] = footer
        if is_cdu:
            timings = json.loads(timing_path.read_text())
            if not timings["complete"] or timings["full_size_interactive_us"] is None:
                raise AssertionError("cdu's first frame was not a complete tree")
            if timings["indexing_started_us"] is not None:
                assert timings["indexing_started_us"] >= timings["full_size_interactive_us"]
            assert timings["first_input_response_us"] is not None
            assert timings["scan_counters"]["reused_directories"] == 0
            row["instrumentation"] = timings
        return row
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=5)
        os.close(master)
        os.close(slave)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=pathlib.Path)
    parser.add_argument("--binary", type=pathlib.Path, default=pathlib.Path("target/release/cdu"))
    parser.add_argument("--baseline", type=pathlib.Path, help="Also compare an older cdu binary")
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--threads", type=int, default=2)
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument("--memory-mib", type=int, default=768)
    parser.add_argument("--require-faster", action="store_true")
    args = parser.parse_args()
    if args.runs < 1:
        parser.error("runs must be positive")
    root = args.root.resolve(strict=True)
    binary = args.binary.resolve(strict=True)
    results = {"cdu": [], "ncdu": []}
    binaries = {"cdu": binary}
    if args.baseline:
        binaries["cdu_before"] = args.baseline.resolve(strict=True)
        results["cdu_before"] = []
    normal_order = ["ncdu", *[name for name in binaries if name != "cdu"], "cdu"]
    for trial in range(args.runs):
        paired = {}
        order = normal_order if trial % 2 == 0 else list(reversed(normal_order))
        for name in order:
            with tempfile.TemporaryDirectory(prefix="cdu-interactive-") as scratch:
                base = pathlib.Path(scratch)
                if root == base or root in base.parents:
                    parser.error("set TMPDIR outside the tree being measured")
                timings = base / "timings.json"
                command = ["ncdu", str(root)] if name == "ncdu" else [
                    str(binaries[name]), str(root), "--cache-dir", str(base / "cache"),
                    "--threads", str(args.threads), "--timings-json", str(timings),
                ]
                row = measure(name, command, timings, args.timeout, args.memory_mib)
                row["trial"] = trial + 1
                print(json.dumps(row), flush=True)
                paired[name] = row
                results[name].append(row["interactive_ms"])
        totals = paired["cdu"]["instrumentation"]["totals"]
        shown = paired["ncdu"]["displayed_totals"]
        units = {"B": 1, "KiB": 1024, "MiB": 1024**2, "GiB": 1024**3,
                 "TiB": 1024**4, "PiB": 1024**5, "EiB": 1024**6}
        for field, byte_field in [("disk", "disk_bytes"), ("apparent", "apparent_bytes")]:
            unit = units[shown[field + "_unit"]]
            if abs(totals[byte_field] / unit - shown[field]) > 0.051:
                raise AssertionError(f"cdu and ncdu disagree on the full {field} size")
        if totals["entries"] - 1 != shown["items"]:
            raise AssertionError("cdu and ncdu disagree on the complete item count")
        if "cdu_before" in paired and paired["cdu_before"]["instrumentation"]["totals"] != totals:
            raise AssertionError("Old/new cdu totals differ: the tree changed or accounting regressed")
    medians = {name: statistics.median(values) for name, values in results.items()}
    print(json.dumps({"median_interactive_ms": medians, "speedup": medians["ncdu"] / medians["cdu"]}), flush=True)
    if args.require_faster and medians["cdu"] >= medians["ncdu"]:
        raise SystemExit("cdu did not beat ncdu's complete-browser interactivity")
    if args.require_faster and args.baseline and medians["cdu"] >= medians["cdu_before"]:
        raise SystemExit("cdu did not improve the supplied cdu baseline")


if __name__ == "__main__":
    main()
