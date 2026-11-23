"""Exercise the real terminal UI on synthetic data; no terminal services persist."""

import fcntl
import os
import pathlib
import pty
import select
import sqlite3
import struct
import subprocess
import tempfile
import termios
import time
import unittest


class Terminal:
    def __init__(self, root, cache):
        self.master, self.slave = pty.openpty()
        fcntl.ioctl(self.slave, termios.TIOCSWINSZ, struct.pack("HHHH", 24, 110, 0, 0))
        self.original = termios.tcgetattr(self.slave)
        self.process = subprocess.Popen(
            [os.environ["CDU_TEST_BINARY"], str(root), "--cache-dir", str(cache)],
            stdin=self.slave,
            stdout=self.slave,
            stderr=self.slave,
            env={**os.environ, "TERM": "xterm-256color"},
            start_new_session=True,
        )
        self.output = b""

    def wait_for(self, text, start=0):
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if text in self.output[start:]:
                return
            if select.select([self.master], [], [], 0.1)[0]:
                self.output += os.read(self.master, 65536)
            if self.process.poll() is not None:
                break
        raise AssertionError(f"Missing {text!r}; output tail: {self.output[-4000:]!r}")

    def send(self, value):
        start = len(self.output)
        os.write(self.master, value)
        return start

    def quit(self):
        self.send(b"q")
        self.process.wait(timeout=10)
        if self.process.returncode != 0:
            raise AssertionError(f"UI exited with {self.process.returncode}")
        current = termios.tcgetattr(self.slave)
        mask = termios.ICANON | termios.ECHO | termios.ISIG
        if current[3] & mask != self.original[3] & mask:
            raise AssertionError("Terminal mode was not restored")

    def close(self):
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        os.close(self.master)
        os.close(self.slave)


class TerminalTests(unittest.TestCase):
    @unittest.skipIf(os.geteuid() == 0, "root can read mode-000 directories")
    def test_recorded_errors_can_be_reviewed_and_opened(self):
        with tempfile.TemporaryDirectory(prefix="cdu-errors-") as temp:
            root = pathlib.Path(temp) / "tree"
            cache = pathlib.Path(temp) / "cache"
            blocked = root / "blocked"
            blocked.mkdir(parents=True)
            blocked.chmod(0)
            terminal = Terminal(root, cache)
            try:
                terminal.wait_for(b"1 errors")
                terminal.wait_for(b"[incomplete]")
                start = terminal.send(b"E")
                terminal.wait_for(b"Recorded filesystem errors", start)
                terminal.wait_for(str(blocked).encode(), start)
                terminal.wait_for(b"Permission denied", start)
                start = terminal.send(b"\r")
                terminal.wait_for(str(root).encode(), start)
                terminal.wait_for(b"Permission denied", start)
                terminal.quit()
            finally:
                blocked.chmod(0o700)
                terminal.close()

    def test_navigation_help_controls_and_quit(self):
        with tempfile.TemporaryDirectory(prefix="cdu-ui-") as temp:
            root = pathlib.Path(temp) / "tree"
            cache = pathlib.Path(temp) / "cache"
            (root / "nested").mkdir(parents=True)
            (root / "nested" / "leaf").write_bytes(b"data")
            (root / "root-file").write_bytes(b"data")
            (root / "bad-\x1b]0;INJECT\x07").write_bytes(b"data")
            subprocess.run(
                [os.environ["CDU_TEST_BINARY"], str(root), "--cache-dir", str(cache),
                 "--quick", "--summary"],
                check=True, stdout=subprocess.DEVNULL, timeout=15,
            )
            legacy = next(cache.glob("*/index.sqlite3"), None)
            if legacy:
                with sqlite3.connect(legacy) as connection:
                    connection.execute("ALTER TABLE nodes DROP COLUMN direct_children")
                    connection.execute("PRAGMA user_version=1")
            terminal = Terminal(root, cache)
            try:
                terminal.wait_for(b"root-file")
                terminal.wait_for(b"dirs checked")
                terminal.wait_for(b"[ready]")
                start = terminal.send(b"\r")
                terminal.wait_for(str(root / "nested").encode(), start)
                terminal.wait_for(b"leaf", start)
                (root / "nested" / "new").write_bytes(b"new data")
                start = terminal.send(b"r")
                terminal.wait_for(str(root / "nested").encode(), start)
                terminal.wait_for(b"new", start)
                start = terminal.send(b"?")
                terminal.wait_for(b"Directory timestamps cannot reveal", start)
                terminal.send(b"?")
                terminal.send(b"h")
                terminal.quit()
                self.assertNotIn(b"\x1b]0;INJECT\x07", terminal.output)
            finally:
                terminal.close()

    def test_cancel_exits_without_leaving_a_writer(self):
        with tempfile.TemporaryDirectory(prefix="cdu-cancel-") as temp:
            root = pathlib.Path(temp) / "tree"
            cache = pathlib.Path(temp) / "cache"
            root.mkdir()
            for index in range(6000):
                (root / f"file-{index}").touch()
            terminal = Terminal(root, cache)
            try:
                terminal.wait_for(b" - cached disk usage")
                terminal.quit()
            finally:
                terminal.close()
            result = subprocess.run(
                [os.environ["CDU_TEST_BINARY"], str(root), "--cache-dir", str(cache),
                 "--quick", "--summary", "--json"],
                capture_output=True, timeout=30,
            )
            self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
