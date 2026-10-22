"""The shared JVM server's lifecycle: start, reuse, replace, give up, and clean up."""

import gc
import os
import signal
import subprocess
import sys
import time
import warnings
from collections.abc import Iterator
from pathlib import Path

import pytest

from sparkwarp.config import OWNER_FILE, SERVER_LOCK_FILE
from sparkwarp.jvm_server import (
    AtomicFile,
    DirectoryLock,
    JavaRuntime,
    JvmUnavailableError,
    ServerLaunch,
    ServerRecord,
    SharedJvmServer,
    StateDirectory,
)

needs_java = pytest.mark.skipif(
    not JavaRuntime.available(), reason="the JVM server needs Java 17+"
)
OWNER_GONE_SECONDS = 20
POLL_SECONDS = 0.2
SLOW_PROBES = 3


@pytest.mark.parametrize(
    ("output", "major"),
    [
        ('openjdk version "17.0.2" 2022-01-18', 17),
        ('java version "1.8.0_292"', 8),
        ('openjdk version "21" 2023-09-19', 21),
        ("no java here", 0),
    ],
)
def test_java_versions_are_read_like_spark_needs(output: str, major: int) -> None:
    assert JavaRuntime.major(output) == major


@pytest.fixture
def directory(tmp_path: Path) -> Iterator[StateDirectory]:
    (tmp_path / OWNER_FILE).write_text(str(os.getpid()))
    yield StateDirectory(tmp_path, owned=True)


@needs_java
def test_a_dead_server_is_replaced_and_the_new_one_stopped(
    directory: StateDirectory,
) -> None:
    server = SharedJvmServer(directory)
    server.url()
    first = server._read()
    assert first is not None
    os.killpg(first.pid, signal.SIGKILL)
    deadline = time.monotonic() + OWNER_GONE_SECONDS
    while first.alive() and time.monotonic() < deadline:
        time.sleep(POLL_SECONDS)
    server._record = None
    server.url()
    second = server._read()
    assert second is not None and second.pid != first.pid and second.alive()
    server.stop()
    assert not second.alive()


LOCK_HOLDER = """
import fcntl, os, sys, time
handle = os.open(sys.argv[1], os.O_CREAT | os.O_RDWR)
fcntl.flock(handle, fcntl.LOCK_EX)
print("locked", flush=True)
time.sleep(600)
"""


def test_a_lock_held_by_a_killed_process_is_free_again(tmp_path: Path) -> None:
    lock_file = tmp_path / SERVER_LOCK_FILE
    holder = subprocess.Popen(
        [sys.executable, "-c", LOCK_HOLDER, str(lock_file)],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert holder.stdout is not None and holder.stdout.readline().strip() == "locked"
    holder.kill()
    holder.wait()
    with DirectoryLock(lock_file):
        assert lock_file.exists()


@needs_java
def test_the_watchdog_stops_the_server_when_its_owner_dies(tmp_path: Path) -> None:
    owner = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(600)"])
    (tmp_path / OWNER_FILE).write_text(str(owner.pid))
    server = SharedJvmServer(StateDirectory(tmp_path, owned=False))
    server.url()
    record = server._read()
    assert record is not None and record.alive()
    owner.kill()
    owner.wait()
    deadline = time.monotonic() + OWNER_GONE_SECONDS
    while record.alive() and time.monotonic() < deadline:
        time.sleep(POLL_SECONDS)
    assert not record.alive()


class FailingLaunch:
    """Stands in for the spark-submit command with one that exits at once."""

    def __init__(self) -> None:
        self.attempts = 0

    def command(self, home: Path, port: int) -> list[str]:
        self.attempts += 1
        return [sys.executable, "-c", "raise SystemExit(1)"]


def test_a_failed_start_is_tried_once_per_run_and_its_log_kept(
    directory: StateDirectory, monkeypatch: pytest.MonkeyPatch
) -> None:
    launches = FailingLaunch()
    monkeypatch.setattr(ServerLaunch, "_command", launches.command)
    server = SharedJvmServer(directory)
    another_worker = SharedJvmServer(StateDirectory(directory.path, owned=False))
    with pytest.raises(JvmUnavailableError) as failure:
        server.url()
    with pytest.raises(JvmUnavailableError):
        server.url()
    with pytest.raises(JvmUnavailableError, match="did not start"):
        another_worker.url()
    assert launches.attempts == 1
    kept_log = Path(str(failure.value).rsplit("see ", 1)[1])
    directory.release()
    assert kept_log.exists()
    kept_log.unlink()


@needs_java
def test_a_started_server_leaves_no_unclosed_process_warning(
    directory: StateDirectory,
) -> None:
    server = SharedJvmServer(directory)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        server.url()
        gc.collect()
    server.stop()
    assert not [w for w in caught if issubclass(w.category, ResourceWarning)]


class SlowProbe:
    """A port probe that misses a few times, as under a heavily loaded run."""

    def __init__(self, record: ServerRecord, misses: int) -> None:
        self._record = record
        self._real = ServerRecord.answers
        self.misses = misses

    def answers(self) -> bool:
        if self.misses:
            self.misses -= 1
            return False
        return self._real(self._record)


@needs_java
def test_a_server_slow_to_answer_is_waited_for_not_replaced(
    directory: StateDirectory, monkeypatch: pytest.MonkeyPatch
) -> None:
    server = SharedJvmServer(directory)
    url = server.url()
    record = server._read()
    assert record is not None
    probe = SlowProbe(record, SLOW_PROBES)
    monkeypatch.setattr(ServerRecord, "answers", probe.answers)
    another_worker = SharedJvmServer(StateDirectory(directory.path, owned=False))
    try:
        assert another_worker.url() == url
        assert probe.misses == 0
        assert server._read() == record
    finally:
        monkeypatch.undo()
        server.stop()


WITHOUT_POSIX = """
import sys
import pytest, pysail.spark, pyspark.sql.connect.session
import pyspark.sql.connect.client.core
sys.modules["fcntl"] = None
sys.platform = "win32"
import sparkwarp.pytest_plugin
from sparkwarp.jvm_server import JavaRuntime
assert not JavaRuntime.available()
"""


def test_the_plugin_loads_where_there_is_no_fcntl() -> None:
    """Windows has no fcntl; sparkwarp still loads there, with no JVM routing."""
    loaded = subprocess.run(
        [sys.executable, "-c", WITHOUT_POSIX], capture_output=True, text=True
    )
    assert loaded.returncode == 0, loaded.stderr


def test_state_files_are_replaced_whole(tmp_path: Path) -> None:
    target = tmp_path / "server.json"
    AtomicFile(target).write("first")
    AtomicFile(target).write("second")
    assert target.read_text() == "second"
    assert [path.name for path in tmp_path.iterdir()] == ["server.json"]
