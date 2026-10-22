"""One JVM Spark Connect server per pytest run, shared by every worker process.

The first process that needs it starts it, under a lock in a state directory that the
main pytest process creates; the main process owns the directory and stops the server.
The server runs from the installed PySpark, so its version always matches the client,
under a watchdog that stops it if the owner dies without doing so.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from dataclasses import asdict, dataclass
from functools import cache
from pathlib import Path

import pyspark

from sparkwarp.config import (
    ATOMIC_SUFFIX,
    CONNECT_BIND_ADDRESS,
    CONNECT_MASTER,
    CONNECT_MODE_ENV_VAR,
    CONNECT_PORT_KEY,
    CONNECT_SERVER_CLASS,
    CONNECT_SERVER_CONF,
    CONNECT_URL_SCHEME,
    FAILED_LOG_NAME,
    FAILURE_FILE,
    JAVA_BIN_DIR,
    JAVA_EXECUTABLE,
    JAVA_HOME_ENV_VAR,
    JAVA_LEGACY_MAJOR,
    JAVA_MINIMUM_VERSION,
    JAVA_VERSION_PATTERN,
    JVM_BUSY_WAIT_SECONDS,
    JVM_DEFAULT_TIMEZONE_OPTION,
    JVM_DIR_ENV_VAR,
    JVM_DIR_PREFIX,
    JVM_START_FAILED,
    JVM_START_TIMEOUT_SECONDS,
    JVM_STOP_TIMEOUT_SECONDS,
    LOCK_POLL_SECONDS,
    LOCK_TIMEOUT,
    LOCK_WAIT_SECONDS,
    MASTER_ENV_VAR,
    OWNER_FILE,
    PORT_PROBE_SECONDS,
    PROC_CMDLINE,
    PROC_STATUS,
    PYSPARK_PYTHON_ENV_VAR,
    REMOTE_ENV_VAR,
    SERVER_LOCK_FILE,
    SERVER_LOG_FILE,
    SERVER_STATE_FILE,
    SPARK_HOME_ENV_VAR,
    SPARK_INTERNAL_RESOURCE,
    SPARK_SUBMIT,
    START_EXITED,
    START_TIMED_OUT,
    WATCHDOG_MODULE,
    WINDOWS_PLATFORM,
    ZOMBIE_STATE,
)

POSIX = sys.platform != WINDOWS_PLATFORM
if POSIX:
    import fcntl


class JvmUnavailableError(RuntimeError):
    """The JVM server cannot run here; the router keeps everything on Sail."""


class JavaRuntime:
    """The Java that spark-submit will use, and whether it is new enough for Spark 4."""

    @staticmethod
    def executable() -> str | None:
        java_home = os.environ.get(JAVA_HOME_ENV_VAR, "")
        from_home = Path(java_home, JAVA_BIN_DIR, JAVA_EXECUTABLE)
        if java_home and from_home.exists():
            return str(from_home)
        return shutil.which(JAVA_EXECUTABLE)

    @staticmethod
    @cache
    def available() -> bool:
        """Java 17+ on a POSIX system; the server's process handling needs POSIX."""
        java = JavaRuntime.executable()
        if java is None or not POSIX:
            return False
        try:
            probe = subprocess.run(
                [java, "-version"], capture_output=True, text=True, check=False
            )
        except OSError:
            return False
        return JavaRuntime.major(probe.stderr + probe.stdout) >= JAVA_MINIMUM_VERSION

    @staticmethod
    def major(version_output: str) -> int:
        """`17.0.2` is 17; the old `1.8.0` scheme is 8; no version is 0."""
        found = re.search(JAVA_VERSION_PATTERN, version_output)
        if found is None:
            return 0
        first, second = int(found.group(1)), int(found.group(2) or 0)
        return second if first == JAVA_LEGACY_MAJOR else first


@dataclass(frozen=True)
class ServerRecord:
    """Where the shared server listens, and the watchdog process that runs it."""

    pid: int
    port: int

    @property
    def url(self) -> str:
        return f"{CONNECT_URL_SCHEME}{CONNECT_BIND_ADDRESS}:{self.port}"

    def alive(self) -> bool:
        return ProcessGroup(self.pid).running() and self.answers()

    def answers(self) -> bool:
        address = (CONNECT_BIND_ADDRESS, self.port)
        try:
            with socket.create_connection(address, timeout=PORT_PROBE_SECONDS):
                return True
        except OSError:
            return False

    def settles(self, seconds: float) -> bool:
        """Running and answering within `seconds`: a busy server is not a dead one."""
        deadline = time.monotonic() + seconds
        while ProcessGroup(self.pid).running():
            if self.answers():
                return True
            if time.monotonic() > deadline:
                return False
            time.sleep(LOCK_POLL_SECONDS)
        return False


class StateDirectory:
    """The run's state directory: created by the main process, inherited by workers."""

    def __init__(self, path: Path, owned: bool) -> None:
        self.path = path
        self.owned = owned

    @classmethod
    def claim(cls) -> StateDirectory:
        inherited = os.environ.get(JVM_DIR_ENV_VAR)
        if inherited:
            return cls(Path(inherited), owned=False)
        path = Path(tempfile.mkdtemp(prefix=JVM_DIR_PREFIX))
        (path / OWNER_FILE).write_text(str(os.getpid()))
        os.environ[JVM_DIR_ENV_VAR] = str(path)
        return cls(path, owned=True)

    @property
    def owner_pid(self) -> int:
        owner = self.path / OWNER_FILE
        return int(owner.read_text()) if owner.exists() else os.getpid()

    def release(self) -> None:
        if self.owned:
            os.environ.pop(JVM_DIR_ENV_VAR, None)
            shutil.rmtree(self.path, ignore_errors=True)


class SharedJvmServer:
    """Starts the server on first need and hands every caller the same address."""

    def __init__(self, directory: StateDirectory) -> None:
        self._directory = directory
        self._record: ServerRecord | None = None
        self._failure: str | None = None
        self._launches: list[ServerLaunch] = []

    @property
    def owned(self) -> bool:
        return self._directory.owned

    @property
    def directory(self) -> Path:
        return self._directory.path

    def answering(self) -> bool:
        """The server this process uses still answers; never starts one."""
        return self._record is not None and self._record.alive()

    def recorded_failure(self) -> str | None:
        """Why the run's server could not start, if any process has tried."""
        return self._failure or self._shared_failure()

    def url(self) -> str:
        """The server's address, starting it if needed; a failure is remembered.

        A failed start is written to the state directory, so the run tries only once.
        """
        self._failure = self._failure or self._shared_failure()
        if self._failure is not None:
            raise JvmUnavailableError(self._failure)
        if self._record is not None and self._record.alive():
            return self._record.url
        try:
            self._record = self._running_record()
        except (OSError, RuntimeError) as error:
            self._failure = str(error)
            raise JvmUnavailableError(self._failure) from error
        return self._record.url

    def stop(self) -> None:
        """Stop the server and drop the state directory; only the owner does this.

        Another process leaves the server running for the rest of the run.
        """
        if self.owned:
            record = self._read()
            if record is not None:
                ProcessGroup(record.pid).stop()
            self._directory.release()
        self._forget_finished_launches()

    def _running_record(self) -> ServerRecord:
        with DirectoryLock(self._directory.path / SERVER_LOCK_FILE):
            failure = self._shared_failure()
            if failure is not None:
                raise RuntimeError(failure)
            record = self._read()
            if record is not None and record.settles(JVM_BUSY_WAIT_SECONDS):
                return record
            if record is not None:
                ProcessGroup(record.pid).stop()
                self._forget_finished_launches()
            record = self._launch()
            state = self._directory.path / SERVER_STATE_FILE
            AtomicFile(state).write(json.dumps(asdict(record)))
            return record

    def _launch(self) -> ServerRecord:
        launch = ServerLaunch(self._directory)
        self._launches.append(launch)
        try:
            return launch.run()
        except RuntimeError as error:
            AtomicFile(self._directory.path / FAILURE_FILE).write(str(error))
            raise

    def _forget_finished_launches(self) -> None:
        """Polls launches reaped elsewhere, so Popen does not warn they still run."""
        self._launches = [launch for launch in self._launches if launch.running()]

    def _shared_failure(self) -> str | None:
        failure = self._directory.path / FAILURE_FILE
        return failure.read_text() if failure.exists() else None

    def _read(self) -> ServerRecord | None:
        state = self._directory.path / SERVER_STATE_FILE
        if not state.exists():
            return None
        return ServerRecord(**json.loads(state.read_text()))


class AtomicFile:
    """A file other processes read whole: written aside, then renamed into place."""

    def __init__(self, path: Path) -> None:
        self._path = path

    def write(self, text: str) -> None:
        aside = self._path.with_name(self._path.name + ATOMIC_SUFFIX)
        aside.write_text(text)
        os.replace(aside, self._path)


class DirectoryLock:
    """An exclusive `flock` on a file; the kernel releases it if the holder dies."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._handle: int | None = None

    def __enter__(self) -> DirectoryLock:
        handle = os.open(self._path, os.O_CREAT | os.O_RDWR)
        deadline = time.monotonic() + LOCK_WAIT_SECONDS
        while True:
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() > deadline:
                    os.close(handle)
                    reason = LOCK_TIMEOUT.format(
                        path=self._path, seconds=LOCK_WAIT_SECONDS
                    )
                    raise TimeoutError(reason) from None
                time.sleep(LOCK_POLL_SECONDS)
        self._handle = handle
        return self

    def __exit__(self, *exc_info: object) -> None:
        handle, self._handle = self._handle, None
        if handle is not None:
            fcntl.flock(handle, fcntl.LOCK_UN)
            os.close(handle)


class ServerLaunch:
    """Runs `spark-submit` under the watchdog and waits until the server answers."""

    def __init__(self, directory: StateDirectory) -> None:
        self._directory = directory
        self.process: subprocess.Popen[bytes] | None = None

    def run(self) -> ServerRecord:
        """Starts the server; the process stays referenced so it is never collected."""
        port = self._free_port()
        home = Path(pyspark.__file__).parent
        log_path = self._directory.path / SERVER_LOG_FILE
        with open(log_path, "ab") as log:
            process = subprocess.Popen(
                self._command(home, port),
                env=self._environment(home),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=log,
                start_new_session=True,
            )
        self.process = process
        record = ServerRecord(process.pid, port)
        with self._stopped_on_failure(record, log_path):
            self._wait_until_ready(process, record)
        return record

    def running(self) -> bool:
        return self.process is not None and self.process.poll() is None

    def _command(self, home: Path, port: int) -> list[str]:
        watchdog = [
            sys.executable,
            "-m",
            WATCHDOG_MODULE,
            str(self._directory.owner_pid),
        ]
        settings = {**CONNECT_SERVER_CONF, CONNECT_PORT_KEY: str(port)}
        confs = [
            part
            for key, value in settings.items()
            for part in ("--conf", f"{key}={value}")
        ]
        submit = [
            str(home.joinpath(*SPARK_SUBMIT)),
            "--class",
            CONNECT_SERVER_CLASS,
            "--driver-java-options",
            JVM_DEFAULT_TIMEZONE_OPTION,
        ]
        return [
            *watchdog,
            *submit,
            "--master",
            CONNECT_MASTER,
            *confs,
            SPARK_INTERNAL_RESOURCE,
        ]

    @staticmethod
    def _environment(home: Path) -> dict[str, str]:
        """The installed PySpark and this Python, without the routing to Sail."""
        routing = {REMOTE_ENV_VAR, CONNECT_MODE_ENV_VAR, MASTER_ENV_VAR}
        environment = {k: v for k, v in os.environ.items() if k not in routing}
        environment[SPARK_HOME_ENV_VAR] = str(home)
        environment[PYSPARK_PYTHON_ENV_VAR] = sys.executable
        return environment

    @staticmethod
    def _free_port() -> int:
        with socket.socket() as probe:
            probe.bind((CONNECT_BIND_ADDRESS, 0))
            port: int = probe.getsockname()[1]
            return port

    @staticmethod
    @contextmanager
    def _stopped_on_failure(record: ServerRecord, log: Path) -> Iterator[None]:
        """Stops a server that did not come up, even on Ctrl-C, and keeps its log."""
        try:
            yield
        except BaseException as error:
            ProcessGroup(record.pid).stop()
            if not isinstance(error, RuntimeError):
                raise
            kept = Path(tempfile.gettempdir(), FAILED_LOG_NAME.format(pid=os.getpid()))
            shutil.copyfile(log, kept)
            reason = JVM_START_FAILED.format(reason=error, log=kept)
            raise RuntimeError(reason) from error

    @staticmethod
    def _wait_until_ready(
        process: subprocess.Popen[bytes], record: ServerRecord
    ) -> None:
        deadline = time.monotonic() + JVM_START_TIMEOUT_SECONDS
        while not record.alive():
            exited = process.poll() is not None
            if exited or time.monotonic() > deadline:
                reason = START_EXITED if exited else START_TIMED_OUT
                raise RuntimeError(reason)
            time.sleep(LOCK_POLL_SECONDS)


class ProcessGroup:
    """The watchdog and the server under it, stopped together."""

    def __init__(self, leader: int) -> None:
        self._leader = leader

    def running(self) -> bool:
        """Alive, not a zombie, and still sparkwarp's watchdog (pids get reused)."""
        with suppress(ChildProcessError):
            reaped, _ = os.waitpid(self._leader, os.WNOHANG)
            if reaped:
                return False
        try:
            os.killpg(self._leader, 0)
        except (ProcessLookupError, PermissionError):
            return False
        return not self._zombie() and self._is_watchdog()

    def stop(self) -> None:
        if not self.running():
            return
        self._signal(signal.SIGTERM)
        deadline = time.monotonic() + JVM_STOP_TIMEOUT_SECONDS
        while self.running() and time.monotonic() < deadline:
            time.sleep(LOCK_POLL_SECONDS)
        if self.running():
            self._signal(signal.SIGKILL)

    def _signal(self, signal_number: int) -> None:
        with suppress(ProcessLookupError):
            os.killpg(self._leader, signal_number)

    def _zombie(self) -> bool:
        status = Path(PROC_STATUS.format(pid=self._leader))
        return status.exists() and ZOMBIE_STATE in status.read_text()

    def _is_watchdog(self) -> bool:
        cmdline = Path(PROC_CMDLINE.format(pid=self._leader))
        if not cmdline.exists():
            return True
        return WATCHDOG_MODULE in cmdline.read_text(errors="replace")
