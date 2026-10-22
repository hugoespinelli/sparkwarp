"""Sail, run in-process as a Spark Connect server that `pyspark.sql` routes to.

The engine changes process-wide state (env vars, the SparkSession builder). Only
`sparkwarp.runtime.Runtime` should own one; it guarantees a single engine per process.
"""

from __future__ import annotations

import os
import shutil
import tempfile
from collections.abc import Mapping
from contextlib import ExitStack
from pathlib import Path

from pysail.spark import SparkConnectServer
from pyspark.sql import SparkSession
from pyspark.sql.connect.session import SparkSession as ConnectSession

from sparkwarp.compat import CompatShims
from sparkwarp.config import (
    ANSI_KEY,
    ANSI_ON,
    ANY_FREE_PORT,
    CHECKPOINT_DIR_PREFIX,
    CHECKPOINT_ENV_VAR,
    DEFAULT_TIMEZONE,
    ENGINE_LABEL,
    LOCALHOST,
    SAIL_STACK_SIZE_BYTES,
    STACK_SIZE_ENV_VAR,
    TIMEZONE_KEY,
)
from sparkwarp.environment import (
    EnvironmentPatch,
    MasterlessPatch,
    ServerAddress,
    SessionContractPatch,
)
from sparkwarp.errors import EngineStoppedError
from sparkwarp.jvm_server import SharedJvmServer, StateDirectory
from sparkwarp.router import JvmSession, PlanRouter, RoutingSummary


class CheckpointDir:
    """A per-engine temp directory for `localCheckpoint()` and `checkpoint()`.

    A path the user already set wins, and then sparkwarp creates nothing.
    """

    def __init__(self) -> None:
        self.path: Path | None = None

    def create(self) -> Path | None:
        """A fresh directory, or None when the user chose a path."""
        user_chose_a_path = bool(os.environ.get(CHECKPOINT_ENV_VAR))
        if not user_chose_a_path:
            self.path = Path(tempfile.mkdtemp(prefix=CHECKPOINT_DIR_PREFIX))
        return self.path

    def remove(self) -> None:
        path, self.path = self.path, None
        if path is not None:
            shutil.rmtree(path, ignore_errors=True)


class ServerSettings:
    """Sail settings that sparkwarp chooses, set only while the server starts.

    Sail reads its configuration when a server object is built, so the settings never
    reach the process environment or child processes. A value the user set wins. Sail
    reads the stack size once per process, when the first server is built.
    The larger thread stack lets Sail plan columns nested several hundred calls deep, so
    PySpark's own recursion limit stops a deeper column before Sail can overflow.
    """

    def __init__(self, checkpoint_path: Path | None) -> None:
        self._checkpoint_path = checkpoint_path

    def scoped_vars(self) -> dict[str, str]:
        scoped: dict[str, str] = {}
        user_chose_a_stack_size = bool(os.environ.get(STACK_SIZE_ENV_VAR))
        if not user_chose_a_stack_size:
            scoped[STACK_SIZE_ENV_VAR] = str(SAIL_STACK_SIZE_BYTES)
        if self._checkpoint_path is not None:
            scoped[CHECKPOINT_ENV_VAR] = str(self._checkpoint_path)
        return scoped

    def launch(self) -> SparkConnectServer:
        """Build and start a Sail server with these settings."""
        scoped = EnvironmentPatch(set_vars=self.scoped_vars())
        scoped.apply()
        try:
            server = SparkConnectServer(LOCALHOST, ANY_FREE_PORT)
            server.start(background=True)
            return server
        finally:
            scoped.restore()


class SailEngine:
    """Owns one Sail server plus the env vars and patches that point PySpark at it."""

    def __init__(self, timezone: str = DEFAULT_TIMEZONE) -> None:
        self.timezone = timezone
        self._server: SparkConnectServer | None = None
        self._environment: EnvironmentPatch | None = None
        self._masterless_patch = MasterlessPatch()
        self._contract_patch = SessionContractPatch(
            self.contract, self.has_live_session
        )
        self._jvm_session = JvmSession(self.contract)
        self.router = PlanRouter(self._jvm_session)
        self._shims = CompatShims(self.router)
        self._jvm_server: SharedJvmServer | None = None
        self._checkpoint = CheckpointDir()
        self._stopped = False

    @property
    def checkpoint_dir(self) -> Path | None:
        return self._checkpoint.path

    @property
    def contract(self) -> dict[str, str]:
        return {TIMEZONE_KEY: self.timezone, ANSI_KEY: ANSI_ON}

    @property
    def label(self) -> str:
        return f"{ENGINE_LABEL} ({self.timezone})"

    @property
    def prepared(self) -> bool:
        return self._server is not None

    @property
    def stopped(self) -> bool:
        return self._stopped

    def prepare_environment(self) -> None:
        """Start the server and route PySpark to it. Idempotent; refused after stop."""
        if self._stopped:
            raise EngineStoppedError()
        if self.prepared:
            return
        server = ServerSettings(self._checkpoint.create()).launch()
        self._server = server
        host, port = server.listening_address
        self._environment = EnvironmentPatch.connect_to(ServerAddress(host, port))
        self._environment.apply()
        self._masterless_patch.install()
        self._contract_patch.install()
        self._shims.install()
        self._jvm_server = SharedJvmServer(StateDirectory.claim())
        self._jvm_session.attach(self._jvm_server)

    def start_session(self) -> SparkSession:
        """Return the live session; build one when none is open."""
        self.prepare_environment()
        return SparkSession.builder.getOrCreate()

    def environ_without_routing(self, environ: Mapping[str, str]) -> dict[str, str]:
        """`environ` with this engine's env changes undone, for child processes."""
        if self._environment is None:
            return dict(environ)
        return self._environment.undone_on(environ)

    def has_live_session(self) -> bool:
        return bool(self._live_sessions())

    def stop(self) -> None:
        """Release everything, even if a step fails. Idempotent; the engine is spent."""
        self._stopped = True
        with ExitStack() as cleanup:
            cleanup.callback(self._stop_jvm_server)
            cleanup.callback(self._checkpoint.remove)
            cleanup.callback(self._contract_patch.uninstall)
            cleanup.callback(self._masterless_patch.uninstall)
            cleanup.callback(self._shims.uninstall)
            cleanup.callback(self._shims.shutdown_release_pool)
            cleanup.callback(self._restore_environment)
            cleanup.callback(self._stop_server)
            for session in self._live_sessions():
                cleanup.callback(self._shims.close, session)
            cleanup.callback(self._close_jvm_session)

    def _live_sessions(self) -> list[ConnectSession]:
        """Every open Connect session sparkwarp knows of: shared, active, and held.

        Sail hangs on shutdown while a client channel is open, so stop() closes these
        first. Runtime allows one Sail server per process, so all are its clients.
        """
        candidates = (
            ConnectSession.getActiveSession(),
            ConnectSession._get_default_session(),
            *self._shims.held_sessions,
        )
        unique = {id(s): s for s in candidates if s is not None}
        return [session for session in unique.values() if not session.is_stopped]

    def routing_summary(self) -> RoutingSummary:
        """What this run sent to the JVM, across the processes sharing the server."""
        server = self._jvm_server
        directory = server.directory if server is not None else None
        return RoutingSummary(directory, self.router.counts)

    def _close_jvm_session(self) -> None:
        self._jvm_session.close(self._shims.close)

    def _stop_jvm_server(self) -> None:
        """Save this process's routing counts; the run's owner also stops the server."""
        server, self._jvm_server = self._jvm_server, None
        if server is None:
            return
        self.router.save_counts(server.directory)
        server.stop()

    def _stop_server(self) -> None:
        server, self._server = self._server, None
        if server is not None:
            server.stop()

    def _restore_environment(self) -> None:
        environment, self._environment = self._environment, None
        if environment is not None:
            environment.restore()
