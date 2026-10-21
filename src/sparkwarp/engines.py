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
    TIMEZONE_KEY,
)
from sparkwarp.environment import (
    EnvironmentPatch,
    MasterlessPatch,
    ServerAddress,
    SessionContractPatch,
)
from sparkwarp.errors import EngineStoppedError


class CheckpointDir:
    """A per-engine temp directory for `localCheckpoint()` and `checkpoint()`.

    Sail reads its configuration when the server object is built, so the setting
    exists only while the server is built and started. It never reaches the process
    environment or child processes.
    A path the user already set wins, and then sparkwarp creates nothing.
    """

    def __init__(self) -> None:
        self.path: Path | None = None

    def launch(self) -> SparkConnectServer:
        """Build and start a Sail server that knows where to checkpoint."""
        user_chose_a_path = bool(os.environ.get(CHECKPOINT_ENV_VAR))
        if user_chose_a_path:
            return self._started()
        self.path = Path(tempfile.mkdtemp(prefix=CHECKPOINT_DIR_PREFIX))
        scoped = EnvironmentPatch(set_vars={CHECKPOINT_ENV_VAR: str(self.path)})
        scoped.apply()
        try:
            return self._started()
        finally:
            scoped.restore()

    @staticmethod
    def _started() -> SparkConnectServer:
        server = SparkConnectServer(LOCALHOST, ANY_FREE_PORT)
        server.start(background=True)
        return server

    def remove(self) -> None:
        path, self.path = self.path, None
        if path is not None:
            shutil.rmtree(path, ignore_errors=True)


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
        self._shims = CompatShims()
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
        server = self._checkpoint.launch()
        self._server = server
        host, port = server.listening_address
        self._environment = EnvironmentPatch.connect_to(ServerAddress(host, port))
        self._environment.apply()
        self._masterless_patch.install()
        self._contract_patch.install()
        self._shims.install()

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
            cleanup.callback(self._checkpoint.remove)
            cleanup.callback(self._contract_patch.uninstall)
            cleanup.callback(self._masterless_patch.uninstall)
            cleanup.callback(self._shims.uninstall)
            cleanup.callback(self._shims.shutdown_release_pool)
            cleanup.callback(self._restore_environment)
            cleanup.callback(self._stop_server)
            for session in self._live_sessions():
                cleanup.callback(self._shims.close, session)

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

    def _stop_server(self) -> None:
        server, self._server = self._server, None
        if server is not None:
            server.stop()

    def _restore_environment(self) -> None:
        environment, self._environment = self._environment, None
        if environment is not None:
            environment.restore()
