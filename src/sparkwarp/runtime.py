"""The one engine a process runs, who owns it, and a stop order that never hangs."""

from __future__ import annotations

import atexit
import threading
import warnings
from collections.abc import Mapping
from typing import ClassVar

from pyspark.sql import SparkSession

from sparkwarp.config import (
    DEFAULT_TIMEZONE,
    ENGINE_LABEL,
    PLUGIN_OWNS_STOP_WARNING,
    WARNING_STACKLEVEL,
    Ownership,
)
from sparkwarp.engines import SailEngine
from sparkwarp.errors import EngineConflictError, SparkwarpWarning


class Runtime:
    """Sole owner of the process's engine.

    The pytest plugin claims ownership for a whole run; library calls then join it and
    cannot stop it. A `None` timezone means "whatever is running, else UTC".
    """

    ENGINE_CLASS: ClassVar[type[SailEngine]] = SailEngine
    _active: ClassVar[Runtime | None] = None
    _lock: ClassVar[threading.Lock] = threading.Lock()
    _exit_hook_registered: ClassVar[bool] = False

    def __init__(self, engine: SailEngine, owner: Ownership) -> None:
        self.engine = engine
        self.owner = owner

    @classmethod
    def current(cls) -> Runtime | None:
        return cls._active

    @classmethod
    def start(
        cls, timezone: str | None = None, owner: Ownership = Ownership.LIBRARY
    ) -> Runtime:
        with cls._lock:
            running = cls._active
            if running is not None:
                running.ensure_matches(timezone)
                return running
            engine = cls.ENGINE_CLASS(timezone or DEFAULT_TIMEZONE)
            cls._active = cls._prepared(engine, owner)
            cls._register_exit_hook()
            return cls._active

    @classmethod
    def release(cls, requester: Ownership) -> None:
        """Stop the engine if `requester` may; otherwise warn and keep it running."""
        with cls._lock:
            running = cls._active
            plugin_keeps_it = (
                running is not None
                and running.owner is Ownership.PLUGIN
                and requester is not Ownership.PLUGIN
            )
            if not plugin_keeps_it:
                cls._active = None
        if plugin_keeps_it:
            warnings.warn(
                PLUGIN_OWNS_STOP_WARNING,
                SparkwarpWarning,
                stacklevel=WARNING_STACKLEVEL,
            )
        elif running is not None:
            running.engine.stop()

    @classmethod
    def shutdown(cls) -> None:
        """Stop whatever runs, whoever owns it. For process exit and test isolation."""
        with cls._lock:
            running, cls._active = cls._active, None
        if running is not None:
            running.engine.stop()

    @classmethod
    def claim(cls, owner: Ownership) -> Ownership | None:
        """Make `owner` the owner of the running engine; return the previous owner."""
        with cls._lock:
            running = cls._active
            if running is None:
                return None
            previous, running.owner = running.owner, owner
            return previous

    def environ_for_children(self, environ: Mapping[str, str]) -> dict[str, str]:
        return self.engine.environ_without_routing(environ)

    def open_session(self) -> SparkSession:
        return self.engine.start_session()

    def ensure_matches(self, timezone: str | None) -> None:
        other_timezone = timezone is not None and timezone != self.engine.timezone
        if other_timezone:
            raise EngineConflictError(self.engine.label, f"{ENGINE_LABEL} ({timezone})")

    @classmethod
    def _prepared(cls, engine: SailEngine, owner: Ownership) -> Runtime:
        try:
            engine.prepare_environment()
        except BaseException:
            engine.stop()
            raise
        return Runtime(engine, owner)

    @classmethod
    def _register_exit_hook(cls) -> None:
        if not cls._exit_hook_registered:
            atexit.register(cls.shutdown)
            cls._exit_hook_registered = True
