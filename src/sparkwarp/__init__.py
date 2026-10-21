"""sparkwarp: run existing PySpark DataFrame tests in milliseconds, with no JVM."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

from sparkwarp.config import Ownership
from sparkwarp.environment import EnvironmentPatch
from sparkwarp.errors import (
    EngineConflictError,
    EngineStoppedError,
    SparkwarpError,
    SparkwarpWarning,
)
from sparkwarp.runtime import Runtime

__version__ = "0.2.0"
__all__ = [
    "EngineConflictError",
    "EngineStoppedError",
    "SparkwarpError",
    "SparkwarpWarning",
    "__version__",
    "clean_environ",
    "session",
    "stop",
]


def session(timezone: str | None = None) -> SparkSession:
    """Return the process-wide SparkSession on Sail, starting it on first call.

    Omit `timezone` to join the running session; the default for a new one is UTC.
    """
    return Runtime.start(timezone).open_session()


def stop() -> None:
    """Stop the session and engine. Ignored with a warning inside a sparkwarp run."""
    Runtime.release(Ownership.LIBRARY)


def clean_environ() -> dict[str, str]:
    """A copy of os.environ as it was before sparkwarp, for `subprocess` calls.

    Child processes inherit SPARK_REMOTE but not sparkwarp's builder patch, so a child
    that builds its own session should get this environment. It also drops SPARKWARP,
    so a child `pytest` runs real Spark.
    """
    running = Runtime.current()
    if running is None:
        return EnvironmentPatch.inherited().undone_on(os.environ)
    return running.environ_for_children(os.environ)
