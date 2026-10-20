"""Errors and warnings sparkwarp raises. Each says what failed and how to fix it."""

from __future__ import annotations


class SparkwarpError(Exception):
    """Base class for sparkwarp errors."""


class SparkwarpWarning(UserWarning):
    """A sparkwarp call was ignored or adjusted; the run continues."""


class EngineConflictError(SparkwarpError, RuntimeError):
    """A session with another timezone was requested while one is running."""

    def __init__(self, running: str, requested: str) -> None:
        super().__init__(
            f"sparkwarp is already running {running} in this process; "
            f"cannot start {requested}. Omit the timezone to join the running "
            "session, or set the timezone per test with spark.conf.set."
        )


class EngineStoppedError(SparkwarpError, RuntimeError):
    """A stopped engine was asked for a session. Engines are not restartable."""

    def __init__(self) -> None:
        super().__init__(
            "this sparkwarp engine was stopped; call sparkwarp.session() for a new one."
        )
