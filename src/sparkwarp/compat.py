"""Spark Connect compatibility shims for lines that most PySpark conftests contain.

Each shim is a reversible class patch, installed while sparkwarp owns an engine:
`spark.sparkContext.setLogLevel(...)`, `spark.catalog.clearCache()`, `spark.stop()` and
`df.rdd.isEmpty()` keep working. Anything else on `sparkContext` or `rdd` still fails as
loudly as before.
"""

from __future__ import annotations

from collections.abc import Callable
from types import MethodType
from typing import NoReturn, cast

from pyspark.errors import PySparkAttributeError, PySparkNotImplementedError
from pyspark.sql.connect.catalog import Catalog as ConnectCatalog
from pyspark.sql.connect.client.reattach import ExecutePlanResponseReattachableIterator
from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame
from pyspark.sql.connect.session import SparkSession as ConnectSession

from sparkwarp.config import (
    CLOSE_WITHOUT_ORIGINAL_ERROR,
    FEATURE_PARAMETER,
    JVM_ONLY_CONDITION,
    NOT_IMPLEMENTED_CONDITION,
    RDD_FEATURE,
    STUB_ATTRIBUTE_PARAMETER,
    STUB_DEFAULT_PARALLELISM,
    STUB_PREFIX,
)
from sparkwarp.patches import ClassPatch


class SparkContextStub:
    """The few `SparkContext` members that tests touch only for setup and logging."""

    def __init__(self, session: ConnectSession) -> None:
        self._session = session

    def setLogLevel(self, level: str) -> None:  # noqa: N802 - PySpark's name
        """Accepted and ignored: Sail's logging is not controlled from PySpark."""

    @property
    def applicationId(self) -> str:  # noqa: N802 - PySpark's name
        return str(self._session.session_id)

    @property
    def defaultParallelism(self) -> int:  # noqa: N802 - PySpark's name
        return STUB_DEFAULT_PARALLELISM

    @property
    def version(self) -> str:
        return str(self._session.version)

    def __getattr__(self, name: str) -> NoReturn:
        raise PySparkAttributeError(
            errorClass=JVM_ONLY_CONDITION,
            messageParameters={STUB_ATTRIBUTE_PARAMETER: STUB_PREFIX + name},
        )


class StubbedSparkContext:
    """Descriptor that answers `session.sparkContext` with a stub."""

    def __get__(
        self, session: ConnectSession | None, owner: type | None = None
    ) -> SparkContextStub | StubbedSparkContext:
        if session is None:
            return self
        return SparkContextStub(session)


class DataFrameRddStub:
    """`df.rdd.isEmpty()`: the one `RDD` call that asks only about the DataFrame."""

    def __init__(self, frame: ConnectDataFrame) -> None:
        self._frame = frame

    def isEmpty(self) -> bool:  # noqa: N802 - PySpark's name
        return self._frame.isEmpty()

    def __getattr__(self, name: str) -> NoReturn:
        raise PySparkNotImplementedError(
            errorClass=NOT_IMPLEMENTED_CONDITION,
            messageParameters={FEATURE_PARAMETER: RDD_FEATURE},
        )


class StubbedRdd:
    """Descriptor that answers `df.rdd` with a stub."""

    def __get__(
        self, frame: ConnectDataFrame | None, owner: type | None = None
    ) -> DataFrameRddStub | StubbedRdd:
        if frame is None:
            return self
        return DataFrameRddStub(frame)


class IgnoredCall:
    """Descriptor for a method that does nothing and returns None."""

    def __get__(
        self, instance: object, owner: type | None = None
    ) -> Callable[..., None]:
        return self.ignore

    @staticmethod
    def ignore(*args: object, **kwargs: object) -> None:
        """Accepts any call and does nothing."""


class SparkContextPatch(ClassPatch):
    TARGET = ConnectSession
    ATTRIBUTE = "sparkContext"

    def _replacement(self, previous: object) -> object:
        return StubbedSparkContext()


class RddPatch(ClassPatch):
    """`df.rdd` answers `isEmpty()`; every other member still raises NOT_IMPLEMENTED."""

    TARGET = ConnectDataFrame
    ATTRIBUTE = "rdd"

    def _replacement(self, previous: object) -> object:
        return StubbedRdd()


class ClearCachePatch(ClassPatch):
    """Sail keeps no DataFrame cache (persist is a no-op there), so nothing to clear."""

    TARGET = ConnectCatalog
    ATTRIBUTE = "clearCache"

    def _replacement(self, previous: object) -> object:
        return IgnoredCall()


class SessionStop:
    """Stands in for `ConnectSession.stop`: holds the shared session, stops any other.

    The process-wide default session is the one libraries cache (cuallee does at
    import), so stopping it is held and recorded, and the engine closes it at stop.
    Any other session, including one a worker thread built and made its own active
    session, is stopped for real; left open past the engine, it would hang exit.
    """

    def __init__(self, original: Callable[[ConnectSession], None]) -> None:
        self.original = original
        self.held: list[ConnectSession] = []

    def __get__(
        self, session: ConnectSession | None, owner: type | None = None
    ) -> Callable[[], None] | SessionStop:
        if session is None:
            return self
        return MethodType(self, session)

    def __call__(self, session: ConnectSession) -> None:
        is_shared_session = session is ConnectSession._get_default_session()
        if not is_shared_session:
            self.original(session)
            return
        already_held = any(session is held for held in self.held)
        if not already_held:
            self.held.append(session)


class HeldStopPatch(ClassPatch):
    """User code cannot stop the session that sparkwarp shares; the engine closes it."""

    TARGET = ConnectSession
    ATTRIBUTE = "stop"

    def __init__(self) -> None:
        super().__init__()
        self._stop: SessionStop | None = None

    @property
    def held_sessions(self) -> list[ConnectSession]:
        """Sessions whose stop was held while installed; the engine closes them."""
        return list(self._stop.held) if self._stop is not None else []

    def _replacement(self, previous: object) -> object:
        self._stop = SessionStop(cast(Callable[[ConnectSession], None], previous))
        return self._stop

    def close(self, session: ConnectSession) -> None:
        """Really stop `session`, with the original method."""
        original_stop = self.original
        if not callable(original_stop):
            raise TypeError(CLOSE_WITHOUT_ORIGINAL_ERROR.format(original=original_stop))
        original_stop(session)


class ReleasePoolPatch(ClassPatch):
    """Holds PySpark 4.0/4.1's shared release-pool shutdown while sparkwarp runs.

    There, closing any Connect client shuts down one thread pool shared by every
    session, even when the client is already closed; a session garbage-collected while
    another one runs a query then deadlocks or breaks it. PySpark 4.2 fixed this with an
    idle-only shutdown. Here the shutdown is a no-op while the engine runs, and the
    engine runs the real one once at stop, when no session is left.
    """

    TARGET = ExecutePlanResponseReattachableIterator
    ATTRIBUTE = "shutdown"

    @property
    def applies(self) -> bool:
        return self.ATTRIBUTE in vars(self.TARGET)

    def _replacement(self, previous: object) -> object:
        return IgnoredCall()

    def shutdown_now(self) -> None:
        """Run the original shutdown, if this PySpark has one."""
        original = self.original
        if isinstance(original, classmethod):
            original.__func__(self.TARGET)


class CompatShims:
    """Installs and removes all shims as one unit, skipping those this PySpark lacks."""

    def __init__(self) -> None:
        self.held_stop = HeldStopPatch()
        self.release_pool = ReleasePoolPatch()
        self._patches: tuple[ClassPatch, ...] = (
            SparkContextPatch(),
            RddPatch(),
            ClearCachePatch(),
            self.held_stop,
            self.release_pool,
        )

    def close(self, session: ConnectSession) -> None:
        """Really stop `session`, whatever the held stop says."""
        self.held_stop.close(session)

    @property
    def held_sessions(self) -> list[ConnectSession]:
        return self.held_stop.held_sessions

    def shutdown_release_pool(self) -> None:
        """Run PySpark's shared release-pool shutdown once, if this PySpark has one."""
        self.release_pool.shutdown_now()

    def install(self) -> None:
        for patch in self._patches:
            if patch.applies:
                patch.install()

    def uninstall(self) -> None:
        for patch in reversed(self._patches):
            patch.uninstall()
