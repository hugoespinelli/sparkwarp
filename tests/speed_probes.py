"""Measuring tools for the speed guard: round trips on the wire and paired timings."""

from __future__ import annotations

import statistics
import time
from collections import Counter
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from types import TracebackType

import pytest
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.connect.client import SparkConnectClient

from sparkwarp.config import CONNECT_MODE_ENV_VAR, CONNECT_MODE_ON
from sparkwarp.engines import ServerSettings
from tests.shapes import Slug

EXECUTE_PLAN = "ExecutePlan"
REQUEST_RPCS = (EXECUTE_PLAN, "AnalyzePlan", "Config")
NO_CHECKPOINTS = None
SMALL_ROWS = [(i, f"name {i}") for i in range(20)]
SMALL_SCHEMA = "id int, name string"
SMALL_CUTOFF = 3
DEEP_ROWS = [("abc",)]
DEEP_SCHEMA = "s string"
GROUPS = 3
Action = Callable[[], object]
Rpc = Callable[..., object]


class CountedRpc:
    """Stands in for one stub method and counts its calls."""

    def __init__(self, name: str, original: Rpc, counts: Counter[str]) -> None:
        self._name = name
        self._original = original
        self._counts = counts

    def __call__(self, *args: object, **kwargs: object) -> object:
        self._counts[self._name] += 1
        return self._original(*args, **kwargs)


class RpcCounter:
    """Counts the request RPCs one Connect client sends while the block runs.

    PySpark sends ReleaseExecute from a background thread after a stream ends, so it is
    never counted: its number depends on thread timing.
    """

    def __init__(self, client: SparkConnectClient) -> None:
        self._stub = client._stub
        self._originals: dict[str, Rpc] = {}
        self.counts: Counter[str] = Counter()

    def __enter__(self) -> RpcCounter:
        for name in REQUEST_RPCS:
            original: Rpc = getattr(self._stub, name)
            self._originals[name] = original
            setattr(self._stub, name, CountedRpc(name, original, self.counts))
        return self

    def __exit__(
        self,
        _kind: type[BaseException] | None,
        _error: BaseException | None,
        _trace: TracebackType | None,
    ) -> None:
        for name, original in self._originals.items():
            setattr(self._stub, name, original)


class Stopwatch:
    """Median seconds of one action, over a number of calls."""

    @staticmethod
    def median(action: Action, calls: int) -> float:
        return statistics.median(Stopwatch.each(action, calls))

    @staticmethod
    def each(action: Action, calls: int) -> list[float]:
        seconds: list[float] = []
        for _ in range(calls):
            started = time.perf_counter()
            action()
            seconds.append(time.perf_counter() - started)
        return seconds

    @staticmethod
    def once(action: Action) -> float:
        return Stopwatch.each(action, 1)[0]

    @staticmethod
    def until_raised(action: Action, error: type[BaseException]) -> float:
        """Seconds until `action` raises `error`; any other outcome is a failure."""
        started = time.perf_counter()
        try:
            action()
        except error:
            return time.perf_counter() - started
        raise AssertionError(f"{error.__name__} was not raised")


class RawSail:
    """A plain Connect session on a Sail server with sparkwarp's settings.

    Sail reads its stack size once per process, so a bare server would shrink it for
    later engines. `create()` turns on Connect mode for the process; it is put back.
    """

    @staticmethod
    @contextmanager
    def session() -> Iterator[SparkSession]:
        server = ServerSettings(NO_CHECKPOINTS).launch()
        host, port = server.listening_address
        with pytest.MonkeyPatch.context() as env:
            env.setenv(CONNECT_MODE_ENV_VAR, CONNECT_MODE_ON)
            try:
                spark = SparkSession.builder.remote(f"sc://{host}:{port}").create()
                try:
                    yield spark
                finally:
                    spark.stop()
            finally:
                server.stop()


class ShallowQuery:
    """A small filter and aggregate, the shape of most unit tests."""

    @staticmethod
    def on(spark: SparkSession) -> DataFrame:
        frame = spark.createDataFrame(SMALL_ROWS, SMALL_SCHEMA)
        kept = frame.filter(F.col("id") > SMALL_CUTOFF)
        return kept.groupBy((F.col("id") % GROUPS).alias("k")).agg(
            F.count("*").alias("c")
        )


class DeepChain:
    """A column `depth` calls deep, the shape of a transliteration table."""

    @staticmethod
    def of(depth: int) -> Column:
        return Slug.of(F.col("s"), depth)

    @staticmethod
    def frame(spark: SparkSession, depth: int) -> DataFrame:
        rows = spark.createDataFrame(DEEP_ROWS, DEEP_SCHEMA)
        return rows.select(DeepChain.of(depth).alias("r"))
