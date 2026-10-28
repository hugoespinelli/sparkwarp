"""Speed guard: the fast paths keep their speed.

Counters run with the normal suite. Checks marked `timing` compare times measured in
this process and run alone in CI (`pytest -m timing`), away from other tests' CPU load.
"""

import statistics
from collections.abc import Iterator
from functools import partial

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from sparkwarp.config import CUT_ENV_VAR, JVM_DEPTH_ENV_VAR, JVM_ENV_VAR
from sparkwarp.engines import SailEngine
from sparkwarp.jvm_server import JavaRuntime
from sparkwarp.router import DeepColumnError
from tests.shapes import Rounds
from tests.speed_probes import (
    EXECUTE_PLAN,
    DeepChain,
    RawSail,
    RpcCounter,
    ShallowQuery,
    Stopwatch,
)

TOO_DEEP = 300
SHALLOW_ROUNDS = 2
WARM_UP_CALLS = 10
TIMED_CALLS = 40
BLOCKS = 2
LOOP_ROUNDS = 10
MAX_ACTION_OVERHEAD = 1.5
MAX_FALLBACK_COST = 3.0
MAX_CUT_SHARE = 0.6
MAX_FAIL_FAST_SECONDS = 2.0
MAX_SESSION_START_SECONDS = 3.0
NATIVE_PATTERN = r"(\d+)"
FALLBACK_PATTERN = r"(\d+)(?=usd)"
PRICES = [(f"{i}usd",) for i in range(20)]
PRICES_SCHEMA = "s string"
SWITCH_OFF = "0"
SWITCH_ON = "1"


class Engines:
    """A Sail engine with routing to the JVM off unless asked for."""

    @staticmethod
    def sail_only(monkeypatch: pytest.MonkeyPatch) -> SailEngine:
        monkeypatch.setenv(JVM_ENV_VAR, SWITCH_OFF)
        monkeypatch.delenv(JVM_DEPTH_ENV_VAR, raising=False)
        monkeypatch.delenv(CUT_ENV_VAR, raising=False)
        return SailEngine()


@pytest.fixture
def engine(monkeypatch: pytest.MonkeyPatch) -> Iterator[SailEngine]:
    sail = Engines.sail_only(monkeypatch)
    yield sail
    sail.stop()


@pytest.fixture
def spark(engine: SailEngine) -> SparkSession:
    return engine.start_session()


class PairedSamples:
    """Seconds per action of one query, plain Sail against sparkwarp, in turns."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self._monkeypatch = monkeypatch
        self.plain: list[float] = []
        self.warped: list[float] = []

    def take_turns(self) -> None:
        for _ in range(BLOCKS):
            with RawSail.session() as raw:
                self.plain += self._block(raw)
            sail = Engines.sail_only(self._monkeypatch)
            try:
                self.warped += self._block(sail.start_session())
            finally:
                sail.stop()

    def ratio(self) -> float:
        return statistics.median(self.warped) / statistics.median(self.plain)

    @staticmethod
    def _block(spark: SparkSession) -> list[float]:
        query = ShallowQuery.on(spark)
        Stopwatch.each(query.collect, WARM_UP_CALLS)
        return Stopwatch.each(query.collect, TIMED_CALLS)


def test_a_small_action_sends_what_plain_pyspark_sends(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with RawSail.session() as raw:
        query = ShallowQuery.on(raw)
        with RpcCounter(raw.client) as plain:
            query.collect()
    sail = Engines.sail_only(monkeypatch)
    try:
        spark = sail.start_session()
        query = ShallowQuery.on(spark)
        with RpcCounter(spark.client) as warped:
            query.collect()
    finally:
        sail.stop()
    assert warped.counts == plain.counts


def test_a_too_deep_column_without_java_never_reaches_sail(
    spark: SparkSession,
) -> None:
    deep = DeepChain.frame(spark, TOO_DEEP)
    with RpcCounter(spark.client) as sent, pytest.raises(DeepColumnError):
        deep.collect()
    assert sent.counts[EXECUTE_PLAN] == 0


@pytest.mark.skipif(not JavaRuntime.available(), reason="needs a Java runtime")
def test_a_shallow_run_never_opens_the_jvm(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(JVM_ENV_VAR, raising=False)
    monkeypatch.delenv(JVM_DEPTH_ENV_VAR, raising=False)
    monkeypatch.delenv(CUT_ENV_VAR, raising=False)
    sail = SailEngine()
    try:
        spark = sail.start_session()
        ShallowQuery.on(spark).collect()
        Rounds.run(spark, Rounds.dedupe, SHALLOW_ROUNDS)
        server = sail.router.jvm.server
        jvm_answers = server is not None and server.answering()
        assert sail.router.jvm.opened is None
        assert not jvm_answers
    finally:
        sail.stop()


@pytest.mark.timing
def test_an_action_costs_at_most_half_again_plain_sail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Healthy: 0.76-0.93x (about 7 ms per action); plain PySpark keeps reattach."""
    samples = PairedSamples(monkeypatch)
    samples.take_turns()
    assert samples.ratio() <= MAX_ACTION_OVERHEAD


@pytest.mark.timing
def test_the_regex_fallback_stays_near_the_native_pattern(spark: SparkSession) -> None:
    """Healthy: 1.12-1.35x; the fallback runs the pattern in Python."""
    prices = spark.createDataFrame(PRICES, PRICES_SCHEMA)
    native = prices.select(F.regexp_extract("s", NATIVE_PATTERN, 1))
    fallback = prices.select(F.regexp_extract("s", FALLBACK_PATTERN, 1))
    Stopwatch.each(native.collect, WARM_UP_CALLS)
    Stopwatch.each(fallback.collect, WARM_UP_CALLS)
    native_median = Stopwatch.median(native.collect, TIMED_CALLS)
    fallback_median = Stopwatch.median(fallback.collect, TIMED_CALLS)
    assert fallback_median <= native_median * MAX_FALLBACK_COST


@pytest.mark.timing
def test_the_lineage_cut_shortens_a_growing_loop(
    engine: SailEngine, spark: SparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Healthy: 0.32-0.38x on 10 rounds (about 6 s grown, 2 s cut).

    The cut loop runs first, cold, so warm-up never favours it.
    """
    loop = partial(Rounds.run, spark, Rounds.dedupe, LOOP_ROUNDS)
    monkeypatch.setenv(CUT_ENV_VAR, SWITCH_ON)
    cut = Stopwatch.once(loop)
    monkeypatch.delenv(CUT_ENV_VAR)
    grown = Stopwatch.once(loop)
    assert engine.router.lineage.plans_cut > 0
    assert cut <= grown * MAX_CUT_SHARE


@pytest.mark.timing
def test_a_too_deep_column_without_java_fails_fast(spark: SparkSession) -> None:
    """Healthy: 35-51 ms. On Sail it would take about 50 s."""
    deep = DeepChain.frame(spark, TOO_DEEP)
    seconds = Stopwatch.until_raised(deep.collect, DeepColumnError)
    assert seconds <= MAX_FAIL_FAST_SECONDS


@pytest.mark.timing
def test_a_session_starts_quickly(monkeypatch: pytest.MonkeyPatch) -> None:
    """Healthy: 10-150 ms."""
    sail = Engines.sail_only(monkeypatch)
    try:
        seconds = Stopwatch.once(sail.start_session)
    finally:
        sail.stop()
    assert seconds <= MAX_SESSION_START_SECONDS
