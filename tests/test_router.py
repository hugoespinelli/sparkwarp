"""Routing: deep or Sail-gap plans run on the shared JVM server; the rest on Sail."""

import sys
import tempfile
import time
from collections.abc import Iterator
from pathlib import Path

import pytest
from pyspark.errors import AnalysisException, PySparkException
from pyspark.sql import Column, SparkSession, Window
from pyspark.sql import functions as F

from sparkwarp.config import (
    FAILURE_FILE,
    JVM_DIR_ENV_VAR,
    JVM_ENV_VAR,
    LOOKUP_CAST_SIGNAL,
    TIMEZONE_KEY,
)
from sparkwarp.engines import SailEngine
from sparkwarp.jvm_server import (
    JavaRuntime,
    ProcessGroup,
    ServerLaunch,
    SharedJvmServer,
    StateDirectory,
)
from sparkwarp.router import DeepColumnError

pytestmark = pytest.mark.skipif(
    not JavaRuntime.available(), reason="routing to the JVM needs a Java runtime"
)

DEEP = 300
ROUTED_DEEP_SECONDS = 60
VIEW = "router_view"
OTHER_TIMEZONE = "America/Sao_Paulo"
OTHER_WORKERS_FAILURE = "the JVM Spark Connect server did not start (it exited)"
STALL_SECONDS = 15


class Chain:
    """A column `depth` calls deep, the shape of a transliteration table."""

    @staticmethod
    def of(depth: int) -> Column:
        column = F.col("s")
        for _ in range(depth):
            column = F.regexp_replace(column, "a", "b")
        return column


@pytest.fixture(scope="module")
def shared_server() -> Iterator[SharedJvmServer]:
    """One JVM server for the module, owned here as the main pytest process would."""
    directory = StateDirectory.claim()
    server = SharedJvmServer(directory)
    yield server
    server.stop()


@pytest.fixture
def engine(
    shared_server: SharedJvmServer, monkeypatch: pytest.MonkeyPatch
) -> Iterator[SailEngine]:
    monkeypatch.delenv(JVM_ENV_VAR, raising=False)
    monkeypatch.setenv(JVM_DIR_ENV_VAR, str(shared_server.directory))
    sail = SailEngine()
    sail.start_session()
    yield sail
    sail.stop()


@pytest.fixture
def spark(engine: SailEngine) -> SparkSession:
    return engine.start_session()


def test_a_deep_self_contained_plan_runs_on_the_jvm(
    engine: SailEngine, spark: SparkSession
) -> None:
    frame = spark.createDataFrame([("abc",)], "s string")
    started = time.perf_counter()
    assert frame.select(Chain.of(DEEP).alias("r")).first() == ("bbc",)
    assert time.perf_counter() - started < ROUTED_DEEP_SECONDS
    assert engine.router.deep_plans == 1


def test_a_deep_write_runs_on_the_jvm_and_sail_reads_the_files(
    engine: SailEngine, spark: SparkSession, tmp_path: Path
) -> None:
    target = str(tmp_path / "deep")
    frame = spark.createDataFrame([("abc",)], "s string")
    frame.select(Chain.of(DEEP).alias("r")).write.parquet(target)
    assert spark.read.parquet(target).first() == ("bbc",)
    assert engine.router.deep_plans == 1


def test_describing_a_deep_plan_is_routed_too(
    engine: SailEngine, spark: SparkSession
) -> None:
    frame = spark.createDataFrame([("abc",)], "s string")
    assert frame.select(Chain.of(DEEP).alias("r")).columns == ["r"]
    assert engine._jvm_session.opened is not None
    assert engine.router.deep_plans == 0


def test_a_deep_plan_over_a_temp_view_stays_on_sail(
    engine: SailEngine, spark: SparkSession
) -> None:
    spark.createDataFrame([("abc",)], "s string").createOrReplaceTempView(VIEW)
    deep_enough = 70
    assert spark.table(VIEW).select(Chain.of(deep_enough)).first() == ("bbc",)
    assert engine.router.deep_plans == 0
    assert engine._jvm_session.opened is None


def test_an_array_index_from_a_column_runs_on_the_jvm(
    engine: SailEngine, spark: SparkSession
) -> None:
    frame = spark.createDataFrame([([10, 20, 30], 1)], "parts array<int>, i int")
    assert frame.select(F.col("parts")[F.col("i")]).first() == (20,)
    assert engine.router.sail_gaps == 1


def test_a_window_sail_cannot_plan_is_retried_on_the_jvm(
    engine: SailEngine, spark: SparkSession
) -> None:
    frame = spark.createDataFrame(
        [(1, "a", "bob"), (1, "b", "alexander"), (1, "c", "al")],
        "k int, id string, name string",
    )
    order = F.when(F.length("name") > 3, 0).otherwise(1)
    window = Window.partitionBy("k").orderBy(order, "id")
    ranked = frame.select("id", F.row_number().over(window).alias("rn")).orderBy("id")
    assert [tuple(row) for row in ranked.collect()] == [("a", 2), ("b", 1), ("c", 3)]
    assert engine.router.sail_gaps == 1


def test_input_file_name_is_retried_on_the_jvm(
    engine: SailEngine, spark: SparkSession
) -> None:
    frame = spark.createDataFrame([("abc",)], "s string")
    assert frame.select(F.input_file_name()).first() == ("",)
    assert engine.router.sail_gaps == 1


def test_a_union_of_string_and_boolean_is_retried_on_the_jvm(
    engine: SailEngine, spark: SparkSession
) -> None:
    text = spark.createDataFrame([("true",)], "flag string")
    flags = spark.createDataFrame([(False,)], "flag boolean")
    merged = sorted(row.flag for row in text.unionByName(flags).collect())
    assert merged == [False, True]
    assert engine.router.sail_gaps == 1


def test_a_deep_call_to_a_function_registered_on_sail_stays_there(
    engine: SailEngine, spark: SparkSession
) -> None:
    spark.udf.register("Shout", lambda text: text.upper(), "string")
    frame = spark.createDataFrame([("abc",)], "s string")
    deep_enough = 70
    shouted = frame.select(F.call_udf("shout", Chain.of(deep_enough))).first()
    assert shouted == ("BBC",)
    assert engine.router.deep_plans == 0
    assert engine._jvm_session.opened is None


def exiting_command(launch: ServerLaunch, home: Path, port: int) -> list[str]:
    """Stands in for spark-submit with a command that exits at once."""
    return [sys.executable, "-c", "raise SystemExit(1)"]


def test_a_failed_start_names_its_reason_in_the_deep_column_error(
    engine: SailEngine,
    spark: SparkSession,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(ServerLaunch, "_command", exiting_command)
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    monkeypatch.setattr(
        engine._jvm_session,
        "_server",
        SharedJvmServer(StateDirectory(tmp_path, owned=False)),
    )
    frame = spark.createDataFrame([("abc",)], "s string")
    with pytest.raises(DeepColumnError, match="did not start"):
        frame.select(Chain.of(DEEP)).collect()


def test_another_workers_failed_start_makes_an_unmovable_deep_column_fail_fast(
    engine: SailEngine,
    spark: SparkSession,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    (tmp_path / FAILURE_FILE).write_text(OTHER_WORKERS_FAILURE)
    monkeypatch.setattr(
        engine._jvm_session,
        "_server",
        SharedJvmServer(StateDirectory(tmp_path, owned=False)),
    )
    spark.createDataFrame([("abc",)], "s string").createOrReplaceTempView(VIEW)
    deep_enough = 70
    with pytest.raises(DeepColumnError, match="did not start"):
        spark.table(VIEW).select(Chain.of(deep_enough)).collect()


def test_a_deep_python_udf_stays_on_sail(
    engine: SailEngine, spark: SparkSession
) -> None:
    shout = F.udf(lambda text: text.upper(), "string")
    frame = spark.createDataFrame([("abc",)], "s string")
    deep_enough = 70
    assert frame.select(shout(Chain.of(deep_enough))).first() == ("BBC",)
    assert engine.router.deep_plans == 0
    assert engine._jvm_session.opened is None


def test_a_map_looked_up_by_a_column_stays_on_sail(
    engine: SailEngine, spark: SparkSession
) -> None:
    frame = spark.createDataFrame(
        [({"a": 1, "b": 2}, "b")], "m map<string,int>, k string"
    )
    assert frame.select(F.col("m")[F.col("k")]).first() == (2,)
    assert engine._jvm_session.opened is None


def test_a_sail_gap_over_a_temp_view_raises_sails_error(
    engine: SailEngine, spark: SparkSession
) -> None:
    rows = spark.createDataFrame([([10, 20, 30], 1)], "parts array<int>, i int")
    rows.createOrReplaceTempView(VIEW)
    with pytest.raises(PySparkException, match=LOOKUP_CAST_SIGNAL):
        spark.table(VIEW).select(F.col("parts")[F.col("i")]).collect()
    assert engine.router.sail_gaps == 0
    assert engine._jvm_session.opened is None


def test_describing_a_sail_gap_is_retried_on_the_jvm_uncounted(
    engine: SailEngine, spark: SparkSession
) -> None:
    frame = spark.createDataFrame([("abc",)], "s string")
    assert frame.select(F.input_file_name().alias("source")).columns == ["source"]
    assert engine._jvm_session.opened is not None
    assert engine.router.sail_gaps == 0


def test_an_ordinary_error_stays_on_sail_without_a_jvm(
    engine: SailEngine, spark: SparkSession
) -> None:
    with pytest.raises(AnalysisException):
        spark.range(3).select("missing_column").collect()
    assert engine._jvm_session.opened is None


def test_conf_set_before_routing_reaches_the_jvm(spark: SparkSession) -> None:
    spark.conf.set(TIMEZONE_KEY, OTHER_TIMEZONE)
    frame = spark.createDataFrame([("abc",)], "s string")
    routed = frame.select(
        Chain.of(DEEP).alias("r"), F.current_timezone().alias("tz")
    ).first()
    assert routed is not None and routed.tz == OTHER_TIMEZONE


def test_conf_written_after_the_jvm_opened_reaches_it_and_unset_too(
    spark: SparkSession,
) -> None:
    frame = spark.createDataFrame([("abc",)], "s string")
    frame.select(Chain.of(DEEP)).collect()
    spark.conf.set(TIMEZONE_KEY, OTHER_TIMEZONE)
    routed = frame.select(Chain.of(DEEP), F.current_timezone().alias("tz")).first()
    assert routed is not None and routed.tz == OTHER_TIMEZONE
    spark.conf.unset(TIMEZONE_KEY)
    routed = frame.select(Chain.of(DEEP), F.current_timezone().alias("tz")).first()
    on_sail = spark.range(1).select(F.current_timezone().alias("tz")).first()
    assert routed is not None and on_sail is not None
    assert routed.tz == on_sail.tz != OTHER_TIMEZONE


def test_an_owning_engine_stops_its_jvm_server(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(JVM_DIR_ENV_VAR, raising=False)
    engine = SailEngine()
    spark = engine.start_session()
    frame = spark.createDataFrame([("abc",)], "s string")
    frame.select(Chain.of(DEEP)).collect()
    session = engine._jvm_session.opened
    server = engine._jvm_session.server
    assert session is not None and server is not None
    directory = server.directory
    record = server._read()
    engine.stop()
    assert session.is_stopped
    assert not directory.exists()
    assert record is not None and not record.alive()


def test_a_jvm_server_lost_mid_run_is_started_again(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(JVM_DIR_ENV_VAR, raising=False)
    engine = SailEngine()
    try:
        frame = engine.start_session().createDataFrame([("abc",)], "s string")
        frame.select(Chain.of(DEEP)).collect()
        server = engine._jvm_session.server
        assert server is not None
        lost = server._read()
        assert lost is not None
        ProcessGroup(lost.pid).stop()
        assert frame.select(Chain.of(DEEP).alias("r")).first() == ("bbc",)
        restarted = server._read()
        assert restarted is not None and restarted.pid != lost.pid
    finally:
        engine.stop()


def test_a_lost_jvm_server_stalls_neither_conf_writes_nor_stop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(JVM_DIR_ENV_VAR, raising=False)
    engine = SailEngine()
    spark = engine.start_session()
    spark.createDataFrame([("abc",)], "s string").select(Chain.of(DEEP)).collect()
    server = engine._jvm_session.server
    lost = server._read() if server is not None else None
    assert lost is not None
    ProcessGroup(lost.pid).stop()
    started = time.perf_counter()
    spark.conf.set(TIMEZONE_KEY, OTHER_TIMEZONE)
    engine.stop()
    assert time.perf_counter() - started < STALL_SECONDS
