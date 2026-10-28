"""Lineage cut: a loop that re-derives a DataFrame stops re-sending its history."""

from collections.abc import Iterator
from contextlib import suppress

import pyspark.sql.connect.proto as proto
import pytest
from google.protobuf.message import Message
from pyspark.errors import AnalysisException, PySparkException
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F

from sparkwarp import lineage
from sparkwarp.config import (
    CUT_ENV_VAR,
    JVM_DEPTH_ENV_VAR,
    JVM_ENV_VAR,
    TIMEZONE_KEY,
)
from sparkwarp.engines import SailEngine
from sparkwarp.lineage import ActionWrappers, Fingerprint, SailCheckpoints
from sparkwarp.router import RoutingSummary
from tests.shapes import ROUNDS, TITLES, Rounds

ROUNDS_OVER_BUDGET = 3
SMALL_CAP = 2
SHADOW = "SHADOW"
VIEW = "lineage_view"
OTHER_TIMEZONE = "America/Sao_Paulo"


@pytest.fixture
def engine(monkeypatch: pytest.MonkeyPatch) -> Iterator[SailEngine]:
    """Sail alone, with the cut switched on."""
    monkeypatch.setenv(CUT_ENV_VAR, "1")
    monkeypatch.setenv(JVM_ENV_VAR, "0")
    monkeypatch.setenv(JVM_DEPTH_ENV_VAR, "0")
    sail = SailEngine()
    sail.start_session()
    yield sail
    sail.stop()


@pytest.fixture
def spark(engine: SailEngine) -> SparkSession:
    return engine.start_session()


def test_a_grown_loop_is_cut_and_keeps_its_rows(
    engine: SailEngine, spark: SparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    cut = Rounds.run(spark, Rounds.dedupe)
    assert engine.router.lineage.plans_cut >= ROUNDS // 2
    monkeypatch.setenv(CUT_ENV_VAR, "0")
    assert Rounds.run(spark, Rounds.dedupe) == cut


def test_a_cut_plan_is_much_smaller_than_the_grown_one(
    engine: SailEngine, spark: SparkSession
) -> None:
    Rounds.run(spark, Rounds.dedupe)
    frame = spark.createDataFrame(TITLES, "id int, title string")
    for _ in range(ROUNDS):
        frame = Rounds.dedupe(frame)
    grown = len(frame._plan.to_proto(spark.client).SerializeToString())
    assert engine.router.lineage.last_sent_bytes * 3 < grown


def test_the_cut_is_off_by_default(
    engine: SailEngine, spark: SparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv(CUT_ENV_VAR)
    Rounds.run(spark, Rounds.dedupe)
    assert engine.router.lineage.plans_cut == 0
    assert engine.router.lineage.cached_roots == 0


def test_a_non_deterministic_loop_is_never_cut(
    engine: SailEngine, spark: SparkSession
) -> None:
    Rounds.run(spark, Rounds.noisy)
    assert engine.router.lineage.plans_cut == 0
    assert engine.router.lineage.cached_roots == 0


def test_a_column_reference_into_a_cut_dataframe_gives_the_same_rows(
    engine: SailEngine, spark: SparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    frame = spark.createDataFrame(TITLES, "id int, title string")
    for _ in range(ROUNDS):
        frame = Rounds.dedupe(frame)
        frame.count()
    referenced = frame.filter(frame["id"] > 3).select(frame["title"])
    cut = sorted(referenced.collect())
    assert engine.router.lineage.plans_cut > 0
    monkeypatch.setenv(CUT_ENV_VAR, "0")
    assert sorted(referenced.collect()) == cut


def test_engine_stop_releases_every_cached_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(CUT_ENV_VAR, "1")
    monkeypatch.setenv(JVM_ENV_VAR, "0")
    engine = SailEngine()
    Rounds.run(engine.start_session(), Rounds.dedupe)
    assert engine.router.lineage.cached_roots > 0
    engine.stop()
    assert engine.router.lineage.cached_roots == 0


class Wrapped:
    """The relation an action sends on top of the DataFrame the user holds."""

    @staticmethod
    def root(frame: DataFrame) -> proto.Relation:
        relation: proto.Relation = frame._plan.to_proto(frame.sparkSession.client).root
        return relation

    @staticmethod
    def tail(frame: DataFrame) -> proto.Relation:
        relation = proto.Relation()
        relation.tail.input.CopyFrom(Wrapped.root(frame))
        relation.tail.limit = 5
        return relation

    @staticmethod
    def show(frame: DataFrame) -> proto.Relation:
        relation = proto.Relation()
        relation.show_string.input.CopyFrom(Wrapped.root(frame))
        relation.show_string.num_rows = 20
        return relation


def test_action_wrappers_are_stripped_but_a_grouped_count_is_not(
    spark: SparkSession,
) -> None:
    frame = spark.createDataFrame(TITLES, "id int, title string").filter("id > 3")
    bare = Fingerprint.of(Wrapped.root(frame))
    wrapped = [
        Wrapped.root(frame.agg(F.count(F.lit(1)))),
        Wrapped.root(frame.limit(1)),
        Wrapped.tail(frame),
        Wrapped.show(frame),
    ]
    for relation in wrapped:
        assert Fingerprint.of(ActionWrappers.strip(relation)) == bare
    grouped = Wrapped.root(frame.groupBy("id").count())
    assert Fingerprint.of(ActionWrappers.strip(grouped)) != bare


def test_a_dataframe_keeps_its_fingerprint_and_new_content_changes_it(
    spark: SparkSession,
) -> None:
    frame = spark.createDataFrame(TITLES, "id int, title string").filter("id > 3")
    assert Fingerprint.of(Wrapped.root(frame)) == Fingerprint.of(Wrapped.root(frame))
    other = frame.filter("id > 4")
    assert Fingerprint.of(Wrapped.root(other)) != Fingerprint.of(Wrapped.root(frame))


def test_the_summary_counts_cut_plans_apart_from_the_jvm() -> None:
    lines = RoutingSummary(None, {"deep_plans": 0, "sail_gaps": 0, "plans_cut": 3})
    assert lines.lines() == ["sparkwarp: 3 growing plans cut (checkpointed on Sail)"]
    both = RoutingSummary(None, {"deep_plans": 1, "sail_gaps": 0, "plans_cut": 1})
    assert len(both.lines()) == 2


class BigFrame:
    """One DataFrame over the cut budget, built from inline rows or from a view."""

    @staticmethod
    def inline(spark: SparkSession) -> DataFrame:
        frame = spark.createDataFrame(TITLES, "id int, title string")
        for _ in range(ROUNDS_OVER_BUDGET):
            frame = Rounds.dedupe(frame)
        return frame

    @staticmethod
    def over_view(spark: SparkSession, rows: list[tuple[int, str]]) -> DataFrame:
        rows_frame = spark.createDataFrame(rows, "id int, title string")
        rows_frame.createOrReplaceTempView(VIEW)
        frame = spark.table(VIEW)
        for _ in range(ROUNDS_OVER_BUDGET):
            frame = Rounds.dedupe(frame)
        return frame


class Reads:
    """What a test reads from a sorted frame: every id, the first row, the last two."""

    @staticmethod
    def ordered(frame: DataFrame) -> tuple[list[int], Row | None, list[Row]]:
        return [row.id for row in frame.collect()], frame.first(), frame.tail(2)


class FailingCheckpoints:
    """Stands in for the checkpoint call: counts attempts and stores nothing."""

    def __init__(self) -> None:
        self.attempts = 0

    def checkpoint(self, client: object, relation: object) -> None:
        self.attempts += 1


def test_the_first_action_runs_on_its_stored_dataframe(
    engine: SailEngine, spark: SparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    frame = BigFrame.inline(spark)
    stored = sorted(frame.collect())
    assert engine.router.lineage.cached_roots == 1
    assert engine.router.lineage.plans_cut == 0
    assert sorted(frame.collect()) == stored
    assert engine.router.lineage.plans_cut == 1
    monkeypatch.setenv(CUT_ENV_VAR, "0")
    assert sorted(frame.collect()) == stored


def test_a_dataframe_over_a_replaced_view_is_never_stored(
    engine: SailEngine, spark: SparkSession
) -> None:
    first = BigFrame.over_view(spark, [(1, "a")])
    assert [row.id for row in first.collect()] == [1]
    BigFrame.over_view(spark, [(2, "b"), (3, "c")])
    assert sorted(row.id for row in first.collect()) == [2, 3]
    assert engine.router.lineage.cached_roots == 0


def test_sql_text_with_uuid_is_never_stored(
    engine: SailEngine, spark: SparkSession
) -> None:
    frame = BigFrame.inline(spark).selectExpr("id", "uuid() AS u")
    frame.collect()
    assert engine.router.lineage.cached_roots == 0


def test_storing_stops_at_the_session_cap_and_rows_stay_the_same(
    engine: SailEngine, spark: SparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(lineage, "CUT_MAX_STORED", SMALL_CAP)
    cut = Rounds.run(spark, Rounds.dedupe)
    assert engine.router.lineage.cached_roots == SMALL_CAP
    monkeypatch.setenv(CUT_ENV_VAR, "0")
    assert Rounds.run(spark, Rounds.dedupe) == cut


def test_a_sorted_dataframe_is_never_stored(
    engine: SailEngine, spark: SparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    ordered = BigFrame.inline(spark).orderBy(F.desc("id"))
    seen = Reads.ordered(ordered)
    assert engine.router.lineage.cached_roots == 0
    monkeypatch.setenv(CUT_ENV_VAR, "0")
    assert Reads.ordered(ordered) == seen


def test_an_aliased_dataframe_still_joins_by_its_alias(
    engine: SailEngine, spark: SparkSession
) -> None:
    aliased = BigFrame.inline(spark).alias("a")
    aliased.collect()
    assert engine.router.lineage.cached_roots == 0
    other = spark.createDataFrame(TITLES, "id int, title string").alias("b")
    joined = aliased.join(other, F.col("a.id") == F.col("b.id"))
    assert joined.select("a.title", "b.title").count() == aliased.count()


def test_a_range_source_is_never_stored(
    engine: SailEngine, spark: SparkSession
) -> None:
    frame = spark.range(40).withColumn("title", F.lit("Title a"))
    for _ in range(ROUNDS_OVER_BUDGET):
        frame = Rounds.dedupe(frame)
    frame.collect()
    assert engine.router.lineage.cached_roots == 0


def test_first_and_show_read_without_storing(
    engine: SailEngine, spark: SparkSession
) -> None:
    frame = BigFrame.inline(spark)
    frame.first()
    frame.limit(3).collect()
    assert engine.router.lineage.cached_roots == 0
    frame.collect()
    assert engine.router.lineage.cached_roots == 1


def test_a_failed_checkpoint_keeps_the_action_and_is_not_retried(
    engine: SailEngine, spark: SparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    failing = FailingCheckpoints()
    monkeypatch.setattr(SailCheckpoints, "checkpoint", failing.checkpoint)
    frame = BigFrame.inline(spark)
    first = sorted(frame.collect())
    assert sorted(frame.collect()) == first
    assert failing.attempts == 1
    assert engine.router.lineage.cached_roots == 0


def test_a_conf_write_drops_the_stored_dataframes(
    engine: SailEngine, spark: SparkSession
) -> None:
    Rounds.run(spark, Rounds.dedupe)
    assert engine.router.lineage.cached_roots > 0
    spark.conf.set(TIMEZONE_KEY, OTHER_TIMEZONE)
    assert engine.router.lineage.cached_roots == 0


def test_a_sql_set_drops_the_stored_dataframes(
    engine: SailEngine, spark: SparkSession
) -> None:
    Rounds.run(spark, Rounds.dedupe)
    assert engine.router.lineage.cached_roots > 0
    with suppress(PySparkException):
        spark.sql(f"SET {TIMEZONE_KEY}={OTHER_TIMEZONE}")
    assert engine.router.lineage.cached_roots == 0


class FingerprintCalls:
    """Counts fingerprints taken, to show a miss does not hash every subtree."""

    def __init__(self) -> None:
        self.calls = 0
        self._real = Fingerprint.of

    def of(self, relation: Message) -> bytes:
        self.calls += 1
        return self._real(relation)


def test_a_python_udf_is_never_stored(engine: SailEngine, spark: SparkSession) -> None:
    shout = F.udf(lambda text: text.upper(), "string")
    BigFrame.inline(spark).withColumn("loud", shout(F.col("title"))).collect()
    assert engine.router.lineage.cached_roots == 0


def test_a_function_registered_by_name_is_never_stored(
    engine: SailEngine, spark: SparkSession
) -> None:
    spark.udf.register("shout", lambda text: text.upper(), "string")
    frame = BigFrame.inline(spark).withColumn("loud", F.call_udf("shout", "title"))
    frame.collect()
    assert engine.router.lineage.cached_roots == 0


def test_a_time_function_is_never_stored(
    engine: SailEngine, spark: SparkSession
) -> None:
    BigFrame.inline(spark).withColumn("now", F.unix_timestamp()).collect()
    assert engine.router.lineage.cached_roots == 0


def test_a_sql_reset_drops_the_stored_dataframes(
    engine: SailEngine, spark: SparkSession
) -> None:
    Rounds.run(spark, Rounds.dedupe)
    assert engine.router.lineage.cached_roots > 0
    with suppress(PySparkException):
        spark.sql("RESET")
    assert engine.router.lineage.cached_roots == 0


def test_the_cap_counts_checkpoints_a_conf_write_cannot_free(
    engine: SailEngine, spark: SparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(lineage, "CUT_MAX_STORED", SMALL_CAP)
    Rounds.run(spark, Rounds.dedupe)
    spark.conf.set(TIMEZONE_KEY, OTHER_TIMEZONE)
    Rounds.run(spark, Rounds.dedupe)
    assert engine.router.lineage.cached_roots == 0


def test_a_join_on_an_ancestor_fails_the_same_way(
    engine: SailEngine, spark: SparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    ancestor = spark.createDataFrame(TITLES, "id int, title string")
    child = ancestor
    for _ in range(ROUNDS_OVER_BUDGET):
        child = Rounds.dedupe(child)
    child.collect()
    joined = child.join(ancestor, child["id"] == ancestor["id"])
    picked = joined.select(ancestor["title"], child["title"])
    with pytest.raises(AnalysisException) as cut_on:
        picked.collect()
    monkeypatch.setenv(CUT_ENV_VAR, "0")
    with pytest.raises(AnalysisException) as cut_off:
        picked.collect()
    assert type(cut_on.value) is type(cut_off.value)


def test_a_miss_fingerprints_only_the_candidate(
    engine: SailEngine, spark: SparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    BigFrame.inline(spark).collect()
    counter = FingerprintCalls()
    monkeypatch.setattr(Fingerprint, "of", counter.of)
    other = spark.createDataFrame(TITLES, "id int, title string").filter("id > 1")
    for _ in range(ROUNDS_OVER_BUDGET):
        other = Rounds.dedupe(other)
    other.limit(1).collect()
    assert counter.calls == 1


def test_a_function_registered_later_under_a_built_in_name_is_used(
    engine: SailEngine, spark: SparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    shouted = BigFrame.inline(spark).withColumn("u", F.upper("title"))
    shouted.collect()
    assert engine.router.lineage.cached_roots == 1
    spark.udf.register("upper", lambda text: SHADOW, "string")
    later = shouted.withColumn("k", F.lit(1))
    cut = sorted(row.u for row in later.collect())
    assert engine.router.lineage.cached_roots == 0
    monkeypatch.setenv(CUT_ENV_VAR, "0")
    assert sorted(row.u for row in later.collect()) == cut
