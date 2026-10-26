"""Lineage cut: a loop that re-derives a DataFrame stops re-sending its history."""

from collections.abc import Callable, Iterator

import pyspark.sql.connect.proto as proto
import pytest
from pyspark.sql import Column, DataFrame, Row, SparkSession, Window
from pyspark.sql import functions as F

from sparkwarp.config import CUT_ENV_VAR, JVM_DEPTH_ENV_VAR, JVM_ENV_VAR
from sparkwarp.engines import SailEngine
from sparkwarp.lineage import ActionWrappers, Fingerprint
from sparkwarp.router import RoutingSummary

ROUNDS = 8
SLUG_DEPTH = 50
TITLES = [(i, f"Title {i % 7} a") for i in range(40)]
Step = Callable[[DataFrame], DataFrame]


class Slug:
    """A column about as deep as the production slug normalization."""

    @staticmethod
    def of(column: Column) -> Column:
        for _ in range(SLUG_DEPTH):
            column = F.regexp_replace(column, "a", "b")
        return column


class Rounds:
    """The iterative shape: each round re-derives the DataFrame and runs one action."""

    @staticmethod
    def dedupe(frame: DataFrame) -> DataFrame:
        keyed = frame.withColumn("slug", Slug.of(F.col("title")))
        window = Window.partitionBy("slug").orderBy("id")
        ranked = keyed.withColumn("rn", F.row_number().over(window))
        kept = ranked.filter(F.col("rn") == 1).drop("rn", "slug")
        return kept.withColumn("title", F.concat(F.col("title"), F.lit("x")))

    @staticmethod
    def noisy(frame: DataFrame) -> DataFrame:
        return Rounds.dedupe(frame).withColumn("noise", F.rand())

    @staticmethod
    def run(spark: SparkSession, step: Step, rounds: int = ROUNDS) -> list[list[Row]]:
        frame = spark.createDataFrame(TITLES, "id int, title string")
        seen: list[list[Row]] = []
        for _ in range(rounds):
            frame = step(frame)
            seen.append(sorted(frame.select("id", "title").collect()))
        return seen


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


def test_a_column_reference_into_a_cut_subtree_still_resolves(
    engine: SailEngine, spark: SparkSession
) -> None:
    frame = spark.createDataFrame(TITLES, "id int, title string")
    for _ in range(ROUNDS):
        frame = Rounds.dedupe(frame)
        frame.count()
    left, right = frame, frame.select("id")
    joined = left.join(right, left["id"] == right["id"]).count()
    assert joined == frame.count()


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

    @staticmethod
    def filtered(spark: SparkSession, threshold: int) -> bytes:
        frame = spark.createDataFrame(TITLES, "id int, title string")
        return Fingerprint.of(Wrapped.root(frame.filter(F.col("id") > threshold)))


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


def test_fingerprints_ignore_plan_ids_but_not_content(spark: SparkSession) -> None:
    assert Wrapped.filtered(spark, 3) == Wrapped.filtered(spark, 3)
    assert Wrapped.filtered(spark, 3) != Wrapped.filtered(spark, 4)


def test_the_summary_counts_cut_plans_apart_from_the_jvm() -> None:
    lines = RoutingSummary(None, {"deep_plans": 0, "sail_gaps": 0, "plans_cut": 3})
    assert lines.lines() == ["sparkwarp: 3 growing plans cut (checkpointed on Sail)"]
    both = RoutingSummary(None, {"deep_plans": 1, "sail_gaps": 0, "plans_cut": 1})
    assert len(both.lines()) == 2
