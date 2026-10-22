"""PlanShape: how deep a plan's expressions go, and whether it can leave Sail."""

from collections.abc import Callable, Iterator
from pathlib import Path

import pyspark.sql.connect.proto as proto
import pytest
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from sparkwarp.engines import SailEngine
from sparkwarp.plan_shape import PlanShape

DEEP = 300
VIEW = "plan_shape_view"
COMPRESSED = "compressed_operation"
SAVE_AS_TABLE = "save_as_table"
TABLE = "plan_shape_table"


@pytest.fixture
def spark() -> Iterator[SparkSession]:
    engine = SailEngine()
    yield engine.start_session()
    engine.stop()


class Plans:
    """Builds the request plan PySpark would send for a DataFrame or a command."""

    @staticmethod
    def of(frame: DataFrame) -> proto.Plan:
        plan: proto.Plan = frame._plan.to_proto(frame.sparkSession.client)
        return plan

    @staticmethod
    def chain(depth: int) -> Column:
        column = F.col("s")
        for _ in range(depth):
            column = F.regexp_replace(column, "a", "b")
        return column

    @staticmethod
    def write_to(frame: DataFrame, path: str) -> proto.Plan:
        write = frame.write.format("parquet")._write
        write.mode = "overwrite"
        write.path = path
        plan = proto.Plan()
        plan.command.CopyFrom(write.command(frame.sparkSession.client))
        return plan

    @staticmethod
    def save_as_table(frame: DataFrame, table: str) -> proto.Plan:
        write = frame.write._write
        write.table_name = table
        write.table_save_method = SAVE_AS_TABLE
        plan = proto.Plan()
        plan.command.CopyFrom(write.command(frame.sparkSession.client))
        return plan


class Passthrough:
    """Inline Python code for mapInArrow: hands the batches back unchanged."""

    @staticmethod
    def batches(batches: Iterator[object]) -> Iterator[object]:
        yield from batches


def test_depth_counts_nested_function_calls(spark: SparkSession) -> None:
    frame = spark.createDataFrame([("abc",)], "s string")
    assert PlanShape(Plans.of(frame.select(Plans.chain(DEEP)))).expression_depth > DEEP


def test_a_shallow_plan_is_shallow(spark: SparkSession) -> None:
    frame = spark.range(3).select((F.col("id") + 1).alias("x"))
    assert PlanShape(Plans.of(frame)).expression_depth < 5


BuildWithPath = Callable[[SparkSession, str], DataFrame]
Build = Callable[[SparkSession], DataFrame]


@pytest.mark.parametrize(
    "build",
    [
        lambda s, _: s.createDataFrame([("abc",)], "s string"),
        lambda s, _: s.range(5).filter(F.col("id") > 1),
        lambda s, path: s.read.parquet(path),
    ],
    ids=["create_data_frame", "range", "read_a_path"],
)
def test_inline_data_and_paths_are_self_contained(
    spark: SparkSession, tmp_path: Path, build: BuildWithPath
) -> None:
    path = str(tmp_path / "data")
    spark.range(2).write.parquet(path)
    frame = build(spark, path)
    assert PlanShape(Plans.of(frame)).self_contained


@pytest.mark.parametrize(
    "build",
    [
        lambda s: s.table(VIEW),
        lambda s: s.sql(f"SELECT * FROM {VIEW}"),
        lambda s: s.range(3).localCheckpoint(),
        lambda s: s.range(3).join(s.table(VIEW), "id"),
        lambda s: s.range(3).filter("id > 1"),
        lambda s: s.range(3).selectExpr("id + 1"),
        lambda s: s.range(3).select(F.expr("upper(cast(id as string))")),
        lambda s: s.range(3).select(F.call_function("upper", F.col("id"))),
        lambda s: s.read.format("delta").load("/tmp/table"),
        lambda s: s.read.parquet("relative/path"),
    ],
    ids=[
        "temp_view",
        "sql_text",
        "checkpoint",
        "join_with_a_view",
        "filter_text",
        "select_expr",
        "expr",
        "call_function",
        "delta_format",
        "relative_path",
    ],
)
def test_server_state_keeps_a_plan_on_sail(spark: SparkSession, build: Build) -> None:
    spark.range(3).createOrReplaceTempView(VIEW)
    frame = build(spark)
    assert not PlanShape(Plans.of(frame)).self_contained


def test_a_write_to_a_path_is_self_contained(
    spark: SparkSession, tmp_path: Path
) -> None:
    plan = Plans.write_to(spark.range(3), str(tmp_path / "out"))
    assert PlanShape(plan).self_contained


def test_a_function_registered_on_sail_keeps_a_plan_there(
    spark: SparkSession,
) -> None:
    called = spark.range(3).select(F.upper(F.col("id").cast("string")))
    assert PlanShape(Plans.of(called)).self_contained
    assert not PlanShape(
        Plans.of(called), registered=frozenset({"upper"})
    ).self_contained


def test_a_write_to_a_relative_path_stays_on_sail(spark: SparkSession) -> None:
    assert not PlanShape(Plans.write_to(spark.range(3), "relative/out")).self_contained


def test_a_deep_write_to_a_table_is_measured_and_stays_on_sail(
    spark: SparkSession,
) -> None:
    frame = spark.createDataFrame([("abc",)], "s string").select(Plans.chain(DEEP))
    shape = PlanShape(Plans.save_as_table(frame, TABLE))
    assert shape.expression_depth > DEEP
    assert not shape.self_contained


@pytest.mark.parametrize(
    "build",
    [
        lambda s: s.range(3).select(F.udf(lambda n: n + 1, "long")(F.col("id"))),
        lambda s: s.range(3).mapInArrow(Passthrough.batches, "id long"),
    ],
    ids=["python_udf", "map_in_arrow"],
)
def test_inline_python_code_keeps_a_plan_on_sail(
    spark: SparkSession, build: Build
) -> None:
    assert not PlanShape(Plans.of(build(spark))).self_contained


def test_a_lookup_keyed_by_a_column_is_noted(spark: SparkSession) -> None:
    frame = spark.createDataFrame([([1, 2], 1)], "parts array<int>, i int")
    by_column = PlanShape(Plans.of(frame.select(F.col("parts")[F.col("i")])))
    by_literal = PlanShape(Plans.of(frame.select(F.col("parts")[0])))
    assert by_column.looks_up_by_column
    assert not by_literal.looks_up_by_column


def test_other_commands_stay_on_sail() -> None:
    plan = proto.Plan()
    plan.command.create_dataframe_view.name = VIEW
    assert not PlanShape(plan).self_contained


@pytest.mark.skipif(
    COMPRESSED not in proto.Plan.DESCRIPTOR.fields_by_name,
    reason="PySpark sends compressed plans from 4.2",
)
def test_a_compressed_plan_is_not_inspected() -> None:
    plan = proto.Plan()
    plan.compressed_operation.data = b"zstd"
    assert not PlanShape(plan).self_contained
