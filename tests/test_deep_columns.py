"""Deeply nested columns: the client's protobuf depth limit and Sail's thread stacks."""

from collections.abc import Iterator

import pytest
from google.protobuf.internal import api_implementation
from google.protobuf.message import DecodeError
from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.connect.proto import expressions_pb2

from sparkwarp.config import (
    JVM_DEPTH_ENV_VAR,
    JVM_ENV_VAR,
    SAIL_STACK_SIZE_BYTES,
    STACK_SIZE_ENV_VAR,
    UPB_BACKEND,
)
from sparkwarp.engines import SailEngine, ServerSettings
from sparkwarp.router import DeepColumnError

DEEP = 100
BUILDABLE = 450
PAST_PYTHON_RECURSION = 3000
USER_STACK_SIZE = str(16 * 1024 * 1024)
PAST_THE_LIMIT = 70
TABLE = "deep_columns_table"


@pytest.fixture
def spark(monkeypatch: pytest.MonkeyPatch) -> Iterator[SparkSession]:
    """Sail itself, with routing to the JVM off: these tests measure Sail."""
    monkeypatch.setenv(JVM_ENV_VAR, "0")
    monkeypatch.setenv(JVM_DEPTH_ENV_VAR, "0")
    engine = SailEngine()
    yield engine.start_session()
    engine.stop()


@pytest.fixture
def without_a_jvm(monkeypatch: pytest.MonkeyPatch) -> Iterator[SparkSession]:
    """No JVM, with the default depth limit: too-deep columns fail fast."""
    monkeypatch.setenv(JVM_ENV_VAR, "0")
    monkeypatch.delenv(JVM_DEPTH_ENV_VAR, raising=False)
    engine = SailEngine()
    yield engine.start_session()
    engine.stop()


class NestedPlan:
    """Builds a nested Spark Connect expression the way PySpark does, by `extend`."""

    @staticmethod
    def column(depth: int) -> Column:
        column = F.col("s")
        for _ in range(depth):
            column = F.regexp_replace(column, "a", "b")
        return column

    @staticmethod
    def message(depth: int) -> expressions_pb2.Expression:
        current = expressions_pb2.Expression()
        current.unresolved_attribute.unparsed_identifier = "s"
        for _ in range(depth):
            parent = expressions_pb2.Expression()
            parent.unresolved_function.function_name = "regexp_replace"
            parent.unresolved_function.arguments.extend([current])
            current = parent
        return current


needs_upb = pytest.mark.skipif(
    api_implementation.Type() != UPB_BACKEND,
    reason="only the upb protobuf backend has the limit",
)


@needs_upb
def test_a_column_100_calls_deep_collects(spark: SparkSession) -> None:
    frame = spark.createDataFrame([("abc",)], "s string")
    assert frame.select(NestedPlan.column(DEEP).alias("r")).first() == ("bbc",)


@needs_upb
def test_engine_stop_turns_the_protobuf_limit_back_on() -> None:
    engine = SailEngine()
    try:
        engine.start_session()
        NestedPlan.message(DEEP)
    finally:
        engine.stop()
    with pytest.raises(DecodeError):
        NestedPlan.message(DEEP)


def test_sail_gets_a_larger_thread_stack(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(STACK_SIZE_ENV_VAR, raising=False)
    scoped = ServerSettings(checkpoint_path=None).scoped_vars()
    assert scoped == {STACK_SIZE_ENV_VAR: str(SAIL_STACK_SIZE_BYTES)}


def test_a_stack_size_the_user_set_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(STACK_SIZE_ENV_VAR, USER_STACK_SIZE)
    assert STACK_SIZE_ENV_VAR not in ServerSettings(checkpoint_path=None).scoped_vars()


@needs_upb
def test_plans_build_up_to_python_recursion_limit(spark: SparkSession) -> None:
    frame = spark.createDataFrame([("abc",)], "s string")
    deep = frame.select(NestedPlan.column(BUILDABLE).alias("r"))
    assert deep._plan.to_proto(spark.client).ByteSize() > 0
    with pytest.raises(RecursionError):
        deeper = frame.select(NestedPlan.column(PAST_PYTHON_RECURSION).alias("r"))
        deeper._plan.to_proto(spark.client)


def test_without_a_jvm_a_deep_table_write_fails_fast(
    without_a_jvm: SparkSession,
) -> None:
    frame = without_a_jvm.createDataFrame([("abc",)], "s string")
    deep = frame.select(NestedPlan.column(PAST_THE_LIMIT).alias("r"))
    with pytest.raises(DeepColumnError):
        deep.write.saveAsTable(TABLE)
