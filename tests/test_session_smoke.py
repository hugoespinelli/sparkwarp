from collections.abc import Callable, Iterator

import pytest
from pyspark.sql import Column, DataFrame, GroupedData, Row, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.sql.window import WindowSpec
from pyspark.testing.utils import assertDataFrameEqual

import sparkwarp
from corpus.gaps import KnownGap

SCHEMA = StructType(
    [StructField("id", IntegerType()), StructField("name", StringType())]
)


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    yield sparkwarp.session()
    sparkwarp.stop()


def test_dataframes_are_pyspark_dataframes(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1, "a")], SCHEMA)
    assert isinstance(df, DataFrame)


@KnownGap("ADOPT-001").mark(on_sail=True)
def test_session_is_a_classic_sparksession_instance(spark: SparkSession) -> None:
    assert isinstance(spark, SparkSession)


class ApiObjects:
    """Objects user code type-checks, built on the session under test."""

    def __init__(self, spark: SparkSession) -> None:
        self.df = spark.createDataFrame([(1, "a")], SCHEMA)

    def get(self, kind: str) -> object:
        builders: dict[str, Callable[[], object]] = {
            "schema": lambda: self.df.schema,
            "row": lambda: self.df.first(),
            "column": lambda: F.col("id"),
            "window": lambda: Window.orderBy("id"),
            "grouped": lambda: self.df.groupBy("id"),
        }
        return builders[kind]()


@pytest.mark.parametrize(
    ("kind", "expected_class"),
    [
        ("schema", StructType),
        ("row", Row),
        ("column", Column),
        ("window", WindowSpec),
        pytest.param(
            "grouped",
            GroupedData,
            marks=KnownGap("ADOPT-006").mark(on_sail=True),
        ),
    ],
)
def test_api_object_passes_isinstance(
    spark: SparkSession, kind: str, expected_class: type
) -> None:
    assert isinstance(ApiObjects(spark).get(kind), expected_class)


def test_filter_and_count(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1, "a"), (2, "b")], SCHEMA)
    assert df.filter(F.col("id") > 1).count() == 1


def test_assert_dataframe_equal_works(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1, "a")], SCHEMA)
    assertDataFrameEqual(df, spark.createDataFrame([(1, "a")], SCHEMA))
