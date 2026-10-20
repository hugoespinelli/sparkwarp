from collections.abc import Iterator

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.testing.utils import assertDataFrameEqual

import sparkwarp

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


@pytest.mark.xfail(
    strict=True,
    reason="ADOPT-001: Connect SparkSession does not subclass the classic one",
)
def test_session_is_a_classic_sparksession_instance(spark: SparkSession) -> None:
    assert isinstance(spark, SparkSession)


def test_filter_and_count(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1, "a"), (2, "b")], SCHEMA)
    assert df.filter(F.col("id") > 1).count() == 1


def test_assert_dataframe_equal_works(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1, "a")], SCHEMA)
    assertDataFrameEqual(df, spark.createDataFrame([(1, "a")], SCHEMA))
