from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.testing.utils import assertDataFrameEqual

from benchmarks.schemas import VERSIONED
from benchmarks.transformers import LatestRecordTransformer

ROWS = [
    ("a", 1, "old"),
    ("a", 3, "new"),
    ("a", 2, "mid"),
    ("b", 1, "only"),
    ("c", 2, "x"),
    ("c", 2, "y"),
]


def test_latest_version_wins_per_key(spark: SparkSession) -> None:
    df = spark.createDataFrame(ROWS[:4], VERSIONED)
    result = LatestRecordTransformer(["key"], "version").process(df)
    expected = spark.createDataFrame([("a", 3, "new"), ("b", 1, "only")], VERSIONED)
    assertDataFrameEqual(result.orderBy("key"), expected.orderBy("key"))


def test_single_row_per_key_is_kept_unchanged(spark: SparkSession) -> None:
    df = spark.createDataFrame([("b", 1, "only")], VERSIONED)
    result = LatestRecordTransformer(["key"], "version").process(df)
    assertDataFrameEqual(result, df)


def test_ties_keep_exactly_one_row(spark: SparkSession) -> None:
    df = spark.createDataFrame(ROWS, VERSIONED)
    result = LatestRecordTransformer(["key"], "version").process(df)
    assert result.count() == 3
    assert result.filter(F.col("key") == "c").count() == 1


def test_drop_duplicates_on_subset(spark: SparkSession) -> None:
    df = spark.createDataFrame(ROWS, VERSIONED)
    result = df.dropDuplicates(["key"]).select("key")
    expected = spark.createDataFrame([("a",), ("b",), ("c",)], "key string")
    assertDataFrameEqual(result.orderBy("key"), expected.orderBy("key"))


def test_running_total_over_ordered_window(spark: SparkSession) -> None:
    from pyspark.sql import Window

    df = spark.createDataFrame(ROWS[:3], VERSIONED)
    window = Window.partitionBy("key").orderBy("version")
    result = df.withColumn("running", F.sum("version").over(window)).select(
        "version", "running"
    )
    expected = spark.createDataFrame(
        [(1, 1), (2, 3), (3, 6)], "version int, running long"
    )
    assertDataFrameEqual(result.orderBy("version"), expected.orderBy("version"))


def test_distinct_and_count_distinct_agree(spark: SparkSession) -> None:
    df = spark.createDataFrame(ROWS, VERSIONED)
    assert df.select("key").distinct().count() == 3
    assert df.agg(F.countDistinct("key")).collect()[0][0] == 3
