from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.testing.utils import assertDataFrameEqual

from benchmarks.markers import known_sail_gap
from benchmarks.schemas import EVENTS, FLAGGED, ORDERS
from benchmarks.transformers import EventTimeTransformer, NullPolicy, SqlTemplateRunner

EVENT_ROWS = [(1, "2024-05-17 13:45:00"), (2, "2024-05-18 00:00:01")]


@known_sail_gap("GAP-002")
def test_event_day_is_truncated_to_midnight(spark: SparkSession) -> None:
    df = spark.createDataFrame(EVENT_ROWS, EVENTS)
    result = EventTimeTransformer("event_time", "2024-05-20").process(df)
    expected = (
        spark.createDataFrame(EVENT_ROWS, EVENTS)
        .select(
            "id",
            F.to_timestamp(F.substring("event_time", 1, 10)).alias("event_day"),
            F.lit(0).alias("age_days"),
        )
        .withColumn("age_days", F.when(F.col("id") == 1, 3).otherwise(2))
    )
    assertDataFrameEqual(result.orderBy("id"), expected.orderBy("id"))


def test_age_in_days_counts_calendar_days(spark: SparkSession) -> None:
    df = spark.createDataFrame(EVENT_ROWS, EVENTS)
    ages = [
        r["age_days"]
        for r in EventTimeTransformer("event_time", "2024-05-20")
        .process(df)
        .orderBy("id")
        .collect()
    ]
    assert ages == [3, 2]


def test_current_timestamp_is_not_null_and_is_recent(spark: SparkSession) -> None:
    row = (
        spark.range(1)
        .select(F.current_timestamp().alias("now"), F.year(F.current_date()).alias("y"))
        .collect()[0]
    )
    assert row["now"] is not None
    assert row["y"] >= 2024


def test_null_policy_defaults_amount_and_labels_status(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1, None, 1), (2, 5, 0), (3, 7, None)], FLAGGED)
    result = NullPolicy("amount", "flag").process(df)
    expected = spark.createDataFrame(
        [(1, 0, 1, "active"), (2, 5, 0, "inactive"), (3, 7, None, "inactive")],
        "id int, amount int, flag int, status string",
    )
    assertDataFrameEqual(result.orderBy("id"), expected.orderBy("id"))


def test_fillna_and_drop_na(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1, None, 1), (2, 5, None)], FLAGGED)
    assert df.fillna(0).filter(F.col("amount") == 0).count() == 1
    assert df.dropna().count() == 0
    assert df.na.fill({"flag": 9}).filter(F.col("flag") == 9).count() == 1


def test_cast_and_try_cast(spark: SparkSession) -> None:
    df = spark.createDataFrame([("12",), ("abc",)], "s string")
    assert (
        df.select(F.col("s").try_cast("int").alias("i")).orderBy("i").collect()[0]["i"]
        is None
    )
    assert (
        df.filter(F.col("s") == "12")
        .select(F.col("s").cast("int").alias("i"))
        .collect()[0]["i"]
        == 12
    )


def test_sql_template_on_temp_view(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [(1, 10, "BR", 100), (2, 10, "BR", 5), (3, 11, "US", 70)], ORDERS
    )
    result = SqlTemplateRunner(spark, "country", "amount", 50).process(df)
    expected = spark.createDataFrame(
        [("BR", 1, 100), ("US", 1, 70)], "country string, n long, total long"
    )
    assertDataFrameEqual(result.orderBy("country"), expected.orderBy("country"))


def test_sql_can_read_two_views_and_join(spark: SparkSession) -> None:
    spark.createDataFrame([(1, "a")], "id int, name string").createOrReplaceTempView(
        "left_v"
    )
    spark.createDataFrame([(1, 9)], "id int, score int").createOrReplaceTempView(
        "right_v"
    )
    row = spark.sql(
        "SELECT l.name, r.score FROM left_v l JOIN right_v r ON l.id = r.id"
    ).collect()[0]
    assert (row["name"], row["score"]) == ("a", 9)


def test_when_chain_with_null_condition_falls_to_otherwise(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1, None, 0), (2, 15, 0), (3, 3, 0)], FLAGGED)
    label = (
        F.when(F.col("amount") > 10, "big")
        .when(F.col("amount") > 0, "small")
        .otherwise("none")
    )
    result = df.select("id", label.alias("size"))
    expected = spark.createDataFrame(
        [(1, "none"), (2, "big"), (3, "small")], "id int, size string"
    )
    assertDataFrameEqual(result.orderBy("id"), expected.orderBy("id"))
