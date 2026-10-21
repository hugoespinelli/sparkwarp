from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.testing.utils import assertDataFrameEqual

from benchmarks.schemas import ORDER_TOTALS, ORDERS
from benchmarks.transformers import RevenueAggregator

ROWS = [
    (1, 10, "BR", 100),
    (2, 10, "BR", 50),
    (3, 11, "US", 70),
    (4, 12, "US", None),
    (5, 13, "PT", 20),
]


def test_totals_per_country(spark: SparkSession) -> None:
    df = spark.createDataFrame(ROWS, ORDERS)
    result = RevenueAggregator(["country"], "amount").process(df)
    expected = spark.createDataFrame(
        [("BR", 150, 2, 75.0, 100), ("US", 70, 2, 70.0, 70), ("PT", 20, 1, 20.0, 20)],
        ORDER_TOTALS,
    )
    assertDataFrameEqual(result.orderBy("country"), expected.orderBy("country"))


def test_null_amounts_are_ignored_by_sum_and_avg_but_counted(
    spark: SparkSession,
) -> None:
    df = spark.createDataFrame(ROWS, ORDERS)
    row = (
        RevenueAggregator(["country"], "amount")
        .process(df)
        .filter(F.col("country") == "US")
        .collect()[0]
    )
    assert row["total"] == 70
    assert row["orders"] == 2
    assert row["average"] == 70.0


def test_empty_input_produces_no_groups(spark: SparkSession) -> None:
    df = spark.createDataFrame([], ORDERS)
    assert RevenueAggregator(["country"], "amount").process(df).count() == 0


def test_global_aggregate_without_group(spark: SparkSession) -> None:
    df = spark.createDataFrame(ROWS, ORDERS)
    row = df.agg(
        F.sum("amount").alias("s"), F.count("*").alias("n"), F.min("amount").alias("m")
    ).collect()[0]
    assert (row["s"], row["n"], row["m"]) == (240, 5, 20)


def test_group_by_two_keys(spark: SparkSession) -> None:
    df = spark.createDataFrame(ROWS, ORDERS)
    result = df.groupBy("country", "customer_id").agg(F.sum("amount").alias("total"))
    expected = spark.createDataFrame(
        [("BR", 10, 150), ("US", 11, 70), ("US", 12, None), ("PT", 13, 20)],
        "country string, customer_id int, total long",
    )
    assertDataFrameEqual(
        result.orderBy("country", "customer_id"),
        expected.orderBy("country", "customer_id"),
    )


def test_pivot_like_conditional_sums(spark: SparkSession) -> None:
    df = spark.createDataFrame(ROWS, ORDERS)
    row = df.agg(
        F.sum(F.when(F.col("country") == "BR", F.col("amount")).otherwise(0)).alias(
            "br"
        ),
        F.sum(F.when(F.col("country") != "BR", F.col("amount")).otherwise(0)).alias(
            "other"
        ),
    ).collect()[0]
    assert (row["br"], row["other"]) == (150, 90)
