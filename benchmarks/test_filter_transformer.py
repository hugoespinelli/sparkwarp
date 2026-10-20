from pyspark.sql import SparkSession
from pyspark.testing.utils import assertDataFrameEqual

from benchmarks.schemas import CUSTOMERS
from benchmarks.transformers import FilterTransformer

ROWS = [
    (1, "alice", 30),
    (2, "bob", 25),
    (3, "carol", 40),
    (4, "bob", 45),
    (5, None, None),
]


def test_and_clause_keeps_only_rows_matching_every_filter(spark: SparkSession) -> None:
    df = spark.createDataFrame(ROWS, CUSTOMERS)
    result = FilterTransformer(
        [("age", ">=", 30), ("name", "in", ["bob", "carol"])]
    ).process(df)
    expected = spark.createDataFrame([(3, "carol", 40), (4, "bob", 45)], CUSTOMERS)
    assertDataFrameEqual(result.orderBy("id"), expected.orderBy("id"))


def test_or_clause_keeps_rows_matching_any_filter(spark: SparkSession) -> None:
    df = spark.createDataFrame(ROWS, CUSTOMERS)
    result = FilterTransformer(
        [("age", "<", 26), ("name", "==", "carol")], "or"
    ).process(df)
    expected = spark.createDataFrame([(2, "bob", 25), (3, "carol", 40)], CUSTOMERS)
    assertDataFrameEqual(result.orderBy("id"), expected.orderBy("id"))


def test_not_equal_drops_null_rows(spark: SparkSession) -> None:
    df = spark.createDataFrame(ROWS, CUSTOMERS)
    result = FilterTransformer([("name", "!=", "bob")]).process(df)
    expected = spark.createDataFrame([(1, "alice", 30), (3, "carol", 40)], CUSTOMERS)
    assertDataFrameEqual(result.orderBy("id"), expected.orderBy("id"))


def test_is_null_selects_only_null_rows(spark: SparkSession) -> None:
    df = spark.createDataFrame(ROWS, CUSTOMERS)
    result = FilterTransformer([("age", "is_null", None)]).process(df)
    expected = spark.createDataFrame([(5, None, None)], CUSTOMERS)
    assertDataFrameEqual(result, expected)


def test_contains_and_startswith_on_strings(spark: SparkSession) -> None:
    df = spark.createDataFrame(ROWS, CUSTOMERS)
    result = FilterTransformer(
        [("name", "contains", "o"), ("name", "startswith", "b")]
    ).process(df)
    expected = spark.createDataFrame([(2, "bob", 25), (4, "bob", 45)], CUSTOMERS)
    assertDataFrameEqual(result.orderBy("id"), expected.orderBy("id"))


def test_empty_input_yields_empty_output_with_schema(spark: SparkSession) -> None:
    df = spark.createDataFrame([], CUSTOMERS)
    result = FilterTransformer([("age", ">=", 30)]).process(df)
    assert result.count() == 0
    assert result.schema == CUSTOMERS


def test_filter_on_every_operator_counts(spark: SparkSession) -> None:
    df = spark.createDataFrame(ROWS, CUSTOMERS)
    assert FilterTransformer([("age", "==", 30)]).process(df).count() == 1
    assert FilterTransformer([("age", ">=", 30)]).process(df).count() == 3
    assert FilterTransformer([("name", "in", ["bob"])]).process(df).count() == 2
