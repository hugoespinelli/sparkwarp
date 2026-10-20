from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.testing.utils import assertDataFrameEqual

from benchmarks.markers import known_sail_gap
from benchmarks.schemas import COUNTRY_LOOKUP, ORDERS
from benchmarks.transformers import EnrichmentJoiner, SchemaUnion

ORDER_ROWS = [(1, 10, "BR", 100), (2, 11, "US", 70), (3, 12, "XX", 5)]
LOOKUP_ROWS = [("BR", "latam"), ("US", "na"), ("PT", "eu")]


def test_left_join_keeps_unmatched_rows_with_null(spark: SparkSession) -> None:
    orders = spark.createDataFrame(ORDER_ROWS, ORDERS)
    lookup = spark.createDataFrame(LOOKUP_ROWS, COUNTRY_LOOKUP)
    result = (
        EnrichmentJoiner(lookup, ["country"], "left")
        .process(orders)
        .select("order_id", "region")
    )
    expected = spark.createDataFrame(
        [(1, "latam"), (2, "na"), (3, None)], "order_id int, region string"
    )
    assertDataFrameEqual(result.orderBy("order_id"), expected.orderBy("order_id"))


def test_inner_join_drops_unmatched_rows(spark: SparkSession) -> None:
    orders = spark.createDataFrame(ORDER_ROWS, ORDERS)
    lookup = spark.createDataFrame(LOOKUP_ROWS, COUNTRY_LOOKUP)
    assert EnrichmentJoiner(lookup, ["country"], "inner").process(orders).count() == 2


def test_left_anti_join_finds_orphans(spark: SparkSession) -> None:
    orders = spark.createDataFrame(ORDER_ROWS, ORDERS)
    lookup = spark.createDataFrame(LOOKUP_ROWS, COUNTRY_LOOKUP)
    result = (
        EnrichmentJoiner(lookup, ["country"], "left_anti")
        .process(orders)
        .select("order_id")
    )
    assertDataFrameEqual(result, spark.createDataFrame([(3,)], "order_id int"))


def test_left_semi_join_keeps_only_left_columns_key_first(spark: SparkSession) -> None:
    orders = spark.createDataFrame(ORDER_ROWS, ORDERS)
    lookup = spark.createDataFrame(LOOKUP_ROWS, COUNTRY_LOOKUP)
    result = EnrichmentJoiner(lookup, ["country"], "left_semi").process(orders)
    assert result.columns == ["country", "order_id", "customer_id", "amount"]
    assert result.count() == 2


def test_join_on_two_keys_with_expression(spark: SparkSession) -> None:
    left = spark.createDataFrame([(1, "a", 1), (2, "b", 2)], "k1 int, k2 string, v int")
    right = spark.createDataFrame(
        [(1, "a", 10), (2, "z", 20)], "k1 int, k2 string, w int"
    )
    cond = (left["k1"] == right["k1"]) & (left["k2"] == right["k2"])
    result = left.join(right, cond, "inner").select(left["k1"], F.col("v"), F.col("w"))
    assertDataFrameEqual(
        result, spark.createDataFrame([(1, 1, 10)], "k1 int, v int, w int")
    )


@known_sail_gap("GAP-001")
def test_union_by_name_fills_missing_columns(spark: SparkSession) -> None:
    base = spark.createDataFrame([(1, "a")], "id int, name string")
    extra = spark.createDataFrame([("b", 2, "x")], "name string, id int, note string")
    result = SchemaUnion(extra).process(base)
    expected = spark.createDataFrame(
        [(1, "a", None), (2, "b", "x")], "id int, name string, note string"
    )
    assertDataFrameEqual(result.orderBy("id"), expected.orderBy("id"))


def test_plain_union_appends_positionally(spark: SparkSession) -> None:
    a = spark.createDataFrame([(1, "a")], "id int, name string")
    b = spark.createDataFrame([(2, "b")], "id int, name string")
    assert a.union(b).count() == 2
    assert a.union(b).unionByName(a).distinct().count() == 2
