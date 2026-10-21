from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.testing.utils import assertDataFrameEqual

from benchmarks.schemas import TEXT
from benchmarks.transformers import JsonPayloadExpander, TextNormalizer

PAYLOAD_DDL = "order_id INT, items ARRAY<STRING>"


def test_normalizer_lowercases_trims_and_strips_punctuation(
    spark: SparkSession,
) -> None:
    df = spark.createDataFrame([(1, "  Hello, World! "), (2, "A-B_C 9")], TEXT)
    result = TextNormalizer(["title"]).process(df)
    expected = spark.createDataFrame([(1, "hello world"), (2, "abc 9")], TEXT)
    assertDataFrameEqual(result.orderBy("id"), expected.orderBy("id"))


def test_normalizer_keeps_null(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1, None)], TEXT)
    assertDataFrameEqual(TextNormalizer(["title"]).process(df), df)


def test_json_payload_is_parsed_and_exploded(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [(1, '{"order_id": 7, "items": ["x", "y"]}')], "id int, payload string"
    )
    result = JsonPayloadExpander("payload", PAYLOAD_DDL, "items").process(df)
    expected = spark.createDataFrame([(1, "x"), (1, "y")], "id int, item string")
    assertDataFrameEqual(result.orderBy("item"), expected.orderBy("item"))


def test_json_parsed_struct_fields_are_selectable(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [(1, '{"order_id": 7, "items": []}')], "id int, payload string"
    )
    parsed = df.select(F.from_json("payload", PAYLOAD_DDL).alias("p")).select(
        "p.order_id", F.size("p.items").alias("n")
    )
    assertDataFrameEqual(parsed, spark.createDataFrame([(7, 0)], "order_id int, n int"))


def test_concat_ws_and_split(spark: SparkSession) -> None:
    df = spark.createDataFrame([("a", "b")], "x string, y string")
    joined = df.select(F.concat_ws("-", "x", "y").alias("j"))
    assert joined.collect()[0]["j"] == "a-b"
    assert joined.select(F.split("j", "-").alias("parts")).collect()[0]["parts"] == [
        "a",
        "b",
    ]


def test_regexp_extract_and_rlike_use_java_regex(spark: SparkSession) -> None:
    df = spark.createDataFrame([("order-0042",), ("nope",)], "code string")
    result = df.filter(F.col("code").rlike(r"^order-\d{4}$")).select(
        F.regexp_extract("code", r"(\d+)", 1).alias("n")
    )
    assertDataFrameEqual(result, spark.createDataFrame([("0042",)], "n string"))


def test_struct_and_array_constructors(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1, "a")], "id int, name string")
    row = df.select(
        F.struct("id", "name").alias("s"), F.array(F.lit(1), F.lit(2)).alias("arr")
    ).collect()[0]
    assert row["s"]["name"] == "a"
    assert row["arr"] == [1, 2]
