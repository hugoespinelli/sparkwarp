"""Explicit schemas for the benchmark, so type inference never differs by engine."""

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

CUSTOMERS = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
    ]
)
ORDERS = StructType(
    [
        StructField("order_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("country", StringType(), True),
        StructField("amount", IntegerType(), True),
    ]
)
ORDER_TOTALS = StructType(
    [
        StructField("country", StringType(), True),
        StructField("total", LongType(), True),
        StructField("orders", LongType(), False),
        StructField("average", DoubleType(), True),
        StructField("largest", IntegerType(), True),
    ]
)
VERSIONED = StructType(
    [
        StructField("key", StringType(), True),
        StructField("version", IntegerType(), True),
        StructField("payload", StringType(), True),
    ]
)
COUNTRY_LOOKUP = StructType(
    [
        StructField("country", StringType(), True),
        StructField("region", StringType(), True),
    ]
)
TEXT = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("title", StringType(), True),
    ]
)
EVENTS = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("event_time", StringType(), True),
    ]
)
FLAGGED = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("amount", IntegerType(), True),
        StructField("flag", IntegerType(), True),
    ]
)
