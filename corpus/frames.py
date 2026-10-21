"""Tiny input frames for corpus cases. Explicit schemas, so inference never varies."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


class Frames:
    """Factory for the corpus's input DataFrames."""

    NUMBERS_DDL = "id int, v int, d double, s string"
    PEOPLE_DDL = "id int, name string, age int, dept string"
    EVENTS_DDL = "id int, ts string, payload string"
    ORDERS_DDL = "order_id int, customer_id int, country string, amount int"
    LOOKUP_DDL = "country string, region string"

    @classmethod
    def numbers(cls, spark: SparkSession) -> DataFrame:
        rows = [
            (1, None, 1.5, "a"),
            (2, 5, None, None),
            (3, 10, -2.25, "c"),
            (4, 0, 0.0, ""),
        ]
        return spark.createDataFrame(rows, cls.NUMBERS_DDL)

    @classmethod
    def people(cls, spark: SparkSession) -> DataFrame:
        rows = [
            (1, "alice", 30, "eng"),
            (2, "bob", 25, "eng"),
            (3, "carol", 40, "ops"),
            (4, "bob", 45, "ops"),
            (5, None, None, "ops"),
            (6, "dave", 30, None),
        ]
        return spark.createDataFrame(rows, cls.PEOPLE_DDL)

    @classmethod
    def events(cls, spark: SparkSession) -> DataFrame:
        rows = [
            (1, "2024-05-17 13:45:10", '{"a": 1, "tags": ["x", "y"]}'),
            (2, "2024-02-29 00:00:00", '{"a": 2, "tags": []}'),
            (3, None, None),
            (4, "2023-12-31 23:59:59", '{"a": null, "tags": ["z"]}'),
        ]
        return spark.createDataFrame(rows, cls.EVENTS_DDL)

    @classmethod
    def events_with_tags(cls, spark: SparkSession) -> DataFrame:
        return cls.events(spark).filter("id IN (1, 4)")

    @classmethod
    def orders(cls, spark: SparkSession) -> DataFrame:
        rows = [
            (1, 10, "BR", 100),
            (2, 10, "BR", 50),
            (3, 11, "US", 70),
            (4, 12, "US", None),
            (5, 13, "PT", 20),
            (6, 14, "XX", 5),
        ]
        return spark.createDataFrame(rows, cls.ORDERS_DDL)

    @classmethod
    def lookup(cls, spark: SparkSession) -> DataFrame:
        rows = [("BR", "latam"), ("US", "na"), ("PT", "eu"), ("DE", "eu")]
        return spark.createDataFrame(rows, cls.LOOKUP_DDL)
