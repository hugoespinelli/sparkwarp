"""Builds that register a temp view and query it, the way SQL-template code does."""

from __future__ import annotations

from collections.abc import Callable

from pyspark.sql import DataFrame, SparkSession

FrameFactory = Callable[[SparkSession], DataFrame]


class ViewQuery:
    """Registers `frame` as `view`, then returns `spark.sql(query)`."""

    def __init__(self, frame: FrameFactory, view: str, query: str) -> None:
        self.frame = frame
        self.view = view
        self.query = query

    def __call__(self, spark: SparkSession) -> DataFrame:
        self.frame(spark).createOrReplaceTempView(self.view)
        return spark.sql(self.query.format(view=self.view))
