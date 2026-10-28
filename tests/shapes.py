"""Query shapes shared by the lineage tests and the speed guard."""

from collections.abc import Callable

from pyspark.sql import Column, DataFrame, Row, SparkSession, Window
from pyspark.sql import functions as F

ROUNDS = 8
SLUG_DEPTH = 50
TITLES = [(i, f"Title {i % 7} a") for i in range(40)]
TITLES_SCHEMA = "id int, title string"
Step = Callable[[DataFrame], DataFrame]


class Slug:
    """A column about as deep as the production slug normalization."""

    @staticmethod
    def of(column: Column, depth: int = SLUG_DEPTH) -> Column:
        for _ in range(depth):
            column = F.regexp_replace(column, "a", "b")
        return column


class Rounds:
    """The iterative shape: each round re-derives the DataFrame and runs one action."""

    @staticmethod
    def dedupe(frame: DataFrame) -> DataFrame:
        keyed = frame.withColumn("slug", Slug.of(F.col("title")))
        window = Window.partitionBy("slug").orderBy("id")
        ranked = keyed.withColumn("rn", F.row_number().over(window))
        kept = ranked.filter(F.col("rn") == 1).drop("rn", "slug")
        return kept.withColumn("title", F.concat(F.col("title"), F.lit("x")))

    @staticmethod
    def noisy(frame: DataFrame) -> DataFrame:
        return Rounds.dedupe(frame).withColumn("noise", F.rand())

    @staticmethod
    def run(spark: SparkSession, step: Step, rounds: int = ROUNDS) -> list[list[Row]]:
        frame = spark.createDataFrame(TITLES, TITLES_SCHEMA)
        seen: list[list[Row]] = []
        for _ in range(rounds):
            frame = step(frame)
            seen.append(sorted(frame.select("id", "title").collect()))
        return seen
