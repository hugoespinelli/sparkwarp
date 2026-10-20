"""A small transformer library shaped like a real lakehouse repo. Code under test."""

from __future__ import annotations

import operator
from collections.abc import Callable, Sequence
from enum import Enum
from functools import reduce

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

FilterValue = str | int | float | Sequence[str] | Sequence[int] | None
FilterSpec = tuple[str, str, FilterValue]
ColumnBuilder = Callable[[Column, FilterValue], Column]


class FilterOperator(str, Enum):
    EQ = "=="
    NE = "!="
    GE = ">="
    LT = "<"
    IN = "in"
    IS_NULL = "is_null"
    CONTAINS = "contains"
    STARTSWITH = "startswith"


class CombineMode(str, Enum):
    AND = "and"
    OR = "or"


class FilterOperators:
    BUILDERS: dict[FilterOperator, ColumnBuilder] = {
        FilterOperator.EQ: lambda c, v: c == F.lit(v),
        FilterOperator.NE: lambda c, v: c != F.lit(v),
        FilterOperator.GE: lambda c, v: c >= F.lit(v),
        FilterOperator.LT: lambda c, v: c < F.lit(v),
        FilterOperator.IN: lambda c, v: c.isin(list(v)),  # type: ignore[arg-type]
        FilterOperator.IS_NULL: lambda c, _: c.isNull(),
        FilterOperator.CONTAINS: lambda c, v: c.contains(F.lit(v)),
        FilterOperator.STARTSWITH: lambda c, v: c.startswith(F.lit(v)),
    }

    @classmethod
    def build(cls, column: Column, op: str, value: FilterValue) -> Column:
        return cls.BUILDERS[FilterOperator(op)](column, value)


class FilterTransformer:
    """Keeps rows that satisfy every (or any) filter spec."""

    def __init__(
        self, filters: Sequence[FilterSpec], combine_with: str = "and"
    ) -> None:
        self._filters = filters
        self._combine = (
            operator.and_
            if CombineMode(combine_with) is CombineMode.AND
            else operator.or_
        )

    def process(self, df: DataFrame) -> DataFrame:
        conditions = [
            FilterOperators.build(F.col(c), op, v) for c, op, v in self._filters
        ]
        return df.filter(reduce(self._combine, conditions))


class LatestRecordTransformer:
    """Keeps the newest row per key, ordered by a version column."""

    ROW_NUMBER = "_rn"

    def __init__(self, keys: Sequence[str], order_by: str) -> None:
        self._keys = list(keys)
        self._order_by = order_by

    def process(self, df: DataFrame) -> DataFrame:
        window = Window.partitionBy(*self._keys).orderBy(F.col(self._order_by).desc())
        ranked = df.withColumn(self.ROW_NUMBER, F.row_number().over(window))
        return ranked.filter(F.col(self.ROW_NUMBER) == 1).drop(self.ROW_NUMBER)


class RevenueAggregator:
    """Sums, counts and averages an amount column per group."""

    def __init__(self, group_by: Sequence[str], amount: str) -> None:
        self._group_by = list(group_by)
        self._amount = amount

    def process(self, df: DataFrame) -> DataFrame:
        return df.groupBy(*self._group_by).agg(
            F.sum(self._amount).alias("total"),
            F.count(F.lit(1)).alias("orders"),
            F.avg(self._amount).alias("average"),
            F.max(self._amount).alias("largest"),
        )


class EnrichmentJoiner:
    """Joins a lookup frame onto the input on shared keys."""

    def __init__(
        self, lookup: DataFrame, keys: Sequence[str], how: str = "left"
    ) -> None:
        self._lookup = lookup
        self._keys = list(keys)
        self._how = how

    def process(self, df: DataFrame) -> DataFrame:
        return df.join(self._lookup, on=self._keys, how=self._how)


class SchemaUnion:
    """Appends frames by column name, filling columns one side lacks with null."""

    def __init__(self, other: DataFrame) -> None:
        self._other = other

    def process(self, df: DataFrame) -> DataFrame:
        return df.unionByName(self._other, allowMissingColumns=True)


class TextNormalizer:
    """Lower-cases, trims and strips non-alphanumerics from text columns."""

    NON_ALNUM = r"[^a-z0-9 ]"

    def __init__(self, columns: Sequence[str]) -> None:
        self._columns = list(columns)

    def process(self, df: DataFrame) -> DataFrame:
        for column in self._columns:
            cleaned = F.regexp_replace(
                F.lower(F.trim(F.col(column))), self.NON_ALNUM, ""
            )
            df = df.withColumn(column, cleaned)
        return df


class JsonPayloadExpander:
    """Parses a JSON string column with a DDL schema and explodes its item array."""

    PARSED = "_parsed"

    def __init__(self, json_column: str, ddl: str, array_field: str) -> None:
        self._json_column = json_column
        self._ddl = ddl
        self._array_field = array_field

    def process(self, df: DataFrame) -> DataFrame:
        parsed = df.withColumn(
            self.PARSED, F.from_json(F.col(self._json_column), self._ddl)
        )
        exploded = parsed.withColumn(
            "item", F.explode(F.col(f"{self.PARSED}.{self._array_field}"))
        )
        return exploded.drop(self._json_column, self.PARSED)


class EventTimeTransformer:
    """Derives a day bucket and an age in days from an ISO timestamp string."""

    def __init__(self, event_time: str, as_of: str) -> None:
        self._event_time = event_time
        self._as_of = as_of

    def process(self, df: DataFrame) -> DataFrame:
        ts = F.to_timestamp(F.col(self._event_time))
        return (
            df.withColumn("event_day", F.date_trunc("day", ts))
            .withColumn(
                "age_days", F.datediff(F.to_date(F.lit(self._as_of)), F.to_date(ts))
            )
            .drop(self._event_time)
        )


class NullPolicy:
    """Fills gaps: defaults a numeric column, labels a status from a nullable flag."""

    def __init__(self, amount: str, flag: str) -> None:
        self._amount = amount
        self._flag = flag

    def process(self, df: DataFrame) -> DataFrame:
        status = F.when(F.col(self._flag) > 0, "active").otherwise("inactive")
        return df.withColumn(
            self._amount, F.coalesce(F.col(self._amount), F.lit(0))
        ).withColumn("status", status)


class SqlTemplateRunner:
    """Registers the input as a view and runs a parametrized SQL template on it."""

    VIEW = "input_view"
    TEMPLATE = (
        "SELECT {key}, COUNT(*) AS n, SUM({amount}) AS total "
        "FROM {view} WHERE {amount} > {floor} GROUP BY {key}"
    )

    def __init__(self, spark: SparkSession, key: str, amount: str, floor: int) -> None:
        self._spark = spark
        self._key = key
        self._amount = amount
        self._floor = floor

    def process(self, df: DataFrame) -> DataFrame:
        df.createOrReplaceTempView(self.VIEW)
        sql = self.TEMPLATE.format(
            key=self._key, amount=self._amount, view=self.VIEW, floor=self._floor
        )
        return self._spark.sql(sql)
