"""Ordering lane: which row orders are only accidental, and whether Sail shares them.

Spark guarantees no order without `orderBy`. On `local[1]` many orders are stable by
accident, and tests come to rely on them. Cases here compare rows in order.
"""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from corpus.frames import Frames
from corpus.gaps import KnownGap
from corpus.harness import Case, ParityChecker

P = Frames.people
ORD = Frames.orders
L = Frames.lookup

GUARANTEED = [
    Case(
        "order_by_ascending_puts_nulls_first",
        lambda s: P(s).orderBy("age", "id").select("id", "age"),
        ordered=True,
    ),
    Case(
        "order_by_descending_puts_nulls_last",
        lambda s: P(s).orderBy(F.col("age").desc(), "id").select("id", "age"),
        ordered=True,
    ),
    Case(
        "order_by_explicit_nulls_last",
        lambda s: P(s).orderBy(F.col("age").asc_nulls_last(), "id").select("id", "age"),
        ordered=True,
    ),
    Case("sort_within_limit", lambda s: P(s).orderBy("id").limit(3), ordered=True),
]

ACCIDENTAL = [
    Case(
        "accidental_filter_keeps_insertion_order",
        lambda s: P(s).filter(F.col("age") > 26).select("id"),
        ordered=True,
    ),
    Case(
        "accidental_union_appends_in_order",
        lambda s: P(s).select("id").union(ORD(s).select("order_id")),
        ordered=True,
    ),
    Case(
        "accidental_left_join_order",
        lambda s: ORD(s).join(L(s), "country", "left").select("order_id"),
        ordered=True,
        gap=KnownGap("ORDER-001", stable=False),
    ),
    Case(
        "accidental_group_by_order",
        lambda s: P(s).groupBy("dept").count(),
        ordered=True,
    ),
    Case(
        "accidental_distinct_order",
        lambda s: P(s).select("dept").distinct(),
        ordered=True,
    ),
    Case(
        "accidental_drop_duplicates_order",
        lambda s: P(s).dropDuplicates(["dept"]).select("dept"),
        ordered=True,
    ),
    Case(
        "accidental_first_in_group",
        lambda s: P(s).groupBy("dept").agg(F.first("name").alias("f")).orderBy("dept"),
        ordered=True,
    ),
    Case(
        "accidental_collect_list_order",
        lambda s: P(s)
        .groupBy("dept")
        .agg(F.collect_list("id").alias("ids"))
        .orderBy("dept"),
        ordered=True,
    ),
]

ALL_CASES = GUARANTEED + ACCIDENTAL


@pytest.mark.parametrize("case", Case.params(ALL_CASES))
def test_ordering(case: Case, spark: SparkSession, parity: ParityChecker) -> None:
    parity.check(case, spark)
