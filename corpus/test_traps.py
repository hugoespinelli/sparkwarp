"""The brief's ten semantic traps, each as a handful of small cases."""

from __future__ import annotations

import pytest
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from corpus.frames import Frames
from corpus.gaps import KnownGap
from corpus.harness import Case, ParityChecker
from corpus.views import ViewQuery

N = Frames.numbers
P = Frames.people
E = Frames.events
ORD = Frames.orders
L = Frames.lookup


def one(spark: SparkSession) -> DataFrame:
    return spark.range(1)


NULL_LOGIC = [
    Case(
        "null_when_falls_to_otherwise",
        lambda s: N(s).select(
            "id", F.when(F.col("v") > 1, "big").otherwise("small").alias("r")
        ),
    ),
    Case(
        "null_not_equal_drops_null_rows",
        lambda s: N(s).filter(F.col("v") != 5).select("id"),
    ),
    Case(
        "null_eq_null_safe",
        lambda s: N(s).select("id", F.col("v").eqNullSafe(F.lit(None)).alias("r")),
    ),
    Case(
        "null_and_false_is_false",
        lambda s: N(s).select("id", ((F.col("v") > 1) & F.lit(False)).alias("r")),
    ),
    Case(
        "null_or_true_is_true",
        lambda s: N(s).select("id", ((F.col("v") > 1) | F.lit(True)).alias("r")),
    ),
    Case(
        "null_isin_with_null_member",
        lambda s: N(s).select("id", F.col("v").isin(5, None).alias("r")),
    ),
    Case(
        "null_count_col_vs_star",
        lambda s: N(s).agg(F.count("v").alias("c"), F.count("*").alias("n")),
    ),
    Case(
        "null_sum_avg_skip_nulls",
        lambda s: N(s).agg(F.sum("v").alias("s"), F.avg("v").alias("a")),
    ),
    Case(
        "null_concat_with_null",
        lambda s: N(s).select("id", F.concat(F.col("s"), F.lit("-")).alias("r")),
    ),
    Case(
        "null_concat_ws_skips_null",
        lambda s: N(s).select(
            "id", F.concat_ws("-", F.col("s"), F.lit("x")).alias("r")
        ),
    ),
]

COERCION = [
    Case(
        "coerce_coalesce_int_string_bad_value",
        lambda s: N(s).select(F.coalesce(F.col("v"), F.lit("x")).alias("c")),
        expects_error=True,
    ),
    Case(
        "coerce_coalesce_int_string_schema",
        lambda s: N(s).select("id", F.coalesce(F.col("v"), F.lit("7")).alias("c")),
        gap=KnownGap("GAP-003"),
    ),
    Case(
        "coerce_int_equals_string",
        lambda s: N(s).select("id", (F.col("id") == F.lit("1")).alias("r")),
    ),
    Case(
        "coerce_int_div_int_is_double",
        lambda s: one(s).select((F.lit(1) / F.lit(2)).alias("r")),
    ),
    Case(
        "coerce_int_plus_long",
        lambda s: one(s).select((F.lit(1) + F.col("id")).alias("r")),
    ),
    Case(
        "coerce_int_plus_double",
        lambda s: N(s).select("id", (F.col("v") + F.col("d")).alias("r")),
    ),
    Case(
        "coerce_when_branches_string_int",
        lambda s: N(s).select(
            "id", F.when(F.col("v") > 1, "a").otherwise(0).alias("r")
        ),
        gap=KnownGap("GAP-004"),
        expects_error=True,
    ),
    Case(
        "coerce_array_int_and_string",
        lambda s: one(s).select(F.array(F.lit(1), F.lit("a")).alias("r")),
        gap=KnownGap("GAP-004"),
        expects_error=True,
    ),
    Case(
        "coerce_string_plus_int",
        lambda s: one(s).select((F.lit("2") + F.lit(3)).alias("r")),
        gap=KnownGap("GAP-005"),
    ),
    Case(
        "coerce_greatest_int_double",
        lambda s: one(s).select(F.greatest(F.lit(1), F.lit(2.5)).alias("r")),
    ),
]

CASTS = [
    Case(
        "cast_bad_string_to_int_raises",
        lambda s: one(s).select(F.lit("abc").cast("int").alias("r")),
        expects_error=True,
    ),
    Case(
        "cast_try_cast_bad_string_is_null",
        lambda s: one(s).select(F.lit("abc").try_cast("int").alias("r")),
    ),
    Case(
        "cast_decimal_string_to_int_raises",
        lambda s: one(s).select(F.lit("1.9").cast("int").alias("r")),
        expects_error=True,
    ),
    Case(
        "cast_double_to_int_truncates",
        lambda s: one(s).select(F.lit(-1.9).cast("int").alias("r")),
    ),
    Case(
        "cast_int_overflow_raises",
        lambda s: one(s).select(F.lit(2**31).cast("int").alias("r")),
        gap=KnownGap("GAP-006"),
        expects_error=True,
    ),
    Case(
        "cast_string_to_boolean",
        lambda s: one(s).select(F.lit("true").cast("boolean").alias("r")),
    ),
    Case(
        "cast_string_to_date",
        lambda s: one(s).select(F.lit("2024-02-29").cast("date").alias("r")),
    ),
    Case(
        "cast_bad_date_raises",
        lambda s: one(s).select(F.lit("2024-02-30").cast("date").alias("r")),
        gap=KnownGap("GAP-006"),
        expects_error=True,
    ),
    Case(
        "cast_double_to_string",
        lambda s: one(s).select(F.lit(0.1 + 0.2).cast("string").alias("r")),
    ),
    Case(
        "cast_long_to_decimal",
        lambda s: one(s).select(F.lit(12345).cast("decimal(10,2)").alias("r")),
    ),
]

ARBITRARY_ROWS = [
    Case(
        "dedup_subset_keeps_one_per_key",
        lambda s: P(s).dropDuplicates(["name"]).select("name"),
    ),
    Case("distinct_keeps_unique_rows", lambda s: P(s).select("dept").distinct()),
    Case("dedup_full_row", lambda s: P(s).union(P(s)).dropDuplicates()),
]

WINDOW_FRAMES = [
    Case(
        "window_running_sum_default_frame",
        lambda s: P(s).select(
            "id", F.sum("age").over(Window.partitionBy("dept").orderBy("id")).alias("r")
        ),
    ),
    Case(
        "window_whole_partition_without_order",
        lambda s: P(s).select(
            "id", F.sum("age").over(Window.partitionBy("dept")).alias("r")
        ),
    ),
    Case(
        "window_range_frame_with_ties",
        lambda s: P(s).select(
            "id", F.count("*").over(Window.orderBy("age")).alias("r")
        ),
    ),
    Case(
        "window_rows_frame",
        lambda s: P(s).select(
            "id", F.sum("age").over(Window.orderBy("id").rowsBetween(-1, 0)).alias("r")
        ),
    ),
    Case(
        "window_row_number",
        lambda s: P(s).select(
            "id",
            F.row_number()
            .over(Window.partitionBy("dept").orderBy(F.col("age").desc()))
            .alias("r"),
        ),
    ),
    Case(
        "window_rank_and_dense_rank_ties",
        lambda s: P(s).select(
            "id",
            F.rank().over(Window.orderBy("age")).alias("r"),
            F.dense_rank().over(Window.orderBy("age")).alias("d"),
        ),
    ),
    Case(
        "window_lag_lead_defaults",
        lambda s: P(s).select(
            "id",
            F.lag("age").over(Window.orderBy("id")).alias("p"),
            F.lead("age", 1, -1).over(Window.orderBy("id")).alias("n"),
        ),
    ),
    Case(
        "window_nulls_sort_first_ascending",
        lambda s: P(s).select(
            "id", F.row_number().over(Window.orderBy("age", "id")).alias("r")
        ),
    ),
]

JAVA_REGEX = [
    Case(
        "regex_replace_digits",
        lambda s: one(s).select(
            F.regexp_replace(F.lit("a1b22c333"), r"\d+", "#").alias("r")
        ),
    ),
    Case(
        "regex_extract_group",
        lambda s: one(s).select(
            F.regexp_extract(F.lit("order-0042"), r"order-(\d+)", 1).alias("r")
        ),
    ),
    Case(
        "regex_rlike_anchor",
        lambda s: one(s).select(F.lit("order-0042").rlike(r"^order-\d{4}$").alias("r")),
    ),
    Case(
        "regex_lookahead",
        lambda s: one(s).select(
            F.regexp_replace(F.lit("price100usd"), r"\d+(?=usd)", "N").alias("r")
        ),
        gap=KnownGap("GAP-007"),
    ),
    Case(
        "regex_backreference",
        lambda s: one(s).select(F.lit("abab").rlike(r"^(ab)\1$").alias("r")),
        gap=KnownGap("GAP-007"),
    ),
    Case(
        "regex_posix_class",
        lambda s: one(s).select(
            F.regexp_replace(F.lit("a1-b2"), r"\p{Alpha}", "x").alias("r")
        ),
    ),
    Case(
        "regex_replacement_group_ref",
        lambda s: one(s).select(
            F.regexp_replace(F.lit("john smith"), r"(\w+) (\w+)", "$2, $1").alias("r")
        ),
    ),
    Case(
        "regex_case_insensitive_flag",
        lambda s: one(s).select(F.lit("HELLO").rlike("(?i)hello").alias("r")),
    ),
]

TIME = [
    Case(
        "time_to_timestamp",
        lambda s: E(s).select("id", F.to_timestamp("ts").alias("r")),
    ),
    Case(
        "time_date_trunc_month",
        lambda s: E(s).select(
            "id", F.date_trunc("month", F.to_timestamp("ts")).alias("r")
        ),
    ),
    Case(
        "time_trunc_returns_date",
        lambda s: E(s).select("id", F.trunc(F.to_date("ts"), "MM").alias("r")),
    ),
    Case(
        "time_to_date_with_format",
        lambda s: one(s).select(
            F.to_date(F.lit("17/05/2024"), "dd/MM/yyyy").alias("r")
        ),
    ),
    Case(
        "time_bad_format_raises_in_ansi",
        lambda s: one(s).select(
            F.to_date(F.lit("not a date"), "yyyy-MM-dd").alias("r")
        ),
        gap=KnownGap("GAP-006"),
        expects_error=True,
    ),
    Case(
        "time_unix_timestamp_utc",
        lambda s: E(s).select("id", F.unix_timestamp("ts").alias("r")),
    ),
    Case(
        "time_from_unixtime",
        lambda s: one(s).select(F.from_unixtime(F.lit(1715953510)).alias("r")),
    ),
    Case(
        "time_date_format_pattern",
        lambda s: E(s).select(
            "id", F.date_format(F.to_timestamp("ts"), "yyyy-MM-dd HH:mm EEE").alias("r")
        ),
    ),
    Case(
        "time_from_json_ddl",
        lambda s: E(s).select(
            "id", F.from_json("payload", "a INT, tags ARRAY<STRING>").alias("r")
        ),
    ),
    Case(
        "time_size_of_null_array",
        lambda s: E(s).select(
            "id",
            F.size(
                F.from_json("payload", "a INT, tags ARRAY<STRING>").getField("tags")
            ).alias("r"),
        ),
    ),
    Case(
        "time_datediff_type",
        lambda s: one(s).select(
            F.datediff(
                F.lit("2024-05-20").cast("date"), F.lit("2024-05-17").cast("date")
            ).alias("r")
        ),
        gap=KnownGap("GAP-002"),
    ),
    Case(
        "time_add_months_end_of_month",
        lambda s: one(s).select(
            F.add_months(F.lit("2024-01-31").cast("date"), 1).alias("r")
        ),
    ),
]

SCHEMA = [
    Case(
        "schema_infers_long_for_python_int",
        lambda s: s.createDataFrame([(1, "a")], ["i", "s"]),
    ),
    Case(
        "schema_keeps_explicit_nullability",
        lambda s: s.createDataFrame([(1, "a")], "i int not null, s string"),
        strict_nullable=True,
    ),
    Case(
        "schema_literal_is_not_nullable",
        lambda s: one(s).select(
            F.lit(1).alias("one"), F.lit(None).cast("int").alias("nothing")
        ),
        strict_nullable=True,
    ),
    Case(
        "schema_struct_field_order",
        lambda s: N(s).select(F.struct("s", "id").alias("st")),
    ),
    Case(
        "schema_union_nullability",
        lambda s: s.createDataFrame([(1,)], "i int not null").union(
            s.createDataFrame([(None,)], "i int")
        ),
        strict_nullable=True,
    ),
    Case(
        "schema_decimal_arithmetic_precision",
        lambda s: one(s).select(
            (F.lit(1.10).cast("decimal(5,2)") * F.lit(3).cast("decimal(3,0)")).alias(
                "r"
            )
        ),
    ),
    Case(
        "schema_union_by_name_missing_column",
        lambda s: s.createDataFrame([(1, "a")], "id int, name string").unionByName(
            s.createDataFrame([("b", 2, "x")], "name string, id int, note string"),
            allowMissingColumns=True,
        ),
        gap=KnownGap("GAP-001"),
    ),
    Case(
        "schema_count_is_long_not_null",
        lambda s: N(s).groupBy("s").count(),
        strict_nullable=True,
    ),
]

JOINS = [
    Case("join_inner_on_list", lambda s: ORD(s).join(L(s), ["country"], "inner")),
    Case("join_left_keeps_unmatched", lambda s: ORD(s).join(L(s), "country", "left")),
    Case("join_full_outer", lambda s: ORD(s).join(L(s), on="country", how="full")),
    Case("join_left_anti", lambda s: ORD(s).join(L(s), on="country", how="left_anti")),
    Case(
        "join_left_semi_columns",
        lambda s: ORD(s).join(L(s), on="country", how="left_semi"),
    ),
    Case(
        "join_expression_keeps_both_keys",
        lambda s: ORD(s)
        .alias("o")
        .join(L(s).alias("l"), F.col("o.country") == F.col("l.country")),
    ),
    Case(
        "join_null_keys_do_not_match",
        lambda s: N(s)
        .alias("a")
        .join(N(s).alias("b"), F.col("a.s") == F.col("b.s"))
        .select("a.id", "b.id"),
    ),
    Case(
        "join_cross", lambda s: L(s).crossJoin(L(s).select(F.col("region").alias("r2")))
    ),
]

NULLABILITY_FLAGS = [
    Case(
        "metadata_of_dict_agg_alias",
        lambda s: P(s).groupBy("dept").agg({"age": "avg"}),
        strict_nullable=True,
        gap=KnownGap("GAP-008"),
    ),
    Case(
        "nullable_flag_of_when_otherwise",
        lambda s: N(s).select(
            F.when(F.col("v") > 1, "big").otherwise("small").alias("r")
        ),
        strict_nullable=True,
        gap=KnownGap("GAP-008"),
    ),
    Case(
        "nullable_flag_of_cast_literal",
        lambda s: one(s).select(F.lit("true").cast("boolean").alias("r")),
        strict_nullable=True,
        gap=KnownGap("GAP-008"),
    ),
]

SQL = [
    Case(
        "sql_temp_view_group_by",
        ViewQuery(
            ORD,
            "corpus_orders",
            "SELECT country, SUM(amount) AS total FROM {view} GROUP BY country",
        ),
    ),
    Case(
        "sql_cte_and_case",
        ViewQuery(
            ORD,
            "corpus_orders",
            "WITH t AS (SELECT * FROM {view} WHERE amount IS NOT NULL) "
            "SELECT order_id, CASE WHEN amount > 60 THEN 'big' ELSE 'small' END "
            "AS size FROM t",
        ),
    ),
    Case(
        "sql_try_to_timestamp",
        lambda s: s.sql(
            "SELECT try_to_timestamp('2024-05-17 10:00:00') AS ok, "
            "try_to_timestamp('nope') AS bad"
        ),
    ),
    Case(
        "sql_get_is_zero_based",
        lambda s: s.sql(
            "SELECT get(array(10, 20, 30), 0) AS first, "
            "get(array(10, 20, 30), 5) AS missing"
        ),
    ),
    Case(
        "sql_from_json_array_of_struct",
        lambda s: s.sql(
            """SELECT from_json('[{"a":1},{"a":2}]', 'ARRAY<STRUCT<a: INT>>') AS r"""
        ),
    ),
    Case(
        "sql_trunc_month_returns_date",
        lambda s: s.sql("SELECT trunc(DATE'2024-05-17', 'MM') AS r"),
    ),
    Case(
        "sql_backticked_reserved_identifier",
        lambda s: s.sql("SELECT 1 AS `at`, 2 AS `order`"),
    ),
    Case(
        "sql_cast_raises_in_ansi",
        lambda s: s.sql("SELECT CAST('abc' AS INT) AS r"),
        expects_error=True,
    ),
]

REAL_USAGE = [
    Case(
        "window_order_by_case_on_function_result",
        lambda s: P(s).select(
            "id",
            F.row_number()
            .over(
                Window.partitionBy("dept").orderBy(
                    F.when(F.length("name") > 3, 0).otherwise(1),
                    "id",
                )
            )
            .alias("rn"),
        ),
        gap=KnownGap("GAP-014"),
    ),
    Case(
        "union_by_name_string_and_boolean",
        lambda s: s.createDataFrame([("true",)], "c string").unionByName(
            s.createDataFrame([(False,)], "c boolean")
        ),
        gap=KnownGap("GAP-017"),
    ),
]

ALL_CASES = (
    NULL_LOGIC
    + COERCION
    + CASTS
    + ARBITRARY_ROWS
    + WINDOW_FRAMES
    + JAVA_REGEX
    + TIME
    + SCHEMA
    + JOINS
    + SQL
    + NULLABILITY_FLAGS
    + REAL_USAGE
)


@pytest.mark.parametrize("case", Case.params(ALL_CASES))
def test_trap(case: Case, spark: SparkSession, parity: ParityChecker) -> None:
    parity.check(case, spark)
