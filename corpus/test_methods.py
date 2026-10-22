"""Method catalog: DataFrame methods a typical lakehouse repo calls, one case each."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from corpus.frames import Frames
from corpus.gaps import KnownGap
from corpus.harness import Case, ParityChecker
from corpus.views import ViewQuery

P = Frames.people
ORD = Frames.orders
L = Frames.lookup
N = Frames.numbers


TRANSFORMATIONS = [
    Case("df_select", lambda s: P(s).select("id", F.col("name").alias("who"))),
    Case("df_select_expr", lambda s: P(s).selectExpr("id", "age * 2 AS double_age")),
    Case(
        "df_alias_and_qualified_columns",
        lambda s: P(s).alias("p").select("p.id", "p.name"),
    ),
    Case("df_with_column", lambda s: P(s).withColumn("adult", F.col("age") >= 30)),
    Case(
        "df_with_columns",
        lambda s: P(s).withColumns({"a": F.lit(1), "b": F.col("id") + 1}),
    ),
    Case("df_with_column_renamed", lambda s: P(s).withColumnRenamed("name", "who")),
    Case(
        "df_with_columns_renamed",
        lambda s: P(s).withColumnsRenamed({"name": "who", "age": "years"}),
    ),
    Case("df_to_df", lambda s: P(s).toDF("a", "b", "c", "d")),
    Case("df_drop", lambda s: P(s).drop("age", "missing_column")),
    Case("df_filter_string_expr", lambda s: P(s).filter("age > 26 AND dept = 'ops'")),
    Case("df_where", lambda s: P(s).where(F.col("name").isNotNull())),
    Case(
        "df_order_by",
        lambda s: P(s).orderBy(F.col("age").desc_nulls_last(), "id"),
        ordered=True,
    ),
    Case("df_sort", lambda s: P(s).sort("dept", "id"), ordered=True),
    Case("df_limit", lambda s: P(s).orderBy("id").limit(2), ordered=True),
    Case("df_distinct", lambda s: P(s).select("dept").distinct()),
    Case("df_drop_duplicates", lambda s: P(s).dropDuplicates(["dept"]).select("dept")),
    Case(
        "df_group_by_agg",
        lambda s: P(s)
        .groupBy("dept")
        .agg(F.count("*").alias("n"), F.max("age").alias("oldest")),
    ),
    Case("df_group_by_dict_agg", lambda s: P(s).groupBy("dept").agg({"age": "avg"})),
    Case(
        "df_pivot",
        lambda s: ORD(s)
        .groupBy("customer_id")
        .pivot("country", ["BR", "US"])
        .sum("amount"),
    ),
    Case(
        "df_rollup",
        lambda s: ORD(s).rollup("country").agg(F.sum("amount").alias("total")),
    ),
    Case("df_cube", lambda s: ORD(s).cube("country").count()),
    Case("df_join", lambda s: ORD(s).join(L(s), "country")),
    Case(
        "df_cross_join",
        lambda s: L(s).crossJoin(L(s).select(F.col("country").alias("c2"))),
    ),
    Case("df_union", lambda s: L(s).union(L(s))),
    Case(
        "df_union_by_name", lambda s: L(s).unionByName(L(s).select("region", "country"))
    ),
    Case("df_intersect", lambda s: L(s).intersect(L(s).filter("region = 'eu'"))),
    Case("df_except_all", lambda s: L(s).exceptAll(L(s).filter("region = 'eu'"))),
    Case("df_subtract", lambda s: L(s).subtract(L(s).filter("region = 'eu'"))),
    Case("df_fillna", lambda s: N(s).fillna(0)),
    Case("df_fillna_dict", lambda s: N(s).fillna({"v": -1, "s": "?"})),
    Case("df_dropna", lambda s: N(s).dropna()),
    Case("df_na_replace", lambda s: N(s).na.replace(["a"], "A", ["s"])),
    Case("df_replace", lambda s: P(s).replace("bob", "robert")),
    Case("df_transform", lambda s: P(s).transform(lambda df: df.filter("age > 26"))),
    Case("df_coalesce_partitions", lambda s: P(s).coalesce(1)),
    Case("df_repartition", lambda s: P(s).repartition(2, "dept")),
    Case("df_cache_and_persist", lambda s: P(s).cache().persist()),
    Case("df_hint", lambda s: ORD(s).join(L(s).hint("broadcast"), "country")),
    Case(
        "df_describe",
        lambda s: P(s).describe("age"),
        ordered=True,
        gap=KnownGap("GAP-012", stable=False),
    ),
    Case(
        "df_summary",
        lambda s: P(s).select("age").summary("count", "min", "max"),
        ordered=True,
        gap=KnownGap("GAP-012", stable=False),
    ),
    Case("df_col_regex", lambda s: P(s).select(P(s).colRegex("`^(id|age)$`"))),
    Case(
        "spark_sql_on_temp_view",
        ViewQuery(
            P, "corpus_people", "SELECT dept, COUNT(*) AS n FROM {view} GROUP BY dept"
        ),
    ),
    Case("spark_range", lambda s: s.range(0, 10, 3)),
    Case(
        "spark_create_data_frame_from_dicts",
        lambda s: s.createDataFrame([{"a": 1, "b": "x"}, {"a": 2, "b": None}]),
    ),
    Case(
        "spark_create_data_frame_empty",
        lambda s: s.createDataFrame([], "a int, b string"),
    ),
]

ACTIONS = [
    Case("action_count", lambda s: P(s).count()),
    Case("action_collect", lambda s: P(s).orderBy("id").collect()),
    Case("action_first", lambda s: P(s).orderBy("id").first()),
    Case("action_head", lambda s: P(s).orderBy("id").head(2)),
    Case("action_take", lambda s: P(s).orderBy("id").take(2)),
    Case("action_tail", lambda s: P(s).orderBy("id").tail(2)),
    Case("action_is_empty", lambda s: P(s).filter("age > 100").isEmpty()),
    Case("action_columns", lambda s: P(s).columns),
    Case("action_dtypes", lambda s: P(s).dtypes),
    Case("action_schema_simple_string", lambda s: P(s).schema.simpleString()),
    Case(
        "action_to_pandas",
        lambda s: P(s).orderBy("id").toPandas().to_dict(orient="list"),
    ),
    Case(
        "action_to_json",
        lambda s: P(s).orderBy("id").toJSON().collect(),
        gap=KnownGap("ADOPT-004"),
    ),
    Case(
        "action_rdd_is_unsupported",
        lambda s: P(s).rdd.count(),
        gap=KnownGap("ADOPT-005"),
    ),
    Case(
        "action_spark_context_is_unsupported",
        lambda s: s.sparkContext.appName,
        gap=KnownGap("ADOPT-003"),
    ),
]


class CacheCycle:
    """Caches, materialises and unpersists a frame, then asks whether it is cached."""

    def __call__(self, spark: SparkSession) -> bool:
        frame = P(spark)
        frame.cache()
        frame.count()
        frame.unpersist()
        return bool(frame.is_cached)


REAL_USAGE = [
    Case("action_is_cached_after_unpersist", CacheCycle(), gap=KnownGap("GAP-015")),
    Case("action_rdd_is_empty", lambda s: P(s).rdd.isEmpty()),
    Case("action_rdd_is_empty_on_empty_frame", lambda s: P(s).limit(0).rdd.isEmpty()),
    Case("action_set_log_level", lambda s: s.sparkContext.setLogLevel("WARN")),
    Case("action_catalog_clear_cache", lambda s: s.catalog.clearCache()),
]

ALL_CASES = TRANSFORMATIONS + ACTIONS + REAL_USAGE


@pytest.mark.parametrize("case", Case.params(ALL_CASES))
def test_method(case: Case, spark: SparkSession, parity: ParityChecker) -> None:
    parity.check(case, spark)
