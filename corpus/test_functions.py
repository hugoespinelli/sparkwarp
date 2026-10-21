"""Function catalog: the `F.*` functions a typical lakehouse repo uses, one case each.

Doubles as the support audit (A4.1): the generated support page reads this list.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.window import WindowSpec

from corpus.frames import Frames
from corpus.gaps import KnownGap
from corpus.harness import Case, ParityChecker

FrameFactory = Callable[[SparkSession], DataFrame]


class RowBuild:
    """Selects `id` and the expression over one input frame."""

    def __init__(self, frame: FrameFactory, expr: Callable[[], Column]) -> None:
        self.frame = frame
        self.expr = expr

    def __call__(self, s: SparkSession) -> DataFrame:
        return self.frame(s).select("id", self.expr().alias("r"))


class ScalarBuild:
    """Evaluates the expression once, over a one-row frame."""

    def __init__(self, expr: Callable[[], Column]) -> None:
        self.expr = expr

    def __call__(self, s: SparkSession) -> DataFrame:
        return s.range(1).select(self.expr().alias("r"))


class AggBuild:
    """Aggregates the expression per `dept`."""

    def __init__(self, frame: FrameFactory, expr: Callable[[], Column]) -> None:
        self.frame = frame
        self.expr = expr

    def __call__(self, s: SparkSession) -> DataFrame:
        return self.frame(s).groupBy("dept").agg(self.expr().alias("r"))


class Catalog:
    """Names a case after its function and picks the build shape."""

    PREFIX = "fn_"

    @classmethod
    def row(
        cls,
        name: str,
        expr: Callable[[], Column],
        frame: FrameFactory = Frames.numbers,
        deterministic: bool = True,
        gap: KnownGap | None = None,
        expects_error: bool = False,
    ) -> Case:
        build = RowBuild(frame, expr)
        return Case(
            cls.PREFIX + name,
            build,
            deterministic=deterministic,
            gap=gap,
            expects_error=expects_error,
        )

    @classmethod
    def scalar(
        cls, name: str, expr: Callable[[], Column], deterministic: bool = True
    ) -> Case:
        return Case(cls.PREFIX + name, ScalarBuild(expr), deterministic=deterministic)

    @classmethod
    def agg(
        cls,
        name: str,
        expr: Callable[[], Column],
        frame: FrameFactory = Frames.people,
        gap: KnownGap | None = None,
    ) -> Case:
        return Case(cls.PREFIX + name, AggBuild(frame, expr), gap=gap)


row, scalar, agg = Catalog.row, Catalog.scalar, Catalog.agg
P, E = Frames.people, Frames.events


class Exprs:
    """Column expressions several cases share."""

    PAYLOAD_DDL = "a INT, tags ARRAY<STRING>"

    @staticmethod
    def window() -> WindowSpec:
        return Window.partitionBy("dept").orderBy("id")

    @staticmethod
    def ts() -> Column:
        return F.to_timestamp("ts")

    @staticmethod
    def date() -> Column:
        return F.to_date("ts")

    @classmethod
    def payload(cls) -> Column:
        return F.from_json("payload", cls.PAYLOAD_DDL)


W, TS, D, J = Exprs.window, Exprs.ts, Exprs.date, Exprs.payload

CORE = [
    row("col", lambda: F.col("v")),
    row("lit", lambda: F.lit(7)),
    row(
        "when",
        lambda: F.when(F.col("v") > 1, F.lit("big"))
        .when(F.col("v") == 0, "zero")
        .otherwise("small"),
    ),
    row("coalesce", lambda: F.coalesce(F.col("v"), F.lit(-1))),
    row("isnull", lambda: F.isnull("v")),
    row("isnan", lambda: F.isnan("d"), gap=KnownGap("GAP-009")),
    row("nanvl", lambda: F.nanvl(F.col("d"), F.lit(0.0))),
    row("nullif", lambda: F.nullif(F.col("v"), F.lit(0))),
    row("ifnull", lambda: F.ifnull(F.col("v"), F.lit(9))),
    row("nvl", lambda: F.nvl(F.col("s"), F.lit("none"))),
    row("greatest", lambda: F.greatest("id", "v")),
    row("least", lambda: F.least("id", "v")),
    row("expr", lambda: F.expr("id * 2 + 1")),
    row("struct", lambda: F.struct("id", "s")),
    row("create_map", lambda: F.create_map(F.coalesce("s", F.lit("?")), F.col("id"))),
    row(
        "create_map_null_key_raises",
        lambda: F.create_map(F.col("s"), F.col("id")),
        expects_error=True,
    ),
    row("hash", lambda: F.hash("id", "s")),
    row("xxhash64", lambda: F.xxhash64("id")),
    row("md5", lambda: F.md5("s")),
    row("sha1", lambda: F.sha1("s")),
    row("sha2", lambda: F.sha2("s", 256)),
    row("crc32", lambda: F.crc32("s")),
    row("base64", lambda: F.base64(F.col("s").cast("binary"))),
    row(
        "monotonically_increasing_id",
        lambda: F.monotonically_increasing_id(),
        deterministic=False,
    ),
    scalar("rand", lambda: F.rand(), deterministic=False),
    scalar("uuid", lambda: F.expr("uuid()"), deterministic=False),
]

STRINGS = [
    row("trim", lambda: F.trim(F.concat(F.lit("  "), "s", F.lit(" ")))),
    row("ltrim", lambda: F.ltrim(F.concat(F.lit("  "), "s"))),
    row("rtrim", lambda: F.rtrim(F.concat("s", F.lit("  ")))),
    row("lower", lambda: F.lower(F.lit("ABC"))),
    row("upper", lambda: F.upper("s")),
    row("initcap", lambda: F.initcap(F.lit("hello big world"))),
    row("length", lambda: F.length("s")),
    row("concat", lambda: F.concat("s", F.lit("_"), F.col("id").cast("string"))),
    row("concat_ws", lambda: F.concat_ws("|", "s", F.col("id").cast("string"))),
    row("substring", lambda: F.substring(F.lit("abcdef"), 2, 3)),
    row("substring_index", lambda: F.substring_index(F.lit("a.b.c"), ".", 2)),
    row("instr", lambda: F.instr(F.lit("banana"), "an")),
    row("locate", lambda: F.locate("an", F.lit("banana"), 3)),
    row("lpad", lambda: F.lpad("s", 4, "*")),
    row("rpad", lambda: F.rpad("s", 4, "*")),
    row("repeat", lambda: F.repeat("s", 3)),
    row("reverse", lambda: F.reverse("s")),
    row("translate", lambda: F.translate(F.lit("hello"), "el", "ip")),
    row("split", lambda: F.split(F.lit("a,b,,c"), ",")),
    row("split_limit", lambda: F.split(F.lit("a,b,c"), ",", 2)),
    row("regexp_replace", lambda: F.regexp_replace(F.lit("2024-05-17"), "-", "/")),
    row("regexp_extract", lambda: F.regexp_extract(F.lit("id=42;x"), r"id=(\d+)", 1)),
    row(
        "regexp_extract_no_match",
        lambda: F.regexp_extract(F.lit("none"), r"id=(\d+)", 1),
    ),
    row("rlike", lambda: F.col("s").rlike("^[a-c]$")),
    row("like", lambda: F.col("s").like("a%")),
    row("ilike", lambda: F.col("s").ilike("A%")),
    row("contains", lambda: F.col("s").contains("a")),
    row("startswith", lambda: F.col("s").startswith("c")),
    row("endswith", lambda: F.col("s").endswith("c")),
    row("ascii", lambda: F.ascii("s")),
    row("levenshtein", lambda: F.levenshtein(F.lit("kitten"), F.lit("sitting"))),
    row("soundex", lambda: F.soundex(F.lit("Robert"))),
    row("format_string", lambda: F.format_string("%s-%05d", F.col("s"), F.col("id"))),
    row("format_number", lambda: F.format_number(F.lit(1234567.891), 2)),
    row("encode_decode", lambda: F.decode(F.encode(F.lit("olá"), "UTF-8"), "UTF-8")),
    row("overlay", lambda: F.overlay(F.lit("SPARK_SQL"), F.lit("CORE"), 7)),
]

NUMBERS = [
    row("abs", lambda: F.abs(F.col("d"))),
    row("round", lambda: F.round(F.lit(2.5), 0)),
    row("round_half_up_negative", lambda: F.round(F.lit(-2.5), 0)),
    row("bround", lambda: F.bround(F.lit(2.5), 0)),
    row("floor", lambda: F.floor("d")),
    row("ceil", lambda: F.ceil("d")),
    row("sqrt", lambda: F.sqrt(F.col("v"))),
    row("pow", lambda: F.pow(F.col("id"), 2)),
    row("exp", lambda: F.exp(F.lit(1.0))),
    row("log", lambda: F.log(F.lit(10.0))),
    row("log10", lambda: F.log10(F.lit(1000.0))),
    row("signum", lambda: F.signum("d")),
    row("pmod", lambda: F.pmod(F.lit(-7), F.lit(3))),
    row("modulo_operator", lambda: F.lit(-7) % F.lit(3)),
    row("integer_division", lambda: F.expr("7 div 2")),
    row("try_divide_by_zero", lambda: F.try_divide(F.col("id"), F.lit(0))),
    row("divide_by_zero_raises", lambda: F.col("id") / F.lit(0), expects_error=True),
    row("try_add", lambda: F.try_add(F.lit(2**31 - 1), F.lit(1))),
    row("rand_seeded", lambda: F.rand(42), deterministic=False),
]

DATES = [
    row("to_date", D, E),
    row("to_timestamp", TS, E),
    row("date_trunc_day", lambda: F.date_trunc("day", TS()), E),
    row("date_trunc_week", lambda: F.date_trunc("week", TS()), E),
    row("trunc_year", lambda: F.trunc(D(), "year"), E),
    row("date_add", lambda: F.date_add(D(), 10), E),
    row("date_sub", lambda: F.date_sub(D(), 10), E),
    row(
        "datediff_days",
        lambda: F.datediff(F.lit("2024-03-01").cast("date"), D()),
        E,
        gap=KnownGap("GAP-002"),
    ),
    row("add_months", lambda: F.add_months(D(), 1), E),
    row(
        "months_between",
        lambda: F.months_between(F.lit("2024-06-30").cast("date"), D()),
        E,
    ),
    row("year", lambda: F.year(TS()), E),
    row("quarter", lambda: F.quarter(TS()), E),
    row("month", lambda: F.month(TS()), E),
    row("dayofmonth", lambda: F.dayofmonth(TS()), E),
    row("dayofweek", lambda: F.dayofweek(TS()), E),
    row("dayofyear", lambda: F.dayofyear(TS()), E),
    row("weekofyear", lambda: F.weekofyear(TS()), E),
    row("hour", lambda: F.hour(TS()), E),
    row("minute", lambda: F.minute(TS()), E),
    row("second", lambda: F.second(TS()), E),
    row("last_day", lambda: F.last_day(D()), E),
    row("next_day", lambda: F.next_day(D(), "Mon"), E),
    row("date_format", lambda: F.date_format(TS(), "dd/MM/yyyy"), E),
    row("unix_timestamp", lambda: F.unix_timestamp("ts"), E),
    row(
        "from_unixtime",
        lambda: F.from_unixtime(F.unix_timestamp("ts"), "yyyy-MM-dd"),
        E,
    ),
    row("to_utc_timestamp", lambda: F.to_utc_timestamp(TS(), "America/Sao_Paulo"), E),
    row("from_utc_timestamp", lambda: F.from_utc_timestamp(TS(), "Asia/Tokyo"), E),
    row("make_date", lambda: F.make_date(F.lit(2024), F.lit(2), F.lit(29))),
    scalar("current_date", lambda: F.current_date(), deterministic=False),
    scalar("current_timestamp", lambda: F.current_timestamp(), deterministic=False),
]

COLLECTIONS = [
    row("array", lambda: F.array("id", "v")),
    row("array_contains", lambda: F.array_contains(J().getField("tags"), "x"), E),
    row(
        "array_distinct",
        lambda: F.array_distinct(F.array(F.lit(1), F.lit(1), F.lit(2))),
    ),
    row("array_join", lambda: F.array_join(J().getField("tags"), "+"), E),
    row("array_sort", lambda: F.array_sort(F.array(F.lit(3), F.lit(1), F.lit(2)))),
    row(
        "sort_array_desc",
        lambda: F.sort_array(F.array(F.lit(3), F.lit(1), F.lit(2)), asc=False),
    ),
    row(
        "array_union",
        lambda: F.array_union(F.array(F.lit(1), F.lit(2)), F.array(F.lit(2), F.lit(3))),
    ),
    row("array_position", lambda: F.array_position(F.array(F.lit(5), F.lit(6)), 6)),
    row("element_at", lambda: F.element_at(F.array(F.lit(5), F.lit(6)), 2)),
    row(
        "element_at_out_of_range_raises",
        lambda: F.element_at(F.array(F.lit(5)), 3),
        expects_error=True,
        gap=KnownGap("GAP-006"),
    ),
    row("slice", lambda: F.slice(F.array(F.lit(1), F.lit(2), F.lit(3)), 2, 2)),
    row("size", lambda: F.size(J().getField("tags")), E),
    row(
        "flatten",
        lambda: F.flatten(F.array(F.array(F.lit(1)), F.array(F.lit(2), F.lit(3)))),
    ),
    row("sequence", lambda: F.sequence(F.lit(1), F.col("id"))),
    row("map_keys", lambda: F.map_keys(F.create_map(F.lit("k"), F.col("id")))),
    row("map_values", lambda: F.map_values(F.create_map(F.lit("k"), F.col("id")))),
    row("get_field", lambda: J().getField("a"), E),
    row("get_item", lambda: J().getField("tags").getItem(0), Frames.events_with_tags),
    row("to_json", lambda: F.to_json(F.struct("id", "s"))),
    row("get_json_object", lambda: F.get_json_object("payload", "$.tags[0]"), E),
    row("get_json_object_scalar", lambda: F.get_json_object("payload", "$.a"), E),
    row(
        "transform", lambda: F.transform(F.array(F.lit(1), F.lit(2)), lambda x: x * 10)
    ),
    row(
        "filter_higher_order",
        lambda: F.filter(F.array(F.lit(1), F.lit(2), F.lit(3)), lambda x: x > 1),
    ),
    row("exists", lambda: F.exists(F.array(F.lit(1), F.lit(2)), lambda x: x > 1)),
    row(
        "aggregate",
        lambda: F.aggregate(
            F.array(F.lit(1), F.lit(2), F.lit(3)), F.lit(0), lambda acc, x: acc + x
        ),
    ),
]

AGGREGATES = [
    agg("count", lambda: F.count("age")),
    agg("count_distinct", lambda: F.countDistinct("name")),
    agg("approx_count_distinct", lambda: F.approx_count_distinct("name")),
    agg("sum", lambda: F.sum("age")),
    agg("avg", lambda: F.avg("age")),
    agg("min", lambda: F.min("age")),
    agg("max", lambda: F.max("name")),
    agg("first_ignore_nulls", lambda: F.first("age", ignorenulls=True)),
    agg("collect_set", lambda: F.array_sort(F.collect_set("name"))),
    agg("stddev", lambda: F.stddev("age")),
    agg("variance", lambda: F.variance("age")),
    agg("sum_distinct", lambda: F.sum_distinct("age")),
    agg("max_by", lambda: F.max_by("name", "age")),
    agg("bool_and", lambda: F.bool_and(F.col("age") > 20)),
    agg(
        "percentile_approx",
        lambda: F.percentile_approx("age", 0.5),
        gap=KnownGap("GAP-010"),
    ),
    agg("sum_when", lambda: F.sum(F.when(F.col("age") > 26, 1).otherwise(0))),
]

WINDOWS = [
    row("row_number", lambda: F.row_number().over(W()), P),
    row("rank", lambda: F.rank().over(Window.orderBy("age")), P),
    row("dense_rank", lambda: F.dense_rank().over(Window.orderBy("age")), P),
    row("percent_rank", lambda: F.percent_rank().over(Window.orderBy("age")), P),
    row("cume_dist", lambda: F.cume_dist().over(Window.orderBy("age")), P),
    row("ntile", lambda: F.ntile(2).over(Window.orderBy("id")), P),
    row("lag", lambda: F.lag("age", 1).over(W()), P),
    row("lead_with_default", lambda: F.lead("age", 1, 0).over(W()), P),
    row("sum_over_partition", lambda: F.sum("age").over(Window.partitionBy("dept")), P),
    row(
        "avg_over_rows_frame",
        lambda: F.avg("age").over(Window.orderBy("id").rowsBetween(-1, 1)),
        P,
    ),
    row("first_over_window", lambda: F.first("name").over(W()), P),
    row(
        "last_over_unbounded",
        lambda: F.last("name").over(
            W().rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        ),
        P,
    ),
    row("count_over_window", lambda: F.count("*").over(Window.partitionBy("dept")), P),
]

GENERATORS = [
    Case(
        "fn_explode",
        lambda s: Frames.events(s).select(
            "id",
            F.explode(
                F.from_json("payload", "a INT, tags ARRAY<STRING>").getField("tags")
            ).alias("t"),
        ),
    ),
    Case(
        "fn_explode_outer",
        lambda s: Frames.events(s).select(
            "id",
            F.explode_outer(
                F.from_json("payload", "a INT, tags ARRAY<STRING>").getField("tags")
            ).alias("t"),
        ),
    ),
    Case(
        "fn_posexplode",
        lambda s: s.range(1).select(F.posexplode(F.array(F.lit("a"), F.lit("b")))),
    ),
    Case(
        "fn_explode_map",
        lambda s: s.range(1).select(F.explode(F.create_map(F.lit("k"), F.lit(1)))),
        gap=KnownGap("GAP-011"),
    ),
    Case(
        "fn_inline",
        lambda s: s.range(1).select(
            F.inline(F.array(F.struct(F.lit(1).alias("a"), F.lit("x").alias("b"))))
        ),
    ),
]

ALL_CASES = (
    CORE + STRINGS + NUMBERS + DATES + COLLECTIONS + AGGREGATES + WINDOWS + GENERATORS
)


@pytest.mark.parametrize("case", Case.params(ALL_CASES))
def test_function(case: Case, spark: SparkSession, parity: ParityChecker) -> None:
    parity.check(case, spark)
