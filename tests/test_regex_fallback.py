"""Java-only regex patterns run in Python; expected values come from real Spark."""

import re
import sys
from collections.abc import Callable, Iterator

import pytest
from pyspark.errors import PythonException
from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.connect.expressions import UnresolvedFunction

from sparkwarp.engines import SailEngine
from sparkwarp.regex_fallback import (
    FallbackInit,
    JavaPattern,
    JavaReplacement,
    RegexFallbackPatch,
)

ROWS = [
    ("price100usd 7usd",),
    ("abab",),
    (None,),
    ("no digits",),
    ("aXbXc\n",),
    ("John Smith",),
]
SPARK_RESULTS: list[tuple[str, Callable[[], Column], list[object]]] = [
    (
        "extract_lookahead",
        lambda: F.regexp_extract("s", r"(\d+)(?=usd)", 1),
        ["100", "", None, "", "", ""],
    ),
    (
        "extract_lookbehind_whole_match",
        lambda: F.regexp_extract("s", r"(?<=X)\w", 0),
        ["", "", None, "", "b", ""],
    ),
    (
        "extract_unmatched_group",
        lambda: F.regexp_extract("s", r"(z)?(?=b)", 1),
        ["", "", None, "", "", ""],
    ),
    (
        "extract_named_backreference",
        lambda: F.regexp_extract("s", r"(?<p>ab)\k<p>", 0),
        ["", "abab", None, "", "", ""],
    ),
    (
        "extract_all_lookahead",
        lambda: F.regexp_extract_all("s", F.lit(r"(\d+)(?=usd)"), 1),
        [["100", "7"], [], None, [], [], []],
    ),
    (
        "extract_all_default_group",
        lambda: F.regexp_extract_all("s", F.lit(r"(\w)(?=X)")),
        [[], [], None, [], ["a", "b"], []],
    ),
    (
        "replace_lookahead",
        lambda: F.regexp_replace("s", r"\d+(?=usd)", "N"),
        ["priceNusd Nusd", "abab", None, "no digits", "aXbXc\n", "John Smith"],
    ),
    (
        "replace_with_group_references",
        lambda: F.regexp_replace("s", r"(\w+) (?=Smith)(\w+)", "$2, $1 \\$"),
        ["price100usd 7usd", "abab", None, "no digits", "aXbXc\n", "Smith, John $"],
    ),
    (
        "replace_backreference",
        lambda: F.regexp_replace("s", r"(ab)\1", "<$1>"),
        ["price100usd 7usd", "<ab>", None, "no digits", "aXbXc\n", "John Smith"],
    ),
    (
        "rlike_backreference",
        lambda: F.col("s").rlike(r"^(ab)\1$"),
        [False, True, None, False, False, False],
    ),
    (
        "regexp_like_negative_lookahead",
        lambda: F.regexp_like("s", F.lit(r"^(?!no).*")),
        [True, True, None, False, True, True],
    ),
    (
        "java_end_of_input",
        lambda: F.regexp_extract("s", r"(c)\Z", 1),
        ["", "", None, "", "c", ""],
    ),
]
EDGE_ROWS = [
    ("abab\r\n",),
    ("c\r\n",),
    ("a\rb",),
    ("(<x>",),
    ("John Smith",),
    ("x\\Zy",),
    (None,),
    ("aab",),
    ("café",),
    ("caf!",),
]
EDGE_RESULTS: list[tuple[str, Callable[[], Column], list[object]]] = [
    (
        "escaped_parenthesis_before_named_text",
        lambda: F.regexp_extract("s", r"(?=\()\(?<x>", 0),
        ["", "", "", "(<x>", "", "", None, "", "", ""],
    ),
    (
        "escaped_backslash_before_z",
        lambda: F.regexp_extract("s", r"(?=x)x\\Z", 0),
        ["", "", "", "", "", "x\\Z", None, "", "", ""],
    ),
    (
        "dollar_before_a_final_crlf",
        lambda: F.col("s").rlike(r"^(ab)\1$"),
        [True, False, False, False, False, False, None, False, False, False],
    ),
    (
        "java_end_before_a_final_crlf",
        lambda: F.regexp_extract("s", r"(c)(?=c?)\Z", 1),
        ["", "c", "", "", "", "", None, "", "", ""],
    ),
    (
        "dot_does_not_match_a_carriage_return",
        lambda: F.col("s").rlike(r"(?=a)a.b"),
        [False, False, False, False, False, False, None, True, False, False],
    ),
    (
        "dollar_twelve_with_two_groups",
        lambda: F.regexp_replace("s", r"(\w+) (?=Smith)(\w+)", "$12"),
        [
            "abab\r\n",
            "c\r\n",
            "a\rb",
            "(<x>",
            "John2",
            "x\\Zy",
            None,
            "aab",
            "café",
            "caf!",
        ],
    ),
    (
        "named_group_reference",
        lambda: F.regexp_replace("s", r"(?<f>\w+) (?=Smith)(\w+)", "${f}!"),
        [
            "abab\r\n",
            "c\r\n",
            "a\rb",
            "(<x>",
            "John!",
            "x\\Zy",
            None,
            "aab",
            "café",
            "caf!",
        ],
    ),
    (
        "regexp_function",
        lambda: F.regexp("s", F.lit(r"^(?!J)")),
        [True, True, True, True, False, True, None, True, True, True],
    ),
    (
        "case_insensitive_flag",
        lambda: F.col("s").rlike(r"(?i)^(?=j)JOHN"),
        [False, False, False, False, True, False, None, False, False, False],
    ),
    (
        "non_ascii_digit_after_a_group_reference",
        lambda: F.regexp_replace("s", r"(x)(?=1)", "$1\u00b2"),
        [
            "abab\r\n",
            "c\r\n",
            "a\rb",
            "(<x>",
            "John Smith",
            "x\\Zy",
            None,
            "aab",
            "café",
            "caf!",
        ],
    ),
]
MATCHING_ROWS = [("aa",), ("ab",), ("abab",), ("abc",), ("a\u0085",), ("A",)]
JAVA_MATCHING_RESULTS: list[tuple[str, Callable[[], Column], list[object]]] = [
    (
        "empty_match_moves_one_character_on",
        lambda: F.regexp_replace("s", r"a??(?!q)", "-"),
        ["-a-a-", "-a-b-", "-a-b-a-b-", "-a-b-c-", "-a-\x85-", "-A-"],
    ),
    (
        "empty_alternative_first",
        lambda: F.regexp_replace("s", r"(?:|a)(?!q)", "-"),
        ["-a-a-", "-a-b-", "-a-b-a-b-", "-a-b-c-", "-a-\x85-", "-A-"],
    ),
    (
        "lazy_group_extract_all",
        lambda: F.regexp_extract_all("s", F.lit(r"(?:ab)??(?!q)"), 0),
        [
            ["", "", ""],
            ["", "", ""],
            ["", "", "", "", ""],
            ["", "", "", ""],
            ["", "", ""],
            ["", ""],
        ],
    ),
    (
        "look_ahead_or_letter",
        lambda: F.regexp_extract_all("s", F.lit(r"(?=.)|b"), 0),
        [["", ""], ["", ""], ["", "", "", ""], ["", "", ""], [""], [""]],
    ),
    (
        "quantified_dollar",
        lambda: F.regexp_extract("s", r"(?=a)a$?b", 0),
        ["", "ab", "ab", "ab", "", ""],
    ),
    (
        "dot_stops_at_next_line",
        lambda: F.col("s").rlike(r"(?=a)a."),
        [True, True, True, True, False, False],
    ),
    (
        "scoped_case_insensitive_group",
        lambda: F.col("s").rlike(r"(?i:a)(?!x)"),
        [True, True, True, True, True, True],
    ),
]
ATOMIC_GROUP = (
    lambda: F.regexp_extract("s", r"(?>a+)b", 0),
    ["ab", "", "", "", "", "", None, "aab", "", ""],
)
INVALID_REPLACEMENTS = ["$x", "a\\", "a$"]
SMILEY = "\U0001f600"
SPARK_NAMES: list[tuple[Callable[[], Column], str]] = [
    (
        lambda: F.regexp_extract("s", r"(\d+)(?=usd)", 1),
        r"regexp_extract(s, (\d+)(?=usd), 1)",
    ),
    (
        lambda: F.regexp_extract_all("s", F.lit(r"(\d+)(?=usd)")),
        r"regexp_extract_all(s, (\d+)(?=usd), 1)",
    ),
    (
        lambda: F.regexp_replace("s", r"\d+(?=usd)", "N"),
        r"regexp_replace(s, \d+(?=usd), N, 1)",
    ),
    (lambda: F.col("s").rlike(r"^(ab)\1$"), r"RLIKE(s, ^(ab)\1$)"),
    (lambda: F.regexp_like("s", F.lit(r"^(?!no).*")), r"REGEXP_LIKE(s, ^(?!no).*)"),
    (
        lambda: F.upper(F.regexp_extract("s", r"(\d+)(?=usd)", 1)),
        r"upper(regexp_extract(s, (\d+)(?=usd), 1))",
    ),
]


@pytest.fixture
def spark() -> Iterator[SparkSession]:
    engine = SailEngine()
    yield engine.start_session()
    engine.stop()


@pytest.mark.parametrize(
    ("pattern", "needs_python"),
    [
        (r"a(?=b)", True),
        (r"a(?!b)", True),
        (r"(?<=a)b", True),
        (r"(?<!a)b", True),
        (r"(a)\1", True),
        (r"(?<n>a)\k<n>", True),
        (r"(?>ab)", True),
        (r"ab\Z", True),
        (r"(?<n>a)b", False),
        (r"[(?=]x", False),
        (r"\(?=x", False),
        (r"\\1", False),
        (r"(\d+)", False),
    ],
)
def test_only_patterns_rust_rejects_go_to_python(
    pattern: str, needs_python: bool
) -> None:
    assert JavaPattern(pattern).needs_python is needs_python


def test_java_syntax_is_translated_to_python() -> None:
    compiled = JavaPattern(r"(?<word>\w+) \k<word>").compiled()
    assert compiled is not None
    assert compiled.search("go go").group("word") == "go"


def test_java_word_classes_stay_ascii() -> None:
    compiled = JavaPattern(r"\w+(?=!)").compiled()
    assert compiled is not None
    assert compiled.search("café!") is None


def test_a_pattern_python_cannot_express_keeps_the_native_path() -> None:
    assert JavaPattern(r"\p{Alpha}+(?=x)").compiled() is None


def test_java_replacement_expands_groups_and_escapes() -> None:
    match = re.search(r"(?P<first>\w+) (\w+)", "John Smith")
    assert match is not None
    assert JavaReplacement(r"$2 ${first} \$1").expand(match) == "Smith John $1"


@pytest.mark.parametrize(
    ("make", "expected"),
    [(make, expected) for _, make, expected in SPARK_RESULTS],
    ids=[name for name, _, _ in SPARK_RESULTS],
)
def test_results_match_real_spark(
    spark: SparkSession, make: Callable[[], Column], expected: list[object]
) -> None:
    frame = spark.createDataFrame(ROWS, "s string")
    assert [row[0] for row in frame.select(make()).collect()] == expected


@pytest.mark.parametrize(
    ("make", "name"), SPARK_NAMES, ids=[name for _, name in SPARK_NAMES]
)
def test_column_names_match_real_spark(
    spark: SparkSession, make: Callable[[], Column], name: str
) -> None:
    frame = spark.createDataFrame(ROWS, "s string")
    assert frame.select(make()).columns == [name]


def test_engine_stop_restores_the_plan_builder() -> None:
    original = vars(UnresolvedFunction)[RegexFallbackPatch.ATTRIBUTE]
    engine = SailEngine()
    try:
        engine.start_session()
        assert vars(UnresolvedFunction)[RegexFallbackPatch.ATTRIBUTE] is not original
    finally:
        engine.stop()
    assert vars(UnresolvedFunction)[RegexFallbackPatch.ATTRIBUTE] is original


@pytest.mark.parametrize(
    ("make", "expected"),
    [(make, expected) for _, make, expected in EDGE_RESULTS],
    ids=[name for name, _, _ in EDGE_RESULTS],
)
def test_edge_cases_match_real_spark(
    spark: SparkSession, make: Callable[[], Column], expected: list[object]
) -> None:
    frame = spark.createDataFrame(EDGE_ROWS, "s string")
    assert [row[0] for row in frame.select(make()).collect()] == expected


@pytest.mark.skipif(sys.version_info < (3, 11), reason="re has atomic groups from 3.11")
def test_atomic_groups_match_real_spark(spark: SparkSession) -> None:
    make, expected = ATOMIC_GROUP
    frame = spark.createDataFrame(EDGE_ROWS, "s string")
    assert [row[0] for row in frame.select(make()).collect()] == expected


@pytest.mark.parametrize("replacement", INVALID_REPLACEMENTS)
def test_an_invalid_java_replacement_still_fails(
    spark: SparkSession, replacement: str
) -> None:
    frame = spark.createDataFrame([("John Smith",)], "s string")
    replaced = frame.select(F.regexp_replace("s", r"(\w+) (?=Smith)", replacement))
    with pytest.raises(PythonException):
        replaced.collect()


def test_a_pattern_rust_accepts_keeps_the_native_plan(spark: SparkSession) -> None:
    native = F.regexp_extract("s", r"(\d+)", 1)._expr
    fallback = F.regexp_extract("s", r"(\d+)(?=usd)", 1)._expr
    assert FallbackInit.PLAN_METHOD not in vars(native)
    assert FallbackInit.PLAN_METHOD in vars(fallback)


@pytest.mark.parametrize(
    "pattern",
    [
        r"[a-z&&[^b]](?=x)",
        r"[[a]b](?=x)",
        r"(?s)a.(?=b)",
        r"(?P<n>a)(?=b)",
        r"\v(?=x)",
        r"[\v](?=x)",
        r"(?<=c)af\b",
        r"a(?i)b(?=c)",
        r"\0101(?=x)",
        r"(?#c)(?=x)",
        r"(?(1)b|c)(?=x)",
        r"a{,3}(?=x)",
        r"a{(?=x)",
        r"\U00000041(?=x)",
        r"(a)\12",
        r"(?=(b))c|a",
        r"(?:(?>(a))x|a)",
        r"(?:(?<=(a))x|b)",
        r"(a)?+x|b(?!c)",
        r"(?:a|(b))*+(?=x)",
        r"\ud83d\ude00(?=x)",
        r"[^\ud83d](?=x)",
    ],
)
def test_patterns_python_would_read_differently_keep_the_native_path(
    pattern: str,
) -> None:
    assert JavaPattern(pattern).compiled() is None


@pytest.mark.parametrize(
    ("make", "expected"),
    [(make, expected) for _, make, expected in JAVA_MATCHING_RESULTS],
    ids=[name for name, _, _ in JAVA_MATCHING_RESULTS],
)
def test_java_matching_rules_match_real_spark(
    spark: SparkSession, make: Callable[[], Column], expected: list[object]
) -> None:
    frame = spark.createDataFrame(MATCHING_ROWS, "s string")
    assert [row[0] for row in frame.select(make()).collect()] == expected


@pytest.mark.parametrize(
    "make",
    [
        lambda: F.regexp_replace("s", r"x*(?!q)", "-"),
        lambda: F.regexp_extract("s", r"(?<=a.)x", 0),
        lambda: F.col("s").rlike(r"(?!^)."),
    ],
    ids=["empty_match_next_to_it", "look_behind_over_it", "match_starting_inside_it"],
)
def test_text_outside_the_bmp_fails_where_java_would_differ(
    spark: SparkSession, make: Callable[[], Column]
) -> None:
    frame = spark.createDataFrame([(f"a{SMILEY}x",)], "s string")
    with pytest.raises(PythonException, match="Basic Multilingual Plane"):
        frame.select(make()).collect()


def test_a_pattern_held_in_a_column_keeps_the_native_plan(
    spark: SparkSession,
) -> None:
    from_column = F.regexp_extract_all("s", F.col("p"), 0)._expr
    assert FallbackInit.PLAN_METHOD not in vars(from_column)


def test_a_replace_with_a_start_position_keeps_the_native_plan(
    spark: SparkSession,
) -> None:
    later_start = F.call_function(
        "regexp_replace", F.col("s"), F.lit(r"a(?=b)"), F.lit("-"), F.lit(2)
    )._expr
    assert FallbackInit.PLAN_METHOD not in vars(later_start)
