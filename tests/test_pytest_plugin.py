import os
import re
from collections.abc import Iterator

import pytest

from sparkwarp.config import (
    ACTIVATION_ENV_VAR,
    CONNECT_MODE_ENV_VAR,
    MASTER_ENV_VAR,
    REMOTE_ENV_VAR,
    Ownership,
)
from sparkwarp.runtime import Runtime

SUBPROCESS_TIMEOUT_SECONDS = 120


@pytest.fixture(autouse=True)
def isolated_environment(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    for name in (
        ACTIVATION_ENV_VAR,
        CONNECT_MODE_ENV_VAR,
        REMOTE_ENV_VAR,
        MASTER_ENV_VAR,
    ):
        monkeypatch.delenv(name, raising=False)
    yield


FIXTURE_TEST = """
from pyspark.sql import functions as F
from pyspark.sql.utils import is_remote

def test_uses_the_plugin_fixture(spark):
    assert is_remote()
    df = spark.createDataFrame([(1, "a"), (2, "b")], "id int, name string")
    assert df.filter(F.col("id") > 1).count() == 1
"""

USER_CONFTEST = """
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("user-suite")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
"""

UNITTEST_SHAPE = """
import unittest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

class PySparkTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[1]").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

class TestShape(PySparkTestCase):
    def test_filter(self):
        df = self.spark.createDataFrame([(1,), (2,)], "id int")
        self.assertEqual(df.filter(F.col("id") > 1).count(), 1)

    def test_contract(self):
        from pyspark.sql.utils import is_remote
        self.assertTrue(is_remote())
        self.assertEqual(self.spark.conf.get("spark.sql.session.timeZone"), "UTC")
"""

CONTRACT_TEST = """
def test_user_session_gets_utc_and_ansi(spark):
    from pyspark.sql.utils import is_remote
    assert is_remote()
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"
    assert spark.conf.get("spark.sql.ansi.enabled") == "true"
"""


STOPS_IN_TEARDOWN = """
import pytest
import sparkwarp

@pytest.fixture(scope="module", autouse=True)
def stop_at_module_end():
    yield
    sparkwarp.stop()

def test_first_module(spark):
    assert spark.range(2).count() == 2
"""

USES_FIXTURE_LATER = """
def test_second_module(spark):
    assert spark.range(3).count() == 3
"""


def test_cli_flag_provides_the_spark_fixture(pytester: pytest.Pytester) -> None:
    pytester.makepyfile(FIXTURE_TEST)
    result = pytester.runpytest_subprocess(
        "--sparkwarp", "-p", "no:cacheprovider", timeout=SUBPROCESS_TIMEOUT_SECONDS
    )
    result.assert_outcomes(passed=1)


def test_cli_flag_followed_by_a_path_keeps_the_path(pytester: pytest.Pytester) -> None:
    test_file = pytester.makepyfile(FIXTURE_TEST)
    result = pytester.runpytest_subprocess(
        "--sparkwarp",
        str(test_file),
        "-p",
        "no:cacheprovider",
        timeout=SUBPROCESS_TIMEOUT_SECONDS,
    )
    result.assert_outcomes(passed=1)


def test_env_flag_runs_a_user_conftest_unchanged(
    pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(ACTIVATION_ENV_VAR, "1")
    pytester.makeconftest(USER_CONFTEST)
    pytester.makepyfile(test_fixture=FIXTURE_TEST, test_contract=CONTRACT_TEST)
    result = pytester.runpytest_subprocess(
        "-p", "no:cacheprovider", timeout=SUBPROCESS_TIMEOUT_SECONDS
    )
    result.assert_outcomes(passed=2)


def test_env_flag_runs_the_unittest_shape_unchanged(
    pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(ACTIVATION_ENV_VAR, "1")
    pytester.makepyfile(UNITTEST_SHAPE)
    result = pytester.runpytest_subprocess(
        "-p", "no:cacheprovider", timeout=SUBPROCESS_TIMEOUT_SECONDS
    )
    result.assert_outcomes(passed=2)


def test_user_stop_inside_a_plugin_run_is_ignored_with_a_warning(
    pytester: pytest.Pytester,
) -> None:
    pytester.makepyfile(test_a=STOPS_IN_TEARDOWN, test_b=USES_FIXTURE_LATER)
    result = pytester.runpytest_subprocess(
        "--sparkwarp", "-p", "no:cacheprovider", timeout=SUBPROCESS_TIMEOUT_SECONDS
    )
    result.assert_outcomes(passed=2)
    result.stdout.fnmatch_lines(["*SparkwarpWarning*plugin owns*"])


def test_plugin_is_inert_without_activation(pytester: pytest.Pytester) -> None:
    pytester.makepyfile(FIXTURE_TEST)
    result = pytester.runpytest_subprocess(
        "-p", "no:cacheprovider", timeout=SUBPROCESS_TIMEOUT_SECONDS
    )
    result.assert_outcomes(errors=1)
    result.stdout.fnmatch_lines(["*fixture 'spark' not found*"])


def test_explicit_off_is_inert(
    pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(ACTIVATION_ENV_VAR, "0")
    pytester.makepyfile(FIXTURE_TEST)
    result = pytester.runpytest_subprocess(
        "-p", "no:cacheprovider", timeout=SUBPROCESS_TIMEOUT_SECONDS
    )
    result.assert_outcomes(errors=1)


NESTED_CONFTEST = """
pytest_plugins = ["pytester"]
"""

NESTED_RUNS = '''
INNER = """
def test_inner(spark):
    assert spark.range(1).count() == 1
"""


def test_a_runs_an_inner_pytest_in_process(pytester, spark):
    assert spark.range(2).count() == 2
    pytester.makepyfile(INNER)
    result = pytester.runpytest("--sparkwarp", "-p", "no:cacheprovider")
    result.assert_outcomes(passed=1)


def test_b_outer_engine_survives(spark):
    assert spark.range(4).count() == 4
'''


def test_nested_in_process_run_does_not_stop_the_outer_engine(
    pytester: pytest.Pytester,
) -> None:
    pytester.makeconftest(NESTED_CONFTEST)
    pytester.makepyfile(NESTED_RUNS)
    result = pytester.runpytest_subprocess(
        "--sparkwarp", "-p", "no:cacheprovider", timeout=SUBPROCESS_TIMEOUT_SECONDS
    )
    result.assert_outcomes(passed=2)


JOINED_RUN = """
import sparkwarp


def test_a_user_stop_is_ignored(spark):
    assert spark.range(1).count() == 1
    sparkwarp.stop()


def test_b_session_still_works(spark):
    assert spark.range(2).count() == 2
"""


def test_plugin_claims_an_engine_a_script_started(pytester: pytest.Pytester) -> None:
    library_runtime = Runtime.start()
    try:
        pytester.makepyfile(JOINED_RUN)
        result = pytester.runpytest("--sparkwarp", "-p", "no:cacheprovider")
        result.assert_outcomes(passed=2)
        result.stdout.fnmatch_lines(["*SparkwarpWarning*plugin owns*"])
        assert Runtime.current() is library_runtime
        assert library_runtime.owner is Ownership.LIBRARY
    finally:
        Runtime.shutdown()


LEGACY_CLASS_STOPS_ITS_SESSION = """
import unittest

from pyspark.sql import SparkSession


def test_a_before(spark):
    assert spark.range(1).count() == 1


class TestLegacy(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[1]").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_b_legacy(self):
        self.assertEqual(self.spark.range(2).count(), 2)


def test_c_after(spark):
    assert spark.range(3).count() == 3
"""


def test_fixture_survives_a_legacy_class_that_stops_the_session(
    pytester: pytest.Pytester,
) -> None:
    pytester.makepyfile(LEGACY_CLASS_STOPS_ITS_SESSION)
    result = pytester.runpytest_subprocess(
        "--sparkwarp", "-p", "no:cacheprovider", timeout=SUBPROCESS_TIMEOUT_SECONDS
    )
    result.assert_outcomes(passed=3)


BROKEN_CONFTEST = """
import no_such_module_anywhere
"""


@pytest.mark.parametrize(
    ("extra_args", "conftest"),
    [(["--no-such-flag"], None), ([], BROKEN_CONFTEST)],
    ids=["usage-error", "conftest-import-error"],
)
def test_an_aborted_in_process_run_leaves_nothing_behind(
    pytester: pytest.Pytester, extra_args: list[str], conftest: str | None
) -> None:
    Runtime.shutdown()
    if conftest is not None:
        pytester.makeconftest(conftest)
    pytester.makepyfile(FIXTURE_TEST)
    result = pytester.runpytest("--sparkwarp", *extra_args)
    assert result.ret != 0
    assert Runtime.current() is None
    assert REMOTE_ENV_VAR not in os.environ


ENV_SETTING_PLUGIN = """
import os

import pytest


@pytest.hookimpl(tryfirst=True)
def pytest_load_initial_conftests(early_config):
    os.environ["SPARKWARP"] = "1"
"""


def test_activation_set_by_an_env_plugin_is_seen(pytester: pytest.Pytester) -> None:
    pytester.makepyfile(envsetter=ENV_SETTING_PLUGIN, test_fixture=FIXTURE_TEST)
    result = pytester.runpytest_subprocess(
        "-p", "envsetter", "-p", "no:cacheprovider", timeout=SUBPROCESS_TIMEOUT_SECONDS
    )
    result.assert_outcomes(passed=1)


SPARK_ONLY_SUITE = """
import pytest



@pytest.mark.spark_only
def test_marked_function(spark):
    raise AssertionError("must not run on Sail")


@pytest.mark.spark_only
class TestMarkedClass:
    def test_inside(self, spark):
        raise AssertionError("must not run on Sail")


def test_unmarked(spark):
    assert spark.range(1).count() == 1
"""

SPARK_ONLY_MODULE = """
import pytest

pytestmark = pytest.mark.spark_only


def test_in_marked_module(spark):
    raise AssertionError("must not run on Sail")
"""


def test_spark_only_tests_are_skipped_on_sail(pytester: pytest.Pytester) -> None:
    pytester.makepyfile(test_marks=SPARK_ONLY_SUITE, test_module=SPARK_ONLY_MODULE)
    result = pytester.runpytest_subprocess(
        "--sparkwarp",
        "-rs",
        "-p",
        "no:cacheprovider",
        timeout=SUBPROCESS_TIMEOUT_SECONDS,
    )
    result.assert_outcomes(passed=1, skipped=3)
    result.stdout.fnmatch_lines(["*spark_only: needs real Spark*SPARKWARP=0*"])


def test_spark_only_tests_run_when_sparkwarp_is_off(pytester: pytest.Pytester) -> None:
    pytester.makepyfile(
        SPARK_ONLY_MODULE.replace(
            'raise AssertionError("must not run on Sail")', "assert True"
        ).replace("(spark)", "()")
    )
    result = pytester.runpytest_subprocess(
        "-p", "no:cacheprovider", timeout=SUBPROCESS_TIMEOUT_SECONDS
    )
    result.assert_outcomes(passed=1)


UNSUPPORTED_FEATURE = """
def test_uses_rdd(spark):
    spark.range(3).rdd.count()


def test_uses_spark_context(spark):
    spark.sparkContext.parallelize([1, 2])


def test_plain_failure(spark):
    assert spark.range(1).count() == 2


def test_python_bug_with_not_supported_message(spark):
    sorted([r.v for r in spark.createDataFrame([(1,), (None,)], "v int").collect()])


def test_user_error_with_not_supported_message(spark):
    raise ValueError("currency EUR not supported yet")


def test_wrapped_unsupported_error(spark):
    try:
        spark.range(1).rdd.count()
    except Exception as error:
        raise RuntimeError("the job failed") from error
"""


def test_hint_only_for_unsupported_pyspark_features(pytester: pytest.Pytester) -> None:
    pytester.makepyfile(UNSUPPORTED_FEATURE)
    result = pytester.runpytest_subprocess(
        "--sparkwarp", "-p", "no:cacheprovider", timeout=SUBPROCESS_TIMEOUT_SECONDS
    )
    result.assert_outcomes(failed=6)
    hinted = UnsupportedHints(result.stdout.lines).tests_with_hint()
    expected = {
        "test_uses_rdd",
        "test_uses_spark_context",
        "test_wrapped_unsupported_error",
    }
    assert hinted == expected


class UnsupportedHints:
    """Maps each failure section in pytest's output to whether it carries the hint."""

    FAILURE_HEADER = re.compile(r"^_+ (test_\w+) _+$")
    HINT_TEXT = "hit a feature that Spark Connect or Sail does not support"

    def __init__(self, lines: list[str]) -> None:
        self._lines = lines

    def tests_with_hint(self) -> set[str]:
        hinted: set[str] = set()
        current: str | None = None
        for line in self._lines:
            header = self.FAILURE_HEADER.match(line)
            if header:
                current = header.group(1)
            elif current and self.HINT_TEXT in line:
                hinted.add(current)
        return hinted


SESSION_CACHED_AT_IMPORT = """
import os

from pyspark.sql.connect.session import SparkSession as ConnectSession

CACHED = ConnectSession.builder.remote(os.environ["SPARK_REMOTE"]).getOrCreate()
"""

STOPS_THEN_USES_THE_CACHED_SESSION = """
import unittest

from pyspark.sql import SparkSession

import cachedlib


class TestStopsInTeardown(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[1]").getOrCreate()
        cls.spark.sparkContext.setLogLevel("WARN")

    @classmethod
    def tearDownClass(cls):
        cls.spark.catalog.clearCache()
        cls.spark.stop()

    def test_a(self):
        self.assertEqual(self.spark.range(1).count(), 1)


def test_b_library_session_still_works():
    assert cachedlib.CACHED.range(2).count() == 2
"""


def test_common_conftest_lines_and_a_cached_session_work(
    pytester: pytest.Pytester,
) -> None:
    pytester.makepyfile(cachedlib=SESSION_CACHED_AT_IMPORT)
    pytester.makepyfile(test_cached=STOPS_THEN_USES_THE_CACHED_SESSION)
    result = pytester.runpytest_subprocess(
        "--sparkwarp", "-p", "no:cacheprovider", timeout=SUBPROCESS_TIMEOUT_SECONDS
    )
    result.assert_outcomes(passed=2)


DEEP_EXPRESSION = """
from pyspark.sql import functions as F


def test_deep_chain(spark):
    column = F.col("s")
    for _ in range(100):
        column = F.regexp_replace(column, "a", "b")
    spark.createDataFrame([("a",)], "s string").select(column).collect()
"""


def test_a_column_100_calls_deep_runs_in_a_plugin_run(
    pytester: pytest.Pytester,
) -> None:
    pytester.makepyfile(DEEP_EXPRESSION)
    result = pytester.runpytest_subprocess(
        "--sparkwarp", "-p", "no:cacheprovider", timeout=SUBPROCESS_TIMEOUT_SECONDS
    )
    result.assert_outcomes(passed=1)
