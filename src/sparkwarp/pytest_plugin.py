"""pytest plugin: opt-in switch that runs a PySpark suite on Sail with no test edits."""

from __future__ import annotations

import os
from collections.abc import Generator, Mapping

import pytest
from pyspark.errors import PySparkException
from pyspark.sql import SparkSession

from sparkwarp.config import (
    ACTIVATION_ENV_VAR,
    CLI_FLAG,
    CLI_FLAG_DEST,
    CLI_FLAG_HELP,
    CLI_GROUP,
    HINT_SECTION_TITLE,
    ON_FLAG_VALUES,
    PLUGIN_NAME,
    SPARK_ONLY_MARKER,
    SPARK_ONLY_MARKER_HELP,
    SPARK_ONLY_SKIP_REASON,
    UNSUPPORTED_CONDITIONS,
    UNSUPPORTED_ERROR_CLASSES,
    UNSUPPORTED_HINT,
    Ownership,
)
from sparkwarp.runtime import Runtime


class Activation:
    """Decides whether this run uses sparkwarp. The CLI flag beats the env var."""

    def __init__(self, flag: bool, environ: Mapping[str, str]) -> None:
        self._flag = flag
        self._environ = environ

    @property
    def active(self) -> bool:
        switch = self._environ.get(ACTIVATION_ENV_VAR, "").strip().lower()
        return self._flag or switch in ON_FLAG_VALUES


class EngineLease:
    """The plugin's hold on the engine for one pytest run.

    A run that starts the engine stops it; a run that joins one hands it back to its
    previous owner. `release` is registered as a config cleanup, which pytest runs
    even when the run fails before `pytest_configure` (usage or conftest errors).
    """

    def __init__(self) -> None:
        joins_existing_engine = Runtime.current() is not None
        Runtime.start(owner=Ownership.PLUGIN)
        self._started_engine = not joins_existing_engine
        self._previous_owner = Runtime.claim(Ownership.PLUGIN)

    def release(self) -> None:
        if self._started_engine:
            Runtime.release(Ownership.PLUGIN)
        elif self._previous_owner is not None:
            Runtime.claim(self._previous_owner)


class SparkwarpSession:
    """Registered only when active: an inactive install provides no `spark` fixture.

    Function-scoped on purpose: each test asks the running engine for its live session
    instead of keeping a reference that could go stale.
    """

    @pytest.fixture
    def spark(self) -> SparkSession:
        return Runtime.start(owner=Ownership.PLUGIN).open_session()

    def pytest_collection_modifyitems(self, items: list[pytest.Item]) -> None:
        skip = pytest.mark.skip(reason=SPARK_ONLY_SKIP_REASON)
        for item in items:
            needs_real_spark = item.get_closest_marker(SPARK_ONLY_MARKER) is not None
            if needs_real_spark:
                item.add_marker(skip)

    def pytest_terminal_summary(
        self, terminalreporter: pytest.TerminalReporter
    ) -> None:
        runtime = Runtime.current()
        if runtime is None:
            return
        line = runtime.engine.routing_summary().line()
        if line is not None:
            terminalreporter.write_line(line)

    @pytest.hookimpl(wrapper=True)
    def pytest_runtest_makereport(
        self, call: pytest.CallInfo[None]
    ) -> Generator[None, pytest.TestReport, pytest.TestReport]:
        report = yield
        failed_on_unsupported = report.failed and UnsupportedFeature.raised_in(call)
        if failed_on_unsupported:
            report.sections.append((HINT_SECTION_TITLE, UNSUPPORTED_HINT))
        return report


class UnsupportedFeature:
    """Recognises PySpark's own "this feature is not supported" errors, nothing else.

    Matching on message text would also catch ordinary bugs ("'<' not supported
    between ..."), so only PySpark exception types and error conditions count.
    """

    @classmethod
    def raised_in(cls, call: pytest.CallInfo[None]) -> bool:
        if call.excinfo is None:
            return False
        return any(
            cls.is_unsupported(error) for error in cls._chain(call.excinfo.value)
        )

    @staticmethod
    def is_unsupported(error: BaseException) -> bool:
        if not isinstance(error, PySparkException):
            return False
        known_condition = error.getCondition() in UNSUPPORTED_CONDITIONS
        known_class = type(error).__name__ in UNSUPPORTED_ERROR_CLASSES
        return known_condition or known_class

    @staticmethod
    def _chain(error: BaseException) -> list[BaseException]:
        """The error and everything it was raised from, so wrapped errors count too."""
        chain: list[BaseException] = []
        current: BaseException | None = error
        while current is not None and current not in chain:
            chain.append(current)
            hidden_context = current.__suppress_context__
            implicit = None if hidden_context else current.__context__
            current = current.__cause__ or implicit
        return chain


def pytest_addoption(parser: pytest.Parser) -> None:
    group = parser.getgroup(CLI_GROUP)
    group.addoption(
        CLI_FLAG, dest=CLI_FLAG_DEST, action="store_true", help=CLI_FLAG_HELP
    )


def pytest_load_initial_conftests(early_config: pytest.Config) -> None:
    """Runs at normal priority: after env-setting plugins such as pytest-env (tryfirst),
    before pytest loads conftest files (trylast), so user conftests see the routing."""
    flag = bool(getattr(early_config.known_args_namespace, CLI_FLAG_DEST, False))
    if not Activation(flag, os.environ).active:
        return
    lease = EngineLease()
    early_config.add_cleanup(lease.release)
    early_config.pluginmanager.register(SparkwarpSession(), PLUGIN_NAME)


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", SPARK_ONLY_MARKER_HELP)
