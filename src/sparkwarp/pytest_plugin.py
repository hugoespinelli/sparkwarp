"""pytest plugin: opt-in switch that runs a PySpark suite on Sail with no test edits."""

from __future__ import annotations

import os
from collections.abc import Mapping

import pytest
from pyspark.sql import SparkSession

from sparkwarp.config import (
    ACTIVATION_ENV_VAR,
    CLI_FLAG,
    CLI_FLAG_DEST,
    CLI_FLAG_HELP,
    CLI_GROUP,
    ON_FLAG_VALUES,
    PLUGIN_NAME,
    SPARK_ONLY_MARKER_HELP,
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

    Function-scoped on purpose: each test gets the live session, so a legacy
    `tearDownClass` that calls `spark.stop()` cannot hand later tests a dead one.
    """

    @pytest.fixture
    def spark(self) -> SparkSession:
        return Runtime.start(owner=Ownership.PLUGIN).open_session()


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
