"""Keeps the package's own tests hermetic even when SPARKWARP is set."""

import os

import pytest

from sparkwarp.config import ACTIVATION_ENV_VAR
from sparkwarp.runtime import Runtime

pytest_plugins = ["pytester"]


def pytest_configure(config: pytest.Config) -> None:
    Runtime.shutdown()
    os.environ.pop(ACTIVATION_ENV_VAR, None)
