"""Corpus fixtures. The session is built the way a user conftest builds it, so the same
corpus runs on real Spark (sparkwarp off) and on Sail (SPARKWARP=1)."""

from __future__ import annotations

import os
import time
from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession

from corpus.harness import GoldenStore, ParityChecker

UTC = "UTC"
os.environ["TZ"] = UTC
time.tzset()
os.environ["SPARKWARP_JVM"] = "0"
os.environ["SPARKWARP_JVM_DEPTH"] = "0"

TUNED_FOR_TESTS: dict[str, str] = {
    "spark.ui.enabled": "false",
    "spark.ui.showConsoleProgress": "false",
    "spark.sql.shuffle.partitions": "1",
    "spark.default.parallelism": "1",
    "spark.sql.adaptive.enabled": "false",
    "spark.sql.session.timeZone": UTC,
    "spark.sql.ansi.enabled": "true",
}


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    builder = SparkSession.builder.master("local[1]").appName("sparkwarp-corpus")
    session = builder.config(map=dict(TUNED_FOR_TESTS)).getOrCreate()
    yield session
    session.stop()


@pytest.fixture(scope="session")
def golden_store() -> Iterator[GoldenStore]:
    store = GoldenStore()
    yield store
    store.flush()


@pytest.fixture
def parity(request: pytest.FixtureRequest, golden_store: GoldenStore) -> ParityChecker:
    module = request.module.__name__.rsplit(".", 1)[-1]
    return ParityChecker(golden_store, module, request.node.name)
