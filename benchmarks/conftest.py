"""A session fixture shaped like a real suite's conftest, tuned the way guides advise.

Run as-is for real Spark. With SPARKWARP=1 the same builder lands on Sail, unchanged.
"""

from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.utils import is_remote

from sparkwarp.runtime import Runtime

MASTER = "local[1]"
APP_NAME = "sparkwarp-benchmark"
TUNED_FOR_TESTS: dict[str, str] = {
    "spark.ui.enabled": "false",
    "spark.ui.showConsoleProgress": "false",
    "spark.sql.shuffle.partitions": "1",
    "spark.default.parallelism": "1",
    "spark.sql.adaptive.enabled": "false",
    "spark.sql.codegen.wholeStage": "false",
    "spark.sql.codegen.factoryMode": "NO_CODEGEN",
    "spark.sql.execution.arrow.pyspark.enabled": "false",
    "spark.sql.session.timeZone": "UTC",
    "spark.sql.ansi.enabled": "true",
}


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    builder = SparkSession.builder.master(MASTER).appName(APP_NAME)
    session = builder.config(map=dict(TUNED_FOR_TESTS)).getOrCreate()
    sparkwarp_is_running = Runtime.current() is not None
    assert is_remote() is sparkwarp_is_running, "the benchmark is not on its engine"
    yield session
    session.stop()
