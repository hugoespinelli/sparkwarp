"""Spark Connect compatibility shims: common conftest lines keep working."""

import os
import threading
from collections.abc import Iterator

import pytest
from pyspark.errors import PySparkAttributeError
from pyspark.sql import SparkSession
from pyspark.sql.connect.catalog import Catalog as ConnectCatalog
from pyspark.sql.connect.client.reattach import (
    ExecutePlanResponseReattachableIterator as ReattachIterator,
)
from pyspark.sql.connect.session import SparkSession as ConnectSession

from sparkwarp.config import JVM_ONLY_CONDITION, REMOTE_ENV_VAR, TIMEZONE_KEY
from sparkwarp.engines import SailEngine

PYSPARK_SHARES_ONE_RELEASE_POOL = "shutdown" in vars(ReattachIterator)
WORKER_TIMEOUT_SECONDS = 30


@pytest.fixture
def spark() -> Iterator[SparkSession]:
    engine = SailEngine()
    yield engine.start_session()
    engine.stop()


def test_set_log_level_is_accepted(spark: SparkSession) -> None:
    assert spark.sparkContext.setLogLevel("WARN") is None


def test_stub_answers_the_common_read_only_questions(spark: SparkSession) -> None:
    context = spark.sparkContext
    assert context.defaultParallelism == 1
    assert context.version == spark.version
    assert isinstance(context.applicationId, str)


def test_other_spark_context_attributes_still_fail_loudly(spark: SparkSession) -> None:
    with pytest.raises(PySparkAttributeError) as raised:
        spark.sparkContext.wholeTextFiles("/tmp")
    assert raised.value.getCondition() == JVM_ONLY_CONDITION


def test_clear_cache_is_a_no_op(spark: SparkSession) -> None:
    assert spark.catalog.clearCache() is None


def test_user_stop_keeps_the_session_usable(spark: SparkSession) -> None:
    spark.stop()
    assert not spark.is_stopped
    assert spark.range(2).count() == 2


def test_local_checkpoint_works(spark: SparkSession) -> None:
    assert spark.range(5).localCheckpoint(eager=True).count() == 5


def test_engine_stop_removes_every_shim() -> None:
    original_stop = ConnectSession.stop
    original_clear_cache = ConnectCatalog.clearCache
    original_shutdown = vars(ReattachIterator).get("shutdown")
    engine = SailEngine()
    try:
        engine.start_session()
    finally:
        engine.stop()
    assert ConnectSession.stop is original_stop
    assert ConnectCatalog.clearCache is original_clear_cache
    assert vars(ReattachIterator).get("shutdown") is original_shutdown
    assert "sparkContext" not in vars(ConnectSession)


def test_a_session_built_separately_is_really_stopped(spark: SparkSession) -> None:
    separate = ConnectSession.builder.remote(os.environ[REMOTE_ENV_VAR]).create()
    assert separate.range(1).count() == 1
    separate.stop()
    assert separate.is_stopped
    assert not spark.is_stopped


def test_held_stop_keeps_session_state_as_documented(spark: SparkSession) -> None:
    spark.conf.set(TIMEZONE_KEY, "America/New_York")
    spark.range(1).createOrReplaceTempView("carried_over")
    spark.stop()
    assert spark.conf.get(TIMEZONE_KEY) == "America/New_York"
    assert spark.catalog.tableExists("carried_over")


@pytest.mark.skipif(
    not PYSPARK_SHARES_ONE_RELEASE_POOL,
    reason="PySpark 4.2+ shuts the pool down if idle",
)
def test_a_separate_session_stop_keeps_the_shared_release_pool() -> None:
    engine = SailEngine()
    try:
        engine.start_session().range(3).collect()
        pool = ReattachIterator._release_thread_pool_instance
        assert pool is not None
        separate = ConnectSession.builder.remote(os.environ[REMOTE_ENV_VAR]).create()
        separate.range(1).collect()
        separate.stop()
        assert ReattachIterator._release_thread_pool_instance is pool
    finally:
        engine.stop()
    assert ReattachIterator._release_thread_pool_instance is None


def test_a_session_a_worker_thread_builds_is_really_stopped(
    spark: SparkSession,
) -> None:
    built: list[ConnectSession] = []
    worker = threading.Thread(target=WorkerSession(built).run)
    worker.start()
    worker.join(WORKER_TIMEOUT_SECONDS)
    assert built and built[0].is_stopped
    assert not spark.is_stopped


class WorkerSession:
    """Builds a separate session in its own thread, uses it, and stops it."""

    def __init__(self, built: list[ConnectSession]) -> None:
        self._built = built

    def run(self) -> None:
        session = ConnectSession.builder.remote(os.environ[REMOTE_ENV_VAR]).create()
        session.range(1).collect()
        session.stop()
        self._built.append(session)
