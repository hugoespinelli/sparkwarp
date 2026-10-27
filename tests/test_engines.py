import os
from collections.abc import Iterator
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from sparkwarp.config import (
    ANSI_KEY,
    CHECKPOINT_ENV_VAR,
    DEFAULT_TIMEZONE,
    PARALLELISM_ENV_VAR,
    SAIL_TEST_PARALLELISM,
    STACK_SIZE_ENV_VAR,
    TIMEZONE_KEY,
)
from sparkwarp.engines import SailEngine, ServerSettings
from sparkwarp.errors import EngineStoppedError

SAO_PAULO = "America/Sao_Paulo"
USER_PARALLELISM = "4"
GROUP_KEYS = 40
GROUPED_ROWS = 80
KEY_SCHEMA = "k int"
PARTITION_ALIAS = "p"


class PartitionsUsed:
    """How many partitions Sail spread a small grouped result over."""

    @staticmethod
    def by_a_grouped_count(spark: SparkSession) -> int:
        rows = [(i % GROUP_KEYS,) for i in range(GROUPED_ROWS)]
        counted = spark.createDataFrame(rows, KEY_SCHEMA).groupBy("k").count()
        partition_ids = F.spark_partition_id().alias(PARTITION_ALIAS)
        partitions = counted.select(partition_ids).distinct()
        return partitions.count()


@pytest.fixture
def sail() -> Iterator[SailEngine]:
    engine = SailEngine()
    yield engine
    engine.stop()


def test_session_pins_utc_and_ansi(sail: SailEngine) -> None:
    spark = sail.start_session()
    assert spark.conf.get(TIMEZONE_KEY) == DEFAULT_TIMEZONE
    assert spark.conf.get(ANSI_KEY) == "true"


def test_timezone_override() -> None:
    engine = SailEngine(timezone=SAO_PAULO)
    try:
        assert engine.start_session().conf.get(TIMEZONE_KEY) == SAO_PAULO
    finally:
        engine.stop()


def test_runs_a_query(sail: SailEngine) -> None:
    assert sail.start_session().range(5).count() == 5


def test_user_stop_is_held_and_the_session_stays_usable(sail: SailEngine) -> None:
    session = sail.start_session()
    session.stop()
    assert sail.start_session() is session
    assert session.range(2).count() == 2
    assert session.conf.get(TIMEZONE_KEY) == DEFAULT_TIMEZONE


def test_scoped_server_settings_are_temporary_and_not_inherited(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(CHECKPOINT_ENV_VAR, raising=False)
    monkeypatch.delenv(STACK_SIZE_ENV_VAR, raising=False)
    sail = SailEngine()
    try:
        sail.start_session()
        checkpoint_dir = sail.checkpoint_dir
        assert checkpoint_dir is not None and checkpoint_dir.is_dir()
        assert CHECKPOINT_ENV_VAR not in os.environ
        assert STACK_SIZE_ENV_VAR not in os.environ
        assert PARALLELISM_ENV_VAR not in os.environ
    finally:
        sail.stop()
    assert not checkpoint_dir.exists()


def test_a_checkpoint_path_the_user_set_wins(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv(CHECKPOINT_ENV_VAR, str(tmp_path))
    engine = SailEngine()
    try:
        session = engine.start_session()
        assert engine.checkpoint_dir is None
        assert session.range(3).localCheckpoint(eager=True).count() == 3
    finally:
        engine.stop()
    assert any(tmp_path.iterdir())


def test_stop_twice_is_safe_and_the_engine_is_spent(sail: SailEngine) -> None:
    sail.start_session()
    sail.stop()
    sail.stop()
    assert sail.stopped
    assert not sail.prepared


def test_stopped_engine_refuses_to_start_again(sail: SailEngine) -> None:
    sail.stop()
    with pytest.raises(EngineStoppedError):
        sail.start_session()


def test_sail_runs_one_partition_per_plan_node(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(PARALLELISM_ENV_VAR, raising=False)
    engine = SailEngine()
    try:
        spark = engine.start_session()
        assert PartitionsUsed.by_a_grouped_count(spark) == SAIL_TEST_PARALLELISM
    finally:
        engine.stop()


def test_a_parallelism_the_user_set_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(PARALLELISM_ENV_VAR, USER_PARALLELISM)
    assert PARALLELISM_ENV_VAR not in ServerSettings(checkpoint_path=None).scoped_vars()
    engine = SailEngine()
    try:
        spark = engine.start_session()
        assert PartitionsUsed.by_a_grouped_count(spark) > SAIL_TEST_PARALLELISM
    finally:
        engine.stop()
