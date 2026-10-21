import os
from collections.abc import Iterator
from pathlib import Path

import pytest

from sparkwarp.config import (
    ANSI_KEY,
    CHECKPOINT_ENV_VAR,
    DEFAULT_TIMEZONE,
    TIMEZONE_KEY,
)
from sparkwarp.engines import SailEngine
from sparkwarp.errors import EngineStoppedError

SAO_PAULO = "America/Sao_Paulo"


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


def test_checkpoint_dir_is_temporary_and_not_inherited(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(CHECKPOINT_ENV_VAR, raising=False)
    sail = SailEngine()
    try:
        sail.start_session()
        checkpoint_dir = sail.checkpoint_dir
        assert checkpoint_dir is not None and checkpoint_dir.is_dir()
        assert CHECKPOINT_ENV_VAR not in os.environ
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
