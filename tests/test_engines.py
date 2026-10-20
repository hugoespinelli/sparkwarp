from collections.abc import Iterator

import pytest

from sparkwarp.config import ANSI_KEY, DEFAULT_TIMEZONE, TIMEZONE_KEY
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


def test_session_comes_back_after_user_stops_it(sail: SailEngine) -> None:
    sail.start_session().stop()
    revived = sail.start_session()
    assert revived.range(2).count() == 2
    assert revived.conf.get(TIMEZONE_KEY) == DEFAULT_TIMEZONE


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
