import json
import os
import subprocess
import sys
import threading
from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession

import sparkwarp
from sparkwarp.config import (
    ACTIVATION_ENV_VAR,
    MASTER_ENV_VAR,
    ORIGINAL_ENV_VAR,
    REMOTE_ENV_VAR,
    TIMEZONE_KEY,
    Ownership,
)
from sparkwarp.engines import SailEngine
from sparkwarp.errors import EngineConflictError, SparkwarpWarning
from sparkwarp.runtime import Runtime

HANG_GUARD_SECONDS = 10.0


class FailingSailEngine(SailEngine):
    """Starts its server, then fails, the way a broken environment would."""

    def prepare_environment(self) -> None:
        super().prepare_environment()
        raise RuntimeError("boom")


@pytest.fixture
def clean_runtime(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.delenv(REMOTE_ENV_VAR, raising=False)
    monkeypatch.delenv(ORIGINAL_ENV_VAR, raising=False)
    Runtime.shutdown()
    yield
    Runtime.shutdown()


def test_session_is_cached_per_process(clean_runtime: None) -> None:
    assert sparkwarp.session() is sparkwarp.session()


def test_stop_does_not_hang_and_clears_the_runtime(clean_runtime: None) -> None:
    assert sparkwarp.session().range(3).count() == 3
    stopper = threading.Thread(target=sparkwarp.stop, daemon=True)
    stopper.start()
    stopper.join(HANG_GUARD_SECONDS)
    assert not stopper.is_alive()
    assert Runtime.current() is None


def test_stop_restores_env_and_builder(clean_runtime: None) -> None:
    original_config = SparkSession.Builder.config
    original_get_or_create = SparkSession.Builder.getOrCreate
    sparkwarp.session()
    assert REMOTE_ENV_VAR in os.environ
    sparkwarp.stop()
    assert REMOTE_ENV_VAR not in os.environ
    assert SparkSession.Builder.config is original_config
    assert SparkSession.Builder.getOrCreate is original_get_or_create


def test_failed_start_cleans_up_and_leaves_no_runtime(
    clean_runtime: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    original_config = SparkSession.Builder.config
    monkeypatch.setattr(Runtime, "ENGINE_CLASS", FailingSailEngine)
    with pytest.raises(RuntimeError, match="boom"):
        sparkwarp.session()
    assert Runtime.current() is None
    assert REMOTE_ENV_VAR not in os.environ
    assert SparkSession.Builder.config is original_config


def test_other_timezone_is_refused(clean_runtime: None) -> None:
    sparkwarp.session()
    with pytest.raises(EngineConflictError, match="Sao_Paulo"):
        sparkwarp.session(timezone="America/Sao_Paulo")


def test_session_without_arguments_joins_the_running_one(clean_runtime: None) -> None:
    first = sparkwarp.session(timezone="America/Sao_Paulo")
    assert sparkwarp.session() is first


def test_user_built_session_gets_the_contract(clean_runtime: None) -> None:
    sparkwarp.session().stop()
    user_session = SparkSession.builder.master("local[1]").getOrCreate()
    assert user_session.conf.get(TIMEZONE_KEY) == "UTC"


def test_plain_get_or_create_keeps_a_conf_the_user_set(clean_runtime: None) -> None:
    spark = sparkwarp.session()
    spark.conf.set(TIMEZONE_KEY, "Asia/Tokyo")
    assert SparkSession.builder.getOrCreate().conf.get(TIMEZONE_KEY) == "Asia/Tokyo"
    assert sparkwarp.session().conf.get(TIMEZONE_KEY) == "Asia/Tokyo"


def test_user_choice_of_timezone_wins(clean_runtime: None) -> None:
    sparkwarp.session().stop()
    builder = SparkSession.builder.config(TIMEZONE_KEY, "Asia/Tokyo")
    assert builder.getOrCreate().conf.get(TIMEZONE_KEY) == "Asia/Tokyo"


def test_library_stop_is_ignored_while_the_plugin_owns_the_engine(
    clean_runtime: None,
) -> None:
    runtime = Runtime.start(owner=Ownership.PLUGIN)
    with pytest.warns(SparkwarpWarning, match="plugin owns"):
        sparkwarp.stop()
    assert Runtime.current() is runtime
    assert runtime.open_session().range(1).count() == 1
    Runtime.release(Ownership.PLUGIN)
    assert Runtime.current() is None


def test_remote_local_builder_lands_on_sail_not_a_jvm(clean_runtime: None) -> None:
    sail_session = sparkwarp.session()
    assert SparkSession.builder.remote("local[1]").getOrCreate() is sail_session


def test_claim_switches_owner_and_returns_the_previous_one(
    clean_runtime: None,
) -> None:
    Runtime.start()
    assert Runtime.claim(Ownership.PLUGIN) is Ownership.LIBRARY
    assert Runtime.claim(Ownership.LIBRARY) is Ownership.PLUGIN


def test_children_inherit_routing_unless_given_the_clean_environ(
    clean_runtime: None,
) -> None:
    sparkwarp.session()
    probe = [
        sys.executable,
        "-c",
        f"import os; print(os.environ.get('{REMOTE_ENV_VAR}'))",
    ]
    inherited = subprocess.run(probe, capture_output=True, text=True, check=True)
    clean = subprocess.run(
        probe, capture_output=True, text=True, check=True, env=sparkwarp.clean_environ()
    )
    assert inherited.stdout.strip().startswith("sc://")
    assert clean.stdout.strip() == "None"


def test_clean_environ_gives_back_the_users_own_values(
    clean_runtime: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(MASTER_ENV_VAR, "local[2]")
    sparkwarp.session()
    child_env = sparkwarp.clean_environ()
    assert child_env[MASTER_ENV_VAR] == "local[2]"
    assert REMOTE_ENV_VAR not in child_env
    assert MASTER_ENV_VAR not in os.environ


CHILD_REPORTS_ITS_CLEAN_ENV = """
import json, sparkwarp
sparkwarp.session()
env = sparkwarp.clean_environ()
print(json.dumps({k: env.get(k) for k in ("MASTER", "SPARK_REMOTE", "SPARKWARP")}))
"""


def test_a_child_of_a_sparkwarp_process_gets_the_original_env(
    clean_runtime: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(MASTER_ENV_VAR, "local[2]")
    monkeypatch.setenv(ACTIVATION_ENV_VAR, "1")
    sparkwarp.session()
    child = subprocess.run(
        [sys.executable, "-c", CHILD_REPORTS_ITS_CLEAN_ENV],
        capture_output=True,
        text=True,
        check=True,
        timeout=HANG_GUARD_SECONDS * 6,
    )
    reported = json.loads(child.stdout.strip().splitlines()[-1])
    assert reported == {"MASTER": "local[2]", "SPARK_REMOTE": None, "SPARKWARP": None}


def test_stop_without_start_is_a_no_op(clean_runtime: None) -> None:
    sparkwarp.stop()
    sparkwarp.stop()
    assert Runtime.current() is None
