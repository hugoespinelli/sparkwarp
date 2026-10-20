import json
import os
from collections.abc import Iterator

import pytest
from pyspark import SparkConf
from pyspark.errors import PySparkRuntimeError
from pyspark.sql import SparkSession

from sparkwarp.config import (
    ANSI_KEY,
    CONNECT_MODE_ENV_VAR,
    MASTER_ENV_VAR,
    ORIGINAL_ENV_VAR,
    REMOTE_ENV_VAR,
    SPARK_MASTER_KEY,
    TIMEZONE_KEY,
)
from sparkwarp.environment import (
    ContractedGetOrCreate,
    EnvironmentPatch,
    MasterlessPatch,
    ServerAddress,
    SessionContractPatch,
)

ADDRESS = ServerAddress("127.0.0.1", 1234)
PARTITIONS_KEY = "spark.sql.shuffle.partitions"
CONTRACT = {TIMEZONE_KEY: "UTC", ANSI_KEY: "true"}


def options_of(builder: SparkSession.Builder) -> dict[str, str]:
    return dict(builder._options)


@pytest.fixture(autouse=True)
def no_inherited_record(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(ORIGINAL_ENV_VAR, raising=False)


@pytest.fixture
def connect_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (CONNECT_MODE_ENV_VAR, MASTER_ENV_VAR):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv(REMOTE_ENV_VAR, "sc://127.0.0.1:1")


class FakeGetOrCreate:
    """Records the options a builder carried when getOrCreate was called."""

    def __init__(self) -> None:
        self.seen: dict[str, str] = {}

    def __call__(self, builder: SparkSession.Builder) -> SparkSession | None:
        self.seen = options_of(builder)
        return None


@pytest.fixture
def shim(connect_env: None) -> Iterator[MasterlessPatch]:
    installed = MasterlessPatch()
    installed.install()
    yield installed
    installed.uninstall()


def test_server_address_renders_a_connect_url() -> None:
    assert ADDRESS.url == "sc://127.0.0.1:1234"


def test_connect_patch_sets_vars_and_drops_master(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(MASTER_ENV_VAR, "local[2]")
    monkeypatch.delenv(REMOTE_ENV_VAR, raising=False)
    monkeypatch.delenv(CONNECT_MODE_ENV_VAR, raising=False)
    patch = EnvironmentPatch.connect_to(ADDRESS)
    patch.apply()
    try:
        assert os.environ[CONNECT_MODE_ENV_VAR] == "1"
        assert os.environ[REMOTE_ENV_VAR] == "sc://127.0.0.1:1234"
        assert MASTER_ENV_VAR not in os.environ
    finally:
        patch.restore()


def test_restore_puts_previous_values_back(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(MASTER_ENV_VAR, "local[2]")
    monkeypatch.setenv(REMOTE_ENV_VAR, "sc://elsewhere:1")
    monkeypatch.delenv(CONNECT_MODE_ENV_VAR, raising=False)
    patch = EnvironmentPatch.connect_to(ADDRESS)
    patch.apply()
    patch.restore()
    assert os.environ[MASTER_ENV_VAR] == "local[2]"
    assert os.environ[REMOTE_ENV_VAR] == "sc://elsewhere:1"
    assert CONNECT_MODE_ENV_VAR not in os.environ
    assert not patch.applied


def test_undone_on_restores_user_values_without_touching_the_process(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(MASTER_ENV_VAR, "local[2]")
    monkeypatch.delenv(REMOTE_ENV_VAR, raising=False)
    monkeypatch.delenv(CONNECT_MODE_ENV_VAR, raising=False)
    patch = EnvironmentPatch.connect_to(ADDRESS)
    patch.apply()
    try:
        child_env = patch.undone_on(os.environ)
        assert child_env[MASTER_ENV_VAR] == "local[2]"
        assert REMOTE_ENV_VAR not in child_env
        assert CONNECT_MODE_ENV_VAR not in child_env
        assert os.environ[REMOTE_ENV_VAR] == ADDRESS.url
    finally:
        patch.restore()


def test_a_patch_in_a_child_process_undoes_to_the_grandparent_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = {
        MASTER_ENV_VAR: "local[2]",
        REMOTE_ENV_VAR: None,
        CONNECT_MODE_ENV_VAR: None,
    }
    monkeypatch.setenv(ORIGINAL_ENV_VAR, json.dumps(original))
    monkeypatch.setenv(REMOTE_ENV_VAR, "sc://parent:1")
    monkeypatch.setenv(CONNECT_MODE_ENV_VAR, "1")
    monkeypatch.delenv(MASTER_ENV_VAR, raising=False)
    patch = EnvironmentPatch.connect_to(ADDRESS)
    patch.apply()
    try:
        child_env = patch.undone_on(os.environ)
        assert child_env[MASTER_ENV_VAR] == "local[2]"
        assert REMOTE_ENV_VAR not in child_env
        assert ORIGINAL_ENV_VAR not in child_env
    finally:
        patch.restore()
    assert os.environ[REMOTE_ENV_VAR] == "sc://parent:1"
    assert json.loads(os.environ[ORIGINAL_ENV_VAR]) == original


def test_shim_drops_master_from_master_call(shim: MasterlessPatch) -> None:
    builder = SparkSession.builder.master("local[1]")
    assert SPARK_MASTER_KEY not in options_of(builder)


def test_shim_drops_master_from_key_value(shim: MasterlessPatch) -> None:
    builder = SparkSession.builder.config(SPARK_MASTER_KEY, "local[4]")
    assert SPARK_MASTER_KEY not in options_of(builder)


def test_shim_drops_master_from_map_and_keeps_the_rest(shim: MasterlessPatch) -> None:
    builder = SparkSession.builder.config(
        map={SPARK_MASTER_KEY: "local[1]", PARTITIONS_KEY: "1"}
    )
    assert options_of(builder) == {PARTITIONS_KEY: "1"}


def test_shim_drops_master_from_spark_conf(shim: MasterlessPatch) -> None:
    conf = SparkConf(loadDefaults=False).setMaster("local[1]").set(PARTITIONS_KEY, "1")
    builder = SparkSession.builder.config(conf=conf)
    assert options_of(builder) == {PARTITIONS_KEY: "1"}


def test_shim_drops_remote_so_spark_remote_env_wins(shim: MasterlessPatch) -> None:
    builder = SparkSession.builder.remote("local[1]")
    assert options_of(builder) == {}


def test_shim_accepts_positional_arguments(shim: MasterlessPatch) -> None:
    conf = SparkConf(loadDefaults=False).set(PARTITIONS_KEY, "2")
    builder = SparkSession.builder.config(None, None, conf)
    assert options_of(builder) == {PARTITIONS_KEY: "2"}


def test_uninstalled_shim_restores_pyspark_validation(connect_env: None) -> None:
    shim = MasterlessPatch()
    shim.install()
    shim.uninstall()
    with pytest.raises(PySparkRuntimeError):
        SparkSession.builder.master("local[1]")


def test_patches_install_and_uninstall_twice_and_restore_originals() -> None:
    original_config = SparkSession.Builder.config
    original_get_or_create = SparkSession.Builder.getOrCreate
    patches = (MasterlessPatch(), SessionContractPatch(CONTRACT, lambda: False))
    try:
        for patch in patches:
            patch.install()
            patch.install()
    finally:
        for patch in patches:
            patch.uninstall()
            patch.uninstall()
    assert not any(patch.installed for patch in patches)
    assert SparkSession.Builder.config is original_config
    assert SparkSession.Builder.getOrCreate is original_get_or_create


def test_contract_fills_missing_defaults_for_a_new_session() -> None:
    original = FakeGetOrCreate()
    contracted = ContractedGetOrCreate(original, CONTRACT, lambda: False)
    contracted(SparkSession.builder.config(TIMEZONE_KEY, "Asia/Tokyo"))
    assert original.seen == {TIMEZONE_KEY: "Asia/Tokyo", ANSI_KEY: "true"}


def test_contract_leaves_a_live_session_alone() -> None:
    original = FakeGetOrCreate()
    contracted = ContractedGetOrCreate(original, CONTRACT, lambda: True)
    contracted(SparkSession.builder)
    assert original.seen == {}
