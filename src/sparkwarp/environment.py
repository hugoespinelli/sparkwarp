"""Process-level switches that send PySpark's own imports to a Spark Connect server."""

from __future__ import annotations

import json
import os
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from types import MethodType
from typing import TYPE_CHECKING, cast

from pyspark import SparkConf
from pyspark.sql import SparkSession

from sparkwarp.config import (
    ACTIVATION_ENV_VAR,
    CLUSTER_URL_KEYS,
    CONNECT_MODE_ENV_VAR,
    CONNECT_MODE_ON,
    CONNECT_URL_SCHEME,
    MASTER_ENV_VAR,
    ORIGINAL_ENV_VAR,
    REMOTE_ENV_VAR,
)
from sparkwarp.patches import ClassPatch

if TYPE_CHECKING:
    from pyspark.sql._typing import OptionalPrimitiveType

SPARKWARP_OWN_VARS = frozenset({ACTIVATION_ENV_VAR, ORIGINAL_ENV_VAR})
BuilderConfig = Callable[..., SparkSession.Builder]
BuilderGetOrCreate = Callable[[SparkSession.Builder], SparkSession]


@dataclass(frozen=True)
class ServerAddress:
    """Host and port of a running Spark Connect server."""

    host: str
    port: int

    @property
    def url(self) -> str:
        return f"{CONNECT_URL_SCHEME}{self.host}:{self.port}"


@dataclass
class EnvironmentPatch:
    """Sets and unsets env vars, and puts every touched var back on restore.

    The first patch in a process tree records the untouched values in
    SPARKWARP_ORIGINAL_ENV. Child processes inherit that record, so `undone_on` gives
    the original environment even in a process that was born already patched.
    """

    set_vars: Mapping[str, str] = field(default_factory=dict)
    unset_vars: tuple[str, ...] = ()
    _saved: dict[str, str | None] | None = field(default=None, init=False)
    _original: dict[str, str | None] = field(default_factory=dict, init=False)

    @classmethod
    def connect_to(cls, address: ServerAddress) -> EnvironmentPatch:
        connect_vars = {
            CONNECT_MODE_ENV_VAR: CONNECT_MODE_ON,
            REMOTE_ENV_VAR: address.url,
        }
        return cls(set_vars=connect_vars, unset_vars=(MASTER_ENV_VAR,))

    @classmethod
    def inherited(cls) -> EnvironmentPatch:
        """An unapplied patch that knows the original env recorded by a parent."""
        patch = cls()
        patch._original = cls._inherited_original() or {}
        return patch

    @property
    def applied(self) -> bool:
        return self._saved is not None

    @property
    def touched(self) -> tuple[str, ...]:
        return (*self.set_vars, *self.unset_vars)

    def apply(self) -> None:
        if self.applied:
            return
        self._saved = {
            name: os.environ.get(name) for name in (*self.touched, ORIGINAL_ENV_VAR)
        }
        self._original = self._inherited_original() or {
            name: os.environ.get(name) for name in self.touched
        }
        os.environ.update(self.set_vars)
        for name in self.unset_vars:
            os.environ.pop(name, None)
        os.environ[ORIGINAL_ENV_VAR] = json.dumps(self._original)

    def restore(self) -> None:
        if self._saved is None:
            return
        for name, previous in self._saved.items():
            if previous is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = previous
        self._saved = None

    def undone_on(self, environ: Mapping[str, str]) -> dict[str, str]:
        """A copy of `environ` as it was before any sparkwarp patch in this tree."""
        before = {k: v for k, v in environ.items() if k not in SPARKWARP_OWN_VARS}
        for name, original in self._original.items():
            if original is None:
                before.pop(name, None)
            else:
                before[name] = original
        return before

    @staticmethod
    def _inherited_original() -> dict[str, str | None] | None:
        recorded = os.environ.get(ORIGINAL_ENV_VAR)
        if recorded is None:
            return None
        loaded: dict[str, str | None] = json.loads(recorded)
        return loaded


class MasterlessConfig:
    """Stands in for `SparkSession.Builder.config` and drops cluster URLs.

    A `spark.master` makes PySpark refuse the build in Connect mode, and a
    `spark.remote` such as "local[1]" would start a JVM Connect server. Dropping both
    leaves SPARK_REMOTE, which points at Sail. `Builder.master` and `Builder.remote`
    delegate to `config`, so this one patch covers them and `key=`, `map=`, `conf=`.
    """

    def __init__(self, original: BuilderConfig) -> None:
        self.original = original

    def __get__(
        self, builder: SparkSession.Builder | None, owner: type | None = None
    ) -> BuilderConfig:
        if builder is None:
            return self
        return MethodType(self, builder)

    def __call__(
        self,
        builder: SparkSession.Builder,
        key: str | None = None,
        value: OptionalPrimitiveType = None,
        conf: SparkConf | None = None,
        *,
        map: dict[str, OptionalPrimitiveType] | None = None,
    ) -> SparkSession.Builder:
        sets_cluster_url = key in CLUSTER_URL_KEYS
        if sets_cluster_url:
            return builder
        if conf is not None:
            return self.original(builder, conf=self._without_master_conf(conf))
        if map is not None:
            masterless = {k: v for k, v in map.items() if k not in CLUSTER_URL_KEYS}
            return self.original(builder, map=masterless)
        return self.original(builder, key, value)

    @staticmethod
    def _without_master_conf(conf: SparkConf) -> SparkConf:
        pairs = [(k, v) for k, v in conf.getAll() if k not in CLUSTER_URL_KEYS]
        return SparkConf(loadDefaults=False).setAll(pairs)


class ContractedGetOrCreate:
    """Stands in for `Builder.getOrCreate`; adds the defaults the user left out.

    Defaults go in only when no live session exists. Builders re-apply their options
    to an existing session, so injecting later would undo a user's `conf.set`.
    """

    def __init__(
        self,
        original: BuilderGetOrCreate,
        defaults: Mapping[str, str],
        has_live_session: Callable[[], bool],
    ) -> None:
        self.original = original
        self.defaults = defaults
        self._has_live_session = has_live_session

    def __get__(
        self, builder: SparkSession.Builder | None, owner: type | None = None
    ) -> Callable[..., SparkSession]:
        if builder is None:
            return self
        return MethodType(self, builder)

    def __call__(self, builder: SparkSession.Builder) -> SparkSession:
        creates_new_session = not self._has_live_session()
        if creates_new_session:
            chosen_by_user = builder._options
            missing: dict[str, OptionalPrimitiveType] = {
                k: v for k, v in self.defaults.items() if k not in chosen_by_user
            }
            if missing:
                builder.config(map=missing)
        return self.original(builder)


class BuilderPatch(ClassPatch):
    """Replaces one `SparkSession.Builder` method and puts the original back."""

    TARGET = SparkSession.Builder


class MasterlessPatch(BuilderPatch):
    """Needed only in Connect mode, where PySpark rejects any master URL."""

    ATTRIBUTE = "config"

    def _replacement(self, previous: object) -> object:
        return MasterlessConfig(cast(BuilderConfig, previous))


class SessionContractPatch(BuilderPatch):
    """Gives each new session sparkwarp's contract, minus what the user set."""

    ATTRIBUTE = "getOrCreate"

    def __init__(
        self, defaults: Mapping[str, str], has_live_session: Callable[[], bool]
    ) -> None:
        super().__init__()
        self._defaults = defaults
        self._has_live_session = has_live_session

    def _replacement(self, previous: object) -> object:
        get_or_create = cast(BuilderGetOrCreate, previous)
        return ContractedGetOrCreate(
            get_or_create, self._defaults, self._has_live_session
        )
