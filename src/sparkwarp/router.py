"""Which engine runs a plan: Sail by default, the shared JVM where Sail falls short.

A self-contained plan goes to the JVM when its expressions nest deeper than the limit,
where Sail's planning time explodes, or when Sail rejects it with a known Sail gap.
Everything else, and anything that needs server-side state, stays on Sail.
"""

from __future__ import annotations

import json
import os
from collections import Counter
from collections.abc import Callable, Mapping
from contextlib import suppress
from enum import Enum
from pathlib import Path

import pyspark.sql.connect.proto as proto
from pyspark.errors import PySparkException, PySparkNotImplementedError
from pyspark.sql.connect.client.core import SparkConnectClient
from pyspark.sql.connect.session import SparkSession as ConnectSession

from sparkwarp.config import (
    COMMAND,
    CONF_SET,
    CONF_WRITES,
    COUNTS_FILE_GLOB,
    COUNTS_FILE_PATTERN,
    DEEP_COLUMN_FEATURE,
    DEFAULT_JVM_DEPTH,
    FEATURE_PARAMETER,
    JVM_DEPTH_ENV_VAR,
    JVM_ENV_VAR,
    JVM_LOST,
    LOOKUP_CAST_SIGNAL,
    NO_JVM_REASON,
    NO_JVM_SERVER,
    NOT_IMPLEMENTED_CONDITION,
    OFF_FLAG_VALUES,
    QUERY_NOUNS,
    REGISTER_FUNCTION,
    ROUTING_SUMMARY,
    SAIL_GAP_SIGNALS,
)
from sparkwarp.jvm_server import JavaRuntime, JvmUnavailableError, SharedJvmServer
from sparkwarp.plan_shape import PlanShape


class Route(Enum):
    SAIL = "sail"
    JVM = "jvm"


class DeepColumnError(PySparkNotImplementedError):
    """A plan too deep for Sail and no JVM to run it; the hint names the fix."""

    def __init__(self, depth: int, limit: int, why: str) -> None:
        feature = DEEP_COLUMN_FEATURE.format(depth=depth, limit=limit, why=why)
        super().__init__(
            errorClass=NOT_IMPLEMENTED_CONDITION,
            messageParameters={FEATURE_PARAMETER: feature},
        )


class RoutingPolicy:
    """The switches, read when a plan is routed so a conftest can still change them."""

    @property
    def jvm_allowed(self) -> bool:
        switch = os.environ.get(JVM_ENV_VAR, "").strip().lower()
        return switch not in OFF_FLAG_VALUES and JavaRuntime.available()

    @property
    def depth_limit(self) -> int | None:
        """Plans at least this deep leave Sail; 0 turns the limit off."""
        raw = os.environ.get(JVM_DEPTH_ENV_VAR, "")
        limit = int(raw) if raw.strip().lstrip("-").isdigit() else DEFAULT_JVM_DEPTH
        return limit if limit > 0 else None


class JvmSession:
    """A Spark Connect session on the shared JVM server, opened on first need.

    It is never the default or active session: user code keeps talking to Sail. It gets
    sparkwarp's session contract and every `spark.conf` write made so far.
    """

    def __init__(self, contract: Mapping[str, str]) -> None:
        self._server: SharedJvmServer | None = None
        self._contract = dict(contract)
        self._conf_writes: dict[str, proto.ConfigRequest.Operation] = {}
        self._session: ConnectSession | None = None
        self._url: str | None = None
        self._retired: list[ConnectSession] = []

    @property
    def opened(self) -> ConnectSession | None:
        return self._session

    def drop_retired(self) -> None:
        """Closes the channels of sessions whose server is gone, without a release RPC.

        Releasing would retry against the dead address for minutes.
        """
        retired, self._retired = self._retired, []
        for session in retired:
            session.client.close()

    @property
    def server(self) -> SharedJvmServer | None:
        return self._server

    def attach(self, server: SharedJvmServer) -> None:
        self._server = server

    def client(self) -> SparkConnectClient:
        """The JVM session's client; a server restarted under it gets a new session."""
        if self._server is None:
            raise RuntimeError(NO_JVM_SERVER)
        url = self._server.url()
        session = self._session
        is_current = session is not None and url == self._url
        if session is None or not is_current:
            session = self._open(url)
        client: SparkConnectClient = session.client
        return client

    def record_conf(self, operation: proto.ConfigRequest.Operation) -> None:
        """Keeps a write for the JVM; one the JVM rejects is not the user's error."""
        kind = operation.WhichOneof("op_type")
        if kind is None or kind not in CONF_WRITES:
            return
        for key, single in ConfWrite.per_key(operation):
            self._conf_writes.pop(key, None)
            self._conf_writes[key] = single
        session = self._session
        if session is None:
            return
        if self._server_answers():
            self._apply(session, operation)
        else:
            self._retire_current()

    def recorded_failure(self) -> str | None:
        return self._server.recorded_failure() if self._server is not None else None

    def close(self, release: Callable[[ConnectSession], None]) -> None:
        """Releases the session on a live server; on a gone one, closes the channel."""
        self.drop_retired()
        session = self._session
        if session is None or session.is_stopped:
            return
        if self._server_answers():
            release(session)
        else:
            session.client.close()

    def _retire_current(self) -> None:
        """The next routed query opens a new session, which replays every write."""
        if self._session is not None:
            self._retired.append(self._session)
        self._session, self._url = None, None

    def _server_answers(self) -> bool:
        """RPCs to a gone server retry for minutes, so ask the server first."""
        return self._server is not None and self._server.answering()

    def _open(self, url: str) -> ConnectSession:
        """Registers the session before its conf writes, so they are not mirrored."""
        if self._session is not None:
            self._retired.append(self._session)
        session = ConnectSession(connection=url)
        self._session, self._url = session, url
        try:
            for key, value in self._contract.items():
                session.conf.set(key, value)
        except PySparkException as error:
            self._session, self._url = None, None
            self._retired.append(session)
            raise JvmUnavailableError(JVM_LOST.format(reason=error)) from error
        for operation in list(self._conf_writes.values()):
            self._apply(session, operation)
        return session

    @staticmethod
    def _apply(
        session: ConnectSession, operation: proto.ConfigRequest.Operation
    ) -> None:
        with suppress(PySparkException):
            session.client.config(operation)


class ConfWrite:
    """Splits a conf write into one operation per key, so only the last one is kept."""

    @staticmethod
    def per_key(
        operation: proto.ConfigRequest.Operation,
    ) -> list[tuple[str, proto.ConfigRequest.Operation]]:
        if operation.WhichOneof("op_type") == CONF_SET:
            return [(pair.key, ConfWrite._set(pair)) for pair in operation.set.pairs]
        return [(key, ConfWrite._unset(key)) for key in operation.unset.keys]

    @staticmethod
    def _set(pair: proto.KeyValue) -> proto.ConfigRequest.Operation:
        single = proto.ConfigRequest.Operation()
        single.set.pairs.add(key=pair.key, value=pair.value)
        return single

    @staticmethod
    def _unset(key: str) -> proto.ConfigRequest.Operation:
        single = proto.ConfigRequest.Operation()
        single.unset.keys.append(key)
        return single


class PlanRouter:
    """Decides each plan's engine and counts what went to the JVM."""

    def __init__(self, jvm: JvmSession, policy: RoutingPolicy | None = None) -> None:
        self.jvm = jvm
        self._policy = policy or RoutingPolicy()
        self._registered: set[str] = set()
        self._jvm_failure: str | None = None
        self.deep_plans = 0
        self.sail_gaps = 0

    def is_jvm_client(self, client: SparkConnectClient) -> bool:
        session = self.jvm.opened
        return session is not None and client is session.client

    def note_registration(self, plan: proto.Plan) -> None:
        """A function registered on Sail by name; plans that call it stay on Sail."""
        is_command = plan.WhichOneof("op_type") == COMMAND
        registers = is_command and (
            plan.command.WhichOneof("command_type") == REGISTER_FUNCTION
        )
        if registers:
            name = plan.command.register_function.function_name
            self._registered.add(name.lower())

    def route(self, plan: proto.Plan, count: bool = True) -> Route:
        limit = self._policy.depth_limit
        nothing_to_decide = limit is None and not self._policy.jvm_allowed
        if nothing_to_decide:
            return Route.SAIL
        shape = PlanShape(plan, frozenset(self._registered))
        too_deep = limit is not None and shape.expression_depth >= limit
        movable = shape.self_contained and too_deep
        if movable and self._jvm_ready():
            self.deep_plans += int(count)
            return Route.JVM
        if too_deep:
            self._refuse_without_jvm(shape.expression_depth, limit or 0)
        return Route.SAIL

    def _refuse_without_jvm(self, depth: int, limit: int) -> None:
        """A too-deep plan that the JVM will not take fails fast instead of crawling."""
        if not self._jvm_usable():
            raise DeepColumnError(depth, limit, self._jvm_failure or NO_JVM_REASON)

    def retries_on_jvm(
        self, plan: proto.Plan, error: BaseException, count: bool = True
    ) -> bool:
        """Whether a Sail failure is a known Sail gap that the JVM can run instead."""
        shape = PlanShape(plan, frozenset(self._registered))
        message = str(error)
        is_known_gap = any(signal in message for signal in SAIL_GAP_SIGNALS)
        is_lookup_gap = shape.looks_up_by_column and LOOKUP_CAST_SIGNAL in message
        is_sail_gap = is_known_gap or is_lookup_gap
        retry = is_sail_gap and shape.self_contained and self._jvm_ready()
        counted = retry and count
        self.sail_gaps += int(counted)
        return retry

    def _jvm_usable(self) -> bool:
        """Java is there and no process of the run failed to start the server."""
        self._jvm_failure = self._jvm_failure or self.jvm.recorded_failure()
        return self._jvm_failure is None and self._policy.jvm_allowed

    def _jvm_ready(self) -> bool:
        """The server runs, started now if needed; a failed start keeps Sail."""
        if not self._jvm_usable():
            return False
        try:
            self.jvm.client()
        except JvmUnavailableError as error:
            self._jvm_failure = str(error)
            return False
        return True

    @property
    def counts(self) -> dict[str, int]:
        return {"deep_plans": self.deep_plans, "sail_gaps": self.sail_gaps}

    def save_counts(self, directory: Path) -> None:
        target = directory / COUNTS_FILE_PATTERN.format(pid=os.getpid())
        if directory.exists() and any(self.counts.values()):
            target.write_text(json.dumps(self.counts))


class RoutingSummary:
    """What every process of the run sent to the JVM, for the terminal summary."""

    def __init__(self, directory: Path | None, own: Mapping[str, int]) -> None:
        self._directory = directory
        self._own = own

    def line(self) -> str | None:
        totals = Counter(self._own)
        for saved in self._saved():
            totals.update(saved)
        total = sum(totals.values())
        if not total:
            return None
        noun = QUERY_NOUNS[total != 1]
        return ROUTING_SUMMARY.format(total=total, noun=noun, **totals)

    def _saved(self) -> list[dict[str, int]]:
        if self._directory is None or not self._directory.exists():
            return []
        saved: list[dict[str, int]] = []
        for file in self._directory.glob(COUNTS_FILE_GLOB):
            with suppress(OSError, ValueError):
                saved.append(json.loads(file.read_text()))
        return saved
