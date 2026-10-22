"""Client patches that send each request to the engine the router picks.

They wrap the Spark Connect client methods every query goes through: the streaming
execute, the result-less execute, analyze, and config. Requests from the JVM session's
own client pass straight through, so nothing is routed twice.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from types import MethodType
from typing import cast

import pyspark.sql.connect.proto as proto
from pyspark.errors import PySparkException
from pyspark.sql.connect.client.core import SparkConnectClient

from sparkwarp.patches import ClassPatch
from sparkwarp.router import PlanRouter, Route

Stream = Callable[..., Iterator[object]]
Call = Callable[..., object]


class ClientMethod:
    """A descriptor that binds a routing wrapper to each client instance."""

    def __init__(self, original: Call, router: PlanRouter) -> None:
        self.original = original
        self.router = router

    def __get__(
        self, client: SparkConnectClient | None, owner: type | None = None
    ) -> Callable[..., object] | ClientMethod:
        if client is None:
            return self
        return MethodType(cast(Call, self), client)

    def _jvm_request(
        self, request: proto.ExecutePlanRequest
    ) -> tuple[SparkConnectClient, proto.ExecutePlanRequest]:
        """The same plan, in a request that belongs to the JVM session."""
        jvm_client = self.router.jvm.client()
        jvm_request: proto.ExecutePlanRequest = (
            jvm_client._execute_plan_request_with_metadata()
        )
        jvm_request.plan.CopyFrom(request.plan)
        jvm_request.tags.extend(request.tags)
        return jvm_client, jvm_request


class RoutedStream(ClientMethod):
    """Stands in for `_execute_and_fetch_as_iterator`."""

    @property
    def _stream(self) -> Stream:
        return cast(Stream, self.original)

    def __call__(
        self,
        client: SparkConnectClient,
        request: proto.ExecutePlanRequest,
        *args: object,
        **kwargs: object,
    ) -> Iterator[object]:
        if self.router.is_jvm_client(client):
            yield from self._stream(client, request, *args, **kwargs)
            return
        if self.router.route(request.plan) is Route.JVM:
            yield from self._on_jvm(request, args, kwargs)
            return
        answered = False
        try:
            for response in self._stream(client, request, *args, **kwargs):
                answered = True
                yield response
        except PySparkException as error:
            if answered or not self.router.retries_on_jvm(request.plan, error):
                raise
            yield from self._on_jvm(request, args, kwargs)

    def _on_jvm(
        self,
        request: proto.ExecutePlanRequest,
        args: tuple[object, ...],
        kwargs: dict[str, object],
    ) -> Iterator[object]:
        jvm_client, jvm_request = self._jvm_request(request)
        yield from self._stream(jvm_client, jvm_request, *args, **kwargs)


class WatchedExecute(ClientMethod):
    """Stands in for `_execute`, which registers functions: remembers their names."""

    def __call__(
        self, client: SparkConnectClient, request: proto.ExecutePlanRequest
    ) -> object:
        if not self.router.is_jvm_client(client):
            self.router.note_registration(request.plan)
        return self.original(client, request)


class RoutedAnalyze(ClientMethod):
    """Stands in for `_analyze`: describing a deep plan is slow on Sail too."""

    def __call__(
        self, client: SparkConnectClient, method: str, **kwargs: object
    ) -> object:
        plans = [value for value in kwargs.values() if isinstance(value, proto.Plan)]
        if self.router.is_jvm_client(client) or not plans:
            return self.original(client, method, **kwargs)
        if self._all_on_jvm(plans):
            return self.original(self.router.jvm.client(), method, **kwargs)
        try:
            return self.original(client, method, **kwargs)
        except PySparkException as error:
            retry = all(
                self.router.retries_on_jvm(plan, error, count=False) for plan in plans
            )
            if not retry:
                raise
            return self.original(self.router.jvm.client(), method, **kwargs)

    def _all_on_jvm(self, plans: list[proto.Plan]) -> bool:
        """Every plan of the request can move; `same_semantics` carries two."""
        routes = [self.router.route(plan, count=False) for plan in plans]
        return all(route is Route.JVM for route in routes)


class MirroredConfig(ClientMethod):
    """Stands in for `config`: writes also reach the JVM session, now or once open."""

    def __call__(
        self, client: SparkConnectClient, operation: proto.ConfigRequest.Operation
    ) -> object:
        result = self.original(client, operation)
        if not self.router.is_jvm_client(client):
            self.router.jvm.record_conf(operation)
        return result


class RoutingPatch(ClassPatch):
    """One client method wrapped for routing; the subclasses name which."""

    TARGET = SparkConnectClient
    WRAPPER: type[ClientMethod]

    def __init__(self, router: PlanRouter) -> None:
        super().__init__()
        self._router = router

    @property
    def applies(self) -> bool:
        return self.ATTRIBUTE in vars(self.TARGET)

    def _replacement(self, previous: object) -> object:
        return self.WRAPPER(cast(Call, previous), self._router)


class StreamRoutingPatch(RoutingPatch):
    ATTRIBUTE = "_execute_and_fetch_as_iterator"
    WRAPPER = RoutedStream


class ExecuteRoutingPatch(RoutingPatch):
    ATTRIBUTE = "_execute"
    WRAPPER = WatchedExecute


class AnalyzeRoutingPatch(RoutingPatch):
    ATTRIBUTE = "_analyze"
    WRAPPER = RoutedAnalyze


class ConfigMirrorPatch(RoutingPatch):
    ATTRIBUTE = "config"
    WRAPPER = MirroredConfig


class RoutingPatches:
    """The four patches, in install order."""

    @staticmethod
    def for_router(router: PlanRouter) -> tuple[RoutingPatch, ...]:
        return (
            StreamRoutingPatch(router),
            ExecuteRoutingPatch(router),
            AnalyzeRoutingPatch(router),
            ConfigMirrorPatch(router),
        )
