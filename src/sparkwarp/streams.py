"""Plain result streams for the Connect clients of sparkwarp's own Sail server.

PySpark's reattachable execute lets a client resume a result stream that a remote
cluster dropped, and pays for it with a ReleaseExecute round trip for each streamed
message. Sail runs in this process on loopback, where those round trips are most of a
small action's cost. Without reattach, PySpark's retry policy re-runs the whole plan
after a retryable error, so a stream broken midway can repeat rows; on loopback that
error means Sail is gone. Clients of any other server, such as the JVM server, keep
reattach.
"""

from __future__ import annotations

from collections.abc import Callable
from types import MethodType
from typing import cast

from pyspark.sql.connect.client.core import DefaultChannelBuilder, SparkConnectClient

from sparkwarp.patches import ClassPatch

ClientInit = Callable[..., None]
EndpointLookup = Callable[[], str | None]


class PlainStreamInit:
    """Stands in for `SparkConnectClient.__init__`: Sail's clients skip reattach."""

    def __init__(self, original: ClientInit, sail_endpoint: EndpointLookup) -> None:
        self.original = original
        self._sail_endpoint = sail_endpoint

    def __get__(
        self, client: SparkConnectClient | None, owner: type | None = None
    ) -> Callable[..., None] | PlainStreamInit:
        if client is None:
            return self
        return MethodType(self, client)

    def __call__(
        self, client: SparkConnectClient, *args: object, **kwargs: object
    ) -> None:
        self.original(client, *args, **kwargs)
        builder = client._builder
        talks_to_sail = (
            isinstance(builder, DefaultChannelBuilder)
            and builder.endpoint == self._sail_endpoint()
        )
        if talks_to_sail:
            client.disable_reattachable_execute()


class PlainStreamsPatch(ClassPatch):
    """Every client built while installed checks its endpoint against Sail's."""

    TARGET = SparkConnectClient
    ATTRIBUTE = "__init__"

    def __init__(self, sail_endpoint: EndpointLookup) -> None:
        super().__init__()
        self._sail_endpoint = sail_endpoint

    def _replacement(self, previous: object) -> object:
        return PlainStreamInit(cast(ClientInit, previous), self._sail_endpoint)
