"""Sail's own clients stream results without reattach; any other server keeps it."""

from collections.abc import Iterator

import pyspark.sql.connect.proto as proto
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.connect.client.core import SparkConnectClient
from pyspark.sql.connect.session import SparkSession as ConnectSession

from sparkwarp.engines import SailEngine, ServerSettings
from sparkwarp.environment import ServerAddress
from sparkwarp.streams import PlainStreamsPatch

ROWS = 3
NO_CHECKPOINTS = None


class SentRequests:
    """Keeps every ExecutePlan request one client sends, then forwards it."""

    def __init__(self, client: SparkConnectClient) -> None:
        self._stub = client._stub
        self._original = self._stub.ExecutePlan
        self.sent: list[proto.ExecutePlanRequest] = []
        self._stub.ExecutePlan = self

    def __call__(
        self, request: proto.ExecutePlanRequest, *args: object, **kwargs: object
    ) -> object:
        self.sent.append(request)
        return self._original(request, *args, **kwargs)

    @property
    def asked_to_reattach(self) -> bool:
        return any(
            option.reattach_options.reattachable
            for request in self.sent
            for option in request.request_options
        )


class Actions:
    """Runs one action and reports whether its request asked for reattach."""

    @staticmethod
    def reattaches(session: SparkSession) -> bool:
        requests = SentRequests(session.client)
        assert session.range(ROWS).count() == ROWS
        return requests.asked_to_reattach


@pytest.fixture
def sail() -> Iterator[SailEngine]:
    engine = SailEngine()
    engine.prepare_environment()
    yield engine
    engine.stop()


@pytest.fixture
def other_server() -> Iterator[ServerAddress]:
    server = ServerSettings(NO_CHECKPOINTS).launch()
    host, port = server.listening_address
    yield ServerAddress(host, port)
    server.stop()


def test_the_shared_session_streams_without_reattach(sail: SailEngine) -> None:
    assert not Actions.reattaches(sail.start_session())


def test_a_created_session_streams_without_reattach(sail: SailEngine) -> None:
    session = SparkSession.builder.create()
    try:
        assert not Actions.reattaches(session)
    finally:
        session.stop()


def test_a_client_of_another_server_keeps_reattach(
    sail: SailEngine, other_server: ServerAddress
) -> None:
    session = ConnectSession(connection=other_server.url)
    try:
        assert Actions.reattaches(session)
    finally:
        session.stop()


def test_stop_puts_the_client_constructor_back() -> None:
    original = vars(SparkConnectClient)["__init__"]
    engine = SailEngine()
    try:
        engine.prepare_environment()
        assert vars(SparkConnectClient)["__init__"] is not original
    finally:
        engine.stop()
    assert vars(SparkConnectClient)["__init__"] is original


def test_without_a_running_sail_no_client_loses_reattach(
    other_server: ServerAddress,
) -> None:
    patch = PlainStreamsPatch(lambda: None)
    patch.install()
    try:
        session = ConnectSession(connection=other_server.url)
        try:
            assert Actions.reattaches(session)
        finally:
            session.stop()
    finally:
        patch.uninstall()
