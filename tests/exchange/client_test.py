from __future__ import annotations

import threading
from collections.abc import Generator
from typing import Any

import pytest

from academy.exception import BadEntityIdError
from academy.exchange import ExchangeFactory
from academy.exchange import MailboxStatus
from academy.exchange import UserExchangeClient
from academy.identifier import AgentId
from academy.identifier import UserId
from academy.message import PingRequest
from academy.message import PingResponse
from academy.message import RequestMessage
from testing.behavior import EmptyBehavior
from testing.constant import TEST_THREAD_JOIN_TIMEOUT
from testing.constant import TEST_WAIT_TIMEOUT

# These fixtures are defined in testing/exchange.py
EXCHANGE_FACTORY_FIXTURES = (
    'http_exchange_factory',
    'hybrid_exchange_factory',
    'redis_exchange_factory',
    'thread_exchange_factory',
)


@pytest.fixture(params=EXCHANGE_FACTORY_FIXTURES)
def factory(request) -> ExchangeFactory[Any]:
    return request.getfixturevalue(request.param)


@pytest.fixture(params=EXCHANGE_FACTORY_FIXTURES)
def client(request) -> Generator[UserExchangeClient[Any]]:
    factory = request.getfixturevalue(request.param)
    with factory.create_user_client(start_listener=False) as client:
        yield client


def test_create_user_client(factory: ExchangeFactory[Any]) -> None:
    with factory.create_user_client(start_listener=False) as client:
        assert isinstance(repr(client), str)
        assert isinstance(str(client), str)


def test_create_agent_client(factory: ExchangeFactory[Any]) -> None:
    with factory.create_user_client(start_listener=False) as client:
        aid, info = client.register_agent(EmptyBehavior)
        with factory.create_agent_client(
            aid,
            info,
            lambda _: None,
        ) as agent_client:
            assert isinstance(repr(agent_client), str)
            assert isinstance(str(agent_client), str)


def test_create_agent_client_unregistered(
    factory: ExchangeFactory[Any],
) -> None:
    with factory.create_user_client(start_listener=False) as client:
        _, info = client.register_agent(EmptyBehavior)
        aid: AgentId[EmptyBehavior] = AgentId.new()
        with pytest.raises(BadEntityIdError):
            factory.create_agent_client(aid, info, lambda _: None)


def test_client_discover(client: UserExchangeClient[Any]) -> None:
    aid, _ = client.register_agent(EmptyBehavior)
    assert client.discover(EmptyBehavior) == (aid,)


def test_client_get_factory(client: UserExchangeClient[Any]) -> None:
    assert isinstance(client.factory(), ExchangeFactory)


def test_client_get_handle(client: UserExchangeClient[Any]) -> None:
    aid, _ = client.register_agent(EmptyBehavior)
    with client.get_handle(aid):
        pass


def test_client_get_handle_type_error(client: UserExchangeClient[Any]) -> None:
    with pytest.raises(TypeError):
        client.get_handle(UserId.new())  # type: ignore[arg-type]


def test_client_get_status(client: UserExchangeClient[Any]) -> None:
    uid = UserId.new()
    assert client.status(uid) == MailboxStatus.MISSING
    aid, info = client.register_agent(EmptyBehavior)
    assert client.status(aid) == MailboxStatus.ACTIVE
    client.terminate(aid)
    assert client.status(aid) == MailboxStatus.TERMINATED


def test_client_to_agent_message(factory: ExchangeFactory[Any]) -> None:
    received = threading.Event()

    def _handler(_: RequestMessage) -> None:
        received.set()

    with factory.create_user_client(start_listener=False) as user_client:
        aid, info = user_client.register_agent(EmptyBehavior)
        with factory.create_agent_client(aid, info, _handler) as agent_client:
            thread = threading.Thread(target=agent_client._listen_for_messages)
            thread.start()

            message = PingRequest(
                src=user_client.user_id,
                dest=agent_client.agent_id,
            )
            user_client.send(message)

            received.wait(TEST_WAIT_TIMEOUT)

            user_client.terminate(aid)
            thread.join(TEST_THREAD_JOIN_TIMEOUT)


def test_client_reply_error_on_request(factory: ExchangeFactory[Any]) -> None:
    with factory.create_user_client(start_listener=False) as client1:
        with factory.create_user_client(start_listener=True) as client2:
            message = PingRequest(src=client1.user_id, dest=client2.user_id)
            client1.send(message)
            response = client1._transport.recv()
            assert isinstance(response, PingResponse)
            assert isinstance(response.exception, TypeError)
