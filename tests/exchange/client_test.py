from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from typing import Any

import pytest
import pytest_asyncio

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
from testing.constant import TEST_WAIT_TIMEOUT
from testing.fixture import EXCHANGE_FACTORY_TYPES


@pytest_asyncio.fixture(params=EXCHANGE_FACTORY_TYPES)
async def factory(
    request,
    get_factory,
) -> AsyncGenerator[ExchangeFactory[Any]]:
    return get_factory(request.param)


@pytest_asyncio.fixture(params=EXCHANGE_FACTORY_TYPES)
async def client(
    request,
    get_factory,
) -> AsyncGenerator[ExchangeFactory[Any]]:
    factory = get_factory(request.param)
    client = await factory.create_user_client(start_listener=False)
    try:
        yield client
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_create_user_client(factory: ExchangeFactory[Any]) -> None:
    async with await factory.create_user_client(
        start_listener=False,
    ) as client:
        assert isinstance(repr(client), str)
        assert isinstance(str(client), str)


async def _request_handler(_: Any) -> None:  # pragma: no cover
    pass


@pytest.mark.asyncio
async def test_create_agent_client(factory: ExchangeFactory[Any]) -> None:
    async with await factory.create_user_client(
        start_listener=False,
    ) as client:
        registration = await client.register_agent(EmptyBehavior)
        async with await factory.create_agent_client(
            registration,
            _request_handler,
        ) as agent_client:
            assert isinstance(repr(agent_client), str)
            assert isinstance(str(agent_client), str)


@pytest.mark.asyncio
async def test_create_agent_client_unregistered(
    factory: ExchangeFactory[Any],
) -> None:
    async with await factory.create_user_client(
        start_listener=False,
    ) as client:
        registration = await client.register_agent(EmptyBehavior)
        registration.agent_id = AgentId.new()
        with pytest.raises(BadEntityIdError):
            await factory.create_agent_client(registration, _request_handler)


@pytest.mark.asyncio
async def test_client_discover(client: UserExchangeClient[Any]) -> None:
    registration = await client.register_agent(EmptyBehavior)
    assert await client.discover(EmptyBehavior) == (registration.agent_id,)


@pytest.mark.asyncio
async def test_client_get_factory(client: UserExchangeClient[Any]) -> None:
    assert isinstance(client.factory(), ExchangeFactory)


@pytest.mark.asyncio
async def test_client_get_handle(client: UserExchangeClient[Any]) -> None:
    registration = await client.register_agent(EmptyBehavior)
    async with await client.get_handle(registration.agent_id):
        pass


@pytest.mark.asyncio
async def test_client_get_handle_type_error(
    client: UserExchangeClient[Any],
) -> None:
    with pytest.raises(TypeError):
        await client.get_handle(UserId.new())  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_client_get_status(client: UserExchangeClient[Any]) -> None:
    uid = UserId.new()
    assert await client.status(uid) == MailboxStatus.MISSING
    registration = await client.register_agent(EmptyBehavior)
    agent_id = registration.agent_id
    assert await client.status(agent_id) == MailboxStatus.ACTIVE
    await client.terminate(agent_id)
    assert await client.status(agent_id) == MailboxStatus.TERMINATED


@pytest.mark.asyncio
async def test_client_to_agent_message(factory: ExchangeFactory[Any]) -> None:
    received = asyncio.Event()

    async def _handler(_: RequestMessage) -> None:
        received.set()

    async with await factory.create_user_client(
        start_listener=False,
    ) as user_client:
        registration = await user_client.register_agent(EmptyBehavior)
        async with await factory.create_agent_client(
            registration,
            _handler,
        ) as agent_client:
            task = asyncio.Task(agent_client._listen_for_messages())

            message = PingRequest(
                src=user_client.client_id,
                dest=agent_client.client_id,
            )
            await user_client.send(message)

            await asyncio.wait_for(received.wait(), timeout=TEST_WAIT_TIMEOUT)

            await user_client.terminate(registration.agent_id)
            await task


@pytest.mark.asyncio
async def test_client_reply_error_on_request(
    factory: ExchangeFactory[Any],
) -> None:
    async with await factory.create_user_client(
        start_listener=False,
    ) as client1:
        async with await factory.create_user_client(
            start_listener=True,
        ) as client2:
            message = PingRequest(
                src=client1.client_id,
                dest=client2.client_id,
            )
            await client1.send(message)
            response = await client1._transport.recv()
            assert isinstance(response, PingResponse)
            assert isinstance(response.exception, TypeError)
