from __future__ import annotations

import pickle
from collections.abc import AsyncGenerator
from typing import Any

import pytest
import pytest_asyncio

from academy.agent import Agent
from academy.exception import BadEntityIdError
from academy.exception import MailboxTerminatedError
from academy.exchange import ExchangeFactory
from academy.exchange import MailboxStatus
from academy.exchange.hybrid import HybridExchangeFactory
from academy.exchange.transport import AgentRegistrationT
from academy.exchange.transport import ExchangeTransport
from academy.identifier import AgentId
from academy.identifier import UserId
from academy.message import ErrorResponse
from academy.message import Message
from academy.message import PingRequest
from academy.message import SuccessResponse
from testing.agents import EmptyAgent
from testing.constant import TEST_SLEEP_INTERVAL
from testing.constant import TEST_WAIT_TIMEOUT
from testing.fixture import EXCHANGE_FACTORY_TYPES


@pytest_asyncio.fixture(params=EXCHANGE_FACTORY_TYPES)
async def transport(
    request,
    get_factory,
) -> AsyncGenerator[ExchangeTransport[AgentRegistrationT]]:
    factory = get_factory(request.param)
    async with await factory._create_transport() as transport:
        yield transport


@pytest.mark.asyncio
async def test_transport_repr(
    transport: ExchangeTransport[AgentRegistrationT],
) -> None:
    assert isinstance(repr(transport), str)
    assert isinstance(str(transport), str)


@pytest.mark.asyncio
async def test_transport_create_factory(
    transport: ExchangeTransport[AgentRegistrationT],
) -> None:
    new_factory = transport.factory()
    assert isinstance(new_factory, ExchangeFactory)


@pytest.mark.asyncio
async def test_transport_register_agent(
    transport: ExchangeTransport[AgentRegistrationT],
) -> None:
    registration = await transport.register_agent(EmptyAgent)
    status = await transport.status(registration.agent_id)
    assert status == MailboxStatus.ACTIVE


@pytest.mark.asyncio
async def test_transport_status(
    transport: ExchangeTransport[AgentRegistrationT],
) -> None:
    uid = UserId.new()
    status = await transport.status(uid)
    assert status == MailboxStatus.MISSING
    registration = await transport.register_agent(EmptyAgent)
    status = await transport.status(registration.agent_id)
    assert status == MailboxStatus.ACTIVE
    await transport.terminate(registration.agent_id)
    await transport.terminate(registration.agent_id)  # Idempotency
    status = await transport.status(registration.agent_id)
    assert status == MailboxStatus.TERMINATED


@pytest.mark.asyncio
async def test_transport_send_recv(
    transport: ExchangeTransport[AgentRegistrationT],
) -> None:
    listener = transport.listen(timeout=TEST_WAIT_TIMEOUT)
    for _ in range(3):
        message = Message.create(
            src=transport.mailbox_id,
            dest=transport.mailbox_id,
            body=PingRequest(),
        )
        await transport.send(message)

        response = await anext(listener)
        assert response == message


@pytest.mark.asyncio
async def test_transport_send_bad_identifier_error(
    transport: ExchangeTransport[AgentRegistrationT],
) -> None:
    uid: AgentId[Any] = AgentId.new()
    with pytest.raises(BadEntityIdError):
        await transport.send(
            Message.create(
                src=transport.mailbox_id,
                dest=uid,
                body=PingRequest(),
            ),
        )


@pytest.mark.asyncio
async def test_transport_send_mailbox_closed(
    transport: ExchangeTransport[AgentRegistrationT],
) -> None:
    registration = await transport.register_agent(EmptyAgent)
    await transport.terminate(registration.agent_id)
    with pytest.raises(MailboxTerminatedError):
        await transport.send(
            Message.create(
                src=transport.mailbox_id,
                dest=registration.agent_id,
                body=PingRequest(),
            ),
        )


@pytest.mark.asyncio
async def test_transport_recv_mailbox_closed(
    transport: ExchangeTransport[AgentRegistrationT],
) -> None:
    await transport.terminate(transport.mailbox_id)
    with pytest.raises(MailboxTerminatedError):
        await anext(transport.listen(timeout=TEST_WAIT_TIMEOUT))


@pytest.mark.asyncio
async def test_transport_recv_timeout(
    transport: ExchangeTransport[AgentRegistrationT],
) -> None:
    with pytest.raises(TimeoutError):
        await anext(transport.listen(timeout=TEST_SLEEP_INTERVAL))


@pytest.mark.asyncio
async def test_transport_terminate_unknown_ok(
    transport: ExchangeTransport[AgentRegistrationT],
) -> None:
    await transport.terminate(UserId.new())


@pytest.mark.parametrize('factory_type', EXCHANGE_FACTORY_TYPES)
async def test_transport_terminate_reply_pending_requests(
    factory_type: type[ExchangeFactory[Any]],
    get_factory,
) -> None:
    if factory_type is HybridExchangeFactory:
        pytest.skip(
            'HybridExchangeTransport termination behavior is unreliable.',
        )

    factory = get_factory(factory_type)
    async with await factory._create_transport() as transport1:
        async with await factory._create_transport() as transport2:
            message1 = Message.create(
                src=transport1.mailbox_id,
                dest=transport2.mailbox_id,
                body=PingRequest(),
            )
            message2 = Message.create(
                src=transport1.mailbox_id,
                dest=transport2.mailbox_id,
                body=SuccessResponse(),
            )
            message3 = Message.create(
                src=transport1.mailbox_id,
                dest=transport2.mailbox_id,
                body=PingRequest(),
            )

            await transport1.send(message1)
            listener2 = transport2.listen(TEST_WAIT_TIMEOUT)
            received = await anext(listener2)
            assert received == message1

            # Send message1 and message2; they remain in the queue unread
            # when transport2 is terminated.
            await transport1.send(message2)
            await transport1.send(message3)

            await transport2.terminate(transport2.mailbox_id)

            # Both pending requests (message3 was read but unanswered;
            # message1 was never read) must each receive an ErrorResponse.
            listener = transport1.listen(TEST_WAIT_TIMEOUT)
            tags: set[object] = set()
            for _ in range(2):
                response = await anext(listener)
                body = response.get_body()
                assert isinstance(body, ErrorResponse)
                assert isinstance(body.get_exception(), MailboxTerminatedError)
                tags.add(response.tag)
            assert tags == {message1.tag, message3.tag}
            # No other messages should have been received
            with pytest.raises(TimeoutError):  # pragma: <3.14 cover
                await anext(listener)


@pytest.mark.asyncio
async def test_transport_non_pickleable(
    transport: ExchangeTransport[AgentRegistrationT],
) -> None:
    with pytest.raises(pickle.PicklingError):
        pickle.dumps(transport)


class A(Agent): ...


class B(Agent): ...


class C(B): ...


@pytest.mark.asyncio
async def test_transport_discover(
    transport: ExchangeTransport[AgentRegistrationT],
) -> None:
    bid = (await transport.register_agent(B)).agent_id
    cid = (await transport.register_agent(C)).agent_id
    did = (await transport.register_agent(C)).agent_id
    await transport.terminate(did)

    found = await transport.discover(A)
    assert len(found) == 0
    found = await transport.discover(B, allow_subclasses=False)
    assert found == (bid,)
    found = await transport.discover(B, allow_subclasses=True)
    assert found == (bid, cid)

    aid = (await transport.register_agent(A)).agent_id
    found = await transport.discover(Agent)
    assert set(found) == {bid, cid, aid}
