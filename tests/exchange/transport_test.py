from __future__ import annotations

import pickle
from collections.abc import AsyncGenerator
from typing import Any

import pytest
import pytest_asyncio

from academy.behavior import Behavior
from academy.exception import BadEntityIdError
from academy.exception import MailboxTerminatedError
from academy.exchange import ExchangeFactory
from academy.exchange import MailboxStatus
from academy.exchange.transport import AgentRegistrationT
from academy.exchange.transport import ExchangeTransport
from academy.identifier import AgentId
from academy.identifier import UserId
from academy.message import PingRequest
from testing.behavior import EmptyBehavior
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
    registration = await transport.register_agent(EmptyBehavior)
    status = await transport.status(registration.agent_id)
    assert status == MailboxStatus.ACTIVE


@pytest.mark.asyncio
async def test_transport_status(
    transport: ExchangeTransport[AgentRegistrationT],
) -> None:
    uid = UserId.new()
    status = await transport.status(uid)
    assert status == MailboxStatus.MISSING
    registration = await transport.register_agent(EmptyBehavior)
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
    for _ in range(3):
        message = PingRequest(
            src=transport.mailbox_id,
            dest=transport.mailbox_id,
        )
        await transport.send(message)
        assert await transport.recv() == message


@pytest.mark.asyncio
async def test_transport_send_bad_identifier_error(
    transport: ExchangeTransport[AgentRegistrationT],
) -> None:
    uid: AgentId[Any] = AgentId.new()
    with pytest.raises(BadEntityIdError):
        await transport.send(PingRequest(src=transport.mailbox_id, dest=uid))


@pytest.mark.asyncio
async def test_transport_send_mailbox_closed(
    transport: ExchangeTransport[AgentRegistrationT],
) -> None:
    registration = await transport.register_agent(EmptyBehavior)
    await transport.terminate(registration.agent_id)
    with pytest.raises(MailboxTerminatedError):
        await transport.send(
            PingRequest(src=transport.mailbox_id, dest=registration.agent_id),
        )


@pytest.mark.asyncio
async def test_transport_recv_mailbox_closed(
    transport: ExchangeTransport[AgentRegistrationT],
) -> None:
    await transport.terminate(transport.mailbox_id)
    with pytest.raises(MailboxTerminatedError):
        await transport.recv()


@pytest.mark.asyncio
async def test_transport_recv_timeout(
    transport: ExchangeTransport[AgentRegistrationT],
) -> None:
    with pytest.raises(TimeoutError):
        await transport.recv(timeout=0.001)


@pytest.mark.asyncio
async def test_transport_non_pickleable(
    transport: ExchangeTransport[AgentRegistrationT],
) -> None:
    with pytest.raises(pickle.PicklingError):
        pickle.dumps(transport)


class A(Behavior): ...


class B(Behavior): ...


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
