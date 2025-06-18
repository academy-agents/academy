from __future__ import annotations

import pickle
from collections.abc import Generator
from typing import Any

import pytest

from academy.behavior import Behavior
from academy.exception import BadEntityIdError
from academy.exception import MailboxClosedError
from academy.exchange import ExchangeFactory
from academy.exchange import ExchangeTransport
from academy.exchange import MailboxStatus
from academy.identifier import AgentId
from academy.identifier import UserId
from academy.message import PingRequest
from testing.behavior import EmptyBehavior

# These fixtures are defined in testing/exchange.py
EXCHANGE_FACTORY_FIXTURES = (
    'http_exchange_factory',
    'hybrid_exchange_factory',
    'redis_exchange_factory',
    'thread_exchange_factory',
)


@pytest.fixture(params=EXCHANGE_FACTORY_FIXTURES)
def transport(request) -> Generator[ExchangeTransport[Any]]:
    factory = request.getfixturevalue(request.param)
    with factory._create_transport() as transport:
        yield transport


def test_transport_repr(transport: ExchangeTransport[Any]) -> None:
    assert isinstance(repr(transport), str)
    assert isinstance(str(transport), str)


def test_transport_create_factory(transport: ExchangeTransport[Any]) -> None:
    new_factory = transport.factory()
    assert isinstance(new_factory, ExchangeFactory)


def test_transport_register_agent(transport: ExchangeTransport[Any]) -> None:
    agent_id, _ = transport.register_agent(EmptyBehavior)
    assert transport.status(agent_id) == MailboxStatus.ACTIVE


def test_transport_status(transport: ExchangeTransport[Any]) -> None:
    uid = UserId.new()
    assert transport.status(uid) == MailboxStatus.MISSING
    aid, _ = transport.register_agent(EmptyBehavior)
    assert transport.status(aid) == MailboxStatus.ACTIVE
    transport.terminate(aid)
    transport.terminate(aid)  # Idempotency
    assert transport.status(aid) == MailboxStatus.TERMINATED


def test_transport_send_recv(transport: ExchangeTransport[Any]) -> None:
    for _ in range(3):
        message = PingRequest(
            src=transport.mailbox_id,
            dest=transport.mailbox_id,
        )
        transport.send(message)
        assert transport.recv() == message


def test_transport_send_bad_identifier_error(
    transport: ExchangeTransport[Any],
) -> None:
    uid: AgentId[Any] = AgentId.new()
    with pytest.raises(BadEntityIdError):
        transport.send(PingRequest(src=transport.mailbox_id, dest=uid))


def test_transport_send_mailbox_closed(
    transport: ExchangeTransport[Any],
) -> None:
    aid, _ = transport.register_agent(EmptyBehavior)
    transport.terminate(aid)
    with pytest.raises(MailboxClosedError):
        transport.send(PingRequest(src=transport.mailbox_id, dest=aid))


def test_transport_recv_mailbox_closed(
    transport: ExchangeTransport[Any],
) -> None:
    transport.terminate(transport.mailbox_id)
    with pytest.raises(MailboxClosedError):
        transport.recv()


def test_transport_recv_timeout(transport: ExchangeTransport[Any]) -> None:
    with pytest.raises(TimeoutError):
        assert transport.recv(timeout=0.001)


def test_transport_non_pickleable(transport: ExchangeTransport[Any]) -> None:
    with pytest.raises(pickle.PicklingError):
        pickle.dumps(transport)


class A(Behavior): ...


class B(Behavior): ...


class C(B): ...


def test_transport_discover(transport: ExchangeTransport[Any]) -> None:
    bid, _ = transport.register_agent(B)
    cid, _ = transport.register_agent(C)
    did, _ = transport.register_agent(C)
    transport.terminate(did)

    assert len(transport.discover(A)) == 0
    assert transport.discover(B, allow_subclasses=False) == (bid,)
    assert transport.discover(B, allow_subclasses=True) == (bid, cid)
