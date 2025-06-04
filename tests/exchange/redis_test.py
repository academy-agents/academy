from __future__ import annotations

import pickle
from typing import Any

import pytest

from academy.behavior import Behavior
from academy.exception import BadEntityIdError
from academy.exception import MailboxClosedError
from academy.exchange import BoundExchangeClient
from academy.exchange import EMPTY_HANDLER
from academy.exchange import UnboundExchangeClient
from academy.exchange.redis import UnboundRedisExchangeClient
from academy.handle import BoundRemoteHandle
from academy.identifier import AgentId
from academy.identifier import ClientId
from academy.message import PingRequest
from testing.behavior import EmptyBehavior


def test_basic_usage(mock_redis) -> None:
    unbound_exchange = UnboundRedisExchangeClient('localhost', port=0)
    assert isinstance(unbound_exchange, UnboundExchangeClient)

    with unbound_exchange.bind() as exchange:
        assert isinstance(exchange, BoundExchangeClient)
        assert isinstance(repr(exchange), str)
        assert isinstance(str(exchange), str)

        aid = exchange.register_agent(EmptyBehavior)
        exchange.register_agent(
            EmptyBehavior,
            agent_id=aid,
        )  # Idempotency check
        assert isinstance(aid, AgentId)

        with unbound_exchange.bind(
            mailbox_id=aid,
            handler=EMPTY_HANDLER,
        ) as mailbox:
            for _ in range(3):
                message = PingRequest(src=exchange.mailbox_id, dest=aid)
                exchange.send(aid, message)
                assert mailbox.recv() == message

        exchange.terminate(aid)
        exchange.terminate(aid)  # Idempotency check


def test_bad_identifier_error(mock_redis) -> None:
    with UnboundRedisExchangeClient('localhost', port=0).bind() as exchange:
        uid = ClientId.new()
        with pytest.raises(BadEntityIdError):
            exchange.send(uid, PingRequest(src=exchange.mailbox_id, dest=uid))


def test_mailbox_closed_error(mock_redis) -> None:
    with UnboundRedisExchangeClient('localhost', port=0).bind() as exchange:
        aid = exchange.register_agent(EmptyBehavior)
        with exchange.clone().bind(
            mailbox_id=aid,
            handler=EMPTY_HANDLER,
        ) as mailbox:
            exchange.terminate(aid)
            with pytest.raises(MailboxClosedError):
                exchange.send(aid, PingRequest(src=aid, dest=aid))
            with pytest.raises(MailboxClosedError):
                mailbox.recv()


def test_get_handle_to_client(mock_redis) -> None:
    with UnboundRedisExchangeClient('localhost', port=0).bind() as exchange:
        aid = exchange.register_agent(EmptyBehavior)
        handle: BoundRemoteHandle[Any] = exchange.get_handle(aid)
        handle.close()

        with pytest.raises(TypeError, match='Handle must be created from an'):
            exchange.get_handle(ClientId.new())  # type: ignore[arg-type]


def test_mailbox_timeout(mock_redis) -> None:
    with UnboundRedisExchangeClient('localhost', port=0).bind(
        handler=EMPTY_HANDLER,
    ) as exchange:
        with pytest.raises(TimeoutError):
            exchange.recv(timeout=0.001)


def test_exchange_serialization(mock_redis) -> None:
    with UnboundRedisExchangeClient('localhost', port=0).bind() as exchange:
        pickled = pickle.dumps(exchange)
        reconstructed = pickle.loads(pickled)
        assert isinstance(reconstructed, UnboundExchangeClient)


class A(Behavior): ...


class B(Behavior): ...


class C(B): ...


def test_exchange_discover(mock_redis) -> None:
    with UnboundRedisExchangeClient('localhost', port=0).bind() as exchange:
        bid = exchange.register_agent(B)
        cid = exchange.register_agent(C)
        did = exchange.register_agent(C)
        exchange.terminate(did)

        assert len(exchange.discover(A)) == 0
        assert exchange.discover(B, allow_subclasses=False) == (bid,)
        assert exchange.discover(B, allow_subclasses=True) == (bid, cid)
