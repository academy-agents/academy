from __future__ import annotations

from concurrent.futures import Future
from typing import Any
from unittest import mock

import pytest

from academy.exception import HandleClosedError
from academy.exception import HandleNotBoundError
from academy.exception import MailboxClosedError
from academy.exchange import Exchange
from academy.exchange.thread import ThreadExchange
from academy.handle import BoundRemoteHandle
from academy.handle import ClientRemoteHandle
from academy.handle import Handle
from academy.handle import ProxyHandle
from academy.handle import UnboundRemoteHandle
from academy.identifier import AgentId
from academy.identifier import ClientId
from academy.launcher import ThreadLauncher
from academy.message import PingRequest
from testing.behavior import CounterBehavior
from testing.behavior import EmptyBehavior
from testing.behavior import ErrorBehavior
from testing.behavior import SleepBehavior
from testing.constant import TEST_SLEEP


def test_proxy_handle_protocol() -> None:
    behavior = EmptyBehavior()
    handle = ProxyHandle(behavior)
    assert isinstance(handle, Handle)
    assert str(behavior) in str(handle)
    assert repr(behavior) in repr(handle)
    assert handle.ping() >= 0
    handle.shutdown()


def test_proxy_handle_actions() -> None:
    handle = ProxyHandle(CounterBehavior())

    # Via Handle.action()
    assert handle.action('add', 1).result() is None
    assert handle.action('count').result() == 1

    # Via attribute lookup
    assert handle.add(1).result() is None
    assert handle.count().result() == 2  # noqa: PLR2004


def test_proxy_handle_action_errors() -> None:
    handle = ProxyHandle(ErrorBehavior())
    with pytest.raises(RuntimeError, match='This action always fails.'):
        handle.action('fails').result()
    with pytest.raises(AttributeError, match='null'):
        handle.action('null').result()
    with pytest.raises(AttributeError, match='null'):
        handle.null()  # type: ignore[attr-defined]

    handle.behavior.foo = 1  # type: ignore[attr-defined]
    with pytest.raises(AttributeError, match='not a method'):
        handle.foo()  # type: ignore[attr-defined]


def test_proxy_handle_closed_errors() -> None:
    handle = ProxyHandle(EmptyBehavior())
    handle.close()

    with pytest.raises(HandleClosedError):
        handle.action('test')
    with pytest.raises(HandleClosedError):
        handle.ping()
    with pytest.raises(HandleClosedError):
        handle.shutdown()


def test_proxy_handle_agent_shutdown_errors() -> None:
    handle = ProxyHandle(EmptyBehavior())
    handle.shutdown()

    with pytest.raises(MailboxClosedError):
        handle.action('test')
    with pytest.raises(MailboxClosedError):
        handle.ping()
    with pytest.raises(MailboxClosedError):
        handle.shutdown()


def test_unbound_remote_handle_serialize(exchange: Exchange) -> None:
    agent_id = exchange.register_agent(EmptyBehavior)
    with UnboundRemoteHandle(exchange, agent_id) as handle:
        # Note: don't call pickle.dumps here because ThreadExchange
        # is not pickleable so we test __reduce__ directly.
        class_, args = handle.__reduce__()
        with class_(*args) as reconstructed:
            assert isinstance(reconstructed, UnboundRemoteHandle)
            assert str(reconstructed) == str(handle)
            assert repr(reconstructed) == repr(handle)


def test_unbound_remote_handle_bind(exchange: Exchange) -> None:
    agent_id = exchange.register_agent(EmptyBehavior)
    with UnboundRemoteHandle(exchange, agent_id) as handle:
        client_bound: ClientRemoteHandle[Any]
        with handle.bind_as_client() as client_bound:
            assert isinstance(client_bound, ClientRemoteHandle)
        agent_bound: BoundRemoteHandle[Any]
        with handle.bind_to_mailbox(AgentId.new()) as agent_bound:
            assert isinstance(agent_bound, BoundRemoteHandle)


def test_unbound_remote_handle_errors(exchange: Exchange) -> None:
    agent_id = exchange.register_agent(EmptyBehavior)
    with UnboundRemoteHandle(exchange, agent_id) as handle:
        request = PingRequest(src=ClientId.new(), dest=agent_id)
        with pytest.raises(HandleNotBoundError):
            handle._send_request(request)
        with pytest.raises(HandleNotBoundError):
            handle.action('foo')
        with pytest.raises(HandleNotBoundError):
            handle.ping()
        with pytest.raises(HandleNotBoundError):
            handle.shutdown()


def test_remote_handle_closed_error(exchange: Exchange) -> None:
    agent_id = exchange.register_agent(EmptyBehavior)
    handles = [
        BoundRemoteHandle(
            exchange,
            agent_id,
            exchange.register_agent(EmptyBehavior),
        ),
        ClientRemoteHandle(exchange, agent_id, exchange.register_client()),
    ]
    for handle in handles:
        handle.close()
        assert handle.mailbox_id is not None
        with pytest.raises(HandleClosedError):
            handle.action('foo')
        with pytest.raises(HandleClosedError):
            handle.ping()
        with pytest.raises(HandleClosedError):
            handle.shutdown()


def test_agent_remote_handle_serialize(exchange: Exchange) -> None:
    agent_id = exchange.register_agent(EmptyBehavior)
    mailbox_id = exchange.register_agent(EmptyBehavior)
    with BoundRemoteHandle(exchange, agent_id, mailbox_id) as handle:
        # Note: don't call pickle.dumps here because ThreadExchange
        # is not pickleable so we test __reduce__ directly.
        class_, args = handle.__reduce__()
        with class_(*args) as reconstructed:
            assert isinstance(reconstructed, UnboundRemoteHandle)
            assert str(reconstructed) != str(handle)
            assert repr(reconstructed) != repr(handle)
            assert reconstructed.agent_id == handle.agent_id


def test_agent_remote_handle_bind(exchange: Exchange) -> None:
    agent_id = exchange.register_agent(EmptyBehavior)
    mailbox_id = exchange.register_agent(EmptyBehavior)
    with BoundRemoteHandle(exchange, agent_id, mailbox_id) as handle:
        assert isinstance(handle.mailbox_id, AgentId)
        with handle.bind_as_client() as client_bound:
            assert isinstance(client_bound, ClientRemoteHandle)
        with pytest.raises(
            ValueError,
            match=f'Cannot create handle to {handle.agent_id}',
        ):
            handle.bind_to_mailbox(handle.agent_id)
        with handle.bind_to_mailbox(handle.mailbox_id) as agent_bound:
            assert agent_bound is handle
        with handle.bind_to_mailbox(AgentId.new()) as agent_bound:
            assert agent_bound is not handle
            assert isinstance(agent_bound, BoundRemoteHandle)


def test_client_remote_handle_serialize(exchange: Exchange) -> None:
    agent_id = exchange.register_agent(EmptyBehavior)
    mailbox_id = exchange.register_client()
    with ClientRemoteHandle(exchange, agent_id, mailbox_id) as handle:
        # Note: don't call pickle.dumps here because ThreadExchange
        # is not pickleable so we test __reduce__ directly.
        class_, args = handle.__reduce__()
        with class_(*args) as reconstructed:
            assert isinstance(reconstructed, UnboundRemoteHandle)
            assert str(reconstructed) != str(handle)
            assert repr(reconstructed) != repr(handle)
            assert reconstructed.agent_id == handle.agent_id


def test_client_remote_handle_bind(exchange: Exchange) -> None:
    agent_id = exchange.register_agent(EmptyBehavior)
    mailbox_id = exchange.register_client()
    with ClientRemoteHandle(exchange, agent_id, mailbox_id) as handle:
        assert handle.bind_as_client() is handle
        with handle.bind_as_client(exchange.register_client()) as client_bound:
            assert client_bound is not handle
            assert isinstance(client_bound, ClientRemoteHandle)
        with handle.bind_to_mailbox(AgentId.new()) as agent_bound:
            assert isinstance(agent_bound, BoundRemoteHandle)


def test_client_remote_handle_log_bad_response(
    exchange: ThreadExchange,
    launcher: ThreadLauncher,
) -> None:
    behavior = EmptyBehavior()
    with launcher.launch(behavior, exchange) as handle:
        client = handle.bind_as_client()
        assert client.mailbox_id is not None
        # Should log but not crash
        client.exchange.send(
            client.mailbox_id,
            PingRequest(src=client.agent_id, dest=client.mailbox_id),
        )
        assert client.ping() > 0

        client.shutdown()


@pytest.mark.filterwarnings(
    'ignore:.*:pytest.PytestUnhandledThreadExceptionWarning',
)
def test_client_remote_handle_recv_thread_crash(exchange: Exchange) -> None:
    agent_id = exchange.register_agent(EmptyBehavior)

    with mock.patch(
        'academy.handle.ClientRemoteHandle._recv_responses',
        side_effect=Exception(),
    ):
        handle = ClientRemoteHandle(exchange, agent_id)

    with pytest.raises(
        RuntimeError,
        match='This likely means the listener thread crashed.',
    ):
        handle.close()


def test_client_remote_handle_actions(
    exchange: ThreadExchange,
    launcher: ThreadLauncher,
) -> None:
    behavior = CounterBehavior()
    with launcher.launch(behavior, exchange).bind_as_client() as handle:
        assert handle.ping() > 0

        handle.action('add', 1).result()
        count_future: Future[int] = handle.action('count')
        assert count_future.result() == 1

        handle.add(1).result()
        count_future = handle.count()
        assert count_future.result() == 2  # noqa: PLR2004

        handle.shutdown()


def test_client_remote_handle_errors(
    exchange: ThreadExchange,
    launcher: ThreadLauncher,
) -> None:
    behavior = ErrorBehavior()
    with launcher.launch(behavior, exchange).bind_as_client() as handle:
        with pytest.raises(RuntimeError, match='This action always fails.'):
            handle.action('fails').result()
        with pytest.raises(AttributeError, match='null'):
            handle.action('null').result()

        handle.shutdown()


def test_client_remote_handle_wait_futures(
    exchange: ThreadExchange,
    launcher: ThreadLauncher,
) -> None:
    behavior = SleepBehavior()
    unbound_handle = launcher.launch(behavior, exchange)
    handle = unbound_handle.bind_as_client()

    future: Future[None] = handle.action('sleep', TEST_SLEEP)
    handle.close(wait_futures=True)
    future.result(timeout=0)

    # Still need to shutdown agent to exit properly
    handle = unbound_handle.bind_as_client()
    handle.shutdown()


def test_client_remote_handle_cancel_futures(
    exchange: ThreadExchange,
    launcher: ThreadLauncher,
) -> None:
    behavior = SleepBehavior()
    unbound_handle = launcher.launch(behavior, exchange)
    handle = unbound_handle.bind_as_client()

    future: Future[None] = handle.action('sleep', TEST_SLEEP)
    handle.close(wait_futures=False)
    assert future.cancelled()

    # Still need to shutdown agent to exit properly
    handle = unbound_handle.bind_as_client()
    handle.shutdown()
