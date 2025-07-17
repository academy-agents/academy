from __future__ import annotations

import asyncio
from typing import Any

import pytest

from academy.exception import AgentTerminatedError
from academy.exception import ExchangeClientNotFoundError
from academy.exception import HandleClosedError
from academy.exception import HandleReuseError
from academy.exchange import UserExchangeClient
from academy.exchange.local import LocalExchangeFactory
from academy.exchange.local import LocalExchangeTransport
from academy.exchange.transport import MailboxStatus
from academy.handle import exchange_context
from academy.handle import ProxyHandle
from academy.handle import RemoteHandle
from academy.manager import Manager
from academy.message import Message
from academy.message import PingRequest
from testing.agents import CounterAgent
from testing.agents import EmptyAgent
from testing.agents import ErrorAgent
from testing.agents import SleepAgent
from testing.constant import TEST_SLEEP_INTERVAL


@pytest.mark.asyncio
async def test_proxy_handle_protocol() -> None:
    agent = EmptyAgent()
    handle = ProxyHandle(agent)
    assert str(agent) in str(handle)
    assert repr(agent) in repr(handle)
    assert await handle.ping() >= 0
    await handle.shutdown()


@pytest.mark.asyncio
async def test_proxy_handle_actions() -> None:
    handle = ProxyHandle(CounterAgent())

    # Via Handle.action()
    await handle.action('add', 1)
    count: int = await handle.action('count')
    assert count == 1

    # Via attribute lookup
    await handle.add(1)
    count = await handle.count()
    assert count == 2  # noqa: PLR2004


@pytest.mark.asyncio
async def test_proxy_handle_action_errors() -> None:
    handle = ProxyHandle(ErrorAgent())

    with pytest.raises(RuntimeError, match='This action always fails.'):
        await handle.action('fails')

    with pytest.raises(AttributeError, match='null'):
        await handle.action('null')

    with pytest.raises(AttributeError, match='null'):
        await handle.null()  # type: ignore[attr-defined]

    handle.agent.foo = 1  # type: ignore[attr-defined]
    with pytest.raises(AttributeError, match='not a method'):
        await handle.foo()  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_proxy_handle_closed_errors() -> None:
    handle = ProxyHandle(EmptyAgent())
    await handle.close()

    with pytest.raises(HandleClosedError):
        await handle.action('test')
    with pytest.raises(HandleClosedError):
        await handle.ping()
    with pytest.raises(HandleClosedError):
        await handle.shutdown()


@pytest.mark.asyncio
async def test_proxy_handle_agent_shutdown_errors() -> None:
    handle = ProxyHandle(EmptyAgent())
    await handle.shutdown()

    with pytest.raises(AgentTerminatedError):
        await handle.action('test')
    with pytest.raises(AgentTerminatedError):
        await handle.ping()
    with pytest.raises(AgentTerminatedError):
        await handle.shutdown()


@pytest.mark.asyncio
async def test_remote_handle_closed_error(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    registration = await exchange_client.register_agent(EmptyAgent)
    handle = RemoteHandle(registration.agent_id, exchange_client)
    await handle.close()
    assert handle.closed()

    assert handle.client_id is not None
    with pytest.raises(HandleClosedError):
        await handle.action('foo')
    with pytest.raises(HandleClosedError):
        await handle.ping()
    with pytest.raises(HandleClosedError):
        await handle.shutdown()


@pytest.mark.asyncio
async def test_agent_remote_handle_serialize(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    registration = await exchange_client.register_agent(EmptyAgent)
    async with RemoteHandle(registration.agent_id, exchange_client) as handle:
        # Note: don't call pickle.dumps here because ThreadExchange
        # is not pickleable so we test __reduce__ directly.
        class_, args = handle.__reduce__()
        reconstructed = class_(*args)
        assert isinstance(reconstructed, RemoteHandle)
        assert reconstructed.agent_id == handle.agent_id
        # _exchange in handle is empty
        assert reconstructed._exchange is None
        # exchange is returned from context variable
        assert reconstructed.exchange == exchange_client
        assert str(reconstructed) == str(handle)
        assert repr(reconstructed) == repr(handle)


@pytest.mark.asyncio
async def test_agent_remote_handle_context() -> None:
    # We cannot use the fixture here because the fixture will create context
    factory = LocalExchangeFactory()
    exchange_client = await factory.create_user_client()
    registration = await exchange_client.register_agent(EmptyAgent)
    async with RemoteHandle(registration.agent_id) as handle:
        with pytest.raises(ExchangeClientNotFoundError):
            assert handle.exchange == exchange_client

        with pytest.raises(ExchangeClientNotFoundError):
            assert handle.client_id is not None

        unbound_repr = repr(handle)
        unbound_str = str(handle)

        exchange_context.set(exchange_client)
        assert handle.exchange == exchange_client
        assert unbound_repr != repr(handle)
        assert unbound_str != str(handle)


@pytest.mark.asyncio
async def test_agent_remote_handle_clone() -> None:
    # We cannot use the fixture here because the fixture will create context
    factory = LocalExchangeFactory()
    exchange_client = await factory.create_user_client()
    registration = await exchange_client.register_agent(EmptyAgent)
    async with RemoteHandle(registration.agent_id, exchange_client) as handle:
        cloned = handle.clone()

        with pytest.raises(ExchangeClientNotFoundError):
            assert cloned.exchange == exchange_client


@pytest.mark.asyncio
async def test_agent_remote_handle_reuse(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    registration = await exchange_client.register_agent(EmptyAgent)
    async with RemoteHandle(
        registration.agent_id,
        exchange_client,
    ) as handle:
        # Context and exchange match
        assert handle.exchange == exchange_client

    async with RemoteHandle(registration.agent_id) as handle:
        # Exchange is inferred
        assert handle.exchange == exchange_client

        factory = exchange_client.factory()
        async with await factory.create_user_client() as new_client:
            # New client sets its own context
            with pytest.raises(HandleReuseError):
                assert handle.exchange == new_client

            # Cloning fixes the problem
            assert handle.clone().exchange == new_client

    async with RemoteHandle(registration.agent_id) as handle:
        factory = exchange_client.factory()
        async with await factory.create_user_client() as new_client:
            # Binding is lazy
            assert handle.exchange == new_client


@pytest.mark.asyncio
async def test_agent_remote_handle_bind(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    registration = await exchange_client.register_agent(EmptyAgent)
    factory = exchange_client.factory()

    async def _handler(_: Any) -> None:  # pragma: no cover
        pass

    async with await factory.create_agent_client(
        registration,
        request_handler=_handler,
    ) as client:
        with pytest.raises(
            ValueError,
            match='Cannot create handle to self.',
        ):
            RemoteHandle(registration.agent_id, client)


@pytest.mark.asyncio
async def test_client_remote_handle_ping_timeout(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    registration = await exchange_client.register_agent(EmptyAgent)
    handle = RemoteHandle(registration.agent_id, exchange_client)
    with pytest.raises(TimeoutError):
        await handle.ping(timeout=TEST_SLEEP_INTERVAL)


@pytest.mark.asyncio
async def test_client_remote_handle_log_bad_response(
    manager: Manager[LocalExchangeTransport],
) -> None:
    handle = await manager.launch(EmptyAgent())
    # Should log two messages but not crash:
    #   - User client got an unexpected ping request from agent client
    #   - Agent client got an unexpected ping response (containing an
    #     error produced by user) with no corresponding handle to
    #     send the response to.
    await handle.exchange.send(
        Message.create(
            src=handle.agent_id,
            dest=handle.client_id,
            body=PingRequest(),
        ),
    )
    assert await handle.ping() > 0
    await handle.shutdown()


@pytest.mark.asyncio
async def test_client_remote_handle_actions(
    manager: Manager[LocalExchangeTransport],
) -> None:
    handle = await manager.launch(CounterAgent())
    assert await handle.ping() > 0

    await handle.action('add', 1)
    count: int = await handle.action('count')
    assert count == 1

    await handle.add(1)
    count = await handle.count()
    assert count == 2  # noqa: PLR2004

    await handle.shutdown()


@pytest.mark.parametrize('terminate', (True, False))
@pytest.mark.asyncio
async def test_client_remote_shutdown_termination(
    terminate: bool,
    manager: Manager[LocalExchangeTransport],
) -> None:
    handle = await manager.launch(EmptyAgent())
    await handle.shutdown(terminate=terminate)
    await manager.wait({handle})
    status = await manager.exchange_client.status(handle.agent_id)
    if terminate:
        assert status == MailboxStatus.TERMINATED
    else:
        assert status == MailboxStatus.ACTIVE


@pytest.mark.asyncio
async def test_client_remote_handle_errors(
    manager: Manager[LocalExchangeTransport],
) -> None:
    handle = await manager.launch(ErrorAgent())
    with pytest.raises(
        RuntimeError,
        match='This action always fails.',
    ):
        await handle.fails()

    with pytest.raises(AttributeError, match='null'):
        await handle.action('null')

    await handle.shutdown()


@pytest.mark.asyncio
async def test_client_remote_handle_wait_futures(
    manager: Manager[LocalExchangeTransport],
) -> None:
    handle = await manager.launch(SleepAgent())
    sleep_task = asyncio.create_task(handle.sleep(TEST_SLEEP_INTERVAL))

    # Need to ensure that sleep_task starts running before closing the handle
    for _ in range(10):
        await asyncio.sleep(0)

    await handle.close(wait_futures=True)
    await sleep_task

    # Create a new, non-closed handle to shutdown the agent
    shutdown_handle = manager.get_handle(handle.agent_id)
    await shutdown_handle.shutdown()
    await manager.wait({handle.agent_id})


@pytest.mark.asyncio
async def test_client_remote_handle_cancel_futures(
    manager: Manager[LocalExchangeTransport],
) -> None:
    handle = await manager.launch(SleepAgent())
    sleep_task = asyncio.create_task(handle.sleep(TEST_SLEEP_INTERVAL))

    # Need to ensure that sleep_task starts running before closing the handle
    for _ in range(10):
        await asyncio.sleep(0)

    await handle.close(wait_futures=False)
    with pytest.raises(asyncio.CancelledError):
        await sleep_task

    # Create a new, non-closed handle to shutdown the agent
    async with manager.get_handle(handle.agent_id) as shutdown_handle:
        await shutdown_handle.shutdown()
    await manager.wait({handle.agent_id})
