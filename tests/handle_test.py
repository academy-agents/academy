from __future__ import annotations

import asyncio
import pickle
from typing import Any

import pytest

from academy.exception import HandleClosedError
from academy.exception import HandleNotBoundError
from academy.exception import MailboxClosedError
from academy.exchange import ExchangeClient
from academy.exchange import UserExchangeClient
from academy.handle import BoundRemoteHandle
from academy.handle import Handle
from academy.handle import ProxyHandle
from academy.handle import UnboundRemoteHandle
from academy.launcher import ThreadLauncher
from academy.message import PingRequest
from testing.behavior import CounterBehavior
from testing.behavior import EmptyBehavior
from testing.behavior import ErrorBehavior
from testing.behavior import SleepBehavior
from testing.constant import TEST_SLEEP


@pytest.mark.asyncio
async def test_proxy_handle_protocol() -> None:
    behavior = EmptyBehavior()
    handle = ProxyHandle(behavior)
    assert isinstance(handle, Handle)
    assert str(behavior) in str(handle)
    assert repr(behavior) in repr(handle)
    assert await handle.ping() >= 0
    await handle.shutdown()


@pytest.mark.asyncio
async def test_proxy_handle_actions() -> None:
    handle = ProxyHandle(CounterBehavior())

    # Via Handle.action()
    add_future: asyncio.Future[None] = await handle.action('add', 1)
    await add_future
    count_future: asyncio.Future[int] = await handle.action('count')
    assert await count_future == 1

    # Via attribute lookup
    add_future = await handle.add(1)
    await add_future
    count_future = await handle.count()
    assert await count_future == 2  # noqa: PLR2004


@pytest.mark.asyncio
async def test_proxy_handle_action_errors() -> None:
    handle = ProxyHandle(ErrorBehavior())

    fails_future: asyncio.Future[None] = await handle.action('fails')
    with pytest.raises(RuntimeError, match='This action always fails.'):
        await fails_future

    null_future: asyncio.Future[None] = await handle.action('null')
    with pytest.raises(AttributeError, match='null'):
        await null_future

    with pytest.raises(AttributeError, match='null'):
        await handle.null()  # type: ignore[attr-defined]

    handle.behavior.foo = 1  # type: ignore[attr-defined]
    with pytest.raises(AttributeError, match='not a method'):
        await handle.foo()  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_proxy_handle_closed_errors() -> None:
    handle = ProxyHandle(EmptyBehavior())
    await handle.close()

    with pytest.raises(HandleClosedError):
        await handle.action('test')
    with pytest.raises(HandleClosedError):
        await handle.ping()
    with pytest.raises(HandleClosedError):
        await handle.shutdown()


@pytest.mark.asyncio
async def test_proxy_handle_agent_shutdown_errors() -> None:
    handle = ProxyHandle(EmptyBehavior())
    await handle.shutdown()

    with pytest.raises(MailboxClosedError):
        await handle.action('test')
    with pytest.raises(MailboxClosedError):
        await handle.ping()
    with pytest.raises(MailboxClosedError):
        await handle.shutdown()


@pytest.mark.asyncio
async def test_unbound_remote_handle_serialize(
    exchange: UserExchangeClient[Any],
) -> None:
    registration = await exchange.register_agent(EmptyBehavior)
    handle = UnboundRemoteHandle(registration.agent_id)
    assert isinstance(handle, Handle)

    dumped = pickle.dumps(handle)
    reconstructed = pickle.loads(dumped)
    assert isinstance(reconstructed, UnboundRemoteHandle)
    assert str(reconstructed) == str(handle)
    assert repr(reconstructed) == repr(handle)


@pytest.mark.asyncio
async def test_unbound_remote_handle_bind(
    exchange: UserExchangeClient[Any],
) -> None:
    registration = await exchange.register_agent(EmptyBehavior)
    handle = UnboundRemoteHandle(registration.agent_id)
    async with await handle.bind_to_exchange(exchange) as agent_bound:
        assert isinstance(agent_bound, BoundRemoteHandle)


@pytest.mark.asyncio
async def test_unbound_remote_handle_errors(
    exchange: UserExchangeClient[Any],
) -> None:
    registration = await exchange.register_agent(EmptyBehavior)
    handle = UnboundRemoteHandle(registration.agent_id)
    with pytest.raises(HandleNotBoundError):
        await handle.action('foo')
    with pytest.raises(HandleNotBoundError):
        await handle.ping()
    with pytest.raises(HandleNotBoundError):
        await handle.close()
    with pytest.raises(HandleNotBoundError):
        await handle.shutdown()


@pytest.mark.asyncio
async def test_remote_handle_closed_error(
    exchange: UserExchangeClient[Any],
) -> None:
    registration = await exchange.register_agent(EmptyBehavior)
    handle = BoundRemoteHandle(
        exchange,
        registration.agent_id,
        exchange.user_id,
    )
    await handle.close()

    assert handle.mailbox_id is not None
    with pytest.raises(HandleClosedError):
        await handle.action('foo')
    with pytest.raises(HandleClosedError):
        await handle.ping()
    with pytest.raises(HandleClosedError):
        await handle.shutdown()


@pytest.mark.asyncio
async def test_agent_remote_handle_serialize(
    exchange: UserExchangeClient[Any],
) -> None:
    registration = await exchange.register_agent(EmptyBehavior)
    async with BoundRemoteHandle(
        exchange,
        registration.agent_id,
        exchange.user_id,
    ) as handle:
        # Note: don't call pickle.dumps here because ThreadExchange
        # is not pickleable so we test __reduce__ directly.
        class_, args = handle.__reduce__()
        reconstructed = class_(*args)
        assert isinstance(reconstructed, UnboundRemoteHandle)
        assert str(reconstructed) != str(handle)
        assert repr(reconstructed) != repr(handle)
        assert reconstructed.agent_id == handle.agent_id


@pytest.mark.asyncio
async def test_agent_remote_handle_bind(
    exchange: UserExchangeClient[Any],
) -> None:
    registration = await exchange.register_agent(EmptyBehavior)
    factory = exchange.factory()

    async def _handler(_: Any) -> None:  # pragma: no cover
        pass

    async with await factory.create_agent_client(
        registration,
        request_handler=_handler,
    ) as client:
        with pytest.raises(
            ValueError,
            match=f'Cannot create handle to {registration.agent_id}',
        ):
            await client.get_handle(registration.agent_id)


@pytest.mark.asyncio
async def test_client_remote_handle_log_bad_response(
    launcher: ThreadLauncher,
    exchange: ExchangeClient[Any],
) -> None:
    behavior = EmptyBehavior()
    async with await launcher.launch(behavior, exchange) as handle:
        # Should log two messages but not crash:
        #   - User client got an unexpected ping request from agent client
        #   - Agent client got an unexpected ping response (containing an
        #     error produced by user) with no corresponding handle to
        #     send the response to.
        await handle.exchange.send(
            PingRequest(src=handle.agent_id, dest=handle.mailbox_id),
        )
        assert await handle.ping() > 0

        await handle.shutdown()
        await handle.close()


@pytest.mark.asyncio
async def test_client_remote_handle_actions(
    launcher: ThreadLauncher,
    exchange: ExchangeClient[Any],
) -> None:
    behavior = CounterBehavior()
    async with await launcher.launch(behavior, exchange) as handle:
        assert await handle.ping() > 0

        future: asyncio.Future[None] = await handle.action('add', 1)
        await future
        count_future: asyncio.Future[int] = await handle.action('count')
        assert await count_future == 1

        future = await handle.add(1)
        await future
        count_future = await handle.count()
        assert await count_future == 2  # noqa: PLR2004

        await handle.shutdown()


@pytest.mark.asyncio
async def test_client_remote_handle_errors(
    launcher: ThreadLauncher,
    exchange: ExchangeClient[Any],
) -> None:
    behavior = ErrorBehavior()
    async with await launcher.launch(behavior, exchange) as handle:
        action_future = await handle.fails()
        with pytest.raises(
            RuntimeError,
            match='This action always fails.',
        ):
            await action_future

        null_future: asyncio.Future[None] = await handle.action('null')
        with pytest.raises(AttributeError, match='null'):
            await null_future

        await handle.shutdown()


@pytest.mark.asyncio
async def test_client_remote_handle_wait_futures(
    launcher: ThreadLauncher,
    exchange: ExchangeClient[Any],
) -> None:
    behavior = SleepBehavior()
    async with await launcher.launch(behavior, exchange) as handle:
        sleep_future = await handle.sleep(TEST_SLEEP)
        await handle.close(wait_futures=True)
        await sleep_future

        # Still need to shutdown agent to exit properly
        shutdown_handle = await exchange.get_handle(handle.agent_id)
        await shutdown_handle.shutdown()


@pytest.mark.asyncio
async def test_client_remote_handle_cancel_futures(
    launcher: ThreadLauncher,
    exchange: ExchangeClient[Any],
) -> None:
    behavior = SleepBehavior()
    async with await launcher.launch(behavior, exchange) as handle:
        sleep_future = await handle.sleep(TEST_SLEEP)
        await handle.close(wait_futures=False)

        with pytest.raises(asyncio.CancelledError):
            await sleep_future

        # Still need to shutdown agent to exit properly
        shutdown_handle = await exchange.get_handle(handle.agent_id)
        await shutdown_handle.shutdown()
