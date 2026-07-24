from __future__ import annotations

import asyncio
import re
from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor
from unittest import mock

import pytest

from academy.exchange import LocalExchangeFactory
from academy.exchange import LocalExchangeTransport
from academy.exchange import UserExchangeClient
from academy.executor import _EventLoopHost
from academy.executor import EventLoopExecutor
from academy.manager import Manager
from testing.agents import IdentityAgent


def _add(a: int, b: int) -> int:
    return a + b


async def _async_add(a: int, b: int) -> int:
    return a + b


@pytest.mark.asyncio
async def test_event_loop_host(
    manager: Manager[LocalExchangeTransport],
) -> None:

    host = await manager.launch(_EventLoopHost)

    assert await host.submit(_add, (1, 2), {}) == 3  # noqa: PLR2004
    assert await host.submit(_async_add, (1, 2), {}) == 3  # noqa: PLR2004

    await manager.shutdown(host)


@pytest.mark.asyncio
async def test_event_loop_pack_agents(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    factory = exchange_client.factory()
    executor = EventLoopExecutor(ThreadPoolExecutor(max_workers=1), factory)

    async with await Manager.from_exchange_factory(
        factory=factory,
        executors={'multiple': executor},
    ) as manager:
        handles = [
            await manager.launch(IdentityAgent(), executor='multiple')
            for _ in range(3)
        ]

        for handle in handles:
            assert await handle.identity('hello') == 'hello'


@pytest.mark.asyncio
async def test_host_shutdown_submit(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:

    factory = exchange_client.factory()
    executor = EventLoopExecutor(ThreadPoolExecutor(max_workers=1), factory)

    def test_fn() -> None:  # pragma: no cover
        pass

    executor.shutdown()

    with pytest.raises(
        RuntimeError,
        match='Cannot submit after host shutdown',
    ):
        executor.submit(test_fn)


@pytest.mark.asyncio
async def test_aclose_with_no_launch(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:

    factory = exchange_client.factory()
    executor = EventLoopExecutor(ThreadPoolExecutor(max_workers=1), factory)

    await executor.aclose()
    assert executor._shutdown is True


def test_no_event_loop_submit() -> None:
    factory = LocalExchangeFactory()
    executor = EventLoopExecutor(ThreadPoolExecutor(max_workers=1), factory)

    def test_fn() -> None:  # pragma: no cover
        pass

    with pytest.raises(
        RuntimeError,
        match=re.escape(
            'EventloopExecutor submit requires a running event loop to be '
            'used for collecting multiple agent runtimes',
        ),
    ):
        executor.submit(test_fn)


@pytest.mark.asyncio
async def test_cancel_future(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    factory = exchange_client.factory()
    executor = EventLoopExecutor(ThreadPoolExecutor(max_workers=1), factory)

    def test_fn() -> None:  # pragma: no cover
        pass

    future: Future[None] = Future()
    assert future.cancel()

    await executor._submit_async(test_fn, (), {}, future)

    assert future.cancelled()
    await executor.aclose()


@pytest.mark.asyncio
async def test_submit_base_exception(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:

    factory = exchange_client.factory()
    executor = EventLoopExecutor(ThreadPoolExecutor(max_workers=1), factory)

    def test_fn() -> None:  # pragma: no cover
        pass

    first = executor.submit(test_fn)
    await asyncio.wrap_future(first)

    failing_submit = mock.AsyncMock(
        side_effect=RuntimeError('Injected submit failure on host'),
    )

    with (  # noqa: PT012
        mock.patch.object(
            executor._host,
            'submit',
            new=failing_submit,
        ),
        pytest.raises(RuntimeError, match='Injected submit failure on host'),
    ):
        test = executor.submit(test_fn)
        await asyncio.wrap_future(test)

    await executor.aclose()
