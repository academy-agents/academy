from __future__ import annotations

import asyncio
import time
from unittest import mock

import pytest

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
    executor = EventLoopExecutor(factory)

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
    executor = EventLoopExecutor(factory)

    def test_fn() -> None:  # pragma: no cover
        pass

    executor.shutdown()

    with pytest.raises(
        RuntimeError,
        match='Cannot submit after host shutdown',
    ):
        executor.submit(test_fn)


@pytest.mark.asyncio
async def test_submit_base_exception(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:

    factory = exchange_client.factory()
    executor = EventLoopExecutor(factory)

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

    executor.shutdown()


async def test_no_cancel_future(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:

    factory = exchange_client.factory()
    executor = EventLoopExecutor(factory)

    def test_fn() -> None:
        time.sleep(2)

    future = executor.submit(test_fn)

    executor.shutdown(wait=False, cancel_futures=False)

    assert not future.cancelled()

    executor._thread.join()
