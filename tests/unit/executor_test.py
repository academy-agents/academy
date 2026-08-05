from __future__ import annotations

import pytest

from academy.exchange import LocalExchangeTransport
from academy.exchange import UserExchangeClient
from academy.executor import _EventLoopHost
from academy.executor import EventLoopExecutor
from academy.manager import _run_agent_on_worker
from academy.manager import _RunSpec
from academy.manager import Manager
from academy.runtime import RuntimeConfig
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
        executors=executor,
    ) as manager:
        handles = [
            await manager.launch(IdentityAgent(), executor='default')
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
async def test_no_cancel_future(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    factory = exchange_client.factory()
    executor = EventLoopExecutor(factory)

    client = await factory.create_user_client(name='test')
    registration = await client.register_agent(IdentityAgent, name='test')

    spec = _RunSpec(
        agent=IdentityAgent,
        config=RuntimeConfig(),
        exchange_factory=factory,
        registration=registration,
        agent_args=(),
        agent_kwargs={},
        submit_kwargs={},
    )

    future = executor.submit(_run_agent_on_worker, spec)

    executor.shutdown(wait=False, cancel_futures=False)

    assert not future.cancelled()

    executor._thread.join()
    await client.close()


@pytest.mark.asyncio
async def test_cancel_future(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    factory = exchange_client.factory()
    executor = EventLoopExecutor(factory)

    client = await factory.create_user_client(name='test')
    registration = await client.register_agent(IdentityAgent, name='test')

    spec = _RunSpec(
        agent=IdentityAgent,
        config=RuntimeConfig(),
        exchange_factory=factory,
        registration=registration,
        agent_args=(),
        agent_kwargs={},
        submit_kwargs={},
    )

    future = executor.submit(_run_agent_on_worker, spec)

    assert future in executor._pending_futures

    executor.shutdown(wait=True, cancel_futures=True)

    assert future.cancelled()

    await client.close()


@pytest.mark.asyncio
async def test_non_manager_function_submit(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:

    factory = exchange_client.factory()
    executor = EventLoopExecutor(factory)

    def test_fn() -> None:  # pragma: no cover
        pass

    with pytest.raises(
        ValueError,
        match='Only functions of the type _run_agent_on_worker are allowed to be submitted',  # noqa: E501
    ):
        executor.submit(test_fn)

    executor.shutdown()
