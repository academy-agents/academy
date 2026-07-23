from __future__ import annotations

import warnings
from concurrent.futures import ThreadPoolExecutor

import pytest

from academy.exchange import LocalExchangeTransport
from academy.exchange import UserExchangeClient
from academy.executor import _EventLoopHost
from academy.executor import EventLoopExecutor
from academy.manager import Manager
from testing.agents import IdentityAgent
from academy.exception import ActionCancelledError


def _add(a:int, b:int) -> int:
    return a+b

async def _async_add (a:int, b:int) -> int:
    return a+b

@pytest.mark.asyncio
async def test_event_loop_host(
    manager: Manager[LocalExchangeTransport]
)-> None:
    
    host = await manager.launch(_EventLoopHost)

    assert await host.submit(_add, (1,2), {}) == 3
    assert await host.submit(_async_add, (1,2), {}) == 3

    await manager.shutdown(host)

@pytest.mark.asyncio
async def test_event_loop_pack_agents(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    factory = exchange_client.factory()
    executor = EventLoopExecutor(ThreadPoolExecutor(max_workers=1), factory)

    async with await Manager.from_exchange_factory(
        factory=factory,
        executors={'multiple':executor},
    ) as manager:
        handles = [
            await manager.launch(IdentityAgent(), executor='multiple')
            for _ in range (3)
        ]

        for handle in handles:
            assert await handle.identity('hello') == 'hello'







