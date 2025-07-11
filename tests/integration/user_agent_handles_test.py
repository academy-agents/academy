from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pytest

from academy.agent import action
from academy.agent import Agent
from academy.exchange.local import LocalExchangeFactory
from academy.manager import Manager


class Counter(Agent):
    def __init__(self) -> None:
        super().__init__()
        self.count = 0

    @action
    async def increment(self, value: int = 1) -> None:
        self.count += value

    @action
    async def get_count(self) -> int:
        return self.count


@pytest.mark.asyncio
async def test_user_agent_handles() -> None:
    async with await Manager.from_exchange_factory(
        factory=LocalExchangeFactory(),
        executors=ThreadPoolExecutor(),
    ) as manager:
        agent_handle = await manager.launch(Counter)

        assert await agent_handle.ping() > 0

        count_future = await agent_handle.get_count()
        await count_future
        assert count_future.result() == 0

        inc_future = await agent_handle.increment()
        await inc_future

        count_future = await agent_handle.get_count()
        await count_future
        assert count_future.result() == 1

        await agent_handle.shutdown()
        await manager.wait({agent_handle}, timeout=1)
