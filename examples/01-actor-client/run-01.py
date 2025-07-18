from __future__ import annotations

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

from academy.agent import action
from academy.agent import Agent
from academy.exchange import LocalExchangeFactory
from academy.logging import init_logging
from academy.manager import Manager


class Counter(Agent):
    count: int

    async def agent_on_startup(self) -> None:
        self.count = 0

    @action
    async def increment(self, value: int = 1) -> None:
        self.count += value

    @action
    async def get_count(self) -> int:
        return self.count


async def main() -> int:
    init_logging(logging.INFO)

    async with await Manager.from_exchange_factory(
        factory=LocalExchangeFactory(),
        executors=ThreadPoolExecutor(),
    ) as manager:
        agent_handle = await manager.launch(Counter)

        count = await agent_handle.get_count()
        assert count == 0

        await agent_handle.increment()

        count = await agent_handle.get_count()
        assert count == 1

    return 0


if __name__ == '__main__':
    raise SystemExit(asyncio.run(main()))
