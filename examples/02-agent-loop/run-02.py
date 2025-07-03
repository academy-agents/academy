from __future__ import annotations

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

from academy.agent import action
from academy.agent import Agent
from academy.agent import loop
from academy.exchange.local import LocalExchangeFactory
from academy.logging import init_logging
from academy.manager import Manager

logger = logging.getLogger(__name__)


class Counter(Agent):
    count: int

    async def on_setup(self) -> None:
        self.count = 0

    @loop
    async def increment(self, shutdown: asyncio.Event) -> None:
        while not shutdown.is_set():
            await asyncio.sleep(1)
            self.count += 1

    @action
    async def get_count(self) -> int:
        return self.count


async def main() -> int:
    init_logging(logging.INFO)

    async with await Manager.from_exchange_factory(
        factory=LocalExchangeFactory(),
        executors=ThreadPoolExecutor(),
    ) as manager:
        agent = await manager.launch(Counter)

        logger.info('Waiting 2s for agent loops to execute...')
        await asyncio.sleep(2)

        future = await agent.get_count()
        await future
        assert future.result() >= 1
        logger.info('Agent loop executed %s time(s)', future.result())

    return 0


if __name__ == '__main__':
    raise SystemExit(asyncio.run(main()))
