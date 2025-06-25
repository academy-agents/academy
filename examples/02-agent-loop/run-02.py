from __future__ import annotations

import asyncio
import logging

from academy.behavior import action
from academy.behavior import Behavior
from academy.behavior import loop
from academy.exchange.local import LocalExchangeFactory
from academy.launcher import ThreadLauncher
from academy.logging import init_logging
from academy.manager import Manager

logger = logging.getLogger(__name__)


class Counter(Behavior):
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
        launcher=ThreadLauncher(),
    ) as manager:
        behavior = Counter()
        agent = await manager.launch(behavior)

        logger.info('Waiting 2s for agent loops to execute...')
        await asyncio.sleep(2)

        future = await agent.get_count()
        await future
        assert future.result() >= 1
        logger.info('Agent loop executed %s time(s)', future.result())

    return 0


if __name__ == '__main__':
    raise SystemExit(asyncio.run(main()))
