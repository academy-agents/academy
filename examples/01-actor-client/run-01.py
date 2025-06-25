from __future__ import annotations

import asyncio
import logging

from academy.behavior import action
from academy.behavior import Behavior
from academy.exchange.local import LocalExchangeFactory
from academy.launcher import ThreadLauncher
from academy.logging import init_logging
from academy.manager import Manager


class Counter(Behavior):
    count: int

    async def on_setup(self) -> None:
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
        launcher=ThreadLauncher(),
    ) as manager:
        behavior = Counter()
        agent_handle = await manager.launch(behavior)

        count_future = await agent_handle.get_count()
        await count_future
        assert count_future.result() == 0

        inc_future = await agent_handle.increment()
        await inc_future

        count_future = await agent_handle.get_count()
        await count_future
        assert count_future.result() == 1

    return 0


if __name__ == '__main__':
    raise SystemExit(asyncio.run(main()))
