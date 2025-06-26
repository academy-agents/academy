from __future__ import annotations

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

from academy.behavior import action
from academy.behavior import Behavior
from academy.exchange.local import LocalExchangeFactory
from academy.handle import Handle
from academy.logging import init_logging
from academy.manager import Manager

logger = logging.getLogger(__name__)


class Coordinator(Behavior):
    def __init__(
        self,
        lowerer: Handle[Lowerer],
        reverser: Handle[Reverser],
    ) -> None:
        self.lowerer = lowerer
        self.reverser = reverser

    @action
    async def process(self, text: str) -> str:
        future = await self.lowerer.lower(text)
        text = await future
        future = await self.reverser.reverse(text)
        text = await future
        return text


class Lowerer(Behavior):
    @action
    async def lower(self, text: str) -> str:
        return text.lower()


class Reverser(Behavior):
    @action
    async def reverse(self, text: str) -> str:
        return text[::-1]


async def main() -> int:
    init_logging(logging.INFO)

    async with await Manager.from_exchange_factory(
        factory=LocalExchangeFactory(),
        executors=ThreadPoolExecutor(),
    ) as manager:
        lowerer = await manager.launch(Lowerer())
        reverser = await manager.launch(Reverser())
        coordinator = await manager.launch(Coordinator(lowerer, reverser))

        text = 'DEADBEEF'
        expected = 'feebdaed'

        future = await coordinator.process(text)
        logger.info('Invoking process("%s") on %s', text, coordinator.agent_id)
        result = await future
        assert result == expected
        logger.info('Received result: "%s"', result)

    return 0


if __name__ == '__main__':
    raise SystemExit(asyncio.run(main()))
