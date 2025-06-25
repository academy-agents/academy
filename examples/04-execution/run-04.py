from __future__ import annotations

import asyncio
import logging
import multiprocessing
from concurrent.futures import ProcessPoolExecutor

from academy.behavior import action
from academy.behavior import Behavior
from academy.exchange.cloud.client import spawn_http_exchange
from academy.handle import Handle
from academy.launcher import Launcher
from academy.logging import init_logging
from academy.manager import Manager

EXCHANGE_PORT = 5346
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

    with spawn_http_exchange('localhost', EXCHANGE_PORT) as factory:
        mp_context = multiprocessing.get_context('spawn')
        executor = ProcessPoolExecutor(max_workers=3, mp_context=mp_context)
        async with await Manager.from_exchange_factory(
            factory=factory,
            # Agents are launched using a Launcher. The Launcher can
            # use any concurrent.futures.Executor (here, a ProcessPoolExecutor)
            # to execute agents.
            launcher=Launcher(executor),
        ) as manager:
            # Initialize and launch each of the three agents. The returned
            # type is a handle to that agent used to invoke actions.
            lowerer = await manager.launch(Lowerer())
            reverser = await manager.launch(Reverser())
            coordinator = await manager.launch(Coordinator(lowerer, reverser))

            text = 'DEADBEEF'
            expected = 'feebdaed'

            future = await coordinator.process(text)
            logger.info(
                'Invoking process("%s") on %s',
                text,
                coordinator.agent_id,
            )
            result = await future
            assert result == expected
            logger.info('Received result: "%s"', result)

        # Upon exit, the Manager context will instruct each agent to shutdown
        # and then close the handles, exchange, and launcher interfaces.

    return 0


if __name__ == '__main__':
    raise SystemExit(asyncio.run(main()))
