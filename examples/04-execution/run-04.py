from __future__ import annotations

import asyncio
import logging
import multiprocessing
from concurrent.futures import ProcessPoolExecutor

from academy.agent import action
from academy.agent import Agent
from academy.exchange.cloud import spawn_http_exchange
from academy.handle import Handle
from academy.logging import init_logging
from academy.manager import Manager

EXCHANGE_PORT = 5346
logger = logging.getLogger(__name__)


class Coordinator(Agent):
    def __init__(
        self,
        lowerer: Handle[Lowerer],
        reverser: Handle[Reverser],
    ) -> None:
        super().__init__()
        self.lowerer = lowerer
        self.reverser = reverser

    @action
    async def process(self, text: str) -> str:
        text = await self.lowerer.lower(text)
        text = await self.reverser.reverse(text)
        return text


class Lowerer(Agent):
    @action
    async def lower(self, text: str) -> str:
        return text.lower()


class Reverser(Agent):
    @action
    async def reverse(self, text: str) -> str:
        return text[::-1]


async def main() -> int:
    init_logging(logging.INFO)

    with spawn_http_exchange('localhost', EXCHANGE_PORT) as factory:
        mp_context = multiprocessing.get_context('spawn')
        executor = ProcessPoolExecutor(
            max_workers=3,
            initializer=init_logging,
            mp_context=mp_context,
        )

        async with await Manager.from_exchange_factory(
            factory=factory,
            # Agents are run by the manager in the processes of this
            # process pool executor.
            executors=executor,
        ) as manager:
            # Launch each of the three agents types. The returned type is
            # a handle to that agent used to invoke actions.
            lowerer = await manager.launch(Lowerer)
            reverser = await manager.launch(Reverser)
            coordinator = await manager.launch(
                Coordinator,
                args=(lowerer, reverser),
            )

            text = 'DEADBEEF'
            expected = 'feebdaed'

            logger.info(
                'Invoking process("%s") on %s',
                text,
                coordinator.agent_id,
            )
            result = await coordinator.process(text)
            assert result == expected
            logger.info('Received result: "%s"', result)

        # Upon exit, the Manager context will instruct each agent to shutdown,
        # closing their respective handles, and shutting down the executors.

    return 0


if __name__ == '__main__':
    raise SystemExit(asyncio.run(main()))
