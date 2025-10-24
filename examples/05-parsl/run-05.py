from __future__ import annotations

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

import parsl
from parsl import Config
from parsl import HighThroughputExecutor

from academy.agent import action
from academy.agent import Agent
from academy.exchange.cloud import spawn_http_exchange
from academy.logging import init_logging
from academy.manager import Manager

EXCHANGE_PORT = 5346
logger = logging.getLogger(__name__)


@parsl.python_app
def expensive_task() -> int:
    # Do expensive task
    return 42


class SimulationAgent(Agent):
    def __init__(self) -> None:
        self.config = Config(
            executors=[
                HighThroughputExecutor(),
            ],
        )

    async def agent_on_startup(self) -> None:
        self.dfk = parsl.load(self.config)

    async def agent_on_shutdown(self) -> None:
        self.dfk.cleanup()
        self.dfk = None
        parsl.clear()

    @action
    async def run_expensive_task(self) -> int:
        return await asyncio.wrap_future(expensive_task())


async def main() -> int:
    init_logging(logging.INFO)

    with spawn_http_exchange('localhost', EXCHANGE_PORT) as factory:
        executor = ThreadPoolExecutor()

        async with await Manager.from_exchange_factory(
            factory=factory,
            executors=executor,
        ) as manager:
            agent = await manager.launch(SimulationAgent)
            expected = 42

            logger.info(
                'Invoking parsl task on %s',
                agent.agent_id,
            )
            result = await agent.run_expensive_task()
            assert result == expected
            logger.info('The answer to life is: "%s"', result)

        # Upon exit, the Manager context will instruct each agent to shutdown,
        # closing their respective handles, and shutting down the executors.

    return 0


if __name__ == '__main__':
    raise SystemExit(asyncio.run(main()))
