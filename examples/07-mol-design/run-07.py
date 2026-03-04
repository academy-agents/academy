from __future__ import annotations

import asyncio
import logging
import os

from mol_design_agents import XTBConfig
from mol_design_agents import XTBSimulationAgent
from parsl import Config
from parsl import HighThroughputExecutor
from parsl.concurrent import ParslPoolExecutor
from parsl.providers import LocalProvider

from academy.exchange import RedisExchangeFactory
from academy.manager import Manager

logger = logging.getLogger(__name__)


async def main() -> int:
    config = Config(
        executors=[
            HighThroughputExecutor(
                provider=LocalProvider(
                    worker_init=(
                        f'cd {os.getcwd()}conda activate ./mol-design;'
                    ),
                ),
                max_workers_per_node=2,
            ),
        ],
    )
    executor = ParslPoolExecutor(config)

    async with await Manager.from_exchange_factory(
        factory=RedisExchangeFactory('localhost', 6379),
        executors=executor,
    ) as manager:
        seeds = [
            'CNC(N)=O',
            'CC1=C(O)N=C(O)N1',
        ]

        agents = []
        for molecule in seeds:
            agents.append(
                await manager.launch(
                    XTBSimulationAgent,
                    args=(XTBConfig(), molecule),
                ),
            )

        print('Starting discovery campaign')
        print('=' * 80)
        while True:
            await asyncio.sleep(30)
            for i, agent in enumerate(agents):
                print(f'Progress report from agent {i}')
                report = await agent.report()
                print(f'Number of molecules simulated: {len(report)}')
                print(f'Five best molecules: {report[:5]}')
            print('=' * 80)

    return 0


if __name__ == '__main__':
    raise SystemExit(asyncio.run(main()))
