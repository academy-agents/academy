from __future__ import annotations

import asyncio
import pickle
from concurrent.futures import ThreadPoolExecutor

from academy.agent import action
from academy.agent import Agent
from academy.exchange import LocalExchangeFactory
from academy.manager import Manager


class ExampleAgent(Agent):
    @action
    async def square(self, value: float) -> float:
        return value * value


async def main() -> None:
    async with await Manager.from_exchange_factory(
        factory=LocalExchangeFactory(),
        executors=ThreadPoolExecutor(),
    ) as manager:
        agent_handle = await manager.launch(ExampleAgent())
        print(f'Agent handle is: {agent_handle}')

        with open('pickle.handle', 'wb') as f:
            pickle.dump(agent_handle, f)

if __name__ == "__main__":
    asyncio.run(main())
