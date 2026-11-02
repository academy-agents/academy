from __future__ import annotations

import asyncio
import os
import pickle

from academy.agent import Agent
from academy.exchange import HttpExchangeFactory
from academy.manager import Manager

print('importing agent test')


class TestAgent(Agent):
    pass


async def async_main():
    print('in agent test main')

    factory = HttpExchangeFactory('http://localhost:1234')

    async with await Manager.from_exchange_factory(
        factory=factory,
    ) as manager:
        agent_h = await manager.launch(TestAgent)
        print(f'Agent handle is: {agent_h}')

        with open('agent.handle.tmp', 'wb') as f:
            pickle.dump(agent_h, f)
        os.rename('agent.handle.tmp', 'agent.handle')

        await agent_h.ping()
        while True:
            print('looping forever')
            await asyncio.sleep(60)


if __name__ == '__main__':
    asyncio.run(async_main())

print('end of agent test import')
