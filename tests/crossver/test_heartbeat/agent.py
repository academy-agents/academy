from __future__ import annotations

import asyncio
import pickle

from academy.agent import Agent
from academy.exchange import HttpExchangeFactory
from academy.exchange.client_config import ExchangeClientConfig
from academy.manager import Manager

# this requires academy >= dff06fc3bdfe1b906cc9adb9490cc2e22d1406b1 > 0.5.0
# for config parameter and ability to set timeouts
# on the Python API surface.
# i still have to determine what needs to be on the other side of the
# network API surface? dff0... or 0.5.0 (or less)?


class TestAgent(Agent):
    pass


async def async_main():
    print('in agent test main')

    factory = HttpExchangeFactory(
        'http://localhost:1234',
        config=ExchangeClientConfig(
            heartbeat_interval=0.2,
            state_heartbeat_threshold=3,
        ),
    )

    async with await Manager.from_exchange_factory(
        factory=factory,
    ) as manager:
        agent_h = await manager.launch(TestAgent)
        print(f'Agent handle is: {agent_h}')

        # TODO: are handles expected to be pickleable across minor versions?
        with open('agent.handle', 'wb') as f:
            pickle.dump(agent_h, f)

        await agent_h.ping()
        while True:
            print('looping forever')
            await asyncio.sleep(60)


if __name__ == '__main__':
    asyncio.run(async_main())

print('end of agent test import')
