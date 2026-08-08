import asyncio

from academy.agent import Agent
from academy.exchange import HttpExchangeFactory
from academy.manager import Manager

print("importing agent test")

class TestAgent(Agent):
    pass

async def async_main():
    print("in agent test main")

    factory = HttpExchangeFactory(f'http://localhost:1234')

    async with await Manager.from_exchange_factory(
        factory=factory,
        ) as manager:
            agent_h = await manager.launch(TestAgent)
            await agent_h.ping()
            while True:
                print("looping forever")
                await asyncio.sleep(60)
             

if __name__ == "__main__":
    asyncio.run(async_main())

print("end of agent test import")
