import asyncio
import pickle

from academy.agent import Agent
from academy.exchange import HttpExchangeFactory
from academy.manager import Manager

print("importing client test")

async def async_main():
    print("in agent test main")

    factory = HttpExchangeFactory(f'http://localhost:1234')

    async with await Manager.from_exchange_factory(
        factory=factory,
        ) as manager:
            with open("agent.handle", "rb") as f:
                agent_h = pickle.load(f)
            print(f"Agent handle is {agent_h}")

            await agent_h.ping()

            print("ping completed. exiting.")

if __name__ == "__main__":
    asyncio.run(async_main())

print("end of client test import")
