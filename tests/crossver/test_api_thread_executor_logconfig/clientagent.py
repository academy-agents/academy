# this is copied literally from
# https://docs.academy-agents.org/stable/get-started/

import asyncio
from concurrent.futures import ThreadPoolExecutor
from academy.agent import Agent, action
from academy.exchange import LocalExchangeFactory
from academy.logging.recommended import recommended_logging
from academy.manager import Manager

class ExampleAgent(Agent):  
    
  @action  
  async def square(self, value: float) -> float:
    return value * value

async def main() -> None:
  async with await Manager.from_exchange_factory(  
    factory=LocalExchangeFactory(),  
    executors=ThreadPoolExecutor(),  
    log_config=recommended_logging(),
  ) as manager:
        
    agent_handle = await manager.launch(ExampleAgent())  
    result = await agent_handle.square(2)  
    assert result == 4
    await agent_handle.shutdown()  

if __name__ == '__main__':
  asyncio.run(main())

