from __future__ import annotations

import asyncio
import pickle

from academy.exchange import HttpExchangeFactory
from academy.exchange.mailbox_status import MailboxStatus
from academy.manager import Manager

print('importing client test')


async def async_main():
    print('in agent test main')

    factory = HttpExchangeFactory('http://localhost:1234')

    async with await Manager.from_exchange_factory(
        factory=factory,
    ) as manager:
        with open('agent.handle', 'rb') as f:
            agent_h = pickle.load(f)
        print(f'Agent handle is {agent_h}')

        await agent_h.ping()
        print('ping completed.')

        status = await manager.exchange_client.status(agent_h.agent_id)

        print(f'status is: {status}')
        assert status == MailboxStatus.ACTIVE

        print('status call completed.')


if __name__ == '__main__':
    asyncio.run(async_main())

print('end of client test import')
