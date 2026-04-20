from __future__ import annotations

import pickle
import sys
from collections.abc import AsyncGenerator
from typing import Any
from unittest import mock

import pytest_asyncio

from academy.exchange import ExchangeFactory
from academy.exchange.client import ExchangeClient
from academy.remote import _main
from testing.agents import SleepAgent
from testing.fixture import EXCHANGE_FACTORY_TYPES


@pytest_asyncio.fixture(params=EXCHANGE_FACTORY_TYPES)
async def client(
    request,
    get_factory,
) -> AsyncGenerator[ExchangeFactory[Any]]:
    factory = get_factory(request.param)
    client = await factory.create_user_client(start_listener=False)
    try:
        yield client
    finally:
        await client.close()


async def test_run_agent_stdin(client: ExchangeClient[Any]):
    agent_spec = await client.register_agent(SleepAgent)
    with mock.patch('academy.remote._run_agent_on_worker'):
        with mock.patch.object(
            sys.stdin.buffer,
            'read',
            return_value=pickle.dumps(agent_spec),
        ):
            assert _main([]) == 0


async def test_run_agent_file(client: ExchangeClient[Any], tmp_path):
    agent_spec = await client.register_agent(SleepAgent)
    file = tmp_path / 'agent_spec.pkl'
    with file.open('wb') as fp:
        pickle.dump(agent_spec, fp)

    with mock.patch('academy.remote._run_agent_on_worker'):
        assert _main(['--spec', str(tmp_path / 'agent_spec.pkl')]) == 0
