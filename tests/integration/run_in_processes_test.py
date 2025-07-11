from __future__ import annotations

import multiprocessing
from concurrent.futures import ProcessPoolExecutor

import pytest

from academy.agent import action
from academy.agent import Agent
from academy.exchange.cloud.client import spawn_http_exchange
from academy.handle import Handle
from academy.manager import Manager
from academy.socket import open_port


class Coordinator(Agent):
    def __init__(
        self,
        lowerer: Handle[Lowerer],
        reverser: Handle[Reverser],
    ) -> None:
        super().__init__()
        self.lowerer = lowerer
        self.reverser = reverser

    @action
    async def process(self, text: str) -> str:
        future = await self.lowerer.lower(text)
        text = await future
        future = await self.reverser.reverse(text)
        text = await future
        return text


class Lowerer(Agent):
    @action
    async def lower(self, text: str) -> str:
        return text.lower()


class Reverser(Agent):
    @action
    async def reverse(self, text: str) -> str:
        return text[::-1]


@pytest.mark.asyncio
async def test_run_in_processes() -> None:
    with spawn_http_exchange('localhost', open_port()) as factory:
        mp_context = multiprocessing.get_context('spawn')
        executor = ProcessPoolExecutor(max_workers=3, mp_context=mp_context)

        async with await Manager.from_exchange_factory(
            factory=factory,
            executors=executor,
        ) as manager:
            lowerer = await manager.launch(Lowerer)
            reverser = await manager.launch(Reverser)
            coordinator = await manager.launch(
                Coordinator,
                args=(lowerer, reverser),
            )

            text = 'DEADBEEF'
            expected = 'feebdaed'

            future = await coordinator.process(text)
            result = await future
            assert result == expected
