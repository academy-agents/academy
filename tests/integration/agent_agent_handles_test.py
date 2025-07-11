from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pytest

from academy.agent import action
from academy.agent import Agent
from academy.exchange.local import LocalExchangeFactory
from academy.handle import Handle
from academy.manager import Manager


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
async def test_agent_agent_handles() -> None:
    async with await Manager.from_exchange_factory(
        factory=LocalExchangeFactory(),
        executors=ThreadPoolExecutor(),
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
