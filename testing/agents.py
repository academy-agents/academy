from __future__ import annotations

import asyncio
from typing import TypeVar

from academy.agent import action
from academy.agent import Agent
from academy.agent import loop

T = TypeVar('T')


class EmptyAgent(Agent):
    pass


class ErrorAgent(Agent):
    @action
    async def fails(self) -> None:
        raise RuntimeError('This action always fails.')


class IdentityAgent(Agent):
    @action
    async def identity(self, value: T) -> T:
        return value


class WaitAgent(Agent):
    @loop
    async def wait(self, shutdown: asyncio.Event) -> None:
        await shutdown.wait()


class CounterAgent(Agent):
    def __init__(self) -> None:
        super().__init__()
        self._count = 0

    async def agent_on_startup(self) -> None:
        self._count = 0

    @action
    async def add(self, value: int) -> None:
        self._count += value

    @action
    async def count(self) -> int:
        return self._count


class SleepAgent(Agent):
    def __init__(self, loop_sleep: float = 0.001) -> None:
        super().__init__()
        self.loop_sleep = loop_sleep
        self.steps = 0

    @loop
    async def count(self, shutdown: asyncio.Event) -> None:
        while not shutdown.is_set():
            self.steps += 1
            await asyncio.sleep(self.loop_sleep)

    @action
    async def sleep(self, action_sleep: float) -> None:
        await asyncio.sleep(action_sleep)
