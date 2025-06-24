from __future__ import annotations

import asyncio
from typing import TypeVar

from academy.behavior import action
from academy.behavior import Behavior
from academy.behavior import loop
from academy.handle import Handle

T = TypeVar('T')


class EmptyBehavior(Behavior):
    pass


class ErrorBehavior(Behavior):
    @action
    async def fails(self) -> None:
        raise RuntimeError('This action always fails.')


class HandleBehavior(Behavior):
    def __init__(self, handle: Handle[EmptyBehavior]) -> None:
        self.handle = handle


class IdentityBehavior(Behavior):
    @action
    async def identity(self, value: T) -> T:
        return value


class WaitBehavior(Behavior):
    @loop
    async def wait(self, shutdown: asyncio.Event) -> None:
        await shutdown.wait()


class CounterBehavior(Behavior):
    def __init__(self) -> None:
        self._count = 0

    async def on_setup(self) -> None:
        self._count = 0

    @action
    async def add(self, value: int) -> None:
        self._count += value

    @action
    async def count(self) -> int:
        return self._count


class SleepBehavior(Behavior):
    def __init__(self, loop_sleep: float = 0.001) -> None:
        self.loop_sleep = loop_sleep
        self.steps = 0

    async def on_shutdown(self) -> None:
        assert self.steps > 0

    @action
    async def sleep(self, sleep: float) -> None:
        await asyncio.sleep(sleep)

    @loop
    async def count(self, shutdown: asyncio.Event) -> None:
        while not shutdown.is_set():
            self.steps += 1
            await asyncio.sleep(self.loop_sleep)
