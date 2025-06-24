from __future__ import annotations

import asyncio
import fnmatch
from collections.abc import AsyncGenerator
from collections.abc import Generator
from typing import Any
from unittest import mock

import pytest


class MockRedis:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.values: dict[str, str] = {}
        self.lists: dict[str, asyncio.Queue[str]] = {}

    async def blpop(
        self,
        keys: list[str],
        timeout: int = 0,
    ) -> list[str] | None:
        result: list[str] = []
        for key in keys:
            if key not in self.lists:
                self.lists[key] = asyncio.Queue()
            try:
                item = await asyncio.wait_for(
                    self.lists[key].get(),
                    timeout=None if timeout == 0 else timeout,
                )
            except asyncio.TimeoutError:
                return None
            result.extend([key, item])
        return result

    async def aclose(self) -> None:
        pass

    async def delete(self, key: str) -> None:  # pragma: no cover
        if key in self.values:
            del self.values[key]
        elif key in self.lists:
            del self.lists[key]

    async def exists(self, key: str) -> bool:  # pragma: no cover
        return key in self.values or key in self.lists

    async def get(
        self,
        key: str,
    ) -> str | list[str] | None:  # pragma: no cover
        if key in self.values:
            return self.values[key]
        elif key in self.lists:
            return await self.lists[key].get()
        return None

    async def ping(self, **kwargs) -> None:
        pass

    async def rpush(self, key: str, *values: str) -> None:
        if key not in self.lists:
            self.lists[key] = asyncio.Queue()
        for value in values:
            await self.lists[key].put(value)

    async def scan_iter(self, pattern: str) -> AsyncGenerator[str]:
        for key in self.values:
            if fnmatch.fnmatch(key, pattern):
                yield key

    async def set(self, key: str, value: str) -> None:
        self.values[key] = value


@pytest.fixture
def mock_redis() -> Generator[None]:
    redis = MockRedis()
    with mock.patch('redis.asyncio.Redis', return_value=redis):
        yield
