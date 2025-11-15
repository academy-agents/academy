from __future__ import annotations

import asyncio
import fnmatch
import logging
from builtins import set as builtins_set
from collections import defaultdict
from collections.abc import AsyncGenerator
from collections.abc import Generator
from typing import Any
from unittest import mock

import pytest

logger = logging.getLogger(__name__)


class MockRedis:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.values: dict[bytes, bytes] = {}
        self.lists: dict[bytes, list[bytes]] = defaultdict(list)
        self.events: dict[bytes, asyncio.Event] = defaultdict(asyncio.Event)
        self.timeouts: dict[bytes, asyncio.Future[Any]] = {}
        self.sets: dict[bytes, set[bytes]] = {}

        assert (
            'decode_responses' not in kwargs or not kwargs['decode_responses']
        ), 'decode_responses is in compatible with the way we use Redis'

    def _encode(self, value: bytes | str) -> bytes:
        if isinstance(value, bytes):
            return value
        return value.encode()

    async def aclose(self) -> None:
        pass

    async def blpop(
        self,
        keys: list[bytes | str],
        timeout: float = 0,
    ) -> list[bytes] | None:
        result: list[bytes] = []
        for key in keys:
            key = self._encode(key)  # noqa: PLW2901
            if len(self.lists[key]) > 0:
                item = self.lists[key].pop()
                self.events[key].clear()
            else:
                try:
                    await asyncio.wait_for(
                        self.events[key].wait(),
                        timeout=None if timeout == 0 else timeout,
                    )
                except asyncio.TimeoutError:
                    return None
                else:
                    item = self.lists[key].pop()
                    self.events[key].clear()
            result.extend([key, item])
        return result

    async def delete(self, key: bytes | str) -> None:  # pragma: no cover
        key = self._encode(key)
        if key in self.values:
            del self.values[key]
        elif key in self.lists:
            self.lists[key].clear()

    async def exists(self, key: bytes | str) -> bool:  # pragma: no cover
        key = self._encode(key)
        return key in self.values or key in self.lists

    async def _expire_key(self, key: bytes | str, timeout: int):
        key = self._encode(key)
        await asyncio.sleep(timeout)
        logger.info(f'Key {key.decode()} expired.')
        await self.delete(key)
        self.events[key].clear()
        self.timeouts.pop(key, None)

    async def expire(  # noqa: PLR0913
        self,
        key: bytes | str,
        time: int,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> None:
        key = self._encode(key)
        if nx and key in self.timeouts:
            return

        if xx or gt or lt:
            raise NotImplementedError()

        if key in self.timeouts:
            self.timeouts[key].cancel()
            self.timeouts.pop(key, None)

        self.timeouts[key] = asyncio.ensure_future(
            self._expire_key(key, time),
        )

    async def get(
        self,
        key: bytes | str,
    ) -> bytes | list[bytes] | None:
        key = self._encode(key)
        if key in self.values:
            return self.values[key]
        elif key in self.lists:
            raise NotImplementedError()
        return None

    async def lrange(
        self,
        key: bytes | str,
        start: int,
        end: int,
    ) -> list[bytes]:
        key = self._encode(key)
        items = self.lists.get(key, None)
        if items is None:
            return []
        end = end + 1 if end >= 0 else len(items) + end + 1
        return items[start:end]

    async def ping(self, **kwargs) -> None:
        pass

    async def rpush(self, key: bytes | str, *values: bytes | str) -> None:
        key = self._encode(key)
        for value in values:
            value = self._encode(value)  # noqa: PLW2901
            self.lists[key].append(value)
            self.events[key].set()

    async def scan_iter(self, pattern: bytes | str) -> AsyncGenerator[bytes]:
        pattern = self._encode(pattern)
        for key in self.values:
            if fnmatch.fnmatch(key, pattern):
                yield key

    async def set(self, key: bytes | str, value: bytes | str) -> None:
        key = self._encode(key)
        value = self._encode(value)
        self.values[key] = value

    async def sadd(self, key: bytes | str, *values: bytes | str) -> None:
        key = self._encode(key)
        if self.sets.get(key) is None:
            self.sets[key] = set()

        for value in values:
            self.sets[key].add(self._encode(value))

    async def smembers(self, key: bytes | str) -> builtins_set[bytes]:
        return self.sets.get(self._encode(key), set())


@pytest.fixture
def mock_redis() -> Generator[None]:
    redis = MockRedis()
    with mock.patch('redis.asyncio.Redis', return_value=redis):
        yield
