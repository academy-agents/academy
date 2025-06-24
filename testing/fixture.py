from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import Any
from typing import Callable

import pytest
import pytest_asyncio
from aiohttp.web import AppRunner
from aiohttp.web import TCPSite

from academy.exchange import ExchangeFactory
from academy.exchange import UserExchangeClient
from academy.exchange.cloud.client import HttpExchangeFactory
from academy.exchange.cloud.server import create_app
from academy.exchange.hybrid import HybridExchangeFactory
from academy.exchange.local import LocalExchangeFactory
from academy.exchange.local import LocalExchangeTransport
from academy.exchange.redis import RedisExchangeFactory
from academy.launcher import ThreadLauncher
from academy.socket import open_port


@pytest_asyncio.fixture
async def http_exchange_factory(
    http_exchange_server: tuple[str, int],
) -> HttpExchangeFactory:
    host, port = http_exchange_server
    return HttpExchangeFactory(host, port)


@pytest.fixture
def hybrid_exchange_factory(mock_redis) -> HybridExchangeFactory:
    return HybridExchangeFactory(redis_host='localhost', redis_port=0)


@pytest.fixture
def redis_exchange_factory(mock_redis) -> RedisExchangeFactory:
    return RedisExchangeFactory(hostname='localhost', port=0)


@pytest.fixture
def local_exchange_factory() -> LocalExchangeFactory:
    return LocalExchangeFactory()


EXCHANGE_FACTORY_TYPES = (
    HttpExchangeFactory,
    HybridExchangeFactory,
    RedisExchangeFactory,
    LocalExchangeFactory,
)


@pytest_asyncio.fixture
async def get_factory(
    http_exchange_server: tuple[str, int],
    mock_redis,
) -> Callable[[type[ExchangeFactory[Any]]], ExchangeFactory[Any]]:
    # Typically we would parameterize fixtures on a list of the
    # factory fixtures defined above. However, request.getfixturevalue does
    # not work with async fixtures, of which we have a mix, so we need to set
    # them up manually. Instead, we have a fixture that returns a function
    # that can create the factories from a parameterized list of factory types.
    # See: https://github.com/pytest-dev/pytest-asyncio/issues/976
    def _get_factory_for_testing(
        factory_type: type[ExchangeFactory[Any]],
    ) -> ExchangeFactory[Any]:
        if factory_type is HttpExchangeFactory:
            host, port = http_exchange_server
            return HttpExchangeFactory(host, port)
        elif factory_type is HybridExchangeFactory:
            return HybridExchangeFactory(redis_host='localhost', redis_port=0)
        elif factory_type is RedisExchangeFactory:
            return RedisExchangeFactory(hostname='localhost', port=0)
        elif factory_type is LocalExchangeFactory:
            return LocalExchangeFactory()
        else:
            raise AssertionError('Unsupported factory type.')

    return _get_factory_for_testing


@pytest_asyncio.fixture
async def exchange() -> AsyncGenerator[
    UserExchangeClient[LocalExchangeTransport]
]:
    factory = LocalExchangeFactory()
    async with await factory.create_user_client() as client:
        yield client


@pytest.fixture
async def launcher() -> AsyncGenerator[ThreadLauncher]:
    async with ThreadLauncher() as launcher:
        yield launcher


@pytest_asyncio.fixture
async def http_exchange_server() -> AsyncGenerator[tuple[str, int]]:
    host, port = 'localhost', open_port()
    app = create_app()

    runner = AppRunner(app)
    await runner.setup()

    try:
        site = TCPSite(runner, host, port)
        await site.start()
        yield host, port
    finally:
        await runner.cleanup()
