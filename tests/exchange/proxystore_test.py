from __future__ import annotations

import pickle
from collections.abc import Generator
from typing import Any
from typing import Callable

import pytest
from proxystore.connectors.local import LocalConnector
from proxystore.proxy import Proxy
from proxystore.store import Store
from proxystore.store.executor import ProxyAlways
from proxystore.store.executor import ProxyNever

from academy.exchange import MailboxStatus
from academy.exchange.local import LocalExchangeFactory
from academy.exchange.proxystore import ProxyStoreExchangeFactory
from academy.message import ActionRequest
from academy.message import ActionResponse
from academy.message import PingRequest
from testing.behavior import EmptyBehavior


@pytest.fixture
def store() -> Generator[Store[LocalConnector], None, None]:
    with Store(
        'proxystore-exchange-store-fixture',
        LocalConnector(),
        cache_size=0,
        register=True,
    ) as store:
        yield store


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('should_proxy', 'resolve_async'),
    (
        (ProxyNever(), False),
        (ProxyAlways(), True),
        (ProxyAlways(), False),
        (lambda x: isinstance(x, str), True),
    ),
)
async def test_wrap_basic_transport_functionality(
    should_proxy: Callable[[Any], bool],
    resolve_async: bool,
    store: Store[LocalConnector],
    local_exchange_factory: LocalExchangeFactory,
) -> None:
    wrapped_factory = ProxyStoreExchangeFactory(
        base=local_exchange_factory,
        store=store,
        should_proxy=should_proxy,
        resolve_async=resolve_async,
    )

    async with await wrapped_factory._create_transport() as wrapped_transport1:
        new_factory = wrapped_transport1.factory()
        assert isinstance(new_factory, ProxyStoreExchangeFactory)

        src = wrapped_transport1.mailbox_id
        dest = (
            await wrapped_transport1.register_agent(EmptyBehavior)
        ).agent_id
        assert await wrapped_transport1.status(dest) == MailboxStatus.ACTIVE

        wrapped_transport2 = await wrapped_factory._create_transport(
            mailbox_id=dest,
        )
        assert wrapped_transport2.mailbox_id == dest

        ping = PingRequest(src=src, dest=dest)
        await wrapped_transport1.send(ping)
        assert await wrapped_transport2.recv() == ping

        request = ActionRequest(
            src=src,
            dest=dest,
            action='test',
            pargs=('value', 123),
            kargs={'foo': 'value', 'bar': 123},
        )
        await wrapped_transport1.send(request)

        received = await wrapped_transport2.recv()
        assert isinstance(received, ActionRequest)
        assert request.tag == received.tag

        for old, new in zip(request.pargs, received.pargs):
            assert (type(new) is Proxy) == should_proxy(old)
            # will resolve the proxy if it exists
            assert old == new

        for name in request.kargs:
            old, new = request.kargs[name], received.kargs[name]
            assert (type(new) is Proxy) == should_proxy(old)
            assert old == new

        response = request.response('result')
        await wrapped_transport2.send(response)

        received = await wrapped_transport1.recv()
        assert isinstance(received, ActionResponse)
        assert response.tag == received.tag
        assert (type(received.result) is Proxy) == should_proxy(
            response.result,
        )
        assert response.result == received.result

        assert await wrapped_transport1.discover(EmptyBehavior) == (dest,)

        await wrapped_transport1.terminate(wrapped_transport1.mailbox_id)
        await wrapped_transport2.close()


@pytest.mark.asyncio
async def test_serialize_factory(
    http_exchange_factory,
    store: Store[LocalConnector],
) -> None:
    wrapped_factory = ProxyStoreExchangeFactory(
        base=http_exchange_factory,
        store=store,
        should_proxy=ProxyAlways(),
    )
    dumped = pickle.dumps(wrapped_factory)
    reconstructed = pickle.loads(dumped)
    assert isinstance(reconstructed, ProxyStoreExchangeFactory)
