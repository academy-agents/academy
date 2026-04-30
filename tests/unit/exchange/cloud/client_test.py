from __future__ import annotations

import logging
import pickle
import uuid
from typing import Any
from unittest import mock

import aiohttp
import pytest

from academy.exception import BadEntityIdError
from academy.exception import ForbiddenError
from academy.exception import MailboxTerminatedError
from academy.exception import UnauthorizedError
from academy.exchange import HttpExchangeFactory
from academy.exchange import HttpExchangeTransport
from academy.exchange.cloud.app import StatusCode
from academy.exchange.cloud.authenticate import NullAuthenticator
from academy.exchange.cloud.client import _raise_for_status
from academy.exchange.cloud.client import spawn_http_exchange
from academy.exchange.cloud.client_info import ClientInfo
from academy.identifier import AgentId
from academy.identifier import UserId
from academy.message import Message
from academy.message import PingRequest
from academy.socket import open_port
from testing.constant import TEST_CONNECTION_TIMEOUT
from testing.constant import TEST_WAIT_TIMEOUT


def test_factory_serialize(
    http_exchange_factory: HttpExchangeFactory,
) -> None:
    pickled = pickle.dumps(http_exchange_factory)
    reconstructed = pickle.loads(pickled)
    assert isinstance(reconstructed, HttpExchangeFactory)


@pytest.mark.asyncio
async def test_recv_timeout(http_exchange_server: tuple[str, int]) -> None:
    host, port = http_exchange_server
    url = f'http://{host}:{port}'
    factory = HttpExchangeFactory(url, request_timeout_s=TEST_WAIT_TIMEOUT)
    async with await factory._create_transport() as transport:
        with pytest.raises(TimeoutError):  # pragma: <3.14 cover
            await anext(transport.listen(2 * TEST_WAIT_TIMEOUT))


@pytest.mark.asyncio
async def test_additional_headers(
    http_exchange_server: tuple[str, int],
) -> None:
    host, port = http_exchange_server
    url = f'http://{host}:{port}'
    headers = {'Authorization': 'fake auth'}
    factory = HttpExchangeFactory(url, additional_headers=headers)
    async with await factory._create_transport() as transport:
        assert isinstance(transport, HttpExchangeTransport)
        assert 'Authorization' in transport._session.headers


@pytest.mark.asyncio
async def test_default_client_timeout_disables_total_cap(
    http_exchange_server: tuple[str, int],
) -> None:
    # aiohttp's default ClientTimeout(total=300) breaks SSE long-poll
    # listens after 5 minutes; the factory default must override it.
    host, port = http_exchange_server
    url = f'http://{host}:{port}'
    factory = HttpExchangeFactory(url)
    assert factory._info.client_timeout is not None
    assert factory._info.client_timeout.total is None
    async with await factory._create_transport() as transport:
        assert transport._session.timeout.total is None


@pytest.mark.asyncio
async def test_custom_client_timeout_is_honored(
    http_exchange_server: tuple[str, int],
) -> None:
    host, port = http_exchange_server
    url = f'http://{host}:{port}'
    custom = aiohttp.ClientTimeout(total=123, sock_connect=5)
    factory = HttpExchangeFactory(url, client_timeout=custom)
    assert factory._info.client_timeout is custom
    async with await factory._create_transport() as transport:
        assert transport._session.timeout == custom


def test_default_exchange():
    with mock.patch(
        'academy.exchange.cloud.client.get_auth_headers',
    ) as get_auth_headers:
        HttpExchangeFactory()
        get_auth_headers.assert_called_once_with('globus')


def test_default_exchange_from_transport():
    uid = UserId.new()
    with mock.patch(
        'academy.exchange.cloud.client.get_auth_headers',
    ) as get_auth_headers:
        get_auth_headers.return_value = {'Authorization': '<token>'}
        factory = HttpExchangeFactory()
        transport = HttpExchangeTransport(
            uid,
            mock.Mock(),
            factory._info,
        )
        get_auth_headers.assert_called_once_with('globus')

    with mock.patch(
        'academy.exchange.cloud.client.get_auth_headers',
    ) as get_auth_headers:
        # Check recreating the factory does not cause reauthentication
        recreated_factory = transport.factory()
        get_auth_headers.assert_called_once_with(None)
        assert recreated_factory._info == factory._info


def test_raise_for_status_error_conversion() -> None:
    class _MockResponse(aiohttp.ClientResponse):
        def __init__(self, status: int) -> None:
            self.status = status

    response = _MockResponse(StatusCode.OKAY.value)
    _raise_for_status(response, UserId.new())

    response = _MockResponse(StatusCode.UNAUTHORIZED.value)
    with pytest.raises(UnauthorizedError):
        _raise_for_status(response, UserId.new())

    response = _MockResponse(StatusCode.FORBIDDEN.value)
    with pytest.raises(ForbiddenError):
        _raise_for_status(response, UserId.new())

    response = _MockResponse(StatusCode.NOT_FOUND.value)
    with pytest.raises(BadEntityIdError):
        _raise_for_status(response, UserId.new())

    response = _MockResponse(StatusCode.TERMINATED.value)
    with pytest.raises(MailboxTerminatedError):
        _raise_for_status(response, UserId.new())

    response = _MockResponse(StatusCode.TIMEOUT.value)
    with pytest.raises(TimeoutError):
        _raise_for_status(response, UserId.new())


@pytest.mark.asyncio
async def test_create_console(
    http_exchange_factory: HttpExchangeFactory,
) -> None:
    console = await http_exchange_factory.console()
    assert console.factory()._info == http_exchange_factory._info


@pytest.mark.asyncio
async def test_console_share_mailbox(
    http_exchange_factory: HttpExchangeFactory,
) -> None:
    group_id = uuid.uuid1()
    client_info = ClientInfo(client_id='', group_memberships={str(group_id)})

    with mock.patch.object(
        NullAuthenticator,
        'authenticate_user',
        return_value=client_info,
    ):
        async with await http_exchange_factory.create_user_client() as client:
            console = await http_exchange_factory.console()
            await console.share_mailbox(client.client_id, group_id)

            group_ids = await console.get_shared_groups(client.client_id)
            assert len(group_ids) == 1
            assert group_ids[0] == group_id

            await console.remove_shared_group(client.client_id, group_id)
            group_ids = await console.get_shared_groups(client.client_id)
            assert len(group_ids) == 0

            await console.close()


@pytest.mark.asyncio
async def test_console_share_mailbox_forbidden(
    http_exchange_factory: HttpExchangeFactory,
) -> None:
    group_id = uuid.uuid1()

    async with await http_exchange_factory.create_user_client() as client:
        console = await http_exchange_factory.console()
        with pytest.raises(ForbiddenError):
            await console.share_mailbox(client.client_id, group_id)
        await console.close()


@pytest.mark.asyncio
async def test_spawn_http_exchange() -> None:
    with spawn_http_exchange(
        'localhost',
        open_port(),
        level=logging.ERROR,
        timeout=TEST_CONNECTION_TIMEOUT,
    ) as factory:
        async with await factory._create_transport() as transport:
            assert isinstance(transport, HttpExchangeTransport)


async def test_sse_event_parse(
    http_exchange_factory: HttpExchangeFactory,
) -> None:
    uid = UserId.new()
    aid: AgentId[Any] = AgentId.new()
    message = Message.create(
        src=uid,
        dest=aid,
        body=PingRequest(),
    )

    event = [
        'retry: 3000',
        'id: 0',
        f'data: {message.model_dump_json()}',
    ]
    async with await http_exchange_factory._create_transport() as transport:
        parsed = await transport.parse(event)
        assert parsed == message
        assert transport._last_event_id == 0
        assert transport._retry_time_ms == 3000  # noqa: PLR2004


async def test_sse_event_parse_comment(
    http_exchange_factory: HttpExchangeFactory,
) -> None:

    event = [': ping']
    async with await http_exchange_factory._create_transport() as transport:
        parsed = await transport.parse(event)
        assert parsed is None


async def test_sse_event_parse_unexpected_field(
    http_exchange_factory: HttpExchangeFactory,
    caplog,
) -> None:
    uid = UserId.new()
    aid: AgentId[Any] = AgentId.new()
    message = Message.create(
        src=uid,
        dest=aid,
        body=PingRequest(),
    )

    event = [
        'bad: field',
        f'data: {message.model_dump_json()}',
    ]

    async with await http_exchange_factory._create_transport() as transport:
        with caplog.at_level(logging.WARNING):
            parsed = await transport.parse(event)
            assert parsed == message
        assert 'unexpected field in event stream' in caplog.text


async def test_listen_receive_event(
    http_exchange_factory: HttpExchangeFactory,
) -> None:
    uid = UserId.new()
    aid: AgentId[Any] = AgentId.new()
    message = Message.create(
        src=uid,
        dest=aid,
        body=PingRequest(),
    )

    event_stream: list[bytes] = []
    for _ in range(3):
        event_stream.extend(
            [
                f'data: {message.model_dump_json()}'.encode(),
                b'',
                b': ping',
                b'',
            ],
        )

    mock_response = mock.MagicMock()
    mock_response.content.__aiter__.return_value = event_stream

    async with await http_exchange_factory._create_transport() as transport:
        with mock.patch.object(
            transport._session,
            'get',
            new=mock.AsyncMock(),
        ) as mock_get:
            mock_get.return_value = mock_response
            listener = transport.listen(timeout=TEST_WAIT_TIMEOUT)
            for _ in range(3):
                received = await anext(listener)
                assert received == message
