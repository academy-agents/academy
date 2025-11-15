from __future__ import annotations

import logging
import multiprocessing
import pathlib
import uuid
from collections.abc import AsyncGenerator
from typing import Any
from unittest import mock

import pytest
import pytest_asyncio
from aiohttp.test_utils import TestClient
from aiohttp.test_utils import TestServer
from aiohttp.web import Application
from aiohttp.web import Request

from academy.exception import ForbiddenError
from academy.exception import UnauthorizedError
from academy.exchange import HttpExchangeFactory
from academy.exchange.cloud.app import _main
from academy.exchange.cloud.app import _run
from academy.exchange.cloud.app import create_app
from academy.exchange.cloud.app import StatusCode
from academy.exchange.cloud.client_info import ClientInfo
from academy.exchange.cloud.config import ExchangeAuthConfig
from academy.exchange.cloud.config import ExchangeServingConfig
from academy.exchange.cloud.config import PythonBackendConfig
from academy.identifier import AgentId
from academy.identifier import UserId
from academy.message import Message
from academy.message import PingRequest
from academy.socket import open_port
from academy.socket import wait_connection
from testing.constant import TEST_CONNECTION_TIMEOUT
from testing.constant import TEST_SLEEP_INTERVAL
from testing.ssl import SSLContextFixture


def test_server_cli(tmp_path: pathlib.Path) -> None:
    data = """\
host = "localhost"
port = 1234
certfile = "/path/to/cert.pem"
keyfile = "/path/to/privkey.pem"

[auth]
method = "globus"

[auth.kwargs]
client_id = "ABC"
"""

    filepath = tmp_path / 'exchange.toml'
    with open(filepath, 'w') as f:
        f.write(data)

    with mock.patch('academy.exchange.cloud.app._run'):
        assert _main(['--config', str(filepath)]) == 0


@pytest.mark.asyncio
async def test_server_run() -> None:
    config = ExchangeServingConfig(
        host='127.0.0.1',
        port=open_port(),
        log_level=logging.ERROR,
    )

    context = multiprocessing.get_context('spawn')
    process = context.Process(target=_run, args=(config,))
    process.start()

    wait_connection(config.host, config.port, timeout=TEST_CONNECTION_TIMEOUT)
    factory = HttpExchangeFactory(f'http://{config.host}:{config.port}')
    client = await factory.create_user_client()
    await client.close()

    process.terminate()
    process.join()
    assert process.exitcode == 0


@pytest.mark.filterwarnings('ignore:Unverified HTTPS request is being made')
@pytest.mark.asyncio
async def test_server_run_ssl(ssl_context: SSLContextFixture) -> None:
    config = ExchangeServingConfig(
        host='127.0.0.1',
        port=open_port(),
        log_level=logging.ERROR,
    )
    config.certfile = ssl_context.certfile
    config.keyfile = ssl_context.keyfile

    context = multiprocessing.get_context('spawn')
    process = context.Process(target=_run, args=(config,))
    process.start()

    wait_connection(config.host, config.port, timeout=TEST_CONNECTION_TIMEOUT)
    factory = HttpExchangeFactory(
        f'https://{config.host}:{config.port}',
        ssl_verify=False,
    )
    client = await factory.create_user_client()
    await client.close()

    process.terminate()
    process.join()
    assert process.exitcode == 0


@pytest_asyncio.fixture
async def cli() -> AsyncGenerator[TestClient[Request, Application]]:
    app = create_app()
    async with TestClient(TestServer(app)) as client:
        yield client


@pytest.mark.asyncio
async def test_create_mailbox_validation_error(cli) -> None:
    response = await cli.post('/mailbox', json={'mailbox': 'foo'})
    assert response.status == StatusCode.BAD_REQUEST.value
    assert await response.text() == 'Missing or invalid mailbox ID'


@pytest.mark.asyncio
async def test_view_mailbox_shares_error(cli) -> None:
    uid = UserId.new()
    response = await cli.get(
        '/mailbox/share',
        json={
            'mailbox': uid.model_dump_json(),
        },
    )
    assert response.status == StatusCode.NOT_FOUND.value
    assert await response.text() == 'Unknown mailbox ID'


@pytest.mark.asyncio
async def test_view_mailbox_shares_bad_perms(cli) -> None:
    uid = UserId.new()
    response = await cli.get(
        '/mailbox/share',
        json={
            'mailbox': uid.model_dump_json(),
        },
    )
    assert response.status == StatusCode.NOT_FOUND.value
    assert await response.text() == 'Unknown mailbox ID'


@pytest.mark.asyncio
async def test_share_bad_mailbox(cli) -> None:
    response = await cli.post(
        '/mailbox/share',
        json={
            'group_id': 'globus_group',
            'mailbox': 'foo',
        },
    )
    assert response.status == StatusCode.BAD_REQUEST.value
    assert await response.text() == 'Missing or invalid mailbox ID'


@pytest.mark.asyncio
async def test_get_shares_bad_mailbox(cli) -> None:
    response = await cli.get(
        '/mailbox/share',
        json={'mailbox': 'foo'},
    )
    assert response.status == StatusCode.BAD_REQUEST.value
    assert await response.text() == 'Missing or invalid mailbox ID'


@pytest.mark.asyncio
async def test_terminate_validation_error(cli) -> None:
    response = await cli.delete('/mailbox', json={'mailbox': 'foo'})
    assert response.status == StatusCode.BAD_REQUEST.value
    assert await response.text() == 'Missing or invalid mailbox ID'


@pytest.mark.asyncio
async def test_discover_validation_error(cli) -> None:
    response = await cli.get('/discover', json={})
    assert response.status == StatusCode.BAD_REQUEST.value
    assert await response.text() == 'Missing or invalid arguments'


@pytest.mark.asyncio
async def test_check_mailbox_validation_error(cli) -> None:
    response = await cli.get('/mailbox', json={'mailbox': 'foo'})
    assert response.status == StatusCode.BAD_REQUEST.value
    assert await response.text() == 'Missing or invalid mailbox ID'


@pytest.mark.asyncio
async def test_send_mailbox_validation_error(cli) -> None:
    response = await cli.put('/message', json={'message': 'foo'})
    assert response.status == StatusCode.BAD_REQUEST.value
    assert await response.text() == 'Missing or invalid message'


@pytest.mark.asyncio
async def test_recv_mailbox_validation_error(cli) -> None:
    response = await cli.get('/message', json={'mailbox': 'foo'})
    assert response.status == StatusCode.BAD_REQUEST.value
    assert await response.text() == 'Missing or invalid mailbox ID'

    response = await cli.get(
        '/message',
        json={'mailbox': UserId.new().model_dump_json()},
    )
    assert response.status == StatusCode.NOT_FOUND.value
    assert await response.text() == 'Unknown mailbox ID'


@pytest.mark.asyncio
async def test_recv_timeout_error(cli) -> None:
    uid = UserId.new()
    response = await cli.post(
        '/mailbox',
        json={'mailbox': uid.model_dump_json()},
    )
    assert response.status == StatusCode.OKAY.value

    response = await cli.get(
        '/message',
        json={
            'mailbox': uid.model_dump_json(),
            'timeout': TEST_SLEEP_INTERVAL,
        },
    )
    assert response.status == StatusCode.TIMEOUT.value


@pytest.mark.asyncio
async def test_send_mailbox_message_too_large(cli) -> None:
    aid: AgentId[Any] = AgentId.new()
    cid = UserId.new()
    message = Message.create(src=cid, dest=aid, body=PingRequest())

    with mock.patch('sys.getsizeof', return_value=5 * 1024 * 1024):
        # Create agent
        response = await cli.post(
            '/mailbox',
            json={'mailbox': aid.model_dump_json(), 'agent': 'foo'},
            headers={'Authorization': 'Bearer user_1'},
        )
        assert response.status == StatusCode.OKAY.value

        # Create client
        response = await cli.post(
            '/mailbox',
            json={'mailbox': cid.model_dump_json()},
            headers={'Authorization': 'Bearer user_1'},
        )
        assert response.status == StatusCode.OKAY.value

        # Send valid message
        response = await cli.put(
            '/message',
            json={'message': message.model_dump_json()},
            headers={'Authorization': 'Bearer user_1'},
        )
        assert response.status == StatusCode.TOO_LARGE.value


@pytest.mark.asyncio
async def test_null_auth_client() -> None:
    auth = ExchangeAuthConfig()
    backend = PythonBackendConfig()
    app = create_app(backend, auth)
    async with TestClient(TestServer(app)) as client:
        response = await client.get('/message', json={'mailbox': 'foo'})
        assert response.status == StatusCode.BAD_REQUEST.value
        assert await response.text() == 'Missing or invalid mailbox ID'

        response = await client.get(
            '/message',
            json={'mailbox': UserId.new().model_dump_json()},
        )
        assert response.status == StatusCode.NOT_FOUND.value
        assert await response.text() == 'Unknown mailbox ID'


@pytest_asyncio.fixture
async def group_id() -> str:
    return str(uuid.uuid4())


@pytest_asyncio.fixture
async def auth_client(
    group_id: str,
) -> AsyncGenerator[TestClient[Request, Application]]:
    auth = ExchangeAuthConfig(
        method='globus',
        kwargs={'client_id': str(uuid.uuid4()), 'client_secret': 'test'},
    )

    user_1 = ClientInfo(
        client_id='1624cf3f-45ee-4f54-9de4-2d5d79191346',
        group_memberships={group_id},
    )
    user_2 = ClientInfo(
        client_id='316d41e5-56b1-4bce-b704-fd0bc13ba7bb',
        group_memberships={group_id},
    )
    user_3 = ClientInfo(
        client_id='c9c928d2-2589-44f5-89f1-22beed5f3c50',
        group_memberships=set(),
    )

    def authorize(token):
        if 'Authorization' not in token:
            raise UnauthorizedError()
        if 'user_1' in token['Authorization']:
            return user_1
        if 'user_2' in token['Authorization']:
            return user_2
        if 'user_3' in token['Authorization']:
            return user_3
        else:
            raise ForbiddenError()

    backend = PythonBackendConfig()

    with mock.patch(
        'academy.exchange.cloud.authenticate.GlobusAuthenticator.authenticate_user',
    ) as mock_user_auth:
        mock_user_auth.side_effect = authorize
        app = create_app(backend, auth)
        async with TestClient(TestServer(app)) as client:
            yield client


@pytest.mark.asyncio
async def test_globus_auth_client_create_discover_close(auth_client) -> None:
    aid = AgentId.new(name='test').model_dump_json()

    # Create agent
    response = await auth_client.post(
        '/mailbox',
        json={'mailbox': aid, 'agent': 'foo'},
        headers={'Authorization': 'Bearer user_1'},
    )
    assert response.status == StatusCode.OKAY.value

    response = await auth_client.post(
        '/mailbox',
        json={'mailbox': aid, 'agent': 'foo'},
        headers={'Authorization': 'Bearer user_2'},
    )
    assert response.status == StatusCode.FORBIDDEN.value

    # Discover
    response = await auth_client.get(
        '/discover',
        json={'agent': 'foo', 'allow_subclasses': False},
        headers={'Authorization': 'Bearer user_1'},
    )
    response_json = await response.json()
    agent_ids = [
        aid for aid in response_json['agent_ids'].split(',') if len(aid) > 0
    ]
    assert len(agent_ids) == 1
    assert response.status == StatusCode.OKAY.value

    response = await auth_client.get(
        '/discover',
        json={'agent': 'foo', 'allow_subclasses': False},
        headers={'Authorization': 'Bearer user_2'},
    )
    response_json = await response.json()
    agent_ids = [
        aid for aid in response_json['agent_ids'].split(',') if len(aid) > 0
    ]
    assert len(agent_ids) == 0
    assert response.status == StatusCode.OKAY.value

    # Check mailbox
    response = await auth_client.get(
        '/mailbox',
        json={'mailbox': aid},
        headers={'Authorization': 'Bearer user_1'},
    )
    assert response.status == StatusCode.OKAY.value

    response = await auth_client.get(
        '/mailbox',
        json={'mailbox': aid},
        headers={'Authorization': 'Bearer user_2'},
    )
    assert response.status == StatusCode.FORBIDDEN.value

    # Delete mailbox
    response = await auth_client.delete(
        '/mailbox',
        json={'mailbox': aid},
        headers={'Authorization': 'Bearer user_2'},
    )
    assert response.status == StatusCode.FORBIDDEN.value

    response = await auth_client.delete(
        '/mailbox',
        json={'mailbox': aid},
        headers={'Authorization': 'Bearer user_1'},
    )
    assert response.status == StatusCode.OKAY.value


@pytest.mark.asyncio
async def test_globus_auth_client_message(auth_client) -> None:
    aid: AgentId[Any] = AgentId.new(name='test')
    cid = UserId.new()
    message = Message.create(src=cid, dest=aid, body=PingRequest())

    # Create agent
    response = await auth_client.post(
        '/mailbox',
        json={'mailbox': aid.model_dump_json(), 'agent': 'foo'},
        headers={'Authorization': 'Bearer user_1'},
    )
    assert response.status == StatusCode.OKAY.value

    # Create client
    response = await auth_client.post(
        '/mailbox',
        json={'mailbox': cid.model_dump_json()},
        headers={'Authorization': 'Bearer user_1'},
    )
    assert response.status == StatusCode.OKAY.value

    # Send valid message
    response = await auth_client.put(
        '/message',
        json={'message': message.model_dump_json()},
        headers={'Authorization': 'Bearer user_1'},
    )
    assert response.status == StatusCode.OKAY.value

    # Send unauthorized message
    response = await auth_client.put(
        '/message',
        json={'message': message.model_dump_json()},
        headers={'Authorization': 'Bearer user_2'},
    )
    assert response.status == StatusCode.FORBIDDEN.value

    response = await auth_client.get(
        '/message',
        json={'mailbox': aid.model_dump_json()},
        headers={'Authorization': 'Bearer user_1'},
    )
    assert response.status == StatusCode.OKAY.value

    response = await auth_client.get(
        '/message',
        json={'mailbox': aid.model_dump_json()},
        headers={'Authorization': 'Bearer user_2'},
    )
    assert response.status == StatusCode.FORBIDDEN.value


@pytest.mark.asyncio
async def test_globus_auth_client_missing_auth(auth_client) -> None:
    response = await auth_client.get(
        '/discover',
        json={'agent': 'foo', 'allow_subclasses': False},
    )
    assert response.status == StatusCode.UNAUTHORIZED.value


@pytest.mark.asyncio
async def test_globus_auth_client_forbidden(auth_client) -> None:
    response = await auth_client.get(
        '/discover',
        json={'agent': 'foo', 'allow_subclasses': False},
        headers={'Authorization': 'Bearer bogus_user'},
    )
    assert response.status == StatusCode.FORBIDDEN.value


@pytest.mark.asyncio
async def test_share_mailbox(auth_client, group_id) -> None:
    uid = UserId.new()
    response = await auth_client.post(
        '/mailbox',
        json={'mailbox': uid.model_dump_json()},
        headers={'Authorization': 'Bearer user_1'},
    )
    assert response.status == StatusCode.OKAY.value

    response = await auth_client.post(
        '/mailbox/share',
        json={'mailbox': uid.model_dump_json(), 'group_id': group_id},
        headers={'Authorization': 'Bearer user_1'},
    )
    assert response.status == StatusCode.OKAY.value

    response = await auth_client.get(
        '/mailbox/share',
        json={'mailbox': uid.model_dump_json()},
        headers={'Authorization': 'Bearer user_1'},
    )
    assert response.status == StatusCode.OKAY.value
    response_json = await response.json()

    assert 'group_ids' in response_json
    assert len(response_json['group_ids']) == 1
    assert group_id in response_json['group_ids']

    # Test access by user_2 who is in the same group
    response = await auth_client.get(
        '/mailbox',  # Check status
        json={'mailbox': uid.model_dump_json()},
        headers={'Authorization': 'Bearer user_2'},
    )
    assert response.status == StatusCode.OKAY.value

    # Non-owner cannot view group access
    response = await auth_client.get(
        '/mailbox/share',
        json={'mailbox': uid.model_dump_json()},
        headers={'Authorization': 'Bearer user_2'},
    )
    assert response.status == StatusCode.FORBIDDEN.value

    # Test access by user_3 who is *NOT* in the same group
    response = await auth_client.get(
        '/mailbox',  # Check status
        json={'mailbox': uid.model_dump_json()},
        headers={'Authorization': 'Bearer user_3'},
    )
    assert response.status == StatusCode.FORBIDDEN.value


async def test_share_mailbox_forbidden(auth_client, group_id) -> None:
    uid = UserId.new()
    response = await auth_client.post(
        '/mailbox',
        json={'mailbox': uid.model_dump_json()},
        headers={'Authorization': 'Bearer user_3'},
    )
    assert response.status == StatusCode.OKAY.value

    response = await auth_client.post(
        '/mailbox/share',
        json={'mailbox': uid.model_dump_json(), 'group_id': group_id},
        headers={'Authorization': 'Bearer user_3'},
    )
    assert response.status == StatusCode.FORBIDDEN.value
