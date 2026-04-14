"""HTTP message exchange client and server.

To start the exchange:
```bash
python -m academy.exchange.cloud --config exchange.toml
```

Connect to the exchange through the client.
```python
from academy.exchange import HttpExchangeFactory

with HttpExchangeFactory(
    'http://localhost:1234'
).create_user_client() as exchange:
    aid, agent_info = exchange.register_agent()
    ...
```
"""

from __future__ import annotations

import argparse
import enum
import functools
import logging
import ssl
import sys
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Coroutine
from collections.abc import Sequence
from typing import Any

if sys.version_info >= (3, 13):  # pragma: >=3.13 cover
    from asyncio import Queue

    AsyncQueue = Queue
else:  # pragma: <3.13 cover
    # Use of queues here is isolated to a single thread/event loop so
    # we only need culsans queues for the backport of shutdown() agent
    from culsans import Queue

from aiohttp.web import AppKey
from aiohttp.web import Application
from aiohttp.web import json_response
from aiohttp.web import middleware
from aiohttp.web import Request
from aiohttp.web import Response
from aiohttp.web import run_app
from aiohttp_sse import EventSourceResponse
from aiohttp_sse import sse_response
from pydantic import TypeAdapter
from pydantic import ValidationError

from academy.exception import BadEntityIdError
from academy.exception import ForbiddenError
from academy.exception import MailboxTerminatedError
from academy.exception import MessageTooLargeError
from academy.exception import UnauthorizedError
from academy.exchange.cloud.authenticate import Authenticator
from academy.exchange.cloud.authenticate import get_authenticator
from academy.exchange.cloud.backend import MailboxBackend
from academy.exchange.cloud.backend import PythonBackend
from academy.exchange.cloud.client_info import ClientInfo
from academy.exchange.cloud.config import BackendConfig
from academy.exchange.cloud.config import ExchangeAuthConfig
from academy.exchange.cloud.config import ExchangeServingConfig
from academy.exchange.transport import MailboxStatus
from academy.identifier import EntityId
from academy.message import Message

logger = logging.getLogger(__name__)


class StatusCode(enum.Enum):
    """Http status codes."""

    OKAY = 200
    BAD_REQUEST = 400
    UNAUTHORIZED = 401
    FORBIDDEN = 403
    NOT_FOUND = 404
    TIMEOUT = 408
    TOO_LARGE = 413
    TERMINATED = 419
    NO_RESPONSE = 444


MANAGER_KEY = AppKey('manager', MailboxBackend)


def get_client_info(request: Request) -> ClientInfo:
    """Reconstitute client info from Request."""
    client_info = ClientInfo(
        client_id=request.headers.get('client_id', ''),
        group_memberships=set(
            request.headers.get('client_groups', '').split(','),
        ),
    )
    return client_info


def exception_to_response(request_name: str) -> Callable[..., Any]:
    """Convert exceptions to responses.

    Args:
        request_name: Request name for logging.
    """

    def decorator(
        func: Callable[[Any], Coroutine[None, None, Response]],
    ) -> Callable[[Any], Coroutine[None, None, Response]]:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Response:  # noqa: PLR0911
            try:
                return await func(*args, **kwargs)
            except (KeyError, ValidationError) as e:
                logger.warning(
                    f'Missing or invalid field in {request_name} request.',
                )
                return Response(
                    status=StatusCode.BAD_REQUEST.value,
                    text=f'Missing or invalid field: {e}',
                )
            except BadEntityIdError:
                logger.exception(f'Unknown mailbox id in {request_name}')
                return Response(
                    status=StatusCode.NOT_FOUND.value,
                    text='Unknown mailbox ID',
                )
            except ForbiddenError:
                logger.exception(f'Incorrect permissions in {request_name}')
                return Response(
                    status=StatusCode.FORBIDDEN.value,
                    text='Incorrect permissions',
                )
            except MailboxTerminatedError:
                logger.exception(
                    f'Mailbox in {request_name} request was terminated.',
                )
                return Response(
                    status=StatusCode.TERMINATED.value,
                    text='Mailbox was closed',
                )
            except MessageTooLargeError as e:
                logger.exception('Message to mailbox too large.')
                return Response(
                    status=StatusCode.TOO_LARGE.value,
                    text=(
                        f'Message of size {e.size} larger than limit '
                        f'{e.limit}.'
                    ),
                )
            except ConnectionResetError:  # pragma: no cover
                # This happens when the client cancels it's task, and
                # closes its connection. In this case, we don't
                # need to do anything because the client disconnected
                # itself. error, If we don't catch this aiohttp will
                # just log an error message each time this happens.
                return Response(status=StatusCode.NO_RESPONSE.value)

        return wrapper

    return decorator


@exception_to_response('share mailbox')
async def _share_mailbox_route(request: Request) -> Response:
    """Share mailbox with a Globus Group."""
    data = await request.json()
    manager: MailboxBackend = request.app[MANAGER_KEY]
    raw_mailbox_id = data['mailbox']
    mailbox_id: EntityId = TypeAdapter(EntityId).validate_json(
        raw_mailbox_id,
    )
    group_id = data['group_id']
    client = get_client_info(request)
    await manager.share_mailbox(client, mailbox_id, group_id)
    logger.info(
        f'Sharing mailbox {mailbox_id} with group {group_id}',
        extra={
            'academy.mailbox_id': mailbox_id,
            'academy.group_id': group_id,
        },
    )
    return Response(status=StatusCode.OKAY.value)


@exception_to_response('get shares')
async def _get_mailbox_shares_route(request: Request) -> Response:
    """Share mailbox with a Globus Group."""
    data = await request.json()
    manager: MailboxBackend = request.app[MANAGER_KEY]
    raw_mailbox_id = data['mailbox']
    mailbox_id: EntityId = TypeAdapter(EntityId).validate_json(
        raw_mailbox_id,
    )
    client = get_client_info(request)
    shares = await manager.get_mailbox_shares(client, mailbox_id)
    return json_response(
        {'group_ids': shares},
    )


@exception_to_response('remove shares')
async def _remove_mailbox_shares_route(request: Request) -> Response:
    """Stop sharing a mailbox with a Globus Group."""
    data = await request.json()
    manager: MailboxBackend = request.app[MANAGER_KEY]
    raw_mailbox_id = data['mailbox']
    mailbox_id: EntityId = TypeAdapter(EntityId).validate_json(
        raw_mailbox_id,
    )
    group_id = data['group_id']
    client = get_client_info(request)
    await manager.remove_mailbox_shares(client, mailbox_id, group_id)
    logger.info(
        f'Unsharing mailbox {mailbox_id} with group {group_id}',
        extra={
            'academy.mailbox_id': mailbox_id,
            'academy.group_id': group_id,
        },
    )
    return Response(status=StatusCode.OKAY.value)


@exception_to_response('create')
async def _create_mailbox_route(request: Request) -> Response:
    data = await request.json()
    manager: MailboxBackend = request.app[MANAGER_KEY]
    raw_mailbox_id = data['mailbox']
    mailbox_id: EntityId = TypeAdapter(EntityId).validate_json(
        raw_mailbox_id,
    )
    agent_raw = data.get('agent', None)
    agent = agent_raw.split(',') if agent_raw is not None else None

    client = get_client_info(request)
    await manager.create_mailbox(client, mailbox_id, agent)
    logger.info(
        f'Creating mailbox {mailbox_id} of type {agent}',
        extra={
            'academy.mailbox_id': mailbox_id,
            'academy.agent': agent,
            'academy.client_id': client.client_id,
        },
    )
    return Response(status=StatusCode.OKAY.value)


@exception_to_response('terminate')
async def _terminate_route(request: Request) -> Response:
    data = await request.json()
    manager: MailboxBackend = request.app[MANAGER_KEY]
    raw_mailbox_id = data['mailbox']
    mailbox_id: EntityId = TypeAdapter(EntityId).validate_json(
        raw_mailbox_id,
    )
    client = get_client_info(request)
    await manager.terminate(client, mailbox_id)
    logger.info(
        f'Terminating mailbox {mailbox_id}',
        extra={
            'academy.mailbox_id': mailbox_id,
        },
    )
    return Response(status=StatusCode.OKAY.value)


@exception_to_response('discover')
async def _discover_route(request: Request) -> Response:
    data = await request.json()
    manager: MailboxBackend = request.app[MANAGER_KEY]
    agent = data['agent']
    allow_subclasses = data['allow_subclasses']
    client = get_client_info(request)
    agent_ids = await manager.discover(
        client,
        agent,
        allow_subclasses,
    )
    return json_response(
        {'agent_ids': ','.join(str(aid.uid) for aid in agent_ids)},
    )


@exception_to_response('status')
async def _check_mailbox_route(request: Request) -> Response:
    data = await request.json()
    manager: MailboxBackend = request.app[MANAGER_KEY]
    raw_mailbox_id = data['mailbox']
    mailbox_id: EntityId = TypeAdapter(EntityId).validate_json(
        raw_mailbox_id,
    )
    client = get_client_info(request)
    status = await manager.check_mailbox(client, mailbox_id)
    return json_response({'status': status.value})


@exception_to_response('send')
async def _send_message_route(request: Request) -> Response:
    data = await request.json()
    manager: MailboxBackend = request.app[MANAGER_KEY]
    raw_message = data.get('message')
    message: Message[Any] = Message.model_validate_json(raw_message)
    client = get_client_info(request)
    await manager.put(client, message)
    logger.info(
        (
            f'Placing message {message.tag} in mailbox '
            f'{message.dest} from {message.src}'
        ),
        extra={
            'academy.message.event': 'PUT',
            'academy.message.src': message.src,
            'academy.message.dest': message.dest,
            'academy.message.size': sys.getsizeof(message.body),
            'academy.message_tag': message.tag,
        },
    )
    return Response(status=StatusCode.OKAY.value)


@exception_to_response('listen')
async def _listen_mailbox_route(
    request: Request,
) -> EventSourceResponse | Response:
    data = await request.json()
    manager: MailboxBackend = request.app[MANAGER_KEY]
    raw_mailbox_id = data['mailbox']
    mailbox_id: EntityId = TypeAdapter(EntityId).validate_json(
        raw_mailbox_id,
    )
    timeout = data.get('timeout', None)
    client = get_client_info(request)
    status = await manager.check_mailbox(client, mailbox_id)

    if status == MailboxStatus.MISSING:
        logger.exception(f'Listening on unknown mailbox {mailbox_id}.')
        return Response(
            status=StatusCode.NOT_FOUND.value,
            text='Unknown mailbox ID',
        )
    elif status == MailboxStatus.TERMINATED:
        logger.exception(f'Listening on terminated mailbox {mailbox_id}.')
        return Response(
            status=StatusCode.TERMINATED.value,
            text='Mailbox was closed',
        )

    logger.debug(f'Listening for new messages on mailbox {mailbox_id}')
    async with sse_response(request) as response:
        while response.is_connected():  # pragma: no branch
            try:
                message = await manager.get(
                    client,
                    mailbox_id,
                    timeout=timeout,
                )
                logger.info(
                    (
                        f'Fetched message {message.tag} from '
                        f'mailbox {message.dest}'
                    ),
                    extra={
                        'academy.message.event': 'GET',
                        'academy.message.src': message.src,
                        'academy.message.dest': message.dest,
                        'academy.message_tag': message.tag,
                    },
                )
                await response.send(message.model_dump_json())
            except (MailboxTerminatedError, TimeoutError) as e:
                # These messages are not necessarily a sign something is wrong
                # If they were unexpected, the request will be retried and an
                # informative error will be returned to the client. So here we
                # can just log the response and end the event stream.
                logger.info(f'Ending server side event stream for reason {e}.')
                break

    return response


@exception_to_response('recv')
async def _recv_message_route(request: Request) -> Response:
    data = await request.json()
    manager: MailboxBackend = request.app[MANAGER_KEY]
    raw_mailbox_id = data['mailbox']
    mailbox_id: EntityId = TypeAdapter(EntityId).validate_json(
        raw_mailbox_id,
    )
    timeout = data.get('timeout', None)
    client = get_client_info(request)
    try:
        message = await manager.get(client, mailbox_id, timeout=timeout)
    except MailboxTerminatedError:
        # We catch this exception separate from above and log it at a lower
        # level because this happens on an expected (if old) code path and
        # we do not want to flood the logs with error messages.
        logger.info(f'Receive from terminated mailbox {mailbox_id}.')
        return Response(
            status=StatusCode.TERMINATED.value,
            text='Mailbox was closed',
        )
    except TimeoutError:
        # Timeouts are part of expected behavior so log warning, not error
        logger.info(f'Receive from mailbox {mailbox_id} timed-out.')
        return Response(
            status=StatusCode.TIMEOUT.value,
            text='Request timeout',
        )

    logger.info(
        f'Fetched message {message.tag} from mailbox {message.dest}',
        extra={
            'academy.message.event': 'GET',
            'academy.message.src': message.src,
            'academy.message.dest': message.dest,
            'academy.message_tag': message.tag,
        },
    )
    return json_response({'message': message.model_dump_json()})


def authenticate_factory(
    authenticator: Authenticator,
) -> Any:
    """Create an authentication middleware for a given authenticator.

    Args:
        authenticator: Used to validate client id and transform token into id.

    Returns:
        A aiohttp.web.middleware function that will only allow authenticated
            requests.
    """

    @middleware
    async def authenticate(
        request: Request,
        handler: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        try:
            client_info: ClientInfo = await authenticator.authenticate_user(
                request.headers,
            )
        except ForbiddenError:
            logger.exception('Could not authenticate.')
            return Response(
                status=StatusCode.FORBIDDEN.value,
                text='Token expired or revoked.',
            )
        except UnauthorizedError:
            logger.exception('Could not authenticate.')
            return Response(
                status=StatusCode.UNAUTHORIZED.value,
                text='Missing required headers.',
            )

        headers = request.headers.copy()
        headers['client_id'] = client_info.client_id
        headers['client_groups'] = ','.join(client_info.group_memberships)

        # Handle early client-side disconnect in Issue #142
        # This is somewhat hard to reproduce in tests:
        # https://github.com/aio-libs/aiohttp/issues/6978
        if (
            request.transport is None or request.transport.is_closing()
        ):  # pragma: no cover
            return Response(status=StatusCode.NO_RESPONSE.value)

        request = request.clone(headers=headers)
        return await handler(request)

    return authenticate


def create_app(
    backend_config: BackendConfig | None = None,
    auth_config: ExchangeAuthConfig | None = None,
) -> Application:
    """Create a new server application."""
    if backend_config is not None:
        backend = backend_config.get_backend()
    else:
        backend = PythonBackend()

    middlewares = []
    if auth_config is not None:
        authenticator = get_authenticator(auth_config)
        middlewares.append(authenticate_factory(authenticator))

    app = Application(middlewares=middlewares)
    app[MANAGER_KEY] = backend

    app.router.add_post('/mailbox', _create_mailbox_route)
    app.router.add_post('/mailbox/share', _share_mailbox_route)
    app.router.add_get('/mailbox/share', _get_mailbox_shares_route)
    app.router.add_delete('/mailbox/share', _remove_mailbox_shares_route)
    app.router.add_delete('/mailbox', _terminate_route)
    app.router.add_get('/mailbox', _check_mailbox_route)
    app.router.add_put('/message', _send_message_route)
    app.router.add_get('/message', _recv_message_route)
    app.router.add_get('/discover', _discover_route)
    app.router.add_get('/mailbox/listen', _listen_mailbox_route)

    return app


def _run(
    config: ExchangeServingConfig,
) -> None:
    config.logger.init_logger()
    app = create_app(config.backend, config.auth)
    logger = logging.getLogger('root')
    logger.info(
        'Exchange listening on %s:%s (ctrl-C to exit)',
        config.host,
        config.port,
        extra={
            'academy.host': config.host,
            'academy.port': config.port,
        },
    )

    ssl_context: ssl.SSLContext | None = None
    if config.certfile is not None:  # pragma: no cover
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ssl_context.load_cert_chain(config.certfile, keyfile=config.keyfile)

    run_app(
        app,
        host=config.host,
        port=config.port,
        print=None,
        ssl_context=ssl_context,
    )
    logger.info('Exchange closed')


def _main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True)

    argv = sys.argv[1:] if argv is None else argv
    args = parser.parse_args(argv)

    server_config = ExchangeServingConfig.from_toml(args.config)
    _run(server_config)

    return 0
