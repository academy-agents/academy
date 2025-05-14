"""HTTP message exchange client and server.

To start the exchange:
```bash
python -m academy.exchange.cloud --config exchange.yaml
```

Connect to the exchange through the client.
```python
from academy.exchange.cloud.client import HttpExchange

with HttpExchange('localhost', 1234) as exchange:
    aid = exchange.register_agent()
    mailbox = exchange.get_mailbox(aid)
    ...
    mailbox.close()
```
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import ssl
import sys
import uuid
from collections.abc import AsyncGenerator
from collections.abc import Awaitable
from collections.abc import Sequence
from typing import Any
from typing import Callable
from typing import TypeVar

from aiohttp.web import AppKey
from aiohttp.web import Application
from aiohttp.web import AppRunner
from aiohttp.web import json_response
from aiohttp.web import middleware
from aiohttp.web import Request
from aiohttp.web import Response
from aiohttp.web import run_app
from aiohttp.web import TCPSite
from pydantic import TypeAdapter
from pydantic import ValidationError

from academy.behavior import Behavior
from academy.exception import BadEntityIdError
from academy.exception import MailboxClosedError
from academy.exchange.cloud.authenticate import Authenticator
from academy.exchange.cloud.authenticate import get_authenticator
from academy.exchange.cloud.config import ExchangeAuthConfig
from academy.exchange.cloud.config import ExchangeServingConfig
from academy.exchange.cloud.exceptions import ForbiddenError
from academy.exchange.cloud.exceptions import UnauthorizedError
from academy.exchange.queue import AsyncQueue
from academy.exchange.queue import QueueClosedError
from academy.identifier import AgentId
from academy.identifier import EntityId
from academy.logging import init_logging
from academy.message import BaseMessage
from academy.message import Message

logger = logging.getLogger(__name__)

BehaviorT = TypeVar('BehaviorT', bound=Behavior)
_OKAY_CODE = 200
_BAD_REQUEST_CODE = 400
_FORBIDDEN_CODE = 403
_NOT_FOUND_CODE = 404
_UNAUTHORIZED_CODE = 405


class _MailboxManager:
    def __init__(self) -> None:
        self._mailboxes: dict[EntityId, AsyncQueue[Message]] = {}
        self._behaviors: dict[AgentId[Any], tuple[str, ...]] = {}

    def check_mailbox(self, uid: EntityId) -> bool:
        return uid in self._mailboxes

    def create_mailbox(
        self,
        uid: EntityId,
        behavior: tuple[str, ...] | None = None,
    ) -> None:
        if uid not in self._mailboxes or self._mailboxes[uid].closed():
            self._mailboxes[uid] = AsyncQueue()
            if behavior is not None and isinstance(uid, AgentId):
                self._behaviors[uid] = behavior
            logger.info('Created mailbox for %s', uid)

    async def terminate(self, uid: EntityId) -> None:
        mailbox = self._mailboxes.get(uid, None)
        if mailbox is not None:
            await mailbox.close()
            logger.info('Closed mailbox for %s', uid)

    async def discover(
        self,
        behavior: str,
        allow_subclasses: bool,
    ) -> list[AgentId[Any]]:
        found: list[AgentId[Any]] = []
        for aid, behaviors in self._behaviors.items():
            if self._mailboxes[aid].closed():
                continue
            if behavior == behaviors[0] or (
                allow_subclasses and behavior in behaviors
            ):
                found.append(aid)
        return found

    async def get(self, uid: EntityId) -> Message:
        try:
            return await self._mailboxes[uid].get()
        except KeyError as e:
            raise BadEntityIdError(uid) from e
        except QueueClosedError as e:
            raise MailboxClosedError(uid) from e

    async def put(self, message: Message) -> None:
        try:
            await self._mailboxes[message.dest].put(message)
        except KeyError as e:
            raise BadEntityIdError(message.dest) from e
        except QueueClosedError as e:
            raise MailboxClosedError(message.dest) from e


MANAGER_KEY = AppKey('manager', _MailboxManager)


async def _create_mailbox_route(request: Request) -> Response:
    data = await request.json()
    manager: _MailboxManager = request.app[MANAGER_KEY]

    try:
        raw_mailbox_id = data['mailbox']
        mailbox_id: EntityId = TypeAdapter(EntityId).validate_json(
            raw_mailbox_id,
        )
        behavior_raw = data.get('behavior', None)
        behavior = (
            behavior_raw.split(',') if behavior_raw is not None else None
        )
    except (KeyError, ValidationError):
        return Response(
            status=_BAD_REQUEST_CODE,
            text='Missing or invalid mailbox ID',
        )

    manager.create_mailbox(mailbox_id, behavior)
    return Response(status=_OKAY_CODE)


async def _terminate_route(request: Request) -> Response:
    data = await request.json()
    manager: _MailboxManager = request.app[MANAGER_KEY]

    try:
        raw_mailbox_id = data['mailbox']
        mailbox_id: EntityId = TypeAdapter(EntityId).validate_json(
            raw_mailbox_id,
        )
    except (KeyError, ValidationError):
        return Response(
            status=_BAD_REQUEST_CODE,
            text='Missing or invalid mailbox ID',
        )

    await manager.terminate(mailbox_id)
    return Response(status=_OKAY_CODE)


async def _discover_route(request: Request) -> Response:
    data = await request.json()
    manager: _MailboxManager = request.app[MANAGER_KEY]

    try:
        behavior = data['behavior']
        allow_subclasses = data['allow_subclasses']
    except (KeyError, ValidationError):
        return Response(
            status=_BAD_REQUEST_CODE,
            text='Missing or invalid arguments',
        )

    agent_ids = await manager.discover(behavior, allow_subclasses)
    return json_response(
        {'agent_ids': ','.join(str(aid.uid) for aid in agent_ids)},
    )


async def _check_mailbox_route(request: Request) -> Response:
    data = await request.json()
    manager: _MailboxManager = request.app[MANAGER_KEY]

    try:
        raw_mailbox_id = data['mailbox']
        mailbox_id: EntityId = TypeAdapter(EntityId).validate_json(
            raw_mailbox_id,
        )
    except (KeyError, ValidationError):
        return Response(
            status=_BAD_REQUEST_CODE,
            text='Missing or invalid mailbox ID',
        )

    exists = manager.check_mailbox(mailbox_id)
    return json_response({'exists': exists})


async def _send_message_route(request: Request) -> Response:
    data = await request.json()
    manager: _MailboxManager = request.app[MANAGER_KEY]

    try:
        raw_message = data.get('message')
        message = BaseMessage.model_from_json(raw_message)
    except (KeyError, ValidationError):
        return Response(
            status=_BAD_REQUEST_CODE,
            text='Missing or invalid message',
        )

    try:
        await manager.put(message)
    except BadEntityIdError:
        return Response(status=_NOT_FOUND_CODE, text='Unknown mailbox ID')
    except MailboxClosedError:
        return Response(status=_FORBIDDEN_CODE, text='Mailbox was closed')
    else:
        return Response(status=_OKAY_CODE)


async def _recv_message_route(request: Request) -> Response:
    data = await request.json()
    manager: _MailboxManager = request.app[MANAGER_KEY]

    try:
        raw_mailbox_id = data['mailbox']
        mailbox_id: EntityId = TypeAdapter(EntityId).validate_json(
            raw_mailbox_id,
        )
    except (KeyError, ValidationError):
        return Response(
            status=_BAD_REQUEST_CODE,
            text='Missing or invalid mailbox ID',
        )

    try:
        message = await manager.get(mailbox_id)
    except BadEntityIdError:
        return Response(status=_NOT_FOUND_CODE, text='Unknown mailbox ID')
    except MailboxClosedError:
        return Response(status=_FORBIDDEN_CODE, text='Mailbox was closed')
    else:
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
        loop = asyncio.get_running_loop()
        try:
            # Needs to be run in executor because globus client is blocking
            client_uuid: uuid.UUID = await loop.run_in_executor(
                None,
                authenticator.authenticate_user,
                request.headers,
            )
        except ForbiddenError:
            return Response(
                status=_FORBIDDEN_CODE,
                text='Token expired or revoked.',
            )
        except UnauthorizedError:
            return Response(
                status=_UNAUTHORIZED_CODE,
                text='Missing required headers.',
            )

        request['client_id'] = str(client_uuid)
        return await handler(request)

    return authenticate


def create_app(
    auth_config: ExchangeAuthConfig | None = None,
) -> Application:
    """Create a new server application."""
    middlewares = []
    if auth_config is not None:
        authenticator = get_authenticator(auth_config)
        middlewares.append(authenticate_factory(authenticator))

    manager = _MailboxManager()
    app = Application(middlewares=middlewares)
    app[MANAGER_KEY] = manager

    app.router.add_post('/mailbox', _create_mailbox_route)
    app.router.add_delete('/mailbox', _terminate_route)
    app.router.add_get('/mailbox', _check_mailbox_route)
    app.router.add_put('/message', _send_message_route)
    app.router.add_get('/message', _recv_message_route)
    app.router.add_get('/discover', _discover_route)

    return app


@contextlib.asynccontextmanager
async def serve_app(
    app: Application,
    host: str,
    port: int,
) -> AsyncGenerator[None]:
    """Serve an application as a context manager.

    Args:
        app: Application to run.
        host: Host to bind to.
        port: Port to bind to.
    """
    runner = AppRunner(app)
    try:
        await runner.setup()
        site = TCPSite(runner, host, port)
        await site.start()
        logger.info('Exchange listening on %s:%s', host, port)
        yield
    finally:
        await runner.cleanup()
        logger.info('Exchange closed!')


def _run(
    config: ExchangeServingConfig,
) -> None:
    app = create_app(config.auth)
    init_logging(config.log_level, logfile=config.log_file)
    logger = logging.getLogger('root')
    logger.info(
        'Exchange listening on %s:%s (ctrl-C to exit)',
        config.host,
        config.port,
    )

    ssl_context: ssl.SSLContext | None = None
    if config.certfile is not None:
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ssl_context.load_cert_chain(config.certfile, keyfile=config.keyfile)

    run_app(
        app,
        host=config.host,
        port=config.port,
        print=None,
        ssl_context=ssl_context,
    )
    logger.info('Exchange closed!')


def _main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True)

    argv = sys.argv[1:] if argv is None else argv
    args = parser.parse_args(argv)

    server_config = ExchangeServingConfig.from_toml(args.config)
    _run(server_config)

    return 0
