# ruff: noqa: D102
from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import logging
import multiprocessing
import sys
import uuid
from collections.abc import Generator
from typing import Any
from typing import Generic
from typing import Literal
from typing import NamedTuple

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

import aiohttp

from academy.behavior import Behavior
from academy.behavior import BehaviorT
from academy.exception import BadEntityIdError
from academy.exception import MailboxClosedError
from academy.exchange import ExchangeFactory
from academy.exchange.cloud.config import ExchangeServingConfig
from academy.exchange.cloud.server import _FORBIDDEN_CODE
from academy.exchange.cloud.server import _NOT_FOUND_CODE
from academy.exchange.cloud.server import _run
from academy.exchange.cloud.server import _TIMEOUT_CODE
from academy.exchange.transport import ExchangeTransportMixin
from academy.exchange.transport import MailboxStatus
from academy.identifier import AgentId
from academy.identifier import EntityId
from academy.identifier import UserId
from academy.message import BaseMessage
from academy.message import Message
from academy.serialize import NoPickleMixin
from academy.socket import wait_connection

logger = logging.getLogger(__name__)


class _HttpConnectionInfo(NamedTuple):
    host: str
    port: int
    additional_headers: dict[str, str] | None = None
    scheme: Literal['http', 'https'] = 'http'
    ssl_verify: bool | None = None


@dataclasses.dataclass
class HttpAgentRegistration(Generic[BehaviorT]):
    """Agent registration for Http exchanges."""

    agent_id: AgentId[BehaviorT]
    """Unique identifier for the agent created by the exchange."""


class HttpExchangeTransport(ExchangeTransportMixin, NoPickleMixin):
    """Http exchange client.

    Args:
        mailbox_id: Identifier of the mailbox on the exchange. If there is
            not an id provided, the exchange will create a new client mailbox.
        session: Http session.
        connection_info: Exchange connection info.
    """

    def __init__(
        self,
        mailbox_id: EntityId,
        session: aiohttp.ClientSession,
        connection_info: _HttpConnectionInfo,
    ) -> None:
        self._mailbox_id = mailbox_id
        self._session = session
        self._info = connection_info

        scheme, host, port = (
            connection_info.scheme,
            connection_info.host,
            connection_info.port,
        )
        self._mailbox_url = f'{scheme}://{host}:{port}/mailbox'
        self._message_url = f'{scheme}://{host}:{port}/message'
        self._discover_url = f'{scheme}://{host}:{port}/discover'

    @classmethod
    async def new(
        cls,
        *,
        connection_info: _HttpConnectionInfo,
        mailbox_id: EntityId | None = None,
        name: str | None = None,
    ) -> Self:
        """Instantiate a new transport.

        Args:
            connection_info: Exchange connection information.
            mailbox_id: Bind the transport to the specific mailbox. If `None`,
                a new user entity will be registered and the transport will be
                bound to that mailbox.
            name: Display name of the redistered entity if `mailbox_id` is
                `None`.

        Returns:
            An instantiated transport bound to a specific mailbox.
        """
        ssl_verify = connection_info.ssl_verify
        if ssl_verify is None:  # pragma: no branch
            ssl_verify = connection_info.scheme == 'https'

        session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(ssl=ssl_verify),
            headers=connection_info.additional_headers,
        )

        if mailbox_id is None:
            scheme, host, port = (
                connection_info.scheme,
                connection_info.host,
                connection_info.port,
            )
            mailbox_id = UserId.new(name=name)
            async with session.post(
                f'{scheme}://{host}:{port}/mailbox',
                json={'mailbox': mailbox_id.model_dump_json()},
            ) as response:
                response.raise_for_status()
            logger.info('Registered %s in exchange', mailbox_id)

        return cls(mailbox_id, session, connection_info)

    @property
    def mailbox_id(self) -> EntityId:
        return self._mailbox_id

    async def close(self) -> None:
        await self._session.close()

    async def discover(
        self,
        behavior: type[Behavior],
        *,
        allow_subclasses: bool = True,
    ) -> tuple[AgentId[Any], ...]:
        behavior_str = f'{behavior.__module__}.{behavior.__name__}'
        async with self._session.get(
            self._discover_url,
            json={
                'behavior': behavior_str,
                'allow_subclasses': allow_subclasses,
            },
        ) as response:
            response.raise_for_status()
            agent_ids_str = (await response.json())['agent_ids']
        agent_ids = [aid for aid in agent_ids_str.split(',') if len(aid) > 0]
        return tuple(AgentId(uid=uuid.UUID(aid)) for aid in agent_ids)

    def factory(self) -> HttpExchangeFactory:
        return HttpExchangeFactory(
            host=self._info.host,
            port=self._info.port,
            additional_headers=self._info.additional_headers,
            scheme=self._info.scheme,
            ssl_verify=self._info.ssl_verify,
        )

    async def recv(self, timeout: float | None = None) -> Message:
        try:
            async with self._session.get(
                self._message_url,
                json={
                    'mailbox': self.mailbox_id.model_dump_json(),
                    'timeout': timeout,
                },
                timeout=aiohttp.ClientTimeout(timeout),
            ) as response:
                if response.status == _FORBIDDEN_CODE:
                    raise MailboxClosedError(self.mailbox_id)
                elif response.status == _TIMEOUT_CODE:
                    raise TimeoutError()
                response.raise_for_status()
                message_raw = (await response.json()).get('message')
        except asyncio.TimeoutError as e:
            # In older versions of Python, ayncio.TimeoutError and TimeoutError
            # are different types.
            raise TimeoutError(
                f'Failed to receive response in {timeout} seconds.',
            ) from e

        return BaseMessage.model_from_json(message_raw)

    async def register_agent(
        self,
        behavior: type[BehaviorT],
        *,
        name: str | None = None,
    ) -> HttpAgentRegistration[BehaviorT]:
        aid: AgentId[BehaviorT] = AgentId.new(name=name)
        async with self._session.post(
            self._mailbox_url,
            json={
                'mailbox': aid.model_dump_json(),
                'behavior': ','.join(behavior.behavior_mro()),
            },
        ) as response:
            response.raise_for_status()
        return HttpAgentRegistration(agent_id=aid)

    async def send(self, message: Message) -> None:
        async with self._session.put(
            self._message_url,
            json={'message': message.model_dump_json()},
        ) as response:
            if response.status == _NOT_FOUND_CODE:
                raise BadEntityIdError(message.dest)
            elif response.status == _FORBIDDEN_CODE:
                raise MailboxClosedError(message.dest)
            response.raise_for_status()

    async def status(self, uid: EntityId) -> MailboxStatus:
        async with self._session.get(
            self._mailbox_url,
            json={'mailbox': uid.model_dump_json()},
        ) as response:
            response.raise_for_status()
            status = (await response.json())['status']
            return MailboxStatus(status)

    async def terminate(self, uid: EntityId) -> None:
        async with self._session.delete(
            self._mailbox_url,
            json={'mailbox': uid.model_dump_json()},
        ) as response:
            response.raise_for_status()


class HttpExchangeFactory(ExchangeFactory[HttpExchangeTransport]):
    """Http exchange client factory.

    Args:
        host: Host name of the exchange server.
        port: Port of the exchange server.
        additional_headers: Any other information necessary to communicate
            with the exchange. Used for passing the Globus bearer token
        scheme: HTTP scheme, non-protected "http" by default.
        ssl_verify: Same as requests.Session.verify. If the server's TLS
            certificate should be validated. Should be true if using HTTPS
            Only set to false for testing or local development.
    """

    def __init__(
        self,
        host: str,
        port: int,
        additional_headers: dict[str, str] | None = None,
        scheme: Literal['http', 'https'] = 'http',
        ssl_verify: bool | None = None,
    ) -> None:
        self._info = _HttpConnectionInfo(
            host=host,
            port=port,
            additional_headers=additional_headers,
            scheme=scheme,
            ssl_verify=ssl_verify,
        )

    async def _create_transport(
        self,
        mailbox_id: EntityId | None = None,
        *,
        name: str | None = None,
        registration: HttpAgentRegistration[Any] | None = None,  # type: ignore[override]
    ) -> HttpExchangeTransport:
        return await HttpExchangeTransport.new(
            connection_info=self._info,
            mailbox_id=mailbox_id,
            name=name,
        )


@contextlib.contextmanager
def spawn_http_exchange(
    host: str = '0.0.0.0',
    port: int = 5463,
    *,
    level: int | str = logging.WARNING,
    timeout: float | None = None,
) -> Generator[HttpExchangeFactory]:
    """Context manager that spawns an HTTP exchange in a subprocess.

    This function spawns a new process (rather than forking) and wait to
    return until a connection with the exchange has been established.
    When exiting the context manager, `SIGINT` will be sent to the exchange
    process. If the process does not exit within 5 seconds, it will be
    killed.

    Warning:
        The exclusion of authentication and ssl configuration is
        intentional. This method should only be used for temporary exchanges
        in trusted environments (i.e. the login node of a cluster).

    Args:
        host: Host the exchange should listen on.
        port: Port the exchange should listen on.
        level: Logging level.
        timeout: Connection timeout when waiting for exchange to start.

    Returns:
        Exchange interface connected to the spawned exchange.
    """
    # Fork is not safe in multi-threaded context.
    multiprocessing.set_start_method('spawn')

    config = ExchangeServingConfig(host=host, port=port, log_level=level)
    exchange_process = multiprocessing.Process(
        target=_run,
        args=(config,),
    )
    exchange_process.start()

    logger.info('Starting exchange server...')
    wait_connection(host, port, timeout=timeout)
    logger.info('Started exchange server!')

    factory = HttpExchangeFactory(host, port)
    try:
        yield factory
    finally:
        logger.info('Terminating exchange server...')
        wait = 5
        exchange_process.terminate()
        exchange_process.join(timeout=wait)
        if exchange_process.exitcode is None:  # pragma: no cover
            logger.info(
                'Killing exchange server after waiting %s seconds',
                wait,
            )
            exchange_process.kill()
        else:
            logger.info('Terminated exchange server!')
        exchange_process.close()
