# ruff: noqa: D102
from __future__ import annotations

import asyncio
import contextlib
import logging
import multiprocessing
import sys
import time
import uuid
from collections.abc import AsyncGenerator
from collections.abc import Generator
from typing import Any
from typing import Generic
from typing import Literal
from typing import NamedTuple
from typing import TYPE_CHECKING
from urllib.parse import urlparse

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

import aiohttp
from aiohttp import hdrs
from pydantic import BaseModel
from pydantic import Field

from academy.exception import BadEntityIdError
from academy.exception import ForbiddenError
from academy.exception import MailboxTerminatedError
from academy.exception import UnauthorizedError
from academy.exchange.cloud.app import _run
from academy.exchange.cloud.app import StatusCode
from academy.exchange.cloud.config import ExchangeServingConfig
from academy.exchange.cloud.config import LogConfig
from academy.exchange.cloud.login import get_auth_headers
from academy.exchange.factory import ExchangeFactory
from academy.exchange.transport import ExchangeTransportMixin
from academy.exchange.transport import MailboxStatus
from academy.identifier import AgentId
from academy.identifier import EntityId
from academy.identifier import UserId
from academy.message import Message
from academy.serialize import NoPickleMixin
from academy.socket import wait_connection

if TYPE_CHECKING:
    from academy.agent import Agent
    from academy.agent import AgentT
else:
    from academy.identifier import AgentT

logger = logging.getLogger(__name__)

DEFAULT_EXCHANGE_URL = 'https://exchange.academy-agents.org'


class _HttpConnectionInfo(NamedTuple):
    url: str
    additional_headers: dict[str, str] | None = None
    ssl_verify: bool | None = None
    request_timeout_s: float = 60
    client_timeout: aiohttp.ClientTimeout | None = None


class HttpAgentRegistration(BaseModel, Generic[AgentT]):
    """Agent registration for Http exchanges."""

    agent_id: AgentId[AgentT]
    """Unique identifier for the agent created by the exchange."""

    exchange_type: Literal['http'] = Field('http', repr=False)


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
        self._retry_time_ms: float = 1000
        self._last_event_id: int | None = None

        base_url = self._info.url
        self._mailbox_url = f'{base_url}/mailbox'
        self._message_url = f'{base_url}/message'
        self._discover_url = f'{base_url}/discover'
        self._listen_url = f'{base_url}/mailbox/listen'
        self._heartbeat_url = f'{base_url}/mailbox/heartbeat'

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
            name: Display name of the registered entity if `mailbox_id` is
                `None`.

        Returns:
            An instantiated transport bound to a specific mailbox.
        """
        ssl_verify = connection_info.ssl_verify
        if ssl_verify is None:  # pragma: no branch
            scheme = urlparse(connection_info.url).scheme
            ssl_verify = scheme == 'https'

        session_kwargs: dict[str, Any] = {}
        if connection_info.client_timeout is not None:
            session_kwargs['timeout'] = connection_info.client_timeout

        session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(ssl=ssl_verify),
            headers=connection_info.additional_headers,
            trust_env=True,
            **session_kwargs,
        )

        if mailbox_id is None:
            mailbox_id = UserId.new(name=name)
            async with session.post(
                f'{connection_info.url}/mailbox',
                json={'mailbox': mailbox_id.model_dump_json()},
            ) as response:
                _raise_for_status(response, mailbox_id)
            logger.info(
                'Registered %s in exchange',
                mailbox_id,
                extra={'academy.mailbox_id': mailbox_id},
            )

        return cls(mailbox_id, session, connection_info)

    @property
    def mailbox_id(self) -> EntityId:
        return self._mailbox_id

    async def close(self) -> None:
        await self._session.close()

    async def discover(
        self,
        agent: type[Agent] | str,
        *,
        allow_subclasses: bool = True,
    ) -> tuple[AgentId[Any], ...]:

        if isinstance(agent, str):
            agent_str = agent
        else:
            agent_str = f'{agent.__module__}.{agent.__name__}'

        async with self._session.get(
            self._discover_url,
            json={
                'agent': agent_str,
                'allow_subclasses': allow_subclasses,
            },
        ) as response:
            _raise_for_status(response, self.mailbox_id)
            agent_ids_str = (await response.json())['agent_ids']
        agent_ids = [aid for aid in agent_ids_str.split(',') if len(aid) > 0]
        return tuple(AgentId(uid=uuid.UUID(aid)) for aid in agent_ids)

    def factory(self) -> HttpExchangeFactory:
        # Note: When getting factory, auth method is not preserved
        # but auth headers (i.e. the bearer token) is.
        return HttpExchangeFactory(
            url=self._info.url,
            additional_headers=self._info.additional_headers,
            ssl_verify=self._info.ssl_verify,
        )

    async def parse(self, raw_lines: list[str]) -> Message[Any] | None:
        data = ''
        for line in raw_lines:
            if line[0] == ':':
                logger.debug(f'Received comment from server: {line[1:]}')
                continue
            fields = line.split(':', 1)
            field_name = fields[0]
            field_value = fields[1].lstrip(' ') if len(fields) > 1 else ''
            if field_name == 'id':
                self._last_event_id = int(field_value)
            elif field_name == 'retry':
                self._retry_time_ms = int(field_value)
            elif field_name == 'data':
                data += f'{field_value}\n'
            else:
                logger.warning(
                    'Received unexpected field in event stream '
                    f'{field_name}: {field_value}',
                )
        if data == '':
            # This happens when the server sends a ping message to keep the
            #  stream alive
            return None
        return Message.model_validate_json(data)

    async def listen(
        self,
        timeout: float | None = None,
    ) -> AsyncGenerator[Message[Any]]:

        prev_time = time.time()
        headers: dict[str, str] = {
            hdrs.ACCEPT: 'text/event-stream',
            hdrs.CACHE_CONTROL: 'no-cache',
        }
        if self._last_event_id:  # pragma: no cover
            # Right now we do not keep track of the last message we've seen
            # because we don't store messages for retransmission.
            headers['Last-Event-Id'] = str(self._last_event_id)

        while True:
            current_time = time.time()
            internal_timeout = (
                self._info.request_timeout_s
                if timeout is None
                else min(
                    (prev_time + timeout) - current_time,
                    self._info.request_timeout_s,
                )
            )
            if internal_timeout <= 0:
                raise TimeoutError()

            response = await self._session.get(
                self._listen_url,
                json={
                    'mailbox': self.mailbox_id.model_dump_json(),
                    'timeout': internal_timeout,
                },
                headers=headers,
            )
            _raise_for_status(response, self.mailbox_id)

            current_message_lines: list[str] = []
            async for line_in_bytes in response.content:
                line = line_in_bytes.decode('utf8')  # type: str
                line = line.rstrip('\n').rstrip('\r')
                if line == '':
                    message = await self.parse(current_message_lines)
                    current_message_lines = []
                    if message is None:
                        continue
                    prev_time = time.time()
                    yield message
                    continue

                current_message_lines.append(line)

            current_time = time.time()
            if timeout and current_time - prev_time > timeout:
                raise TimeoutError()
            await asyncio.sleep(self._retry_time_ms / 1000)

    async def register_agent(
        self,
        agent: type[AgentT],
        *,
        name: str | None = None,
    ) -> HttpAgentRegistration[AgentT]:
        aid: AgentId[AgentT] = AgentId.new(name=name)
        async with self._session.post(
            self._mailbox_url,
            json={
                'mailbox': aid.model_dump_json(),
                'agent': ','.join(agent._agent_mro()),
            },
        ) as response:
            _raise_for_status(response, self.mailbox_id, aid)
        return HttpAgentRegistration(agent_id=aid)

    async def send(self, message: Message[Any]) -> None:
        async with self._session.put(
            self._message_url,
            json={'message': message.model_dump_json()},
        ) as response:
            _raise_for_status(response, self.mailbox_id, message.dest)

    async def status(self, uid: EntityId) -> MailboxStatus:
        async with self._session.get(
            self._mailbox_url,
            json={'mailbox': uid.model_dump_json()},
        ) as response:
            _raise_for_status(response, self.mailbox_id, uid)
            status = (await response.json())['status']
            return MailboxStatus(status)

    async def terminate(self, uid: EntityId) -> None:
        async with self._session.delete(
            self._mailbox_url,
            json={'mailbox': uid.model_dump_json()},
        ) as response:
            _raise_for_status(response, self.mailbox_id, uid)

    async def update_heartbeat(self) -> None:
        pass  # Server tracks this automatically via listen/send

    async def heartbeat_status(self, uid: EntityId) -> float | None:
        async with self._session.get(
            self._heartbeat_url,
            json={'mailbox': uid.model_dump_json()},
        ) as response:
            _raise_for_status(response, self.mailbox_id, uid)
            return (await response.json())['heartbeat']


class HttpExchangeConsole:
    """Client for Http/Cloud specific exchange operations.

    Args:
        session: Http session.
        connection_info: Exchange connection info.
    """

    def __init__(
        self,
        session: aiohttp.ClientSession,
        connection_info: _HttpConnectionInfo,
    ) -> None:
        self._session = session
        self._info = connection_info

        base_url = self._info.url
        self._share_url = f'{base_url}/mailbox/share'

    @classmethod
    async def new(
        cls,
        *,
        connection_info: _HttpConnectionInfo,
    ) -> Self:
        """Instantiate a new console.

        Args:
            connection_info: Exchange connection information.

        Returns:
            An instantiated transport bound to a specific mailbox.
        """
        ssl_verify = connection_info.ssl_verify
        if ssl_verify is None:  # pragma: no branch
            scheme = urlparse(connection_info.url).scheme
            ssl_verify = scheme == 'https'

        session_kwargs: dict[str, Any] = {}
        if connection_info.client_timeout is not None:
            session_kwargs['timeout'] = connection_info.client_timeout

        session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(ssl=ssl_verify),
            headers=connection_info.additional_headers,
            trust_env=True,
            **session_kwargs,
        )
        return cls(session, connection_info)

    def factory(self) -> HttpExchangeFactory:
        # Note: When getting factory, auth method is not preserved
        # but auth headers (i.e. the bearer token) is.
        return HttpExchangeFactory(
            url=self._info.url,
            additional_headers=self._info.additional_headers,
            ssl_verify=self._info.ssl_verify,
            request_timeout_s=self._info.request_timeout_s,
            client_timeout=self._info.client_timeout,
        )

    async def share_mailbox(
        self,
        mailbox_id: EntityId,
        group_id: uuid.UUID,
    ) -> None:
        """Share mailbox with group.

        Args:
            mailbox_id: Either AgentId or UserId of mailbox
            group_id: Id of globus group. User must be part of group to share
                mailbox.
        """
        async with self._session.post(
            self._share_url,
            json={
                'mailbox': mailbox_id.model_dump_json(),
                'group_id': str(group_id),
            },
        ) as response:
            _raise_for_status(response, None, mailbox_id)

    async def get_shared_groups(self, mailbox_id: EntityId) -> list[uuid.UUID]:
        """Get the groups mailbox is shared with.

        Args:
            mailbox_id: Either AgentId or UserId of mailbox
        """
        async with self._session.get(
            self._share_url,
            json={
                'mailbox': mailbox_id.model_dump_json(),
            },
        ) as response:
            _raise_for_status(response, None, mailbox_id)
            groups_str = (await response.json())['group_ids']
            return [uuid.UUID(group_id) for group_id in groups_str]

    async def remove_shared_group(
        self,
        mailbox_id: EntityId,
        group_id: uuid.UUID,
    ) -> None:
        """Stop sharing mailbox with a group.

        Args:
            mailbox_id: Either AgentId or UserId of mailbox
            group_id: Id of globus group. User must be part of group to share
                mailbox.
        """
        async with self._session.delete(
            self._share_url,
            json={
                'mailbox': mailbox_id.model_dump_json(),
                'group_id': str(group_id),
            },
        ) as response:
            _raise_for_status(response, None, mailbox_id)

    async def close(self) -> None:
        """Close the console session."""
        await self._session.close()


class HttpExchangeFactory(ExchangeFactory[HttpExchangeTransport]):
    """Http exchange client factory.

    Args:
        url: Address of HTTP exchange. Defaults to the Academy-hosted exchange
        auth_method: Method to get authorization headers
        additional_headers: Any other information necessary to communicate
            with the exchange. Used for passing the Globus bearer token
        request_timeout_s: Server-side maximum length (seconds) of an SSE
            listen request before the server returns and the client reopens.
        ssl_verify: Same as requests.Session.verify. If the server's TLS
            certificate should be validated. Should be true if using HTTPS
            Only set to false for testing or local development.
        client_timeout: Timeout applied to the underlying
            [`aiohttp.ClientSession`][aiohttp.ClientSession]. Defaults to
            `aiohttp.ClientTimeout(total=None, sock_connect=30)`, which
            disables aiohttp's default 5-minute total request cap so that
            long-lived SSE listen connections are not severed mid-stream.
            Pass a custom [`aiohttp.ClientTimeout`][aiohttp.ClientTimeout] to
            override.
    """

    def __init__(  # noqa: PLR0913
        self,
        url: str = DEFAULT_EXCHANGE_URL,
        auth_method: Literal['globus'] | None = None,
        additional_headers: dict[str, str] | None = None,
        request_timeout_s: float = 60,
        ssl_verify: bool | None = None,
        client_timeout: aiohttp.ClientTimeout | None = None,
    ) -> None:
        if additional_headers is None:
            additional_headers = {}

        if (
            url == DEFAULT_EXCHANGE_URL
            and 'Authorization' not in additional_headers
        ):
            auth_method = 'globus'

        additional_headers |= get_auth_headers(auth_method)

        if client_timeout is None:
            # aiohttp's default ClientTimeout(total=300) closes SSE listen
            # connections after 5 minutes, leaving agents unable to receive
            # further messages. Disable the total cap and keep a connect
            # timeout so unreachable hosts still fail fast.
            client_timeout = aiohttp.ClientTimeout(total=None, sock_connect=30)

        self._info = _HttpConnectionInfo(
            url=url,
            additional_headers=additional_headers,
            ssl_verify=ssl_verify,
            request_timeout_s=request_timeout_s,
            client_timeout=client_timeout,
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

    async def console(self) -> HttpExchangeConsole:
        return await HttpExchangeConsole.new(
            connection_info=self._info,
        )


def _raise_for_status(
    response: aiohttp.ClientResponse,
    client_id: EntityId | None,
    resource_id: EntityId | None = None,
) -> None:
    # Parse HTTP error codes into the correct error types.
    #   - client_id is the ID of the transport client that is making
    #     the request.
    #   - resource_id is the ID of the resource being accessed in the case
    #     of operations like send/status/terminate.
    if response.status == StatusCode.UNAUTHORIZED.value:
        raise UnauthorizedError(
            f'Exchange entity {client_id} does not have the required '
            'authorization credentials.',
        )
    elif response.status == StatusCode.FORBIDDEN.value:
        raise ForbiddenError(
            f'Exchange entity {client_id} is not authorized to access '
            'this resource.',
        )
    elif response.status == StatusCode.NOT_FOUND.value:
        entity_id = resource_id or client_id
        assert entity_id is not None, (
            'Either client_id or resource_id must be provided.'
        )
        raise BadEntityIdError(entity_id)
    elif response.status == StatusCode.TERMINATED.value:
        entity_id = resource_id or client_id
        assert entity_id is not None, (
            'Either client_id or resource_id must be provided.'
        )
        raise MailboxTerminatedError(entity_id)
    elif response.status == StatusCode.TIMEOUT.value:
        raise TimeoutError()
    else:
        response.raise_for_status()


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
        in trusted environments (such as a fully firewalled individual
        workstation with no other user access).

    Args:
        host: Host the exchange should listen on.
        port: Port the exchange should listen on.
        level: Logging level.
        timeout: Connection timeout when waiting for exchange to start.

    Returns:
        Exchange interface connected to the spawned exchange.
    """
    config = ExchangeServingConfig(
        host=host,
        port=port,
        logger=LogConfig(level=level),
    )

    # Fork is not safe in multi-threaded context.
    context = multiprocessing.get_context('spawn')
    exchange_process = context.Process(target=_run, args=(config,))
    exchange_process.start()

    logger.info('Starting exchange server...')
    wait_connection(host, port, timeout=timeout)
    logger.info('Started exchange server')

    base_url = f'http://{host}:{port}'
    factory = HttpExchangeFactory(base_url)
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
                extra={'academy.delay': wait},
            )
            exchange_process.kill()
            exchange_process.join()
        else:
            logger.info('Terminated exchange server')
        exchange_process.close()
