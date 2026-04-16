# ruff: noqa: D102
from __future__ import annotations

import asyncio
import dataclasses
import functools
import logging
import sys
import threading
import time
import uuid
from collections.abc import AsyncGenerator
from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from datetime import timedelta
from typing import Any
from typing import ClassVar
from typing import Generic
from typing import NamedTuple

from aiohttp import hdrs

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from typing import TYPE_CHECKING
from typing import TypeVar

import globus_sdk
from globus_sdk import AuthClient
from globus_sdk import DependentScopeSpec
from globus_sdk import GlobusApp
from globus_sdk import Scope
from globus_sdk.authorizers import GlobusAuthorizer
from globus_sdk.client import GlobusHTTPResponse
from globus_sdk.exc import GlobusAPIError
from globus_sdk.gare import GlobusAuthorizationParameters
from globus_sdk.scopes import AuthScopes
from globus_sdk.transport.requests import RequestsTransport

from academy.exception import BadEntityIdError
from academy.exception import MailboxTerminatedError
from academy.exception import UnauthorizedError
from academy.exchange.cloud.app import StatusCode
from academy.exchange.cloud.login import get_globus_app
from academy.exchange.cloud.scopes import AcademyExchangeScopes
from academy.exchange.cloud.scopes import get_academy_exchange_scope_id
from academy.exchange.factory import ExchangeFactory
from academy.exchange.transport import ExchangeTransportMixin
from academy.exchange.transport import MailboxStatus
from academy.identifier import AgentId
from academy.identifier import EntityId
from academy.identifier import UserId
from academy.message import Message
from academy.serialize import NoPickleMixin

if TYPE_CHECKING:
    from academy.agent import Agent
    from academy.agent import AgentT
else:
    AgentT = TypeVar('AgentT')

logger = logging.getLogger(__name__)


class _AcademyConnectionInfo(NamedTuple):
    project_id: uuid.UUID
    client_params: dict[str, Any] | None = None
    request_timeout_s: float = 60


class AcademyAPIError(GlobusAPIError):
    """Error class to represent error responses from Academy."""


class AcademyGlobusClient(globus_sdk.BaseClient):
    """A globus service client to make requests to hosted exchange.

    The GlobusExchangeClient acts as a wrapper through which authenticated
    requests are issued. The exchange automatically handles things like
    retrying, refreshing tokens, and exponential backoff.
    The [`BaseClient`][globus_sdk.BaseClient] is implemented using requests,
    so calls are synchronous, and must be run within a thread when using
    asyncio.
    """

    base_url = 'https://exchange.academy-agents.org'
    scopes = AcademyExchangeScopes
    default_scope_requirements: ClassVar[list[Scope]] = [
        AcademyExchangeScopes.academy_exchange,
    ]
    error_class = AcademyAPIError

    _mailbox_url = '/mailbox'
    _message_url = '/message'
    _discover_url = '/discover'
    _listen_url = '/mailbox/listen'

    def discover(
        self,
        agent: type[Agent] | str,
        *,
        allow_subclasses: bool = True,
    ) -> GlobusHTTPResponse:
        if isinstance(agent, str):
            agent_str = agent
        else:
            agent_str = f'{agent.__module__}.{agent.__name__}'
        return self.request(
            'GET',
            self._discover_url,
            data={
                'agent': agent_str,
                'allow_subclasses': allow_subclasses,
            },
        )

    def _parse(self, raw_lines: list[str]) -> Message[Any] | None:
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

    def listen(
        self,
        mailbox_id: EntityId,
        timeout: float,
    ) -> Generator[Message[Any]]:

        headers: dict[str, str] = {
            hdrs.ACCEPT: 'text/event-stream',
            hdrs.CACHE_CONTROL: 'no-cache',
        }
        globus_response = self.request(
            'GET',
            self._listen_url,
            data={
                'mailbox': mailbox_id.model_dump_json(),
                'timeout': timeout,
            },
            headers=headers,
            stream=True,
        )
        raw_response = globus_response._raw_response

        current_message_lines: list[str] = []
        for line_in_bytes in raw_response.iter_lines():
            line = line_in_bytes.decode('utf8')  # type: str
            line = line.rstrip('\n').rstrip('\r')
            if line == '':
                message = self._parse(current_message_lines)
                current_message_lines = []
                if message is None:
                    continue
                yield message
                continue

            current_message_lines.append(line)

    def recv(
        self,
        mailbox_id: EntityId,
        timeout: float | None = None,
    ) -> GlobusHTTPResponse:
        return self.request(
            'GET',
            self._message_url,
            data={
                'mailbox': mailbox_id.model_dump_json(),
                'timeout': timeout,
            },
        )

    def register_agent(
        self,
        agent_id: AgentId[AgentT],
        agent: type[AgentT],
    ) -> GlobusHTTPResponse:
        return self.post(
            self._mailbox_url,
            data={
                'mailbox': agent_id.model_dump_json(),
                'agent': ','.join(agent._agent_mro()),
            },
        )

    def register_client(
        self,
        mailbox_id: EntityId,
    ) -> GlobusHTTPResponse:
        return self.post(
            self._mailbox_url,
            data={
                'mailbox': mailbox_id.model_dump_json(),
            },
        )

    def send(self, message: Message[Any]) -> GlobusHTTPResponse:
        return self.put(
            self._message_url,
            data={'message': message.model_dump_json()},
        )

    def status(self, uid: EntityId) -> GlobusHTTPResponse:
        return self.request(
            'GET',
            self._mailbox_url,
            data={'mailbox': uid.model_dump_json()},
        )

    def terminate(self, uid: EntityId) -> GlobusHTTPResponse:
        return self.request(
            'DELETE',
            self._mailbox_url,
            data={'mailbox': uid.model_dump_json()},
        )


@dataclasses.dataclass
class _PendingRegistration(Generic[AgentT]):
    """Intermediate state for a registration awaiting token delegation.

    Created during the prepare phase of batch registration. Contains
    the Globus Auth client, credential, and scope but not yet the
    delegated token (which requires user consent).
    """

    agent_id: AgentId[AgentT]
    client_id: uuid.UUID
    secret: str
    scope: Scope


@dataclasses.dataclass
class GlobusAgentRegistration(Generic[AgentT]):
    """Agent registration for hosted globus exchange."""

    agent_id: AgentId[AgentT]
    """Unique identifier for the agent created by the exchange."""

    client_id: uuid.UUID
    """Client ID of Globus resource server.

    Each agent is a resources server.  This allows the agent to exchange
    delegated tokens to act on the exchange on behalf of the client, and
    to create it's own delegated tokens so other agents can act on its
    behalf.
    """

    token: str
    """Auth. token provided by launching client (user or another agent)."""

    secret: str
    """Secret for agent to use to authenticate itself with Globus Auth.

    Agents are created as hybrid resource servers within the Globus ecosystem.
    This allows them to use delegated tokens, but also requires the them to
    be able to store secrets. In order to support the launching of agents,
    we pass a secret as part of the agent initialization. We assume the
    security of the launching channel (typically Globus Compute in the
    Academy ecosystem).
    """


class GlobusExchangeTransport(ExchangeTransportMixin, NoPickleMixin):
    """Globus exchange client.

    Args:
        mailbox_id: Identifier of the mailbox on the exchange. If there is
            not an id provided, the exchange will create a new client mailbox.
        connection_info: Project id, client parameters and other information
            about the connection to the service.
        app: For user authorization through token retrieval.
        authorizer: For service authorization through token retrieval.
    """

    def __init__(
        self,
        mailbox_id: EntityId,
        *,
        connection_info: _AcademyConnectionInfo,
        app: GlobusApp | None = None,
        authorizer: GlobusAuthorizer | None = None,
    ) -> None:
        self._mailbox_id = mailbox_id
        self.project = connection_info.project_id
        self.child_clients: list[uuid.UUID] = []
        self.client_params = connection_info.client_params or {}

        self.login_time = datetime.min
        self._app = app
        self._authorizer = authorizer
        self._auth_lock = threading.Lock()
        self._local_data = threading.local()
        self.executor = ThreadPoolExecutor(
            thread_name_prefix='exchange-globus-thread',
        )

        # We need use a single thread for the listener connection
        # because we need to make sure calls to next get routed to
        # the same AcademyGlobusClient
        self.listening_thread = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix='exchange-globus-thread-listen',
        )
        self.request_timeout_s = connection_info.request_timeout_s

    @property
    def exchange_client(self) -> AcademyGlobusClient:
        """A thread local copy of the Globus AuthClient."""
        try:
            return self._local_data.exchange_client
        except AttributeError:
            self._local_data.exchange_client = AcademyGlobusClient(
                app=self._app,
                authorizer=self._authorizer,
                transport=RequestsTransport(http_timeout=-1),
                **self.client_params,
            )
            return self._local_data.exchange_client

    @property
    def auth_client(self) -> AuthClient:
        """A thread local copy of the Globus AuthClient."""
        # Fast path: return cached thread-local client.
        if datetime.now() - self.login_time < timedelta(minutes=30):
            try:
                return self._local_data.auth_client
            except AttributeError:
                logger.debug(
                    'Auth client does not exist for thread.',
                )
        else:
            # Login required — serialize to prevent duplicate
            # interactive prompts from concurrent threads.
            with self._auth_lock:
                if datetime.now() - self.login_time >= timedelta(minutes=30):
                    logger.debug('Auth client login timeout.')

                    if self._app is None:
                        raise NotImplementedError(
                            'Launching child agents is'
                            ' currently not implemented.',
                        )

                    self._app.add_scope_requirements(
                        {
                            AuthScopes.resource_server: [
                                AuthScopes.manage_projects,
                                AuthScopes.email,
                                AuthScopes.profile,
                            ],
                        },
                    )

                    self._app.login(
                        force=True,
                        auth_params=GlobusAuthorizationParameters(
                            prompt='login',
                        ),
                    )
                    self.login_time = datetime.now()

        logger.info('Initializing auth client.')
        self._local_data.auth_client = AuthClient(
            app=self._app,
        )
        return self._local_data.auth_client

    def _register_client(self) -> None:
        self.exchange_client.register_client(self.mailbox_id)

    @classmethod
    async def new(
        cls,
        *,
        connection_info: _AcademyConnectionInfo,
        app: GlobusApp | None = None,
        authorizer: GlobusAuthorizer | None = None,
        mailbox_id: EntityId | None = None,
        name: str | None = None,
    ) -> Self:
        """Instantiate a new transport.

        Args:
            connection_info: Project id, client parameters, and request timeout
                to establish connection
            app: For user authorization through token retrieval
            authorizer: For service authorization through token retrieval
            mailbox_id: Bind the transport to the specific mailbox. If `None`,
                a new user entity will be registered and the transport will be
                bound to that mailbox.
            name: Display name of the registered entity if `mailbox_id` is
                `None`.

        Returns:
            An instantiated transport bound to a specific mailbox.
        """
        loop = asyncio.get_running_loop()

        if mailbox_id is None:
            mailbox_id = UserId.new(name=name)
            client = cls(
                mailbox_id,
                connection_info=connection_info,
                app=app,
                authorizer=authorizer,
            )
            await loop.run_in_executor(
                client.executor,
                client._register_client,
            )
            logger.info(
                'Registered %s in exchange',
                mailbox_id,
                extra={'academy.mailbox_id': mailbox_id},
            )
            return client

        return cls(
            mailbox_id,
            connection_info=connection_info,
            app=app,
            authorizer=authorizer,
        )

    @property
    def mailbox_id(self) -> EntityId:
        return self._mailbox_id

    def _clean_up(self) -> None:
        for client_id in self.child_clients:
            self.auth_client.delete_client(client_id)

    async def close(self) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self.executor, self._clean_up)

    def _discover(
        self,
        agent: type[Agent] | str,
        allow_subclasses: bool,
    ) -> GlobusHTTPResponse:
        return self.exchange_client.discover(
            agent,
            allow_subclasses=allow_subclasses,
        )

    async def discover(
        self,
        agent: type[Agent] | str,
        *,
        allow_subclasses: bool = True,
    ) -> tuple[AgentId[Any], ...]:
        loop = asyncio.get_running_loop()
        response = await loop.run_in_executor(
            self.executor,
            self._discover,
            agent,
            allow_subclasses,
        )

        agent_ids_str = response['agent_ids']
        agent_ids = [aid for aid in agent_ids_str.split(',') if len(aid) > 0]
        return tuple(AgentId(uid=uuid.UUID(aid)) for aid in agent_ids)

    def factory(self) -> GlobusExchangeFactory:
        return GlobusExchangeFactory(
            self.project,
            self.client_params,
            self.request_timeout_s,
        )

    def _create_listener(self, timeout: float) -> None:
        self._local_data._listener = self.exchange_client.listen(
            self.mailbox_id,
            timeout,
        )

    def _listener_next(self) -> Message[Any] | None:
        try:
            return next(self._local_data._listener, None)
        except AcademyAPIError as e:
            if e.http_status == StatusCode.TERMINATED.value:
                raise MailboxTerminatedError(self.mailbox_id) from e
            raise e  # pragma: no cover

    async def listen(
        self,
        timeout: float | None = None,
    ) -> AsyncGenerator[Message[Any]]:
        loop = asyncio.get_running_loop()
        prev_time = time.time()
        while True:
            current_time = time.time()
            internal_timeout = (
                self.request_timeout_s
                if timeout is None
                else min(
                    (prev_time + timeout) - current_time,
                    self.request_timeout_s,
                )
            )
            if internal_timeout <= 0:
                raise TimeoutError()

            # Create the listen request stream
            await loop.run_in_executor(
                self.listening_thread,
                self._create_listener,
                internal_timeout,
            )

            while True:
                # Get next message
                message = await loop.run_in_executor(
                    self.listening_thread,
                    self._listener_next,
                )
                logger.debug(f'{self.mailbox_id} fetched message {message}.')
                if message is None:
                    break
                yield message

                prev_time = time.time()

    def _prepare_registration(
        self,
        aid: AgentId[AgentT],
    ) -> _PendingRegistration[AgentT]:
        """Create Globus Auth client, credential, and scope for an agent.

        This is phase 1 of registration. It creates all Globus Auth
        entities but does **not** accumulate scope requirements or
        trigger user consent. The caller is responsible for adding
        the returned scope via
        :meth:`GlobusApp.add_scope_requirements` and triggering
        login before calling :meth:`_finalize_registration`.
        """
        # Create new resource server (auth entity)
        logger.info('Creating new auth client')
        client_response = self.auth_client.create_client(
            str(aid),
            project=self.project,
            client_type='hybrid_confidential_client_resource_server',
            visibility='private',
        )
        client_id = client_response['client']['id']
        logger.info(
            f'Created client id: {client_id}',
            extra={'academy.client_id': client_id},
        )

        # Create secret
        logger.info('Creating secret.')
        credentials_response = self.auth_client.create_client_credential(
            client_id,
            'Launch Credentials',
        )
        secret = credentials_response['credential']['secret']

        # Create scope
        logger.info('Creating new scope.')
        scope_response = self.auth_client.create_scope(
            client_id,
            'Agent launch',
            'Launch agent',
            'launch',
            dependent_scopes=[
                DependentScopeSpec(
                    get_academy_exchange_scope_id(),
                    optional=False,
                    requires_refresh_token=True,
                ),
            ],
            allows_refresh_token=True,
        )
        scope = Scope.parse(scope_response['scopes'][0]['scope_string'])

        return _PendingRegistration(
            agent_id=aid,
            client_id=client_id,
            secret=secret,
            scope=scope,
        )

    def _finalize_registration(
        self,
        pending: _PendingRegistration[AgentT],
    ) -> GlobusAgentRegistration[AgentT]:
        """Obtain delegated token for a previously prepared agent.

        This is phase 2 of registration. The GlobusApp must already
        have valid tokens for the agent's scope (via a prior
        ``login()`` call that covered all accumulated scopes).
        """
        logger.info('Creating delegated token.')
        assert self._app is not None
        authorizer = self._app.get_authorizer(str(pending.client_id))
        bearer = authorizer.get_authorization_header()
        if bearer is None:  # pragma: no cover
            raise UnauthorizedError(
                'Unable to get authorization headers.',
            )

        auth_header_parts = bearer.split(' ')
        token = auth_header_parts[1]

        return GlobusAgentRegistration(
            agent_id=pending.agent_id,
            client_id=pending.client_id,
            token=token,
            secret=pending.secret,
        )

    def _create_registration(
        self,
        aid: AgentId[AgentT],
    ) -> GlobusAgentRegistration[AgentT]:
        """Create a single agent registration (prepare + finalize).

        Convenience method that runs both phases sequentially for a
        single agent. Triggers a consent prompt for the new scope.
        """
        pending = self._prepare_registration(aid)
        assert self._app is not None
        self._app.add_scope_requirements(
            {str(pending.client_id): [pending.scope]},
        )
        return self._finalize_registration(pending)

    def _create_mailbox(
        self,
        aid: AgentId[AgentT],
        agent: type[AgentT],
    ) -> None:
        self.exchange_client.register_agent(aid, agent)

    async def register_agent(
        self,
        agent: type[AgentT],
        *,
        name: str | None = None,
    ) -> GlobusAgentRegistration[AgentT]:
        loop = asyncio.get_running_loop()

        # Create registration
        aid: AgentId[AgentT] = AgentId.new(name=name)
        registration = await loop.run_in_executor(
            self.executor,
            self._create_registration,
            aid,
        )

        # Create mailbox
        await loop.run_in_executor(
            self.executor,
            self._create_mailbox,
            aid,
            agent,
        )

        self.child_clients.append(registration.client_id)

        return registration

    async def register_agents(
        self,
        agents: list[tuple[type[AgentT], str | None]],
    ) -> list[GlobusAgentRegistration[AgentT]]:
        """Register multiple agents with a single auth prompt.

        Args:
            agents: List of (agent_type, name) pairs to register.

        Returns:
            List of agent registrations in order of input.
        """
        loop = asyncio.get_running_loop()

        # Phase 1: create auth entities concurrently.
        # _prepare_registration no longer mutates shared GlobusApp
        # state, and the auth_client property serializes interactive
        # login via _auth_lock, so parallel dispatch is safe.
        aids: list[AgentId[AgentT]] = [
            AgentId.new(name=name) for _, name in agents
        ]
        prepared: list[_PendingRegistration[AgentT]] = list(
            await asyncio.gather(
                *(
                    loop.run_in_executor(
                        self.executor,
                        self._prepare_registration,
                        aid,
                    )
                    for aid in aids
                ),
            ),
        )
        pending = list(
            zip(
                prepared,
                [at for at, _ in agents],
                strict=True,
            ),
        )

        # Accumulate scope requirements, then do a single login
        # that satisfies all of them at once.
        assert self._app is not None
        for p, _ in pending:
            self._app.add_scope_requirements(
                {str(p.client_id): [p.scope]},
            )

        if self._app.login_required():
            await loop.run_in_executor(
                self.executor,
                self._app.login,
            )

        # Phase 2: extract delegated tokens and create mailboxes.
        # Safe to parallelize — no shared state mutation, and the
        # app already holds valid tokens for all scopes.
        regs: list[GlobusAgentRegistration[AgentT]] = list(
            await asyncio.gather(
                *(
                    loop.run_in_executor(
                        self.executor,
                        self._finalize_registration,
                        p,
                    )
                    for p, _ in pending
                ),
            ),
        )

        await asyncio.gather(
            *(
                loop.run_in_executor(
                    self.executor,
                    self._create_mailbox,
                    reg.agent_id,
                    agent_type,
                )
                for reg, (_, agent_type) in zip(
                    regs,
                    pending,
                    strict=True,
                )
            ),
        )

        for reg in regs:
            self.child_clients.append(reg.client_id)

        return regs

    def _send(self, message: Message[Any]) -> None:
        self.exchange_client.send(message)

    async def send(self, message: Message[Any]) -> None:
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(
                self.executor,
                self._send,
                message,
            )
        except AcademyAPIError as e:
            if e.http_status == StatusCode.NOT_FOUND.value:
                raise BadEntityIdError(message.dest) from e
            elif e.http_status == StatusCode.TERMINATED.value:
                raise MailboxTerminatedError(message.dest) from e
            raise e  # pragma: no cover

    def _status(self, uid: EntityId) -> MailboxStatus:
        response = self.exchange_client.status(uid)
        status = response['status']
        return MailboxStatus(status)

    async def status(self, uid: EntityId) -> MailboxStatus:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self.executor,
            self._status,
            uid,
        )

    def _terminate(self, uid: EntityId) -> None:
        self.exchange_client.terminate(uid)

    async def terminate(self, uid: EntityId) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            self.executor,
            self._terminate,
            uid,
        )


class GlobusExchangeFactory(ExchangeFactory[GlobusExchangeTransport]):
    """Globus exchange client factory.

    Args:
        project_id: Project to create new clients under. Must be able
            to authenticate as a administrator.
        client_params: Additional parameters for globus client.
    """

    def __init__(
        self,
        project_id: uuid.UUID,
        client_params: dict[str, Any] | None = None,
        request_timeout_s: float = 60,
    ) -> None:
        self.info = _AcademyConnectionInfo(
            project_id=project_id,
            client_params=client_params,
            request_timeout_s=request_timeout_s,
        )

    async def _create_transport(
        self,
        mailbox_id: EntityId | None = None,
        *,
        name: str | None = None,
        registration: GlobusAgentRegistration[Any] | None = None,  # type: ignore[override]
    ) -> GlobusExchangeTransport:
        loop = asyncio.get_running_loop()

        if registration is None:
            app = await loop.run_in_executor(
                None,
                get_globus_app,
            )
            return await GlobusExchangeTransport.new(
                app=app,
                mailbox_id=mailbox_id,
                name=name,
                connection_info=self.info,
            )
        else:
            logger.info('Initializing auth client for new agent.')
            auth_client = globus_sdk.ConfidentialAppAuthClient(
                client_id=registration.client_id,
                client_secret=registration.secret,
            )

            logger.info('Exchanging dependent token.')
            dependent_token_response = await loop.run_in_executor(
                None,
                functools.partial(
                    auth_client.oauth2_get_dependent_tokens,
                    registration.token,
                    refresh_tokens=True,
                    scope=str(AcademyExchangeScopes.academy_exchange),
                ),
            )

            # TODO: dependent token only provides access to exchange
            # So agents do not have access to auth. to create new agents.
            authorizer = globus_sdk.RefreshTokenAuthorizer(
                dependent_token_response.by_resource_server[
                    AcademyExchangeScopes.resource_server
                ]['refresh_token'],
                auth_client,
            )

            return await GlobusExchangeTransport.new(
                authorizer=authorizer,
                mailbox_id=mailbox_id,
                name=name,
                connection_info=self.info,
            )
