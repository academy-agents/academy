from __future__ import annotations

import abc
import asyncio
import contextlib
import logging
import sys
import uuid
from collections.abc import Callable
from collections.abc import Coroutine
from types import TracebackType
from typing import Any
from typing import Generic
from typing import TYPE_CHECKING
from typing import TypeAlias
from typing import TypeVar
from weakref import WeakValueDictionary

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from academy.exception import MailboxTerminatedError
from academy.exchange.transport import AgentRegistration
from academy.exchange.transport import ExchangeTransportT
from academy.exchange.transport import MailboxStatus
from academy.handle import exchange_context
from academy.handle import Handle
from academy.identifier import AgentId
from academy.identifier import EntityId
from academy.identifier import UserId
from academy.message import ErrorResponse
from academy.message import Message
from academy.message import RequestT_co
from academy.task import spawn_guarded_background_task

if TYPE_CHECKING:
    from academy.agent import Agent
    from academy.agent import AgentT
    from academy.exchange.factory import ExchangeFactory
else:
    AgentT = TypeVar('AgentT')


logger = logging.getLogger(__name__)

RequestHandler: TypeAlias = Callable[
    [Message[RequestT_co]],
    Coroutine[None, None, None],
]


class ExchangeClient(abc.ABC, Generic[ExchangeTransportT]):
    """Base exchange client.

    Warning:
        Exchange clients should only be created via
        [`ExchangeFactory.create_agent_client()`][academy.exchange.ExchangeFactory.create_agent_client]
        or
        [`ExchangeFactory.create_user_client()`][academy.exchange.ExchangeFactory.create_user_client]!

    Args:
        transport: Exchange transport bound to a mailbox.
    """

    def __init__(
        self,
        transport: ExchangeTransportT,
    ) -> None:
        self._transport = transport
        self._handles: WeakValueDictionary[uuid.UUID, Handle[Any]] = (
            WeakValueDictionary()
        )
        self._close_lock = asyncio.Lock()
        self._closed = False

    async def __aenter__(self) -> Self:
        self.exchange_context_token = exchange_context.set(self)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        exchange_context.reset(self.exchange_context_token)
        await self.close()

    def __repr__(self) -> str:
        return f'{type(self).__name__}({self.client_id!r})'

    def __str__(self) -> str:
        return f'{type(self).__name__}<{self.client_id}>'

    @property
    @abc.abstractmethod
    def client_id(self) -> EntityId:
        """Client ID as registered with the exchange."""
        ...

    @abc.abstractmethod
    async def close(self) -> None:
        """Close the transport."""
        ...

    async def discover(
        self,
        agent: type[Agent],
        *,
        allow_subclasses: bool = True,
    ) -> tuple[AgentId[Any], ...]:
        """Discover peer agents with a given agent.

        Args:
            agent: Agent type of interest.
            allow_subclasses: Return agents implementing subclasses of the
                agent.

        Returns:
            Tuple of agent IDs implementing the agent.
        """
        return await self._transport.discover(
            agent,
            allow_subclasses=allow_subclasses,
        )

    def factory(self) -> ExchangeFactory[ExchangeTransportT]:
        """Get an exchange factory."""
        return self._transport.factory()

    def register_handle(self, handle: Handle[AgentT]) -> None:
        """Register an existing handle to receive messages.

        Args:
            handle: Handle to register.
        """
        self._handles[handle.handle_id] = handle

    async def register_agent(
        self,
        agent: type[AgentT],
        *,
        name: str | None = None,
    ) -> AgentRegistration[AgentT]:
        """Register a new agent and associated mailbox with the exchange.

        Args:
            agent: Agent type of the agent.
            name: Optional display name for the agent.

        Returns:
            Agent registration info.
        """
        registration = await self._transport.register_agent(
            agent,
            name=name,
        )
        logger.info(
            'Registered %s in exchange',
            registration.agent_id,
            extra={'academy.agent_id': registration.agent_id},
        )
        return registration

    async def send(self, message: Message[Any]) -> None:
        """Send a message to a mailbox.

        Args:
            message: Message to send.

        Raises:
            BadEntityIdError: If a mailbox for `message.dest` does not exist.
            MailboxTerminatedError: If the mailbox was closed.
        """
        await self._transport.send(message)
        logger.debug(
            'Sent %s to %s',
            type(message.body).__name__,
            message.dest,
            extra=message.log_extra(),
        )

    async def status(self, uid: EntityId) -> MailboxStatus:
        """Check the status of a mailbox in the exchange.

        Args:
            uid: Entity identifier of the mailbox to check.
        """
        return await self._transport.status(uid)

    async def terminate(self, uid: EntityId) -> None:
        """Terminate a mailbox in the exchange.

        Terminating a mailbox means that the corresponding entity will no
        longer be able to receive messages.

        Note:
            This method is a no-op if the mailbox does not exist.

        Args:
            uid: Entity identifier of the mailbox to close.
        """
        await self._transport.terminate(uid)

    async def _listen_for_messages(self) -> None:
        while True:
            try:
                message = await self._transport.recv()
            except (asyncio.CancelledError, MailboxTerminatedError):
                break
            logger.debug(
                'Received %s from %s for %s',
                type(message.body).__name__,
                message.src,
                self.client_id,
                extra=message.log_extra(),
            )
            await self._handle_message(message)

    @abc.abstractmethod
    async def _handle_message(self, message: Message[Any]) -> None: ...


class AgentExchangeClient(
    ExchangeClient[ExchangeTransportT],
    Generic[AgentT, ExchangeTransportT],
):
    """Agent exchange client.

    Warning:
        Agent exchange clients should only be created via
        [`ExchangeFactory.create_agent_client()`][academy.exchange.ExchangeFactory.create_agent_client]!

    Args:
        agent_id: Agent ID.
        transport: Exchange transport bound to `agent_id`.
        request_handler: Request handler of the agent that will be called
            for each message received to this agent's mailbox.
            start_listener: Start a message listener thread.
    """

    def __init__(
        self,
        agent_id: AgentId[AgentT],
        transport: ExchangeTransportT,
        request_handler: RequestHandler[RequestT_co],
    ) -> None:
        super().__init__(transport)
        self._agent_id = agent_id
        self._request_handler = request_handler

    @property
    def client_id(self) -> AgentId[AgentT]:
        """Agent ID of the client."""
        return self._agent_id

    async def close(self) -> None:
        """Close the user client.

        This closes the underlying exchange transport and all handles created
        by this client. The agent's mailbox will not be terminated so the agent
        can be started again later.
        """
        async with self._close_lock:
            if self._closed:
                return

            await self._transport.close()
            self._closed = True
            logger.info(
                'Closed exchange client for %s',
                self.client_id,
                extra={'academy.mailbox_id': self.client_id},
            )

    async def _handle_message(self, message: Message[Any]) -> None:
        if message.is_request():
            await self._request_handler(message)
        elif message.is_response():
            if message.label is None or message.label not in self._handles:
                logger.warning(
                    'Exchange client for %s received an unexpected response '
                    'message from %s but no corresponding handle exists.',
                    self.client_id,
                    message.src,
                    extra=message.log_extra(),
                )
                return
            handle = self._handles[message.label]
            await handle._process_response(message)
        else:
            raise AssertionError('Unreachable.')


class UserExchangeClient(ExchangeClient[ExchangeTransportT]):
    """User exchange client.

    Warning:
        User exchange clients should only be created via
        [`ExchangeFactory.create_user_client()`][academy.exchange.ExchangeFactory.create_user_client]!

    Args:
        user_id: User ID.
        transport: Exchange transport bound to `user_id`.
        start_listener: Start a message listener thread.
    """

    def __init__(
        self,
        user_id: UserId,
        transport: ExchangeTransportT,
        *,
        start_listener: bool = True,
    ) -> None:
        super().__init__(transport)
        self._user_id = user_id
        self._listener_task: asyncio.Task[None] | None = None
        if start_listener:
            self._listener_task = spawn_guarded_background_task(
                self._listen_for_messages(),
                name=f'user-exchange-listener-{self.client_id}',
            )

    @property
    def client_id(self) -> UserId:
        """User ID of the client."""
        return self._user_id

    async def close(self) -> None:
        """Close the user client.

        This terminates the user's mailbox, closes the underlying exchange
        transport.
        """
        async with self._close_lock:
            if self._closed:
                return

            await self._transport.terminate(self.client_id)
            logger.info(
                f'Terminated mailbox for {self.client_id}',
                extra={'academy.mailbox_id': self.client_id},
            )
            await self._stop_listener_task()
            await self._transport.close()
            self._closed = True
            logger.info(
                'Closed exchange client for %s',
                self.client_id,
                extra={'academy.mailbox_id': self.client_id},
            )

    async def _handle_message(self, message: Message[Any]) -> None:
        if message.is_request():
            error = TypeError(f'{self.client_id} cannot fulfill requests.')
            response = message.create_response(ErrorResponse(exception=error))
            await self._transport.send(response)
            logger.warning(
                'Exchange client for %s received unexpected request message '
                'from %s',
                self.client_id,
                message.src,
                extra=message.log_extra(),
            )
        elif message.is_response():
            if message.label is None or message.label not in self._handles:
                logger.warning(
                    'Exchange client for %s received an unexpected response '
                    'message from %s but no corresponding handle exists.',
                    self.client_id,
                    message.src,
                    extra=message.log_extra(),
                )
                return
            handle = self._handles[message.label]
            await handle._process_response(message)
        else:
            raise AssertionError('Unreachable.')

    async def _stop_listener_task(self) -> None:
        if self._listener_task is not None:
            self._listener_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._listener_task
            self._listener_task = None
