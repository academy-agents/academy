from __future__ import annotations

import asyncio
import functools
import logging
import sys
import time
import uuid
from contextvars import ContextVar
from pickle import PicklingError
from typing import Any
from typing import Generic
from typing import Protocol
from typing import runtime_checkable
from typing import TYPE_CHECKING
from typing import TypeVar
from weakref import WeakSet

if sys.version_info >= (3, 10):  # pragma: >=3.10 cover
    from typing import ParamSpec
else:  # pragma: <3.10 cover
    from typing_extensions import ParamSpec

from academy.exception import AgentTerminatedError
from academy.exception import ExchangeClientNotFoundError
from academy.identifier import AgentId
from academy.message import ActionRequest
from academy.message import ActionResponse
from academy.message import ErrorResponse
from academy.message import Message
from academy.message import PingRequest
from academy.message import Response
from academy.message import ShutdownRequest
from academy.message import SuccessResponse

if TYPE_CHECKING:
    from academy.agent import AgentT
    from academy.exchange import ExchangeClient
else:
    # Agent is only used in the bounding of the AgentT TypeVar.
    AgentT = TypeVar('AgentT')

logger = logging.getLogger(__name__)

K = TypeVar('K')
P = ParamSpec('P')
R = TypeVar('R')


@runtime_checkable
class Handle(Protocol[AgentT]):
    """Agent handle protocol.

    A handle enables an agent or user to invoke actions on another agent.
    """

    def __getattr__(self, name: str) -> Any:
        # This dummy method definition is required to signal to mypy that
        # any attribute access is "valid" on a Handle type. This forces
        # mypy into calling our mypy plugin (academy.mypy_plugin) which then
        # validates the exact semantics of the attribute access depending
        # on the concrete type for the AgentT that Handle is generic on.
        ...

    @property
    def agent_id(self) -> AgentId[AgentT]:
        """ID of the agent this is a handle to."""
        ...

    async def action(self, action: str, /, *args: Any, **kwargs: Any) -> R:
        """Invoke an action on the agent.

        Args:
            action: Action to invoke.
            args: Positional arguments for the action.
            kwargs: Keywords arguments for the action.

        Returns:
            Result of the action.

        Raises:
            AgentTerminatedError: If the agent's mailbox was closed. This
                typically indicates the agent shutdown for another reason
                (it self terminated or via another handle).
            Exception: Any exception raised by the action.
        """
        ...

    async def ping(self, *, timeout: float | None = None) -> float:
        """Ping the agent.

        Ping the agent and wait to get a response. Agents process messages
        in order so the round-trip time will include processing time of
        earlier messages in the queue.

        Args:
            timeout: Optional timeout in seconds to wait for the response.

        Returns:
            Round-trip time in seconds.

        Raises:
            AgentTerminatedError: If the agent's mailbox was closed. This
                typically indicates the agent shutdown for another reason
                (it self terminated or via another handle).
            TimeoutError: If the timeout is exceeded.
        """
        ...

    async def shutdown(self, *, terminate: bool | None = None) -> None:
        """Instruct the agent to shutdown.

        This is non-blocking and will only send the message.

        Args:
            terminate: Override the termination behavior of the agent defined
                in the [`RuntimeConfig`][academy.runtime.RuntimeConfig].

        Raises:
            AgentTerminatedError: If the agent's mailbox was closed. This
                typically indicates the agent shutdown for another reason
                (it self terminated or via another handle).
        """
        ...


class ProxyHandle(Generic[AgentT]):
    """Proxy handle.

    A proxy handle is thin wrapper around a
    [`Agent`][academy.agent.Agent] instance that is useful for testing
    agents that are initialized with a handle to another agent without
    needing to spawn agents. This wrapper invokes actions synchronously.
    """

    def __init__(self, agent: AgentT) -> None:
        self.agent = agent
        self.agent_id: AgentId[AgentT] = AgentId.new()
        self._agent_closed = False

    def __repr__(self) -> str:
        return f'{type(self).__name__}(agent={self.agent!r})'

    def __str__(self) -> str:
        return f'{type(self).__name__}<{self.agent}>'

    def __getattr__(self, name: str) -> Any:
        method = getattr(self.agent, name)
        if not callable(method):
            raise AttributeError(
                f'Attribute {name} of {type(self.agent)} is not a method.',
            )

        @functools.wraps(method)
        async def func(*args: Any, **kwargs: Any) -> R:
            return await self.action(name, *args, **kwargs)

        return func

    async def action(self, action: str, /, *args: Any, **kwargs: Any) -> R:
        """Invoke an action on the agent.

        Args:
            action: Action to invoke.
            args: Positional arguments for the action.
            kwargs: Keywords arguments for the action.

        Returns:
            Result of the action.

        Raises:
            AgentTerminatedError: If the agent's mailbox was closed. This
                typically indicates the agent shutdown for another reason
                (it self terminated or via another handle).
            Exception: Any exception raised by the action.
        """
        if self._agent_closed:
            raise AgentTerminatedError(self.agent_id)

        method = getattr(self.agent, action)
        return await method(*args, **kwargs)

    async def ping(self, *, timeout: float | None = None) -> float:
        """Ping the agent.

        Ping the agent and wait to get a response. Agents process messages
        in order so the round-trip time will include processing time of
        earlier messages in the queue.

        Note:
            This is a no-op for proxy handles and returns 0 latency.

        Args:
            timeout: Optional timeout in seconds to wait for the response.

        Returns:
            Round-trip time in seconds.

        Raises:
            AgentTerminatedError: If the agent's mailbox was closed. This
                typically indicates the agent shutdown for another reason
                (it self terminated or via another handle).
            TimeoutError: If the timeout is exceeded.
        """
        if self._agent_closed:
            raise AgentTerminatedError(self.agent_id)
        return 0

    async def shutdown(self, *, terminate: bool | None = None) -> None:
        """Instruct the agent to shutdown.

        This is non-blocking and will only send the message.

        Args:
            terminate: Override the termination behavior of the agent defined
                in the [`RuntimeConfig`][academy.runtime.RuntimeConfig].

        Raises:
            AgentTerminatedError: If the agent's mailbox was closed. This
                typically indicates the agent shutdown for another reason
                (it self terminated or via another handle).
        """
        if self._agent_closed:
            raise AgentTerminatedError(self.agent_id)
        self._agent_closed = True if terminate is None else terminate


exchange_context: ContextVar[ExchangeClient[Any]] = ContextVar(
    'exchange_context',
)


class RemoteHandle(Generic[AgentT]):
    """Handle to a remote agent.

    By default a remote handle uses the 'exchange_context' ContextVar
    as the exchange client to send messages. This allows the outgoing
    mailbox to change depending on the context in which the handle is
    used. The `exchange` argument is used as the default client when
    the ContextVar is not set. `ignore_context` will cause the handle
    to only use the `exchange` argument provided in the initializer no
    matter the context where the handle is used. This should only be for
    advanced usage.

    Args:
        agent_id: EntityId of the target agent of this handle.
        exchange: Exchange client to use to send messages.
        ignore_context: Ignore exchange client set in context.
    """

    def __init__(
        self,
        agent_id: AgentId[AgentT],
        *,
        exchange: ExchangeClient[Any] | None = None,
        ignore_context: bool = False,
    ) -> None:
        self.agent_id = agent_id
        self._exchange = exchange
        self._registered_exchanges: WeakSet[ExchangeClient[Any]] = WeakSet()
        self.ignore_context = ignore_context

        if ignore_context and not exchange:
            raise ValueError(
                'Cannot initialize handle with ignore_context=True '
                'and no explicit exchange.',
            )

        # Unique identifier for each handle object; used to disambiguate
        # messages when multiple handles are bound to the same mailbox.
        self.handle_id = uuid.uuid4()
        self._futures: dict[uuid.UUID, asyncio.Future[Any]] = {}

        if self._exchange is not None:
            self._register_with_exchange(self._exchange)

    @property
    def exchange(self) -> ExchangeClient[Any]:
        """Exchange client used to send messages.

        Returns:
            The ExchangeClient

        Raises:
            HandleNotBoundError: If the exchange client can't be found.

        """
        if self.ignore_context:
            assert self._exchange is not None
            return self._exchange

        try:
            return exchange_context.get()
        except LookupError as e:
            if self._exchange is not None:
                return self._exchange

            raise ExchangeClientNotFoundError(self.agent_id) from e

    def __reduce__(
        self,
    ) -> tuple[
        type[RemoteHandle[Any]],
        tuple[Any, ...],
    ]:
        if self.ignore_context:
            raise PicklingError(
                'Handle with ignore_context=True is not pickle-able',
            )
        return (RemoteHandle, (self.agent_id,))

    def __repr__(self) -> str:
        return (
            f'{type(self).__name__}(agent_id={self.agent_id!r}, '
            f'exchange={self._exchange!r}, '
            f'ignore_context={self.ignore_context!r})'
        )

    def __str__(self) -> str:
        name = type(self).__name__
        return f'{name}<agent: {self.agent_id}>'

    def __getattr__(self, name: str) -> Any:
        async def remote_method_call(*args: Any, **kwargs: Any) -> R:
            return await self.action(name, *args, **kwargs)

        return remote_method_call

    async def _process_response(self, response: Message[Response]) -> None:
        future = self._futures[response.tag]
        assert not future.cancelled()

        body = response.get_body()
        if isinstance(body, ActionResponse):
            future.set_result(body.get_result())
        elif isinstance(body, ErrorResponse):
            future.set_exception(body.exception)
        elif isinstance(body, SuccessResponse):
            future.set_result(None)
        else:
            raise AssertionError('Unreachable.')

    def _register_with_exchange(self, exchange: ExchangeClient[Any]) -> None:
        """Register to receive messages from exchange.

        Typically this will be called internally when sending a message.

        Args:
            exchange: Exchange client to listen to.
        """
        if exchange not in self._registered_exchanges:
            exchange.register_handle(self)
            self._registered_exchanges.add(exchange)

    async def action(self, action: str, /, *args: Any, **kwargs: Any) -> R:
        """Invoke an action on the agent.

        Args:
            action: Action to invoke.
            args: Positional arguments for the action.
            kwargs: Keywords arguments for the action.

        Returns:
            Result of the action.

        Raises:
            AgentTerminatedError: If the agent's mailbox was closed. This
                typically indicates the agent shutdown for another reason
                (it self terminated or via another handle).
            Exception: Any exception raised by the action.
        """
        exchange = self.exchange
        self._register_with_exchange(exchange)

        request = Message.create(
            src=exchange.client_id,
            dest=self.agent_id,
            label=self.handle_id,
            body=ActionRequest(action=action, pargs=args, kargs=kwargs),
        )
        loop = asyncio.get_running_loop()
        future: asyncio.Future[R] = loop.create_future()
        self._futures[request.tag] = future

        await self.exchange.send(request)
        logger.debug(
            'Sent action request from %s to %s (action=%r)',
            self.client_id,
            self.agent_id,
            action,
        )
        await future
        return future.result()

    async def ping(self, *, timeout: float | None = None) -> float:
        """Ping the agent.

        Ping the agent and wait to get a response. Agents process messages
        in order so the round-trip time will include processing time of
        earlier messages in the queue.

        Args:
            timeout: Optional timeout in seconds to wait for the response.

        Returns:
            Round-trip time in seconds.

        Raises:
            AgentTerminatedError: If the agent's mailbox was closed. This
                typically indicates the agent shutdown for another reason
                (it self terminated or via another handle).
            TimeoutError: If the timeout is exceeded.
        """
        exchange = self.exchange
        self._register_with_exchange(exchange)

        request = Message.create(
            src=exchange.client_id,
            dest=self.agent_id,
            label=self.handle_id,
            body=PingRequest(),
        )
        loop = asyncio.get_running_loop()
        future: asyncio.Future[None] = loop.create_future()
        self._futures[request.tag] = future
        start = time.perf_counter()
        await self.exchange.send(request)
        logger.debug('Sent ping from %s to %s', self.client_id, self.agent_id)

        done, pending = await asyncio.wait({future}, timeout=timeout)
        if future in pending:
            raise TimeoutError(
                f'Did not receive ping response within {timeout} seconds.',
            )
        elapsed = time.perf_counter() - start
        logger.debug(
            'Received ping from %s to %s in %.1f ms',
            exchange.client_id,
            self.agent_id,
            elapsed * 1000,
        )
        return elapsed

    async def shutdown(self, *, terminate: bool | None = None) -> None:
        """Instruct the agent to shutdown.

        This is non-blocking and will only send the message.

        Args:
            terminate: Override the termination behavior of the agent defined
                in the [`RuntimeConfig`][academy.runtime.RuntimeConfig].

        Raises:
            AgentTerminatedError: If the agent's mailbox was closed. This
                typically indicates the agent shutdown for another reason
                (it self terminated or via another handle).
        """
        exchange = self.exchange
        self._register_with_exchange(exchange)

        request = Message.create(
            src=exchange.client_id,
            dest=self.agent_id,
            label=self.handle_id,
            body=ShutdownRequest(terminate=terminate),
        )
        await self.exchange.send(request)
        logger.debug(
            'Sent shutdown request from %s to %s',
            exchange.client_id,
            self.agent_id,
        )
