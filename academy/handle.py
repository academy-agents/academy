from __future__ import annotations

import asyncio
import functools
import logging
import time
import uuid
from contextvars import ContextVar
from pickle import PicklingError
from typing import Any
from typing import Generic
from typing import ParamSpec
from typing import TYPE_CHECKING
from typing import TypeVar
from weakref import WeakSet

from academy.exception import AgentTerminatedError
from academy.exception import ExchangeClientNotFoundError
from academy.identifier import AgentId
from academy.message import ActionRequest
from academy.message import ActionResponse
from academy.message import ErrorResponse
from academy.message import Message
from academy.message import PingRequest
from academy.message import ResponseT
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

exchange_context: ContextVar[ExchangeClient[Any]] = ContextVar(
    'exchange_context',
)


class Handle(Generic[AgentT]):
    """Handle to a remote agent.

    Internally, handles use an
    [`ExchangeClient`][academy.exchange.ExchangeClient] to send requests to
    and receive responses from the remote agent. By default the correct
    exchange client is inferred from the context using a
    [context variable][contextvars] (specifically, the
    `academy.handle.exchange_context` variable). This allows the same handle
    to be used in different contexts, automatically using the correct client
    to send messages.

    When a handle is used in contexts that have not configured the exchange
    client (such as outside of an agent runtime or
    [`Manager`][academy.manager.Manager]), a default exchange can be provided
    via the `exchange` argument. For advanced usage, the `ignore_context` flag
    will cause the handle to only use the `exchange` argument no matter what
    the current context is.

    Note:
        The `exchange` argument will not be included when a handle is pickled.
        Thus, unpickled handles must be used in a context that configures
        an exchange client.

    Args:
        agent_id: ID of the remote agent.
        exchange: A default exchange client to be used if an exchange client
            is not configured in the current context.
        ignore_context: Ignore the current context and force use of `exchange`
            for communication.

    Raises:
        ValueError: If `ignore_context=True` but `exchange` is not provided.
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
        self._pending_response_futures: dict[
            uuid.UUID,
            asyncio.Future[Any],
        ] = {}
        self._shutdown_requests: set[uuid.UUID] = set()

        if self._exchange is not None:
            self._register_with_exchange(self._exchange)

    @property
    def exchange(self) -> ExchangeClient[Any]:
        """Exchange client used to send messages.

        Returns:
            Exchange client.

        Raises:
            ExchangeClientNotFoundError: If no exchange client is set in the
                current context nor was one provided to the handle.
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
        type[Handle[Any]],
        tuple[Any, ...],
    ]:
        if self.ignore_context:
            raise PicklingError(
                'Handle with ignore_context=True is not pickle-able',
            )
        return (Handle, (self.agent_id,))

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

    async def _process_response(self, response: Message[ResponseT]) -> None:
        # Check if this is an error response for a shutdown request as those
        # are handled differently than other requests types (action, ping)
        # which always expect a response.
        if response.tag in self._shutdown_requests:
            self._shutdown_requests.remove(response.tag)
            body = response.get_body()
            assert isinstance(body, ErrorResponse)
            exception = body.get_exception()
            # The only ok error to be ignored is if the agent we intended to
            # shutdown was already shutdown.
            if (
                not isinstance(exception, AgentTerminatedError)
                or exception.uid != self.agent_id
            ):
                logger.error(
                    'Failure requesting shutdown for %s: %s (type: %s)',
                    self.agent_id,
                    exception,
                    type(exception),
                    extra={
                        'academy.agent_id': self.agent_id,
                        'academy.exception': exception,
                        'academy.exception_type': type(exception),
                    },
                )
            return

        future = self._pending_response_futures.pop(response.tag)

        if not future.cancelled():
            body = response.get_body()
            if isinstance(body, ActionResponse):
                future.set_result(body.get_result())
            elif isinstance(body, ErrorResponse):
                future.set_exception(body.get_exception())
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
        self._pending_response_futures[request.tag] = future

        await self.exchange.send(request)
        logger.debug(
            'Sent action request from %s to %s (action=%r)',
            exchange.client_id,
            self.agent_id,
            action,
            extra=request.log_extra()
            | {
                'academy.action': action,
            },
        )
        await future
        return future.result()

    async def ping(self, *, timeout: float | None = None) -> float:
        """Ping the agent.

        Ping the agent and wait to get a response.

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
        self._pending_response_futures[request.tag] = future
        start = time.perf_counter()
        await self.exchange.send(request)
        logger.debug(
            'Sent ping from %s to %s',
            exchange.client_id,
            self.agent_id,
            extra=request.log_extra(),
        )

        await asyncio.wait_for(future, timeout)

        elapsed = time.perf_counter() - start
        logger.debug(
            'Received ping from %s to %s in %.1f ms',
            exchange.client_id,
            self.agent_id,
            elapsed * 1000,
            extra=request.log_extra()
            | {
                'academy.ping_time_s': elapsed,
            },
        )
        return elapsed

    async def shutdown(self, *, terminate: bool | None = None) -> None:
        """Instruct the agent to shutdown.

        This is non-blocking and will only send the request. Any unexpected
        error responses sent by the exchange will be logged.

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
        self._shutdown_requests.add(request.tag)
        await self.exchange.send(request)
        logger.debug(
            'Sent shutdown request from %s to %s',
            exchange.client_id,
            self.agent_id,
            extra=request.log_extra(),
        )


class ProxyHandle(Handle[AgentT]):
    """Proxy handle.

    A proxy handle is thin wrapper around an
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

    def __reduce__(
        self,
    ) -> tuple[
        type[Handle[Any]],
        tuple[Any, ...],
    ]:
        return (ProxyHandle, (self.agent,))

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
