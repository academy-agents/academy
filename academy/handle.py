from __future__ import annotations

import asyncio
import contextlib
import functools
import logging
import time
import uuid
from pickle import PicklingError
from typing import Any
from typing import Generic
from typing import ParamSpec
from typing import TYPE_CHECKING
from typing import TypeVar
from weakref import WeakSet

from academy.exception import AgentInactiveError
from academy.exception import AgentTerminatedError
from academy.exception import ExchangeClientNotFoundError
from academy.exchange.client import exchange_context
from academy.exchange.mailbox_status import MailboxStatus
from academy.identifier import AgentId
from academy.message import ActionRequest
from academy.message import ActionResponse
from academy.message import CancelRequest
from academy.message import ErrorResponse
from academy.message import Message
from academy.message import PingRequest
from academy.message import Response
from academy.message import ResponseT
from academy.message import ShutdownRequest
from academy.serialize import default_serializer
from academy.serialize import SerializationStrategy
from academy.stats import AgentStats

if TYPE_CHECKING:
    from academy.agent import Agent
    from academy.agent import AgentT
    from academy.exchange import ExchangeClient

    AgentT_co = TypeVar('AgentT_co', bound=Agent, covariant=True)
else:
    # Agent is only used in the bounding of the AgentT TypeVar.
    AgentT_co = TypeVar('AgentT_co', covariant=True)
    from academy.identifier import AgentT

logger = logging.getLogger(__name__)

K = TypeVar('K')
P = ParamSpec('P')
R = TypeVar('R')


class Handle(Generic[AgentT_co]):
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
        agent_id: ID of the remote agent, or ``None`` to defer
            binding (used by ``batch.queue()``).
        exchange: A default exchange client to be used if an exchange client
            is not configured in the current context.
        ignore_context: Ignore the current context and force use of `exchange`
            for communication.
        request_serializer: Strategy used to serialize arguments. If None,
            use the
            [`academy.serialize.default_serializer`][academy.serialize.default_serializer]
        result_serializer: Strategy used to serialize results. If false-y, use
            the same strategy as the request serializer.
        exception_serializer: Strategy used to serialize results. If false-y,
            use the same strategy as the result serializer.
        polling_interval: Interval to poll exchange to see if agent is active.
        reject_on_inactive: Return an error when target agent is inactive.

    Raises:
        ValueError: If `ignore_context=True` but `exchange` is not provided.
    """

    def __init__(  # noqa: PLR0913
        self,
        agent_id: AgentId[AgentT_co] | None = None,
        *,
        exchange: ExchangeClient[Any] | None = None,
        ignore_context: bool = False,
        request_serializer: SerializationStrategy | None = None,
        result_serializer: SerializationStrategy | None = None,
        exception_serializer: SerializationStrategy | None = None,
        polling_interval: float = 60,
        reject_on_inactive: bool = False,
    ) -> None:
        self._agent_id: AgentId[AgentT_co] | None = agent_id
        self._exchange = exchange
        self.request_serializer = request_serializer
        self.result_serializer = result_serializer
        self.exception_serializer = exception_serializer
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
        self._pending_actions: set[uuid.UUID] = set()
        self._pending_actions_lock = asyncio.Lock()

        if self._exchange is not None:
            self._register_with_exchange(self._exchange)

        # _agent_status is the private variable to keep track of the mailbox
        # status in the polling loop --- it is not always current (based on
        # when the last poll was), and is only updated if reject_on_inactive
        # is true. For live mailbox status use `agent_status`
        self._agent_status: MailboxStatus | None = None
        self.polling_interval = polling_interval
        self._reject_on_inactive: bool = False
        self.reject_on_inactive = reject_on_inactive

    @property
    def agent_id(self) -> AgentId[AgentT_co]:
        """ID of the remote agent.

        Raises:
            RuntimeError: If the handle is not bound.
        """
        if self._agent_id is None:
            raise RuntimeError(
                'Handle is not bound to a registered agent. '
                'Submit the enclosing batch before reading '
                'agent_id.',
            )
        return self._agent_id

    @agent_id.setter
    def agent_id(self, value: AgentId[AgentT_co]) -> None:
        assert self._agent_id is None, 'Handle is already bound.'
        self._agent_id = value

    @property
    def reject_on_inactive(self) -> bool:
        """Reject actions when agent is inactive."""
        return self._reject_on_inactive

    @reject_on_inactive.setter
    def reject_on_inactive(self, value: bool) -> None:
        old = self._reject_on_inactive
        if not old and value:
            # Start loop if needed
            self._agent_status_set = asyncio.Event()
            self._reject_on_inactive = value  # Set after event to avoid race
            self._status_task: asyncio.Task[None] = asyncio.create_task(
                self._status_loop(),
                name=f'{self.handle_id}-status-loop',
            )
        elif old and not value:
            # Stop loop if not needed
            self._reject_on_inactive = value  # Set before cancel to avoid race
            self._status_task.cancel()
            self._agent_status = None

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
        dict[str, Any],
    ]:
        if self.ignore_context:
            raise PicklingError(
                'Handle with ignore_context=True is not pickle-able',
            )

        if self._agent_id is None:
            raise PicklingError('Cannot pickle an unbound handle.')

        return (
            Handle,
            (self._agent_id,),
            {
                'request_serializer': self.request_serializer,
                'result_serializer': self.result_serializer,
                'exception_serializer': self.exception_serializer,
                'polling_interval': self.polling_interval,
                'reject_on_inactive': self.reject_on_inactive,
            },
        )

    def __setstate__(self, state: dict[str, Any]) -> None:
        """Set state.

        This is necessary for unpickling to not treat set state as a
        remote action.
        """
        reject_on_inactive: bool = state.pop('reject_on_inactive', False)
        self.__dict__.update(state)
        self.reject_on_inactive = reject_on_inactive

    def __repr__(self) -> str:
        return (
            f'{type(self).__name__}(agent_id={self._agent_id!r}, '
            f'exchange={self._exchange!r}, '
            f'ignore_context={self.ignore_context!r})'
        )

    def __str__(self) -> str:
        name = type(self).__name__
        return f'{name}<agent: {self._agent_id}>'

    def __getattr__(self, name: str) -> Any:
        async def remote_method_call(*args: Any, **kwargs: Any) -> R:
            return await self.action(name, *args, **kwargs)

        return remote_method_call

    async def agent_stats(self) -> AgentStats:
        """Return live exchange-level metrics for the remote agent."""
        return await self.exchange.agent_stats(self.agent_id)

    async def agent_status(self) -> MailboxStatus:
        """Return live status of the agent mailbox."""
        status = await self.exchange.status(self.agent_id)
        if self._reject_on_inactive:
            # To avoid a situation where agent_status shows returns
            # active, and actions are rejected with a stale status
            await self._update_agent_status(status)

        return status

    async def _update_agent_status(self, status: MailboxStatus) -> None:
        """Update _agent_status, notifying pending actions if needed."""
        old_status = self._agent_status
        self._agent_status = status
        self._agent_status_set.set()
        if (
            self._agent_status == MailboxStatus.INACTIVE
            and self._agent_status != old_status
        ):
            await self._notify_pending_actions()

    async def _status_loop(self) -> None:
        """Poll exchange for agent status."""
        while True:
            if self._agent_id is not None:
                with contextlib.suppress(ExchangeClientNotFoundError):
                    status = await self.exchange.status(self.agent_id)
                    await self._update_agent_status(status)

            await asyncio.sleep(self.polling_interval)

    async def _notify_pending_actions(self) -> None:
        """Notify pending actions when agent becomes inactive."""
        loop = asyncio.get_event_loop()

        # Typically self._agent_status == INACTIVE should act as a lock, but
        # while we are rejecting actions, the agent could become active
        # and new actions could be added to the pending_actions set.
        async with self._pending_actions_lock:
            for request_tag in self._pending_actions:
                future = self._pending_response_futures[request_tag]
                # If agents toggle back and forth between ACTIVE and INACTIVE
                # the future might already be cancelled.
                if future.done():
                    continue

                cancel_request = Message.create(
                    src=self.exchange.client_id,
                    dest=self.agent_id,
                    label=self.handle_id,
                    body=CancelRequest(target_tag=request_tag),
                )
                logger.debug(
                    'Cancelling action tag id %s',
                    request_tag,
                    extra=cancel_request.log_extra()
                    | {
                        'academy.action_state': 'cancelled',
                    },
                )
                cancel_future: asyncio.Future[None] = loop.create_future()
                self._pending_response_futures[cancel_request.tag] = (
                    cancel_future
                )
                await self.exchange.send(cancel_request)
                future.set_exception(AgentInactiveError(self.agent_id))

    async def _check_status(self) -> None:
        """Check if agent is inactive."""
        if (
            self._status_task.done() and self._status_task.exception()
        ):  # pragma: no cover
            raise RuntimeError(
                'Error polling status of agent',
            ) from self._status_task.exception()

        await self._agent_status_set.wait()
        if self._agent_status == MailboxStatus.INACTIVE:
            raise AgentInactiveError(self.agent_id)

    async def _process_response(self, response: Message[ResponseT]) -> None:
        future = self._pending_response_futures.pop(response.tag)
        async with self._pending_actions_lock:
            self._pending_actions.discard(response.tag)
        if not future.done():
            future.set_result(response)

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
            AgentInactiveError: If the agent is inactive (missed heartbeats)
                and handle is configured to reject actions when agent is
                inactive (reject_on_inactive=True)
            Exception: Any exception raised by the action.
        """
        tag_id = uuid.uuid4()
        invocation_extra = {
            'academy.action': action,
            'academy.action_tag': tag_id,
        }

        logger.debug(
            'Invoking action %s with tag id %s',
            action,
            tag_id,
            extra=invocation_extra
            | {
                'academy.action_state': 'start',
                'academy.action_args': args,
                'academy.action_kwargs': kwargs,
                'academy.agent_id': self.agent_id,
            },
        )
        if self.reject_on_inactive:
            await self._check_status()

        exchange = self.exchange
        self._register_with_exchange(exchange)
        serialization = self.request_serializer or default_serializer.get()

        request = Message.create(
            src=exchange.client_id,
            dest=self.agent_id,
            label=self.handle_id,
            tag=tag_id,
            body=ActionRequest(
                action=action,
                pargs=args,
                kargs=kwargs,
                serialization=serialization,
                result_serialization=self.result_serializer,
                exception_serialization=self.exception_serializer,
            ),
        )
        loop = asyncio.get_running_loop()
        future: asyncio.Future[Message[Response]] = loop.create_future()
        self._pending_response_futures[request.tag] = future
        async with self._pending_actions_lock:
            self._pending_actions.add(request.tag)

        try:
            logger.debug(
                'Sending action request from %s to %s (action=%r)',
                exchange.client_id,
                self.agent_id,
                action,
                extra=request.log_extra()
                | invocation_extra
                | {'academy.action_state': 'sending'},
            )
            await self.exchange.send(request)
            logger.debug(
                'Waiting for result of action %s with tag id %s',
                action,
                tag_id,
                extra=invocation_extra
                | {
                    'academy.action_state': 'waiting',
                },
            )
            await future
        except asyncio.CancelledError:
            cancel_request = Message.create(
                src=exchange.client_id,
                dest=self.agent_id,
                label=self.handle_id,
                body=CancelRequest(target_tag=request.tag),
            )
            logger.debug(
                'Cancelling action %s with tag id %s',
                action,
                tag_id,
                extra=cancel_request.log_extra()
                | invocation_extra
                | {
                    'academy.action_state': 'cancelled',
                },
            )
            cancel_future: asyncio.Future[None] = loop.create_future()
            self._pending_response_futures[cancel_request.tag] = cancel_future
            await self.exchange.send(cancel_request)
            raise

        assert future.done()
        assert future.exception() is None
        message = future.result()
        body = message.get_body()

        if isinstance(body, ActionResponse):
            result = body.get_result()
            logger.debug(
                'Successfully completed action %s with tag id %s',
                action,
                tag_id,
                extra=invocation_extra
                | {
                    'academy.action_state': 'success',
                    'academy.result': result,
                    'academy.agent_id': self.agent_id,
                },
            )
            return result
        elif isinstance(body, ErrorResponse):
            exception = body.get_exception()
            logger.debug(
                'Completed action %s with tag id %s with exception',
                action,
                tag_id,
                extra=invocation_extra
                | {
                    'academy.action_state': 'exception',
                },
            )
            raise exception

        raise RuntimeError(
            'Invalid response received from action request.',
        )  # pragma: no cover

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
        future: asyncio.Future[Message[Response]] = loop.create_future()
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

        assert future.done()
        message = future.result()
        body = message.get_body()

        if isinstance(body, ErrorResponse):
            raise body.get_exception()

        if self.reject_on_inactive:
            # A successful ping indicates active regardless of
            # heartbeat, so we can use ping to wait for an agent
            # to become active.
            await self._update_agent_status(MailboxStatus.ACTIVE)

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

    def _shutdown_callback(
        self,
        future: asyncio.Future[Message[ResponseT]],
    ) -> None:
        exception: BaseException
        if future.exception() is not None:
            exception = future.exception()  # type: ignore[assignment]
        else:
            message = future.result()
            body = message.get_body()
            if not isinstance(body, ErrorResponse):
                return
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

        loop = asyncio.get_running_loop()
        future: asyncio.Future[Message[Response]] = loop.create_future()
        self._pending_response_futures[request.tag] = future
        await self.exchange.send(request)

        logger.debug(
            'Sent shutdown request from %s to %s',
            exchange.client_id,
            self.agent_id,
            extra=request.log_extra(),
        )

        future.add_done_callback(self._shutdown_callback)


class ProxyHandle(Handle[AgentT]):
    """Proxy handle.

    A proxy handle is thin wrapper around an
    [`Agent`][academy.agent.Agent] instance that is useful for testing
    agents that are initialized with a handle to another agent without
    needing to spawn agents. This wrapper invokes actions synchronously.
    """

    def __init__(self, agent: AgentT) -> None:
        self.agent = agent
        self._agent_id: AgentId[AgentT] = AgentId.new()
        self._agent_closed = False
        self._reject_on_inactive = False

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
        dict[str, Any],
    ]:
        return (ProxyHandle, (self.agent,), {})

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
