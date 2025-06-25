from __future__ import annotations

import asyncio
import functools
import logging
import sys
import time
import uuid
from collections.abc import Iterable
from collections.abc import Mapping
from types import TracebackType
from typing import Any
from typing import Generic
from typing import Protocol
from typing import TYPE_CHECKING
from typing import TypeVar

if sys.version_info >= (3, 10):  # pragma: >=3.10 cover
    from typing import ParamSpec
else:  # pragma: <3.10 cover
    from typing_extensions import ParamSpec

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from academy.exception import HandleClosedError
from academy.exception import HandleNotBoundError
from academy.exception import MailboxClosedError
from academy.identifier import AgentId
from academy.identifier import EntityId
from academy.identifier import UserId
from academy.message import ActionRequest
from academy.message import ActionResponse
from academy.message import PingRequest
from academy.message import PingResponse
from academy.message import ResponseMessage
from academy.message import ShutdownRequest
from academy.message import ShutdownResponse

if TYPE_CHECKING:
    from academy.behavior import BehaviorT
    from academy.exchange import ExchangeClient
else:
    # Behavior is only used in the bounding of the BehaviorT TypeVar.
    BehaviorT = TypeVar('BehaviorT')

logger = logging.getLogger(__name__)

K = TypeVar('K')
P = ParamSpec('P')
R = TypeVar('R')


class Handle(Protocol[BehaviorT]):
    """Agent handle protocol.

    A handle enables an agent or user to invoke actions on another agent.
    """

    agent_id: AgentId[BehaviorT]
    client_id: EntityId

    def __getattr__(self, name: str) -> Any:
        # This dummy method definition is required to signal to mypy that
        # any attribute access is "valid" on a Handle type. This forces
        # mypy into calling our mypy plugin (academy.mypy_plugin) which then
        # validates the exact semantics of the attribute access depending
        # on the concrete type for the BehaviorT that Handle is generic on.
        ...

    async def action(
        self,
        action: str,
        /,
        *args: Any,
        **kwargs: Any,
    ) -> asyncio.Future[R]:
        """Invoke an action on the agent.

        Args:
            action: Action to invoke.
            args: Positional arguments for the action.
            kwargs: Keywords arguments for the action.

        Returns:
            Future to the result of the action.

        Raises:
            HandleClosedError: If the handle was closed.
            MailboxClosedError: If the agent's mailbox was closed. This
                typically indicates the agent shutdown for another reason
                (it self terminated or via another handle).
        """
        ...

    async def close(
        self,
        wait_futures: bool = True,
        *,
        timeout: float | None = None,
    ) -> None:
        """Close this handle.

        Args:
            wait_futures: Wait to return until all pending futures are done
                executing. If `False`, pending futures are cancelled.
            timeout: Optional timeout used when `wait=True`.
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
            HandleClosedError: If the handle was closed.
            MailboxClosedError: If the agent's mailbox was closed. This
                typically indicates the agent shutdown for another reason
                (it self terminated or via another handle).
            TimeoutError: If the timeout is exceeded.
        """
        ...

    async def shutdown(self) -> None:
        """Instruct the agent to shutdown.

        This is non-blocking and will only send the message.

        Raises:
            HandleClosedError: If the handle was closed.
            MailboxClosedError: If the agent's mailbox was closed. This
                typically indicates the agent shutdown for another reason
                (it self terminated or via another handle).
        """
        ...


class HandleDict(dict[K, Handle[BehaviorT]]):
    """Dictionary mapping keys to handles.

    Tip:
        The `HandleDict` is required when storing a mapping of handles as
        attributes of a `Behavior` so that those handles get bound to the
        correct agent when running.
    """

    def __init__(
        self,
        values: Mapping[K, Handle[BehaviorT]]
        | Iterable[tuple[K, Handle[BehaviorT]]] = (),
        /,
        **kwargs: dict[str, Handle[BehaviorT]],
    ) -> None:
        super().__init__(values, **kwargs)


class HandleList(list[Handle[BehaviorT]]):
    """List of handles.

    Tip:
        The `HandleList` is required when storing a list of handles as
        attributes of a `Behavior` so that those handles get bound to the
        correct agent when running.
    """

    def __init__(
        self,
        iterable: Iterable[Handle[BehaviorT]] = (),
        /,
    ) -> None:
        super().__init__(iterable)


class ProxyHandle(Generic[BehaviorT]):
    """Proxy handle.

    A proxy handle is thin wrapper around a
    [`Behavior`][academy.behavior.Behavior] instance that is useful for testing
    behaviors that are initialized with a handle to another agent without
    needing to spawn agents. This wrapper invokes actions synchronously.
    """

    def __init__(self, behavior: BehaviorT) -> None:
        self.behavior = behavior
        self.agent_id: AgentId[BehaviorT] = AgentId.new()
        self.client_id: EntityId = UserId.new()
        self._agent_closed = False
        self._handle_closed = False

    def __repr__(self) -> str:
        return f'{type(self).__name__}(behavior={self.behavior!r})'

    def __str__(self) -> str:
        return f'{type(self).__name__}<{self.behavior}>'

    def __getattr__(self, name: str) -> Any:
        method = getattr(self.behavior, name)
        if not callable(method):
            raise AttributeError(
                f'Attribute {name} of {type(self.behavior)} is not a method.',
            )

        @functools.wraps(method)
        async def func(*args: Any, **kwargs: Any) -> asyncio.Future[R]:
            return await self.action(name, *args, **kwargs)

        return func

    async def action(
        self,
        action: str,
        /,
        *args: Any,
        **kwargs: Any,
    ) -> asyncio.Future[R]:
        """Invoke an action on the agent.

        Args:
            action: Action to invoke.
            args: Positional arguments for the action.
            kwargs: Keywords arguments for the action.

        Returns:
            Future to the result of the action.

        Raises:
            HandleClosedError: If the handle was closed.
            MailboxClosedError: If the agent's mailbox was closed. This
                typically indicates the agent shutdown for another reason
                (it self terminated or via another handle).
        """
        if self._agent_closed:
            raise MailboxClosedError(self.agent_id)
        elif self._handle_closed:
            raise HandleClosedError(self.agent_id, self.client_id)

        future: asyncio.Future[R] = asyncio.get_running_loop().create_future()
        try:
            method = getattr(self.behavior, action)
            result = await method(*args, **kwargs)
        except Exception as e:
            future.set_exception(e)
        else:
            future.set_result(result)
        return future

    async def close(
        self,
        wait_futures: bool = True,
        *,
        timeout: float | None = None,
    ) -> None:
        """Close this handle.

        Note:
            This is a no-op for proxy handles.

        Args:
            wait_futures: Wait to return until all pending futures are done
                executing. If `False`, pending futures are cancelled.
            timeout: Optional timeout used when `wait=True`.
        """
        self._handle_closed = True

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
            HandleClosedError: If the handle was closed.
            MailboxClosedError: If the agent's mailbox was closed. This
                typically indicates the agent shutdown for another reason
                (it self terminated or via another handle).
            TimeoutError: If the timeout is exceeded.
        """
        if self._agent_closed:
            raise MailboxClosedError(self.agent_id)
        elif self._handle_closed:
            raise HandleClosedError(self.agent_id, self.client_id)
        return 0

    async def shutdown(self) -> None:
        """Instruct the agent to shutdown.

        This is non-blocking and will only send the message.

        Raises:
            HandleClosedError: If the handle was closed.
            MailboxClosedError: If the agent's mailbox was closed. This
                typically indicates the agent shutdown for another reason
                (it self terminated or via another handle).
        """
        if self._agent_closed:
            raise MailboxClosedError(self.agent_id)
        elif self._handle_closed:
            raise HandleClosedError(self.agent_id, self.client_id)
        self._agent_closed = True


class UnboundRemoteHandle(Generic[BehaviorT]):
    """Handle to a remote agent that not bound to a mailbox.

    Warning:
        An unbound handle must be bound before use. Otherwise all methods
        will raise an `HandleNotBoundError` when attempting to send a message
        to the remote agent.

    Args:
        agent_id: EntityId of the agent.
    """

    def __init__(self, agent_id: AgentId[BehaviorT]) -> None:
        self.agent_id = agent_id

    def __repr__(self) -> str:
        name = type(self).__name__
        return f'{name}(agent_id={self.agent_id!r})'

    def __str__(self) -> str:
        return f'{type(self).__name__}<agent: {self.agent_id}>'

    def __getattr__(self, name: str) -> Any:
        raise AttributeError(
            'Actions cannot be invoked via an unbound handle.',
        )

    @property
    def client_id(self) -> EntityId:
        """Raises [`RuntimeError`][RuntimeError] when unbound."""
        raise RuntimeError('An unbound handle has no client ID.')

    async def bind_to_exchange(
        self,
        exchange: ExchangeClient[Any],
    ) -> RemoteHandle[BehaviorT]:
        """Bind the handle to an existing mailbox.

        Args:
            exchange: Client exchange to associate with handle

        Returns:
            Remote handle bound to the identifier.
        """
        return await exchange.get_handle(self.agent_id)

    async def action(
        self,
        action: str,
        /,
        *args: Any,
        **kwargs: Any,
    ) -> asyncio.Future[R]:
        """Raises [`HandleNotBoundError`][academy.exception.HandleNotBoundError]."""  # noqa: E501
        raise HandleNotBoundError(self.agent_id)

    async def close(self) -> None:
        """Raises [`HandleNotBoundError`][academy.exception.HandleNotBoundError]."""  # noqa: E501
        raise HandleNotBoundError(self.agent_id)

    async def ping(self, *, timeout: float | None = None) -> float:
        """Raises [`HandleNotBoundError`][academy.exception.HandleNotBoundError]."""  # noqa: E501
        raise HandleNotBoundError(self.agent_id)

    async def shutdown(self) -> None:
        """Raises [`HandleNotBoundError`][academy.exception.HandleNotBoundError]."""  # noqa: E501
        raise HandleNotBoundError(self.agent_id)


class RemoteHandle(Generic[BehaviorT]):
    """Handle to a remote agent bound to an exchange client.

    Args:
        exchange: Exchange client used for agent communication.
        agent_id: EntityId of the target agent of this handle.
    """

    def __init__(
        self,
        exchange: ExchangeClient[Any],
        agent_id: AgentId[BehaviorT],
    ) -> None:
        self.exchange = exchange
        self.agent_id = agent_id
        self.client_id = exchange.client_id

        if self.agent_id == self.client_id:
            raise ValueError(
                'Cannot create handle to self. The IDs of the exchange '
                f'client and the target agent are the same: {self.agent_id}.',
            )
        # Unique identifier for each handle object; used to disambiguate
        # messages when multiple handles are bound to the same mailbox.
        self.handle_id = uuid.uuid4()

        self._futures: dict[uuid.UUID, asyncio.Future[Any]] = {}
        self._closed = False

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        await self.close()

    def __reduce__(
        self,
    ) -> tuple[
        type[UnboundRemoteHandle[Any]],
        tuple[AgentId[BehaviorT]],
    ]:
        return (UnboundRemoteHandle, (self.agent_id,))

    def __repr__(self) -> str:
        return (
            f'{type(self).__name__}(agent_id={self.agent_id!r}, '
            f'client_id={self.client_id!r}, exchange={self.exchange!r})'
        )

    def __str__(self) -> str:
        name = type(self).__name__
        return f'{name}<agent: {self.agent_id}; mailbox: {self.client_id}>'

    def __getattr__(self, name: str) -> Any:
        async def remote_method_call(
            *args: Any,
            **kwargs: Any,
        ) -> asyncio.Future[R]:
            return await self.action(name, *args, **kwargs)

        return remote_method_call

    async def bind_to_exchange(
        self,
        exchange: ExchangeClient[Any],
    ) -> RemoteHandle[BehaviorT]:
        """Bind the handle to an existing mailbox.

        Args:
            exchange: Client exchange to associate with handle

        Returns:
            Remote handle bound to the identifier.
        """
        unbound = self.clone()
        return await unbound.bind_to_exchange(exchange)

    def clone(self) -> UnboundRemoteHandle[BehaviorT]:
        """Create an unbound copy of this handle."""
        return UnboundRemoteHandle(self.agent_id)

    async def _process_response(self, response: ResponseMessage) -> None:
        if isinstance(response, (ActionResponse, PingResponse)):
            future = self._futures.pop(response.tag)
            if response.exception is not None:
                future.set_exception(response.exception)
            elif isinstance(response, ActionResponse):
                future.set_result(response.result)
            elif isinstance(response, PingResponse):
                future.set_result(None)
            else:
                raise AssertionError('Unreachable.')
        elif isinstance(response, ShutdownResponse):  # pragma: no cover
            # Shutdown responses are not implemented yet.
            pass
        else:
            raise AssertionError('Unreachable.')

    async def close(
        self,
        wait_futures: bool = True,
        *,
        timeout: float | None = None,
    ) -> None:
        """Close this handle.

        Note:
            This does not close the exchange client.

        Args:
            wait_futures: Wait to return until all pending futures are done
                executing. If `False`, pending futures are cancelled.
            timeout: Optional timeout used when `wait=True`.
        """
        self._closed = True

        if len(self._futures) == 0:
            return
        if wait_futures:
            logger.debug('Waiting on pending futures for %s', self)
            await asyncio.wait(
                list(self._futures.values()),
                timeout=timeout,
            )
        else:
            logger.debug('Cancelling pending futures for %s', self)
            for future in self._futures:
                self._futures[future].cancel()

    async def action(
        self,
        action: str,
        /,
        *args: Any,
        **kwargs: Any,
    ) -> asyncio.Future[R]:
        """Invoke an action on the agent.

        Args:
            action: Action to invoke.
            args: Positional arguments for the action.
            kwargs: Keywords arguments for the action.

        Returns:
            Future to the result of the action.

        Raises:
            HandleClosedError: If the handle was closed.
            MailboxClosedError: If the agent's mailbox was closed. This
                typically indicates the agent shutdown for another reason
                (it self terminated or via another handle).
        """
        if self._closed:
            raise HandleClosedError(self.agent_id, self.client_id)

        request = ActionRequest(
            src=self.client_id,
            dest=self.agent_id,
            label=self.handle_id,
            action=action,
            pargs=args,
            kargs=kwargs,
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
        return future

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
            HandleClosedError: If the handle was closed.
            MailboxClosedError: If the agent's mailbox was closed. This
                typically indicates the agent shutdown for another reason
                (it self terminated or via another handle).
            TimeoutError: If the timeout is exceeded.
        """
        if self._closed:
            raise HandleClosedError(self.agent_id, self.client_id)

        request = PingRequest(
            src=self.client_id,
            dest=self.agent_id,
            label=self.handle_id,
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
            self.client_id,
            self.agent_id,
            elapsed * 1000,
        )
        return elapsed

    async def shutdown(self) -> None:
        """Instruct the agent to shutdown.

        This is non-blocking and will only send the message.

        Raises:
            HandleClosedError: If the handle was closed.
            MailboxClosedError: If the agent's mailbox was closed. This
                typically indicates the agent shutdown for another reason
                (it self terminated or via another handle).
        """
        if self._closed:
            raise HandleClosedError(self.agent_id, self.client_id)

        request = ShutdownRequest(
            src=self.client_id,
            dest=self.agent_id,
            label=self.handle_id,
        )
        await self.exchange.send(request)
        logger.debug(
            'Sent shutdown request from %s to %s',
            self.client_id,
            self.agent_id,
        )
