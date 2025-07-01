from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import enum
import logging
from collections.abc import Awaitable
from typing import Any
from typing import Callable
from typing import Generic
from typing import TypeVar

from academy.behavior import BehaviorT
from academy.context import ActionContext
from academy.context import AgentContext
from academy.exception import BadEntityIdError
from academy.exception import MailboxClosedError
from academy.exception import raise_exceptions
from academy.exchange import AgentExchangeClient
from academy.exchange import ExchangeFactory
from academy.exchange.transport import AgentRegistrationT
from academy.exchange.transport import ExchangeTransportT
from academy.handle import Handle
from academy.handle import HandleDict
from academy.handle import HandleList
from academy.handle import ProxyHandle
from academy.handle import RemoteHandle
from academy.handle import UnboundRemoteHandle
from academy.identifier import EntityId
from academy.message import ActionRequest
from academy.message import PingRequest
from academy.message import RequestMessage
from academy.message import ResponseMessage
from academy.message import ShutdownRequest
from academy.serialize import NoPickleMixin

logger = logging.getLogger(__name__)

T = TypeVar('T')


class _AgentState(enum.Enum):
    INITIALIZED = 'initialized'
    STARTING = 'starting'
    RUNNING = 'running'
    TERMINTATING = 'terminating'
    SHUTDOWN = 'shutdown'


@dataclasses.dataclass(frozen=True)
class AgentRunConfig:
    """Agent run configuration.

    Attributes:
        max_action_concurrency: Maximum size of the thread pool used to
            concurrently execute action requests.
        shutdown_on_loop_error: Shutdown the agent if any loop raises an error.
        terminate_on_error: Terminate the agent by closing its mailbox
            permanently if the agent shuts down due to an error.
        terminate_on_success: Terminate the agent by closing its mailbox
            permanently if the agent shuts down without an error.
    """

    max_action_concurrency: int | None = None
    shutdown_on_loop_error: bool = True
    terminate_on_error: bool = True
    terminate_on_success: bool = True


class Agent(Generic[BehaviorT], NoPickleMixin):
    """Executable agent.

    An agent executes predefined [`Behavior`][academy.behavior.Behavior]. An
    agent can operate independently or as part of a broader multi-agent
    system.

    Note:
        An agent can only be run once. Calling
        [`run()`][academy.agent.Agent.run] multiple times will raise a
        [`RuntimeError`][RuntimeError].

    Note:
        If any `@loop` method raises an error, the agent will be signaled
        to shutdown if `shutdown_on_loop_error` is set in the `config`.

    Args:
        behavior: Behavior that the agent will exhibit.
        exchange_factory: Message exchange factory.
        registration: Agent registration info returned by the exchange.
        config: Agent execution parameters.
    """

    def __init__(
        self,
        behavior: BehaviorT,
        *,
        exchange_factory: ExchangeFactory[ExchangeTransportT],
        registration: AgentRegistrationT,
        config: AgentRunConfig | None = None,
    ) -> None:
        self.agent_id = registration.agent_id
        self.behavior = behavior
        self.factory = exchange_factory
        self.registration = registration
        self.config = config if config is not None else AgentRunConfig()

        self._actions = behavior.behavior_actions()
        self._loops = behavior.behavior_loops()

        self._started_event = asyncio.Event()
        self._shutdown_event = asyncio.Event()
        self._expected_shutdown = True
        self._behavior_startup_called = False

        self._action_tasks: dict[ActionRequest, asyncio.Task[None]] = {}
        self._loop_tasks: dict[str, asyncio.Task[None]] = {}
        self._loop_exceptions: list[tuple[str, Exception]] = []

        self._exchange_client: (
            AgentExchangeClient[BehaviorT, ExchangeTransportT] | None
        ) = None
        self._exchange_listener_task: asyncio.Task[None] | None = None

    def __repr__(self) -> str:
        name = type(self).__name__
        return f'{name}({self.behavior!r}, {self._exchange_client!r})'

    def __str__(self) -> str:
        name = type(self).__name__
        behavior = type(self.behavior).__name__
        return f'{name}<{behavior}; {self.agent_id}>'

    async def _send_response(self, response: ResponseMessage) -> None:
        assert self._exchange_client is not None
        try:
            await self._exchange_client.send(response)
        except (BadEntityIdError, MailboxClosedError):  # pragma: no cover
            logger.warning(
                'Failed to send response from %s to %s. '
                'This likely means the destination mailbox was '
                'removed from the exchange.',
                self.agent_id,
                response.dest,
            )

    async def _execute_action(self, request: ActionRequest) -> None:
        try:
            result = await self.action(
                request.action,
                request.src,
                args=request.pargs,
                kwargs=request.kargs,
            )
        except Exception as e:
            response = request.error(exception=e)
        else:
            response = request.response(result=result)
        await self._send_response(response)

    async def _execute_loop(
        self,
        name: str,
        method: Callable[[asyncio.Event], Awaitable[None]],
    ) -> None:
        try:
            await method(self._shutdown_event)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self._loop_exceptions.append((name, e))
            logger.exception(
                'Error in loop %r (signaling shutdown: %s)',
                name,
                self.config.shutdown_on_loop_error,
            )
            if self.config.shutdown_on_loop_error:
                self.signal_shutdown(expected=False)

    async def _request_handler(self, request: RequestMessage) -> None:
        if isinstance(request, ActionRequest):
            task = asyncio.create_task(
                self._execute_action(request),
                name=f'execute-action-{request.action}-{request.tag}',
            )
            self._action_tasks[request] = task
            task.add_done_callback(
                lambda _: self._action_tasks.pop(request),
            )
        elif isinstance(request, PingRequest):
            logger.info('Ping request received by %s', self.agent_id)
            await self._send_response(request.response())
        elif isinstance(request, ShutdownRequest):
            self.signal_shutdown()
        else:
            raise AssertionError('Unreachable.')

    async def action(
        self,
        action: str,
        source_id: EntityId,
        *,
        args: Any,
        kwargs: Any,
    ) -> Any:
        """Invoke an action of the agent's behavior.

        Args:
            action: Name of action to invoke.
            source_id: ID of the source that requested the action.
            args: Tuple of positional arguments.
            kwargs: Dictionary of keyword arguments.

        Returns:
            Result of the action.

        Raises:
            AttributeError: If an action with this name is not implemented by
                the agent's behavior.
        """
        logger.debug('Invoking "%s" action on %s', action, self.agent_id)
        if action not in self._actions:
            raise AttributeError(
                f'{self.behavior} does not have an action named "{action}".',
            )
        action_method = self._actions[action]
        if action_method._action_method_context:
            assert self._exchange_client is not None
            context = ActionContext(source_id, self._exchange_client)
            return await action_method(*args, context=context, **kwargs)
        else:
            return await action_method(*args, **kwargs)

    async def run(self) -> None:
        """Run the agent.

        Agent startup involves:
        1. Creates a new exchange client for the agent.
        1. Sets the runtime context on the behavior.
        1. Binds all handles of the behavior to this agent's exchange client.
        1. Calls [`Behavior.on_setup()`][academy.behavior.Behavior.on_setup].
        1. Starts a [`Task`][asyncio.Task] for all control loops defined on
           the behavior.
        1. Starts a [`Task`][asyncio.Task] to listen for messages in the
           agent's mailbox in the exchange.
        1. Waits for the agent to be shutdown, such as due to a failure in
           a control loop and a received shutdown message.

        Agent shutdown involves:
        1. Cancels the mailbox message listener so no new requests are
           received.
        1. Waits for any currently executing actions to complete.
        1. Cancels running control loop tasks.
        1. Calls
           [`Behavior.on_shutdown()`][academy.behavior.Behavior.on_shutdown].
        1. Terminates the agent's mailbox in the exchange if configured.
        1. Closes the exchange client.

        Raises:
            RuntimeError: If the agent has already been shutdown.
            Exception: Any exceptions raised during startup, shutdown, or
                inside of control loops.
        """
        try:
            await self._start()
        except:
            logger.exception('Agent startup failed (%r)', self)
            self.signal_shutdown(expected=False)
            await self._shutdown()
            raise

        try:
            await self._shutdown_event.wait()
        finally:
            await self._shutdown()

            # Raise loop exceptions so the caller of run() sees the errors,
            # even if the loop errors didn't cause the shutdown.
            raise_exceptions(
                (e for _, e in self._loop_exceptions),
                message='Caught failures in agent loops while shutting down.',
            )

    async def _start(self) -> None:
        if self._shutdown_event.is_set():
            raise RuntimeError('Agent has already been shutdown.')

        logger.debug(
            'Starting agent... (%s; %s)',
            self.agent_id,
            self.behavior,
        )

        self._exchange_client = await self.factory.create_agent_client(
            self.registration,
            request_handler=self._request_handler,
        )

        context = AgentContext(
            agent_id=self.agent_id,
            exchange_client=self._exchange_client,
            shutdown_event=self._shutdown_event,
        )
        self.behavior._agent_set_context(context)

        _bind_behavior_handles(self.behavior, self._exchange_client)
        await self.behavior.on_setup()
        self._behavior_startup_called = True

        for name, method in self._loops.items():
            task = asyncio.create_task(
                self._execute_loop(name, method),
                name=f'execute-loop-{name}-{self.agent_id}',
            )
            self._loop_tasks[name] = task

        self._exchange_listener_task = asyncio.create_task(
            self._exchange_client._listen_for_messages(),
            name=f'exchange-listener-{self.agent_id}',
        )

        self._started_event.set()
        logger.info('Running agent (%s; %s)', self.agent_id, self.behavior)

    async def _shutdown(self) -> None:
        assert self._shutdown_event.is_set()

        logger.debug(
            'Shutting down agent... (expected: %s; %s; %s)',
            self._expected_shutdown,
            self.agent_id,
            self.behavior,
        )

        # If _start() fails early, the listener task may not have started.
        if self._exchange_listener_task is not None:
            self._exchange_listener_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._exchange_listener_task

        # Wait for running actions to complete
        for task in tuple(self._action_tasks.values()):
            await task

        # Cancel running control loop tasks
        for task in self._loop_tasks.values():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

        if self._behavior_startup_called:
            # Don't call on_shutdown() if we never called on_setup()
            await self.behavior.on_shutdown()

        terminate_for_success = (
            self.config.terminate_on_success and self._expected_shutdown
        )
        terminate_for_error = (
            self.config.terminate_on_error and not self._expected_shutdown
        )
        if self._exchange_client is not None:
            if terminate_for_success or terminate_for_error:
                await self._exchange_client.terminate(self.agent_id)
            await self._exchange_client.close()

        logger.info('Shutdown agent (%s; %s)', self.agent_id, self.behavior)

    def signal_shutdown(self, expected: bool = True) -> None:
        """Signal that the agent should exit.

        If the agent has not started, this will cause the agent to immediately
        shutdown when next started. If the agent is shutdown, this has no
        effect.
        """
        self._expected_shutdown = expected
        self._shutdown_event.set()


def _bind_behavior_handles(
    behavior: BehaviorT,
    client: AgentExchangeClient[BehaviorT, Any],
) -> None:
    """Bind all handle instance attributes on a behavior.

    Warning:
        This mutates the behavior, replacing the attributes with new handles
        bound to the agent's exchange client.

    Args:
        behavior: The behavior to bind handles on.
        client: The agent's exchange client used to bind the handles.
    """

    def _bind(handle: Handle[BehaviorT]) -> Handle[BehaviorT]:
        if isinstance(handle, ProxyHandle):
            return handle
        if (
            isinstance(handle, RemoteHandle)
            and handle.client_id == client.client_id
        ):
            return handle

        assert isinstance(handle, (UnboundRemoteHandle, RemoteHandle))
        bound = client.get_handle(handle.agent_id)
        logger.debug(
            'Bound %s of %s to %s',
            handle,
            behavior,
            client.client_id,
        )
        return bound

    for attr, handles in behavior.behavior_handles().items():
        if isinstance(handles, HandleDict):
            bound_dict = HandleDict(
                {k: _bind(h) for k, h in handles.items()},
            )
            setattr(behavior, attr, bound_dict)
        elif isinstance(handles, HandleList):
            bound_list = HandleList([_bind(h) for h in handles])
            setattr(behavior, attr, bound_list)
        else:
            setattr(behavior, attr, _bind(handles))
