from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import enum
import logging
import sys
import threading
from collections.abc import Awaitable
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from typing import Callable
from typing import Generic
from typing import TypeVar

from academy.behavior import BehaviorT
from academy.exception import BadEntityIdError
from academy.exception import MailboxClosedError
from academy.exchange import AgentExchangeClient
from academy.exchange import ExchangeFactory
from academy.exchange.transport import AgentRegistrationT
from academy.exchange.transport import ExchangeTransportT
from academy.handle import Handle
from academy.handle import ProxyHandle
from academy.handle import RemoteHandle
from academy.handle import UnboundRemoteHandle
from academy.message import ActionRequest
from academy.message import PingRequest
from academy.message import RequestMessage
from academy.message import ResponseMessage
from academy.message import ShutdownRequest

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


# Helper for Agent.__reduce__ which cannot handle the keyword arguments
# of the Agent constructor.
def _agent_trampoline(
    behavior: BehaviorT,
    exchange_factory: ExchangeFactory[ExchangeTransportT],
    registration: AgentRegistrationT,
    config: AgentRunConfig,
) -> Agent[AgentRegistrationT, BehaviorT]:
    return Agent(
        behavior,
        exchange_factory=exchange_factory,
        registration=registration,
        config=config,
    )


class Agent(Generic[AgentRegistrationT, BehaviorT]):
    """Executable agent.

    An agent executes predefined [`Behavior`][academy.behavior.Behavior]. An
    agent can operate independently or as part of a broader multi-agent
    system.

    Note:
        An agent can only be run once. After `shutdown()` is called, later
        operations will raise a `RuntimeError`.

    Note:
        If any `@loop` method raises an error, the agent will be signaled
        to shutdown if `shutdown_on_loop_error` is set in the `config`.

    Args:
        behavior: Behavior that the agent will exhibit.
        config: Agent execution parameters.
        exchange_factory: Message exchange factory.
        registration: Agent registration info returned by the exchange.
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
        self.exchange = exchange_factory
        self.registration = registration
        self.config = config if config is not None else AgentRunConfig()

        self._actions = behavior.behavior_actions()
        self._loops = behavior.behavior_loops()

        self._shutdown_agent: asyncio.Event | None = None
        self._shutdown_loop = threading.Event()
        self._expected_shutdown = False
        self._state_lock = asyncio.Lock()
        self._state = _AgentState.INITIALIZED

        self._action_pool: ThreadPoolExecutor | None = None
        self._action_tasks: dict[ActionRequest, asyncio.Task[None]] = {}
        self._loop_pool: ThreadPoolExecutor | None = None
        self._loop_tasks: dict[str, asyncio.Task[None]] = {}
        self._loop_exceptions: list[tuple[str, Exception]] = []

        self._exchange_client: (
            AgentExchangeClient[BehaviorT, ExchangeTransportT] | None
        ) = None
        self._exchange_listener_task: asyncio.Task[None] | None = None

    def __repr__(self) -> str:
        name = type(self).__name__
        return (
            f'{name}(agent_id={self.agent_id!r}, behavior={self.behavior!r}, '
            f'exchange={self.exchange!r})'
        )

    def __str__(self) -> str:
        name = type(self).__name__
        behavior = type(self.behavior).__name__
        return f'{name}<{behavior}; {self.agent_id}>'

    def __reduce__(self) -> Any:
        return (
            _agent_trampoline,
            (
                # The order of these must match the __init__ params!
                self.behavior,
                self.exchange,
                self.registration,
                self.config,
            ),
        )

    async def _bind_handles(self) -> None:
        async def _bind(handle: Handle[BehaviorT]) -> Handle[BehaviorT]:
            if isinstance(handle, ProxyHandle):  # pragma: no cover
                return handle
            if (
                isinstance(handle, RemoteHandle)
                and handle.client_id == self.agent_id
            ):
                return handle

            assert isinstance(handle, (UnboundRemoteHandle, RemoteHandle))
            assert self._exchange_client is not None
            bound = await handle.bind_to_exchange(self._exchange_client)
            logger.debug(
                'Bound handle to %s to running agent with %s',
                bound.agent_id,
                self.agent_id,
            )
            return bound

        await self.behavior.behavior_handles_bind(_bind)

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
        assert self._action_pool is not None
        try:
            result = await self.action(
                request.action,
                request.pargs,
                request.kargs,
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
        assert self._loop_pool is not None
        assert self._shutdown_agent is not None
        try:
            await method(self._shutdown_agent)
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

    async def action(self, action: str, args: Any, kwargs: Any) -> Any:
        """Invoke an action of the agent.

        Args:
            action: Name of action to invoke.
            args: Tuple of positional arguments.
            kwargs: Dictionary of keyword arguments.

        Returns:
            Result of the action.

        Raises:
            AttributeError: if an action with this name is not implemented by
                the behavior of the agent.
        """
        logger.debug('Invoking "%s" action on %s', action, self.agent_id)
        if action not in self._actions:
            raise AttributeError(
                f'Agent[{type(self.behavior).__name__}] does not have an '
                f'action named "{action}".',
            )
        return await self._actions[action](*args, **kwargs)

    async def run(self) -> None:
        """Run the agent.

        Starts the agent, waits for another thread to call `signal_shutdown()`,
        and then shuts down the agent.

        Raises:
            Exception: Any exceptions raised inside threads.
        """
        try:
            await self.start()
            assert self._shutdown_agent is not None
            await self._shutdown_agent.wait()
        except:
            logger.exception('Running agent %s failed!', self.agent_id)
            raise
        finally:
            await self.shutdown()

    async def start(self) -> None:
        """Start the agent.

        Note:
            This method is idempotent; it will return if the agent is
            already running. However, it will raise an error if the agent
            is shutdown.

        1. Binds all unbound handles to remote agents to this agent.
        1. Calls [`Behavior.on_setup()`][academy.behavior.Behavior.on_setup].
        1. Starts threads for all control loops defined on the agent's
           [`Behavior`][academy.behavior.Behavior].
        1. Starts a thread for listening to messages from the exchange.

        Raises:
            RuntimeError: If the agent has been shutdown.
        """
        async with self._state_lock:
            if self._state is _AgentState.SHUTDOWN:
                raise RuntimeError('Agent has already been shutdown.')
            elif self._state is _AgentState.RUNNING:
                return

            logger.debug(
                'Starting agent... (%s; %s)',
                self.agent_id,
                self.behavior,
            )
            if self._shutdown_agent is None:
                self._shutdown_agent = asyncio.Event()
            self._state = _AgentState.STARTING

            self._exchange_client = await self.exchange.create_agent_client(
                self.registration,
                request_handler=self._request_handler,
            )
            await self._bind_handles()
            await self.behavior.on_setup()
            self._action_pool = ThreadPoolExecutor(
                max_workers=self.config.max_action_concurrency,
            )

            if len(self._loops) > 0:
                self._loop_pool = ThreadPoolExecutor(
                    max_workers=len(self._loops),
                )
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

            self._state = _AgentState.RUNNING

            logger.info('Running agent (%s; %s)', self.agent_id, self.behavior)

    async def shutdown(self) -> None:
        """Shutdown the agent.

        Note:
            This method is idempotent.

        1. Sets the shutdown [`Event`][threading.Event] passed to all control
           loops.
        1. Waits for any currently executing actions to complete.
        1. Closes the agent's mailbox indicating that no further messages
           will be processed.
        1. Waits for the control loop and message listener threads to exit.
        1. Closes the exchange.
        1. Calls
           [`Behavior.on_shutdown()`][academy.behavior.Behavior.on_shutdown].

        Raises:
            Exception: Any exceptions raised inside threads.
        """
        async with self._state_lock:
            if self._state in (_AgentState.INITIALIZED, _AgentState.SHUTDOWN):
                return

            logger.debug(
                'Shutting down agent... (expected: %s; %s; %s)',
                self._expected_shutdown,
                self.agent_id,
                self.behavior,
            )
            self._state = _AgentState.TERMINTATING
            if self._shutdown_agent is not None:  # pragma: no branch
                self._shutdown_agent.set()
            self._shutdown_loop.set()

            if self._exchange_listener_task is not None:
                self._exchange_listener_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._exchange_listener_task

            # Wait for currently running actions to complete. No more
            # should come in now that exchange's listener thread is done.
            if self._action_pool is not None:
                self._action_pool.shutdown(wait=True, cancel_futures=True)

            # Shutdown the loop pool after waiting on the loops to exit.
            if self._loop_pool is not None:
                self._loop_pool.shutdown(wait=True)
                # TODO: remove pools????
                for task in self._loop_tasks.values():
                    task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await task

            await self.behavior.on_shutdown()

            terminate_for_success = (
                self.config.terminate_on_success and self._expected_shutdown
            )
            terminate_for_error = (
                self.config.terminate_on_error and not self._expected_shutdown
            )
            if self._exchange_client is not None:  # pragma: no branch
                if terminate_for_success or terminate_for_error:
                    await self._exchange_client.terminate(self.agent_id)
                await self._exchange_client.close()

            self._state = _AgentState.SHUTDOWN

            # Raise any exceptions from the loop threads as the final step.
            _raise_exceptions(self._loop_exceptions)

            logger.info(
                'Shutdown agent (%s; %s)',
                self.agent_id,
                self.behavior,
            )

    def signal_shutdown(self, expected: bool = True) -> None:
        """Signal that the agent should exit.

        If the agent has not started, this will cause the agent to immediately
        shutdown when next started. If the agent is shutdown, this has no
        effect.
        """
        self._expected_shutdown = expected
        if self._shutdown_agent is None:
            self._shutdown_agent = asyncio.Event()
        self._shutdown_agent.set()
        self._shutdown_loop.set()


def _raise_exceptions(exceptions: Sequence[tuple[str, Exception]]) -> None:
    if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
        exceptions_only = [e for _, e in exceptions]
        if len(exceptions_only) > 0:
            raise ExceptionGroup(  # noqa: F821
                'Caught failures in agent loops while shutting down.',
                exceptions_only,
            )
    else:  # pragma: <3.11 cover
        for _, exception in exceptions:
            raise exception
