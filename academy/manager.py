from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import logging
import sys
from collections.abc import MutableMapping
from concurrent.futures import Executor
from types import TracebackType
from typing import Any
from typing import Generic

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from academy.agent import Agent
from academy.agent import AgentRunConfig
from academy.behavior import BehaviorT
from academy.exception import BadEntityIdError
from academy.exception import MailboxClosedError
from academy.exchange import ExchangeFactory
from academy.exchange import UserExchangeClient
from academy.exchange.transport import AgentRegistration
from academy.exchange.transport import ExchangeTransportT
from academy.handle import RemoteHandle
from academy.identifier import AgentId
from academy.identifier import UserId
from academy.serialize import NoPickleMixin

logger = logging.getLogger(__name__)


async def _run_agent_on_worker_async(
    behavior: BehaviorT,
    config: AgentRunConfig,
    exchange_factory: ExchangeFactory[ExchangeTransportT],
    registration: AgentRegistration[BehaviorT],
) -> None:
    agent = Agent(
        behavior,
        config=config,
        exchange_factory=exchange_factory,
        registration=registration,
    )
    await agent.run()


def _run_agent_on_worker(*args: Any) -> None:
    asyncio.run(_run_agent_on_worker_async(*args))


@dataclasses.dataclass
class _ACB(Generic[BehaviorT]):
    # Agent Control Block
    agent_id: AgentId[BehaviorT]
    executor: str
    task: asyncio.Future[None]


class Manager(Generic[ExchangeTransportT], NoPickleMixin):
    """Launch and manage running agents.

    A manager is used to launch agents using one or more
    [`Executors`][concurrent.future.Executor] and interact with/manage those
    agents.

    Tip:
        This class can be used as a context manager. Upon exiting the context,
        running agents will be shutdown, any agent handles created by the
        manager will be closed, and the executors will be shutdown.

    Note:
        The manager takes ownership of the exchange client and executors,
        meaning the manager will clean up those resources when the manager
        is closed.

    Args:
        exchange_client: Exchange client.
        executors: An executor instance or mapping of names to executors to
            use to run agents. If a single executor is provided, it is set
            as the default executor with name `'default'`, overriding any
            value of `default_executor`.
        default_executor: Specify the name of the default executor to use
            when not specified in `launch()`.
        max_retries: Maximum number of times to retry running an agent
            if it exits with an error.

    Raises:
        ValueError: If `default_executor` is specified but does not exist
            in `executors`.
    """

    def __init__(
        self,
        exchange_client: UserExchangeClient[ExchangeTransportT],
        executors: Executor | MutableMapping[str, Executor],
        *,
        default_executor: str | None = None,
        max_retries: int = 0,
    ) -> None:
        if isinstance(executors, Executor):
            executors = {'default': executors}
            default_executor = 'default'

        if default_executor is not None and default_executor not in executors:
            raise ValueError(
                f'No executor named "{default_executor}" was provided to '
                'use as the default.',
            )

        self._exchange_client = exchange_client
        self._exchange_factory = exchange_client.factory()
        self._user_id = self._exchange_client.client_id
        self._executors = executors
        self._default_executor = default_executor
        self._max_retries = max_retries

        self._handles: dict[AgentId[Any], RemoteHandle[Any]] = {}
        self._acbs: dict[AgentId[Any], _ACB[Any]] = {}

        logger.info('Initialized manager (%s)', self.user_id)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        await self.close()

    def __repr__(self) -> str:
        executors_repr = ', '.join(
            f'{k}: {v!r}' for k, v in self._executors.items()
        )
        return (
            f'{type(self).__name__}'
            f'(exchange={self._exchange_client!r}, '
            f'executors={{{executors_repr}}})'
        )

    def __str__(self) -> str:
        return (
            f'{type(self).__name__}<{self.user_id}, {self._exchange_client}>'
        )

    @classmethod
    async def from_exchange_factory(
        cls,
        factory: ExchangeFactory[ExchangeTransportT],
        executors: Executor | MutableMapping[str, Executor],
        *,
        default_executor: str | None = None,
        max_retries: int = 0,
    ) -> Self:
        """Instantiate a new exchange client and manager from a factory."""
        client = await factory.create_user_client()
        return cls(
            client,
            executors,
            default_executor=default_executor,
            max_retries=max_retries,
        )

    @property
    def exchange_client(self) -> UserExchangeClient[ExchangeTransportT]:
        """User client for the exchange."""
        return self._exchange_client

    @property
    def exchange_factory(self) -> ExchangeFactory[ExchangeTransportT]:
        """Client factory for the exchange."""
        return self._exchange_factory

    @property
    def user_id(self) -> UserId:
        """Exchange client user ID of this manager."""
        return self._user_id

    async def close(self) -> None:
        """Close the manager and cleanup resources.

        1. Call shutdown on all running agents.
        1. Close all handles created by the manager.
        1. Close the mailbox associated with the manager.
        1. Close the exchange.
        1. Close all launchers.

        Raises:
            Exception: Any exceptions raised by agents.
        """
        for acb in self._acbs.values():
            if not acb.task.done():
                handle = await self.get_handle(acb.agent_id)
                with contextlib.suppress(MailboxClosedError):
                    await handle.shutdown()
        logger.debug('Requested shutdown from all agents')

        for acb in self._acbs.values():
            await acb.task
            # TODO: do this at the end as an exception group
            # Raise possible errors from agents so user sees them.
            acb.task.result()
        logger.debug('All agents have completed')

        await self.exchange_client.close()
        for executor in self._executors.values():
            executor.shutdown(wait=True, cancel_futures=True)
        logger.info('Closed manager (%s)', self.user_id)

    def add_executor(self, name: str, executor: Executor) -> Self:
        """Add an executor to the manager.

        Note:
            It is not possible to remove an executor as this could create
            complications if an agent is already running in that executor.

        Args:
            name: Name of the executor used when launching agents.
            executor: Executor instance.

        Returns:
            Self for chaining.

        Raises:
            ValueError: If an executor with `name` already exists.
        """
        if name in self._executors:
            raise ValueError(f'Executor named "{name}" already exists.')
        self._executors[name] = executor
        return self

    def set_default_executor(self, name: str | None) -> Self:
        """Set the default executor by name.

        Args:
            name: Name of the executor to use as default. If `None`, no
                default executor is set and all calls to `launch()` must
                specify the executor.

        Returns:
            Self for chaining.

        Raises:
            ValueError: If no executor with `name` exists.
        """
        if name not in self._executors:
            raise ValueError(f'An executor named "{name}" does not exist.')
        self._default_executor = name
        return self

    async def _run_agent_in_executor(
        self,
        executor: Executor,
        behavior: BehaviorT,
        config: AgentRunConfig,
        registration: AgentRegistration[BehaviorT],
    ) -> None:
        agent_id = registration.agent_id
        original_config = config
        loop = asyncio.get_running_loop()
        run_count = 0
        retries = self._max_retries

        while True:
            run_count += 1
            if retries > 0:
                retries -= 1
                # Override this configuration for the case where the agent
                # fails and we will be restarting it.
                config = dataclasses.replace(
                    original_config,
                    terminate_on_error=False,
                )
            else:
                # Otherwise, keep the original config.
                config = original_config

            logger.debug(
                'Launching agent (attempt: %s; retries: %s; %s; %s)',
                run_count,
                retries,
                agent_id,
                behavior,
            )

            try:
                await loop.run_in_executor(
                    executor,
                    _run_agent_on_worker,
                    behavior,
                    config,
                    self.exchange_factory,
                    registration,
                )
            except asyncio.CancelledError:  # pragma: no cover
                logger.warning('Cancelled %s task', agent_id)
                raise
            except Exception:
                if retries == 0:
                    logger.exception('Received exception from %s', agent_id)
                    raise
                else:
                    logger.exception(
                        'Restarting %s due to exception',
                        agent_id,
                    )
            else:
                logger.debug('Completed %s task', agent_id)
                break

    async def launch(
        self,
        behavior: BehaviorT,
        *,
        executor: str | None = None,
        name: str | None = None,
        registration: AgentRegistration[BehaviorT] | None = None,
    ) -> RemoteHandle[BehaviorT]:
        """Launch a new agent with a specified behavior.

        Args:
            behavior: Behavior the agent will implement.
            executor: Name of the executor instance to use. If `None`, uses
                the default executor, if specified, otherwise raises an error.
            name: Readable name of the agent used when registering a new agent.
            registration: If `None`, a new agent will be registered with
                the exchange.

        Returns:
            Handle (client bound) used to interact with the agent.

        Raises:
            RuntimeError: If `registration` is provided and an agent with
                that ID has already been executed.
            ValueError: If no default executor is set and `executor` is not
                specified.
        """
        if self._default_executor is None and executor is None:
            raise ValueError(
                'Must specify the executor when no default is set.',
            )
        executor = executor if executor is not None else self._default_executor
        assert executor is not None
        executor_instance = self._executors[executor]

        if registration is None:
            registration = await self.register_agent(type(behavior), name=name)
        elif registration.agent_id in self._acbs:
            raise RuntimeError(
                f'{registration.agent_id} has already been executed.',
            )

        agent_id = registration.agent_id
        config = AgentRunConfig()

        task = asyncio.create_task(
            self._run_agent_in_executor(
                executor_instance,
                behavior,
                config,
                registration,
            ),
            name=f'manager-run-{agent_id}',
        )

        acb = _ACB(agent_id=agent_id, executor=executor, task=task)
        self._acbs[agent_id] = acb
        handle = await self.get_handle(agent_id)
        logger.info('Launched agent (%s; %s)', agent_id, behavior)
        return handle

    async def get_handle(
        self,
        agent: AgentId[BehaviorT] | AgentRegistration[BehaviorT],
    ) -> RemoteHandle[BehaviorT]:
        """Create a new handle to an agent.

        A handle acts like a reference to a remote agent, enabling a user
        to manage the agent or asynchronously invoke actions.

        Args:
            agent: Agent ID or registration indicating the agent to create
                a handle to. The agent must be registered with the same
                exchange that this manager is a client of.

        Returns:
            Handle to the agent.
        """
        agent_id = agent if isinstance(agent, AgentId) else agent.agent_id
        handle = self._handles.get(agent_id, None)
        if handle is not None and not handle.closed():
            return handle
        handle = await self.exchange_client.get_handle(agent_id)
        self._handles[agent_id] = handle
        return handle

    async def register_agent(
        self,
        behavior: type[BehaviorT],
        *,
        name: str | None = None,
    ) -> AgentRegistration[BehaviorT]:
        """Register a new agent with the exchange.

        Args:
            behavior: Behavior type of the agent.
            name: Optional display name for the agent.

        Returns:
            Agent registration info that can be passed to
            [`launch()`][academy.manager.Manager.launch].
        """
        return await self.exchange_client.register_agent(behavior, name=name)

    def running(self) -> set[AgentId[Any]]:
        """Get a set of IDs of all running agents.

        Returns:
            Set of agent IDs corresponding to all agents launched by this \
            manager that have not completed yet.
        """
        running: set[AgentId[Any]] = set()
        for acb in self._acbs.values():
            if not acb.task.done():
                running.add(acb.agent_id)
        return running

    async def shutdown(
        self,
        agent: AgentId[Any] | RemoteHandle[Any],
        *,
        blocking: bool = True,
        raise_error: bool = True,
        timeout: float | None = None,
    ) -> None:
        """Shutdown a launched agent.

        Args:
            agent: ID or handle to the launched agent.
            blocking: Wait for the agent to exit before returning.
            raise_error: Raise the error returned by the agent if
                `blocking=True`.
            timeout: Optional timeout is seconds when `blocking=True`.

        Raises:
            BadEntityIdError: If an agent with `agent_id` was not
                launched by this manager.
            TimeoutError: If `timeout` was exceeded while blocking for agent.
        """
        agent_id = agent.agent_id if isinstance(agent, RemoteHandle) else agent

        if agent_id not in self._acbs:
            raise BadEntityIdError(agent_id) from None
        if self._acbs[agent_id].task.done():
            return

        handle = await self.get_handle(agent_id)
        with contextlib.suppress(MailboxClosedError):
            await handle.shutdown()

        if blocking:
            await self.wait(agent_id, raise_error=raise_error, timeout=timeout)

    async def wait(
        self,
        agent: AgentId[Any] | RemoteHandle[Any],
        *,
        raise_error: bool = True,
        timeout: float | None = None,
    ) -> None:
        """Wait for a launched agent to exit.

        Note:
            Calling `wait()` is only valid after `launch()` has succeeded.

        Args:
            agent: ID or handle to the launched agent.
            raise_error: Raise the error returned by the agent.
            timeout: Optional timeout in seconds to wait for agent.

        Raises:
            BadEntityIdError: If the agent was not found. This likely means
                the agent was not launched by this manager.
            TimeoutError: If `timeout` was exceeded while waiting for agent.
            Exception: Any exception raised by the agent if it exited due
                to a failure and `raise_error=True`.
        """
        agent_id = agent.agent_id if isinstance(agent, RemoteHandle) else agent

        try:
            acb = self._acbs[agent_id]
        except KeyError:
            raise BadEntityIdError(agent_id) from None

        done, pending = await asyncio.wait({acb.task}, timeout=timeout)

        if acb.task in pending:
            raise TimeoutError(
                f'Agent did not complete within {timeout}s timeout '
                f'({acb.agent_id})',
            )

        if raise_error:
            acb.task.result()
