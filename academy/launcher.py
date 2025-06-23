from __future__ import annotations

import asyncio
import dataclasses
import logging
import sys
from concurrent.futures import Executor
from concurrent.futures import ThreadPoolExecutor
from types import TracebackType
from typing import Any

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from academy.agent import Agent
from academy.agent import AgentRunConfig
from academy.behavior import BehaviorT
from academy.exception import BadEntityIdError
from academy.exchange import ExchangeClient
from academy.exchange.transport import AgentRegistrationT
from academy.exchange.transport import ExchangeTransportT
from academy.handle import BoundRemoteHandle
from academy.identifier import AgentId

logger = logging.getLogger(__name__)


def _run_agent_on_worker(agent: Agent[AgentRegistrationT, BehaviorT]) -> None:
    asyncio.run(agent.run())


@dataclasses.dataclass
class _ACB[BehaviorT]:
    # Agent Control Block
    agent_id: AgentId[BehaviorT]
    task: asyncio.Future[None]


class Launcher:
    """Launcher that wraps a [`concurrent.futures.Executor`][concurrent.futures.Executor].

    Args:
        executor: Executor used for launching agents. Note that this class
            takes ownership of the `executor`.
        max_restarts: Maximum times to restart an agent if it exits with
            an error.
    """  # noqa: E501

    def __init__(
        self,
        executor: Executor,
        *,
        max_restarts: int = 0,
    ) -> None:
        self._executor = executor
        self._max_restarts = max_restarts
        self._acbs: dict[AgentId[Any], _ACB[Any]] = {}

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
        return f'{type(self).__name__}(executor={self._executor!r})'

    def __str__(self) -> str:
        return f'{type(self).__name__}<{type(self._executor).__name__}>'

    async def _run_agent(self, agent: Agent[Any, Any], started: asyncio.Event) -> None:
        agent_id = agent.agent_id
        config = agent.config
        loop = asyncio.get_running_loop()
        run_count = 0
        started.set()

        while run_count <= self._max_restarts:
            run_count += 1

            if run_count < self._max_restarts:
                # Override this configuration for the case where the agent
                # fails and we will restart it.
                agent.config = dataclasses.replace(
                    config,
                    terminate_on_error=False,
                )
            else:
                # Otherwise, keep the original config.
                agent.config = config

            if run_count == 1:
                logger.debug(
                    'Launching agent (%s; %s)',
                    agent_id,
                    agent.behavior,
                )
            else:
                logger.debug(
                    'Restarting agent (%d/%d retries; %s; %s)',
                    run_count,
                    self._max_restarts,
                    agent_id,
                    agent.behavior,
                )        

            try:
                await loop.run_in_executor(
                    self._executor,
                    _run_agent_on_worker,
                    agent,
                )
            except asyncio.CancelledError:  # pragma: no cover
                logger.warning('Cancelled agent task (%s)', agent_id)
                break
            except Exception:  # pragma: no cover
                logger.exception('Received agent exception (%s)', agent_id)
                if run_count == self._max_restarts:
                    break
            else:
                logger.debug('Completed agent task (%s)', agent_id)
                break

    async def close(self) -> None:
        """Close the launcher.

        Warning:
            This will not return until all agents have exited. It is the
            caller's responsibility to shutdown agents prior to closing
            the launcher.
        """
        logger.debug('Waiting for agents to shutdown...')
        for acb in self._acbs.values():
            if acb.task.done():
                # Raise possible errors from agents so user sees them.
                await acb.task
        self._executor.shutdown(wait=True, cancel_futures=True)
        logger.debug('Closed launcher (%s)', self)

    async def launch(
        self,
        behavior: BehaviorT,
        exchange: ExchangeClient[ExchangeTransportT],
        *,
        agent_id: AgentId[BehaviorT] | None = None,
        name: str | None = None,
    ) -> BoundRemoteHandle[BehaviorT]:
        """Launch a new agent with a specified behavior.

        Args:
            behavior: Behavior the agent should implement.
            exchange: Exchange the agent will use for messaging.
            agent_id: Specify ID of the launched agent. If `None`, a new
                agent ID will be created within the exchange.
            name: Readable name of the agent. Ignored if `agent_id` is
                provided.

        Returns:
            Handle (unbound) used to interact with the agent.
        """
        registration = await exchange.register_agent(
            type(behavior),
            name=name,
            _agent_id=agent_id,
        )
        agent_id = registration.agent_id

        agent = Agent(
            behavior,
            config=AgentRunConfig(),
            exchange_factory=exchange.factory(),
            registration=registration,
        )

        started = asyncio.Event()
        task = asyncio.create_task(
            self._run_agent(agent, started),
            name=f'launcher-run-{agent_id}',
        )

        acb = _ACB(agent_id=agent_id, task=task)
        self._acbs[agent_id] = acb

        handle = await exchange.get_handle(agent_id)
        await started.wait()
        return handle

    def running(self) -> set[AgentId[Any]]:
        """Get a set of IDs for all running agents.

        Returns:
            Set of agent IDs corresponding to all agents launched by this \
            launcher that have not completed yet.
        """
        running: set[AgentId[Any]] = set()
        for acb in self._acbs.values():
            if not acb.task.done():
                running.add(acb.agent_id)
        return running

    async def wait(
        self,
        agent_id: AgentId[Any],
        *,
        ignore_error: bool = False,
        timeout: float | None = None,
    ) -> None:
        """Wait for a launched agent to exit.

        Note:
            Calling `wait()` is only valid after `launch()` has succeeded.

        Args:
            agent_id: ID of launched agent.
            ignore_error: Ignore any errors raised by the agent.
            timeout: Optional timeout in seconds to wait for agent.

        Raises:
            BadEntityIdError: If an agent with `agent_id` was not
                launched by this launcher.
            TimeoutError: If `timeout` was exceeded while waiting for agent.
            Exception: Any exception raised by the agent if
                `ignore_error=False`.
        """
        try:
            acb = self._acbs[agent_id]
        except KeyError:
            raise BadEntityIdError(agent_id) from None

        try:
            await asyncio.wait_for(acb.task, timeout=timeout)
        except asyncio.TimeoutError:
            raise TimeoutError(
                f'Agent did not complete within {timeout}s timeout '
                f'({acb.agent_id})',
            ) from None

        if not ignore_error:
            acb.task.result()


class ThreadLauncher(Launcher):
    """Launcher that wraps a default [`concurrent.futures.ThreadPoolExecutor`][concurrent.futures.ThreadPoolExecutor].

    Args:
        max_workers: The maximum number of threads (i.e., agents) in the pool.
        max_restarts: Maximum times to restart an agent if it exits with
            an error.
    """  # noqa: E501

    def __init__(
        self,
        max_workers: int | None = None,
        *,
        max_restarts: int = 0,
    ) -> None:
        executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix='launcher',
        )
        super().__init__(executor, max_restarts=max_restarts)
