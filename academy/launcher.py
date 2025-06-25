from __future__ import annotations

import asyncio
import dataclasses
import logging
import sys
from concurrent.futures import Executor
from concurrent.futures import ThreadPoolExecutor
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
from academy.exchange import ExchangeClient
from academy.exchange import ExchangeFactory
from academy.exchange.transport import AgentRegistration
from academy.exchange.transport import ExchangeTransportT
from academy.handle import RemoteHandle
from academy.identifier import AgentId

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

    async def _run_agent(
        self,
        behavior: BehaviorT,
        config: AgentRunConfig,
        exchange_factory: ExchangeFactory[ExchangeTransportT],
        registration: AgentRegistration[BehaviorT],
    ) -> None:
        agent_id = registration.agent_id
        original_config = config
        loop = asyncio.get_running_loop()
        run_count = 0
        retries = self._max_restarts

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
                    self._executor,
                    _run_agent_on_worker,
                    behavior,
                    config,
                    exchange_factory,
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

    async def close(self) -> None:
        """Close the launcher.

        Warning:
            This will not return until all agents have exited. It is the
            caller's responsibility to shutdown agents prior to closing
            the launcher.
        """
        logger.debug('Waiting for agents to shutdown...')
        for acb in self._acbs.values():
            await acb.task
            # Raise possible errors from agents so user sees them.
            acb.task.result()
        self._executor.shutdown(wait=True, cancel_futures=True)
        logger.debug('Closed launcher (%s)', self)

    async def launch(
        self,
        behavior: BehaviorT,
        exchange: ExchangeClient[ExchangeTransportT],
        *,
        name: str | None = None,
        registration: AgentRegistration[BehaviorT] | None = None,
    ) -> RemoteHandle[BehaviorT]:
        """Launch a new agent with a specified behavior.

        Args:
            behavior: Behavior the agent should implement.
            exchange: Exchange the agent will use for messaging.
            name: Readable name of the agent used when registering a new agent.
            registration: If `None`, a new agent will be registered with
                the exchange.

        Returns:
            Handle (unbound) used to interact with the agent.
        """
        if registration is None:
            registration = await exchange.register_agent(
                type(behavior),
                name=name,
            )
        agent_id = registration.agent_id
        config = AgentRunConfig()
        exchange_factory = exchange.factory()

        task = asyncio.create_task(
            self._run_agent(behavior, config, exchange_factory, registration),
            name=f'launcher-run-{agent_id}',
        )

        acb = _ACB(agent_id=agent_id, task=task)
        self._acbs[agent_id] = acb

        handle = await exchange.get_handle(agent_id)
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

        done, pending = await asyncio.wait({acb.task}, timeout=timeout)

        if acb.task in pending:
            raise TimeoutError(
                f'Agent did not complete within {timeout}s timeout '
                f'({acb.agent_id})',
            )

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
