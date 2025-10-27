from __future__ import annotations

import asyncio
import os
import pathlib
import sys
from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from typing import Callable

from academy.handle import Handle

if sys.version_info >= (3, 10):  # pragma: >=3.10 cover
    from typing import ParamSpec
else:  # pragma: <3.10 cover
    from typing_extensions import ParamSpec

import pytest

from academy.agent import action
from academy.agent import Agent
from academy.exception import BadEntityIdError
from academy.exception import MailboxTerminatedError
from academy.exchange import LocalExchangeFactory
from academy.exchange import LocalExchangeTransport
from academy.exchange import UserExchangeClient
from academy.manager import Manager
from testing.agents import EmptyAgent
from testing.agents import SleepAgent
from testing.constant import TEST_CONNECTION_TIMEOUT
from testing.constant import TEST_SLEEP_INTERVAL
from testing.constant import TEST_WAIT_TIMEOUT

P = ParamSpec('P')


@pytest.mark.asyncio
async def test_from_exchange_factory() -> None:
    async with await Manager.from_exchange_factory(
        factory=LocalExchangeFactory(),
        executors=ThreadPoolExecutor(),
    ) as manager:
        assert isinstance(repr(manager), str)
        assert isinstance(str(manager), str)


@pytest.mark.asyncio
async def test_launch_and_shutdown(
    manager: Manager[LocalExchangeTransport],
) -> None:
    # Two ways to launch: agent instance or deferred agent initialization
    handle1 = await manager.launch(SleepAgent(TEST_SLEEP_INTERVAL))
    handle2 = await manager.launch(SleepAgent, args=(TEST_SLEEP_INTERVAL,))

    assert len(manager.running()) == 2  # noqa: PLR2004

    await asyncio.sleep(5 * TEST_SLEEP_INTERVAL)

    await manager.shutdown(handle1.agent_id)
    await manager.shutdown(handle2)

    await manager.wait({handle1})
    await manager.wait({handle2})

    assert len(manager.running()) == 0

    # Should be a no-op since the agent is already shutdown
    await manager.shutdown(handle1)


@pytest.mark.asyncio
async def test_shutdown_on_exit(
    manager: Manager[LocalExchangeTransport],
) -> None:
    agent = SleepAgent(TEST_SLEEP_INTERVAL)
    # The manager fixture uses a context manager that when exited should
    # shutdown this agent and wait on it
    await manager.launch(agent)
    await asyncio.sleep(5 * TEST_SLEEP_INTERVAL)
    assert len(manager.running()) == 1


@pytest.mark.asyncio
async def test_wait_empty_iterable(
    manager: Manager[LocalExchangeTransport],
) -> None:
    await manager.wait({})


@pytest.mark.asyncio
async def test_wait_bad_identifier(
    manager: Manager[LocalExchangeTransport],
) -> None:
    registration = await manager.register_agent(EmptyAgent)
    with pytest.raises(BadEntityIdError):
        await manager.wait({registration.agent_id})


@pytest.mark.asyncio
async def test_wait_timeout(
    manager: Manager[LocalExchangeTransport],
) -> None:
    agent = SleepAgent(TEST_SLEEP_INTERVAL)
    handle = await manager.launch(agent)
    with pytest.raises(TimeoutError):
        await manager.wait({handle}, timeout=TEST_SLEEP_INTERVAL)


@pytest.mark.asyncio
async def test_wait_timeout_all_completed(
    manager: Manager[LocalExchangeTransport],
) -> None:
    agent = SleepAgent(TEST_SLEEP_INTERVAL)
    handle1 = await manager.launch(agent)
    handle2 = await manager.launch(agent)
    await manager.shutdown(handle1, blocking=True)
    with pytest.raises(TimeoutError):
        await manager.wait(
            {handle1, handle2},
            timeout=TEST_SLEEP_INTERVAL,
            return_when=asyncio.ALL_COMPLETED,
        )


@pytest.mark.asyncio
async def test_shutdown_bad_identifier(
    manager: Manager[LocalExchangeTransport],
) -> None:
    registration = await manager.register_agent(EmptyAgent)
    with pytest.raises(BadEntityIdError):
        await manager.shutdown(registration.agent_id)


@pytest.mark.asyncio
async def test_duplicate_launched_agents_error(
    manager: Manager[LocalExchangeTransport],
) -> None:
    registration = await manager.register_agent(EmptyAgent)
    await manager.launch(EmptyAgent(), registration=registration)
    with pytest.raises(
        RuntimeError,
        match=f'{registration.agent_id} has already been executed.',
    ):
        await manager.launch(EmptyAgent(), registration=registration)
    assert len(manager.running()) == 1


@pytest.mark.asyncio
async def test_shutdown_nonblocking(
    manager: Manager[LocalExchangeTransport],
) -> None:
    agent = SleepAgent(TEST_SLEEP_INTERVAL)
    handle = await manager.launch(agent)
    await manager.shutdown(handle, blocking=False)
    await manager.wait({handle}, timeout=TEST_WAIT_TIMEOUT)


@pytest.mark.asyncio
async def test_shutdown_blocking(
    manager: Manager[LocalExchangeTransport],
) -> None:
    agent = SleepAgent(TEST_SLEEP_INTERVAL)
    handle = await manager.launch(agent)
    await manager.shutdown(handle, blocking=True)
    assert len(manager.running()) == 0


@pytest.mark.asyncio
async def test_bad_default_executor(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    with pytest.raises(ValueError, match='No executor named "second"'):
        Manager(
            exchange_client=exchange_client,
            executors={'first': ThreadPoolExecutor()},
            default_executor='second',
        )


@pytest.mark.asyncio
async def test_add_and_set_executor_errors(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    executor = ThreadPoolExecutor()
    async with Manager(
        exchange_client=exchange_client,
        executors={'first': executor},
    ) as manager:
        with pytest.raises(
            ValueError,
            match=r'Executor named "first" already exists\.',
        ):
            manager.add_executor('first', executor)
        with pytest.raises(
            ValueError,
            match=r'An executor named "second" does not exist\.',
        ):
            manager.set_default_executor('second')


@pytest.mark.asyncio
async def test_multiple_executor(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    async with Manager(
        exchange_client=exchange_client,
        executors={'first': ThreadPoolExecutor()},
    ) as manager:
        await manager.launch(EmptyAgent(), executor='first')

        manager.add_executor('second', ThreadPoolExecutor())
        manager.set_default_executor('second')
        await manager.launch(EmptyAgent())
        await manager.launch(EmptyAgent(), executor='first')


@pytest.mark.asyncio
async def test_multiple_executor_no_default(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    async with Manager(
        exchange_client=exchange_client,
        executors={'first': ThreadPoolExecutor()},
    ) as manager:
        with pytest.raises(ValueError, match=r'no default is set\.'):
            await manager.launch(EmptyAgent())


class FailOnStartupAgent(Agent):
    def __init__(self, max_errors: int | None = None) -> None:
        self.errors = 0
        self.max_errors = max_errors

    async def agent_on_startup(self) -> None:
        if self.max_errors is None or self.errors < self.max_errors:
            self.errors += 1
            raise RuntimeError('Agent startup failed')


@pytest.mark.asyncio
async def test_retry_on_error(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    # Note: this test presently relies on agent to be shared across
    # each agent execution in other threads.
    agent = FailOnStartupAgent(max_errors=2)
    executor = ThreadPoolExecutor(max_workers=1)
    async with Manager(
        exchange_client,
        executors=executor,
        max_retries=3,
    ) as manager:
        handle = await manager.launch(agent)
        await handle.ping(timeout=TEST_CONNECTION_TIMEOUT)
        assert agent.errors == 2  # noqa: PLR2004
        await handle.shutdown()


@pytest.mark.parametrize('raise_error', (True, False))
@pytest.mark.asyncio
async def test_wait_ignore_agent_errors(
    raise_error: bool,
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    agent = FailOnStartupAgent()
    manager = Manager(
        exchange_client,
        executors=ThreadPoolExecutor(max_workers=1),
    )
    handle = await manager.launch(agent)

    if raise_error:
        with pytest.raises(RuntimeError, match='Agent startup failed'):
            await manager.wait({handle}, raise_error=raise_error)
    else:
        await manager.wait({handle}, raise_error=raise_error)

    with pytest.raises(RuntimeError, match='Agent startup failed'):
        await manager.close()


@pytest.mark.asyncio
async def test_warn_executor_overload(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    agent = SleepAgent(TEST_SLEEP_INTERVAL)
    async with Manager(
        exchange_client,
        executors=ThreadPoolExecutor(max_workers=1),
    ) as manager:
        await manager.launch(agent)
        with pytest.warns(RuntimeWarning, match='Executor overload:'):
            await manager.launch(agent)
        assert len(manager.running()) == 2  # noqa: PLR2004


@pytest.mark.asyncio
async def test_executor_pass_kwargs(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    class MockExecutor(ThreadPoolExecutor):
        def submit(
            self,
            fn: Callable[P, Any],
            /,
            *args: P.args,
            **kwargs: P.kwargs,
        ) -> Future[Any]:
            assert 'parsl_resource_spec' in kwargs
            return super().submit(fn, *args, **kwargs)

    agent = SleepAgent(TEST_SLEEP_INTERVAL)
    async with Manager(
        exchange_client,
        executors=MockExecutor(),
    ) as manager:
        await manager.launch(
            agent,
            submit_kwargs={'parsl_resource_spec': {'cores': 1}},
        )


# Note: these tests are just for coverage to make sure the code is functional.
# It does not test the agent of init_logging because pytest captures
# logging already.
@pytest.mark.asyncio
async def test_worker_init_logging_no_logfile(
    manager: Manager[LocalExchangeTransport],
) -> None:
    agent = SleepAgent(TEST_SLEEP_INTERVAL)
    handle = await manager.launch(agent, init_logging=True)
    await handle.shutdown()
    await manager.wait({handle})


@pytest.mark.asyncio
async def test_worker_init_logging_logfile(
    manager: Manager[LocalExchangeTransport],
    tmp_path: pathlib.Path,
) -> None:
    filepath = os.path.join(tmp_path, '{agent_id}-log.txt')
    agent = SleepAgent(TEST_SLEEP_INTERVAL)
    handle = await manager.launch(agent, init_logging=True, logfile=filepath)
    await handle.shutdown()
    await manager.wait({handle})


@pytest.mark.asyncio
async def test_agent_manager_iteraction(
    manager: Manager[LocalExchangeTransport],
) -> None:
    class ChildAgent(Agent):
        @action
        async def echo(self, item: str) -> str:
            return item

    class ParentAgent(Agent):
        """This is an agent that makes children."""

        async def agent_on_startup(self):
            self.manager = Manager(
                self.agent_exchange_client,
                ThreadPoolExecutor(),
            )

        async def agent_on_shutdown(self):
            await self.manager.close(close_exchange=False)
            return await super().agent_on_shutdown()

        @action
        async def launch_child(self) -> Handle[ChildAgent]:
            """Create a child."""
            return await self.manager.launch(ChildAgent)

    parent = await manager.launch(ParentAgent)
    child = await parent.launch_child()

    result = await child.echo('hello')
    assert result == 'hello'

    await manager.shutdown(parent)
    await manager.wait([parent])

    with pytest.raises(MailboxTerminatedError):
        await child.echo('hello')
