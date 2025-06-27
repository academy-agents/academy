from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor

import pytest

from academy.behavior import Behavior
from academy.exception import BadEntityIdError
from academy.exchange import UserExchangeClient
from academy.exchange.local import LocalExchangeFactory
from academy.exchange.local import LocalExchangeTransport
from academy.manager import Manager
from testing.behavior import EmptyBehavior
from testing.behavior import SleepBehavior
from testing.constant import TEST_CONNECTION_TIMEOUT
from testing.constant import TEST_LOOP_SLEEP
from testing.constant import TEST_THREAD_JOIN_TIMEOUT


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
    behavior = SleepBehavior(TEST_LOOP_SLEEP)

    handle1 = await manager.launch(behavior)
    handle2 = await manager.launch(behavior)

    assert len(manager.running()) == 2  # noqa: PLR2004

    await asyncio.sleep(5 * TEST_LOOP_SLEEP)

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
    behavior = SleepBehavior(TEST_LOOP_SLEEP)
    # The manager fixture uses a context manager that when exited should
    # shutdown this agent and wait on it
    await manager.launch(behavior)
    await asyncio.sleep(5 * TEST_LOOP_SLEEP)
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
    registration = await manager.register_agent(EmptyBehavior)
    with pytest.raises(BadEntityIdError):
        await manager.wait({registration.agent_id})


@pytest.mark.asyncio
async def test_wait_timeout(
    manager: Manager[LocalExchangeTransport],
) -> None:
    behavior = SleepBehavior(TEST_LOOP_SLEEP)
    handle = await manager.launch(behavior)
    with pytest.raises(TimeoutError):
        await manager.wait({handle}, timeout=TEST_LOOP_SLEEP)


@pytest.mark.asyncio
async def test_wait_timeout_all_completed(
    manager: Manager[LocalExchangeTransport],
) -> None:
    behavior = SleepBehavior(TEST_LOOP_SLEEP)
    handle1 = await manager.launch(behavior)
    handle2 = await manager.launch(behavior)
    await manager.shutdown(handle1, blocking=True)
    with pytest.raises(TimeoutError):
        await manager.wait(
            {handle1, handle2},
            timeout=TEST_LOOP_SLEEP,
            return_when=asyncio.ALL_COMPLETED,
        )


@pytest.mark.asyncio
async def test_shutdown_bad_identifier(
    manager: Manager[LocalExchangeTransport],
) -> None:
    registration = await manager.register_agent(EmptyBehavior)
    with pytest.raises(BadEntityIdError):
        await manager.shutdown(registration.agent_id)


@pytest.mark.asyncio
async def test_duplicate_launched_agents_error(
    manager: Manager[LocalExchangeTransport],
) -> None:
    registration = await manager.register_agent(EmptyBehavior)
    await manager.launch(EmptyBehavior(), registration=registration)
    with pytest.raises(
        RuntimeError,
        match=f'{registration.agent_id} has already been executed.',
    ):
        await manager.launch(EmptyBehavior(), registration=registration)
    assert len(manager.running()) == 1


@pytest.mark.asyncio
async def test_shutdown_nonblocking(
    manager: Manager[LocalExchangeTransport],
) -> None:
    behavior = SleepBehavior(TEST_LOOP_SLEEP)
    handle = await manager.launch(behavior)
    await manager.shutdown(handle, blocking=False)
    await manager.wait({handle}, timeout=TEST_THREAD_JOIN_TIMEOUT)


@pytest.mark.asyncio
async def test_shutdown_blocking(
    manager: Manager[LocalExchangeTransport],
) -> None:
    behavior = SleepBehavior(TEST_LOOP_SLEEP)
    handle = await manager.launch(behavior)
    await manager.shutdown(handle, blocking=True)
    assert len(manager.running()) == 0


@pytest.mark.asyncio
async def test_bad_default_executor(
    exchange: UserExchangeClient[LocalExchangeTransport],
) -> None:
    with pytest.raises(ValueError, match='No executor named "second"'):
        Manager(
            exchange_client=exchange,
            executors={'first': ThreadPoolExecutor()},
            default_executor='second',
        )


@pytest.mark.asyncio
async def test_add_and_set_executor_errors(
    exchange: UserExchangeClient[LocalExchangeTransport],
) -> None:
    executor = ThreadPoolExecutor()
    async with Manager(
        exchange_client=exchange,
        executors={'first': executor},
    ) as manager:
        with pytest.raises(
            ValueError,
            match='Executor named "first" already exists.',
        ):
            manager.add_executor('first', executor)
        with pytest.raises(
            ValueError,
            match='An executor named "second" does not exist.',
        ):
            manager.set_default_executor('second')


@pytest.mark.asyncio
async def test_multiple_executor(
    exchange: UserExchangeClient[LocalExchangeTransport],
) -> None:
    async with Manager(
        exchange_client=exchange,
        executors={'first': ThreadPoolExecutor()},
    ) as manager:
        await manager.launch(EmptyBehavior(), executor='first')

        manager.add_executor('second', ThreadPoolExecutor())
        manager.set_default_executor('second')
        await manager.launch(EmptyBehavior())
        await manager.launch(EmptyBehavior(), executor='first')


@pytest.mark.asyncio
async def test_multiple_executor_no_default(
    exchange: UserExchangeClient[LocalExchangeTransport],
) -> None:
    async with Manager(
        exchange_client=exchange,
        executors={'first': ThreadPoolExecutor()},
    ) as manager:
        with pytest.raises(ValueError, match='no default is set.'):
            await manager.launch(EmptyBehavior())


class FailOnStartupBehavior(Behavior):
    def __init__(self, max_errors: int | None = None) -> None:
        self.errors = 0
        self.max_errors = max_errors

    async def on_setup(self) -> None:
        if self.max_errors is None or self.errors < self.max_errors:
            self.errors += 1
            raise RuntimeError('Agent startup failed')


@pytest.mark.asyncio
async def test_retry_on_error(
    exchange: UserExchangeClient[LocalExchangeTransport],
) -> None:
    # Note: this test presently relies on behavior to be shared across
    # each agent execution in other threads.
    behavior = FailOnStartupBehavior(max_errors=2)
    executor = ThreadPoolExecutor(max_workers=1)
    async with Manager(exchange, executors=executor, max_retries=3) as manager:
        handle = await manager.launch(behavior)
        await handle.ping(timeout=TEST_CONNECTION_TIMEOUT)
        assert behavior.errors == 2  # noqa: PLR2004
        await handle.shutdown()


@pytest.mark.parametrize('raise_error', (True, False))
@pytest.mark.asyncio
async def test_wait_ignore_agent_errors(
    raise_error: bool,
    exchange: UserExchangeClient[LocalExchangeTransport],
) -> None:
    behavior = FailOnStartupBehavior()
    manager = Manager(exchange, executors=ThreadPoolExecutor(max_workers=1))
    handle = await manager.launch(behavior)

    if raise_error:
        with pytest.raises(RuntimeError, match='Agent startup failed'):
            await manager.wait({handle}, raise_error=raise_error)
    else:
        await manager.wait({handle}, raise_error=raise_error)

    with pytest.raises(RuntimeError, match='Agent startup failed'):
        await manager.close()


@pytest.mark.asyncio
async def test_warn_executor_overload(
    exchange: UserExchangeClient[LocalExchangeTransport],
) -> None:
    behavior = SleepBehavior(TEST_LOOP_SLEEP)
    async with Manager(
        exchange,
        executors=ThreadPoolExecutor(max_workers=1),
    ) as manager:
        await manager.launch(behavior)
        with pytest.warns(RuntimeWarning, match='Executor overload:'):
            await manager.launch(behavior)
        assert len(manager.running()) == 2  # noqa: PLR2004
