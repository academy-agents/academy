from __future__ import annotations

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest

from academy.behavior import action
from academy.behavior import Behavior
from academy.exception import BadEntityIdError
from academy.exchange import ExchangeClient
from academy.identifier import AgentId
from academy.launcher import Launcher
from testing.behavior import SleepBehavior
from testing.constant import TEST_CONNECTION_TIMEOUT
from testing.constant import TEST_LOOP_SLEEP


@pytest.mark.asyncio
async def test_create_launcher() -> None:
    executor = ThreadPoolExecutor(max_workers=2)
    async with Launcher(executor) as launcher:
        assert 'ThreadPoolExecutor' in str(launcher)


@pytest.mark.asyncio
async def test_launch_agents_threads(exchange: ExchangeClient[Any]) -> None:
    behavior = SleepBehavior(TEST_LOOP_SLEEP)
    executor = ThreadPoolExecutor(max_workers=2)
    async with Launcher(executor) as launcher:
        handle1 = await launcher.launch(behavior, exchange)

        registration = await exchange.register_agent(SleepBehavior)
        handle2 = await launcher.launch(
            behavior,
            exchange,
            registration=registration,
        )

        assert len(launcher.running()) == 2  # noqa: PLR2004

        await asyncio.sleep(5 * TEST_LOOP_SLEEP)

        await handle1.shutdown()
        await handle2.shutdown()

        await handle1.close()
        await handle2.close()

        await launcher.wait(handle1.agent_id)
        await launcher.wait(handle2.agent_id)

        assert len(launcher.running()) == 0


@pytest.mark.asyncio
async def test_wait_bad_identifier() -> None:
    executor = ThreadPoolExecutor(max_workers=1)
    async with Launcher(executor) as launcher:
        agent_id: AgentId[Any] = AgentId.new()

        with pytest.raises(BadEntityIdError):
            await launcher.wait(agent_id)


@pytest.mark.asyncio
async def test_wait_timeout(exchange: ExchangeClient[Any]) -> None:
    behavior = SleepBehavior(TEST_LOOP_SLEEP)
    executor = ThreadPoolExecutor(max_workers=1)
    async with Launcher(executor) as launcher:
        handle = await launcher.launch(behavior, exchange)

        with pytest.raises(TimeoutError):
            await launcher.wait(handle.agent_id, timeout=TEST_LOOP_SLEEP)

        await handle.shutdown()
        await handle.close()
        await launcher.wait(handle.agent_id)


class FailOnStartupBehavior(Behavior):
    def __init__(self, max_errors: int | None = None) -> None:
        self.errors = 0
        self.max_errors = max_errors

    async def on_setup(self) -> None:
        if (
            self.max_errors is None or self.errors < self.max_errors
        ):  # pragma: no branch
            self.errors += 1
            raise RuntimeError('Agent startup failed')

    @action
    async def get_errors(self) -> int:  # pragma: no cover
        return self.errors


@pytest.mark.skip(reason='Issue #93')
@pytest.mark.asyncio
async def test_restart_on_error(
    exchange: ExchangeClient[Any],
    caplog,
) -> None:  # pragma: no cover
    # TODO: this test fails because we rerun the same Agent instance
    # in the thread pool executor; after the first run the agent is in a
    # shutdown state and cannot be run again. After deferred behavior
    # initialization, this will be possible.
    # Once fixed, remove no cover in behavior, this test, and the restart
    # loop in the launcher agent task.
    caplog.set_level(logging.DEBUG)
    behavior = FailOnStartupBehavior(max_errors=2)
    executor = ThreadPoolExecutor(max_workers=1)
    async with Launcher(executor, max_restarts=3) as launcher:
        handle = await launcher.launch(behavior, exchange)
        errors_future = await handle.get_errors()
        assert await errors_future == 2  # noqa: PLR2004
        await launcher.wait(handle.agent_id, timeout=TEST_CONNECTION_TIMEOUT)
        await handle.close()


@pytest.mark.parametrize('ignore_error', (True, False))
@pytest.mark.asyncio
async def test_wait_ignore_agent_errors(
    ignore_error: bool,
    exchange: ExchangeClient[Any],
) -> None:
    behavior = FailOnStartupBehavior()
    with ThreadPoolExecutor(max_workers=1) as executor:
        launcher = Launcher(executor)
        handle = await launcher.launch(behavior, exchange)
        if ignore_error:
            await launcher.wait(handle.agent_id, ignore_error=ignore_error)
        else:
            with pytest.raises(RuntimeError, match='Agent startup failed'):
                await launcher.wait(handle.agent_id, ignore_error=ignore_error)
        await handle.close()

        with pytest.raises(RuntimeError, match='Agent startup failed'):
            await launcher.close()
