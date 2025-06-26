from __future__ import annotations

import asyncio

import pytest

from academy.event_loop import EventLoopRunner


def test_event_loop_run_coroutine() -> None:
    async def coro() -> str:
        await asyncio.sleep(0.001)
        return 'done'

    with EventLoopRunner() as runner:
        future = runner.run_coroutine(coro())
        assert future.result() == 'done'


def test_event_loop_run_coroutine_error() -> None:
    async def coro() -> None:
        await asyncio.sleep(0.001)
        raise RuntimeError('Oops!')

    with EventLoopRunner() as runner:
        future = runner.run_coroutine(coro())
        with pytest.raises(RuntimeError, match='Oops!'):
            future.result()


def test_event_loop_stop_idempotent() -> None:
    runner = EventLoopRunner()
    runner.stop()
    runner.stop()


def test_event_loop_run_coroutine_after_stop() -> None:
    async def coro() -> None:
        pass

    runner = EventLoopRunner()
    runner.stop()

    awaitable = coro()
    with pytest.raises(RuntimeError, match='Loop has been stopped.'):
        runner.run_coroutine(awaitable)

    # Run the coroutine so we don't get a coroutine never awaited warning.
    with EventLoopRunner() as runner:
        runner.run_coroutine(awaitable).result()


def test_event_loop_shared() -> None:
    async def coro() -> None:
        pass

    with EventLoopRunner() as runner1:
        with EventLoopRunner(loop=runner1.loop) as runner2:
            runner1.run_coroutine(coro()).result()
            runner2.run_coroutine(coro()).result()
