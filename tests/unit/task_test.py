from __future__ import annotations

import asyncio
import contextlib
import logging

import pytest

from academy.exception import SafeTaskExitError
from academy.task import spawn_guarded_background_task


def test_background_task_exits_on_error() -> None:
    async def okay_task() -> None:
        return

    async def safe_task() -> None:
        raise SafeTaskExitError()

    async def bad_task() -> None:
        raise RuntimeError()

    async def run(task) -> None:
        await spawn_guarded_background_task(
            task(),
            name='test-task',
        )

    with (
        contextlib.redirect_stdout(
            None,
        ),
        contextlib.redirect_stderr(None),
    ):
        asyncio.run(run(okay_task))
        with pytest.raises(SafeTaskExitError):
            asyncio.run(run(safe_task))
        with pytest.raises(SystemExit):
            asyncio.run(run(bad_task))


def test_background_task_error_is_logged(caplog) -> None:
    caplog.set_level(logging.ERROR)

    async def bad_task() -> None:
        raise RuntimeError('Oh no!')

    async def run(task) -> None:
        await spawn_guarded_background_task(
            task(),
            name='test-task',
        )

    with (
        contextlib.redirect_stdout(
            None,
        ),
        contextlib.redirect_stderr(None),
    ):
        with pytest.raises(SystemExit):
            asyncio.run(run(bad_task))

    assert any('Traceback' in record.message for record in caplog.records)
    assert any('Oh no!' in record.message for record in caplog.records)
