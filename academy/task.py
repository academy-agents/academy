from __future__ import annotations

import asyncio
import logging
from collections.abc import Coroutine
from typing import Any

from academy.debug import get_academy_debug
from academy.logging import execute_and_log_traceback

logger = logging.getLogger(__name__)


class SafeTaskExitError(Exception):
    """Exception that can be raised inside a task to safely exit it."""

    pass


def _exit_on_error(task: asyncio.Task[Any]) -> None:
    """Task callback that raises SystemExit on task exception."""
    if (
        not task.cancelled()
        and task.exception() is not None
        and not isinstance(task.exception(), SafeTaskExitError)
        and get_academy_debug()
    ):
        logger.error(
            f'Exception in critical task (name="{task.get_name()}"): '
            f'{task.exception()!r}',
        )
        raise SystemExit(1)


def spawn_guarded_background_task(
    coro: Coroutine[Any, Any, Any],
    *,
    name: str,
    log_exception: bool = True,
) -> asyncio.Task[Any]:
    """Run a coroutine safely in the background.

    Launches the coroutine as an asyncio task. Optionally attaches logging
    to any exception raised, and/or exiting on any exception raised.
    Tasks can raise [`SafeTaskExit`][academy.task.SafeTaskExitError]
    to signal the task is finished but should not cause a system exit.

    Source: https://stackoverflow.com/questions/62588076

    Args:
        coro: Coroutine to run as task.
        name: name of background task. Unlike asyncio.create_task, this is
            required
        log_exception: Write exception to the log. Set to false if exceptions
            are already logged by coro.

    Returns:
        Asyncio task.
    """
    if log_exception:
        fut = asyncio.ensure_future(coro)
        coro = execute_and_log_traceback(fut)

    task = asyncio.create_task(
        coro,
        name=name,
    )
    task.add_done_callback(_exit_on_error)

    return task
