from __future__ import annotations

import asyncio
import logging
from collections.abc import Coroutine
from typing import Any

from academy.exception import SafeTaskExitError
from academy.logging import execute_and_log_traceback

logger = logging.getLogger(__name__)


def exit_on_error(task: asyncio.Task[Any]) -> None:
    """Task callback that raises SystemExit on task exception."""
    if (
        not task.cancelled()
        and task.exception() is not None
        and not isinstance(task.exception(), SafeTaskExitError)
    ):
        logger.error(
            f'Exception in background task (name="{task.get_name()}"): '
            f'{task.exception()!r}',
        )
        raise SystemExit(1)


def spawn_guarded_background_task(
    coro: Coroutine[Any, Any, Any],
    *,
    name: str,
) -> asyncio.Task[Any]:
    """Run a coroutine safely in the background.

    Launches the coroutine as an asyncio task and sets the done
    callback to [`exit_on_error()`][proxystore.utils.tasks.exit_on_error].
    This is "safe" because it will ensure exceptions inside the task get logged
    and cause the program to exit. Otherwise, background tasks that are not
    awaited may not have their exceptions raised such that programs hang with
    no notice of the exception that caused the hang.

    Tasks can raise [`SafeTaskExit`][proxystore.utils.tasks.SafeTaskExitError]
    to signal the task is finished but should not cause a system exit.

    Source: https://stackoverflow.com/questions/62588076

    Args:
        coro: Coroutine to run as task.
        name: name of background task. Unlike asyncio.create_task, this is
            required

    Returns:
        Asyncio task handle.
    """
    task = asyncio.create_task(
        execute_and_log_traceback(coro),
        name=name,
    )
    task.add_done_callback(exit_on_error)
    return task
