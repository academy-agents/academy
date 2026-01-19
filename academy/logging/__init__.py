from __future__ import annotations

import contextlib
import logging
from asyncio import Future
from typing import Any

from academy.logging.recommended import init_logging
from academy.logging.recommended import recommended_dev_log_config


logger = logging.getLogger(__name__)

# extra keys with this prefix will be added to human-readable logs
# when `extra > 1`
ACADEMY_EXTRA_PREFIX = 'academy.'


async def execute_and_log_traceback(
    fut: Future[Any],
) -> Any:
    """Await a future and log any exception..

    Warning:
        For developers. Other functions rely on the first await call to be on
        the wrapped future to that task cancellation can be properly caught
        in the wrapped task.

    Catches any exceptions raised by the coroutine, logs the traceback,
    and re-raises the exception.
    """
    try:
        return await fut
    except Exception:
        logger.exception('Background task raised an exception.')
        raise


@contextlib.contextmanager
def log_context(c):
    print("BENC: entering log_context context manager")
    yield
    print("BENC: leaving log_context context manager")
