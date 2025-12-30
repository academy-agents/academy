from __future__ import annotations

import logging
from asyncio import Future
from typing import Any

import academy.observability.examples as loggers
from academy.logging import config

logger = logging.getLogger(__name__)

# extra keys with this prefix will be added to human-readable logs
# when `extra > 1`
ACADEMY_EXTRA_PREFIX = 'academy.'


async def execute_and_log_traceback(
    fut: Future[Any],
) -> Any:
    """Await a future and log any exception..

    Catches any exceptions raised by the coroutine, logs the traceback,
    and re-raises the exception.
    """
    try:
        return await fut
    except Exception:
        logger.exception('Background task raised an exception.')
        raise


def recommended_dev_log_config() -> config.ObservabilityConfig:
    """Returns a log configuration recommended for development use.

    This will configure console logging for academy WARNINGs and worse,
    and make debug-level logs for all Python logging into JSON formatted
    log files in ~/.academy/
    """
    return loggers.MultiLogConfig(
        [loggers.ConsoleLogging(level=logging.WARN), loggers.FilePoolLog()],
    )
