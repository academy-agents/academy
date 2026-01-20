from __future__ import annotations

import contextlib
import logging
from asyncio import Future
from collections.abc import Generator
from typing import Any

from academy.logging.config import ObservabilityConfig
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


# ID to reference count. ID is e.g. uuid-like as a string
# it is a string, so string comparison rules apply, not
# uuid comparison rules, and it can be any unique-enough ID,
# like a message-id format string too.
initialized_log_contexts: dict[str, int] = {}


@contextlib.contextmanager
def log_context(c: ObservabilityConfig) -> Generator[None, None, None]:
    """Context manager for initializing and uninitializing a log."""
    logger.info(
        f'BENC: entering log_context context manager, with log config {c}',
    )
    if c.uuid not in initialized_log_contexts:
        initialized_log_contexts[c.uuid] = 0

    assert c.uuid in initialized_log_contexts

    if initialized_log_contexts[c.uuid] > 0:
        initialized_log_contexts[c.uuid] += 1
    else:
        initialized_log_contexts[c.uuid] += 1
        uninit = c.init_logging()
        assert callable(uninit), (
            f'Log config {c} should have returned a callable, not {uninit}'
        )
        logger.info('BENC: yielding to inner block')

    yield

    logger.info('BENC: uninitializing')

    # buggy use of reference counting might have removed this from the
    # structure, if unpaired references have been dropped.
    assert c.uuid in initialized_log_contexts

    if initialized_log_contexts[c.uuid] > 1:
        initialized_log_contexts[c.uuid] -= 1
    else:
        initialized_log_contexts[c.uuid] -= 1
        uninit()
        logger.info('BENC: leaving log_context context manager')
        del initialized_log_contexts[c.uuid]
