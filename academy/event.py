from __future__ import annotations

import asyncio
import contextlib


async def wait_event_async(
    *events: asyncio.Event,
    timeout: float | None = None,
) -> asyncio.Event:
    """Wait for the first async event to be set.

    Args:
        events: One or more events to wait on.
        timeout: Maximum number of seconds to wait for an event to finish.

    Returns:
        The first event to finish.

    Raises:
        TimeoutError: If no event finished within `timeout` seconds.
    """
    tasks = {
        asyncio.create_task(
            event.wait(),
            name=f'or-event-waiter-{i}',
        ): event
        for i, event in enumerate(events)
    }
    done, pending = await asyncio.wait(
        tasks.keys(),
        timeout=timeout,
        return_when=asyncio.FIRST_COMPLETED,
    )
    for task in pending:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    if len(done) == 0:
        raise TimeoutError(f'No events were set within {timeout} seconds.')

    return tasks[done.pop()]
