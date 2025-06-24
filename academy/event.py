from __future__ import annotations

import asyncio
import contextlib
import threading
from typing import Any
from typing import Callable


def _or_set(event: Any) -> None:
    event._set()
    event.changed()


def _or_clear(event: Any) -> None:
    event._clear()
    event.changed()


def _orify(
    event: threading.Event,
    changed_callback: Callable[[], None],
) -> None:
    event._set = event.set  # type: ignore[attr-defined]
    event._clear = event.clear  # type: ignore[attr-defined]
    event.changed = changed_callback  # type: ignore[attr-defined]
    event.set = lambda: _or_set(event)  # type: ignore[method-assign]
    event.clear = lambda: _or_clear(event)  # type: ignore[method-assign]


def or_event(*events: threading.Event) -> threading.Event:
    """Create a combined event that is set when any input events are set.

    Note:
        The creator can wait on the combined event, but must still check
        each individual event to see which was set.

    Warning:
        This works by dynamically replacing methods on the inputs events
        with custom methods that trigger callbacks.

    Note:
        Based on this Stack Overflow
        [answer](https://stackoverflow.com/a/12320352).

    Args:
        events: One or more events to combine.

    Returns:
        A single event that is set when any of the input events is set.
    """
    combined = threading.Event()

    def changed() -> None:
        bools = [e.is_set() for e in events]
        if any(bools):
            combined.set()
        else:
            combined.clear()

    for e in events:
        _orify(e, changed)

    changed()
    return combined


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
        asyncio.create_task(event.wait(), name=f'or-event-waiter-{i}'): event
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
