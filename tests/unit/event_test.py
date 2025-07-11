from __future__ import annotations

import asyncio
import threading

import pytest

from academy.event import or_event
from academy.event import wait_event_async
from testing.constant import TEST_SLEEP_INTERVAL


def test_or_event() -> None:
    a = threading.Event()
    b = threading.Event()

    c = or_event(a, b)

    assert not c.is_set()
    a.set()
    assert c.is_set()
    a.clear()
    assert not c.is_set()

    a.set()
    b.set()
    assert c.is_set()

    # Both events must be cleared
    a.clear()
    assert c.is_set()
    b.clear()
    assert not c.is_set()


@pytest.mark.asyncio
async def test_wait_single_event_set_immediately():
    event = asyncio.Event()
    event.set()
    result = await wait_event_async(event)
    assert result is event


@pytest.mark.asyncio
async def test_wait_single_event_set_later():
    event = asyncio.Event()

    async def set_event_later():
        await asyncio.sleep(TEST_SLEEP_INTERVAL)
        event.set()

    task = asyncio.create_task(set_event_later())
    result = await wait_event_async(event)
    await task
    assert result is event


@pytest.mark.asyncio
async def test_wait_multiple_events_first_one_set():
    event1 = asyncio.Event()
    event2 = asyncio.Event()
    event1.set()
    result = await wait_event_async(event1, event2)
    assert result is event1


@pytest.mark.asyncio
async def test_wait_multiple_events_second_one_set_first():
    event1 = asyncio.Event()
    event2 = asyncio.Event()

    async def set_second():
        await asyncio.sleep(TEST_SLEEP_INTERVAL)
        event2.set()

    task = asyncio.create_task(set_second())
    result = await wait_event_async(event1, event2)
    await task
    assert result is event2


@pytest.mark.asyncio
async def test_wait_timeout_raises():
    event = asyncio.Event()
    with pytest.raises(TimeoutError):
        await wait_event_async(event, timeout=TEST_SLEEP_INTERVAL)
