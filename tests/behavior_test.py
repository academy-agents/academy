from __future__ import annotations

import asyncio
import time

import pytest

from academy.behavior import Behavior
from academy.behavior import event
from academy.behavior import loop
from academy.behavior import timer
from academy.handle import Handle
from academy.handle import HandleDict
from academy.handle import HandleList
from academy.handle import ProxyHandle
from testing.behavior import EmptyBehavior
from testing.behavior import HandleBehavior
from testing.behavior import IdentityBehavior
from testing.behavior import WaitBehavior
from testing.constant import TEST_LOOP_SLEEP
from testing.constant import TEST_THREAD_JOIN_TIMEOUT


def test_initialize_base_type_error() -> None:
    error = 'The Behavior type cannot be instantiated directly'
    with pytest.raises(TypeError, match=error):
        Behavior()


@pytest.mark.asyncio
async def test_behavior_empty() -> None:
    behavior = EmptyBehavior()
    await behavior.on_setup()

    assert isinstance(behavior, EmptyBehavior)
    assert isinstance(str(behavior), str)
    assert isinstance(repr(behavior), str)

    assert len(behavior.behavior_actions()) == 0
    assert len(behavior.behavior_loops()) == 0
    assert len(behavior.behavior_handles()) == 0

    await behavior.on_shutdown()


@pytest.mark.asyncio
async def test_behavior_actions() -> None:
    behavior = IdentityBehavior()
    await behavior.on_setup()

    actions = behavior.behavior_actions()
    assert set(actions) == {'identity'}

    assert await behavior.identity(1) == 1

    await behavior.on_shutdown()


@pytest.mark.asyncio
async def test_behavior_loops() -> None:
    behavior = WaitBehavior()
    await behavior.on_setup()

    loops = behavior.behavior_loops()
    assert set(loops) == {'wait'}

    shutdown = asyncio.Event()
    shutdown.set()
    await behavior.wait(shutdown)

    await behavior.on_shutdown()


@pytest.mark.asyncio
async def test_behavior_event() -> None:
    class _Event(Behavior):
        def __init__(self) -> None:
            self.event = asyncio.Event()
            self.ran = asyncio.Event()
            self.bad = 42

        @event('event')
        async def run(self) -> None:
            self.ran.set()

        @event('missing')
        async def missing_event(self) -> None: ...

        @event('bad')
        async def bad_event(self) -> None: ...

    behavior = _Event()

    loops = behavior.behavior_loops()
    assert set(loops) == {'bad_event', 'missing_event', 'run'}

    shutdown = asyncio.Event()

    with pytest.raises(AttributeError, match='missing'):
        await behavior.missing_event(shutdown)
    with pytest.raises(TypeError, match='bad'):
        await behavior.bad_event(shutdown)

    task = asyncio.create_task(behavior.run(shutdown))

    for _ in range(5):
        assert not behavior.ran.is_set()
        behavior.event.set()
        await asyncio.wait_for(behavior.ran.wait(), timeout=1)
        behavior.ran.clear()

    shutdown.set()
    await asyncio.wait_for(task, timeout=TEST_THREAD_JOIN_TIMEOUT)


@pytest.mark.asyncio
async def test_behavior_timer() -> None:
    class _Timer(Behavior):
        def __init__(self) -> None:
            self.count = 0

        @timer(TEST_LOOP_SLEEP)
        async def counter(self) -> None:
            self.count += 1

    behavior = _Timer()

    loops = behavior.behavior_loops()
    assert set(loops) == {'counter'}

    shutdown = asyncio.Event()
    task = asyncio.create_task(behavior.counter(shutdown))

    await asyncio.sleep(TEST_LOOP_SLEEP * 10)
    shutdown.set()

    await asyncio.wait_for(task, timeout=TEST_THREAD_JOIN_TIMEOUT)


@pytest.mark.asyncio
async def test_behavior_handles() -> None:
    handle = ProxyHandle(EmptyBehavior())
    behavior = HandleBehavior(handle)
    await behavior.on_setup()

    handles = behavior.behavior_handles()
    assert set(handles) == {'handle'}

    await behavior.on_shutdown()


@pytest.mark.asyncio
async def test_behavior_handles_bind() -> None:
    class _TestBehavior(Behavior):
        def __init__(self, handle: Handle[EmptyBehavior]) -> None:
            self.direct = handle
            self.sequence = HandleList([handle])
            self.mapping = HandleDict({'x': handle})

    expected_binds = 3
    bind_count = 0

    async def _bind_handle(handle: Handle[EmptyBehavior]) -> Handle[EmptyBehavior]:
        nonlocal bind_count
        bind_count += 1
        return handle

    handle = ProxyHandle(EmptyBehavior())
    behavior = _TestBehavior(handle)
    await behavior.on_setup()

    await behavior.behavior_handles_bind(_bind_handle)
    assert bind_count == expected_binds

    await behavior.on_shutdown()


class A(Behavior): ...


class B(Behavior): ...


class C(A): ...


class D(A, B): ...


def test_behavior_mro() -> None:
    assert Behavior.behavior_mro() == ()
    assert A.behavior_mro() == (f'{__name__}.A',)
    assert B.behavior_mro() == (f'{__name__}.B',)
    assert C.behavior_mro() == (f'{__name__}.C', f'{__name__}.A')
    assert D.behavior_mro() == (
        f'{__name__}.D',
        f'{__name__}.A',
        f'{__name__}.B',
    )


def test_invalid_loop_signature() -> None:
    class BadBehavior(Behavior):
        async def loop(self) -> None: ...

    with pytest.raises(TypeError, match='Signature of loop method "loop"'):
        loop(BadBehavior.loop)
