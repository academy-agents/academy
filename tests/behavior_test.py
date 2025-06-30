from __future__ import annotations

import asyncio
from typing import Any

import pytest

from academy.behavior import Behavior
from academy.behavior import event
from academy.behavior import loop
from academy.behavior import timer
from academy.context import AgentContext
from academy.exception import AgentNotInitializedError
from academy.exchange import UserExchangeClient
from academy.exchange.local import LocalExchangeTransport
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
async def test_agent_context_initialized_ok(
    exchange: UserExchangeClient[LocalExchangeTransport],
) -> None:
    behavior = EmptyBehavior()

    async def _handler(_: Any) -> None:  # pragma: no cover
        pass

    registration = await exchange.register_agent(EmptyBehavior)
    factory = exchange.factory()
    async with await factory.create_agent_client(
        registration,
        _handler,
    ) as client:
        context = AgentContext(
            agent_id=client.client_id,
            exchange_client=client,
            shutdown_event=asyncio.Event(),
        )
        behavior._agent_set_context(context)

        assert behavior.agent_context is context
        assert behavior.agent_id is context.agent_id
        assert behavior.agent_exchange_client is context.exchange_client

        behavior.agent_shutdown()
        assert context.shutdown_event.is_set()


@pytest.mark.asyncio
async def test_agent_context_initialized_error() -> None:
    behavior = EmptyBehavior()

    with pytest.raises(AgentNotInitializedError):
        _ = behavior.agent_context
    with pytest.raises(AgentNotInitializedError):
        _ = behavior.agent_id
    with pytest.raises(AgentNotInitializedError):
        _ = behavior.agent_exchange_client
    with pytest.raises(AgentNotInitializedError):
        behavior.agent_shutdown()


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

    task: asyncio.Task[None] = asyncio.create_task(behavior.run(shutdown))

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
    task: asyncio.Task[None] = asyncio.create_task(behavior.counter(shutdown))

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
        loop(BadBehavior.loop)  # type: ignore[arg-type]
