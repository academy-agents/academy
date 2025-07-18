from __future__ import annotations

import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest

from academy.agent import action
from academy.agent import Agent
from academy.agent import event
from academy.agent import loop
from academy.agent import timer
from academy.context import ActionContext
from academy.context import AgentContext
from academy.exception import AgentNotInitializedError
from academy.exchange import LocalExchangeTransport
from academy.exchange import UserExchangeClient
from academy.identifier import AgentId
from testing.agents import EmptyAgent
from testing.agents import IdentityAgent
from testing.agents import WaitAgent
from testing.constant import TEST_SLEEP_INTERVAL
from testing.constant import TEST_WAIT_TIMEOUT


def test_initialize_base_type_error() -> None:
    error = 'The Agent type cannot be instantiated directly'
    with pytest.raises(TypeError, match=error):
        Agent()


@pytest.mark.asyncio
async def test_agent_context_initialized_ok(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    agent = EmptyAgent()

    async def _handler(_: Any) -> None:  # pragma: no cover
        pass

    registration = await exchange_client.register_agent(EmptyAgent)
    factory = exchange_client.factory()
    async with await factory.create_agent_client(
        registration,
        _handler,
    ) as client:
        context = AgentContext(
            agent_id=client.client_id,
            exchange_client=client,
            executor=None,  # type: ignore[arg-type]
            shutdown_event=asyncio.Event(),
        )
        agent._agent_set_context(context)

        assert agent.agent_context is context
        assert agent.agent_id is context.agent_id
        assert agent.agent_exchange_client is context.exchange_client

        agent.agent_shutdown()
        assert context.shutdown_event.is_set()


@pytest.mark.asyncio
async def test_agent_context_initialized_error() -> None:
    agent = EmptyAgent()

    with pytest.raises(AgentNotInitializedError):
        _ = agent.agent_context
    with pytest.raises(AgentNotInitializedError):
        _ = agent.agent_id
    with pytest.raises(AgentNotInitializedError):
        _ = agent.agent_exchange_client
    with pytest.raises(AgentNotInitializedError):
        agent.agent_shutdown()


@pytest.mark.asyncio
async def test_agent_run_sync() -> None:
    class SyncAgent(Agent):
        def add_sync(self, a: int, b: int) -> int:
            return a + b

        async def add_async(self, a: int, b: int) -> int:
            return await self.agent_run_sync(self.add_sync, a, b)

    agent = SyncAgent()
    with ThreadPoolExecutor() as executor:
        context: AgentContext[SyncAgent] = AgentContext(
            agent_id=AgentId.new(),
            exchange_client=None,  # type: ignore[arg-type]
            executor=executor,
            shutdown_event=asyncio.Event(),
        )
        agent._agent_set_context(context)

        assert await agent.add_async(0, 1) == 1


@pytest.mark.asyncio
async def test_agent_run_sync_overloaded_warning(caplog) -> None:
    agent = EmptyAgent()
    with ThreadPoolExecutor(max_workers=1) as executor:
        context: AgentContext[EmptyAgent] = AgentContext(
            agent_id=AgentId.new(),
            exchange_client=None,  # type: ignore[arg-type]
            executor=executor,
            shutdown_event=asyncio.Event(),
        )
        agent._agent_set_context(context)

        with caplog.at_level(logging.WARNING):
            tasks = tuple(
                asyncio.create_task(
                    agent.agent_run_sync(time.sleep, TEST_SLEEP_INTERVAL),
                )
                for _ in range(8)
            )
            await asyncio.wait(tasks)

        assert 'sync function "sleep" is waiting for a worker' in caplog.text


@pytest.mark.asyncio
async def test_agent_empty() -> None:
    agent = EmptyAgent()
    await agent.agent_on_startup()

    assert isinstance(agent, EmptyAgent)
    assert isinstance(str(agent), str)
    assert isinstance(repr(agent), str)

    assert len(agent._agent_actions()) == 0
    assert len(agent._agent_loops()) == 0

    await agent.agent_on_shutdown()


@pytest.mark.asyncio
async def test_agent_ignore_property_attributes() -> None:
    class Example(Agent):
        @property
        def bad(self) -> str:  # pragma: no cover
            raise RuntimeError('Property was accessed!')

    agent = Example()
    attributes = set(agent._agent_attributes())
    assert len(attributes) == 0


@pytest.mark.asyncio
async def test_agent_actions() -> None:
    agent = IdentityAgent()
    await agent.agent_on_startup()

    actions = agent._agent_actions()
    assert set(actions) == {'identity'}

    assert await agent.identity(1) == 1

    await agent.agent_on_shutdown()


@pytest.mark.asyncio
async def test_agent_loops() -> None:
    agent = WaitAgent()
    await agent.agent_on_startup()

    loops = agent._agent_loops()
    assert set(loops) == {'wait'}

    shutdown = asyncio.Event()
    shutdown.set()
    await agent.wait(shutdown)

    await agent.agent_on_shutdown()


@pytest.mark.asyncio
async def test_agent_event() -> None:
    class _Event(Agent):
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

    agent = _Event()

    loops = agent._agent_loops()
    assert set(loops) == {'bad_event', 'missing_event', 'run'}

    shutdown = asyncio.Event()

    with pytest.raises(AttributeError, match='missing'):
        await agent.missing_event(shutdown)
    with pytest.raises(TypeError, match='bad'):
        await agent.bad_event(shutdown)

    task: asyncio.Task[None] = asyncio.create_task(agent.run(shutdown))

    for _ in range(5):
        assert not agent.ran.is_set()
        agent.event.set()
        await asyncio.wait_for(agent.ran.wait(), timeout=TEST_WAIT_TIMEOUT)
        agent.ran.clear()

    shutdown.set()
    await asyncio.wait_for(task, timeout=TEST_WAIT_TIMEOUT)


@pytest.mark.asyncio
async def test_agent_timer() -> None:
    class _Timer(Agent):
        def __init__(self) -> None:
            self.count = 0

        @timer(TEST_SLEEP_INTERVAL)
        async def counter(self) -> None:
            self.count += 1

    agent = _Timer()

    loops = agent._agent_loops()
    assert set(loops) == {'counter'}

    shutdown = asyncio.Event()
    task: asyncio.Task[None] = asyncio.create_task(agent.counter(shutdown))

    await asyncio.sleep(TEST_SLEEP_INTERVAL * 10)
    shutdown.set()

    await asyncio.wait_for(task, timeout=TEST_WAIT_TIMEOUT)


def test_agent_action_decorator_usage_ok() -> None:
    class _TestAgent(Agent):
        @action
        async def action1(self) -> None: ...

        @action()
        async def action2(self) -> None: ...

        @action(context=True)
        async def action3(self, *, context: ActionContext) -> None: ...

    agent = _TestAgent()
    assert len(agent._agent_actions()) == 3  # noqa: PLR2004


def test_agent_action_decorator_usage_error() -> None:
    class _TestAgent(Agent):
        async def missing_arg(self) -> None: ...
        async def pos_only(self, context: ActionContext, /) -> None: ...

    with pytest.raises(
        TypeError,
        match='Action method "missing_arg" must accept a "context"',
    ):
        action(context=True)(_TestAgent.missing_arg)

    with pytest.raises(
        TypeError,
        match='The "context" argument to action method "pos_only"',
    ):
        action(context=True)(_TestAgent.pos_only)


def test_agent_action_decorator_name_clash_ok() -> None:
    class _TestAgent(Agent):
        async def ping(self) -> None: ...

    action(allow_protected_name=True)(_TestAgent.ping)


def test_agent_action_decorator_name_clash_error() -> None:
    class _TestAgent(Agent):
        async def action(self) -> None: ...
        async def ping(self) -> None: ...
        async def shutdown(self) -> None: ...

    with pytest.warns(
        UserWarning,
        match='The name of the decorated method is "action" which clashes',
    ):
        action(_TestAgent.action)

    with pytest.warns(
        UserWarning,
        match='The name of the decorated method is "ping" which clashes',
    ):
        action(_TestAgent.ping)

    with pytest.warns(
        UserWarning,
        match='The name of the decorated method is "shutdown" which clashes',
    ):
        action(_TestAgent.shutdown)


class A(Agent): ...


class B(Agent): ...


class C(A): ...


class D(A, B): ...


def test_agent_mro() -> None:
    assert Agent._agent_mro() == ()
    assert A._agent_mro() == (f'{__name__}.A',)
    assert B._agent_mro() == (f'{__name__}.B',)
    assert C._agent_mro() == (f'{__name__}.C', f'{__name__}.A')
    assert D._agent_mro() == (
        f'{__name__}.D',
        f'{__name__}.A',
        f'{__name__}.B',
    )


def test_invalid_loop_signature() -> None:
    class BadAgent(Agent):
        async def loop(self) -> None: ...

    with pytest.raises(TypeError, match='Signature of loop method "loop"'):
        loop(BadAgent.loop)  # type: ignore[arg-type]
