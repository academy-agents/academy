from __future__ import annotations

import asyncio
import sys
from typing import Any
from unittest import mock

import pytest

from academy.agent import _bind_behavior_handles
from academy.agent import Agent
from academy.agent import AgentRunConfig
from academy.behavior import action
from academy.behavior import Behavior
from academy.behavior import loop
from academy.exchange import UserExchangeClient
from academy.exchange.local import LocalExchangeFactory
from academy.exchange.transport import MailboxStatus
from academy.handle import Handle
from academy.handle import HandleDict
from academy.handle import HandleList
from academy.handle import ProxyHandle
from academy.handle import RemoteHandle
from academy.handle import UnboundRemoteHandle
from academy.identifier import AgentId
from academy.message import ActionRequest
from academy.message import ActionResponse
from academy.message import PingRequest
from academy.message import PingResponse
from academy.message import ShutdownRequest
from testing.behavior import CounterBehavior
from testing.behavior import EmptyBehavior
from testing.behavior import ErrorBehavior
from testing.constant import TEST_THREAD_JOIN_TIMEOUT


class SignalingBehavior(Behavior):
    def __init__(self) -> None:
        super().__init__()
        self.setup_event = asyncio.Event()
        self.shutdown_event = asyncio.Event()

    async def on_setup(self) -> None:
        self.setup_event.set()

    async def on_shutdown(self) -> None:
        self.shutdown_event.set()

    @loop
    async def shutdown_immediately(self, shutdown: asyncio.Event) -> None:
        shutdown.set()


@pytest.mark.asyncio
async def test_agent_run(exchange: UserExchangeClient[Any]) -> None:
    registration = await exchange.register_agent(SignalingBehavior)
    agent = Agent(
        SignalingBehavior(),
        exchange_factory=exchange.factory(),
        registration=registration,
    )
    assert isinstance(repr(agent), str)
    assert isinstance(str(agent), str)

    await agent.run()

    with pytest.raises(RuntimeError, match='Agent has already been shutdown.'):
        await agent.run()

    assert agent.behavior.setup_event.is_set()
    assert agent.behavior.shutdown_event.is_set()


@pytest.mark.asyncio
async def test_agent_run_in_task(exchange: UserExchangeClient[Any]) -> None:
    registration = await exchange.register_agent(SignalingBehavior)
    agent = Agent(
        SignalingBehavior(),
        exchange_factory=exchange.factory(),
        registration=registration,
    )

    task = asyncio.create_task(agent.run(), name='test-agent-run-in-task')
    await task

    assert agent.behavior.setup_event.is_set()
    assert agent.behavior.shutdown_event.is_set()


@pytest.mark.asyncio
async def test_agent_shutdown_without_terminate(
    exchange: UserExchangeClient[Any],
) -> None:
    registration = await exchange.register_agent(SignalingBehavior)
    agent = Agent(
        SignalingBehavior(),
        exchange_factory=exchange.factory(),
        registration=registration,
        config=AgentRunConfig(terminate_on_success=False),
    )
    await agent.run()
    assert agent._expected_shutdown
    assert await exchange.status(agent.agent_id) == MailboxStatus.ACTIVE


@pytest.mark.asyncio
async def test_agent_startup_failure(
    exchange: UserExchangeClient[Any],
) -> None:
    registration = await exchange.register_agent(SignalingBehavior)
    agent = Agent(
        SignalingBehavior(),
        exchange_factory=exchange.factory(),
        registration=registration,
    )

    with mock.patch.object(agent, '_start', side_effect=Exception('Oops!')):
        with pytest.raises(Exception, match='Oops!'):
            await agent.run()

    assert not agent.behavior.setup_event.is_set()
    assert not agent.behavior.shutdown_event.is_set()


class LoopFailureBehavior(Behavior):
    @loop
    async def bad1(self, shutdown: asyncio.Event) -> None:
        raise RuntimeError('Loop failure 1.')

    @loop
    async def bad2(self, shutdown: asyncio.Event) -> None:
        raise RuntimeError('Loop failure 2.')


@pytest.mark.asyncio
async def test_loop_failure_triggers_shutdown(
    exchange: UserExchangeClient[Any],
) -> None:
    registration = await exchange.register_agent(LoopFailureBehavior)
    agent = Agent(
        LoopFailureBehavior(),
        exchange_factory=exchange.factory(),
        registration=registration,
    )

    if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
        # In Python 3.11 and later, all exceptions are raised in a group.
        with pytest.raises(ExceptionGroup) as exc_info:  # noqa: F821
            await asyncio.wait_for(agent.run(), timeout=1)
        assert len(exc_info.value.exceptions) == 2  # noqa: PLR2004
    else:  # pragma: <3.11 cover
        # In Python 3.10 and older, only the first error will be raised.
        with pytest.raises(RuntimeError, match='Loop failure'):
            await asyncio.wait_for(agent.run(), timeout=1)


@pytest.mark.asyncio
async def test_loop_failure_ignore_shutdown(
    exchange: UserExchangeClient[Any],
) -> None:
    registration = await exchange.register_agent(LoopFailureBehavior)
    agent = Agent(
        LoopFailureBehavior(),
        exchange_factory=exchange.factory(),
        registration=registration,
        config=AgentRunConfig(shutdown_on_loop_error=False),
    )

    task = asyncio.create_task(
        agent.run(),
        name='test-loop-failure-ignore-shutdown',
    )
    await agent._started_event.wait()

    # Should timeout because agent did not shutdown after loop errors
    done, pending = await asyncio.wait({task}, timeout=0.001)
    assert len(done) == 0
    agent.signal_shutdown()

    # Loop errors raised on shutdown
    if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
        # In Python 3.11 and later, all exceptions are raised in a group.
        with pytest.raises(ExceptionGroup) as exc_info:  # noqa: F821
            await task
        assert len(exc_info.value.exceptions) == 2  # noqa: PLR2004
    else:  # pragma: <3.11 cover
        # In Python 3.10 and older, only the first error will be raised.
        with pytest.raises(RuntimeError, match='Loop failure'):
            await task


@pytest.mark.asyncio
async def test_agent_shutdown_message(
    exchange: UserExchangeClient[Any],
) -> None:
    registration = await exchange.register_agent(EmptyBehavior)

    agent = Agent(
        EmptyBehavior(),
        exchange_factory=exchange.factory(),
        registration=registration,
    )
    task = asyncio.create_task(agent.run(), name='test-agent-shutdown-message')
    await agent._started_event.wait()

    shutdown = ShutdownRequest(src=exchange.client_id, dest=agent.agent_id)
    await exchange.send(shutdown)
    await asyncio.wait_for(task, timeout=TEST_THREAD_JOIN_TIMEOUT)


@pytest.mark.asyncio
async def test_agent_ping_message(
    local_exchange_factory: LocalExchangeFactory,
) -> None:
    async with await local_exchange_factory.create_user_client(
        start_listener=False,
    ) as exchange:
        registration = await exchange.register_agent(EmptyBehavior)

        agent = Agent(
            EmptyBehavior(),
            exchange_factory=exchange.factory(),
            registration=registration,
        )
        task = asyncio.create_task(agent.run(), name='test-agent-ping-message')
        await agent._started_event.wait()

        ping = PingRequest(src=exchange.client_id, dest=agent.agent_id)
        await exchange.send(ping)
        message = await exchange._transport.recv()
        assert isinstance(message, PingResponse)

        shutdown = ShutdownRequest(src=exchange.client_id, dest=agent.agent_id)
        await exchange.send(shutdown)
        await asyncio.wait_for(task, timeout=TEST_THREAD_JOIN_TIMEOUT)


@pytest.mark.asyncio
async def test_agent_action_message(
    local_exchange_factory: LocalExchangeFactory,
) -> None:
    async with await local_exchange_factory.create_user_client(
        start_listener=False,
    ) as exchange:
        registration = await exchange.register_agent(CounterBehavior)

        agent = Agent(
            CounterBehavior(),
            exchange_factory=exchange.factory(),
            registration=registration,
        )
        task = asyncio.create_task(
            agent.run(),
            name='test-agent-action-message',
        )
        await agent._started_event.wait()

        value = 42
        request = ActionRequest(
            src=exchange.client_id,
            dest=agent.agent_id,
            action='add',
            pargs=(value,),
        )
        await exchange.send(request)
        message = await exchange._transport.recv()
        assert isinstance(message, ActionResponse)
        assert message.exception is None
        assert message.result is None

        request = ActionRequest(
            src=exchange.client_id,
            dest=agent.agent_id,
            action='count',
        )
        await exchange.send(request)
        message = await exchange._transport.recv()
        assert isinstance(message, ActionResponse)
        assert message.exception is None
        assert message.result == value

        shutdown = ShutdownRequest(src=exchange.client_id, dest=agent.agent_id)
        await exchange.send(shutdown)
        await asyncio.wait_for(task, timeout=TEST_THREAD_JOIN_TIMEOUT)


@pytest.mark.asyncio
async def test_agent_action_message_error(
    local_exchange_factory: LocalExchangeFactory,
) -> None:
    async with await local_exchange_factory.create_user_client(
        start_listener=False,
    ) as exchange:
        registration = await exchange.register_agent(ErrorBehavior)

        agent = Agent(
            ErrorBehavior(),
            exchange_factory=exchange.factory(),
            registration=registration,
        )
        task = asyncio.create_task(
            agent.run(),
            name='test-agent-action-message-error',
        )
        await agent._started_event.wait()

        request = ActionRequest(
            src=exchange.client_id,
            dest=agent.agent_id,
            action='fails',
        )
        await exchange.send(request)
        message = await exchange._transport.recv()
        assert isinstance(message, ActionResponse)
        assert isinstance(message.exception, RuntimeError)
        assert 'This action always fails.' in str(message.exception)

        shutdown = ShutdownRequest(src=exchange.client_id, dest=agent.agent_id)
        await exchange.send(shutdown)
        await asyncio.wait_for(task, timeout=TEST_THREAD_JOIN_TIMEOUT)


@pytest.mark.asyncio
async def test_agent_action_message_unknown(
    local_exchange_factory: LocalExchangeFactory,
) -> None:
    async with await local_exchange_factory.create_user_client(
        start_listener=False,
    ) as exchange:
        registration = await exchange.register_agent(EmptyBehavior)

        agent = Agent(
            EmptyBehavior(),
            exchange_factory=exchange.factory(),
            registration=registration,
        )
        task = asyncio.create_task(
            agent.run(),
            name='test-agent-action-message-unknown',
        )
        await agent._started_event.wait()

        request = ActionRequest(
            src=exchange.client_id,
            dest=agent.agent_id,
            action='null',
        )
        await exchange.send(request)
        message = await exchange._transport.recv()
        assert isinstance(message, ActionResponse)
        assert isinstance(message.exception, AttributeError)
        assert 'null' in str(message.exception)

        shutdown = ShutdownRequest(src=exchange.client_id, dest=agent.agent_id)
        await exchange.send(shutdown)
        await asyncio.wait_for(task, timeout=TEST_THREAD_JOIN_TIMEOUT)


@pytest.mark.asyncio
async def test_behavior_handles_bind(
    exchange: UserExchangeClient[Any],
) -> None:
    class _TestBehavior(Behavior):
        def __init__(
            self,
            handle: Handle[EmptyBehavior],
            proxy: ProxyHandle[EmptyBehavior],
        ) -> None:
            super().__init__()
            self.direct = handle
            self.proxy = proxy
            self.sequence = HandleList([handle])
            self.mapping = HandleDict({'x': handle})

    factory = exchange.factory()
    registration = await exchange.register_agent(_TestBehavior)
    proxy_handle = ProxyHandle(EmptyBehavior())
    unbound_handle = UnboundRemoteHandle(
        (await exchange.register_agent(EmptyBehavior)).agent_id,
    )

    async def _request_handler(_: Any) -> None:  # pragma: no cover
        pass

    async with await factory.create_agent_client(
        registration,
        _request_handler,
    ) as agent_client:
        behavior = _TestBehavior(unbound_handle, proxy_handle)
        await _bind_behavior_handles(behavior, agent_client)

        assert behavior.proxy is proxy_handle
        assert behavior.direct.client_id == agent_client.client_id
        for handle in behavior.sequence:
            assert handle.client_id == agent_client.client_id
        for handle in behavior.mapping.values():
            assert handle.client_id == agent_client.client_id


class HandleBindingBehavior(Behavior):
    def __init__(
        self,
        unbound: UnboundRemoteHandle[EmptyBehavior],
        agent_bound: RemoteHandle[EmptyBehavior],
        self_bound: RemoteHandle[EmptyBehavior],
    ) -> None:
        self.unbound = unbound
        self.agent_bound = agent_bound
        self.self_bound = self_bound

    async def on_setup(self) -> None:
        assert isinstance(self.unbound, RemoteHandle)
        assert isinstance(self.agent_bound, RemoteHandle)
        assert isinstance(self.self_bound, RemoteHandle)

        assert isinstance(self.unbound.client_id, AgentId)
        assert self.unbound.client_id == self.agent_bound.client_id
        assert self.unbound.client_id == self.self_bound.client_id


@pytest.mark.asyncio
async def test_agent_run_bind_handles(
    exchange: UserExchangeClient[Any],
) -> None:
    factory = exchange.factory()
    main_agent_reg = await exchange.register_agent(HandleBindingBehavior)
    remote_agent1_reg = await exchange.register_agent(EmptyBehavior)
    remote_agent1_id = remote_agent1_reg.agent_id
    remote_agent2_reg = await exchange.register_agent(EmptyBehavior)

    async def _request_handler(_: Any) -> None:  # pragma: no cover
        pass

    main_agent_client = await factory.create_agent_client(
        main_agent_reg,
        _request_handler,
    )
    remote_agent2_client = await factory.create_agent_client(
        remote_agent2_reg,
        _request_handler,
    )

    behavior = HandleBindingBehavior(
        unbound=UnboundRemoteHandle(remote_agent1_id),
        agent_bound=RemoteHandle(remote_agent2_client, remote_agent1_id),
        self_bound=RemoteHandle(main_agent_client, remote_agent1_id),
    )

    # The agent is going to create it's own exchange client so we'd end up
    # with two clients for the same agent. Close this one as we just used
    # it to mock a handle already bound to the agent.
    await main_agent_client.close()

    agent = Agent(
        behavior,
        exchange_factory=factory,
        registration=main_agent_reg,
    )
    task = asyncio.create_task(agent.run(), name='test-agent-run-bind-handles')
    await agent._started_event.wait()

    # The self-bound remote handles should be ignored.
    assert agent._exchange_client is not None
    assert len(agent._exchange_client._handles) == 2  # noqa: PLR2004

    agent.signal_shutdown()
    await task
    await remote_agent2_client.close()


class RunBehavior(Behavior):
    def __init__(self, doubler: Handle[DoubleBehavior]) -> None:
        super().__init__()
        self.doubler = doubler

    async def on_shutdown(self) -> None:
        assert isinstance(self.doubler, RemoteHandle)
        await self.doubler.shutdown()

    @action
    async def run(self, value: int) -> int:
        return await (await self.doubler.action('double', value))


class DoubleBehavior(Behavior):
    @action
    async def double(self, value: int) -> int:
        return 2 * value


@pytest.mark.asyncio
async def test_agent_to_agent_handles(local_exchange_factory) -> None:
    factory = local_exchange_factory
    async with await factory.create_user_client() as client:
        runner_info = await client.register_agent(RunBehavior)
        doubler_info = await client.register_agent(DoubleBehavior)

        runner_handle = await client.get_handle(runner_info.agent_id)
        doubler_handle = await client.get_handle(doubler_info.agent_id)

        runner_behavior = RunBehavior(doubler_handle)
        doubler_behavior = DoubleBehavior()

        runner_agent = Agent(
            runner_behavior,
            exchange_factory=factory,
            registration=runner_info,
        )
        doubler_agent = Agent(
            doubler_behavior,
            exchange_factory=factory,
            registration=doubler_info,
        )

        runner_task = asyncio.create_task(
            runner_agent.run(),
            name='test-agent-to-agent-handles-runner',
        )
        doubler_task = asyncio.create_task(
            doubler_agent.run(),
            name='test-agent-to-agent-handles-doubler',
        )

        future = await runner_handle.action('run', 1)
        assert await future == 2  # noqa: PLR2004

        await runner_handle.shutdown()

        await asyncio.wait_for(runner_task, timeout=TEST_THREAD_JOIN_TIMEOUT)
        await asyncio.wait_for(doubler_task, timeout=TEST_THREAD_JOIN_TIMEOUT)

        await runner_handle.close()
        await runner_handle.close()


class ShutdownBehavior(Behavior):
    @action
    async def end(self) -> None:
        self.agent_shutdown()


@pytest.mark.asyncio
async def test_agent_self_termination(
    exchange: UserExchangeClient[Any],
) -> None:
    registration = await exchange.register_agent(ShutdownBehavior)
    agent = Agent(
        ShutdownBehavior(),
        exchange_factory=exchange.factory(),
        registration=registration,
    )

    task = asyncio.create_task(agent.run(), name='test-agent-self-termination')
    await agent._started_event.wait()
    await agent.action('end', (), {})
    await asyncio.wait_for(task, timeout=TEST_THREAD_JOIN_TIMEOUT)
