from __future__ import annotations

import asyncio
import sys
from typing import Any
from unittest import mock

import pytest

from academy.agent import action
from academy.agent import Agent
from academy.agent import loop
from academy.context import ActionContext
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
from academy.identifier import EntityId
from academy.message import ActionRequest
from academy.message import ActionResponse
from academy.message import PingRequest
from academy.message import PingResponse
from academy.message import ShutdownRequest
from academy.runtime import _bind_agent_handles
from academy.runtime import Runtime
from academy.runtime import RuntimeConfig
from testing.agents import CounterAgent
from testing.agents import EmptyAgent
from testing.agents import ErrorAgent
from testing.constant import TEST_THREAD_JOIN_TIMEOUT


class SignalingAgent(Agent):
    def __init__(self) -> None:
        super().__init__()
        self.startup_event = asyncio.Event()
        self.shutdown_event = asyncio.Event()

    async def agent_on_startup(self) -> None:
        self.startup_event.set()

    async def agent_on_shutdown(self) -> None:
        self.shutdown_event.set()

    @loop
    async def shutdown_immediately(self, shutdown: asyncio.Event) -> None:
        shutdown.set()


@pytest.mark.asyncio
async def test_agent_run(exchange: UserExchangeClient[Any]) -> None:
    registration = await exchange.register_agent(SignalingAgent)
    runtime = Runtime(
        SignalingAgent(),
        exchange_factory=exchange.factory(),
        registration=registration,
    )
    assert isinstance(repr(runtime), str)
    assert isinstance(str(runtime), str)

    await runtime.run()

    with pytest.raises(RuntimeError, match='Agent has already been shutdown.'):
        await runtime.run()

    assert runtime.agent.startup_event.is_set()
    assert runtime.agent.shutdown_event.is_set()


@pytest.mark.asyncio
async def test_agent_run_in_task(exchange: UserExchangeClient[Any]) -> None:
    registration = await exchange.register_agent(SignalingAgent)
    runtime = Runtime(
        SignalingAgent(),
        exchange_factory=exchange.factory(),
        registration=registration,
    )

    task = asyncio.create_task(runtime.run(), name='test-agent-run-in-task')
    await task

    assert runtime.agent.startup_event.is_set()
    assert runtime.agent.shutdown_event.is_set()


@pytest.mark.asyncio
async def test_agent_shutdown_without_terminate(
    exchange: UserExchangeClient[Any],
) -> None:
    registration = await exchange.register_agent(SignalingAgent)
    runtime = Runtime(
        SignalingAgent(),
        exchange_factory=exchange.factory(),
        registration=registration,
        config=RuntimeConfig(terminate_on_success=False),
    )
    await runtime.run()
    assert runtime._shutdown_options.expected_shutdown
    assert await exchange.status(runtime.agent_id) == MailboxStatus.ACTIVE


@pytest.mark.asyncio
async def test_agent_shutdown_terminate_override(
    local_exchange_factory: LocalExchangeFactory,
) -> None:
    async with await local_exchange_factory.create_user_client(
        start_listener=False,
    ) as exchange:
        registration = await exchange.register_agent(EmptyAgent)

        runtime = Runtime(
            EmptyAgent(),
            exchange_factory=exchange.factory(),
            registration=registration,
            config=RuntimeConfig(
                terminate_on_success=False,
                terminate_on_error=False,
            ),
        )
        task = asyncio.create_task(
            runtime.run(),
            name='test-agent-shutdown-terminate-override',
        )
        await runtime._started_event.wait()

        shutdown = ShutdownRequest(
            src=exchange.client_id,
            dest=runtime.agent_id,
            terminate=True,
        )
        await exchange.send(shutdown)
        await asyncio.wait_for(task, timeout=TEST_THREAD_JOIN_TIMEOUT)
        assert (
            await exchange.status(runtime.agent_id) == MailboxStatus.TERMINATED
        )


@pytest.mark.asyncio
async def test_agent_startup_failure(
    exchange: UserExchangeClient[Any],
) -> None:
    registration = await exchange.register_agent(SignalingAgent)
    runtime = Runtime(
        SignalingAgent(),
        exchange_factory=exchange.factory(),
        registration=registration,
    )

    with mock.patch.object(runtime, '_start', side_effect=Exception('Oops!')):
        with pytest.raises(Exception, match='Oops!'):
            await runtime.run()

    assert not runtime.agent.startup_event.is_set()
    assert not runtime.agent.shutdown_event.is_set()


class LoopFailureAgent(Agent):
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
    registration = await exchange.register_agent(LoopFailureAgent)
    runtime = Runtime(
        LoopFailureAgent(),
        exchange_factory=exchange.factory(),
        registration=registration,
    )

    if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
        # In Python 3.11 and later, all exceptions are raised in a group.
        with pytest.raises(ExceptionGroup) as exc_info:  # noqa: F821
            await asyncio.wait_for(runtime.run(), timeout=1)
        assert len(exc_info.value.exceptions) == 2  # noqa: PLR2004
    else:  # pragma: <3.11 cover
        # In Python 3.10 and older, only the first error will be raised.
        with pytest.raises(RuntimeError, match='Loop failure'):
            await asyncio.wait_for(runtime.run(), timeout=1)


@pytest.mark.asyncio
async def test_loop_failure_ignore_shutdown(
    exchange: UserExchangeClient[Any],
) -> None:
    registration = await exchange.register_agent(LoopFailureAgent)
    runtime = Runtime(
        LoopFailureAgent(),
        exchange_factory=exchange.factory(),
        registration=registration,
        config=RuntimeConfig(shutdown_on_loop_error=False),
    )

    task = asyncio.create_task(
        runtime.run(),
        name='test-loop-failure-ignore-shutdown',
    )
    await runtime._started_event.wait()

    # Should timeout because agent did not shutdown after loop errors
    done, pending = await asyncio.wait({task}, timeout=0.001)
    assert len(done) == 0
    runtime.signal_shutdown()

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
    registration = await exchange.register_agent(EmptyAgent)

    runtime = Runtime(
        EmptyAgent(),
        exchange_factory=exchange.factory(),
        registration=registration,
    )
    task = asyncio.create_task(
        runtime.run(),
        name='test-agent-shutdown-message',
    )
    await runtime._started_event.wait()

    shutdown = ShutdownRequest(src=exchange.client_id, dest=runtime.agent_id)
    await exchange.send(shutdown)
    await asyncio.wait_for(task, timeout=TEST_THREAD_JOIN_TIMEOUT)


@pytest.mark.asyncio
async def test_agent_ping_message(
    local_exchange_factory: LocalExchangeFactory,
) -> None:
    async with await local_exchange_factory.create_user_client(
        start_listener=False,
    ) as exchange:
        registration = await exchange.register_agent(EmptyAgent)

        runtime = Runtime(
            EmptyAgent(),
            exchange_factory=exchange.factory(),
            registration=registration,
        )
        task = asyncio.create_task(
            runtime.run(),
            name='test-agent-ping-message',
        )
        await runtime._started_event.wait()

        ping = PingRequest(src=exchange.client_id, dest=runtime.agent_id)
        await exchange.send(ping)
        message = await exchange._transport.recv()
        assert isinstance(message, PingResponse)

        shutdown = ShutdownRequest(
            src=exchange.client_id,
            dest=runtime.agent_id,
        )
        await exchange.send(shutdown)
        await asyncio.wait_for(task, timeout=TEST_THREAD_JOIN_TIMEOUT)


@pytest.mark.asyncio
async def test_agent_action_message(
    local_exchange_factory: LocalExchangeFactory,
) -> None:
    async with await local_exchange_factory.create_user_client(
        start_listener=False,
    ) as exchange:
        registration = await exchange.register_agent(CounterAgent)

        runtime = Runtime(
            CounterAgent(),
            exchange_factory=exchange.factory(),
            registration=registration,
        )
        task = asyncio.create_task(
            runtime.run(),
            name='test-agent-action-message',
        )
        await runtime._started_event.wait()

        value = 42
        request = ActionRequest(
            src=exchange.client_id,
            dest=runtime.agent_id,
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
            dest=runtime.agent_id,
            action='count',
        )
        await exchange.send(request)
        message = await exchange._transport.recv()
        assert isinstance(message, ActionResponse)
        assert message.exception is None
        assert message.result == value

        shutdown = ShutdownRequest(
            src=exchange.client_id,
            dest=runtime.agent_id,
        )
        await exchange.send(shutdown)
        await asyncio.wait_for(task, timeout=TEST_THREAD_JOIN_TIMEOUT)


@pytest.mark.asyncio
async def test_agent_action_message_error(
    local_exchange_factory: LocalExchangeFactory,
) -> None:
    async with await local_exchange_factory.create_user_client(
        start_listener=False,
    ) as exchange:
        registration = await exchange.register_agent(ErrorAgent)

        runtime = Runtime(
            ErrorAgent(),
            exchange_factory=exchange.factory(),
            registration=registration,
        )
        task = asyncio.create_task(
            runtime.run(),
            name='test-agent-action-message-error',
        )
        await runtime._started_event.wait()

        request = ActionRequest(
            src=exchange.client_id,
            dest=runtime.agent_id,
            action='fails',
        )
        await exchange.send(request)
        message = await exchange._transport.recv()
        assert isinstance(message, ActionResponse)
        assert isinstance(message.exception, RuntimeError)
        assert 'This action always fails.' in str(message.exception)

        shutdown = ShutdownRequest(
            src=exchange.client_id,
            dest=runtime.agent_id,
        )
        await exchange.send(shutdown)
        await asyncio.wait_for(task, timeout=TEST_THREAD_JOIN_TIMEOUT)


@pytest.mark.asyncio
async def test_agent_action_message_unknown(
    local_exchange_factory: LocalExchangeFactory,
) -> None:
    async with await local_exchange_factory.create_user_client(
        start_listener=False,
    ) as exchange:
        registration = await exchange.register_agent(EmptyAgent)

        runtime = Runtime(
            EmptyAgent(),
            exchange_factory=exchange.factory(),
            registration=registration,
        )
        task = asyncio.create_task(
            runtime.run(),
            name='test-agent-action-message-unknown',
        )
        await runtime._started_event.wait()

        request = ActionRequest(
            src=exchange.client_id,
            dest=runtime.agent_id,
            action='null',
        )
        await exchange.send(request)
        message = await exchange._transport.recv()
        assert isinstance(message, ActionResponse)
        assert isinstance(message.exception, AttributeError)
        assert 'null' in str(message.exception)

        shutdown = ShutdownRequest(
            src=exchange.client_id,
            dest=runtime.agent_id,
        )
        await exchange.send(shutdown)
        await asyncio.wait_for(task, timeout=TEST_THREAD_JOIN_TIMEOUT)


@pytest.mark.asyncio
async def test_agent_handles_bind(
    exchange: UserExchangeClient[Any],
) -> None:
    class _TestAgent(Agent):
        def __init__(
            self,
            handle: Handle[EmptyAgent],
            proxy: ProxyHandle[EmptyAgent],
        ) -> None:
            super().__init__()
            self.direct = handle
            self.proxy = proxy
            self.sequence = HandleList([handle])
            self.mapping = HandleDict({'x': handle})

    factory = exchange.factory()
    registration = await exchange.register_agent(_TestAgent)
    proxy_handle = ProxyHandle(EmptyAgent())
    unbound_handle = UnboundRemoteHandle(
        (await exchange.register_agent(EmptyAgent)).agent_id,
    )

    async def _request_handler(_: Any) -> None:  # pragma: no cover
        pass

    async with await factory.create_agent_client(
        registration,
        _request_handler,
    ) as agent_client:
        agent = _TestAgent(unbound_handle, proxy_handle)
        _bind_agent_handles(agent, agent_client)

        assert agent.proxy is proxy_handle
        assert agent.direct.client_id == agent_client.client_id
        for handle in agent.sequence:
            assert handle.client_id == agent_client.client_id
        for handle in agent.mapping.values():
            assert handle.client_id == agent_client.client_id


class HandleBindingAgent(Agent):
    def __init__(
        self,
        unbound: UnboundRemoteHandle[EmptyAgent],
        agent_bound: RemoteHandle[EmptyAgent],
        self_bound: RemoteHandle[EmptyAgent],
    ) -> None:
        self.unbound = unbound
        self.agent_bound = agent_bound
        self.self_bound = self_bound

    async def agent_on_startup(self) -> None:
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
    main_agent_reg = await exchange.register_agent(HandleBindingAgent)
    remote_agent1_reg = await exchange.register_agent(EmptyAgent)
    remote_agent1_id = remote_agent1_reg.agent_id
    remote_agent2_reg = await exchange.register_agent(EmptyAgent)

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

    agent = HandleBindingAgent(
        unbound=UnboundRemoteHandle(remote_agent1_id),
        agent_bound=RemoteHandle(remote_agent2_client, remote_agent1_id),
        self_bound=RemoteHandle(main_agent_client, remote_agent1_id),
    )

    # The agent is going to create it's own exchange client so we'd end up
    # with two clients for the same agent. Close this one as we just used
    # it to mock a handle already bound to the agent.
    await main_agent_client.close()

    runtime = Runtime(
        agent,
        exchange_factory=factory,
        registration=main_agent_reg,
    )
    task = asyncio.create_task(
        runtime.run(),
        name='test-agent-run-bind-handles',
    )
    await runtime._started_event.wait()

    # The self-bound remote handles should be ignored.
    assert runtime._exchange_client is not None
    assert len(runtime._exchange_client._handles) == 2  # noqa: PLR2004

    runtime.signal_shutdown()
    await task
    await remote_agent2_client.close()


class RunAgent(Agent):
    def __init__(self, doubler: Handle[DoubleAgent]) -> None:
        super().__init__()
        self.doubler = doubler

    async def agent_on_shutdown(self) -> None:
        assert isinstance(self.doubler, RemoteHandle)
        await self.doubler.shutdown()

    @action
    async def run(self, value: int) -> int:
        return await (await self.doubler.action('double', value))


class DoubleAgent(Agent):
    @action
    async def double(self, value: int) -> int:
        return 2 * value


@pytest.mark.asyncio
async def test_agent_to_agent_handles(local_exchange_factory) -> None:
    factory = local_exchange_factory
    async with await factory.create_user_client() as client:
        runner_info = await client.register_agent(RunAgent)
        doubler_info = await client.register_agent(DoubleAgent)

        runner_handle = client.get_handle(runner_info.agent_id)
        doubler_handle = client.get_handle(doubler_info.agent_id)

        runner_agent = RunAgent(doubler_handle)
        doubler_agent = DoubleAgent()

        runner_runtime = Runtime(
            runner_agent,
            exchange_factory=factory,
            registration=runner_info,
        )
        doubler_runtime = Runtime(
            doubler_agent,
            exchange_factory=factory,
            registration=doubler_info,
        )

        runner_task = asyncio.create_task(
            runner_runtime.run(),
            name='test-agent-to-agent-handles-runner',
        )
        doubler_task = asyncio.create_task(
            doubler_runtime.run(),
            name='test-agent-to-agent-handles-doubler',
        )

        future = await runner_handle.action('run', 1)
        assert await future == 2  # noqa: PLR2004

        await runner_handle.shutdown()

        await asyncio.wait_for(runner_task, timeout=TEST_THREAD_JOIN_TIMEOUT)
        await asyncio.wait_for(doubler_task, timeout=TEST_THREAD_JOIN_TIMEOUT)

        await runner_handle.close()
        await runner_handle.close()


class ShutdownAgent(Agent):
    @action
    async def end(self) -> None:
        self.agent_shutdown()


@pytest.mark.asyncio
async def test_agent_self_termination(
    exchange: UserExchangeClient[Any],
) -> None:
    registration = await exchange.register_agent(ShutdownAgent)
    runtime = Runtime(
        ShutdownAgent(),
        exchange_factory=exchange.factory(),
        registration=registration,
    )

    task = asyncio.create_task(
        runtime.run(),
        name='test-agent-self-termination',
    )
    await runtime._started_event.wait()
    await runtime.action('end', AgentId.new(), args=(), kwargs={})
    await asyncio.wait_for(task, timeout=TEST_THREAD_JOIN_TIMEOUT)


class ContextAgent(Agent):
    @action(context=True)
    async def call(
        self,
        source_id: EntityId,
        *,
        context: ActionContext,
    ) -> None:
        assert source_id == context.source_id


@pytest.mark.asyncio
async def test_agent_action_context(
    exchange: UserExchangeClient[Any],
) -> None:
    registration = await exchange.register_agent(ShutdownAgent)
    runtime = Runtime(
        ContextAgent(),
        exchange_factory=exchange.factory(),
        registration=registration,
    )

    task = asyncio.create_task(runtime.run(), name='test-agent-action-context')
    await runtime._started_event.wait()
    await runtime.action(
        'call',
        exchange.client_id,
        args=(exchange.client_id,),
        kwargs={},
    )
    runtime.signal_shutdown()
    await asyncio.wait_for(task, timeout=TEST_THREAD_JOIN_TIMEOUT)
