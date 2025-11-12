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
from academy.debug import set_academy_debug
from academy.exception import ActionCancelledError
from academy.exchange import ExchangeClient
from academy.exchange import LocalExchangeTransport
from academy.exchange import UserExchangeClient
from academy.exchange.transport import MailboxStatus
from academy.handle import Handle
from academy.handle import ProxyHandle
from academy.identifier import AgentId
from academy.identifier import EntityId
from academy.message import ActionRequest
from academy.message import ActionResponse
from academy.message import ErrorResponse
from academy.message import Message
from academy.message import PingRequest
from academy.message import ShutdownRequest
from academy.message import SuccessResponse
from academy.runtime import Runtime
from academy.runtime import RuntimeConfig
from testing.agents import CounterAgent
from testing.agents import EmptyAgent
from testing.agents import ErrorAgent
from testing.constant import TEST_SLEEP_INTERVAL
from testing.constant import TEST_WAIT_TIMEOUT


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
async def test_runtime_context_manager(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    registration = await exchange_client.register_agent(SignalingAgent)
    async with Runtime(
        EmptyAgent(),
        exchange_factory=exchange_client.factory(),
        registration=registration,
    ) as runtime:
        assert isinstance(repr(runtime), str)
        assert isinstance(str(runtime), str)


@pytest.mark.asyncio
async def test_runtime_run_until_complete(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    registration = await exchange_client.register_agent(SignalingAgent)
    runtime = Runtime(
        SignalingAgent(),
        exchange_factory=exchange_client.factory(),
        registration=registration,
    )
    await runtime.run_until_complete()

    with pytest.raises(
        RuntimeError,
        match=r'Agent has already been shutdown\.',
    ):
        await runtime.run_until_complete()

    assert runtime.agent.startup_event.is_set()
    assert runtime.agent.shutdown_event.is_set()


@pytest.mark.asyncio
async def test_runtime_run_until_complete_as_task(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    registration = await exchange_client.register_agent(SignalingAgent)
    runtime = Runtime(
        SignalingAgent(),
        exchange_factory=exchange_client.factory(),
        registration=registration,
    )

    task = asyncio.create_task(
        runtime.run_until_complete(),
        name='test-runtime-run-until-complete-at-task',
    )
    await task

    assert runtime.agent.startup_event.is_set()
    assert runtime.agent.shutdown_event.is_set()


@pytest.mark.asyncio
async def test_runtime_shutdown_without_terminate(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    registration = await exchange_client.register_agent(SignalingAgent)
    runtime = Runtime(
        SignalingAgent(),
        exchange_factory=exchange_client.factory(),
        registration=registration,
        config=RuntimeConfig(terminate_on_success=False),
    )
    await runtime.run_until_complete()
    assert runtime._shutdown_options.expected_shutdown
    assert (
        await exchange_client.status(runtime.agent_id) == MailboxStatus.ACTIVE
    )


@pytest.mark.asyncio
async def test_runtime_shutdown_terminate_override(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    registration = await exchange_client.register_agent(EmptyAgent)

    async with Runtime(
        EmptyAgent(),
        exchange_factory=exchange_client.factory(),
        registration=registration,
        config=RuntimeConfig(
            terminate_on_success=False,
            terminate_on_error=False,
        ),
    ) as runtime:
        runtime.signal_shutdown(expected=True, terminate=True)
        await runtime.wait_shutdown(timeout=TEST_WAIT_TIMEOUT)

    assert (
        await exchange_client.status(runtime.agent_id)
        == MailboxStatus.TERMINATED
    )


@pytest.mark.asyncio
async def test_runtime_startup_failure(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    registration = await exchange_client.register_agent(SignalingAgent)
    runtime = Runtime(
        SignalingAgent(),
        exchange_factory=exchange_client.factory(),
        registration=registration,
    )

    with mock.patch.object(runtime, '_start', side_effect=Exception('Oops!')):
        with pytest.raises(Exception, match='Oops!'):
            await runtime.run_until_complete()

    assert not runtime.agent.startup_event.is_set()
    assert not runtime.agent.shutdown_event.is_set()


@pytest.mark.asyncio
async def test_runtime_wait_shutdown_timeout(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    registration = await exchange_client.register_agent(SignalingAgent)
    async with Runtime(
        EmptyAgent(),
        exchange_factory=exchange_client.factory(),
        registration=registration,
    ) as runtime:
        with pytest.raises(TimeoutError):
            await runtime.wait_shutdown(timeout=TEST_SLEEP_INTERVAL)


class LoopFailureAgent(Agent):
    @loop
    async def bad1(self, shutdown: asyncio.Event) -> None:
        raise RuntimeError('Loop failure 1.')

    @loop
    async def bad2(self, shutdown: asyncio.Event) -> None:
        raise RuntimeError('Loop failure 2.')


@pytest.mark.parametrize('raise_errors', (True, False))
@pytest.mark.asyncio
async def test_runtime_loop_error_causes_shutdown(
    raise_errors: bool,
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    registration = await exchange_client.register_agent(LoopFailureAgent)
    runtime = Runtime(
        LoopFailureAgent(),
        exchange_factory=exchange_client.factory(),
        registration=registration,
        config=RuntimeConfig(raise_loop_errors_on_shutdown=raise_errors),
    )

    if not raise_errors:
        await asyncio.wait_for(
            runtime.run_until_complete(),
            timeout=TEST_WAIT_TIMEOUT,
        )
    elif sys.version_info >= (3, 11):  # pragma: >=3.11 cover
        # In Python 3.11 and later, all exceptions are raised in a group.
        with pytest.raises(ExceptionGroup) as exc_info:  # noqa: F821
            await asyncio.wait_for(
                runtime.run_until_complete(),
                timeout=TEST_WAIT_TIMEOUT,
            )
        assert len(exc_info.value.exceptions) == 2  # noqa: PLR2004
    else:  # pragma: <3.11 cover
        # In Python 3.10 and older, only the first error will be raised.
        with pytest.raises(RuntimeError, match='Loop failure'):
            await asyncio.wait_for(
                runtime.run_until_complete(),
                timeout=TEST_WAIT_TIMEOUT,
            )


@pytest.mark.asyncio
async def test_runtime_loop_error_without_shutdown(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    registration = await exchange_client.register_agent(LoopFailureAgent)
    runtime = Runtime(
        LoopFailureAgent(),
        exchange_factory=exchange_client.factory(),
        registration=registration,
        config=RuntimeConfig(shutdown_on_loop_error=False),
    )

    task = asyncio.create_task(
        runtime.run_until_complete(),
        name='test-runtime-loop-error-without-shutdown',
    )
    await runtime._started_event.wait()

    # Should timeout because agent did not shutdown after loop errors
    done, _ = await asyncio.wait({task}, timeout=TEST_SLEEP_INTERVAL)
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
async def test_runtime_shutdown_message(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    registration = await exchange_client.register_agent(EmptyAgent)

    async with Runtime(
        EmptyAgent(),
        exchange_factory=exchange_client.factory(),
        registration=registration,
    ) as runtime:
        shutdown = Message.create(
            src=exchange_client.client_id,
            dest=runtime.agent_id,
            body=ShutdownRequest(),
        )
        await exchange_client.send(shutdown)
        await runtime.wait_shutdown(timeout=TEST_WAIT_TIMEOUT)


class InfiniteAgent(Agent):
    @loop
    async def wait(self, shutdown: asyncio.Event) -> None:
        while True:
            await asyncio.sleep(5)


@pytest.mark.asyncio
async def test_runtime_cancel_loop(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    registration = await exchange_client.register_agent(InfiniteAgent)

    runtime = Runtime(
        InfiniteAgent(),
        exchange_factory=exchange_client.factory(),
        registration=registration,
    )

    task = asyncio.create_task(
        runtime.run_until_complete(),
        name='test-runtime-cancel-loop',
    )
    await runtime._started_event.wait()

    # Should cancel loop without raising error
    runtime.signal_shutdown()
    await task


@pytest.mark.asyncio
async def test_runtime_ping_message(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    registration = await exchange_client.register_agent(EmptyAgent)
    # Cancel listener so test can intercept agent responses
    await exchange_client._stop_listener_task()

    async with Runtime(
        EmptyAgent(),
        exchange_factory=exchange_client.factory(),
        registration=registration,
    ) as runtime:
        ping = Message.create(
            src=exchange_client.client_id,
            dest=runtime.agent_id,
            body=PingRequest(),
        )
        await exchange_client.send(ping)
        message = await exchange_client._transport.recv()
        assert isinstance(message.get_body(), SuccessResponse)


@pytest.mark.asyncio
async def test_runtime_action_message(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    registration = await exchange_client.register_agent(CounterAgent)
    # Cancel listener so test can intercept agent responses
    await exchange_client._stop_listener_task()

    async with Runtime(
        CounterAgent(),
        exchange_factory=exchange_client.factory(),
        registration=registration,
    ) as runtime:
        value = 42
        request = Message.create(
            src=exchange_client.client_id,
            dest=runtime.agent_id,
            body=ActionRequest(action='add', pargs=(value,)),
        )
        await exchange_client.send(request)
        message = await exchange_client._transport.recv()
        body = message.get_body()
        assert isinstance(body, ActionResponse)
        assert body.get_result() is None

        request = Message.create(
            src=exchange_client.client_id,
            dest=runtime.agent_id,
            body=ActionRequest(action='count'),
        )
        await exchange_client.send(request)
        message = await exchange_client._transport.recv()
        body = message.get_body()
        assert isinstance(body, ActionResponse)
        assert body.get_result() == value


@pytest.mark.parametrize('cancel', (True, False))
@pytest.mark.asyncio
async def test_runtime_cancel_action_requests_on_shutdown(
    cancel: bool,
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    class NoReturnAgent(Agent):
        @action
        async def sleep(self) -> None:
            await asyncio.sleep(1000 if cancel else TEST_SLEEP_INTERVAL)

    registration = await exchange_client.register_agent(ErrorAgent)
    # Cancel listener so test can intercept agent responses
    await exchange_client._stop_listener_task()

    runtime = Runtime(
        NoReturnAgent(),
        exchange_factory=exchange_client.factory(),
        registration=registration,
        config=RuntimeConfig(cancel_actions_on_shutdown=cancel),
    )
    task = asyncio.create_task(
        runtime.run_until_complete(),
        name='test-runtime-cancel-action-requests-on-shutdown',
    )
    await runtime._started_event.wait()

    request = Message.create(
        src=exchange_client.client_id,
        dest=runtime.agent_id,
        body=ActionRequest(action='sleep'),
    )
    await exchange_client.send(request)

    shutdown = Message.create(
        src=exchange_client.client_id,
        dest=runtime.agent_id,
        body=ShutdownRequest(),
    )
    await exchange_client.send(shutdown)

    message = await exchange_client._transport.recv()
    body = message.get_body()
    if cancel:
        assert isinstance(body, ErrorResponse)
        assert isinstance(body.get_exception(), ActionCancelledError)
    else:
        assert isinstance(body, ActionResponse)
        assert body.get_result() is None

    await asyncio.wait_for(task, timeout=TEST_WAIT_TIMEOUT)


@pytest.mark.asyncio
async def test_runtime_action_message_error(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    registration = await exchange_client.register_agent(ErrorAgent)
    # Cancel listener so test can intercept agent responses
    await exchange_client._stop_listener_task()

    async with Runtime(
        ErrorAgent(),
        exchange_factory=exchange_client.factory(),
        registration=registration,
    ) as runtime:
        request = Message.create(
            src=exchange_client.client_id,
            dest=runtime.agent_id,
            body=ActionRequest(action='fails'),
        )
        await exchange_client.send(request)
        message = await exchange_client._transport.recv()
        body = message.get_body()
        assert isinstance(body, ErrorResponse)
        exception = body.get_exception()
        assert isinstance(exception, RuntimeError)
        assert 'This action always fails.' in str(exception)


@pytest.mark.asyncio
async def test_runtime_action_message_unknown(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    registration = await exchange_client.register_agent(EmptyAgent)
    # Cancel listener so test can intercept agent responses
    await exchange_client._stop_listener_task()

    async with Runtime(
        EmptyAgent(),
        exchange_factory=exchange_client.factory(),
        registration=registration,
    ) as runtime:
        request = Message.create(
            src=exchange_client.client_id,
            dest=runtime.agent_id,
            body=ActionRequest(action='null'),
        )
        await exchange_client.send(request)
        message = await exchange_client._transport.recv()
        body = message.get_body()
        assert isinstance(body, ErrorResponse)
        exception = body.get_exception()
        assert isinstance(exception, AttributeError)
        assert 'null' in str(exception)


@pytest.mark.asyncio
async def test_runtime_delay_actions_and_loops_to_after_startup(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    class ExampleAgent(Agent):
        def __init__(self) -> None:
            self.startup_called = False

        async def agent_on_startup(self) -> None:
            # Simulate some work that yields execution to other scheduled
            # tasks so that the scheduled action tasks gets run concurrently
            # with the startup callback. The action task should wait
            # on the startup sequence to finish and immediately yield back
            # control so the callback can finish
            for _ in range(10):
                await asyncio.sleep(0)
            self.startup_called = True

        @action
        async def check_action(self) -> None:
            assert self.startup_called

        @loop
        async def check_loop(self, shutdown: asyncio.Event) -> None:
            assert self.startup_called

    registration = await exchange_client.register_agent(ExampleAgent)
    # Cancel listener so test can intercept agent responses
    await exchange_client._stop_listener_task()

    # Send action request before starting agent so its immediately
    # available when message listener task starts
    request = Message.create(
        src=exchange_client.client_id,
        dest=registration.agent_id,
        body=ActionRequest(action='check_action'),
    )
    await exchange_client.send(request)

    async with Runtime(
        ExampleAgent(),
        exchange_factory=exchange_client.factory(),
        registration=registration,
    ):
        message = await exchange_client._transport.recv()
        body = message.get_body()
        assert isinstance(body, ActionResponse)
        assert body.get_result() is None


@pytest.mark.asyncio
async def test_agent_exchange_context(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
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
            self.sequence = [handle]
            self.mapping = {'x': handle}

    factory = exchange_client.factory()
    registration = await exchange_client.register_agent(_TestAgent)
    proxy_handle = ProxyHandle(EmptyAgent())
    unbound_handle = Handle(
        (await exchange_client.register_agent(EmptyAgent)).agent_id,
    )

    async def _request_handler(_: Any) -> None:  # pragma: no cover
        pass

    async with await factory.create_agent_client(
        registration,
        _request_handler,
    ) as agent_client:
        agent = _TestAgent(unbound_handle, proxy_handle)
        assert agent.proxy is proxy_handle
        assert isinstance(agent.direct, Handle)
        assert agent.direct.exchange is agent_client
        for handle in agent.sequence:
            assert isinstance(handle, Handle)
            assert handle.exchange is agent_client
        for handle in agent.mapping.values():
            assert isinstance(handle, Handle)
            assert handle.exchange is agent_client


class ShutdownAgent(Agent):
    @action
    async def end(self) -> None:
        self.agent_shutdown()


@pytest.mark.asyncio
async def test_runtime_agent_self_termination(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    registration = await exchange_client.register_agent(ShutdownAgent)

    async with Runtime(
        ShutdownAgent(),
        exchange_factory=exchange_client.factory(),
        registration=registration,
    ) as runtime:
        await runtime.action('end', AgentId.new(), args=(), kwargs={})
        await runtime.wait_shutdown(timeout=TEST_WAIT_TIMEOUT)


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
async def test_runtime_agent_action_context(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    registration = await exchange_client.register_agent(ShutdownAgent)

    async with Runtime(
        ContextAgent(),
        exchange_factory=exchange_client.factory(),
        registration=registration,
    ) as runtime:
        await runtime.action(
            'call',
            exchange_client.client_id,
            args=(exchange_client.client_id,),
            kwargs={},
        )


def test_runtime_background_error(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    async def run():
        registration = await exchange_client.register_agent(EmptyAgent)
        with mock.patch.object(
            ExchangeClient,
            '_listen_for_messages',
        ) as listener:
            listener.side_effect = Exception('Unexpected Exception')

            await Runtime(
                EmptyAgent(),
                exchange_factory=exchange_client.factory(),
                registration=registration,
                config=RuntimeConfig(),
            ).run_until_complete()

    set_academy_debug()
    with pytest.raises(SystemExit):
        asyncio.run(run())
