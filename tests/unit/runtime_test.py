from __future__ import annotations

import asyncio
import dataclasses
import sys
import uuid
from collections.abc import AsyncGenerator
from typing import Any
from unittest import mock

import pytest

from academy.agent import action
from academy.agent import Agent
from academy.agent import loop
from academy.context import ActionContext
from academy.debug import set_academy_debug
from academy.exception import ActionCancelledError
from academy.exception import ActionInvalidStateError
from academy.exception import DeserializationMethodProhibitedError
from academy.exception import RequestForbiddenError
from academy.exchange import ExchangeClient
from academy.exchange import LocalExchangeTransport
from academy.exchange import UserExchangeClient
from academy.exchange.cloud.client import HttpExchangeFactory
from academy.exchange.cloud.client import HttpExchangeTransport
from academy.exchange.transport import MailboxStatus
from academy.handle import Handle
from academy.handle import ProxyHandle
from academy.identifier import AgentId
from academy.identifier import EntityId
from academy.message import ActionRequest
from academy.message import ActionResponse
from academy.message import CancelRequest
from academy.message import ErrorResponse
from academy.message import Header
from academy.message import Message
from academy.message import PingRequest
from academy.message import ShutdownRequest
from academy.message import SuccessResponse
from academy.runtime import Runtime
from academy.runtime import RuntimeConfig
from academy.serialize import allowed_deserializers
from academy.serialize import default_serializer
from academy.serialize import SerializationStrategy
from testing.agents import CounterAgent
from testing.agents import EmptyAgent
from testing.agents import ErrorAgent
from testing.agents import SleepAgent
from testing.constant import TEST_SLEEP_INTERVAL
from testing.constant import TEST_WAIT_TIMEOUT


@dataclasses.dataclass
class _OwnableReg:
    """Minimal registration with an owner for testing."""

    agent_id: AgentId[Any]
    owner: EntityId | None = None
    exchange_type: str = 'local'


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
    listener = exchange_client._transport.listen(TEST_SLEEP_INTERVAL)

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
        message = await anext(listener)
        assert isinstance(message.get_body(), SuccessResponse)


@pytest.mark.asyncio
async def test_runtime_action_message(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    registration = await exchange_client.register_agent(CounterAgent)
    # Cancel listener so test can intercept agent responses
    await exchange_client._stop_listener_task()
    listener = exchange_client._transport.listen(TEST_SLEEP_INTERVAL)

    async with Runtime(
        CounterAgent(),
        exchange_factory=exchange_client.factory(),
        registration=registration,
    ) as runtime:
        value = 42
        request = Message.create(
            src=exchange_client.client_id,
            dest=runtime.agent_id,
            body=ActionRequest(
                action='add',
                pargs=(value,),
                serialization=SerializationStrategy.PICKLE,
            ),
        )
        await exchange_client.send(request)
        message = await anext(listener)

        body = message.get_body()
        assert isinstance(body, ActionResponse)
        assert body.get_result() is None

        request = Message.create(
            src=exchange_client.client_id,
            dest=runtime.agent_id,
            body=ActionRequest(
                action='count',
                serialization=SerializationStrategy.PICKLE,
            ),
        )
        await exchange_client.send(request)
        message = await anext(listener)
        body = message.get_body()
        assert isinstance(body, ActionResponse)
        assert body.get_result() == value


@pytest.mark.asyncio
async def test_runtime_cancel_action_message(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    registration = await exchange_client.register_agent(SleepAgent)
    # Cancel listener so test can intercept agent responses
    await exchange_client._stop_listener_task()
    listener = exchange_client._transport.listen(TEST_SLEEP_INTERVAL)

    async with Runtime(
        SleepAgent(),
        exchange_factory=exchange_client.factory(),
        registration=registration,
    ) as runtime:
        value = 1
        request = Message.create(
            src=exchange_client.client_id,
            dest=runtime.agent_id,
            body=ActionRequest(
                action='sleep',
                pargs=(value,),
                serialization=SerializationStrategy.PICKLE,
            ),
        )
        await exchange_client.send(request)
        cancel_request = Message.create(
            src=exchange_client.client_id,
            dest=runtime.agent_id,
            body=CancelRequest(target_tag=request.tag),
        )
        await exchange_client.send(cancel_request)

        for _ in range(2):
            message = await anext(listener)
            body = message.get_body()
            if message.tag == request.tag:
                assert isinstance(body, ErrorResponse)
                assert isinstance(body.get_exception(), ActionCancelledError)
            elif message.tag == cancel_request.tag:
                assert isinstance(body, SuccessResponse)
            else:  # pragma: no cover
                pytest.fail('Received unexpected message.')


@pytest.mark.asyncio
async def test_runtime_cancel_action_message_invalid(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    registration = await exchange_client.register_agent(SleepAgent)
    # Cancel listener so test can intercept agent responses
    await exchange_client._stop_listener_task()
    listener = exchange_client._transport.listen(TEST_SLEEP_INTERVAL)
    async with Runtime(
        SleepAgent(),
        exchange_factory=exchange_client.factory(),
        registration=registration,
    ) as runtime:
        cancel_request = Message.create(
            src=exchange_client.client_id,
            dest=runtime.agent_id,
            body=CancelRequest(target_tag=uuid.uuid4()),
        )

        await exchange_client.send(cancel_request)
        message = await anext(listener)
        body = message.get_body()
        assert isinstance(body, ErrorResponse)
        assert isinstance(body.get_exception(), ActionInvalidStateError)


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
    listener = exchange_client._transport.listen()

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
        body=ActionRequest(
            action='sleep',
            serialization=SerializationStrategy.PICKLE,
        ),
    )
    await exchange_client.send(request)

    shutdown = Message.create(
        src=exchange_client.client_id,
        dest=runtime.agent_id,
        body=ShutdownRequest(),
    )
    await exchange_client.send(shutdown)

    for _ in range(2):
        message = await anext(listener)
        body = message.get_body()
        if message.tag == shutdown.tag:
            assert isinstance(body, SuccessResponse)
        elif cancel:
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
    listener = exchange_client._transport.listen(TEST_SLEEP_INTERVAL)

    async with Runtime(
        ErrorAgent(),
        exchange_factory=exchange_client.factory(),
        registration=registration,
    ) as runtime:
        request = Message.create(
            src=exchange_client.client_id,
            dest=runtime.agent_id,
            body=ActionRequest(
                action='fails',
                serialization=SerializationStrategy.PICKLE,
            ),
        )
        await exchange_client.send(request)
        message = await anext(listener)
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
    listener = exchange_client._transport.listen(TEST_SLEEP_INTERVAL)
    async with Runtime(
        EmptyAgent(),
        exchange_factory=exchange_client.factory(),
        registration=registration,
    ) as runtime:
        request = Message.create(
            src=exchange_client.client_id,
            dest=runtime.agent_id,
            body=ActionRequest(
                action='null',
                serialization=SerializationStrategy.PICKLE,
            ),
        )
        await exchange_client.send(request)
        message = await anext(listener)
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
    listener = exchange_client._transport.listen(TEST_SLEEP_INTERVAL)

    # Send action request before starting agent so its immediately
    # available when message listener task starts
    request = Message.create(
        src=exchange_client.client_id,
        dest=registration.agent_id,
        body=ActionRequest(
            action='check_action',
            serialization=SerializationStrategy.PICKLE,
        ),
    )
    await exchange_client.send(request)

    async with Runtime(
        ExampleAgent(),
        exchange_factory=exchange_client.factory(),
        registration=registration,
    ):
        message = await anext(listener)
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


@pytest.mark.asyncio
async def test_runtime_uses_default_serializer(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
):
    class _CheckSerializationAgent(Agent):
        async def agent_on_startup(self):
            assert default_serializer.get() == SerializationStrategy.JSON

    registration = await exchange_client.register_agent(
        _CheckSerializationAgent,
    )

    token = default_serializer.set(SerializationStrategy.JSON)
    async with Runtime(
        _CheckSerializationAgent(),
        exchange_factory=exchange_client.factory(),
        registration=registration,
    ) as runtime:
        runtime.signal_shutdown()
    default_serializer.reset(token)


@pytest.mark.asyncio
async def test_runtime_sets_default_serializer(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
):
    class _CheckSerializationAgent(Agent):
        async def agent_on_startup(self):
            assert default_serializer.get() == SerializationStrategy.JSON

    registration = await exchange_client.register_agent(
        _CheckSerializationAgent,
    )
    async with Runtime(
        _CheckSerializationAgent(),
        exchange_factory=exchange_client.factory(),
        registration=registration,
        config=RuntimeConfig(default_serializer=SerializationStrategy.JSON),
    ) as runtime:
        runtime.signal_shutdown()


@pytest.fixture
async def http_exchange_client(
    http_exchange_factory: HttpExchangeFactory,
) -> AsyncGenerator[UserExchangeClient[HttpExchangeTransport]]:
    async with await http_exchange_factory.create_user_client(
        start_listener=False,
    ) as client:
        yield client


@pytest.mark.asyncio
async def test_runtime_respects_allowed_deserializers(
    http_exchange_client: UserExchangeClient[HttpExchangeTransport],
):
    registration = await http_exchange_client.register_agent(SleepAgent)
    listener = http_exchange_client._transport.listen(1)

    async with Runtime(
        SleepAgent(),
        exchange_factory=http_exchange_client.factory(),
        registration=registration,
        config=RuntimeConfig(
            allowed_deserializers={SerializationStrategy.JSON},
        ),
    ) as runtime:
        value = 1
        request = Message.create(
            src=http_exchange_client.client_id,
            dest=runtime.agent_id,
            body=ActionRequest(
                action='sleep',
                pargs=(value,),
                serialization=SerializationStrategy.PICKLE,
            ),
        )
        await http_exchange_client.send(request)
        response = await anext(listener)
        body = response.get_body()
        assert isinstance(body, ErrorResponse)
        token = allowed_deserializers.set({SerializationStrategy.PICKLE})
        assert isinstance(
            body.get_exception(),
            DeserializationMethodProhibitedError,
        )
        allowed_deserializers.reset(token)


@pytest.mark.asyncio
async def test_runtime_uses_result_serialization(
    http_exchange_client: UserExchangeClient[HttpExchangeTransport],
):
    registration = await http_exchange_client.register_agent(SleepAgent)
    listener = http_exchange_client._transport.listen(1)

    async with Runtime(
        SleepAgent(),
        exchange_factory=http_exchange_client.factory(),
        registration=registration,
    ) as runtime:
        value = TEST_SLEEP_INTERVAL
        request = Message.create(
            src=http_exchange_client.client_id,
            dest=runtime.agent_id,
            body=ActionRequest(
                action='sleep',
                pargs=(value,),
                serialization=SerializationStrategy.PICKLE,
                result_serialization=SerializationStrategy.JSON,
            ),
        )
        await http_exchange_client.send(request)
        response = await anext(listener)
        body = response.get_body()

        assert isinstance(body, ActionResponse)
        assert body.serialization == SerializationStrategy.JSON


@pytest.mark.asyncio
async def test_runtime_uses_exception_serialization(
    http_exchange_client: UserExchangeClient[HttpExchangeTransport],
):
    registration = await http_exchange_client.register_agent(SleepAgent)
    # Cancel listener so test can intercept agent responses
    await http_exchange_client._stop_listener_task()
    listener = http_exchange_client._transport.listen(0.1)

    async with Runtime(
        ErrorAgent(),
        exchange_factory=http_exchange_client.factory(),
        registration=registration,
    ) as runtime:
        request = Message.create(
            src=http_exchange_client.client_id,
            dest=runtime.agent_id,
            body=ActionRequest(
                action='fails',
                serialization=SerializationStrategy.PICKLE,
                exception_serialization=SerializationStrategy.JSON,
            ),
        )
        await http_exchange_client.send(request)
        response = await anext(listener)
        _body = response.get_body()


GROUP_A = 'group-a'
GROUP_B = 'group-b'


class GatedAgent(Agent):
    @action(sharing=[GROUP_A])
    async def restricted(self) -> str:
        return 'ok'

    @action(sharing=[GROUP_B])
    async def restricted_b(self) -> str:
        return 'ok'

    @action
    async def open_(self) -> str:
        return 'ok'


def _make_group_request(
    dest: EntityId,
    src: EntityId,
    body: ActionRequest | CancelRequest | ShutdownRequest | PingRequest,
    groups: frozenset[str] = frozenset(),
) -> Message[Any]:
    header = Header(
        src=src,
        dest=dest,
        tag=uuid.uuid4(),
        kind='request',
        groups=groups,
    )
    return Message(header=header, body=body)


async def _listen_on(
    client: UserExchangeClient[LocalExchangeTransport],
) -> AsyncGenerator[Message[Any]]:
    await client._stop_listener_task()
    return client._transport.listen(TEST_WAIT_TIMEOUT)


@pytest.mark.asyncio
async def test_runtime_owner_bypasses_group_check(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    owner_id = exchange_client.client_id
    registration = await exchange_client.register_agent(GatedAgent)
    registration = _OwnableReg(agent_id=registration.agent_id, owner=owner_id)
    listener = await _listen_on(exchange_client)

    async with Runtime(
        GatedAgent(),
        exchange_factory=exchange_client.factory(),
        registration=registration,
    ) as runtime:
        request = _make_group_request(
            runtime.agent_id,
            owner_id,
            ActionRequest(
                action='restricted',
                serialization=SerializationStrategy.PICKLE,
            ),
        )
        await exchange_client.send(request)
        message = await anext(listener)
        assert isinstance(message.get_body(), ActionResponse)

        shutdown = _make_group_request(
            runtime.agent_id,
            owner_id,
            ShutdownRequest(),
        )
        await exchange_client.send(shutdown)
        message = await anext(listener)
        assert isinstance(message.get_body(), SuccessResponse)


@pytest.mark.asyncio
async def test_group_denies_without_membership(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    owner_id = exchange_client.client_id
    factory = exchange_client.factory()
    registration = await exchange_client.register_agent(GatedAgent)
    registration = _OwnableReg(agent_id=registration.agent_id, owner=owner_id)

    async with (
        await factory.create_user_client() as stranger,
        Runtime(
            GatedAgent(),
            exchange_factory=factory,
            registration=registration,
        ) as runtime,
    ):
        listener = await _listen_on(stranger)

        request = _make_group_request(
            runtime.agent_id,
            stranger.client_id,
            ActionRequest(
                action='restricted',
                serialization=SerializationStrategy.PICKLE,
            ),
        )
        await stranger.send(request)
        message = await anext(listener)
        body = message.get_body()
        assert isinstance(body, ErrorResponse)
        assert isinstance(body.get_exception(), RequestForbiddenError)
        exc = body.get_exception()
        assert isinstance(exc, RequestForbiddenError)
        assert exc.mailbox_id == runtime.agent_id
        assert str(runtime.agent_id) in str(exc)


@pytest.mark.asyncio
async def test_group_allows_with_membership(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    owner_id = exchange_client.client_id
    factory = exchange_client.factory()
    registration = await exchange_client.register_agent(GatedAgent)
    registration = _OwnableReg(agent_id=registration.agent_id, owner=owner_id)

    async with (
        await factory.create_user_client() as stranger,
        Runtime(
            GatedAgent(),
            exchange_factory=factory,
            registration=registration,
        ) as runtime,
    ):
        listener = await _listen_on(stranger)

        request = _make_group_request(
            runtime.agent_id,
            stranger.client_id,
            ActionRequest(
                action='restricted',
                serialization=SerializationStrategy.PICKLE,
            ),
            groups=frozenset({GROUP_A}),
        )
        await stranger.send(request)
        message = await anext(listener)
        assert isinstance(message.get_body(), ActionResponse)


@pytest.mark.asyncio
async def test_per_action_sharing_is_isolated(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    owner_id = exchange_client.client_id
    factory = exchange_client.factory()
    registration = await exchange_client.register_agent(GatedAgent)
    registration = _OwnableReg(agent_id=registration.agent_id, owner=owner_id)

    async with (
        await factory.create_user_client() as stranger,
        Runtime(
            GatedAgent(),
            exchange_factory=factory,
            registration=registration,
        ) as runtime,
    ):
        listener = await _listen_on(stranger)

        # GROUP_B sender is forbidden from `restricted` (only GROUP_A).
        request = _make_group_request(
            runtime.agent_id,
            stranger.client_id,
            ActionRequest(
                action='restricted',
                serialization=SerializationStrategy.PICKLE,
            ),
            groups=frozenset({GROUP_B}),
        )
        await stranger.send(request)
        message = await anext(listener)
        body = message.get_body()
        assert isinstance(body, ErrorResponse)
        assert isinstance(body.get_exception(), RequestForbiddenError)

        # Same sender is allowed on `restricted_b` (permits GROUP_B).
        request = _make_group_request(
            runtime.agent_id,
            stranger.client_id,
            ActionRequest(
                action='restricted_b',
                serialization=SerializationStrategy.PICKLE,
            ),
            groups=frozenset({GROUP_B}),
        )
        await stranger.send(request)
        message = await anext(listener)
        assert isinstance(message.get_body(), ActionResponse)


class OwnerOnlyAgent(Agent):
    @action(sharing=[GROUP_A])
    async def shared(self) -> str:
        return 'ok'

    @action(sharing=[])
    async def owner_only(self) -> str:
        return 'ok'


@pytest.mark.asyncio
async def test_explicit_empty_sharing_is_owner_only(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    owner_id = exchange_client.client_id
    factory = exchange_client.factory()
    registration = await exchange_client.register_agent(
        OwnerOnlyAgent,
    )
    registration = _OwnableReg(
        agent_id=registration.agent_id,
        owner=owner_id,
    )
    owner_listener = await _listen_on(exchange_client)

    async with (
        await factory.create_user_client() as member,
        Runtime(
            OwnerOnlyAgent(),
            exchange_factory=factory,
            registration=registration,
        ) as runtime,
    ):
        member_listener = await _listen_on(member)

        # A GROUP_A member may call `shared`...
        request = _make_group_request(
            runtime.agent_id,
            member.client_id,
            ActionRequest(
                action='shared',
                serialization=SerializationStrategy.PICKLE,
            ),
            groups=frozenset({GROUP_A}),
        )
        await member.send(request)
        message = await anext(member_listener)
        assert isinstance(message.get_body(), ActionResponse)

        # ...but not `owner_only`, despite the group membership.
        request = _make_group_request(
            runtime.agent_id,
            member.client_id,
            ActionRequest(
                action='owner_only',
                serialization=SerializationStrategy.PICKLE,
            ),
            groups=frozenset({GROUP_A}),
        )
        await member.send(request)
        message = await anext(member_listener)
        body = message.get_body()
        assert isinstance(body, ErrorResponse)
        assert isinstance(
            body.get_exception(),
            RequestForbiddenError,
        )

        # The owner may call it.
        request = _make_group_request(
            runtime.agent_id,
            owner_id,
            ActionRequest(
                action='owner_only',
                serialization=SerializationStrategy.PICKLE,
            ),
        )
        await exchange_client.send(request)
        message = await anext(owner_listener)
        assert isinstance(message.get_body(), ActionResponse)


@pytest.mark.asyncio
async def test_unknown_action_forbidden_for_unauthorized_sender(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    owner_id = exchange_client.client_id
    factory = exchange_client.factory()
    registration = await exchange_client.register_agent(GatedAgent)
    registration = _OwnableReg(
        agent_id=registration.agent_id,
        owner=owner_id,
    )
    owner_listener = await _listen_on(exchange_client)

    async with (
        await factory.create_user_client() as stranger,
        Runtime(
            GatedAgent(),
            exchange_factory=factory,
            registration=registration,
        ) as runtime,
    ):
        stranger_listener = await _listen_on(stranger)

        # Unauthorized sender probing a nonexistent action must get
        # FORBIDDEN, not "unknown action" (action-name oracle).
        request = _make_group_request(
            runtime.agent_id,
            stranger.client_id,
            ActionRequest(
                action='does_not_exist',
                serialization=SerializationStrategy.PICKLE,
            ),
        )
        await stranger.send(request)
        message = await anext(stranger_listener)
        body = message.get_body()
        assert isinstance(body, ErrorResponse)
        assert isinstance(
            body.get_exception(),
            RequestForbiddenError,
        )

        # The owner still gets the real unknown-action error.
        request = _make_group_request(
            runtime.agent_id,
            owner_id,
            ActionRequest(
                action='does_not_exist',
                serialization=SerializationStrategy.PICKLE,
            ),
        )
        await exchange_client.send(request)
        message = await anext(owner_listener)
        body = message.get_body()
        assert isinstance(body, ErrorResponse)
        assert not isinstance(
            body.get_exception(),
            RequestForbiddenError,
        )


@pytest.mark.asyncio
async def test_undecorated_action_uses_group_union(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    owner_id = exchange_client.client_id
    factory = exchange_client.factory()
    registration = await exchange_client.register_agent(GatedAgent)
    registration = _OwnableReg(agent_id=registration.agent_id, owner=owner_id)

    async with (
        await factory.create_user_client() as stranger,
        Runtime(
            GatedAgent(),
            exchange_factory=factory,
            registration=registration,
        ) as runtime,
    ):
        listener = await _listen_on(stranger)

        # Undecorated action falls back to the agent's permitted-groups
        # union, so GROUP_A grants access.
        request = _make_group_request(
            runtime.agent_id,
            stranger.client_id,
            ActionRequest(
                action='open_',
                serialization=SerializationStrategy.PICKLE,
            ),
            groups=frozenset({GROUP_A}),
        )
        await stranger.send(request)
        message = await anext(listener)
        assert isinstance(message.get_body(), ActionResponse)


@pytest.mark.asyncio
async def test_non_owner_cannot_shutdown(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    owner_id = exchange_client.client_id
    factory = exchange_client.factory()
    registration = await exchange_client.register_agent(GatedAgent)
    registration = _OwnableReg(agent_id=registration.agent_id, owner=owner_id)

    async with (
        await factory.create_user_client() as stranger,
        Runtime(
            GatedAgent(),
            exchange_factory=factory,
            registration=registration,
        ) as runtime,
    ):
        listener = await _listen_on(stranger)

        request = _make_group_request(
            runtime.agent_id,
            stranger.client_id,
            ShutdownRequest(),
        )
        await stranger.send(request)
        message = await anext(listener)
        body = message.get_body()
        assert isinstance(body, ErrorResponse)
        assert isinstance(body.get_exception(), RequestForbiddenError)
        assert not runtime._shutdown_event.is_set()


@pytest.mark.asyncio
async def test_runtime_cancel_allowed_for_requester_or_owner(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    owner_id = exchange_client.client_id
    factory = exchange_client.factory()
    registration = await exchange_client.register_agent(SleepAgent)
    registration = _OwnableReg(agent_id=registration.agent_id, owner=owner_id)
    owner_listener = await _listen_on(exchange_client)

    async with (
        await factory.create_user_client() as requester,
        await factory.create_user_client() as stranger,
        Runtime(
            SleepAgent(),
            exchange_factory=factory,
            registration=registration,
        ) as runtime,
    ):
        requester_listener = await _listen_on(requester)
        stranger_listener = await _listen_on(stranger)

        def build_sleep_request() -> Message[Any]:
            return _make_group_request(
                runtime.agent_id,
                requester.client_id,
                ActionRequest(
                    action='sleep',
                    pargs=(TEST_SLEEP_INTERVAL * 10,),
                    serialization=SerializationStrategy.PICKLE,
                ),
            )

        action_request = build_sleep_request()
        await requester.send(action_request)
        await asyncio.sleep(TEST_SLEEP_INTERVAL)

        cancel_request = _make_group_request(
            runtime.agent_id,
            stranger.client_id,
            CancelRequest(target_tag=action_request.tag),
        )
        await stranger.send(cancel_request)
        message = await anext(stranger_listener)
        body = message.get_body()
        assert isinstance(body, ErrorResponse)
        assert isinstance(body.get_exception(), RequestForbiddenError)

        cancel_request = _make_group_request(
            runtime.agent_id,
            requester.client_id,
            CancelRequest(target_tag=action_request.tag),
        )
        await requester.send(cancel_request)
        for _ in range(2):
            message = await anext(requester_listener)
            body = message.get_body()
            if message.tag == cancel_request.tag:
                assert isinstance(body, SuccessResponse)
            elif message.tag == action_request.tag:
                assert isinstance(body, ErrorResponse)
                assert isinstance(body.get_exception(), ActionCancelledError)
            else:  # pragma: no cover
                pytest.fail('Received unexpected message.')

        action_request = build_sleep_request()
        await requester.send(action_request)
        await asyncio.sleep(TEST_SLEEP_INTERVAL)

        cancel_request = _make_group_request(
            runtime.agent_id,
            owner_id,
            CancelRequest(target_tag=action_request.tag),
        )
        await exchange_client.send(cancel_request)
        message = await anext(owner_listener)
        assert isinstance(message.get_body(), SuccessResponse)
        message = await anext(requester_listener)
        assert message.tag == action_request.tag
        assert isinstance(
            message.get_body().get_exception(),
            ActionCancelledError,
        )


@pytest.mark.asyncio
async def test_cancel_ping_requires_requester_or_owner(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    owner_id = exchange_client.client_id
    factory = exchange_client.factory()
    registration = await exchange_client.register_agent(GatedAgent)
    registration = _OwnableReg(
        agent_id=registration.agent_id,
        owner=owner_id,
    )

    async def slow_ping(self: Any, request: Any) -> None:
        await asyncio.sleep(TEST_SLEEP_INTERVAL * 10)

    async with (
        await factory.create_user_client() as requester,
        await factory.create_user_client() as stranger,
        Runtime(
            GatedAgent(),
            exchange_factory=factory,
            registration=registration,
        ) as runtime,
    ):
        requester_listener = await _listen_on(requester)
        stranger_listener = await _listen_on(stranger)

        with mock.patch.object(Runtime, '_execute_ping', slow_ping):
            ping = _make_group_request(
                runtime.agent_id,
                requester.client_id,
                PingRequest(),
                groups=frozenset({GROUP_A}),
            )
            await requester.send(ping)
            await asyncio.sleep(TEST_SLEEP_INTERVAL)

            # A third party may not cancel someone else's ping.
            cancel = _make_group_request(
                runtime.agent_id,
                stranger.client_id,
                CancelRequest(target_tag=ping.tag),
            )
            await stranger.send(cancel)
            message = await anext(stranger_listener)
            body = message.get_body()
            assert isinstance(body, ErrorResponse)
            assert isinstance(
                body.get_exception(),
                RequestForbiddenError,
            )

            # The requester may cancel their own ping.
            cancel = _make_group_request(
                runtime.agent_id,
                requester.client_id,
                CancelRequest(target_tag=ping.tag),
            )
            await requester.send(cancel)
            message = await anext(requester_listener)
            assert isinstance(message.get_body(), SuccessResponse)
