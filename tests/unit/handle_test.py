from __future__ import annotations

import asyncio
import logging
import pickle
from collections.abc import AsyncGenerator
from typing import Any
from unittest import mock

import pytest
import pytest_asyncio

from academy.exception import AgentTerminatedError
from academy.exception import ExchangeClientNotFoundError
from academy.exchange import LocalExchangeFactory
from academy.exchange import LocalExchangeTransport
from academy.exchange import UserExchangeClient
from academy.exchange.cloud.client import HttpExchangeFactory
from academy.exchange.factory import ExchangeFactory
from academy.exchange.transport import MailboxStatus
from academy.handle import exchange_context
from academy.handle import Handle
from academy.handle import ProxyHandle
from academy.identifier import AgentId
from academy.identifier import UserId
from academy.manager import Manager
from academy.message import ActionRequest
from academy.message import CancelRequest
from academy.message import ErrorResponse
from academy.message import Message
from academy.message import Request
from academy.message import ShutdownRequest
from testing.agents import CounterAgent
from testing.agents import EmptyAgent
from testing.agents import ErrorAgent
from testing.agents import SleepAgent
from testing.constant import TEST_SLEEP_INTERVAL


@pytest.mark.asyncio
async def test_proxy_handle_protocol() -> None:
    agent = EmptyAgent()
    handle = ProxyHandle(agent)
    assert str(agent) in str(handle)
    assert repr(agent) in repr(handle)
    assert await handle.ping() >= 0
    await handle.shutdown()


@pytest.mark.asyncio
async def test_proxy_handle_actions() -> None:
    handle = ProxyHandle(CounterAgent())

    # Via Handle.action()
    await handle.action('add', 1)
    count: int = await handle.action('count')
    assert count == 1

    # Via attribute lookup
    await handle.add(1)
    count = await handle.count()
    assert count == 2  # noqa: PLR2004


@pytest.mark.asyncio
async def test_proxy_handle_action_errors() -> None:
    handle = ProxyHandle(ErrorAgent())

    with pytest.raises(RuntimeError, match=r'This action always fails\.'):
        await handle.action('fails')

    with pytest.raises(AttributeError, match='null'):
        await handle.action('null')

    with pytest.raises(AttributeError, match='null'):
        await handle.null()  # type: ignore[attr-defined]

    handle.agent.foo = 1  # type: ignore[attr-defined]
    with pytest.raises(AttributeError, match='not a method'):
        await handle.foo()  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_proxy_handle_agent_shutdown_errors() -> None:
    handle = ProxyHandle(EmptyAgent())
    await handle.shutdown()

    with pytest.raises(AgentTerminatedError):
        await handle.action('test')
    with pytest.raises(AgentTerminatedError):
        await handle.ping()
    with pytest.raises(AgentTerminatedError):
        await handle.shutdown()


@pytest.mark.asyncio
async def test_agent_proxy_handle_serialize() -> None:
    agent = EmptyAgent()
    handle = ProxyHandle(agent)
    reconstructed = pickle.loads(pickle.dumps(handle))
    assert isinstance(reconstructed, ProxyHandle)
    assert type(reconstructed.agent) is EmptyAgent
    assert str(reconstructed) == str(handle)
    assert repr(reconstructed) == repr(handle)
    assert reconstructed.agent_id != handle.agent_id


@pytest.mark.asyncio
async def test_agent_handle_serialize(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    registration = await exchange_client.register_agent(EmptyAgent)
    handle = Handle(registration.agent_id)
    reconstructed = pickle.loads(pickle.dumps(handle))
    assert isinstance(reconstructed, Handle)
    assert reconstructed.agent_id == handle.agent_id
    assert str(reconstructed) == str(handle)
    assert repr(reconstructed) == repr(handle)


@pytest.mark.asyncio
async def test_agent_handle_context() -> None:
    # We cannot use the fixture here because the fixture will create context
    factory = LocalExchangeFactory()
    exchange_client = await factory.create_user_client()
    registration = await exchange_client.register_agent(EmptyAgent)
    handle = Handle(registration.agent_id)

    with pytest.raises(ExchangeClientNotFoundError):
        assert handle.exchange is exchange_client

    exchange_context.set(exchange_client)
    assert handle.exchange is exchange_client

    await exchange_client.close()


@pytest.mark.asyncio
async def test_handle_exchange_registration(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    assert len(exchange_client._handles) == 0

    registration = await exchange_client.register_agent(EmptyAgent)
    handle = Handle(registration.agent_id)

    assert handle.exchange is exchange_client
    assert len(exchange_client._handles) == 0

    handle._register_with_exchange(exchange_client)
    assert len(handle._registered_exchanges) == 1
    assert len(exchange_client._handles) == 1

    # Registration is idempotent
    handle._register_with_exchange(exchange_client)
    assert len(handle._registered_exchanges) == 1
    assert len(exchange_client._handles) == 1


@pytest.mark.asyncio
async def test_agent_handle_reuse(
    manager: Manager[LocalExchangeTransport],
) -> None:
    exchange_client = manager.exchange_client
    factory = exchange_client.factory()
    destination = await manager.launch(CounterAgent)

    handle = Handle(destination.agent_id)
    assert handle.exchange is exchange_client, 'Client not inferred.'
    assert handle.handle_id not in exchange_client._handles

    assert await handle.ping() > 0
    assert handle.handle_id in exchange_client._handles
    assert exchange_client in handle._registered_exchanges

    async with await factory.create_user_client() as new_client:
        # Exchange is updated in the agent
        assert handle.exchange is new_client
        assert await handle.ping() > 0
        assert handle.handle_id in new_client._handles
        assert new_client in handle._registered_exchanges

    # Exchange is reset after
    assert handle.exchange is exchange_client

    await exchange_client.close()


@pytest.mark.asyncio
async def test_client_handle_ping_timeout(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    registration = await exchange_client.register_agent(EmptyAgent)
    handle = Handle(registration.agent_id)
    with pytest.raises(asyncio.TimeoutError):
        await handle.ping(timeout=TEST_SLEEP_INTERVAL)


@pytest.mark.asyncio
async def test_client_handle_shutdown_ignore_already_terminated() -> None:
    handle: Handle[EmptyAgent] = Handle(AgentId.new())

    request = Message.create(
        src=UserId.new(),
        dest=handle.agent_id,
        body=ShutdownRequest(),
    )
    handle._shutdown_requests.add(request.tag)
    response = request.create_response(
        ErrorResponse(exception=AgentTerminatedError(handle.agent_id)),
    )
    await handle._process_response(response)


@pytest.mark.asyncio
async def test_client_handle_shutdown_log_error_response(caplog) -> None:
    handle: Handle[EmptyAgent] = Handle(AgentId.new())

    request = Message.create(
        src=AgentId.new(),
        dest=handle.agent_id,
        body=ShutdownRequest(),
    )
    handle._shutdown_requests.add(request.tag)
    response = request.create_response(
        ErrorResponse(exception=AgentTerminatedError(AgentId.new())),
    )

    with caplog.at_level(logging.ERROR):
        await handle._process_response(response)
    assert f'Failure requesting shutdown for {handle.agent_id}' in caplog.text


EXCHANGE_FACTORY_TYPES = (
    HttpExchangeFactory,  # Test with serialization
    LocalExchangeFactory,  # Test without serialization
)


@pytest_asyncio.fixture(params=EXCHANGE_FACTORY_TYPES)
async def factory(
    request,
    get_factory,
) -> AsyncGenerator[ExchangeFactory[Any]]:
    return get_factory(request.param)


@pytest.mark.asyncio
async def test_client_handle_actions(
    factory: ExchangeFactory[Any],
) -> None:
    async with await Manager.from_exchange_factory(
        factory=factory,
    ) as manager:
        handle = await manager.launch(CounterAgent())
        assert await handle.ping() > 0

        await handle.action('add', 1)
        count: int = await handle.action('count')
        assert count == 1

        await handle.add(1)
        count = await handle.count()
        assert count == 2  # noqa: PLR2004

        await handle.shutdown()


@pytest.mark.parametrize('terminate', (True, False))
@pytest.mark.asyncio
async def test_client_shutdown_termination(
    terminate: bool,
    factory: ExchangeFactory[Any],
) -> None:
    async with await Manager.from_exchange_factory(
        factory=factory,
    ) as manager:
        handle = await manager.launch(EmptyAgent())
        await handle.shutdown(terminate=terminate)
        await manager.wait({handle})
        status = await manager.exchange_client.status(handle.agent_id)
        if terminate:
            assert status == MailboxStatus.TERMINATED
        else:
            assert status == MailboxStatus.ACTIVE


@pytest.mark.asyncio
async def test_client_handle_errors(
    factory: ExchangeFactory[Any],
) -> None:
    async with await Manager.from_exchange_factory(
        factory=factory,
    ) as manager:
        handle = await manager.launch(ErrorAgent())
        with pytest.raises(
            RuntimeError,
            match=r'This action always fails\.',
        ):
            await handle.fails()

        with pytest.raises(AttributeError, match='null'):
            await handle.action('null')

        await handle.shutdown()


@pytest.mark.asyncio
async def test_client_handle_action_cancelled(
    manager: Manager[LocalExchangeTransport],
) -> None:
    handle = await manager.launch(SleepAgent)
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(handle.action('sleep', 0.1), 0.01)

    await asyncio.wait_for(handle.action('sleep', 0.1), 1.0)


@pytest.mark.asyncio
async def test_client_handle_action_cancelled_sends_request(
    manager: Manager[LocalExchangeTransport],
) -> None:
    handle = await manager.launch(SleepAgent)
    with mock.patch.object(manager.exchange_client, 'send') as send:
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(handle.action('sleep', 0.1), 0.01)

        assert send.call_count == 2  # noqa: PLR2004
        cancel_request = send.call_args.args[0]
        assert isinstance(cancel_request, Message)
        assert isinstance(cancel_request.body, CancelRequest)


@pytest.mark.asyncio
async def test_client_handle_action_cancelled_during_send(
    manager: Manager[LocalExchangeTransport],
) -> None:
    handle = await manager.launch(SleepAgent)
    with mock.patch.object(
        manager.exchange_client,
        'send',
        new_callable=mock.AsyncMock,
    ) as send:
        sending_event = asyncio.Event()
        cancelled_event = asyncio.Event()

        async def send_effect(msg: Message[Request]):
            if isinstance(msg.body, ActionRequest):
                try:
                    sending_event.set()
                    # 600 is an approximation of forever
                    await asyncio.sleep(600)
                    raise RuntimeError(
                        'send sleep should never complete',
                    )  # pragma: no cover
                except asyncio.CancelledError:
                    cancelled_event.set()
                    raise
            elif isinstance(msg.body, CancelRequest):
                # no need for any async behaviour by the time we've reached
                # the cancellation message.
                pass
            else:
                raise RuntimeError(
                    f'This mock cannot handle {msg}',
                )  # pragma: no cover

        send.side_effect = (
            send_effect  # Make sure we yield the scheduler on send.
        )
        task: asyncio.Task[None] = asyncio.create_task(
            handle.action('sleep', 0, 1),
        )

        await sending_event.wait()
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            # Wait for cancel to finish.
            await task

        assert cancelled_event.is_set(), (
            'send_effect should have set cancelled event'
        )
        assert send.call_count > 1, (
            'send should be called once for action and once for cancel'
        )

        cancel_request = send.call_args.args[0]
        assert isinstance(cancel_request, Message), (
            'The last send should have been a message'
        )
        assert isinstance(cancel_request.body, CancelRequest), (
            'The last send should have been a cancellation'
        )


@pytest.mark.asyncio
async def test_handle_default_exchange() -> None:
    factory = LocalExchangeFactory()
    exchange_client = await factory.create_user_client()
    registration = await exchange_client.register_agent(EmptyAgent)
    handle = Handle(registration.agent_id, exchange=exchange_client)

    assert handle.exchange is exchange_client
    assert repr(exchange_client) in repr(handle)

    async with await factory.create_user_client() as new_client:
        assert handle.exchange is new_client

    assert handle.exchange is exchange_client

    await exchange_client.close()


@pytest.mark.asyncio
async def test_handle_ignore_context() -> None:
    factory = LocalExchangeFactory()
    exchange_client = await factory.create_user_client()
    registration = await exchange_client.register_agent(EmptyAgent)
    handle = Handle(
        registration.agent_id,
        exchange=exchange_client,
        ignore_context=True,
    )

    assert handle.exchange is exchange_client
    assert repr(exchange_client) in repr(handle)

    async with await factory.create_user_client():
        assert handle.exchange is exchange_client

    with pytest.raises(pickle.PicklingError):
        pickle.dumps(handle)

    await exchange_client.close()


@pytest.mark.asyncio
async def test_handle_ignore_context_error(
    exchange_client: UserExchangeClient[LocalExchangeTransport],
) -> None:
    registration = await exchange_client.register_agent(EmptyAgent)
    with pytest.raises(ValueError, match='no explicit exchange'):
        Handle(registration.agent_id, ignore_context=True)


def assert_one_tag_id(caplog) -> None:
    ids = {
        r.__dict__['academy.action_tag']
        for r in caplog.records
        if 'academy.action_tag' in r.__dict__
    }
    assert len(ids) == 1, 'Log records should have a single tag ID'


def get_invocation_states(caplog) -> set[str]:
    return {
        r.__dict__['academy.action_state']
        for r in caplog.records
        if 'academy.action_state' in r.__dict__
    }


@pytest.mark.asyncio
async def test_handle_logs_actions_success(
    factory: ExchangeFactory[Any],
    caplog,
) -> None:
    async with await Manager.from_exchange_factory(
        factory=factory,
    ) as manager:
        handle = await manager.launch(CounterAgent())

        with caplog.at_level(logging.DEBUG):
            await handle.action('add', 1)

        assert_one_tag_id(caplog)

        assert get_invocation_states(caplog) == {
            'start',
            'sending',
            'waiting',
            'success',
        }, 'Log records should show successful-path states'

        await handle.shutdown()


@pytest.mark.asyncio
async def test_handle_logs_actions_fails(
    factory: ExchangeFactory[Any],
    caplog,
) -> None:
    async with await Manager.from_exchange_factory(
        factory=factory,
    ) as manager:
        handle = await manager.launch(ErrorAgent())

        with caplog.at_level(logging.DEBUG):
            with pytest.raises(RuntimeError):
                await handle.action('fails')

        assert_one_tag_id(caplog)

        assert get_invocation_states(caplog) == {
            'start',
            'sending',
            'waiting',
            'exception',
        }, 'Log records should show failure-path states'

        await handle.shutdown()


@pytest.mark.asyncio
async def test_handle_logs_actions_cancelled(
    factory: ExchangeFactory[Any],
    caplog,
) -> None:
    async with await Manager.from_exchange_factory(
        factory=factory,
    ) as manager:
        handle = await manager.launch(SleepAgent)
        with caplog.at_level(logging.DEBUG):
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(handle.action('sleep', 0.1), 0.01)

        assert_one_tag_id(caplog)

        # sending and waiting states might appear, or not, depending on
        # when the cancellation happens.
        assert get_invocation_states(caplog) >= {'start', 'cancelled'}, (
            'Log records should show cancelled-path states'
        )

        await handle.shutdown()
