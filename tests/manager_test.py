from __future__ import annotations

import asyncio

import pytest

from academy.exception import BadEntityIdError
from academy.exchange import UserExchangeClient
from academy.exchange.local import LocalExchangeFactory
from academy.exchange.local import LocalExchangeTransport
from academy.launcher import ThreadLauncher
from academy.manager import Manager
from academy.message import PingRequest
from academy.message import PingResponse
from testing.behavior import EmptyBehavior
from testing.behavior import SleepBehavior
from testing.constant import TEST_LOOP_SLEEP
from testing.constant import TEST_THREAD_JOIN_TIMEOUT


@pytest.mark.asyncio
async def test_protocol() -> None:
    async with await Manager.from_exchange_factory(
        factory=LocalExchangeFactory(),
        launcher=ThreadLauncher(),
    ) as manager:
        assert isinstance(repr(manager), str)
        assert isinstance(str(manager), str)


@pytest.mark.asyncio
async def test_basic_usage(
    exchange: UserExchangeClient[LocalExchangeTransport],
) -> None:
    behavior = SleepBehavior(TEST_LOOP_SLEEP)
    async with Manager(
        exchange_client=exchange,
        launcher=ThreadLauncher(),
    ) as manager:
        await manager.launch(behavior)
        await manager.launch(behavior)

        await asyncio.sleep(5 * TEST_LOOP_SLEEP)


@pytest.mark.asyncio
async def test_reply_to_requests_with_error(
    exchange: UserExchangeClient[LocalExchangeTransport],
) -> None:
    factory = exchange.factory()
    async with Manager(
        exchange_client=exchange,
        launcher=ThreadLauncher(),
    ) as manager:
        async with await factory.create_user_client(
            start_listener=False,
        ) as client:
            request = PingRequest(
                src=client.client_id,
                dest=manager.user_id,
            )
            await client.send(request)
            response = await client._transport.recv()
            assert isinstance(response, PingResponse)
            assert isinstance(response.exception, TypeError)


@pytest.mark.asyncio
async def test_wait_bad_identifier(
    exchange: UserExchangeClient[LocalExchangeTransport],
) -> None:
    async with Manager(
        exchange_client=exchange,
        launcher=ThreadLauncher(),
    ) as manager:
        registration = await manager.exchange_client.register_agent(
            EmptyBehavior,
        )

        with pytest.raises(BadEntityIdError):
            await manager.wait(registration.agent_id)


@pytest.mark.asyncio
async def test_wait_timeout(
    exchange: UserExchangeClient[LocalExchangeTransport],
) -> None:
    behavior = SleepBehavior(TEST_LOOP_SLEEP)
    async with Manager(
        exchange_client=exchange,
        launcher=ThreadLauncher(),
    ) as manager:
        handle = await manager.launch(behavior)

        with pytest.raises(TimeoutError):
            await manager.wait(handle.agent_id, timeout=TEST_LOOP_SLEEP)


@pytest.mark.asyncio
async def test_shutdown_bad_identifier(
    exchange: UserExchangeClient[LocalExchangeTransport],
) -> None:
    async with Manager(
        exchange_client=exchange,
        launcher=ThreadLauncher(),
    ) as manager:
        registration = await manager.exchange_client.register_agent(
            EmptyBehavior,
        )

        with pytest.raises(BadEntityIdError):
            await manager.shutdown(registration.agent_id)


@pytest.mark.asyncio
async def test_shutdown_nonblocking(
    exchange: UserExchangeClient[LocalExchangeTransport],
) -> None:
    behavior = SleepBehavior(TEST_LOOP_SLEEP)
    async with Manager(
        exchange_client=exchange,
        launcher=ThreadLauncher(),
    ) as manager:
        handle = await manager.launch(behavior)
        await manager.shutdown(handle.agent_id, blocking=False)
        await manager.wait(handle.agent_id, timeout=TEST_THREAD_JOIN_TIMEOUT)


@pytest.mark.asyncio
async def test_shutdown_blocking(
    exchange: UserExchangeClient[LocalExchangeTransport],
) -> None:
    behavior = SleepBehavior(TEST_LOOP_SLEEP)
    async with Manager(
        exchange_client=exchange,
        launcher=ThreadLauncher(),
    ) as manager:
        handle = await manager.launch(behavior)
        await manager.shutdown(handle.agent_id, blocking=True)
        await manager.wait(handle.agent_id, timeout=TEST_LOOP_SLEEP)


@pytest.mark.asyncio
async def test_bad_default_launcher(
    exchange: UserExchangeClient[LocalExchangeTransport],
) -> None:
    with pytest.raises(ValueError, match='No launcher named "second"'):
        Manager(
            exchange_client=exchange,
            launcher={'first': ThreadLauncher()},
            default_launcher='second',
        )


@pytest.mark.asyncio
async def test_add_and_set_launcher_errors(
    exchange: UserExchangeClient[LocalExchangeTransport],
) -> None:
    launcher = ThreadLauncher()
    async with Manager(
        exchange_client=exchange,
        launcher={'first': launcher},
    ) as manager:
        with pytest.raises(
            ValueError,
            match='Launcher named "first" already exists.',
        ):
            manager.add_launcher('first', launcher)
        with pytest.raises(
            ValueError,
            match='A launcher name "second" does not exist.',
        ):
            manager.set_default_launcher('second')


@pytest.mark.asyncio
async def test_multiple_launcher(
    exchange: UserExchangeClient[LocalExchangeTransport],
) -> None:
    async with Manager(
        exchange_client=exchange,
        launcher={'first': ThreadLauncher()},
    ) as manager:
        await manager.launch(EmptyBehavior(), launcher='first')

        manager.add_launcher('second', ThreadLauncher())
        manager.set_default_launcher('second')
        await manager.launch(EmptyBehavior())
        await manager.launch(EmptyBehavior(), launcher='first')


@pytest.mark.asyncio
async def test_multiple_launcher_no_default(
    exchange: UserExchangeClient[LocalExchangeTransport],
) -> None:
    async with Manager(
        exchange_client=exchange,
        launcher={'first': ThreadLauncher()},
    ) as manager:
        with pytest.raises(ValueError, match='no default is set.'):
            await manager.launch(EmptyBehavior())
