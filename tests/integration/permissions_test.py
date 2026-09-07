"""Integration tests for the Academy permissions system.

These tests exercise the full flow from Manager.launch() with
RuntimeConfig through runtime authorization enforcement.  Unlike the
unit tests (which create Runtime instances directly), these tests go
through the public Manager API and verify the complete pipeline:
config → manager registration → runtime init → message authorization.

Because the local exchange is a trusted environment that does not
stamp group memberships, we simulate group-stamped messages by
injecting groups directly into request Headers (the same pattern used
by the runtime unit tests in tests/unit/runtime_test.py).

Test clients (member, stranger, controller) are created with
``start_listener=False`` to avoid the background listener competing
with the manual read via ``_transport.listen()``.
"""

from __future__ import annotations

import asyncio
import dataclasses
import uuid
from typing import Any
from unittest import mock

import pytest

from academy.agent import action
from academy.agent import Agent
from academy.exception import AgentTerminatedError
from academy.exception import RequestForbiddenError
from academy.exchange import LocalExchangeFactory
from academy.exchange import LocalExchangeTransport
from academy.exchange import UserExchangeClient
from academy.handle import Handle
from academy.identifier import EntityId
from academy.manager import Manager
from academy.message import ActionRequest
from academy.message import ActionResponse
from academy.message import CancelRequest
from academy.message import ErrorResponse
from academy.message import Header
from academy.message import Message
from academy.message import PingRequest
from academy.message import ShutdownRequest
from academy.message import SuccessResponse
from academy.runtime import RuntimeConfig
from academy.serialize import SerializationStrategy
from testing.constant import TEST_SLEEP_INTERVAL
from testing.constant import TEST_WAIT_TIMEOUT

GROUP_A = '00000000-0000-0000-0000-00000000000a'
CONTROL_GROUP = '00000000-0000-0000-0000-0000000000c1'


class SimpleAgent(Agent):
    @action
    async def echo(self, value: str) -> str:
        return value


class GatedAgent(Agent):
    @action(sharing=[GROUP_A])
    async def restricted(self) -> str:
        return 'ok-a'

    @action
    async def open_(self) -> str:
        return 'ok-open'


class OwnerOnlyAgent(Agent):
    @action(sharing=[GROUP_A])
    async def shared(self) -> str:
        return 'ok-shared'

    @action(sharing=[])
    async def owner_only(self) -> str:
        # Body never runs: the test asserts non-owners are denied.
        return 'secret'  # pragma: no cover


class SleepAgent(Agent):
    @action
    async def sleep(self, duration: float) -> None:
        await asyncio.sleep(duration)


@dataclasses.dataclass
class _OwnableReg:
    agent_id: Any  # AgentId[Any] — kept as Any to satisfy the Manager type
    owner: Any  # EntityId
    exchange_type: str = 'local'


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


async def _next_message(
    client: UserExchangeClient[LocalExchangeTransport],
) -> Message[Any]:
    async for msg in client._transport.listen(TEST_WAIT_TIMEOUT):
        return msg
    raise TimeoutError('No message received')  # pragma: no cover


async def _wait_agent_ready(
    handle: Handle[Any],
    timeout: float = TEST_WAIT_TIMEOUT,
) -> None:
    deadline = asyncio.get_event_loop().time() + timeout
    while True:
        try:
            await asyncio.wait_for(handle.ping(), timeout=TEST_WAIT_TIMEOUT)
            return
        except Exception:  # pragma: no cover - retry guard against flakiness
            if asyncio.get_event_loop().time() > deadline:
                raise TimeoutError(
                    f'Agent {handle.agent_id} not ready within {timeout} s',
                ) from None
            await asyncio.sleep(TEST_SLEEP_INTERVAL)


@pytest.mark.asyncio
async def test_access_groups_member_allowed_non_member_denied() -> None:
    factory = LocalExchangeFactory()
    async with (
        await factory.create_user_client(start_listener=False) as _owner,
        await Manager.from_exchange_factory(factory) as manager,
    ):
        # The local exchange is ownerless (self-hosted trust mode); inject
        # an owner so the runtime enforces access_groups instead of
        # allowing every caller. See _OwnableReg and
        # test_control_groups_stranger_shutdown_denied.
        reg = await manager.exchange_client.register_agent(SimpleAgent)
        owned_reg = _OwnableReg(
            agent_id=reg.agent_id,
            owner=manager.exchange_client.client_id,
        )
        handle = await manager.launch(
            SimpleAgent,
            registration=owned_reg,
            config=RuntimeConfig(access_groups={GROUP_A}),
        )
        await _wait_agent_ready(handle)

        async with await factory.create_user_client(
            start_listener=False,
        ) as member_client:
            request = _make_group_request(
                handle.agent_id,
                member_client.client_id,
                ActionRequest(
                    action='echo',
                    pargs=('hello',),
                    serialization=SerializationStrategy.PICKLE,
                ),
                groups=frozenset({GROUP_A}),
            )
            await member_client.send(request)
            msg = await _next_message(member_client)
            body = msg.get_body()
            assert isinstance(body, ActionResponse)
            assert body.get_result() == 'hello'

        async with await factory.create_user_client(
            start_listener=False,
        ) as stranger_client:
            request = _make_group_request(
                handle.agent_id,
                stranger_client.client_id,
                ActionRequest(
                    action='echo',
                    pargs=('hi',),
                    serialization=SerializationStrategy.PICKLE,
                ),
            )
            await stranger_client.send(request)
            msg = await _next_message(stranger_client)
            body = msg.get_body()
            assert isinstance(body, ErrorResponse)
            assert isinstance(body.get_exception(), RequestForbiddenError)

        await handle.shutdown()
        await manager.wait({handle})


@pytest.mark.asyncio
async def test_control_groups_member_can_shutdown() -> None:
    factory = LocalExchangeFactory()
    async with (
        await factory.create_user_client(start_listener=False) as _owner,
        await Manager.from_exchange_factory(factory) as manager,
    ):
        handle = await manager.launch(
            SimpleAgent,
            config=RuntimeConfig(control_groups={CONTROL_GROUP}),
        )
        await _wait_agent_ready(handle)

        async with await factory.create_user_client(
            start_listener=False,
        ) as controller_client:
            shutdown = _make_group_request(
                handle.agent_id,
                controller_client.client_id,
                ShutdownRequest(),
                groups=frozenset({CONTROL_GROUP}),
            )
            await controller_client.send(shutdown)
            msg = await _next_message(controller_client)
            body = msg.get_body()
            assert isinstance(body, SuccessResponse)

        with pytest.raises((TimeoutError, AgentTerminatedError)):
            await asyncio.wait_for(handle.ping(), timeout=TEST_WAIT_TIMEOUT)

        await manager.wait({handle})


@pytest.mark.asyncio
async def test_control_groups_stranger_shutdown_denied() -> None:
    factory = LocalExchangeFactory()
    async with await Manager.from_exchange_factory(factory) as manager:
        reg = await manager.exchange_client.register_agent(SimpleAgent)
        owned_reg = _OwnableReg(
            agent_id=reg.agent_id,
            owner=manager.exchange_client.client_id,
        )
        handle = await manager.launch(
            SimpleAgent,
            registration=owned_reg,
            config=RuntimeConfig(control_groups={CONTROL_GROUP}),
        )
        await _wait_agent_ready(handle)

        async with await factory.create_user_client(
            start_listener=False,
        ) as stranger_client:
            shutdown = _make_group_request(
                handle.agent_id,
                stranger_client.client_id,
                ShutdownRequest(),
            )
            await stranger_client.send(shutdown)
            msg = await _next_message(stranger_client)
            body = msg.get_body()
            assert isinstance(body, ErrorResponse)
            assert isinstance(
                body.get_exception(),
                RequestForbiddenError,
            )

        assert await handle.ping() > 0

        await handle.shutdown()
        await manager.wait({handle})


@pytest.mark.asyncio
async def test_closed_default_mixed_decorators() -> None:
    factory = LocalExchangeFactory()
    async with await Manager.from_exchange_factory(factory) as manager:
        reg = await manager.exchange_client.register_agent(GatedAgent)
        owned_reg = _OwnableReg(
            agent_id=reg.agent_id,
            owner=manager.exchange_client.client_id,
        )
        handle = await manager.launch(
            GatedAgent,
            registration=owned_reg,
        )
        await _wait_agent_ready(handle)

        async with await factory.create_user_client(
            start_listener=False,
        ) as member_client:
            request = _make_group_request(
                handle.agent_id,
                member_client.client_id,
                ActionRequest(
                    action='restricted',
                    serialization=SerializationStrategy.PICKLE,
                ),
                groups=frozenset({GROUP_A}),
            )
            await member_client.send(request)
            msg = await _next_message(member_client)
            body = msg.get_body()
            assert isinstance(body, ActionResponse)
            assert body.get_result() == 'ok-a'

            request = _make_group_request(
                handle.agent_id,
                member_client.client_id,
                ActionRequest(
                    action='open_',
                    serialization=SerializationStrategy.PICKLE,
                ),
                groups=frozenset({GROUP_A}),
            )
            await member_client.send(request)
            msg = await _next_message(member_client)
            body = msg.get_body()
            assert isinstance(body, ErrorResponse)
            assert isinstance(
                body.get_exception(),
                RequestForbiddenError,
            )

        result = await handle.open_()
        assert result == 'ok-open'

        await handle.shutdown()
        await manager.wait({handle})


@pytest.mark.asyncio
async def test_sharing_empty_owner_only_against_access_groups() -> None:
    factory = LocalExchangeFactory()
    async with (
        await factory.create_user_client(start_listener=False) as _owner,
        await Manager.from_exchange_factory(factory) as manager,
    ):
        # Same ownerless-local-exchange setup as the access-groups test.
        reg = await manager.exchange_client.register_agent(OwnerOnlyAgent)
        owned_reg = _OwnableReg(
            agent_id=reg.agent_id,
            owner=manager.exchange_client.client_id,
        )
        handle = await manager.launch(
            OwnerOnlyAgent,
            registration=owned_reg,
            config=RuntimeConfig(access_groups={GROUP_A}),
        )
        await _wait_agent_ready(handle)

        async with await factory.create_user_client(
            start_listener=False,
        ) as member_client:
            request = _make_group_request(
                handle.agent_id,
                member_client.client_id,
                ActionRequest(
                    action='owner_only',
                    serialization=SerializationStrategy.PICKLE,
                ),
                groups=frozenset({GROUP_A}),
            )
            await member_client.send(request)
            msg = await _next_message(member_client)
            body = msg.get_body()
            assert isinstance(body, ErrorResponse)
            assert isinstance(body.get_exception(), RequestForbiddenError)

            request = _make_group_request(
                handle.agent_id,
                member_client.client_id,
                ActionRequest(
                    action='shared',
                    serialization=SerializationStrategy.PICKLE,
                ),
                groups=frozenset({GROUP_A}),
            )
            await member_client.send(request)
            msg = await _next_message(member_client)
            body = msg.get_body()
            assert isinstance(body, ActionResponse)
            assert body.get_result() == 'ok-shared'

        await handle.shutdown()
        await manager.wait({handle})


@pytest.mark.asyncio
async def test_manager_launch_passes_extra_permitted_groups() -> None:
    factory = LocalExchangeFactory()
    async with (
        await factory.create_user_client(start_listener=False) as _owner,
        await Manager.from_exchange_factory(factory) as manager,
    ):
        original_register = manager.exchange_client.register_agent
        calls: list[dict[str, Any]] = []

        async def _spy_register(
            agent,
            *,
            name=None,
            extra_permitted_groups=None,
        ):
            calls.append(
                {
                    'agent': agent,
                    'name': name,
                    'extra_permitted_groups': (
                        set(extra_permitted_groups)
                        if extra_permitted_groups is not None
                        else None
                    ),
                },
            )
            return await original_register(
                agent,
                name=name,
                extra_permitted_groups=extra_permitted_groups,
            )

        with mock.patch.object(
            manager.exchange_client,
            'register_agent',
            side_effect=_spy_register,
        ):
            handle = await manager.launch(
                GatedAgent,
                config=RuntimeConfig(
                    access_groups={GROUP_A},
                    control_groups={CONTROL_GROUP},
                ),
            )

        await _wait_agent_ready(handle)

        assert len(calls) == 1
        call = calls[0]

        assert call['extra_permitted_groups'] == {GROUP_A, CONTROL_GROUP}

        await handle.shutdown()
        await manager.wait({handle})


@pytest.mark.asyncio
async def test_registration_merged_set_runtime_enforcement() -> None:
    factory = LocalExchangeFactory()
    async with (
        await factory.create_user_client(start_listener=False) as _owner,
        await Manager.from_exchange_factory(factory) as manager,
    ):
        handle = await manager.launch(
            GatedAgent,
            config=RuntimeConfig(
                access_groups={GROUP_A},
                control_groups={CONTROL_GROUP},
            ),
        )
        await _wait_agent_ready(handle)

        async with await factory.create_user_client(
            start_listener=False,
        ) as member_client:
            request = _make_group_request(
                handle.agent_id,
                member_client.client_id,
                ActionRequest(
                    action='restricted',
                    serialization=SerializationStrategy.PICKLE,
                ),
                groups=frozenset({GROUP_A}),
            )
            await member_client.send(request)
            msg = await _next_message(member_client)
            assert isinstance(msg.get_body(), ActionResponse)

            request = _make_group_request(
                handle.agent_id,
                member_client.client_id,
                ActionRequest(
                    action='open_',
                    serialization=SerializationStrategy.PICKLE,
                ),
                groups=frozenset({GROUP_A}),
            )
            await member_client.send(request)
            msg = await _next_message(member_client)
            assert isinstance(msg.get_body(), ActionResponse)

        async with await factory.create_user_client(
            start_listener=False,
        ) as controller_client:
            shutdown = _make_group_request(
                handle.agent_id,
                controller_client.client_id,
                ShutdownRequest(),
                groups=frozenset({CONTROL_GROUP}),
            )
            await controller_client.send(shutdown)
            msg = await _next_message(controller_client)
            assert isinstance(msg.get_body(), SuccessResponse)

        await manager.wait({handle})


@pytest.mark.asyncio
async def test_control_groups_cancel() -> None:
    factory = LocalExchangeFactory()
    async with (
        await factory.create_user_client(start_listener=False) as _owner,
        await Manager.from_exchange_factory(factory) as manager,
    ):
        handle = await manager.launch(
            SleepAgent,
            config=RuntimeConfig(control_groups={CONTROL_GROUP}),
        )
        await _wait_agent_ready(handle)

        async with await factory.create_user_client(
            start_listener=False,
        ) as controller_client:
            action_req = _make_group_request(
                handle.agent_id,
                controller_client.client_id,
                ActionRequest(
                    action='sleep',
                    pargs=(TEST_SLEEP_INTERVAL * 10,),
                    serialization=SerializationStrategy.PICKLE,
                ),
                groups=frozenset({CONTROL_GROUP}),
            )
            await controller_client.send(action_req)
            await asyncio.sleep(TEST_SLEEP_INTERVAL)

            cancel_req = _make_group_request(
                handle.agent_id,
                controller_client.client_id,
                CancelRequest(target_tag=action_req.tag),
                groups=frozenset({CONTROL_GROUP}),
            )
            await controller_client.send(cancel_req)

            _expected = 2  # cancel response + action error
            responses = 0
            while responses < _expected:
                msg = await _next_message(controller_client)
                if msg.tag == cancel_req.tag:
                    assert isinstance(msg.get_body(), SuccessResponse)
                else:
                    assert isinstance(msg.get_body(), ErrorResponse)
                responses += 1

        await handle.shutdown()
        await manager.wait({handle})


@pytest.mark.asyncio
async def test_handle_shutdown_wait_for_response_denied() -> None:
    factory = LocalExchangeFactory()
    async with await Manager.from_exchange_factory(factory) as manager:
        reg = await manager.exchange_client.register_agent(SimpleAgent)
        owned_reg = _OwnableReg(
            agent_id=reg.agent_id,
            owner=manager.exchange_client.client_id,
        )
        handle = await manager.launch(
            SimpleAgent,
            registration=owned_reg,
            config=RuntimeConfig(control_groups={CONTROL_GROUP}),
        )
        await _wait_agent_ready(handle)

        async with await factory.create_user_client() as stranger_client:
            stranger_handle = Handle(
                agent_id=handle.agent_id,
                exchange=stranger_client,
                ignore_context=True,
            )

            with pytest.raises(RequestForbiddenError):
                await stranger_handle.shutdown(wait_for_response=True)

        assert await handle.ping() > 0

        await handle.shutdown()
        await manager.wait({handle})


@pytest.mark.asyncio
async def test_handle_shutdown_wait_for_response_success() -> None:
    factory = LocalExchangeFactory()
    async with await Manager.from_exchange_factory(factory) as manager:
        reg = await manager.exchange_client.register_agent(SimpleAgent)
        owned_reg = _OwnableReg(
            agent_id=reg.agent_id,
            owner=manager.exchange_client.client_id,
        )
        handle = await manager.launch(
            SimpleAgent,
            registration=owned_reg,
        )
        await _wait_agent_ready(handle)

        await handle.shutdown(wait_for_response=True)

        await manager.wait({handle})


@pytest.mark.asyncio
async def test_handle_shutdown_default_does_not_raise_on_denial() -> None:
    factory = LocalExchangeFactory()
    async with await Manager.from_exchange_factory(factory) as manager:
        reg = await manager.exchange_client.register_agent(SimpleAgent)
        owned_reg = _OwnableReg(
            agent_id=reg.agent_id,
            owner=manager.exchange_client.client_id,
        )
        handle = await manager.launch(
            SimpleAgent,
            registration=owned_reg,
            config=RuntimeConfig(control_groups={CONTROL_GROUP}),
        )
        await _wait_agent_ready(handle)

        async with await factory.create_user_client() as stranger_client:
            stranger_handle = Handle(
                agent_id=handle.agent_id,
                exchange=stranger_client,
                ignore_context=True,
            )

            await stranger_handle.shutdown()

        assert await handle.ping() > 0

        await handle.shutdown()
        await manager.wait({handle})
