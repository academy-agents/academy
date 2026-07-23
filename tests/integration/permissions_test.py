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
from academy.manager import _groups_from_config
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

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

GROUP_A = 'group-a'
GROUP_B = 'group-b'
CONTROL_GROUP = 'control-group'


# ---------------------------------------------------------------------------
# Agent classes for the integration scenarios
# ---------------------------------------------------------------------------


class SimpleAgent(Agent):
    """Agent with no decorator groups — sharing is purely config-driven."""

    @action
    async def echo(self, value: str) -> str:
        return value

    @action
    async def ping_action(self) -> str:
        return 'pong'


class GatedAgent(Agent):
    """Agent mixing decorator groups and undecorated actions."""

    @action(sharing=[GROUP_A])
    async def restricted(self) -> str:
        return 'ok-a'

    @action(sharing=[GROUP_B])
    async def restricted_b(self) -> str:
        return 'ok-b'

    @action
    async def open_(self) -> str:
        return 'ok-open'


class OwnerOnlyAgent(Agent):
    """Agent with an explicit sharing=[] action (hard owner-only)."""

    @action(sharing=[GROUP_A])
    async def shared(self) -> str:
        return 'ok-shared'

    @action(sharing=[])
    async def owner_only(self) -> str:
        return 'secret'


class SleepAgent(Agent):
    """Agent with a long-running action for cancel tests."""

    @action
    async def sleep(self, duration: float) -> None:
        await asyncio.sleep(duration)


# ---------------------------------------------------------------------------
# Ownable registration wrapper (local exchange is ownerless by design)
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class _OwnableReg:
    """Minimal registration with an owner for testing.

    The local exchange produces ownerless registrations (ownerless =
    trusted), but some permission flows (owner bypass, non-owner
    shutdown denial) require an owner.  This wrapper adds an ``owner``
    attribute that the Runtime's ``_is_owner`` checks via
    ``getattr(registration, 'owner', None)``.
    """

    agent_id: Any  # AgentId[Any] — kept as Any to satisfy the Manager type
    owner: Any  # EntityId
    exchange_type: str = 'local'


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_group_request(
    dest: EntityId,
    src: EntityId,
    body: ActionRequest | CancelRequest | ShutdownRequest | PingRequest,
    groups: frozenset[str] = frozenset(),
) -> Message[Any]:
    """Build a request message with group stamps in the header."""
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
    """Read the next message from the client's transport queue.

    The client must have been created with ``start_listener=False``
    so that no background listener competes for messages.
    """
    async for msg in client._transport.listen(TEST_WAIT_TIMEOUT):
        return msg
    raise TimeoutError('No message received')


async def _wait_agent_ready(
    handle: Handle[Any],
    timeout: float = TEST_WAIT_TIMEOUT,
) -> None:
    """Poll-ping until the agent responds (or *timeout* expires)."""
    deadline = asyncio.get_event_loop().time() + timeout
    while True:
        try:
            await asyncio.wait_for(handle.ping(), timeout=TEST_WAIT_TIMEOUT)
            return
        except Exception:
            if asyncio.get_event_loop().time() > deadline:
                raise TimeoutError(
                    f'Agent {handle.agent_id} not ready within {timeout} s',
                ) from None
            await asyncio.sleep(TEST_SLEEP_INTERVAL)


# ===================================================================
# Test 1: access_groups end-to-end
# ===================================================================


@pytest.mark.asyncio
async def test_access_groups_member_allowed_non_member_denied() -> None:
    """Manager.launch with access_groups={G}.

    - G member can call any action.
    - Non-member (no groups) is denied.
    """
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

        # --- member client (belongs to GROUP_A, simulated via header) --- #
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

        # --- stranger client (no groups) --- #
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


# ===================================================================
# Test 2: control_groups end-to-end
# ===================================================================


@pytest.mark.asyncio
async def test_control_groups_member_can_shutdown() -> None:
    """Manager.launch with control_groups={C}.

    - C member can send ShutdownRequest (accepted).
    """
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

        # --- controller client (belongs to CONTROL_GROUP) --- #
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

        # Agent should now be shut down (and possibly terminated)
        with pytest.raises((TimeoutError, AgentTerminatedError)):
            await asyncio.wait_for(handle.ping(), timeout=TEST_WAIT_TIMEOUT)

        await manager.wait({handle})


@pytest.mark.asyncio
async def test_control_groups_stranger_shutdown_denied() -> None:
    """A non-owner, non-control-group sender is blocked from shutdown.

    Uses an owned registration wrapper because the local exchange is
    ownerless by design; without the wrapper the Runtime would permit
    shutdown from anyone (self-hosted trust mode).

    The owner is set to ``manager.exchange_client.client_id`` so that
    ``handle.shutdown()`` (which uses the manager's client via the
    exchange context) is recognised as the owner.
    """
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

        # Agent should still be alive
        assert await handle.ping() > 0

        await handle.shutdown()
        await manager.wait({handle})


# ===================================================================
# Test 3: Closed-default with mixed decorators
# ===================================================================


@pytest.mark.asyncio
async def test_closed_default_mixed_decorators() -> None:
    """Agent with @action(sharing=[X]) on one method, undecorated on another.

    No RuntimeConfig groups:

    - X member can call the decorated action.
    - X member *cannot* call the undecorated action (closed default).
    - Owner can call the undecorated action.

    Uses an owned registration wrapper because the local exchange is
    ownerless by design; without the wrapper the Runtime would treat
    the owner as any other sender in fine-grained mode.

    The owner is set to ``manager.exchange_client.client_id`` so that
    the handle (which uses the manager's client via exchange context)
    is recognised as the owner.
    """
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

        # --- GROUP_A member: can call restricted, cannot call open_ --- #
        async with await factory.create_user_client(
            start_listener=False,
        ) as member_client:
            # Call decorated action — should succeed
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

            # Call undecorated action — FORBIDDEN (closed default)
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

        # --- Owner can call the undecorated action --- #
        # Use the handle directly since it sends via the manager's
        # client, which IS the owner.
        result = await handle.open_()
        assert result == 'ok-open'

        await handle.shutdown()
        await manager.wait({handle})


# ===================================================================
# Test 4: sharing=[] stays owner-only
# ===================================================================


@pytest.mark.asyncio
async def test_sharing_empty_owner_only_against_access_groups() -> None:
    """@action(sharing=[]) + RuntimeConfig(access_groups={G}).

    - G member CANNOT call the sharing=[] action (explicit owner-only
      beats agent-wide config).
    - G member CAN call other actions (access_groups grant).
    """
    factory = LocalExchangeFactory()
    async with (
        await factory.create_user_client(start_listener=False) as _owner,
        await Manager.from_exchange_factory(factory) as manager,
    ):
        # The local exchange is ownerless (self-hosted trust mode); inject
        # an owner so the runtime enforces sharing/access_groups instead of
        # allowing every caller. See _OwnableReg and
        # test_control_groups_stranger_shutdown_denied.
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
            # 1) sharing=[] action: DENIED to GROUP_A member
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

            # 2) sharing=[GROUP_A] action: ALLOWED (decorator matches)
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


# ===================================================================
# Test 5: Registration merges the group set
# ===================================================================

# 5a — _groups_from_config correctness


@pytest.mark.parametrize(
    ('config', 'expected'),
    (
        (None, None),
        (RuntimeConfig(), None),
        (RuntimeConfig(access_groups={'a'}), {'a'}),
        (RuntimeConfig(control_groups={'c'}), {'c'}),
        (
            RuntimeConfig(access_groups={'a', 'b'}, control_groups={'c'}),
            {'a', 'b', 'c'},
        ),
    ),
)
def test_groups_from_config(config, expected) -> None:
    """_groups_from_config returns the union of access + control groups."""
    assert _groups_from_config(config) == expected


# 5b — Manager passes extra_permitted_groups to register_agent


@pytest.mark.asyncio
async def test_manager_launch_passes_extra_permitted_groups() -> None:
    """Manager.launch calls register_agent with extra_permitted_groups.

    The extra_permitted_groups reflect the RuntimeConfig union.
    """
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
            # Launch with both access and control groups, plus a
            # decorator group (GROUP_B on restricted_b).
            handle = await manager.launch(
                GatedAgent,
                config=RuntimeConfig(
                    access_groups={GROUP_A},
                    control_groups={CONTROL_GROUP},
                ),
            )

        await _wait_agent_ready(handle)

        # Exactly one registration call
        assert len(calls) == 1
        call = calls[0]

        # extra_permitted_groups = access group union control groups
        assert call['extra_permitted_groups'] == {GROUP_A, CONTROL_GROUP}

        await handle.shutdown()
        await manager.wait({handle})


# 5c — Runtime stores the config groups and enforces them


@pytest.mark.asyncio
async def test_registration_merged_set_runtime_enforcement() -> None:
    """End-to-end: Runtime enforces access_groups and control_groups.

    Manager launches with access_groups and control_groups; verify the
    Runtime correctly enforces both.
    """
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

        # --- access_groups member can call any action --- #
        async with await factory.create_user_client(
            start_listener=False,
        ) as member_client:
            # Decorated action (restricted — GROUP_A by decorator)
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

            # Undecorated action (open_) — must succeed via access_groups
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

        # --- control_groups member can shut down --- #
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


# ===================================================================
# Bonus: Cancel with control groups
# ===================================================================


@pytest.mark.asyncio
async def test_control_groups_cancel() -> None:
    """control_groups member can cancel an in-flight action."""
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
            # Start a long-running action from the controller itself
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

            # Cancel own action (controller is also the requester)
            cancel_req = _make_group_request(
                handle.agent_id,
                controller_client.client_id,
                CancelRequest(target_tag=action_req.tag),
                groups=frozenset({CONTROL_GROUP}),
            )
            await controller_client.send(cancel_req)

            # Expect both the cancel success and the cancelled action error
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
