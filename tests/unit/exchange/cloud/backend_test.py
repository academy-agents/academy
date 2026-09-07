from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import AsyncGenerator
from typing import Any

import pytest
import pytest_asyncio

from academy.exception import BadEntityIdError
from academy.exception import ForbiddenError
from academy.exception import MailboxTerminatedError
from academy.exception import MessageTooLargeError
from academy.exchange import MailboxStatus
from academy.exchange.cloud.backend import MailboxBackend
from academy.exchange.cloud.backend import PythonBackend
from academy.exchange.cloud.backend import RedisBackend
from academy.exchange.cloud.client_info import ClientInfo
from academy.identifier import AgentId
from academy.identifier import UserId
from academy.message import ErrorResponse
from academy.message import Message
from academy.message import PingRequest
from academy.message import SuccessResponse

BACKEND_TYPES = (PythonBackend, RedisBackend)


@pytest_asyncio.fixture(params=BACKEND_TYPES)
async def backend(request, mock_redis) -> AsyncGenerator[MailboxBackend]:
    return request.param()


@pytest.mark.asyncio
async def test_mailbox_backend_create_close(backend: MailboxBackend) -> None:
    user_id = ClientInfo(str(uuid.uuid4()), set())

    uid = UserId.new()
    # Should do nothing since mailbox doesn't exist
    await backend.terminate(user_id, uid)
    assert await backend.check_mailbox(user_id, uid) == MailboxStatus.MISSING
    await backend.create_mailbox(user_id, uid)
    assert await backend.check_mailbox(user_id, uid) == MailboxStatus.ACTIVE
    await backend.create_mailbox(user_id, uid)  # Idempotent check

    bad_user = ClientInfo(str(uuid.uuid4()), set())  # Authentication check
    with pytest.raises(ForbiddenError):
        await backend.create_mailbox(bad_user, uid)
    with pytest.raises(ForbiddenError):
        await backend.check_mailbox(bad_user, uid)
    with pytest.raises(ForbiddenError):
        await backend.terminate(bad_user, uid)

    await backend.terminate(user_id, uid)
    await backend.terminate(user_id, uid)  # Idempotent check


@pytest.mark.asyncio
async def test_mailbox_backend_send_recv(backend: MailboxBackend) -> None:
    user_id = ClientInfo(str(uuid.uuid4()), set())
    bad_user = ClientInfo(str(uuid.uuid4()), set())
    uid = UserId.new()
    await backend.create_mailbox(user_id, uid)

    message = Message.create(src=uid, dest=uid, body=PingRequest())
    with pytest.raises(ForbiddenError):
        await backend.put(bad_user, message)
    await backend.put(user_id, message)

    with pytest.raises(ForbiddenError):
        await backend.get(bad_user, uid)
    assert await backend.get(user_id, uid) == message

    await backend.terminate(user_id, uid)


async def test_mailbox_backend_put_annotates_sender_groups(
    backend: MailboxBackend,
) -> None:
    shared_group = str(uuid.uuid4())
    other_group = str(uuid.uuid4())

    owner_id = str(uuid.uuid4())
    # The owner must belong to a group to share the mailbox with it.
    owner = ClientInfo(owner_id, {shared_group})
    # Same owner identity, but sending with a token that no longer carries
    # the shared group. Ownership (by client id), not group membership,
    # grants send access here.
    owner_without_group = ClientInfo(owner_id, set())
    # Sender belongs to a group the mailbox is shared with, plus an
    # unrelated group it does not need.
    sender = ClientInfo(str(uuid.uuid4()), {shared_group, other_group})

    uid = UserId.new()
    sender_uid = UserId.new()
    await backend.create_mailbox(owner, uid)
    await backend.create_mailbox(sender, sender_uid)
    await backend.share_mailbox(owner, uid, shared_group)
    await backend.share_mailbox(sender, sender_uid, shared_group)

    message = Message.create(src=sender_uid, dest=uid, body=PingRequest())
    await backend.put(sender, message)

    received = await backend.get(owner, uid)
    # Only the group actually shared with the mailbox is stamped onto the
    # header, even though the sender belongs to other groups too.
    assert received.header.groups == frozenset({shared_group})

    # The owner can send by virtue of ownership, but since this token
    # carries none of the shared groups the stamped set is empty. This also
    # proves the stamp reflects the *sender's* membership, not the
    # mailbox's shared groups.
    owner_message = Message.create(src=uid, dest=uid, body=PingRequest())
    await backend.put(owner_without_group, owner_message)
    owner_received = await backend.get(owner, uid)
    assert owner_received.header.groups == frozenset()

    await backend.terminate(owner, uid)
    await backend.terminate(sender, sender_uid)


@pytest.mark.asyncio
async def test_mailbox_backend_bad_identifier(backend: MailboxBackend) -> None:
    uid = UserId.new()
    empty_uid = ClientInfo('', set())
    message = Message.create(src=uid, dest=uid, body=PingRequest())
    with pytest.raises(BadEntityIdError):
        await backend.get(empty_uid, uid)

    with pytest.raises(BadEntityIdError):
        await backend.put(empty_uid, message)


@pytest.mark.asyncio
async def test_mailbox_backend_mailbox_closed(backend: MailboxBackend) -> None:
    uid = UserId.new()
    client = ClientInfo(str(uid), set())
    await backend.create_mailbox(client, uid)
    await backend.terminate(client, uid)
    message = Message.create(src=uid, dest=uid, body=PingRequest())
    with pytest.raises(MailboxTerminatedError):
        await backend.get(client, uid)

    with pytest.raises(MailboxTerminatedError):
        await backend.put(client, message)


@pytest.mark.asyncio
async def test_mailbox_backend_mailbox_create_forbidden(
    backend: MailboxBackend,
) -> None:
    uid = UserId.new()
    await backend.create_mailbox(ClientInfo('me', set()), uid)
    with pytest.raises(ForbiddenError):
        await backend.create_mailbox(ClientInfo('not_me', set()), uid)


@pytest.mark.asyncio
async def test_mailbox_backend_mailbox_delete_agent(
    backend: MailboxBackend,
) -> None:
    uid = UserId.new()
    aid: AgentId[Any] = AgentId.new()
    client = ClientInfo(str(uid), set())
    await backend.create_mailbox(client, uid)
    await backend.create_mailbox(client, aid, ('EmptyAgent',))

    request = Message.create(src=uid, dest=aid, body=PingRequest())
    await backend.put(client, request)
    response = Message.create(src=uid, dest=aid, body=SuccessResponse())
    await backend.put(client, response)
    await backend.terminate(client, aid)

    message = await backend.get(client, uid, timeout=0.01)
    assert isinstance(message.get_body(), ErrorResponse)
    assert isinstance(message.body.get_exception(), MailboxTerminatedError)

    with pytest.raises(TimeoutError):
        await backend.get(client, uid, timeout=0.01)


@pytest.mark.asyncio
async def test_mailbox_backend_discover(backend: MailboxBackend) -> None:
    aid1: AgentId[Any] = AgentId.new()
    aid2: AgentId[Any] = AgentId.new()
    me_client = ClientInfo('me', set())
    not_me_client = ClientInfo('not_me', set())
    await backend.create_mailbox(me_client, aid1, ('EmptyAgent',))
    await backend.create_mailbox(me_client, aid2, ('SubClass', 'EmptyAgent'))

    agents = await backend.discover(me_client, 'EmptyAgent', False)
    assert aid1 in agents
    assert aid2 not in agents

    agents = await backend.discover(me_client, 'EmptyAgent', True)
    assert aid1 in agents
    assert aid2 in agents

    agents = await backend.discover(not_me_client, 'EmptyAgent', True)
    assert len(agents) == 0


@pytest.mark.asyncio
async def test_mailbox_backend_timeout(backend: MailboxBackend) -> None:
    uid = UserId.new()
    client = ClientInfo(str(uid), set())
    await backend.create_mailbox(client, uid)

    with pytest.raises(TimeoutError):
        await backend.get(client, uid, timeout=0.01)


@pytest.mark.asyncio
async def test_mailbox_backend_sharing(backend: MailboxBackend) -> None:
    friend_group = str(uuid.uuid4())

    client = ClientInfo(str(uuid.uuid4()), {friend_group})
    friend = ClientInfo(str(uuid.uuid4()), {friend_group})

    uid = UserId.new()
    await backend.create_mailbox(client, uid)
    assert (await backend.check_mailbox(client, uid)) == MailboxStatus.ACTIVE
    with pytest.raises(ForbiddenError):
        await backend.check_mailbox(friend, uid)

    await backend.share_mailbox(client, uid, friend_group)
    assert (await backend.check_mailbox(friend, uid)) == MailboxStatus.ACTIVE


@pytest.mark.asyncio
async def test_mailbox_backend_sharing_multi_group(
    backend: MailboxBackend,
) -> None:
    friend_group_1 = str(uuid.uuid4())
    friend_group_2 = str(uuid.uuid4())

    client = ClientInfo(str(uuid.uuid4()), {friend_group_1, friend_group_2})
    friend_1 = ClientInfo(str(uuid.uuid4()), {friend_group_1})
    friend_2 = ClientInfo(str(uuid.uuid4()), {friend_group_2})

    uid = UserId.new()
    await backend.create_mailbox(client, uid)

    assert (await backend.check_mailbox(client, uid)) == MailboxStatus.ACTIVE
    for friend in (friend_1, friend_2):
        with pytest.raises(ForbiddenError):
            await backend.check_mailbox(friend, uid)

    await backend.share_mailbox(client, uid, friend_group_1)

    # Only friend1 should have perms now
    assert (await backend.check_mailbox(friend_1, uid)) == MailboxStatus.ACTIVE
    with pytest.raises(ForbiddenError):
        await backend.check_mailbox(friend_2, uid)

    await backend.share_mailbox(client, uid, friend_group_2)
    # Now both friends should have access via groups sharing

    for friend in (friend_1, friend_2):
        assert (
            await backend.check_mailbox(friend, uid)
        ) == MailboxStatus.ACTIVE


@pytest.mark.asyncio
async def test_mailbox_backend_sharing_invalid(
    backend: MailboxBackend,
) -> None:
    friend_group = str(uuid.uuid4())
    client = ClientInfo(str(uuid.uuid4()), set())
    uid = UserId.new()
    with pytest.raises(BadEntityIdError):
        await backend.share_mailbox(client, uid, friend_group)


@pytest.mark.asyncio
async def test_mailbox_backend_sharing_terminated(
    backend: MailboxBackend,
) -> None:
    friend_group = str(uuid.uuid4())
    client = ClientInfo(str(uuid.uuid4()), set())
    uid = UserId.new()
    await backend.create_mailbox(client, uid)
    await backend.terminate(client, uid)
    with pytest.raises(MailboxTerminatedError):
        await backend.share_mailbox(client, uid, friend_group)


@pytest.mark.asyncio
async def test_mailbox_backend_sharing_requires_ownership(
    backend: MailboxBackend,
) -> None:
    friend_group = str(uuid.uuid4())

    client = ClientInfo(str(uuid.uuid4()), set())

    uid = UserId.new()
    await backend.create_mailbox(client, uid)

    client2 = ClientInfo(str(uuid.uuid4()), set(friend_group))
    with pytest.raises(ForbiddenError):
        await backend.share_mailbox(client2, uid, friend_group)


@pytest.mark.asyncio
async def test_mailbox_backend_sharing_requires_group(
    backend: MailboxBackend,
) -> None:
    friend_group = str(uuid.uuid4())

    client = ClientInfo(str(uuid.uuid4()), set())

    uid = UserId.new()
    await backend.create_mailbox(client, uid)

    with pytest.raises(ForbiddenError):
        await backend.share_mailbox(client, uid, friend_group)


@pytest.mark.asyncio
async def test_mailbox_backend_get_mailbox_shares(
    backend: MailboxBackend,
) -> None:
    friend_group = str(uuid.uuid4())
    client = ClientInfo(str(uuid.uuid4()), {friend_group})

    uid = UserId.new()
    await backend.create_mailbox(client, uid)
    shares = await backend.get_mailbox_shares(client, uid)
    assert len(shares) == 0

    await backend.share_mailbox(client, uid, friend_group)
    shares = await backend.get_mailbox_shares(client, uid)
    assert len(shares) == 1
    assert shares[0] == friend_group


@pytest.mark.asyncio
async def test_mailbox_backend_get_mailbox_shares_invalid(
    backend: MailboxBackend,
) -> None:
    friend_group = str(uuid.uuid4())
    client = ClientInfo(str(uuid.uuid4()), {friend_group})
    uid = UserId.new()
    with pytest.raises(BadEntityIdError):
        await backend.get_mailbox_shares(client, uid)


@pytest.mark.asyncio
async def test_mailbox_backend_get_mailbox_shares_terminated(
    backend: MailboxBackend,
) -> None:
    friend_group = str(uuid.uuid4())
    client = ClientInfo(str(uuid.uuid4()), {friend_group})
    uid = UserId.new()
    await backend.create_mailbox(client, uid)
    await backend.terminate(client, uid)
    with pytest.raises(MailboxTerminatedError):
        await backend.get_mailbox_shares(client, uid)


@pytest.mark.asyncio
async def test_mailbox_backend_get_mailbox_shares_requires_ownership(
    backend: MailboxBackend,
) -> None:
    friend_group = str(uuid.uuid4())
    client = ClientInfo(str(uuid.uuid4()), {friend_group})
    friend = ClientInfo(str(uuid.uuid4()), {friend_group})

    uid = UserId.new()
    await backend.create_mailbox(client, uid)
    await backend.share_mailbox(client, uid, friend_group)

    with pytest.raises(ForbiddenError):
        await backend.get_mailbox_shares(friend, uid)


@pytest.mark.asyncio
async def test_mailbox_backend_remove_mailbox_shares(
    backend: MailboxBackend,
) -> None:
    friend_group = str(uuid.uuid4())
    client = ClientInfo(str(uuid.uuid4()), {friend_group})

    uid = UserId.new()
    await backend.create_mailbox(client, uid)
    shares = await backend.get_mailbox_shares(client, uid)
    assert len(shares) == 0

    await backend.share_mailbox(client, uid, friend_group)
    shares = await backend.get_mailbox_shares(client, uid)
    assert len(shares) == 1
    assert shares[0] == friend_group

    await backend.remove_mailbox_shares(client, uid, friend_group)
    shares = await backend.get_mailbox_shares(client, uid)
    assert len(shares) == 0

    # Remove is idempotent
    await backend.remove_mailbox_shares(client, uid, friend_group)
    shares = await backend.get_mailbox_shares(client, uid)
    assert len(shares) == 0


@pytest.mark.asyncio
async def test_mailbox_backend_remove_mailbox_shares_invalid(
    backend: MailboxBackend,
) -> None:
    friend_group = str(uuid.uuid4())
    client = ClientInfo(str(uuid.uuid4()), {friend_group})
    uid = UserId.new()
    with pytest.raises(BadEntityIdError):
        await backend.remove_mailbox_shares(client, uid, friend_group)


@pytest.mark.asyncio
async def test_mailbox_backend_remove_mailbox_shares_terminated(
    backend: MailboxBackend,
) -> None:
    friend_group = str(uuid.uuid4())
    client = ClientInfo(str(uuid.uuid4()), {friend_group})
    uid = UserId.new()
    await backend.create_mailbox(client, uid)
    await backend.terminate(client, uid)
    with pytest.raises(MailboxTerminatedError):
        await backend.remove_mailbox_shares(client, uid, friend_group)


@pytest.mark.asyncio
async def test_mailbox_backend_remove_mailbox_shares_requires_ownership(
    backend: MailboxBackend,
) -> None:
    friend_group = str(uuid.uuid4())
    client = ClientInfo(str(uuid.uuid4()), {friend_group})
    friend = ClientInfo(str(uuid.uuid4()), {friend_group})

    uid = UserId.new()
    await backend.create_mailbox(client, uid)
    await backend.share_mailbox(client, uid, friend_group)

    with pytest.raises(ForbiddenError):
        await backend.remove_mailbox_shares(friend, uid, friend_group)


@pytest.mark.asyncio
async def test_mailbox_backend_create_does_not_transfer_ownership(
    backend: MailboxBackend,
) -> None:
    group = str(uuid.uuid4())
    owner = ClientInfo(str(uuid.uuid4()), set())
    member = ClientInfo(str(uuid.uuid4()), {group})

    uid = UserId.new()
    await backend.create_mailbox(owner, uid, permitted_groups={group})

    # A group member passes the coarse permission check on the
    # idempotent create route, but must not become the owner or
    # rewrite the share set.
    await backend.create_mailbox(
        member,
        uid,
        permitted_groups={str(uuid.uuid4())},
    )

    with pytest.raises(ForbiddenError):
        await backend.terminate(member, uid)

    # Original owner is intact.
    await backend.terminate(owner, uid)


@pytest.mark.asyncio
async def test_python_backend_message_size() -> None:
    backend = PythonBackend(message_size_limit_kb=0)
    uid = UserId.new()
    client = ClientInfo(str(uid), set())
    await backend.create_mailbox(client, uid)
    message = Message.create(src=uid, dest=uid, body=PingRequest())
    with pytest.raises(MessageTooLargeError):
        await backend.put(client, message)


@pytest.mark.asyncio
async def test_redis_backend_message_size(mock_redis) -> None:
    backend = RedisBackend(message_size_limit_kb=0)
    uid = UserId.new()
    client = ClientInfo(str(uid), set())
    await backend.create_mailbox(client, uid)
    message = Message.create(src=uid, dest=uid, body=PingRequest())
    with pytest.raises(MessageTooLargeError):
        await backend.put(client, message)


@pytest.mark.asyncio
async def test_python_backend_put_mailbox_removed_between_checks() -> None:
    backend = PythonBackend()
    uid = UserId.new()
    client = ClientInfo(str(uid), set())
    await backend.create_mailbox(client, uid)
    other = UserId.new()
    await backend.create_mailbox(client, other)

    class _PhantomDict(dict[Any, Any]):
        def __getitem__(self, key: Any) -> Any:
            if key == uid:
                raise KeyError(key)
            return super().__getitem__(key)

    backend._mailboxes = _PhantomDict(backend._mailboxes)
    message = Message.create(src=uid, dest=uid, body=PingRequest())
    with pytest.raises(BadEntityIdError):
        await backend.put(client, message)

    other_message = Message.create(src=other, dest=other, body=PingRequest())
    await backend.put(client, other_message)
    assert await backend.get(client, other) == other_message


@pytest.mark.asyncio
async def test_redis_backend_put_mailbox_deleted_between_checks(
    mock_redis,
) -> None:
    backend = RedisBackend()
    uid = UserId.new()
    client = ClientInfo(str(uid), set())
    await backend.create_mailbox(client, uid)

    active_key = backend._active_key(uid)
    original_get = backend._client.get
    active_calls = 0

    async def _flaky_get(key: bytes | str) -> bytes | list[bytes] | None:
        nonlocal active_calls
        if key == active_key:
            active_calls += 1
            if active_calls > 1:
                return None
        return await original_get(key)

    backend._client.get = _flaky_get  # type: ignore[assignment]
    message = Message.create(src=uid, dest=uid, body=PingRequest())
    with pytest.raises(BadEntityIdError):
        await backend.put(client, message)


@pytest.mark.asyncio
async def test_redis_backend_gravestone_expire(mock_redis) -> None:
    backend = RedisBackend(gravestone_expiration_s=1)
    user_id = str(uuid.uuid4())
    aid: AgentId[Any] = AgentId.new()
    client = ClientInfo(str(user_id), set())
    await backend.create_mailbox(client, aid, ('EmptyAgent',))
    await asyncio.sleep(2)
    assert await backend.check_mailbox(client, aid) == MailboxStatus.MISSING
    agents = await backend.discover(client, 'EmptyAgent', True)
    assert len(agents) == 0

    # Mailbox does not expire because of get call
    uid = UserId.new()
    client2 = ClientInfo(str(uid), set())
    await backend.create_mailbox(client2, uid)
    message = Message.create(src=uid, dest=uid, body=PingRequest())
    await backend.put(client2, message)
    await asyncio.sleep(0.5)
    assert await backend.get(client2, uid) == message
    await asyncio.sleep(0.5)
    assert await backend.check_mailbox(client2, uid) == MailboxStatus.ACTIVE

    await backend.terminate(client2, uid)
    assert (
        await backend.check_mailbox(client2, uid) == MailboxStatus.TERMINATED
    )
    await asyncio.sleep(2)
    assert await backend.check_mailbox(client2, uid) == MailboxStatus.MISSING


@pytest.mark.asyncio
async def test_redis_backend_mailbox_expire(mock_redis) -> None:
    backend = RedisBackend(mailbox_expiration_s=1)
    client = ClientInfo(str(uuid.uuid4()), set())
    uid = UserId.new()
    await backend.create_mailbox(client, uid)
    message = Message.create(src=uid, dest=uid, body=PingRequest())

    # Message expires after 2 seconds
    await backend.put(client, message)
    await asyncio.sleep(2)
    with pytest.raises(TimeoutError):
        await backend.get(client, uid, timeout=0.01)

    # Get extends expiration
    await backend.put(client, message)
    await backend.put(client, message)
    await backend.put(client, message)
    await asyncio.sleep(0.5)
    assert await backend.get(client, uid) == message
    await asyncio.sleep(0.5)
    assert await backend.get(client, uid) == message
    await asyncio.sleep(2)
    with pytest.raises(TimeoutError):
        await backend.get(client, uid, timeout=0.01)

    await backend.terminate(client, uid)


@pytest.mark.asyncio
async def test_mailbox_backend_request_tracking(
    backend: MailboxBackend,
) -> None:
    client = ClientInfo(str(uuid.uuid4()), set())
    sender_uid = UserId.new()
    receiver_uid = UserId.new()
    await backend.create_mailbox(client, sender_uid)
    await backend.create_mailbox(client, receiver_uid)

    request = Message.create(
        src=sender_uid,
        dest=receiver_uid,
        body=PingRequest(),
    )
    await backend.put(client, request)

    if isinstance(backend, PythonBackend):
        assert receiver_uid in backend._requests
        tracked = backend._requests[receiver_uid].get(request.tag)
        assert tracked is not None
        assert tracked.src == sender_uid
        assert tracked.dest == receiver_uid
    else:
        assert isinstance(backend, RedisBackend)
        request_key = f'request:{receiver_uid.uid}:{request.tag}'
        info_data = await backend._client.get(request_key)
        assert info_data is not None

    response = request.create_response(SuccessResponse())
    await backend.put(client, response)

    if isinstance(backend, PythonBackend):
        still_tracked = request.tag in backend._requests.get(sender_uid, {})
        assert not still_tracked
    else:
        assert isinstance(backend, RedisBackend)
        request_key = f'request:{receiver_uid.uid}:{request.tag}'
        response_data = await backend._client.get(request_key)
        assert response_data is None


@pytest.mark.asyncio
async def test_mailbox_backend_response_without_request(
    backend: MailboxBackend,
) -> None:
    client = ClientInfo(str(uuid.uuid4()), set())
    sender_uid = UserId.new()
    receiver_uid = UserId.new()
    await backend.create_mailbox(client, sender_uid)
    await backend.create_mailbox(client, receiver_uid)

    response = Message.create(
        src=sender_uid,
        dest=receiver_uid,
        body=SuccessResponse(),
    )
    await backend.put(client, response)

    if isinstance(backend, PythonBackend):
        assert receiver_uid not in backend._requests
    else:
        assert isinstance(backend, RedisBackend)
        response_key = f'request:{receiver_uid.uid}'
        tracked = await backend._client.get(response_key)
        assert tracked is None


@pytest.mark.asyncio
async def test_mailbox_backend_response_to_unshared_mailbox(
    backend: MailboxBackend,
) -> None:
    """A response must reach an unshared sender mailbox.

    This reproduces the detachment between group-based authorization on
    requests (checked against the agent's mailbox shares) and
    mailbox-based authorization on responses (previously checked against
    the sender's mailbox shares, which may be empty). With the fix, a
    response that matches a tracked pending request bypasses the
    destination permission check because the original request was
    already authorized.
    """
    shared_group = str(uuid.uuid4())

    user = ClientInfo(str(uuid.uuid4()), {shared_group})
    agent = ClientInfo(str(uuid.uuid4()), set())

    user_uid = UserId.new()
    agent_uid = UserId.new()

    # User's mailbox has no group shares.
    await backend.create_mailbox(user, user_uid)
    # Agent's mailbox is shared with the group the user belongs to.
    await backend.create_mailbox(
        agent,
        agent_uid,
        permitted_groups={shared_group},
    )

    # User sends a request to the agent — passes because the user
    # belongs to the group the agent's mailbox is shared with.
    request = Message.create(
        src=user_uid,
        dest=agent_uid,
        body=PingRequest(),
    )
    await backend.put(user, request)

    # Agent sends the response back to the user. The user's mailbox is
    # not shared with any of the agent's groups, so without the fix this
    # would raise ForbiddenError.
    response = request.create_response(SuccessResponse())
    await backend.put(agent, response)

    # The user should receive the response.
    received = await backend.get(user, user_uid)
    assert received.tag == request.tag
    assert isinstance(received.get_body(), SuccessResponse)

    await backend.terminate(user, user_uid)
    await backend.terminate(agent, agent_uid)


@pytest.mark.asyncio
async def test_mailbox_backend_forged_response_cannot_bypass_permissions(
    backend: MailboxBackend,
) -> None:
    """A forged response cannot bypass the destination permission check.

    A client that owns a mailbox with a pending inbound request must not
    be able to redirect a matching response to an unrelated mailbox it
    has no permission to reach. The response-match shortcut only applies
    when the response returns to the original requester.
    """
    shared_group = str(uuid.uuid4())
    attacker = ClientInfo(str(uuid.uuid4()), set())
    victim_owner = ClientInfo(str(uuid.uuid4()), set())
    requester = ClientInfo(str(uuid.uuid4()), {shared_group})

    attacker_uid = UserId.new()
    victim_uid = UserId.new()
    requester_uid = UserId.new()

    # The attacker owns an agent mailbox shared with the requester's
    # group, so it legitimately receives requests (and could forge
    # responses). The victim's mailbox has no shares.
    await backend.create_mailbox(
        attacker,
        attacker_uid,
        permitted_groups={shared_group},
    )
    await backend.create_mailbox(victim_owner, victim_uid)
    await backend.create_mailbox(requester, requester_uid)

    # A legitimate request creates a tracked pending request addressed to
    # the attacker's mailbox.
    request = Message.create(
        src=requester_uid,
        dest=attacker_uid,
        body=PingRequest(),
    )
    await backend.put(requester, request)

    # The attacker forges a response reusing the matching tag but
    # redirects it to the victim's mailbox, which it cannot reach.
    legit_response = request.create_response(SuccessResponse())
    forged = legit_response.model_copy(
        update={
            'header': legit_response.header.model_copy(
                update={'dest': victim_uid},
            ),
        },
    )

    with pytest.raises(ForbiddenError):
        await backend.put(attacker, forged)


@pytest.mark.asyncio
async def test_mailbox_backend_agent_stats(backend: MailboxBackend) -> None:
    client = ClientInfo(str(uuid.uuid4()), set())
    sender_uid = UserId.new()
    agent_uid = UserId.new()
    await backend.create_mailbox(client, sender_uid)
    await backend.create_mailbox(client, agent_uid)

    req = Message.create(src=sender_uid, dest=agent_uid, body=PingRequest())
    await backend.put(client, req)

    stats = await backend.agent_stats(agent_uid)
    assert stats.incoming == 1
    assert stats.queued == 1

    await backend.get(client, agent_uid)
    stats = await backend.agent_stats(agent_uid)
    assert stats.queued == 0
    assert stats.in_progress == 1

    await backend.put(client, req.create_response(SuccessResponse()))
    stats = await backend.agent_stats(agent_uid)
    assert stats.completed == 1
    assert stats.in_progress == 0

    assert (await backend.agent_stats(sender_uid)).outgoing == 1


async def test_mailbox_backend_heartbeat(backend: MailboxBackend) -> None:
    uid = UserId.new()
    client = ClientInfo(str(uid), set())

    with pytest.raises(BadEntityIdError):
        await backend.heartbeat_status(uid)

    await backend.create_mailbox(client, uid)

    heartbeat = await backend.heartbeat_status(uid)
    assert heartbeat is None

    start = time.time()
    await backend.update_heartbeat(uid)

    heartbeat = await backend.heartbeat_status(uid)
    _elapsed = time.time() - start


@pytest.mark.asyncio
async def test_mailbox_backend_create_with_permitted_groups(
    backend: MailboxBackend,
) -> None:
    group_a = str(uuid.uuid4())
    group_b = str(uuid.uuid4())

    owner_id = str(uuid.uuid4())
    owner = ClientInfo(owner_id, {group_a, group_b})

    sender_a = ClientInfo(str(uuid.uuid4()), {group_a})
    sender_b = ClientInfo(str(uuid.uuid4()), {group_b})
    stranger = ClientInfo(str(uuid.uuid4()), set())

    uid = UserId.new()
    sender_a_uid = UserId.new()
    await backend.create_mailbox(
        owner,
        uid,
        permitted_groups={group_a, group_b},
    )
    await backend.create_mailbox(
        sender_a,
        sender_a_uid,
        permitted_groups={group_a},
    )

    assert (await backend.check_mailbox(owner, uid)) == MailboxStatus.ACTIVE
    assert (await backend.check_mailbox(sender_a, uid)) == MailboxStatus.ACTIVE
    assert (await backend.check_mailbox(sender_b, uid)) == MailboxStatus.ACTIVE
    with pytest.raises(ForbiddenError):
        await backend.check_mailbox(stranger, uid)

    message = Message.create(src=sender_a_uid, dest=uid, body=PingRequest())
    await backend.put(sender_a, message)
    received = await backend.get(owner, uid)
    assert received.header.groups == frozenset({group_a})

    await backend.terminate(owner, uid)
    await backend.terminate(sender_a, sender_a_uid)


@pytest.mark.asyncio
async def test_mailbox_backend_terminate_requires_ownership(
    backend: MailboxBackend,
) -> None:
    shared_group = str(uuid.uuid4())

    owner = ClientInfo(str(uuid.uuid4()), {shared_group})
    shared_user = ClientInfo(str(uuid.uuid4()), {shared_group})

    uid = UserId.new()
    await backend.create_mailbox(owner, uid)
    await backend.share_mailbox(owner, uid, shared_group)

    assert (
        await backend.check_mailbox(shared_user, uid)
    ) == MailboxStatus.ACTIVE

    with pytest.raises(ForbiddenError):
        await backend.terminate(shared_user, uid)

    assert (await backend.check_mailbox(owner, uid)) == MailboxStatus.ACTIVE

    await backend.terminate(owner, uid)
    assert (
        await backend.check_mailbox(owner, uid)
    ) == MailboxStatus.TERMINATED


@pytest.mark.asyncio
async def test_mailbox_backend_permitted_groups_enables_access(
    backend: MailboxBackend,
) -> None:
    permitted_group = str(uuid.uuid4())

    owner = ClientInfo(str(uuid.uuid4()), {permitted_group})
    permitted_sender = ClientInfo(str(uuid.uuid4()), {permitted_group})
    unauthorized = ClientInfo(str(uuid.uuid4()), set())

    uid = UserId.new()
    sender_uid = UserId.new()
    await backend.create_mailbox(
        owner,
        uid,
        permitted_groups={permitted_group},
    )
    await backend.create_mailbox(
        permitted_sender,
        sender_uid,
        permitted_groups={permitted_group},
    )

    message = Message.create(src=sender_uid, dest=uid, body=PingRequest())

    await backend.put(permitted_sender, message)
    received = await backend.get(owner, uid)
    assert received.header.groups == frozenset({permitted_group})

    with pytest.raises(ForbiddenError):
        await backend.put(unauthorized, message)

    await backend.terminate(owner, uid)
    await backend.terminate(permitted_sender, sender_uid)


@pytest.mark.asyncio
async def test_mailbox_backend_sender_must_own_source(
    backend: MailboxBackend,
) -> None:
    shared_group = str(uuid.uuid4())

    owner = ClientInfo(str(uuid.uuid4()), {shared_group})
    sender = ClientInfo(str(uuid.uuid4()), {shared_group})

    owner_mailbox = UserId.new()
    sender_mailbox = UserId.new()

    await backend.create_mailbox(
        owner,
        owner_mailbox,
        permitted_groups={shared_group},
    )
    await backend.create_mailbox(
        sender,
        sender_mailbox,
        permitted_groups={shared_group},
    )

    forged_message = Message.create(
        src=owner_mailbox,
        dest=owner_mailbox,
        body=PingRequest(),
    )

    with pytest.raises(ForbiddenError, match='Sender does not own source'):
        await backend.put(sender, forged_message)

    legitimate_message = Message.create(
        src=sender_mailbox,
        dest=owner_mailbox,
        body=PingRequest(),
    )
    await backend.put(sender, legitimate_message)

    await backend.terminate(owner, owner_mailbox)
    await backend.terminate(sender, sender_mailbox)


async def _make_ownerless(backend: MailboxBackend, uid: UserId) -> None:
    """Delete the owner record for *uid*.

    Simulates the pre-patch ownerless-but-live state.
    """
    if isinstance(backend, PythonBackend):
        backend._owners.pop(uid)
    else:
        assert isinstance(backend, RedisBackend)
        await backend._client.delete(backend._owner_key(uid))


@pytest.mark.asyncio
async def test_ownerless_mailbox_not_terminable(
    backend: MailboxBackend,
) -> None:
    shared_group = str(uuid.uuid4())
    owner = ClientInfo(str(uuid.uuid4()), {shared_group})
    stranger = ClientInfo(str(uuid.uuid4()), {shared_group})

    uid = UserId.new()
    await backend.create_mailbox(owner, uid)
    await backend.share_mailbox(owner, uid, shared_group)

    assert (await backend.check_mailbox(stranger, uid)) == MailboxStatus.ACTIVE

    await _make_ownerless(backend, uid)

    for client in (owner, stranger):
        with pytest.raises(ForbiddenError):
            await backend.terminate(client, uid)


@pytest.mark.asyncio
async def test_ownerless_mailbox_share_operations_forbidden(
    backend: MailboxBackend,
) -> None:
    shared_group = str(uuid.uuid4())
    extra_group = str(uuid.uuid4())
    owner = ClientInfo(str(uuid.uuid4()), {shared_group, extra_group})
    stranger = ClientInfo(str(uuid.uuid4()), {shared_group})

    uid = UserId.new()
    await backend.create_mailbox(owner, uid)
    await backend.share_mailbox(owner, uid, shared_group)

    await _make_ownerless(backend, uid)

    for client in (owner, stranger):
        with pytest.raises(ForbiddenError):
            await backend.share_mailbox(client, uid, extra_group)
        with pytest.raises(ForbiddenError):
            await backend.get_mailbox_shares(client, uid)
        with pytest.raises(ForbiddenError):
            await backend.remove_mailbox_shares(client, uid, shared_group)


@pytest.mark.asyncio
async def test_ownerless_mailbox_put_rejected(
    backend: MailboxBackend,
) -> None:
    owner = ClientInfo(str(uuid.uuid4()), set())
    sender = ClientInfo(str(uuid.uuid4()), set())

    owner_uid = UserId.new()
    sender_uid = UserId.new()
    await backend.create_mailbox(owner, owner_uid)
    await backend.create_mailbox(sender, sender_uid)

    await _make_ownerless(backend, owner_uid)

    message = Message.create(
        src=sender_uid,
        dest=owner_uid,
        body=PingRequest(),
    )
    with pytest.raises(ForbiddenError):
        await backend.put(sender, message)


@pytest.mark.asyncio
async def test_creation_still_works_and_no_ownership_transfer(
    backend: MailboxBackend,
) -> None:
    uid = UserId.new()
    owner = ClientInfo(str(uuid.uuid4()), set())
    stranger = ClientInfo(str(uuid.uuid4()), set())

    await backend.create_mailbox(owner, uid)
    assert (await backend.check_mailbox(owner, uid)) == MailboxStatus.ACTIVE

    await backend.create_mailbox(owner, uid)

    with pytest.raises(ForbiddenError):
        await backend.create_mailbox(stranger, uid)

    await backend.terminate(owner, uid)


@pytest.mark.asyncio
async def test_terminate_never_created_silent_noop_no_gravestone(
    backend: MailboxBackend,
) -> None:
    uid = UserId.new()
    client = ClientInfo(str(uuid.uuid4()), set())

    await backend.terminate(client, uid)

    assert await backend.check_mailbox(client, uid) == MailboxStatus.MISSING

    other = ClientInfo(str(uuid.uuid4()), set())
    await backend.create_mailbox(other, uid)
    assert (await backend.check_mailbox(other, uid)) == MailboxStatus.ACTIVE
    await backend.terminate(other, uid)


@pytest.mark.asyncio
async def test_missing_mailbox_status_not_forbidden(
    backend: MailboxBackend,
) -> None:
    uid = UserId.new()
    client = ClientInfo(str(uuid.uuid4()), set())
    assert await backend.check_mailbox(client, uid) == MailboxStatus.MISSING


@pytest.mark.asyncio
async def test_put_unknown_dest_still_raises_bad_entity_id(
    backend: MailboxBackend,
) -> None:
    client = ClientInfo(str(uuid.uuid4()), set())
    uid = UserId.new()
    sender_uid = UserId.new()
    await backend.create_mailbox(client, sender_uid)

    message = Message.create(
        src=sender_uid,
        dest=uid,  # never created
        body=PingRequest(),
    )
    with pytest.raises(BadEntityIdError):
        await backend.put(client, message)


@pytest.mark.asyncio
async def test_redis_terminate_refreshes_owner_ttl(mock_redis) -> None:
    backend = RedisBackend(gravestone_expiration_s=2)
    uid = UserId.new()
    owner = ClientInfo(str(uuid.uuid4()), set())

    await backend.create_mailbox(owner, uid)
    await asyncio.sleep(1.5)
    await backend.terminate(owner, uid)

    await asyncio.sleep(1)
    assert await backend.check_mailbox(owner, uid) == MailboxStatus.TERMINATED
    assert await backend._has_mailbox_ownership(owner, uid)


@pytest.mark.asyncio
async def test_redis_create_records_owner_before_active(mock_redis) -> None:
    backend = RedisBackend()
    uid = UserId.new()
    owner = ClientInfo(str(uuid.uuid4()), set())
    stranger = ClientInfo(str(uuid.uuid4()), set())

    active_key = backend._active_key(uid)
    owner_key = backend._owner_key(uid)
    real_set = backend._client.set

    async def failing_set(key: Any, *args: Any, **kwargs: Any) -> Any:
        if key == active_key:
            raise RuntimeError('simulated failure marking mailbox active')
        return await real_set(key, *args, **kwargs)

    backend._client.set = failing_set  # type: ignore[assignment]
    with pytest.raises(RuntimeError):
        await backend.create_mailbox(owner, uid)
    backend._client.set = real_set  # type: ignore[method-assign]

    assert await backend._client.get(owner_key) is not None
    assert await backend._client.get(active_key) is None

    assert await backend.check_mailbox(stranger, uid) == MailboxStatus.MISSING
    with pytest.raises(ForbiddenError):
        await backend.create_mailbox(stranger, uid)

    await backend.create_mailbox(owner, uid)
    assert await backend.check_mailbox(owner, uid) == MailboxStatus.ACTIVE
    assert await backend._client.get(owner_key) is not None
    await backend.terminate(owner, uid)


@pytest.mark.asyncio
async def test_terminate_already_terminated_is_noop_for_any_client(
    backend: MailboxBackend,
) -> None:
    """Both backends resolve status first, then ownership.

    PythonBackend used to skip the permission check entirely and fall
    straight through to its ownership check, so a stranger and the owner
    got different errors than they did on RedisBackend.
    """
    owner = ClientInfo(str(uuid.uuid4()), set())
    stranger = ClientInfo(str(uuid.uuid4()), set())

    uid = UserId.new()
    await backend.create_mailbox(owner, uid)
    await backend.terminate(owner, uid)

    # The owner re-terminating is a no-op; a stranger is refused before
    # learning anything, on both backends.
    await backend.terminate(owner, uid)
    with pytest.raises(ForbiddenError):
        await backend.terminate(stranger, uid)
    assert await backend.check_mailbox(owner, uid) == MailboxStatus.TERMINATED


@pytest.mark.asyncio
async def test_unknown_mailbox_reports_missing_not_forbidden(
    backend: MailboxBackend,
) -> None:
    """Existence is resolved before permissions.

    A client with no relationship to an id must be able to tell "never
    existed" from "exists but is not yours", otherwise every reference to
    an unregistered id looks like a misconfigured permission.
    """
    owner = ClientInfo(str(uuid.uuid4()), set())
    stranger = ClientInfo(str(uuid.uuid4()), set())

    unknown = UserId.new()
    assert await backend.check_mailbox(stranger, unknown) == (
        MailboxStatus.MISSING
    )
    with pytest.raises(BadEntityIdError):
        await backend.get(stranger, unknown)

    known = UserId.new()
    await backend.create_mailbox(owner, known)
    with pytest.raises(ForbiddenError):
        await backend.check_mailbox(stranger, known)
    with pytest.raises(ForbiddenError):
        await backend.get(stranger, known)


@pytest.mark.asyncio
async def test_redis_concurrent_register_only_one_owner(mock_redis) -> None:
    """Exactly one client wins the `nx=True` owner write.

    Because ownership now fails closed, the loser is refused outright
    rather than silently completing a registration it does not own.
    """
    backend = RedisBackend()
    uid = UserId.new()
    group_a, group_b = str(uuid.uuid4()), str(uuid.uuid4())
    first = ClientInfo(str(uuid.uuid4()), {group_a})
    second = ClientInfo(str(uuid.uuid4()), {group_b})

    results = await asyncio.gather(
        backend.create_mailbox(first, uid, permitted_groups={group_a}),
        backend.create_mailbox(second, uid, permitted_groups={group_b}),
        return_exceptions=True,
    )
    errors = [r for r in results if isinstance(r, BaseException)]
    assert all(isinstance(e, ForbiddenError) for e in errors)

    assert await backend.check_mailbox(first, uid) == MailboxStatus.ACTIVE
    owners = [
        client
        for client in (first, second)
        if await backend._has_mailbox_ownership(client, uid)
    ]
    assert len(owners) == 1

    loser = second if owners[0] is first else first
    with pytest.raises(ForbiddenError):
        await backend.terminate(loser, uid)
