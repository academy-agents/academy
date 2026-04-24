from __future__ import annotations

import asyncio
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
    assert isinstance(message.body.exception, MailboxTerminatedError)

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
        request_list = backend._requests[receiver_uid]
        tracked = next((m for m in request_list if m.tag == request.tag), None)
        assert tracked is not None
        assert tracked.src == sender_uid
        assert tracked.dest == receiver_uid
    else:
        assert isinstance(backend, RedisBackend)
        request_key = f'request:{receiver_uid.uid}'
        info_data = await backend._client.get(request_key)
        assert info_data is not None

    response = request.create_response(SuccessResponse())
    await backend.put(client, response)

    if isinstance(backend, PythonBackend):
        response_list = backend._requests.get(sender_uid, [])
        still_tracked = any(m.tag == request.tag for m in response_list)
        assert not still_tracked
    else:
        assert isinstance(backend, RedisBackend)
        request_key = f'request:{receiver_uid.uid}'
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
async def test_mailbox_backend_multiple_requests_partial_response(
    backend: MailboxBackend,
) -> None:
    client = ClientInfo(str(uuid.uuid4()), set())
    sender_uid = UserId.new()
    receiver_uid = UserId.new()
    await backend.create_mailbox(client, sender_uid)
    await backend.create_mailbox(client, receiver_uid)

    request1 = Message.create(
        src=sender_uid,
        dest=receiver_uid,
        body=PingRequest(),
    )
    request2 = Message.create(
        src=sender_uid,
        dest=receiver_uid,
        body=PingRequest(),
    )
    await backend.put(client, request1)
    await backend.put(client, request2)

    # Respond to only the first request — second should remain tracked
    response1 = request1.create_response(SuccessResponse())
    await backend.put(client, response1)

    if isinstance(backend, PythonBackend):
        assert receiver_uid in backend._requests
        assert any(
            h.tag == request2.tag for h in backend._requests[receiver_uid]
        )
    else:
        assert isinstance(backend, RedisBackend)
        request_key = f'request:{receiver_uid.uid}'
        data = await backend._client.get(request_key)
        assert data is not None  # request2 is still tracked


@pytest.mark.asyncio
async def test_mailbox_backend_response_tag_mismatch(
    backend: MailboxBackend,
) -> None:
    client = ClientInfo(str(uuid.uuid4()), set())
    sender_uid = UserId.new()
    receiver_uid = UserId.new()
    await backend.create_mailbox(client, sender_uid)
    await backend.create_mailbox(client, receiver_uid)

    request1 = Message.create(
        src=sender_uid,
        dest=receiver_uid,
        body=PingRequest(),
    )
    request2 = Message.create(
        src=sender_uid,
        dest=receiver_uid,
        body=PingRequest(),
    )
    await backend.put(client, request1)
    await backend.put(client, request2)

    # Partially remove one so key still exists
    response1 = request1.create_response(SuccessResponse())
    await backend.put(client, response1)

    # Send a response with a tag that doesn't match any tracked request
    unmatched_response = Message.create(
        src=receiver_uid,
        dest=sender_uid,
        body=SuccessResponse(),
    )
    await backend.put(client, unmatched_response)

    if isinstance(backend, PythonBackend):
        # request2 is still tracked (unmatched response didn't remove it)
        assert receiver_uid in backend._requests
        assert any(
            h.tag == request2.tag for h in backend._requests[receiver_uid]
        )
    else:
        assert isinstance(backend, RedisBackend)
        request_key = f'request:{receiver_uid.uid}'
        data = await backend._client.get(request_key)
        assert data is not None  # request2 still tracked
