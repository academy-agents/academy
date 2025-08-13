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
    user_id = str(uuid.uuid4())
    uid = UserId.new()
    # Should do nothing since mailbox doesn't exist
    await backend.terminate(user_id, uid)
    assert await backend.check_mailbox(user_id, uid) == MailboxStatus.MISSING
    await backend.create_mailbox(user_id, uid)
    assert await backend.check_mailbox(user_id, uid) == MailboxStatus.ACTIVE
    await backend.create_mailbox(user_id, uid)  # Idempotent check

    bad_user = str(uuid.uuid4())  # Authentication check
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
    user_id = str(uuid.uuid4())
    bad_user = str(uuid.uuid4())
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
    message = Message.create(src=uid, dest=uid, body=PingRequest())
    with pytest.raises(BadEntityIdError):
        await backend.get('', uid)

    with pytest.raises(BadEntityIdError):
        await backend.put('', message)


@pytest.mark.asyncio
async def test_mailbox_backend_mailbox_closed(backend: MailboxBackend) -> None:
    uid = UserId.new()
    await backend.create_mailbox(str(uid), uid)
    await backend.terminate(str(uid), uid)
    message = Message.create(src=uid, dest=uid, body=PingRequest())
    with pytest.raises(MailboxTerminatedError):
        await backend.get(str(uid), uid)

    with pytest.raises(MailboxTerminatedError):
        await backend.put(str(uid), message)


@pytest.mark.asyncio
async def test_mailbox_backend_mailbox_create_forbidden(
    backend: MailboxBackend,
) -> None:
    uid = UserId.new()
    await backend.create_mailbox('me', uid)
    with pytest.raises(ForbiddenError):
        await backend.create_mailbox('not_me', uid)


@pytest.mark.asyncio
async def test_mailbox_backend_mailbox_delete_agent(
    backend: MailboxBackend,
) -> None:
    uid = UserId.new()
    aid: AgentId[Any] = AgentId.new()
    await backend.create_mailbox(str(uid), uid)
    await backend.create_mailbox(str(uid), aid, ('EmptyAgent',))

    request = Message.create(src=uid, dest=aid, body=PingRequest())
    await backend.put(str(uid), request)
    response = Message.create(src=uid, dest=aid, body=SuccessResponse())
    await backend.put(str(uid), response)
    await backend.terminate(str(uid), aid)

    message = await backend.get(str(uid), uid, timeout=0.01)
    assert isinstance(message.get_body(), ErrorResponse)
    assert isinstance(message.body.exception, MailboxTerminatedError)

    with pytest.raises(TimeoutError):
        await backend.get(str(uid), uid, timeout=0.01)


@pytest.mark.asyncio
async def test_mailbox_backend_discover(backend: MailboxBackend) -> None:
    aid1: AgentId[Any] = AgentId.new()
    aid2: AgentId[Any] = AgentId.new()
    await backend.create_mailbox('me', aid1, ('EmptyAgent',))
    await backend.create_mailbox('me', aid2, ('SubClass', 'EmptyAgent'))

    agents = await backend.discover('me', 'EmptyAgent', False)
    assert aid1 in agents
    assert aid2 not in agents

    agents = await backend.discover('me', 'EmptyAgent', True)
    assert aid1 in agents
    assert aid2 in agents

    agents = await backend.discover('not_me', 'EmptyAgent', True)
    assert len(agents) == 0


@pytest.mark.asyncio
async def test_mailbox_backend_timeout(backend: MailboxBackend) -> None:
    uid = UserId.new()
    await backend.create_mailbox(str(uid), uid)

    with pytest.raises(TimeoutError):
        await backend.get(str(uid), uid, timeout=0.01)


@pytest.mark.asyncio
async def test_python_backend_message_size() -> None:
    backend = PythonBackend(message_size_limit_kb=0)
    uid = UserId.new()
    await backend.create_mailbox(str(uid), uid)
    message = Message.create(src=uid, dest=uid, body=PingRequest())
    with pytest.raises(MessageTooLargeError):
        await backend.put(str(uid), message)


@pytest.mark.asyncio
async def test_redis_backend_message_size(mock_redis) -> None:
    backend = RedisBackend(message_size_limit_kb=0)
    uid = UserId.new()
    await backend.create_mailbox(str(uid), uid)
    message = Message.create(src=uid, dest=uid, body=PingRequest())
    with pytest.raises(MessageTooLargeError):
        await backend.put(str(uid), message)


@pytest.mark.asyncio
async def test_redis_backend_gravestone_expire(mock_redis) -> None:
    backend = RedisBackend(gravestone_expiration_s=1)
    user_id = str(uuid.uuid4())
    uid = UserId.new()
    await backend.create_mailbox(user_id, uid)
    await asyncio.sleep(2)
    assert await backend.check_mailbox(user_id, uid) == MailboxStatus.MISSING

    # Mailbox does not expire because of get call
    await backend.create_mailbox(user_id, uid)
    message = Message.create(src=uid, dest=uid, body=PingRequest())
    await backend.put(user_id, message)
    await asyncio.sleep(0.5)
    assert await backend.get(user_id, uid) == message
    await asyncio.sleep(0.5)
    assert await backend.check_mailbox(user_id, uid) == MailboxStatus.ACTIVE

    await backend.terminate(user_id, uid)
    assert (
        await backend.check_mailbox(user_id, uid) == MailboxStatus.TERMINATED
    )
    await asyncio.sleep(2)
    assert await backend.check_mailbox(user_id, uid) == MailboxStatus.MISSING


@pytest.mark.asyncio
async def test_redis_backend_mailbox_expire(mock_redis) -> None:
    backend = RedisBackend(mailbox_expiration_s=1)
    user_id = str(uuid.uuid4())
    uid = UserId.new()
    await backend.create_mailbox(user_id, uid)
    message = Message.create(src=uid, dest=uid, body=PingRequest())

    # Message expires after 2 seconds
    await backend.put(user_id, message)
    await asyncio.sleep(2)
    with pytest.raises(TimeoutError):
        await backend.get(user_id, uid, timeout=0.01)

    # Get extends expiration
    await backend.put(user_id, message)
    await backend.put(user_id, message)
    await backend.put(user_id, message)
    await asyncio.sleep(0.5)
    assert await backend.get(user_id, uid) == message
    await asyncio.sleep(0.5)
    assert await backend.get(user_id, uid) == message
    await asyncio.sleep(2)
    with pytest.raises(TimeoutError):
        await backend.get(user_id, uid, timeout=0.01)

    await backend.terminate(user_id, uid)
