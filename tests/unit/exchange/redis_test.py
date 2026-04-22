from __future__ import annotations

import json
import pickle

import pytest

from academy.exchange.redis import _RedisConnectionInfo
from academy.exchange.redis import RedisExchangeFactory
from academy.exchange.redis import RedisExchangeTransport
from academy.message import Message
from academy.message import PingRequest
from academy.message import SuccessResponse


def test_factory_serialize(
    redis_exchange_factory: RedisExchangeFactory,
) -> None:
    pickled = pickle.dumps(redis_exchange_factory)
    reconstructed = pickle.loads(pickled)
    assert isinstance(reconstructed, RedisExchangeFactory)


@pytest.mark.asyncio
async def test_redis_exchange_request_tracking_inflight(mock_redis) -> None:
    """Test that sending a request adds it to Redis."""
    redis_info = _RedisConnectionInfo(
        hostname='localhost',
        port=0,
        kwargs={},
    )
    transport1 = await RedisExchangeTransport.new(redis_info=redis_info)

    # Send a request to itself
    message = Message.create(
        src=transport1.mailbox_id,
        dest=transport1.mailbox_id,
        body=PingRequest(),
    )
    await transport1.send(message)

    # Verify request is tracked in Redis
    request_key = f'request:{message.tag}'
    info_data = await transport1._client.get(request_key)
    assert info_data is not None
    info_dict = json.loads(info_data)
    assert info_dict['src'] == transport1.mailbox_id.model_dump(mode='json')
    assert info_dict['dest'] == transport1.mailbox_id.model_dump(mode='json')

    await transport1.close()


@pytest.mark.asyncio
async def test_redis_exchange_request_tracking_completed(mock_redis) -> None:
    """Test response removes the request from Redis."""
    redis_info = _RedisConnectionInfo(
        hostname='localhost',
        port=0,
        kwargs={},
    )
    transport1 = await RedisExchangeTransport.new(redis_info=redis_info)

    # Send a request to itself
    request_msg = Message.create(
        src=transport1.mailbox_id,
        dest=transport1.mailbox_id,
        body=PingRequest(),
    )
    await transport1.send(request_msg)

    # Verify request is in Redis
    request_key = f'request:{request_msg.tag}'
    info_data = await transport1._client.get(request_key)
    assert info_data is not None

    # Send a response to itself
    response_msg = request_msg.create_response(SuccessResponse())
    await transport1.send(response_msg)

    # Verify request is removed from Redis
    info_data = await transport1._client.get(request_key)
    assert info_data is None

    await transport1.close()


@pytest.mark.asyncio
async def test_redis_exchange_response_without_request(mock_redis) -> None:
    """Test that response without prior request is handled gracefully."""
    redis_info = _RedisConnectionInfo(
        hostname='localhost',
        port=0,
        kwargs={},
    )
    transport = await RedisExchangeTransport.new(redis_info=redis_info)

    # Create a response message with a tag that was never sent as a request
    request_msg = Message.create(
        src=transport.mailbox_id,
        dest=transport.mailbox_id,
        body=PingRequest(),
    )
    # Create a response but don't send the original request
    response_msg = request_msg.create_response(SuccessResponse())

    await transport._client.delete(
        transport._request_key(response_msg.tag),
    )

    # Should not raise an error when sending a response without a request
    await transport.send(response_msg)

    # Verify the response was not tracked in Redis
    request_key = f'request:{response_msg.tag}'
    info_data = await transport._client.get(request_key)
    assert info_data is None

    await transport.close()


@pytest.mark.asyncio
async def test_redis_exchange_terminate_skips_other_mailbox_requests(
    mock_redis,
) -> None:
    """Test terminate does not reply to requests for other mailboxes."""
    redis_info = _RedisConnectionInfo(
        hostname='localhost',
        port=0,
        kwargs={},
    )
    transport1 = await RedisExchangeTransport.new(redis_info=redis_info)
    transport2 = await RedisExchangeTransport.new(redis_info=redis_info)

    # Send a request destined for transport2 (not transport1)
    other_request = Message.create(
        src=transport1.mailbox_id,
        dest=transport2.mailbox_id,
        body=PingRequest(),
    )
    await transport1.send(other_request)

    # Terminate transport1 — should not touch transport2's in-flight request
    await transport1.terminate(transport1.mailbox_id)

    request_key = f'request:{other_request.tag}'
    info_data = await transport1._client.get(request_key)
    assert info_data is not None

    await transport2.close()


@pytest.mark.asyncio
async def test_redis_exchange_terminate_replies_to_pending_requests(
    mock_redis,
) -> None:
    """Terminate replies with MailboxTerminatedError to in-flight requests."""
    redis_info = _RedisConnectionInfo(
        hostname='localhost',
        port=0,
        kwargs={},
    )
    transport1 = await RedisExchangeTransport.new(redis_info=redis_info)
    transport2 = await RedisExchangeTransport.new(redis_info=redis_info)

    # transport1 sends a request destined for transport2
    request_msg = Message.create(
        src=transport1.mailbox_id,
        dest=transport2.mailbox_id,
        body=PingRequest(),
    )
    await transport1.send(request_msg)

    # Terminate transport2 — loop should find request where dest == uid
    # and reply with MailboxTerminatedError to transport1
    await transport2.terminate(transport2.mailbox_id)

    # The request key should have been cleaned up
    request_key = f'request:{request_msg.tag}'
    info_data = await transport1._client.get(request_key)
    assert info_data is None

    await transport1.close()
