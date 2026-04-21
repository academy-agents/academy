from __future__ import annotations

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
    import json
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

    # Should not raise an error when sending a response without a request
    await transport.send(response_msg)

    # Verify the response was not tracked in Redis
    request_key = f'request:{response_msg.tag}'
    info_data = await transport._client.get(request_key)
    assert info_data is None

    await transport.close()

    await transport.close()
