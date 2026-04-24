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
async def test_redis_exchange_request_tracking(mock_redis) -> None:
    redis_info = _RedisConnectionInfo(
        hostname='localhost',
        port=0,
        kwargs={},
    )

    transport1 = await RedisExchangeTransport.new(redis_info=redis_info)
    transport2 = await RedisExchangeTransport.new(redis_info=redis_info)

    request = Message.create(
        src=transport1.mailbox_id,
        dest=transport2.mailbox_id,
        body=PingRequest(),
    )
    await transport1.send(request)
    request_key = f'request:{request.dest.uid}'
    request_data = await transport1._client.get(request_key)
    assert request_data is not None

    response = request.create_response(SuccessResponse())
    await transport2.send(response)
    response_list = await transport1._client.get(request_key)
    assert response_list is None
    await transport1.close()
    await transport2.close()


@pytest.mark.asyncio
async def test_redis_exchange_multiple_requests_partial_response(
    mock_redis,
) -> None:
    redis_info = _RedisConnectionInfo(
        hostname='localhost',
        port=0,
        kwargs={},
    )
    transport1 = await RedisExchangeTransport.new(redis_info=redis_info)
    transport2 = await RedisExchangeTransport.new(redis_info=redis_info)

    # Send two requests from transport1 to transport2
    request1 = Message.create(
        src=transport1.mailbox_id,
        dest=transport2.mailbox_id,
        body=PingRequest(),
    )
    request2 = Message.create(
        src=transport1.mailbox_id,
        dest=transport2.mailbox_id,
        body=PingRequest(),
    )
    await transport1.send(request1)
    await transport1.send(request2)

    response1 = request1.create_response(SuccessResponse())
    await transport2.send(response1)

    request_key = f'request:{transport2.mailbox_id.uid}'
    data = await transport1._client.get(request_key)
    assert data is not None  # request2 is still tracked

    await transport1.close()
    await transport2.close()


@pytest.mark.asyncio
async def test_redis_exchange_response_tag_mismatch(mock_redis) -> None:
    redis_info = _RedisConnectionInfo(
        hostname='localhost',
        port=0,
        kwargs={},
    )
    transport1 = await RedisExchangeTransport.new(redis_info=redis_info)
    transport2 = await RedisExchangeTransport.new(redis_info=redis_info)

    # Send two requests so key exists with two entries
    request1 = Message.create(
        src=transport1.mailbox_id,
        dest=transport2.mailbox_id,
        body=PingRequest(),
    )
    request2 = Message.create(
        src=transport1.mailbox_id,
        dest=transport2.mailbox_id,
        body=PingRequest(),
    )
    await transport1.send(request1)
    await transport1.send(request2)

    # Partially remove one — key still exists with request2 tracked
    response1 = request1.create_response(SuccessResponse())
    await transport2.send(response1)

    # Send a response with a tag that doesn't match any tracked request
    unmatched_response = Message.create(
        src=transport2.mailbox_id,
        dest=transport1.mailbox_id,
        body=SuccessResponse(),
    )
    await transport2.send(unmatched_response)

    # Key should still exist (unmatched response didn't remove request2)
    request_key = f'request:{transport2.mailbox_id.uid}'
    data = await transport1._client.get(request_key)
    assert data is not None

    await transport1.close()
    await transport2.close()
    redis_info = _RedisConnectionInfo(
        hostname='localhost',
        port=0,
        kwargs={},
    )
    transport1 = await RedisExchangeTransport.new(redis_info=redis_info)
    transport2 = await RedisExchangeTransport.new(redis_info=redis_info)

    # Send a response without a corresponding request
    response_msg = Message.create(
        src=transport1.mailbox_id,
        dest=transport2.mailbox_id,
        body=SuccessResponse(),
    )
    await transport1.send(response_msg)

    # Response without request should not create any tracking entry
    response_key = f'request:{response_msg.dest.uid}'
    tracked = await transport1._client.get(response_key)
    assert tracked is None

    await transport1.close()
    await transport2.close()
