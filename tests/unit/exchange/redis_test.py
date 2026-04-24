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
async def test_redis_exchange_response_without_request(mock_redis) -> None:
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
