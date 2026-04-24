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
    redis_info = _RedisConnectionInfo(
        hostname='localhost',
        port=0,
        kwargs={},
    )
    transport1 = await RedisExchangeTransport.new(redis_info=redis_info)
    transport2 = await RedisExchangeTransport.new(redis_info=redis_info)

    message = Message.create(
        src=transport1.mailbox_id,
        dest=transport2.mailbox_id,
        body=PingRequest(),
    )
    await transport1.send(message)

    listener = transport2.listen(timeout=1.0)
    received = await anext(listener)
    assert received.tag == message.tag
    assert isinstance(received.get_body(), PingRequest)

    await transport1.close()
    await transport2.close()


@pytest.mark.asyncio
async def test_redis_exchange_request_tracking_completed(mock_redis) -> None:
    redis_info = _RedisConnectionInfo(
        hostname='localhost',
        port=0,
        kwargs={},
    )
    transport1 = await RedisExchangeTransport.new(redis_info=redis_info)
    transport2 = await RedisExchangeTransport.new(redis_info=redis_info)

    request_msg = Message.create(
        src=transport1.mailbox_id,
        dest=transport2.mailbox_id,
        body=PingRequest(),
    )
    await transport1.send(request_msg)

    listener2 = transport2.listen(timeout=1.0)
    received_request = await anext(listener2)
    assert received_request.tag == request_msg.tag

    response_msg = received_request.create_response(SuccessResponse())
    await transport2.send(response_msg)

    listener1 = transport1.listen(timeout=1.0)
    received_response = await anext(listener1)
    assert received_response.tag == request_msg.tag
    assert isinstance(received_response.get_body(), SuccessResponse)

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

    # Create and send a response message (without a prior request)
    response_msg = Message.create(
        src=transport1.mailbox_id,
        dest=transport2.mailbox_id,
        body=SuccessResponse(),
    )
    await transport1.send(response_msg)

    listener = transport2.listen(timeout=1.0)
    received = await anext(listener)
    assert received.tag == response_msg.tag
    assert isinstance(received.get_body(), SuccessResponse)

    await transport1.close()
    await transport2.close()
