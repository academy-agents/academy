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

    request_key = f'request:{request.dest.uid}:{request.tag}'
    request_data = await transport1._client.get(request_key)
    assert request_data is not None

    response = request.create_response(SuccessResponse())
    await transport2.send(response)
    remaining = await transport1._client.get(request_key)
    assert remaining is None
    await transport1.close()
    await transport2.close()


@pytest.mark.asyncio
async def test_redis_exchange_agent_stats(mock_redis) -> None:
    redis_info = _RedisConnectionInfo(hostname='localhost', port=0, kwargs={})
    sender = await RedisExchangeTransport.new(redis_info=redis_info)
    agent = await RedisExchangeTransport.new(redis_info=redis_info)

    req = Message.create(
        src=sender.mailbox_id,
        dest=agent.mailbox_id,
        body=PingRequest(),
    )
    await sender.send(req)

    stats = await sender.agent_stats(agent.mailbox_id)
    assert stats.incoming == 1
    assert stats.queued == 1

    await agent.send(req.create_response(SuccessResponse()))
    assert (await sender.agent_stats(agent.mailbox_id)).completed == 1
    assert (await agent.agent_stats(sender.mailbox_id)).outgoing == 1

    await sender.close()
    await agent.close()


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
    response_key = f'request:{response_msg.dest.uid}:{response_msg.tag}'
    tracked = await transport1._client.get(response_key)
    assert tracked is None

    await transport1.close()
    await transport2.close()
