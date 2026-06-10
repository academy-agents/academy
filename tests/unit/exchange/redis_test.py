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

    # Zero state before any messages
    stats = await sender.agent_stats(agent.mailbox_id)
    assert stats.incoming == 0
    assert stats.queued == 0
    assert stats.completed == 0

    # Two requests queued at agent
    req1 = Message.create(
        src=sender.mailbox_id,
        dest=agent.mailbox_id,
        body=PingRequest(),
    )
    req2 = Message.create(
        src=sender.mailbox_id,
        dest=agent.mailbox_id,
        body=PingRequest(),
    )
    await sender.send(req1)
    await sender.send(req2)

    stats = await sender.agent_stats(agent.mailbox_id)
    assert stats.incoming == 2  # noqa: PLR2004
    assert stats.queued == 2  # noqa: PLR2004
    assert stats.in_progress == 0
    assert stats.completed == 0

    # Respond to one request: completed rises, pending drops
    resp = req1.create_response(SuccessResponse())
    await agent.send(resp)

    stats = await sender.agent_stats(agent.mailbox_id)
    assert stats.completed == 1
    assert stats.incoming == 2  # noqa: PLR2004

    # Sender's outgoing reflects requests it sent
    sender_stats = await agent.agent_stats(sender.mailbox_id)
    assert sender_stats.outgoing == 2  # noqa: PLR2004

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
