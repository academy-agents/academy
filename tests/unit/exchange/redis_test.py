from __future__ import annotations

import pickle

import pytest

from academy.exchange.redis import _RedisConnectionInfo
from academy.exchange.redis import RedisExchangeFactory
from academy.exchange.redis import RedisExchangeTransport
from academy.message import Message
from academy.message import PingRequest
from academy.message import SuccessResponse
from academy.request_state import RequestStatus


def test_factory_serialize(
    redis_exchange_factory: RedisExchangeFactory,
) -> None:
    pickled = pickle.dumps(redis_exchange_factory)
    reconstructed = pickle.loads(pickled)
    assert isinstance(reconstructed, RedisExchangeFactory)


@pytest.mark.asyncio
async def test_redis_exchange_request_tracking_inflight(mock_redis) -> None:
    """Test that request status is set to INFLIGHT when sending a request."""
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

    # Verify request is tracked with INFLIGHT status
    assert message.tag in transport1._requests
    assert transport1._requests[message.tag].status == RequestStatus.INFLIGHT
    assert transport1._requests[message.tag].src == transport1.mailbox_id
    assert transport1._requests[message.tag].dest == transport1.mailbox_id

    await transport1.close()


@pytest.mark.asyncio
async def test_redis_exchange_request_tracking_completed(mock_redis) -> None:
    """Test response updates request status to COMPLETED."""
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

    # Verify request is INFLIGHT
    assert (
        transport1._requests[request_msg.tag].status == RequestStatus.INFLIGHT
    )

    # Send a response to itself
    response_msg = request_msg.create_response(SuccessResponse())
    await transport1.send(response_msg)

    # Verify request status is updated to COMPLETED
    assert (
        transport1._requests[request_msg.tag].status == RequestStatus.COMPLETED
    )

    await transport1.close()
