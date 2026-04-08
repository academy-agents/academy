from __future__ import annotations

import pickle

import pytest

from academy.exchange.local import LocalExchangeFactory
from academy.message import Message
from academy.message import PingRequest
from academy.message import SuccessResponse
from academy.request_state import RequestStatus


def test_factory_serialize_error(
    local_exchange_factory: LocalExchangeFactory,
) -> None:
    with pytest.raises(pickle.PicklingError):
        pickle.dumps(local_exchange_factory)


@pytest.mark.asyncio
async def test_local_exchange_request_tracking_inflight(
    local_exchange_factory: LocalExchangeFactory,
) -> None:
    """Test that request status is set to INFLIGHT when sending a request."""
    transport = await local_exchange_factory.create_user_client()
    state = local_exchange_factory._state

    # Send a request to itself
    message = Message.create(
        src=transport.client_id,
        dest=transport.client_id,
        body=PingRequest(),
    )
    await transport.send(message)

    # Verify request is tracked with INFLIGHT status
    assert message.tag in state.requests
    assert state.requests[message.tag].status == RequestStatus.INFLIGHT
    assert state.requests[message.tag].src == transport.client_id
    assert state.requests[message.tag].dest == transport.client_id


@pytest.mark.asyncio
async def test_local_exchange_request_tracking_completed(
    local_exchange_factory: LocalExchangeFactory,
) -> None:
    """Test response updates request status to COMPLETED."""
    transport = await local_exchange_factory.create_user_client()
    state = local_exchange_factory._state

    # Send a request to itself
    request_msg = Message.create(
        src=transport.client_id,
        dest=transport.client_id,
        body=PingRequest(),
    )
    await transport.send(request_msg)

    # Verify request is INFLIGHT
    assert state.requests[request_msg.tag].status == RequestStatus.INFLIGHT

    # Send a response to itself
    response_msg = request_msg.create_response(SuccessResponse())
    await transport.send(response_msg)

    # Verify request status is updated to COMPLETED
    assert state.requests[request_msg.tag].status == RequestStatus.COMPLETED
