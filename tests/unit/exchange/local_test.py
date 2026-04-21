from __future__ import annotations

import pickle

import pytest

from academy.exchange.local import LocalExchangeFactory
from academy.message import Message
from academy.message import PingRequest
from academy.message import SuccessResponse


def test_factory_serialize_error(
    local_exchange_factory: LocalExchangeFactory,
) -> None:
    with pytest.raises(pickle.PicklingError):
        pickle.dumps(local_exchange_factory)


@pytest.mark.asyncio
async def test_local_exchange_request_tracking_inflight(
    local_exchange_factory: LocalExchangeFactory,
) -> None:
    """Test that sending a request adds it to the in-flight dict."""
    transport = await local_exchange_factory.create_user_client()
    state = local_exchange_factory._state

    # Send a request to itself
    message = Message.create(
        src=transport.client_id,
        dest=transport.client_id,
        body=PingRequest(),
    )
    await transport.send(message)

    # Verify request is tracked as in-flight
    assert message.tag in state.requests
    assert state.requests[message.tag].src == transport.client_id
    assert state.requests[message.tag].dest == transport.client_id


@pytest.mark.asyncio
async def test_local_exchange_request_tracking_completed(
    local_exchange_factory: LocalExchangeFactory,
) -> None:
    """Test response removes the request from the in-flight dict."""
    transport = await local_exchange_factory.create_user_client()
    state = local_exchange_factory._state

    # Send a request to itself
    request_msg = Message.create(
        src=transport.client_id,
        dest=transport.client_id,
        body=PingRequest(),
    )
    await transport.send(request_msg)

    # Verify request is in-flight
    assert request_msg.tag in state.requests

    # Send a response to itself
    response_msg = request_msg.create_response(SuccessResponse())
    await transport.send(response_msg)

    # Verify request is removed from in-flight dict
    assert request_msg.tag not in state.requests


@pytest.mark.asyncio
async def test_local_exchange_response_without_request(
    local_exchange_factory: LocalExchangeFactory,
) -> None:
    """Test that response without prior request is handled gracefully."""
    transport = await local_exchange_factory.create_user_client()
    state = local_exchange_factory._state

    # Create a response message with a tag that was never sent as a request
    request_msg = Message.create(
        src=transport.client_id,
        dest=transport.client_id,
        body=PingRequest(),
    )
    # Create a response but don't send the original request
    response_msg = request_msg.create_response(SuccessResponse())

    # Should not raise an error when sending a response without a request
    await transport.send(response_msg)

    # Verify the response was still queued but not tracked in requests
    assert response_msg.tag not in state.requests
