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
    factory = local_exchange_factory
    transport1 = await factory._create_transport()
    transport2 = await factory._create_transport()

    # Send a request
    request = Message.create(
        src=transport1.mailbox_id,
        dest=transport2.mailbox_id,
        body=PingRequest(),
    )
    await transport1.send(request)

    # Verify it's received
    listener = transport2.listen(timeout=0.01)
    received = await anext(listener)
    assert received.tag == request.tag
    assert received.src == transport1.mailbox_id
    assert isinstance(received.get_body(), PingRequest)

    await transport1.close()
    await transport2.close()


@pytest.mark.asyncio
async def test_local_exchange_request_tracking_completed(
    local_exchange_factory: LocalExchangeFactory,
) -> None:
    factory = local_exchange_factory
    transport1 = await factory._create_transport()
    transport2 = await factory._create_transport()

    request = Message.create(
        src=transport1.mailbox_id,
        dest=transport2.mailbox_id,
        body=PingRequest(),
    )
    await transport1.send(request)

    listener2 = transport2.listen(timeout=0.01)
    received_request = await anext(listener2)
    assert received_request.tag == request.tag

    response = received_request.create_response(SuccessResponse())
    await transport2.send(response)

    listener1 = transport1.listen(timeout=0.01)
    received_response = await anext(listener1)
    assert received_response.tag == request.tag
    assert isinstance(received_response.get_body(), SuccessResponse)

    await transport1.close()
    await transport2.close()


@pytest.mark.asyncio
async def test_local_exchange_response_without_request(
    local_exchange_factory: LocalExchangeFactory,
) -> None:
    factory = local_exchange_factory
    transport1 = await factory._create_transport()
    transport2 = await factory._create_transport()

    response = Message.create(
        src=transport1.mailbox_id,
        dest=transport2.mailbox_id,
        body=SuccessResponse(),
    )
    await transport1.send(response)

    listener = transport2.listen(timeout=0.01)
    received = await anext(listener)
    assert received.tag == response.tag
    assert isinstance(received.get_body(), SuccessResponse)

    await transport1.close()
    await transport2.close()
