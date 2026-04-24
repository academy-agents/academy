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
async def test_local_exchange_request_tracking(
    local_exchange_factory: LocalExchangeFactory,
) -> None:
    transport1 = await local_exchange_factory._create_transport()
    transport2 = await local_exchange_factory._create_transport()
    state = local_exchange_factory._state

    request = Message.create(
        src=transport1.mailbox_id,
        dest=transport2.mailbox_id,
        body=PingRequest(),
    )
    await transport1.send(request)
    assert transport2.mailbox_id in state.requests

    request_list = state.requests[transport2.mailbox_id]
    tracked = next((m for m in request_list if m.tag == request.tag), None)
    assert tracked is not None
    assert tracked.src == transport1.mailbox_id
    assert tracked.dest == transport2.mailbox_id

    response = request.create_response(SuccessResponse())
    await transport2.send(response)

    response_list = state.requests.get(transport1.mailbox_id, [])
    still_tracked = any(m.tag == request.tag for m in response_list)
    assert not still_tracked

    await transport1.close()
    await transport2.close()


@pytest.mark.asyncio
async def test_local_exchange_response_without_request(
    local_exchange_factory: LocalExchangeFactory,
) -> None:
    transport1 = await local_exchange_factory._create_transport()
    transport2 = await local_exchange_factory._create_transport()
    state = local_exchange_factory._state

    # Send a response without a corresponding request
    response = Message.create(
        src=transport1.mailbox_id,
        dest=transport2.mailbox_id,
        body=SuccessResponse(),
    )
    await transport1.send(response)

    assert transport2.mailbox_id not in state.requests
