from __future__ import annotations

import pickle
from typing import Any
from typing import get_args

import pytest

from academy.identifier import AgentId
from academy.identifier import UserId
from academy.message import ActionRequest
from academy.message import ActionResponse
from academy.message import BaseMessage
from academy.message import Message
from academy.message import PingRequest
from academy.message import PingResponse
from academy.message import RequestMessage
from academy.message import ResponseMessage
from academy.message import ShutdownRequest
from academy.message import ShutdownResponse


def test_shutdown_dest_type() -> None:
    with pytest.raises(
        ValueError,
        match='Destination identifier has the user role.',
    ):
        ShutdownRequest(src=AgentId.new(), dest=UserId.new())


_src: AgentId[Any] = AgentId.new()
_dest: AgentId[Any] = AgentId.new()


@pytest.mark.parametrize(
    'message',
    (
        ActionRequest(src=_src, dest=_dest, action='foo', pargs=(b'bar',)),
        ActionResponse(src=_src, dest=_dest, action='foo', result=b'bar'),
        ActionResponse(
            src=_src,
            dest=_dest,
            action='foo',
            exception=Exception(),
        ),
        PingRequest(src=_src, dest=_dest),
        PingResponse(src=_src, dest=_dest),
        PingResponse(src=_src, dest=_dest, exception=Exception()),
        ShutdownRequest(src=_src, dest=_dest),
        ShutdownResponse(src=_src, dest=_dest),
        ShutdownResponse(src=_src, dest=_dest, exception=Exception()),
    ),
)
def test_message_representations(message: Message) -> None:
    assert isinstance(str(message), str)
    assert isinstance(repr(message), str)
    jsoned = message.model_dump_json()
    recreated = BaseMessage.model_from_json(jsoned)
    assert message == recreated
    assert hash(message) == hash(recreated)
    assert message != object()
    pickled = message.model_serialize()
    recreated = BaseMessage.model_deserialize(pickled)
    assert message == recreated


def test_deserialize_bad_type() -> None:
    pickled = pickle.dumps('string')
    with pytest.raises(
        TypeError,
        match='Deserialized message is not of type Message.',
    ):
        BaseMessage.model_deserialize(pickled)


@pytest.mark.parametrize(
    'request_',
    (
        ActionRequest(src=_src, dest=_dest, action='foo', pargs=(b'bar',)),
        PingRequest(src=_src, dest=_dest),
        ShutdownRequest(src=_src, dest=_dest),
    ),
)
def test_create_response_message(request_: RequestMessage) -> None:
    if isinstance(request_, ActionRequest):
        assert isinstance(
            request_.response(result=42),
            get_args(ResponseMessage),
        )
    else:
        assert isinstance(request_.response(), get_args(ResponseMessage))

    exception = Exception('foo')
    response = request_.error(exception)
    assert isinstance(response, get_args(ResponseMessage))
    assert response.exception == exception


def test_action_request_lazy_deserialize() -> None:
    request = ActionRequest(
        src=_src,
        dest=_dest,
        action='foo',
        pargs=('bar',),
        kargs={'foo': 'bar'},
    )

    json = request.model_dump_json()
    reconstructed = BaseMessage.model_from_json(json)

    assert isinstance(reconstructed, ActionRequest)
    assert isinstance(reconstructed.pargs, str)
    assert isinstance(reconstructed.kargs, str)

    reconstructed.get_args()
    reconstructed.get_kwargs()

    assert isinstance(reconstructed.pargs, tuple)
    assert isinstance(reconstructed.kargs, dict)


def test_action_response_lazy_deserialize() -> None:
    response = ActionResponse(
        src=_src,
        dest=_dest,
        action='foo',
        result={'foo': 'bar'},
    )

    json = response.model_dump_json()
    reconstructed = BaseMessage.model_from_json(json)

    assert isinstance(reconstructed, ActionResponse)
    assert isinstance(reconstructed.result, list)

    reconstructed.get_result()

    assert isinstance(reconstructed.result, dict)


@pytest.mark.parametrize(
    'response',
    (
        ActionResponse(
            src=_src,
            dest=_dest,
            action='foo',
            exception=Exception(),
        ),
        PingResponse(src=_src, dest=_dest, exception=Exception()),
        ShutdownResponse(src=_src, dest=_dest, exception=Exception()),
    ),
)
def test_action_response_exception_equality(response: ResponseMessage) -> None:
    dump = response.model_dump()
    dump['exception'] = Exception()
    other = type(response).model_validate(dump)
    assert response == other
    assert response != BaseMessage(src=response.src, dest=response.dest)
