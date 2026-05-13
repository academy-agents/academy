from __future__ import annotations

import base64
import pickle
import uuid
from typing import Any

import pydantic
import pytest

from academy.exception import ActionCancelledError
from academy.exception import ActionInvalidStateError
from academy.exception import ExceptionSerializationError
from academy.exception import MailboxTerminatedError
from academy.exception import PingCancelledError
from academy.identifier import AgentId
from academy.message import AcademyErrorResponse
from academy.message import ActionRequest
from academy.message import ActionResponse
from academy.message import CancelRequest
from academy.message import ErrorCode
from academy.message import ErrorResponse
from academy.message import Header
from academy.message import Message
from academy.message import PingRequest
from academy.message import ShutdownRequest
from academy.message import SuccessResponse
from academy.message import UserErrorResponse
from academy.serialize import SerializationStrategy


@pytest.mark.parametrize(
    'message_body',
    (
        ActionRequest(
            serialization=SerializationStrategy.PICKLE,
            action='foo',
            pargs=(b'bar',),
        ),
        CancelRequest(target_tag=uuid.uuid4()),
        PingRequest(),
        ShutdownRequest(),
    ),
)
def test_request_message(message_body: Any) -> None:
    message = Message.create(
        src=AgentId.new(),
        dest=AgentId.new(),
        body=message_body,
        tag=uuid.uuid4(),
    )
    assert isinstance(str(message), str)
    assert isinstance(repr(message), str)
    jsoned = message.model_dump_json()
    recreated: Message[Any] = Message.model_validate_json(jsoned)
    assert message == recreated
    assert hash(message) == hash(recreated)
    assert message != object()
    pickled = message.model_serialize()
    recreated = Message.model_deserialize(pickled)
    assert message == recreated


@pytest.mark.parametrize(
    'message_body',
    (
        ActionResponse(
            serialization=SerializationStrategy.PICKLE,
            result=b'bar',
        ),
        AcademyErrorResponse(
            error_code=ErrorCode.PING_CANCELLED,
        ),
        UserErrorResponse(
            serialization=SerializationStrategy.PICKLE,
            exception=Exception(),
        ),
        SuccessResponse(),
    ),
)
def test_response_message(message_body: Any) -> None:
    header = Header(
        src=AgentId.new(),
        dest=AgentId.new(),
        tag=uuid.uuid4(),
        kind='response',
    )
    message: Message[Any] = Message(header=header, body=message_body)
    assert isinstance(str(message), str)
    assert isinstance(repr(message), str)
    jsoned = message.model_dump_json()
    recreated: Message[Any] = Message.model_validate_json(jsoned)
    assert message == recreated
    pickled = message.model_serialize()
    recreated = Message.model_deserialize(pickled)
    assert message == recreated


def test_deserialize_bad_type() -> None:
    pickled = base64.b64encode(pickle.dumps('string'))
    with pytest.raises(pydantic.ValidationError):
        Message.model_deserialize(pickled)


def tests_create_response_from_response_error() -> None:
    message = Message.create(
        src=AgentId.new(),
        dest=AgentId.new(),
        body=SuccessResponse(),
    )
    with pytest.raises(
        ValueError,
        match='Cannot create response header from another response',
    ):
        message.create_response(SuccessResponse())


@pytest.mark.parametrize(
    'serialization_stratgey',
    (
        SerializationStrategy.PICKLE,
        SerializationStrategy.JSON,
    ),
)
def test_action_request_lazy_deserialize(serialization_stratgey) -> None:
    request = ActionRequest(
        serialization=serialization_stratgey,
        action='foo',
        pargs=('bar',),
        kargs={'foo': 'bar'},
    )

    json = request.model_dump_json()
    reconstructed = ActionRequest.model_validate_json(json)

    assert isinstance(reconstructed, ActionRequest)
    assert isinstance(reconstructed.pargs, str)
    assert isinstance(reconstructed.kargs, str)

    reconstructed.get_args()
    reconstructed.get_kwargs()

    assert isinstance(reconstructed.pargs, tuple | list)
    assert isinstance(reconstructed.kargs, dict)


@pytest.mark.parametrize(
    'serialization_stratgey',
    (
        SerializationStrategy.PICKLE,
        SerializationStrategy.JSON,
    ),
)
def test_action_response_lazy_deserialize(serialization_stratgey) -> None:
    response = ActionResponse(
        serialization=serialization_stratgey,
        result={'foo': 'bar'},
    )

    json = response.model_dump_json()
    reconstructed = ActionResponse.model_validate_json(json)

    assert isinstance(reconstructed, ActionResponse)
    assert isinstance(reconstructed.result, list)

    reconstructed.get_result()

    assert isinstance(reconstructed.result, dict)


@pytest.mark.parametrize(
    ('error_code', 'exception_type'),
    (
        (ErrorCode.MAILBOX_TERMINATED, MailboxTerminatedError),
        (ErrorCode.PING_CANCELLED, PingCancelledError),
        (ErrorCode.ACTION_INVALID_STATE, ActionInvalidStateError),
        (ErrorCode.ACTION_CANCELLED, ActionCancelledError),
        (ErrorCode.INVALID_CLIENT, TypeError),
    ),
)
def test_academy_error_response_to_exception(
    error_code: ErrorCode,
    exception_type: type[Exception],
):
    response = AcademyErrorResponse(
        error_code=error_code,
        mailbox_id=AgentId.new(),
    )

    json = response.model_dump_json()
    reconstructed = AcademyErrorResponse.model_validate_json(json)

    assert isinstance(reconstructed, AcademyErrorResponse)
    assert isinstance(reconstructed, ErrorResponse)

    exception = reconstructed.get_exception()

    assert isinstance(exception, exception_type)


@pytest.mark.parametrize(
    'serialization_stratgey',
    (
        SerializationStrategy.PICKLE,
        SerializationStrategy.JSON,
    ),
)
def test_user_error_response_lazy_deserialize(serialization_stratgey) -> None:
    response = UserErrorResponse(
        serialization=serialization_stratgey,
        exception=Exception('Oops!'),
    )

    json = response.model_dump_json()
    reconstructed = UserErrorResponse.model_validate_json(json)

    assert isinstance(reconstructed, ErrorResponse)
    assert isinstance(reconstructed.exception, str)

    reconstructed.get_exception()

    assert isinstance(reconstructed.exception, Exception)


class UnserializableError(Exception):
    def __reduce__(self):
        raise Exception('This exception cannot be serialized.')


def test_user_error_response_serialization_error() -> None:
    response = UserErrorResponse(
        serialization=SerializationStrategy.PICKLE,
        exception=UnserializableError(),
    )

    json = response.model_dump_json()
    reconstructed = UserErrorResponse.model_validate_json(json)

    assert isinstance(reconstructed, ErrorResponse)
    assert isinstance(reconstructed.exception, str)

    reconstructed.get_exception()

    assert isinstance(reconstructed.exception, ExceptionSerializationError)
