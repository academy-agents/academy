from __future__ import annotations

import base64
import pickle
import uuid
from typing import Any
from typing import get_args
from typing import Literal
from typing import Optional
from typing import Union

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_serializer
from pydantic import field_validator
from pydantic import TypeAdapter

from academy.identifier import AgentId
from academy.identifier import EntityId

NO_RESULT = object()


class BaseMessage(BaseModel):
    """Base message type for messages between entities (agents or users).

    Note:
        The [`hash()`][hash] of this type is a combination of the
        message type and message ID.
    """

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra='forbid',
        frozen=False,
        use_enum_values=True,
        validate_default=True,
    )

    tag: uuid.UUID = Field(
        default_factory=uuid.uuid4,
        description='Unique message tag used to match requests and responses.',
    )
    src: EntityId = Field(description='Source mailbox address.')
    dest: EntityId = Field(description='Destination mailbox address.')
    label: Optional[uuid.UUID] = Field(  # noqa: UP007
        None,
        description=(
            'Optional label used to disambiguate response messages when '
            'multiple objects (i.e., handles) share the same mailbox. '
            'This is a different usage from the `tag`.'
        ),
    )

    def __hash__(self) -> int:
        return hash(type(self)) + hash(self.tag) + hash(self.label)

    @classmethod
    def model_from_json(cls, data: str) -> Message:
        """Reconstruct a specific message from a JSON dump.

        Example:
            ```python
            from academy.message import BaseMessage, ActionRequest

            message = ActionRequest(...)
            dump = message.model_dump_json()
            assert BaseMessage.model_from_json(dump) == message
            ```
        """
        return TypeAdapter(Message).validate_json(data)

    @classmethod
    def model_deserialize(cls, data: bytes) -> Message:
        """Deserialize a message.

        Warning:
            This uses pickle and is therefore suceptible to all the
            typical pickle warnings about code injection.
        """
        message = pickle.loads(data)
        if not isinstance(message, get_args(Message)):
            raise TypeError(
                'Deserialized message is not of type Message.',
            )
        return message

    def model_serialize(self) -> bytes:
        """Serialize a message to bytes.

        Warning:
            This uses pickle and is therefore suceptible to all the
            typical pickle warnings about code injection.
        """
        return pickle.dumps(self)


class ActionRequest(BaseMessage):
    """Agent action request message.

    When this message is dumped to a JSON string, the `pargs` and `kargs`
    are pickled and then base64 encoded to a string. This can have non-trivial
    time and space overheads for large arguments.
    """

    action: str = Field(description='Name of the requested action.')
    pargs: tuple[Any, ...] = Field(
        default_factory=tuple,
        description='Positional arguments to the action method.',
    )
    kargs: dict[str, Any] = Field(
        default_factory=dict,
        description='Keyword arguments to the action method.',
    )
    kind: Literal['action-request'] = Field('action-request', repr=False)

    @field_serializer('pargs', 'kargs', when_used='json')
    def _pickle_and_encode_obj(self, obj: Any) -> str:
        raw = pickle.dumps(obj)
        return base64.b64encode(raw).decode('utf-8')

    @field_validator('pargs', 'kargs', mode='before')
    @classmethod
    def _decode_pickled_obj(cls, obj: Any) -> Any:
        if not isinstance(obj, str):
            return obj
        return pickle.loads(base64.b64decode(obj))

    def error(self, exception: Exception) -> ActionResponse:
        """Construct an error response to action request.

        Args:
            exception: Error of the action.
        """
        return ActionResponse(
            tag=self.tag,
            src=self.dest,
            dest=self.src,
            label=self.label,
            action=self.action,
            result=None,
            exception=exception,
        )

    def response(self, result: Any) -> ActionResponse:
        """Construct a success response to action request.

        Args:
            result: Result of the action.
        """
        return ActionResponse(
            tag=self.tag,
            src=self.dest,
            dest=self.src,
            label=self.label,
            action=self.action,
            result=result,
            exception=None,
        )


class ActionResponse(BaseMessage):
    """Agent action response message."""

    action: str = Field(description='Name of the requested action.')
    result: Any = Field(
        None,
        description='Result of the action, if successful.',
    )
    exception: Optional[Exception] = Field(  # noqa: UP007
        None,
        description='Exception of the action, if unsuccessful.',
    )
    kind: Literal['action-response'] = Field('action-response', repr=False)

    @field_serializer('exception', 'result', when_used='json')
    def _pickle_and_encode_obj(self, obj: Any) -> Optional[tuple[str, str]]:  # noqa: UP007
        if obj is None:
            return None
        raw = pickle.dumps(obj)
        # This sential value at the start of the tuple is so we can
        # disambiguate a result that is a str versus the string of a
        # serialized result.
        return ('__pickled__', base64.b64encode(raw).decode('utf-8'))

    @field_validator('exception', 'result', mode='before')
    @classmethod
    def _decode_pickled_obj(cls, obj: Any) -> Any:
        if (
            isinstance(obj, (tuple, list))
            and len(obj) == 2  # noqa: PLR2004
            and obj[0] == '__pickled__'
        ):
            return pickle.loads(base64.b64decode(obj[1]))
        return obj

    def __eq__(self, other: object, /) -> bool:
        if not isinstance(other, ActionResponse):
            return False
        return (
            self.tag == other.tag
            and self.src == other.src
            and self.dest == other.dest
            and self.label == other.label
            and self.action == other.action
            and self.result == other.result
            # Custom __eq__ is required because exception instances need
            # to be compared by type. I.e., Exception() == Exception() is
            # always False.
            and type(self.exception) is type(other.exception)
        )


class PingRequest(BaseMessage):
    """Ping request message."""

    kind: Literal['ping-request'] = Field('ping-request', repr=False)

    def response(self) -> PingResponse:
        """Construct a ping response message."""
        return PingResponse(
            tag=self.tag,
            src=self.dest,
            dest=self.src,
            label=self.label,
            exception=None,
        )

    def error(self, exception: Exception) -> PingResponse:
        """Construct an error response to ping request.

        Args:
            exception: Error of the action.
        """
        return PingResponse(
            tag=self.tag,
            src=self.dest,
            dest=self.src,
            label=self.label,
            exception=exception,
        )


class PingResponse(BaseMessage):
    """Ping response message."""

    exception: Optional[Exception] = Field(  # noqa: UP007
        None,
        description='Exception of the ping, if unsuccessful.',
    )
    kind: Literal['ping-response'] = Field('ping-response', repr=False)

    @field_serializer('exception', when_used='json')
    def _pickle_and_encode_obj(self, obj: Any) -> Optional[str]:  # noqa: UP007
        if obj is None:
            return None
        raw = pickle.dumps(obj)
        return base64.b64encode(raw).decode('utf-8')

    @field_validator('exception', mode='before')
    @classmethod
    def _decode_pickled_obj(cls, obj: Any) -> Any:
        if not isinstance(obj, str):
            return obj
        return pickle.loads(base64.b64decode(obj))

    def __eq__(self, other: object, /) -> bool:
        if not isinstance(other, PingResponse):
            return False
        return (
            self.tag == other.tag
            and self.src == other.src
            and self.dest == other.dest
            and self.label == other.label
            # Custom __eq__ is required because exception instances need
            # to be compared by type. I.e., Exception() == Exception() is
            # always False.
            and type(self.exception) is type(other.exception)
        )


class ShutdownRequest(BaseMessage):
    """Agent shutdown request message."""

    kind: Literal['shutdown-request'] = Field('shutdown-request', repr=False)

    @field_validator('dest', mode='after')
    @classmethod
    def _validate_agent(cls, dest: EntityId) -> EntityId:
        if not isinstance(dest, AgentId):
            raise ValueError(
                'Shutdown requests can only be send to an agent. '
                f'Destination identifier has the {dest.role} role.',
            )
        return dest

    def response(self) -> ShutdownResponse:
        """Construct a shutdown response message."""
        return ShutdownResponse(
            tag=self.tag,
            src=self.dest,
            dest=self.src,
            label=self.label,
            exception=None,
        )

    def error(self, exception: Exception) -> ShutdownResponse:
        """Construct an error response to shutdown request.

        Args:
            exception: Error of the action.
        """
        return ShutdownResponse(
            tag=self.tag,
            src=self.dest,
            dest=self.src,
            label=self.label,
            exception=exception,
        )


class ShutdownResponse(BaseMessage):
    """Agent shutdown response message."""

    exception: Optional[Exception] = Field(  # noqa: UP007
        None,
        description='Exception of the request, if unsuccessful.',
    )
    kind: Literal['shutdown-response'] = Field('shutdown-response', repr=False)

    @field_serializer('exception', when_used='json')
    def _pickle_and_encode_obj(self, obj: Any) -> Optional[str]:  # noqa: UP007
        if obj is None:
            return None
        raw = pickle.dumps(obj)
        return base64.b64encode(raw).decode('utf-8')

    @field_validator('exception', mode='before')
    @classmethod
    def _decode_pickled_obj(cls, obj: Any) -> Any:
        if not isinstance(obj, str):
            return obj
        return pickle.loads(base64.b64decode(obj))

    def __eq__(self, other: object, /) -> bool:
        if not isinstance(other, ShutdownResponse):
            return False
        return (
            self.tag == other.tag
            and self.src == other.src
            and self.dest == other.dest
            # Custom __eq__ is required because exception instances need
            # to be compared by type. I.e., Exception() == Exception() is
            # always False.
            and type(self.exception) is type(other.exception)
        )


RequestMessage = Union[ActionRequest, PingRequest, ShutdownRequest]
ResponseMessage = Union[ActionResponse, PingResponse, ShutdownResponse]
Message = Union[RequestMessage, ResponseMessage]
"""Message union type for type annotations.

Tip:
    This is a parameterized generic type meaning that this type cannot
    be used for [`isinstance`][`builtins.isinstance`] checks:
    ```python
    isinstance(message, Message)  # Fails
    ```
    Instead, use [`typing.get_args()`][typing.get_args]:
    ```
    from typing import get_args

    isinstance(message, get_args(Message))  # Works
    ```
"""
