from __future__ import annotations

import sys
import uuid
from enum import IntEnum
from typing import Any
from typing import Generic
from typing import get_args
from typing import Literal
from typing import Protocol
from typing import runtime_checkable
from typing import TypeVar

from packaging.version import parse
from packaging.version import Version

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_serializer
from pydantic import SkipValidation
from pydantic import TypeAdapter

from academy.exception import ActionCancelledError
from academy.exception import ActionInvalidStateError
from academy.exception import ExceptionSerializationError
from academy.exception import IncompatibleNetworkProtocolError
from academy.exception import MailboxTerminatedError
from academy.exception import PingCancelledError
from academy.exception import RequestForbiddenError
from academy.identifier import EntityId
from academy.serialize import deserialize
from academy.serialize import SerializationStrategy
from academy.serialize import serialize

DEFAULT_FROZEN_CONFIG = ConfigDict(
    arbitrary_types_allowed=True,
    extra='ignore',
    frozen=True,
    use_enum_values=True,
    validate_default=True,
)
DEFAULT_MUTABLE_CONFIG = ConfigDict(
    arbitrary_types_allowed=True,
    extra='ignore',
    frozen=False,
    use_enum_values=True,
    validate_default=True,
)

PROTOCOL_VERSION: Version = Version('1')


def check_version(version: str | None) -> bool:
    """Checks if a `protocol_version` is compatible.

    Messages within a major version are intended to be mutually
    compatible, while minor version changes might introduce
    optional fields or remove existing fields without prohibiting
    their existence. Local tags may be added for branches/forks
    and must match exactly.

    Args:
        version: The version string to check

    Returns:
        True if the version is compatible.
    """
    return (
        version is not None
        and parse(version).major == PROTOCOL_VERSION.major
        and parse(version).local == PROTOCOL_VERSION.local
    )


class ActionRequest(BaseModel):
    """Agent action request message.

    Warning:
        The positional and keywords arguments for the invoked action are
        serialized using `serialization` strategy when the message is
        serialized to JSON. This can have non-trivial time and space
        overheads for large arguments.
    """

    action: str = Field(description='Name of the requested action.')
    serialization: SerializationStrategy = Field(
        description='Serialization strategy used to send args',
    )
    result_serialization: SerializationStrategy | None = Field(
        default=None,
        description=(
            'Requested serialization of results. If none or empty, use the '
            'same method the args were serialized with.'
        ),
    )
    exception_serialization: SerializationStrategy | None = Field(
        default=None,
        description=(
            'Requested serialization of exceptions. If none or empty, use '
            'the same method for exceptions as for results.'
        ),
    )
    pargs: SkipValidation[tuple[Any, ...]] = Field(
        default_factory=tuple,
        description='Positional arguments to the action method.',
    )
    kargs: SkipValidation[dict[str, Any]] = Field(
        default_factory=dict,
        description='Keyword arguments to the action method.',
    )
    kind: Literal['action-request'] = Field('action-request', repr=False)

    model_config = DEFAULT_MUTABLE_CONFIG

    @field_serializer('pargs', 'kargs', when_used='json')
    def _serialize_obj(self, obj: Any) -> str:
        if isinstance(obj, str):  # pragma: no cover
            return obj
        return serialize(obj, self.serialization)

    def get_args(self) -> tuple[Any, ...]:
        """Get the positional arguments.

        Lazily deserializes and returns the positional arguments.
        Caches the result to avoid redundant decoding.

        Returns:
            The deserialized tuple of positional arguments.
        """
        if isinstance(self.pargs, str):
            self.pargs = deserialize(self.pargs, self.serialization)
        return self.pargs

    def get_kwargs(self) -> dict[str, Any]:
        """Get the keyword arguments.

        Lazily deserializes and returns the keyword arguments.
        Caches the result to avoid redundant decoding.

        Returns:
            The deserialized dictionary of keyword arguments.
        """
        if isinstance(self.kargs, str):
            self.kargs = deserialize(self.kargs, self.serialization)
        return self.kargs


class PingRequest(BaseModel):
    """Agent ping request message."""

    kind: Literal['ping-request'] = Field('ping-request', repr=False)

    model_config = DEFAULT_FROZEN_CONFIG


class CancelRequest(BaseModel):
    """Cancel running action."""

    target_tag: uuid.UUID
    kind: Literal['cancel-request'] = Field('cancel-request', repr=False)

    model_config = DEFAULT_FROZEN_CONFIG


class ShutdownRequest(BaseModel):
    """Agent shutdown request message."""

    terminate: bool | None = Field(
        None,
        description='Override the termination behavior of the agent.',
    )
    kind: Literal['shutdown-request'] = Field('shutdown-request', repr=False)

    model_config = DEFAULT_FROZEN_CONFIG


class ActionResponse(BaseModel):
    """Agent action response message.

    Warning:
        The result is serialized using `serialization` when the response is
        serialized to JSON. This can have non-trivial time and space
        overheads for large results.
    """

    result: SkipValidation[Any] = Field(
        description='Result of the action, if successful.',
    )
    serialization: SerializationStrategy = Field(
        description='Serialization strategy used to send result.',
    )
    kind: Literal['action-response'] = Field('action-response', repr=False)

    model_config = DEFAULT_MUTABLE_CONFIG

    @field_serializer('result', when_used='json')
    def _serialize_result(self, obj: Any) -> list[Any] | None:
        if (
            isinstance(obj, list)
            and len(obj) == 2  # noqa PLR2004
            and obj[0] == '__serialized__'
        ):  # pragma: no cover
            # Prevent double serialization
            return obj

        data = serialize(obj, self.serialization)
        # This sential value at the start of the tuple is so we can
        # disambiguate a result that is a str versus the string of a
        # serialized result.
        return ['__serialized__', data]

    def get_result(self) -> Any:
        """Get the result.

        Lazily deserializes and returns the result of the action.
        Caches the result to avoid redundant decoding.

        Returns:
            The deserialized result of the action.
        """
        if (
            isinstance(self.result, list)
            and len(self.result) == 2  # noqa PLR2004
            and self.result[0] == '__serialized__'
        ):
            self.result = deserialize(self.result[1], self.serialization)
        return self.result


@runtime_checkable
class ErrorResponse(Protocol):
    """Protocol for all error messages."""

    def get_exception(self) -> Exception:
        """Get the exception.

        Returns:
            The exception.
        """
        ...


class ErrorCode(IntEnum):
    """Error codes returned by requests.

    These error codes allow us to return errors without serialization.
    """

    MAILBOX_TERMINATED = 0
    PING_CANCELLED = 1
    ACTION_INVALID_STATE = 2
    ACTION_CANCELLED = 3
    INVALID_CLIENT = 4
    INCOMPATIBLE_PROTOCOL = 5
    FORBIDDEN = 6


class AcademyErrorResponse(BaseModel):
    """Error response created by Academy."""

    error_code: ErrorCode = Field(
        description='Error code ',
    )
    mailbox_id: EntityId | None = Field(
        description='Mailbox id if necessary for the error.',
        default=None,
    )
    kind: Literal['academy-error-response'] = Field(
        'academy-error-response',
        repr=False,
    )

    def get_exception(self) -> Exception:  # noqa: PLR0911
        """Get the exception.

        Returns:
            The exception.
        """
        match self.error_code:
            case ErrorCode.MAILBOX_TERMINATED:
                assert self.mailbox_id is not None, (
                    'Improper error response while decoding exception. '
                    'Missing mailbox_id field in MailboxTerminatedError.'
                )
                return MailboxTerminatedError(self.mailbox_id)
            case ErrorCode.PING_CANCELLED:
                return PingCancelledError()
            case ErrorCode.ACTION_INVALID_STATE:
                return ActionInvalidStateError()
            case ErrorCode.ACTION_CANCELLED:
                return ActionCancelledError()
            case ErrorCode.INVALID_CLIENT:
                return TypeError(f'{self.mailbox_id} cannot fulfill requests.')
            case ErrorCode.INCOMPATIBLE_PROTOCOL:
                return IncompatibleNetworkProtocolError()
            case ErrorCode.FORBIDDEN:
                return RequestForbiddenError(self.mailbox_id)
        raise AssertionError('Unreachable.')


class UserErrorResponse(BaseModel):
    """Error response message.

    Contains the exception raised by a failed request.
    """

    serialization: SerializationStrategy = Field(
        description='Serialization strategy used to send exception.',
    )
    exception: SkipValidation[Exception] = Field(
        description='Exception of the failed request.',
    )
    kind: Literal['user-error-response'] = Field(
        'user-error-response',
        repr=False,
    )

    model_config = DEFAULT_MUTABLE_CONFIG

    @field_serializer('exception', when_used='json')
    def _serialize_obj(self, obj: Any) -> str | None:
        try:
            return serialize(obj, self.serialization)
        except Exception:
            # If we get an exception while serializing an exception,
            # we do not want to raise an exception and prevent any response
            # from being returned. Instead we replace the exception with
            # a exception we know can be serialized, letting the client know
            # that a exception was hidden.
            return serialize(
                ExceptionSerializationError(
                    obj.__class__.__name__,
                    self.serialization,
                ),
                self.serialization,
            )

    def get_exception(self) -> Exception:
        """Get the exception.

        Lazily deserializes and returns the exception object.
        Caches the result to avoid redundant decoding.

        Returns:
            The deserialized exception.
        """
        if isinstance(self.exception, str):
            self.exception = deserialize(self.exception, self.serialization)
        return self.exception


class SuccessResponse(BaseModel):
    """Success response message."""

    kind: Literal['success-response'] = Field('success-response', repr=False)

    model_config = DEFAULT_FROZEN_CONFIG


Request = ActionRequest | CancelRequest | PingRequest | ShutdownRequest
Response = (
    ActionResponse | AcademyErrorResponse | UserErrorResponse | SuccessResponse
)
Body = Request | Response

BodyT = TypeVar('BodyT', bound=Body)
RequestT = TypeVar('RequestT', bound=Request)
RequestT_co = TypeVar('RequestT_co', bound=Request, covariant=True)
ResponseT = TypeVar('ResponseT', bound=Response)
ResponseT_co = TypeVar('ResponseT_co', bound=Response, covariant=True)


class Header(BaseModel):
    """Message metadata header.

    Contains information about the sender, receiver, and message context.
    """

    src: EntityId = Field(description='Message source ID.')
    dest: EntityId = Field(description='Message destination ID.')
    tag: uuid.UUID = Field(
        description='Unique message tag used to match requests and responses.',
    )
    kind: Literal['request', 'response'] = Field(
        description='Whether this is a request or response message.',
    )
    label: uuid.UUID | None = Field(
        None,
        description=(
            'Optional label used to disambiguate response messages when '
            'multiple objects (i.e., handles) share the same mailbox. '
            'This is a different usage from the `tag`.'
        ),
    )
    groups: frozenset[str] = Field(
        default_factory=frozenset,
        description=(
            'Globus group IDs the sender belongs to, intersected with the '
            "receiver mailbox's permitted groups. Populated by the "
            'exchange when the message is delivered; never self-reported '
            'by the sender.'
        ),
    )
    protocol_version: str | None = Field(
        str(PROTOCOL_VERSION),
        description=(
            'Version of the academy protocol used. Messages within a major '
            'version are intended to be mutually compatible, while minor '
            'version changes might introduce optional fields or remove '
            'existing fields without prohibiting their existence.'
        ),
    )

    model_config = DEFAULT_FROZEN_CONFIG

    def create_response_header(self) -> Self:
        """Create a response header based on the current request header.

        Swaps the source and destination, retains the tag and label,
        and sets the kind to 'response'.

        Returns:
            A new header instance with reversed roles.

        Raises:
            ValueError: If the current header is already a response.
        """
        if self.kind == 'response':
            raise ValueError(
                'Cannot create response header from another response header',
            )
        return type(self)(
            tag=self.tag,
            src=self.dest,
            dest=self.src,
            label=self.label,
            kind='response',
        )


class Message(BaseModel, Generic[BodyT]):
    """A complete message with header and body.

    Wraps a header and a typed request/response body. Supports lazy
    deserialization of message bodies, metadata access, and convenient
    construction.

    Note:
        The body value is ignored when testing equality or hashing an instance
        because the body value could be in either a serialized or
        deserialized state until
        [`get_body()`][academy.message.Message.get_body] is called.
    """

    header: Header
    body: SkipValidation[BodyT] = Field(discriminator='kind')

    model_config = DEFAULT_MUTABLE_CONFIG

    def __eq__(self, other: object, /) -> bool:
        if not isinstance(other, Message):
            return False
        # body can be in either serialized or unserialized state so
        # we ignore it from equality comparisons
        return self.header == other.header

    def __hash__(self) -> int:
        return hash((type(self), self.header))

    @property
    def src(self) -> EntityId:
        """Message source ID."""
        return self.header.src

    @property
    def dest(self) -> EntityId:
        """Message destination ID."""
        return self.header.dest

    @property
    def tag(self) -> uuid.UUID:
        """Message tag."""
        return self.header.tag

    @property
    def label(self) -> uuid.UUID | None:
        """Message label."""
        return self.header.label

    @property
    def protocol_version(self) -> str | None:
        """Message protocol version."""
        return self.header.protocol_version

    @classmethod
    def create(
        cls,
        src: EntityId,
        dest: EntityId,
        body: BodyT,
        *,
        label: uuid.UUID | None = None,
        tag: uuid.UUID | None = None,
    ) -> Message[BodyT]:
        """Create a new message with the specified header and body.

        Args:
            src: Source entity ID.
            dest: Destination entity ID.
            body: Message body.
            label: Optional label for disambiguation.
            tag: Optional tag for relating responses to requests.

        Returns:
            A new message instance.
        """
        if isinstance(body, get_args(Request)):
            kind = 'request'
        elif isinstance(body, get_args(Response)):
            kind = 'response'
        else:
            raise AssertionError('Unreachable.')

        if tag is None:
            tag = uuid.uuid4()

        header = Header(src=src, dest=dest, label=label, kind=kind, tag=tag)
        request: Message[BodyT] = Message(header=header, body=body)
        return request

    def create_response(self, body: ResponseT) -> Message[ResponseT]:
        """Create a response message from this request message.

        Args:
            body: Response message body.

        Returns:
            A new response message instance.

        Raises:
            ValueError: If this message is already a response.
        """
        header = self.header.create_response_header()
        response: Message[ResponseT] = Message(header=header, body=body)
        return response

    def get_body(self) -> BodyT:
        """Return the message body, deserializing if needed.

        Lazily deserializes and returns the body object.
        Caches the body to avoid redundant decoding.

        Returns:
            The deserialized body.
        """
        if isinstance(self.body, get_args(Body)):
            return self.body

        if not check_version(self.protocol_version):
            # If we are deserializing from another protocol version,
            # we expect the validation to fail, but we would like to
            # raise a more informative error.
            raise IncompatibleNetworkProtocolError(
                self.protocol_version,
                PROTOCOL_VERSION,
            )

        adapter: TypeAdapter[BodyT] = TypeAdapter(Body)
        body = (
            adapter.validate_json(self.body)
            if isinstance(self.body, str)
            else adapter.validate_python(self.body)
        )
        self.body = body
        return self.body

    def is_request(self) -> bool:
        """Check if the message is a request."""
        return self.header.kind == 'request'

    def is_response(self) -> bool:
        """Check if the message is a response."""
        return self.header.kind == 'response'

    @classmethod
    def model_deserialize(cls, data: bytes) -> Message[BodyT]:
        """Deserialize a message from bytes.

        Args:
            data: The serialized message as bytes.

        Returns:
            The deserialized message instance.
        """
        message = cls.model_validate_json(data.decode('utf-8'))
        return message

    def model_serialize(self) -> bytes:
        """Serialize the message to bytes.

        Returns:
            The serialized message as bytes.
        """
        return self.model_dump_json().encode('utf-8')

    def log_extra(self) -> dict[str, object]:
        """Returns extra info useful in logs about this Message."""
        return {
            'academy.message_type': type(self.body).__name__,
            'academy.src': self.src,
            'academy.dest': self.dest,
            'academy.message_tag': self.tag,
            'academy.message_label': self.label,
        }
