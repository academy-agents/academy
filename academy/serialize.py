from __future__ import annotations

import base64
import json
import logging
import pickle
import traceback
from contextvars import ContextVar
from enum import Enum
from typing import Any
from typing import Protocol

from academy.exception import AcademyRemoteError
from academy.exception import DeserializationMethodProhibitedError

logger = logging.getLogger(__name__)


class NoPickleMixin:
    """Mixin that raises an error if a type is pickled."""

    def __getstate__(self) -> Any:
        raise pickle.PicklingError(f'{type(self).__name__} is not pickleable.')


class Serializer(Protocol):
    """Protocol for different types of serialization methods."""

    @classmethod
    def serialize(cls, obj: Any) -> str:
        """Serialize object."""
        ...

    @classmethod
    def deserialize(cls, data: str) -> Any:
        """Deserialize data."""
        ...


def json_exception_serializer(obj: Any) -> dict[str, bool | str]:
    """Serialize exceptions as json.

    Extension to json serialization to deal with arbitrary exceptions.
    """
    if isinstance(obj, BaseException):
        exception_str = ''.join(
            traceback.format_exception(
                type(obj),
                obj,
                obj.__traceback__,
            ),
        )
        return {
            '__exception__': True,
            'exception_str': exception_str,
        }

    raise TypeError(
        f'Cannot serialize object of {type(obj)}',
    )  # pragma: no cover


def json_exception_deserializer(
    dct: dict[str, Any],
) -> Exception | dict[str, Any]:
    """Deserialize exceptions from json.

    Extension to json serializer that parses serialized exceptions into
    a RemoteException.
    """
    if '__exception__' in dct:
        return AcademyRemoteError(dct['exception_str'])

    return dct


class JsonSerializer:
    """Serializes objects into json.

    Works only on basic python types, but allows deserialization without
    arbitrary code execution, and has increased compatibility between
    python versions.
    """

    @classmethod
    def serialize(cls, obj: Any) -> str:
        """Serialize object."""
        return json.dumps(obj, default=json_exception_serializer)

    @classmethod
    def deserialize(cls, data: str) -> Any:
        """Deserialize data."""
        return json.loads(data, object_hook=json_exception_deserializer)


class PickleSerializer:
    """Serializes objects using pickle.

    Seriliazes objects using pickle. Generally allows for a wider range of
    types to be serialized, but cannot deserialize without allowing arbitrary
    code execution. Sending pickled python objects between processes with
    different python versions can lead to unexpected results.
    """

    @classmethod
    def serialize(cls, obj: Any) -> str:
        """Serialize object."""
        raw = pickle.dumps(obj)
        return base64.b64encode(raw).decode('utf-8')

    @classmethod
    def deserialize(cls, data: str) -> Any:
        """Deserialize data."""
        return pickle.loads(base64.b64decode(data))


class SerializationStrategies(str, Enum):
    """Enum for different serialization strategies."""

    PICKLE = 'pickle'
    JSON = 'json'


ALL_SERIALIZERS = {
    SerializationStrategies.PICKLE,
    SerializationStrategies.JSON,
}


default_serializer: ContextVar[SerializationStrategies] = ContextVar(
    'default_serializer',
    default=SerializationStrategies.PICKLE,
)
"""Default serialization method used to send requests."""

allowed_deserializers: ContextVar[set[SerializationStrategies]] = ContextVar(
    'deserialization_allow_list',
    default=ALL_SERIALIZERS,
)
"""Deserializers allowed in this context."""


def _get_serializer(strategy: SerializationStrategies) -> type[Serializer]:
    if strategy == SerializationStrategies.PICKLE:
        return PickleSerializer
    elif strategy == SerializationStrategies.JSON:
        return JsonSerializer

    raise AssertionError('Unreachable')


def serialize(obj: Any, strategy: SerializationStrategies) -> str:
    """Serialize object using strategy.

    Args:
        obj: Object to be serialized.
        strategy: Strategy used for serialization.

    Returns:
        Serialized data.
    """
    serializer = _get_serializer(strategy)
    return serializer.serialize(obj)


def deserialize(obj: str, strategy: SerializationStrategies) -> Any:
    """Deserialize object using strategy.

    Args:
        obj: Json data.
        strategy: Strategy used for serialization.

    Returns:
        Deserialized object.
    """
    if strategy not in allowed_deserializers.get():
        raise DeserializationMethodProhibitedError()
    serializer = _get_serializer(strategy)
    return serializer.deserialize(obj)
