from __future__ import annotations

import base64
from contextvars import ContextVar
from enum import StrEnum
import pickle
from typing import Any, Literal, Protocol, TypeAlias
import traceback
import json
import logging

from academy.exception import DeserializationMethodProhibited, RemoteException

logger = logging.getLogger(__name__)

class NoPickleMixin:
    """Mixin that raises an error if a type is pickled."""

    def __getstate__(self) -> Any:
        raise pickle.PicklingError(f'{type(self).__name__} is not pickleable.')

class Serializer(Protocol):
    """Protocol for different types of serialization methods.
    """
    @classmethod
    def serialize(cls, obj: Any) -> str:
        """Serailize object."""
        ...

    @classmethod
    def deserialize(cls, data: str) -> Any:
        """Deserialize data."""
        ...

def json_exception_serializer(obj: Any) -> dict:
    """Serialize exceptions as json.
    
    Extension to json serialization to deal with arbitrary exceptions.
    """
    if isinstance(obj, BaseException):
        exception_str = "".join(traceback.format_exception(
            type(obj),
            obj,
            obj.__traceback__,
        ))
        return {
            '__exception__': True,
            'exception_str': exception_str
        }
    
    raise TypeError(f'Cannot serialize object of {type(obj)}')

def json_exception_deserializer(dct: dict[str, Any]) -> Exception | dict[str, Any]:
    """Deserialize exceptions from json.

    Extension to json serializer that parses serialized exceptions into
    a RemoteException.
    """
    if '__exception__' in dct:
        return RemoteException(dct['exception_str'])
    
    return dct

class JsonSerializer:
    @classmethod
    def serialize(self, obj: Any) -> str:
        """Serailize object."""
        return json.dumps(obj, default=json_exception_serializer)
    
    @classmethod
    def deserialize(self, data: str) -> Any:
        """Deserialize data."""
        return json.loads(data, object_hook=json_exception_deserializer)
    
class PickleSerializer:
    @classmethod
    def serialize(self, obj: Any) -> str:
        """Serailize object."""
        raw = pickle.dumps(obj)
        return base64.b64encode(raw).decode('utf-8')

    @classmethod    
    def deserialize(self, data: str) -> Any:
        """Deserialize data."""
        return pickle.loads(base64.b64decode(data))
    
class SerializationStrategies(StrEnum):
    PICKLE = "pickle"
    JSON = "json"

ALL_SERIALIZERS = {
    SerializationStrategies.PICKLE, 
    SerializationStrategies.JSON
}

default_serializer: ContextVar[SerializationStrategies] = ContextVar(
    'default_serializer',
    default=SerializationStrategies.PICKLE,
)

allowed_deserializers: ContextVar[set[SerializationStrategies]] = ContextVar(
    'deserialization_allow_list',
    default=ALL_SERIALIZERS,
)
    
def _get_serializer(strategy: SerializationStrategies):
    if strategy == SerializationStrategies.PICKLE:
        return PickleSerializer
    elif strategy == SerializationStrategies.JSON:
        return JsonSerializer
    
    assert False, "Unreachable"

def serialize(obj: Any, strategy: SerializationStrategies) -> str:
    '''Serialize object using strategy.

    Args:
        obj: Object to be serialized.
        strategy: Strategy used for serialization.
    Returns:
        Serialized data.
    '''
    serializer = _get_serializer(strategy)
    return serializer.serialize(obj)

def deserialize(obj: str, strategy: SerializationStrategies) -> Any:
    '''Deserialize object using strategy.

    Args:
        obj: Json data.
        strategy: Strategy used for serialization.
    Returns:
        Deserialized object.
    '''
    logger.debug(f"Allowed Deserializers: {allowed_deserializers.get()}")
    if strategy not in allowed_deserializers.get():
        raise DeserializationMethodProhibited()
    serializer = _get_serializer(strategy)
    return serializer.deserialize(obj)