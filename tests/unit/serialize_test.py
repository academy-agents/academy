from __future__ import annotations

import pickle

import pytest

from academy.exception import AcademyRemoteError
from academy.exception import DeserializationMethodProhibitedError
from academy.serialize import allowed_deserializers
from academy.serialize import deserialize
from academy.serialize import JsonSerializer
from academy.serialize import NoPickleMixin
from academy.serialize import PickleSerializer
from academy.serialize import SerializationStrategy
from academy.serialize import serialize
from academy.serialize import Serializer


class CanItPickle(NoPickleMixin):
    pass


def test_no_pickle_mixin() -> None:
    with pytest.raises(pickle.PicklingError):
        pickle.dumps(CanItPickle())


@pytest.mark.parametrize(
    'serializer',
    (
        JsonSerializer,
        PickleSerializer,
    ),
)
def test_serializer_serialize_deserialize(serializer: Serializer):
    params = [5, 'test', {'key': 'value'}]
    data = serializer.serialize(params)
    reconstructed = serializer.deserialize(data)
    assert reconstructed == params


def test_json_serialize_exception():
    test_exception = Exception()
    json = JsonSerializer.serialize(test_exception)
    reconstructed = JsonSerializer.deserialize(json)
    assert isinstance(reconstructed, AcademyRemoteError)


@pytest.mark.parametrize(
    'strategy',
    (
        SerializationStrategy.PICKLE,
        SerializationStrategy.JSON,
    ),
)
def test_serialize_deserialize(strategy: SerializationStrategy):
    params = [5, 'test', {'key': 'value'}]
    data = serialize(params, strategy)
    reconstructed = deserialize(data, strategy)
    assert reconstructed == params


def test_serialization_allow_list():
    params = [5, 'test', {'key': 'value'}]
    data = serialize(params, SerializationStrategy.PICKLE)
    token = allowed_deserializers.set(set())
    with pytest.raises(DeserializationMethodProhibitedError):
        deserialize(data, SerializationStrategy.PICKLE)
    allowed_deserializers.reset(token)

    deserialize(data, SerializationStrategy.PICKLE)
