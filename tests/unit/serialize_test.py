from __future__ import annotations

import pickle

import pytest

from academy.serialize import NoPickleMixin


class CanItPickle(NoPickleMixin):
    pass


def test_no_pickle_mixin() -> None:
    with pytest.raises(pickle.PicklingError):
        pickle.dumps(CanItPickle())
