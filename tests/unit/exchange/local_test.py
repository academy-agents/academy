from __future__ import annotations

import pickle

import pytest

from academy.exchange.local import LocalExchangeFactory


def test_factory_serialize_error(
    local_exchange_factory: LocalExchangeFactory,
) -> None:
    with pytest.raises(pickle.PicklingError):
        pickle.dumps(local_exchange_factory)
