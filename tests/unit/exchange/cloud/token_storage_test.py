from __future__ import annotations

import time
from unittest import mock

import pytest

from academy.exchange.cloud.token_store import SafeSQLiteTokenStorage


@pytest.fixture
def mock_response():
    res = mock.Mock()
    expiration_time = int(time.time()) + 3600
    res.by_resource_server = {
        'resource_server_1': {
            'access_token': 'access_token_1',
            'expires_at_seconds': expiration_time,
            'refresh_token': 'refresh_token_1',
            'resource_server': 'resource_server_1',
            'scope': 'scope1',
            'token_type': 'Bearer',
        },
        'resource_server_2': {
            'access_token': 'access_token_2',
            'expires_at_seconds': expiration_time,
            'refresh_token': 'refresh_token_2',
            'resource_server': 'resource_server_2',
            'scope': 'scope2 scope2:0 scope2:1',
            'token_type': 'Bearer',
        },
    }
    res.decode_id_token.return_value = {'sub': 'user_id'}

    return res


def test_remove_token_data(tmp_path, mock_response):
    file = tmp_path / 'mydata.db'
    adapter = SafeSQLiteTokenStorage(file)
    adapter.store_token_response(mock_response)

    # remove rs1, confirm only rs2 is still available
    remove_result = adapter.remove_token_data('resource_server_1')
    assert remove_result is True

    assert adapter.get_token_data('resource_server_1') is None

    data = adapter.get_token_data('resource_server_2')
    assert data is not None
    assert data.access_token == 'access_token_2'

    # confirm unable to re-remove rs1
    remove_result = adapter.remove_token_data('resource_server_1')
    assert remove_result is False
