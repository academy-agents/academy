from __future__ import annotations

import os
import uuid
from unittest import mock

from academy.exchange.cloud.scopes import ACADEMY_EXCHANGE_CLIENT_ID_ENV_NAME
from academy.exchange.cloud.scopes import ACADEMY_EXCHANGE_SCOPE_ID_ENV_NAME
from academy.exchange.cloud.scopes import ACADEMY_EXCHANGE_SECRET_ENV_NAME
from academy.exchange.cloud.scopes import DEFAULT_EXCHANGE_CLIENT_ID
from academy.exchange.cloud.scopes import DEFAULT_EXCHANGE_SCOPE_ID
from academy.exchange.cloud.scopes import get_academy_exchange_client_id
from academy.exchange.cloud.scopes import get_academy_exchange_scope_id
from academy.exchange.cloud.scopes import get_academy_exchange_secret


def test_get_exchange_client_id_default():
    cid = get_academy_exchange_client_id()
    assert cid == DEFAULT_EXCHANGE_CLIENT_ID


def test_get_exchange_client_id_env():
    client_uuid = str(uuid.uuid4())
    env = {
        ACADEMY_EXCHANGE_CLIENT_ID_ENV_NAME: client_uuid,
    }
    with mock.patch.dict(os.environ, env):
        cid = get_academy_exchange_client_id()
        assert cid == client_uuid


def test_get_exchange_secret_env():
    client_secret = 'secret'
    env = {
        ACADEMY_EXCHANGE_SECRET_ENV_NAME: client_secret,
    }
    with mock.patch.dict(os.environ, env):
        secret = get_academy_exchange_secret()
        assert secret == client_secret


def test_get_exchange_scope_id_default():
    sid = get_academy_exchange_scope_id()
    assert sid == DEFAULT_EXCHANGE_SCOPE_ID


def test_get_exchange_scope_id_env():
    client_scope = str(uuid.uuid4())
    env = {
        ACADEMY_EXCHANGE_SCOPE_ID_ENV_NAME: client_scope,
    }
    with mock.patch.dict(os.environ, env):
        sid = get_academy_exchange_scope_id()
        assert sid == client_scope
