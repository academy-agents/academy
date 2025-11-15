from __future__ import annotations

import os

from globus_sdk.scopes import ScopeBuilder

ACADEMY_EXCHANGE_CLIENT_ID_ENV_NAME = 'ACADEMY_EXCHANGE_CLIENT_ID'
DEFAULT_EXCHANGE_CLIENT_ID = 'ca1e2fbf-4e73-40a6-b2ab-c9458e5fcf99'

ACADEMY_EXCHANGE_SCOPE_ID_ENV_NAME = 'ACADEMY_GLOBUS_SCOPE_ID'
DEFAULT_EXCHANGE_SCOPE_ID = 'a9e8d535-83e9-40c6-baf4-3f16538b2f10'

ACADEMY_EXCHANGE_SECRET_ENV_NAME = 'ACADEMY_EXCHANGE_SECRET'


def get_academy_exchange_client_id() -> str:
    """Get the client id of the academy_exchange.

    The environment variable can be used for testing, otherwise the
    default value is the id of the hosted exchange.
    """
    try:
        return os.environ[ACADEMY_EXCHANGE_CLIENT_ID_ENV_NAME]
    except KeyError:
        return DEFAULT_EXCHANGE_CLIENT_ID


def get_academy_exchange_secret() -> str:
    """Get the secret of the academy_exchange."""
    return os.environ[ACADEMY_EXCHANGE_SECRET_ENV_NAME]


def get_academy_exchange_scope_id() -> str:
    """Get the id of the academy_exchange scope.

    The environment variable can be used for testing, otherwise the default
    value is the id of the scope associated with the hosted exchange.
    """
    try:
        return os.environ[ACADEMY_EXCHANGE_SCOPE_ID_ENV_NAME]
    except KeyError:
        return DEFAULT_EXCHANGE_SCOPE_ID


AcademyExchangeScopes = ScopeBuilder(
    # "Academy Exchange Server" application client ID
    get_academy_exchange_client_id(),
    known_url_scopes=['academy_exchange'],
)
