from __future__ import annotations

import time
from typing import Any
from unittest import mock

import globus_sdk

from academy.exchange.cloud.globus import _PersistentAgentAuthorizer


def _fake_response(*, target_rs: str, token: str) -> Any:
    """Single-RS stand-in for OAuthClientCredentialsResponse.

    ``RenewingAuthorizer`` reads ``by_resource_server[target_rs]`` and
    treats the value as a dict with ``access_token`` and
    ``expires_at_seconds``.
    """
    response = mock.MagicMock(spec=globus_sdk.OAuthClientCredentialsResponse)
    response.by_resource_server = {
        target_rs: {
            'access_token': token,
            'expires_at_seconds': int(time.time()) + 3600,
        },
    }
    return response


def _fake_chained_response(*, target_rs: str, target_token: str) -> Any:
    """Two-RS stand-in (target + sibling) for chained-token tests."""
    now = int(time.time())
    response = mock.MagicMock(spec=globus_sdk.OAuthClientCredentialsResponse)
    response.by_resource_server = {
        target_rs: {
            'access_token': target_token,
            'expires_at_seconds': now + 3600,
        },
        'sibling_rs': {
            'access_token': 'sibling-token',
            'expires_at_seconds': now + 3600,
        },
    }
    return response


def test_authorizer_caches_token_within_validity() -> None:
    client = mock.MagicMock(spec=globus_sdk.ConfidentialAppAuthClient)
    client.oauth2_client_credentials_tokens.return_value = _fake_response(
        target_rs='exchange_rs',
        token='t',
    )
    authorizer = _PersistentAgentAuthorizer(
        confidential_client=client,
        scope='launch',
        target_resource_server='exchange_rs',
    )
    authorizer.get_authorization_header()
    authorizer.get_authorization_header()
    assert client.oauth2_client_credentials_tokens.call_count == 1


def test_authorizer_renews_after_expiry() -> None:
    client = mock.MagicMock(spec=globus_sdk.ConfidentialAppAuthClient)
    client.oauth2_client_credentials_tokens.return_value = _fake_response(
        target_rs='exchange_rs',
        token='t',
    )
    authorizer = _PersistentAgentAuthorizer(
        confidential_client=client,
        scope='launch',
        target_resource_server='exchange_rs',
    )
    # Force expiry; next call must trigger renewal.
    authorizer.access_token = None
    authorizer.expires_at = 0
    authorizer.get_authorization_header()
    assert client.oauth2_client_credentials_tokens.call_count == 2  # noqa: PLR2004


def test_authorizer_picks_target_rs_from_chained_response() -> None:
    client = mock.MagicMock(spec=globus_sdk.ConfidentialAppAuthClient)
    client.oauth2_client_credentials_tokens.return_value = (
        _fake_chained_response(
            target_rs='exchange_rs',
            target_token='target-token',
        )
    )
    authorizer = _PersistentAgentAuthorizer(
        confidential_client=client,
        scope='launch',
        target_resource_server='exchange_rs',
    )
    assert authorizer.access_token == 'target-token'
