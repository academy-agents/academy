from __future__ import annotations

import json
import logging
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from unittest import mock

import pytest
from globus_sdk import OAuthDependentTokenResponse
from globus_sdk.scopes import GroupsScopes
from requests import Response

from academy.exception import ForbiddenError
from academy.exception import UnauthorizedError
from academy.exchange.cloud.authenticate import get_authenticator
from academy.exchange.cloud.authenticate import get_token_from_headers
from academy.exchange.cloud.authenticate import GlobusAuthenticator
from academy.exchange.cloud.authenticate import NullAuthenticator
from academy.exchange.cloud.client_info import ClientInfo
from academy.exchange.cloud.config import ExchangeAuthConfig


@pytest.mark.asyncio
async def test_null_authenticator() -> None:
    user1 = await NullAuthenticator().authenticate_user({})
    user2 = await NullAuthenticator().authenticate_user(
        {'Authorization': 'token'},
    )
    assert user1 == user2


@pytest.fixture
def dependent_token_response() -> OAuthDependentTokenResponse:
    client = mock.Mock()
    dependent_tokens = [
        {
            'access_token': 'ACCESS_TOKEN',
            'scope': GroupsScopes.view_my_groups_and_memberships,
            'expires_in': 172800,
            'token_type': 'Bearer',
            'resource_server': 'groups.api.globus.org',
        },
    ]

    dep_response = Response()
    dep_response.status_code = 200
    dep_response._content = json.dumps(dependent_tokens).encode('utf-8')
    dep_token_response = OAuthDependentTokenResponse(
        dep_response,
        client=client,
    )
    return dep_token_response


@pytest.mark.asyncio
async def test_authenticate_user_with_token() -> None:
    authenticator = GlobusAuthenticator(str(uuid.uuid4()), 'secret')

    token_meta: dict[str, Any] = {
        'active': True,
        'aud': [authenticator.audience],
        'sub': authenticator.auth_client.client_id,
        'username': 'username',
        'client_id': '1624cf3f-45ee-4f54-9de4-2d5d79191346',
        'email': 'username@example.com',
        'name': 'User Name',
    }

    with mock.patch.multiple(
        authenticator,
        _token_introspect=mock.MagicMock(return_value=token_meta),
        _get_dependent_tokens=mock.MagicMock(return_value=''),
        _get_groups_and_memberships=mock.MagicMock(
            return_value=[],
        ),
    ):
        user: ClientInfo = await authenticator.authenticate_user(
            {'Authorization': 'Bearer <TOKEN>'},
        )

    assert user.client_id == token_meta['username']


@pytest.mark.asyncio
async def test_groups_and_memberships(dependent_token_response) -> None:
    authenticator = GlobusAuthenticator(str(uuid.uuid4()), 'secret')

    groups_response = Response()
    groups_response.status_code = 200

    group_id = '690baf30-b476-11e3-a878-12313809f035'
    group_id2 = str(uuid.uuid1())
    content = [
        {
            'name': 'osg.Swift',
            'id': group_id,
            'group_type': 'regular',
            'enforce_session': False,
            'session_limit': 28800,
            'my_memberships': [
                {
                    'group_id': '690baf30-b476-11e3-a878-12313809f035',
                    'identity_id': 'bc56e1d2-d274-11e5-b190-bf882d473eb3',
                    'username': 'bob',
                    'role': 'manager',
                    'status': 'active',
                    'status_reason': 'active',
                    'invite_email_address': 'bob@email.com',
                    'updated': '2015-07-02T15:11:59.244007',
                },
            ],
            'subscription_id': None,
            'subscription_info': None,
        },
        {
            'name': 'osg.Swift',
            'id': group_id2,
            'my_memberships': [
                {
                    'group_id': group_id2,
                    'status': 'pending',
                },
            ],
        },
    ]

    groups_response._content = json.dumps(content).encode('utf-8')

    with mock.patch(
        'requests.get',
    ) as mock_request:
        mock_request.return_value = groups_response
        groups = authenticator._get_groups_and_memberships(
            dependent_token_response,
        )
        assert groups
        assert group_id in groups
        assert group_id2 not in groups


async def test_groups_missing_dependent_scope() -> None:
    authenticator = GlobusAuthenticator(str(uuid.uuid4()), 'secret')
    dependent_token_response = mock.MagicMock()
    dependent_token_response.by_scopes.__getitem__.side_effect = KeyError()
    groups = authenticator._get_groups_and_memberships(
        dependent_token_response,
    )
    dependent_token_response.by_scopes.__getitem__.assert_called_once()
    assert len(groups) == 0


@pytest.mark.asyncio
async def test_groups_failed_query(dependent_token_response, caplog) -> None:
    authenticator = GlobusAuthenticator(str(uuid.uuid4()), 'secret')
    groups_response = Response()
    groups_response.status_code = 400

    with caplog.at_level(logging.ERROR):
        with mock.patch(
            'requests.get',
        ) as mock_request:
            mock_request.return_value = groups_response
            groups = authenticator._get_groups_and_memberships(
                dependent_token_response,
            )
            assert len(groups) == 0
            assert 'Globus groups query failed' in caplog.text


@pytest.mark.asyncio
async def test_dependent_tokens(dependent_token_response) -> None:
    authenticator = GlobusAuthenticator(str(uuid.uuid4()), 'secret')

    with mock.patch(
        'globus_sdk.ConfidentialAppAuthClient.oauth2_get_dependent_tokens',
    ) as mock_dep_tokens:
        mock_dep_tokens.return_value = dependent_token_response

        dep_tokens: OAuthDependentTokenResponse = (
            authenticator._get_dependent_tokens(
                dependent_token_response,
            )
        )
        assert dep_tokens.by_scopes[
            GroupsScopes.view_my_groups_and_memberships
        ]


@pytest.mark.asyncio
async def test_authenticate_user_with_dependent_tokens(
    dependent_token_response,
) -> None:
    authenticator = GlobusAuthenticator(str(uuid.uuid4()), 'secret')

    client_id = '1624cf3f-45ee-4f54-9de4-2d5d79191346'
    token_meta: dict[str, Any] = {
        'active': True,
        'aud': [authenticator.audience, client_id],
        'sub': authenticator.auth_client.client_id,
        'username': 'username',
        'client_id': client_id,
        'email': 'username@example.com',
        'name': 'User Name',
    }

    with mock.patch.multiple(
        authenticator,
        _token_introspect=mock.MagicMock(return_value=token_meta),
        _get_dependent_tokens=mock.MagicMock(
            return_value=dependent_token_response,
        ),
        _get_groups_and_memberships=mock.MagicMock(
            return_value=['group_1'],
        ),
    ):
        user: ClientInfo = await authenticator.authenticate_user(
            {'Authorization': 'Bearer <TOKEN>'},
        )
        assert user.client_id == token_meta['username']
        assert user.group_memberships == ['group_1']


@pytest.mark.asyncio
async def test_authenticate_user_with_token_expired_token() -> None:
    authenticator = GlobusAuthenticator(str(uuid.uuid4()), 'secret')
    with (
        mock.patch.object(
            authenticator,
            '_token_introspect',
            return_value={'active': False},
        ),
        pytest.raises(
            ForbiddenError,
            match=r'Token is expired or has been revoked\.',
        ),
    ):
        await authenticator.authenticate_user(
            {'Authorization': 'Bearer <TOKEN>'},
        )


@pytest.mark.asyncio
async def test_authenticate_user_with_token_wrong_audience() -> None:
    authenticator = GlobusAuthenticator(
        str(uuid.uuid4()),
        'secret',
    )
    with (
        mock.patch.object(
            authenticator,
            '_token_introspect',
            return_value={'active': True},
        ),
        pytest.raises(
            ForbiddenError,
            match='Token audience does not include',
        ),
    ):
        await authenticator.authenticate_user(
            {'Authorization': 'Bearer <TOKEN>'},
        )


@pytest.fixture
def globus_auth_client():
    with mock.patch('globus_sdk.ConfidentialAppAuthClient') as auth_client:
        yield auth_client


def test_globus_authenticator_token_introspect(globus_auth_client: mock.Mock):
    authenticator = GlobusAuthenticator(str(uuid.uuid4()), 'secret')

    authenticator._token_introspect('test')
    globus_auth_client.assert_called_once()

    # Should use thread local client
    authenticator._token_introspect('test2')
    globus_auth_client.assert_called_once()

    with ThreadPoolExecutor() as executor:
        executor.submit(authenticator._token_introspect, 'test3')
        assert globus_auth_client.call_count == 2  # noqa: PLR2004


def test_get_authenticator() -> None:
    config = ExchangeAuthConfig()
    authenticator = get_authenticator(config)
    assert isinstance(authenticator, NullAuthenticator)

    config = ExchangeAuthConfig(
        method='globus',
        kwargs={
            'client_id': str(uuid.uuid4()),
            'client_secret': 'test',
        },
    )
    authenticator = get_authenticator(config)
    assert isinstance(authenticator, GlobusAuthenticator)


def test_get_authenticator_unknown() -> None:
    config = ExchangeAuthConfig(method='globus')
    # Modify attribute after construction to avoid Pydantic checking string
    # literal type.
    config.method = 'test'  # type: ignore[assignment]
    with pytest.raises(ValueError, match='Unknown authentication method'):
        get_authenticator(config)


def test_get_token_from_headers() -> None:
    headers = {'Authorization': 'Bearer <TOKEN>'}
    assert get_token_from_headers(headers) == '<TOKEN>'


def test_get_token_from_headers_missing() -> None:
    with pytest.raises(
        UnauthorizedError,
        match=r'Request headers are missing authorization header\.',
    ):
        get_token_from_headers({})


def test_get_token_from_headers_malformed() -> None:
    with pytest.raises(
        UnauthorizedError,
        match=r'Bearer token in authorization header is malformed\.',
    ):
        get_token_from_headers({'Authorization': '<TOKEN>'})
