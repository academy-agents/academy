from __future__ import annotations

from globus_sdk._testing.models import RegisteredResponse
from globus_sdk._testing.models import ResponseSet
from globus_sdk._testing.registry import register_response_set
from responses import matchers

from academy.exchange.cloud.scopes import AcademyExchangeScopes
from testing.data.auth._common import CLIENT_ID
from testing.utils import urlencoded_params_matcher_allow_missing

_token = 'DUMMY_TRANSFER_TOKEN_FROM_THE_INTERTUBES'
_academy_scope = f'https://auth.globus.org/scopes/{AcademyExchangeScopes.resource_server}/academy_exchange'
_agent_scope = f'https://auth.globus.org/scopes/{CLIENT_ID}/launch'

RESPONSES = ResponseSet(
    auth=RegisteredResponse(
        service='auth',
        path='/v2/oauth2/token',
        method='POST',
        status=200,
        json={
            'access_token': 'auth_access_token',
            'scope': 'email openid urn:globus:auth:scope:auth.globus.org:manage_projects',  # noqa: E501
            'expires_in': 172800,
            'token_type': 'Bearer',
            'resource_server': 'auth.globus.org',
            'other_tokens': [],
        },
        match=[
            urlencoded_params_matcher_allow_missing(
                {'grant_type': 'client_credentials'},
            ),
        ],
    ),
    auth_2=RegisteredResponse(
        service='auth',
        path='/v2/oauth2/token',
        method='POST',
        status=200,
        json={
            'access_token': 'auth_access_token',
            'scope': 'email openid urn:globus:auth:scope:auth.globus.org:manage_projects profile',  # noqa: E501
            'expires_in': 172800,
            'token_type': 'Bearer',
            'resource_server': 'auth.globus.org',
            'other_tokens': [],
        },
        match=[
            urlencoded_params_matcher_allow_missing(
                {'grant_type': 'client_credentials'},
            ),
        ],
    ),
    client=RegisteredResponse(
        service='auth',
        path='/v2/oauth2/token',
        method='POST',
        status=200,
        json={
            'access_token': _token,
            'scope': _academy_scope,
            'expires_in': 172800,
            'token_type': 'Bearer',
            'resource_server': AcademyExchangeScopes.resource_server,
            'other_tokens': [],
        },
        match=[
            matchers.urlencoded_params_matcher(
                {
                    'grant_type': 'client_credentials',
                    'scope': _academy_scope,
                },
            ),
        ],
        metadata={
            'resource_server': AcademyExchangeScopes.resource_server,
            'access_token': _token,
            'scope': _academy_scope,
        },
    ),
    client_2=RegisteredResponse(
        service='auth',
        path='/v2/oauth2/token',
        method='POST',
        status=200,
        json={
            'access_token': _token,
            'scope': _academy_scope,
            'expires_in': 172800,
            'token_type': 'Bearer',
            'resource_server': AcademyExchangeScopes.resource_server,
            'other_tokens': [],
        },
        match=[
            matchers.urlencoded_params_matcher(
                {
                    'grant_type': 'client_credentials',
                    'scope': f'{_academy_scope} {_academy_scope}',
                },
            ),
        ],
        metadata={
            'resource_server': AcademyExchangeScopes.resource_server,
            'access_token': _token,
            'scope': _academy_scope,
        },
    ),
    agent=RegisteredResponse(
        service='auth',
        path='/v2/oauth2/token',
        method='POST',
        status=200,
        json={
            'access_token': _token,
            'scope': _agent_scope,
            'expires_in': 172800,
            'token_type': 'Bearer',
            'resource_server': CLIENT_ID,
            'other_tokens': [],
        },
        match=[
            matchers.urlencoded_params_matcher(
                {
                    'grant_type': 'client_credentials',
                    'scope': _agent_scope,
                },
            ),
        ],
        metadata={
            'resource_server': CLIENT_ID,
            'access_token': _token,
            'scope': _agent_scope,
        },
    ),
    agent_2=RegisteredResponse(
        service='auth',
        path='/v2/oauth2/token',
        method='POST',
        status=200,
        json={
            'access_token': _token,
            'scope': _agent_scope,
            'expires_in': 172800,
            'token_type': 'Bearer',
            'resource_server': CLIENT_ID,
            'other_tokens': [],
        },
        match=[
            matchers.urlencoded_params_matcher(
                {
                    'grant_type': 'client_credentials',
                    'scope': f'{_agent_scope} {_agent_scope}',
                },
            ),
        ],
        metadata={
            'resource_server': CLIENT_ID,
            'access_token': _token,
            'scope': _agent_scope,
        },
    ),
    agent_3=RegisteredResponse(
        service='auth',
        path='/v2/oauth2/token',
        method='POST',
        status=200,
        json={
            'access_token': _token,
            'scope': _agent_scope,
            'expires_in': 172800,
            'token_type': 'Bearer',
            'resource_server': CLIENT_ID,
            'other_tokens': [],
        },
        match=[
            matchers.urlencoded_params_matcher(
                {
                    'grant_type': 'client_credentials',
                    'scope': f'{_agent_scope} {_agent_scope} {_agent_scope}',
                },
            ),
        ],
        metadata={
            'resource_server': CLIENT_ID,
            'access_token': _token,
            'scope': _agent_scope,
        },
    ),
    dependent=RegisteredResponse(
        service='auth',
        path='/v2/oauth2/token',
        method='POST',
        status=200,
        json={
            'access_token': _token,
            'scope': _academy_scope,
            'expires_in': 172800,
            'token_type': 'Bearer',
            'resource_server': AcademyExchangeScopes.resource_server,
            'other_tokens': [],
        },
        match=[
            matchers.urlencoded_params_matcher(
                {
                    'refresh_token': 'academyRefreshToken',
                    'grant_type': 'refresh_token',
                },
            ),
        ],
        metadata={
            'resource_server': AcademyExchangeScopes.resource_server,
            'access_token': _token,
            'scope': _academy_scope,
        },
    ),
    default=RegisteredResponse(
        service='auth',
        path='/v2/oauth2/token',
        method='POST',
        status=200,
        json={
            'access_token': 'auth_access_token',
            'scope': f'{_agent_scope} openid email {_academy_scope} profile urn:globus:auth:scope:auth.globus.org:manage_projects',  # noqa: E501
            'expires_in': 172800,
            'token_type': 'Bearer',
            'resource_server': 'auth.globus.org',
            'other_tokens': [],
        },
        match=[
            urlencoded_params_matcher_allow_missing(
                {'grant_type': 'client_credentials'},
            ),
        ],
    ),
)

register_response_set('auth.oauth2_client_credentials_tokens', RESPONSES)
