from __future__ import annotations

from globus_sdk._testing.models import RegisteredResponse
from globus_sdk._testing.models import ResponseSet
from globus_sdk._testing.registry import register_response_set
from responses import matchers

from academy.exchange.cloud.scopes import AcademyExchangeScopes

RESPONSES = ResponseSet(
    default=RegisteredResponse(
        service='auth',
        path='/v2/oauth2/token',
        method='POST',
        json=[
            {
                'scope': f'https://auth.globus.org/scopes/{AcademyExchangeScopes.resource_server}/academy_exchange',
                'access_token': 'academyToken',
                'refresh_token': 'academyRefreshToken',
                'token_type': 'bearer',
                'expires_in': 120,
                'resource_server': AcademyExchangeScopes.resource_server,
            },
        ],
        match=[
            matchers.urlencoded_params_matcher(
                {
                    'grant_type': 'urn:globus:auth:grant_type:dependent_token',
                    'token': 'DUMMY_TRANSFER_TOKEN_FROM_THE_INTERTUBES',
                    'access_type': 'offline',
                    'scope': f'https://auth.globus.org/scopes/{AcademyExchangeScopes.resource_server}/academy_exchange',
                },
            ),
        ],
        metadata={
            'rs_data': {
                '{AcademyExchangeScopes.resource_server}': {
                    'access_token': 'academyToken',
                    'refresh_token': 'academyRefreshToken',
                    'scope': f'https://auth.globus.org/scopes/{AcademyExchangeScopes.resource_server}/academy_exchange',
                },
            },
        },
    ),
)

register_response_set('auth.oauth2_get_dependent_tokens', RESPONSES)
