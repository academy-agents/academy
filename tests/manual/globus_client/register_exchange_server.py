from __future__ import annotations

from globus_sdk import AuthClient
from globus_sdk import DependentScopeSpec
from globus_sdk.gare import GlobusAuthorizationParameters
from globus_sdk.scopes import AuthScopes

from academy.exchange.cloud.login import get_user_app
from academy.exchange.cloud.scopes import ACADEMY_EXCHANGE_CLIENT_ID_ENV_NAME
from academy.exchange.cloud.scopes import ACADEMY_EXCHANGE_SCOPE_ID_ENV_NAME
from academy.exchange.cloud.scopes import ACADEMY_EXCHANGE_SECRET_ENV_NAME


def register_new_exchange():
    """Create necessary ids for running exchange server."""
    app = get_user_app()
    app.add_scope_requirements(
        {
            AuthScopes.resource_server: [
                AuthScopes.manage_projects,
                AuthScopes.email,
                AuthScopes.profile,
            ],
        },
    )
    app.login(
        force=True,
        auth_params=GlobusAuthorizationParameters(prompt='login'),
    )
    auth_client = AuthClient(app=app)
    userinfo = auth_client.oauth2_userinfo()
    identity_id = userinfo['sub']
    email = userinfo['email']

    project_response = auth_client.create_project(
        'Academy test project',
        contact_email=email,
        admin_ids=identity_id,
    )
    project = project_response['project']['id']
    print(f'ACADEMY_TEST_PROJECT_ID={project}')

    client_response = auth_client.create_client(
        'academy_exchange',
        project=project,
        client_type='hybrid_confidential_client_resource_server',
        visibility='private',
    )
    client_id = client_response['client']['id']
    print(f'{ACADEMY_EXCHANGE_CLIENT_ID_ENV_NAME}={client_id}')

    # Create secret
    credentials_response = auth_client.create_client_credential(
        client_id,
        'academy_exchange_test',
    )
    secret = credentials_response['credential']['secret']
    print(f'{ACADEMY_EXCHANGE_SECRET_ENV_NAME}={secret}')

    # Add dependent scope on Globus Groups
    groups_scope_spec = DependentScopeSpec(
        '73320ffe-4cb4-4b25-a0a3-83d53d59ce4f',
        True,
        False,
    )
    # Create scope
    scope_response = auth_client.create_scope(
        client_id,
        'Agent registration',
        'Send messages on the exchange',
        'academy_exchange',
        allows_refresh_token=True,
        dependent_scopes=[groups_scope_spec],
    )

    scope_id = scope_response['scopes'][0]['id']
    print(f'{ACADEMY_EXCHANGE_SCOPE_ID_ENV_NAME}={scope_id}')


if __name__ == '__main__':
    register_new_exchange()
