from __future__ import annotations

import os
import uuid
from datetime import datetime
from typing import Any
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
import responses
from globus_sdk.testing import load_response

from academy.exchange.cloud.app import StatusCode
from academy.exchange.cloud.globus import _AcademyConnectionInfo
from academy.exchange.cloud.globus import _PendingRegistration
from academy.exchange.cloud.globus import AcademyGlobusClient
from academy.exchange.cloud.globus import GlobusAgentRegistration
from academy.exchange.cloud.globus import GlobusExchangeTransport
from academy.identifier import AgentId
from academy.identifier import UserId
from academy.message import Message
from academy.message import PingRequest
from testing.agents import EmptyAgent
from testing.constant import TEST_WAIT_TIMEOUT


@pytest.fixture(autouse=True)
def mocked_responses(monkeypatch):
    responses.start()
    monkeypatch.setitem(os.environ, 'GLOBUS_SDK_ENVIRONMENT', 'production')
    yield
    responses.stop()
    responses.reset()


@pytest.fixture
def academy_client():
    return AcademyGlobusClient()


def test_globus_client_discover(academy_client: AcademyGlobusClient):
    load_response(AcademyGlobusClient.discover)
    response = academy_client.discover(EmptyAgent)
    assert response.http_status == StatusCode.OKAY.value
    assert 'agent_ids' in response.data


def test_globus_client_recv(academy_client: AcademyGlobusClient):
    load_response(AcademyGlobusClient.recv)
    response = academy_client.recv(UserId.new(), TEST_WAIT_TIMEOUT)
    assert response.http_status == StatusCode.OKAY.value
    assert 'message' in response.data


def test_globus_client_register_agent(academy_client: AcademyGlobusClient):
    load_response(AcademyGlobusClient.register_agent)
    response = academy_client.register_agent(AgentId.new(), EmptyAgent)
    assert response.http_status == StatusCode.OKAY.value


def test_globus_client_register_client(academy_client: AcademyGlobusClient):
    load_response(AcademyGlobusClient.register_client)
    response = academy_client.register_client(UserId.new())
    assert response.http_status == StatusCode.OKAY.value


def test_globus_client_send(academy_client: AcademyGlobusClient):
    load_response(AcademyGlobusClient.send)
    user = UserId.new()
    agent: AgentId[Any] = AgentId.new()
    message = Message.create(
        src=user,
        dest=agent,
        body=PingRequest(),
    )
    response = academy_client.send(message)
    assert response.http_status == StatusCode.OKAY.value


def test_globus_client_status(academy_client: AcademyGlobusClient):
    load_response(AcademyGlobusClient.status)
    agent: AgentId[Any] = AgentId.new()
    response = academy_client.status(agent)
    assert response.http_status == StatusCode.OKAY.value
    assert 'status' in response.data


def test_globus_client_terminate(academy_client: AcademyGlobusClient):
    load_response(AcademyGlobusClient.terminate)
    agent: AgentId[Any] = AgentId.new()
    response = academy_client.terminate(agent)
    assert response.http_status == StatusCode.OKAY.value


@pytest.mark.asyncio
async def test_register_agents_single_login() -> None:
    """Batch registration triggers at most one login."""
    mock_app = MagicMock()
    mock_app.login_required.return_value = True

    transport = GlobusExchangeTransport(
        UserId.new(),
        connection_info=_AcademyConnectionInfo(project_id=uuid.uuid4()),
        app=mock_app,
    )

    def _make_pending(
        aid: AgentId[Any],
    ) -> _PendingRegistration[Any]:
        return _PendingRegistration(
            agent_id=aid,
            client_id=uuid.uuid4(),
            secret='secret',
            scope=MagicMock(),
        )

    def _make_reg(
        pending: _PendingRegistration[Any],
    ) -> GlobusAgentRegistration[Any]:
        return GlobusAgentRegistration(
            agent_id=pending.agent_id,
            client_id=pending.client_id,
            token='token',
            secret=pending.secret,
        )

    with (
        patch.object(
            transport,
            '_prepare_registration',
            side_effect=_make_pending,
        ),
        patch.object(
            transport,
            '_finalize_registration',
            side_effect=_make_reg,
        ),
        patch.object(transport, '_create_mailbox'),
    ):
        results = await transport.register_agents(
            [
                (EmptyAgent, None),
                (EmptyAgent, None),
                (EmptyAgent, None),
            ],
        )

    assert len(results) == 3  # noqa: PLR2004
    mock_app.login.assert_called_once()

    # Verify each agent's scope was accumulated before login.
    assert mock_app.add_scope_requirements.call_count == 3  # noqa: PLR2004
    for result, call in zip(
        results,
        mock_app.add_scope_requirements.call_args_list,
        strict=True,
    ):
        (scope_dict,) = call.args
        assert str(result.client_id) in scope_dict


def test_auth_client_new_thread_skips_login() -> None:
    """Fresh login_time without cached client skips login."""
    mock_app = MagicMock()

    transport = GlobusExchangeTransport(
        UserId.new(),
        connection_info=_AcademyConnectionInfo(project_id=uuid.uuid4()),
        app=mock_app,
    )
    # Simulate a prior login having already completed.
    transport.login_time = datetime.now()

    with patch(
        'academy.exchange.cloud.globus.AuthClient',
    ) as mock_cls:
        _ = transport.auth_client

    mock_cls.assert_called_once_with(app=mock_app)
    mock_app.login.assert_not_called()
