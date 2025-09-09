from __future__ import annotations

from globus_sdk._testing.models import RegisteredResponse
from globus_sdk._testing.models import ResponseSet
from globus_sdk._testing.registry import register_response_set

from academy.exchange.cloud.globus import AcademyGlobusClient
from academy.identifier import AgentId
from testing.constant import TEST_ACADEMY_PATH

RESPONSES = ResponseSet(
    default=RegisteredResponse(
        path=f'{TEST_ACADEMY_PATH}/discover',
        method='GET',
        json={
            'agent_ids': [
                AgentId.new().model_dump_json(),
                AgentId.new().model_dump_json(),
                AgentId.new().model_dump_json(),
            ],
        },
        status=200,
    ),
)

register_response_set(AcademyGlobusClient.discover, RESPONSES)
