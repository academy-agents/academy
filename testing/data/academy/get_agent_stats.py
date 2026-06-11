from __future__ import annotations

from globus_sdk.testing.models import RegisteredResponse
from globus_sdk.testing.models import ResponseSet
from globus_sdk.testing.registry import register_response_set

from academy.exchange.cloud.globus import AcademyGlobusClient

RESPONSES = ResponseSet(
    default=RegisteredResponse(
        path=f'{AcademyGlobusClient.base_url}/mailbox/stats',
        method='GET',
        json={
            'incoming': 3,
            'outgoing': 1,
            'completed': 2,
            'in_progress': 1,
            'queued': 0,
        },
        status=200,
    ),
)

register_response_set(AcademyGlobusClient.get_agent_stats, RESPONSES)
