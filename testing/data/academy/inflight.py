from __future__ import annotations

from globus_sdk.testing.models import RegisteredResponse
from globus_sdk.testing.models import ResponseSet
from globus_sdk.testing.registry import register_response_set

from academy.exchange.cloud.globus import AcademyGlobusClient

RESPONSES = ResponseSet(
    default=RegisteredResponse(
        path=f'{AcademyGlobusClient.base_url}/mailbox/inflight',
        method='GET',
        json={'count': 0},
        status=200,
    ),
)

register_response_set(AcademyGlobusClient.get_inflight_messages, RESPONSES)
