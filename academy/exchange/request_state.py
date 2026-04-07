"""Request lifecycle state tracking.

Tracks the processing status of requests through the exchange system:
- CREATED: Request message instantiated
- INFLIGHT: Request sent and awaiting processing
- COMPLETED: Response received for the request
"""

from __future__ import annotations

import enum
import logging
import uuid

logger = logging.getLogger(__name__)


class RequestStatus(enum.Enum):
    """Request processing status."""

    CREATED = 'CREATED'
    INFLIGHT = 'INFLIGHT'
    COMPLETED = 'COMPLETED'


class RequestState:
    """Tracks the lifecycle status of request messages."""

    REQUEST_STATE_MAP: dict[uuid.UUID, RequestStatus] = {}

    @classmethod
    def set_request_status(cls, tag: uuid.UUID, status: RequestStatus) -> None:
        """Set the current processing status of a request."""
        cls.REQUEST_STATE_MAP[tag] = status
        logger.info('REQUEST_STATE_MAP updated: %s', cls.REQUEST_STATE_MAP)

    @classmethod
    def get_request_status(cls, tag: uuid.UUID) -> RequestStatus | None:
        """Get the current processing status of a request."""
        return cls.REQUEST_STATE_MAP.get(tag)
