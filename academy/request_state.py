"""Request processing status enumeration."""

from __future__ import annotations

import enum


class RequestStatus(enum.Enum):
    """Request processing status."""

    INFLIGHT = 'INFLIGHT'
    COMPLETED = 'COMPLETED'
