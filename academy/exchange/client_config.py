from __future__ import annotations

from pydantic import BaseModel
from pydantic import Field


class ExchangeClientConfig(BaseModel):
    """Common runtime parameters for exchange clients."""

    heartbeat_interval: float = Field(
        default=30,
        description='Frequency to send liveness messages to the exchange',
    )
    stale_heartbeat_threshold: int = Field(
        default=3,
        description=(
            'Missed heartbeats before an agent is considered inactive.'
        ),
    )
