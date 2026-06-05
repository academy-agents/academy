from __future__ import annotations

import dataclasses
import time
from datetime import timedelta

from academy.identifier import EntityId


@dataclasses.dataclass
class AgentStats:
    """Runtime metrics for a running agent."""

    completed_messages: dict[EntityId, int] = dataclasses.field(
        default_factory=dict,
    )
    
    inflight_messages: int = 0

    _start_time: float | None = dataclasses.field(
        default=None,
        init=False,
        repr=False,
    )

    @property
    def lifetime(self) -> timedelta | None:
        """Time elapsed since the agent finished startup.

        Returns ``None`` if the agent has not yet started.
        """
        if self._start_time is None:
            return None
        return timedelta(seconds=time.monotonic() - self._start_time)
