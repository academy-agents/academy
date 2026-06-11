from __future__ import annotations

import dataclasses


@dataclasses.dataclass
class AgentStats:
    """Runtime metrics for a running agent."""

    incoming: int = 0
    """Total requests received (completed + in_progress + queued)."""

    outgoing: int = 0
    """Total requests sent by this agent to other agents."""

    completed: int = 0
    """Requests fully processed (response sent back to caller)."""

    in_progress: int = 0
    """Requests dequeued and actively being executed."""

    queued: int = 0
    """Requests waiting in the mailbox queue, not yet dequeued."""
