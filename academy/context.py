from __future__ import annotations

import asyncio
import dataclasses
from typing import Any
from typing import Generic
from typing import TYPE_CHECKING
from typing import TypeVar

from academy.exchange import AgentExchangeClient
from academy.identifier import AgentId

if TYPE_CHECKING:
    from academy.behavior import BehaviorT
else:
    BehaviorT = TypeVar('BehaviorT')


@dataclasses.dataclass(frozen=True)
class AgentContext(Generic[BehaviorT]):
    """Agent runtime context."""

    agent_id: AgentId[BehaviorT]
    """ID of the exchange as registered with the exchange."""
    exchange_client: AgentExchangeClient[BehaviorT, Any]
    """Client used by agent to communicate with the exchange."""
    shutdown_event: asyncio.Event
    """Shutdown event used to signal the agent to shutdown."""
