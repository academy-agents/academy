from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ClientInfo:
    """Hold client info including group and membership info."""

    client_id: str
    group_memberships: set[str]
