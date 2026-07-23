from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ClientInfo:
    """Hold client info including group and membership info."""

    client_id: str
    group_memberships: set[str]

    def __post_init__(self) -> None:
        # The Globus authenticator builds memberships from a list; coerce
        # to a set so group intersection checks are always safe.
        if not isinstance(self.group_memberships, set):
            self.group_memberships = set(self.group_memberships)
