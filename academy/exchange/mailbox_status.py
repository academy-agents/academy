from __future__ import annotations

import enum


class MailboxStatus(enum.Enum):
    """Exchange mailbox status."""

    MISSING = 'MISSING'
    """Mailbox does not exist."""
    ACTIVE = 'ACTIVE'
    """Mailbox exists and is accepting messages."""
    INACTIVE = 'INACTIVE'
    """Mailbox accepting messages but has missed heartbeats."""
    TERMINATED = 'TERMINATED'
    """Mailbox was terminated and no longer accepts messages."""
