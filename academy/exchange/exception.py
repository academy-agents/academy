from __future__ import annotations

from academy.identifier import EntityId


class ExchangeError(Exception):
    """Base type for exchange related errors."""

    pass


class BadEntityIdError(ExchangeError):
    """Entity associated with the identifier is unknown."""

    def __init__(self, uid: EntityId) -> None:
        super().__init__(f'Unknown identifier {uid}.')


class MailboxTerminatedError(ExchangeError):
    """Entity mailbox is terminated and cannot send or receive messages."""

    def __init__(self, uid: EntityId) -> None:
        super().__init__(f'Mailbox for {uid} has been terminated.')
