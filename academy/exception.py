from __future__ import annotations

import sys
from collections.abc import Iterable
from typing import Any

from academy.identifier import AgentId
from academy.identifier import EntityId


class BadEntityIdError(Exception):
    """Entity associated with the identifier is unknown."""

    def __init__(self, uid: EntityId) -> None:
        super().__init__(f'Unknown identifier {uid}.')


class HandleClosedError(Exception):
    """Agent handle has been closed."""

    def __init__(
        self,
        agent_id: AgentId[Any],
        client_id: EntityId | None,
    ) -> None:
        message = (
            f'Handle to {agent_id} bound to {client_id} has been closed.'
            if client_id is not None
            else f'Handle to {agent_id} has been closed.'
        )
        super().__init__(message)


class HandleNotBoundError(Exception):
    """Handle to agent is in an unbound state.

    An unbound handle (typically, an instance of `UnboundRemoteHandle`) is
    initialized with a target agent ID and exchange, but does not have an
    identifier itself. Thus, the handle does not have a mailbox in the exchange
    to receive response messages.

    A handle must be bound to be used, either as a unique user program with its
    own mailbox or as bound to a running agent where it shares a mailbox with
    that running agent. To create a bound handle, use
    `handle.bind_to_client()`.

    Any agent behavior that has a handle to another agent as an instance
    attribute will be automatically bound to the agent when the agent begins
    running.
    """

    def __init__(self, aid: AgentId[Any]) -> None:
        super().__init__(
            f'Handle to {aid} to an exchange. See the exception docstring '
            'for troubleshooting.',
        )


class MailboxClosedError(Exception):
    """Mailbox is closed and cannot send or receive messages."""

    def __init__(self, uid: EntityId) -> None:
        super().__init__(f'Mailbox for {uid} has been closed.')


def raise_exceptions(
    exceptions: Iterable[BaseException],
    *,
    message: str | None = None,
) -> None:
    """Raise exceptions as a group.

    Raises a set of exceptions as an [`ExceptionGroup`][ExceptionGroup]
    in Python 3.11 and later. If only one exception is provided, it is raised
    directly. In Python 3.10 and older, only one exception is raised.

    This is a no-op if the size of `exceptions` is zero.

    Args:
        exceptions: An iterable of exceptions to raise.
        message: Custom error message for the exception group.
    """
    excs = tuple(exceptions)
    if len(excs) == 0:
        return

    if sys.version_info >= (3, 11) and len(excs) > 1:  # pragma: >=3.11 cover
        message = (
            message if message is not None else 'Caught multiple exceptions!'
        )
        # Note that BaseExceptionGroup will return ExceptionGroup if all
        # of the errors are Exception, rather than BaseException, so that this
        # can be caught by "except Exception".
        raise BaseExceptionGroup(message, excs)  # noqa: F821
    else:  # pragma: <3.11 cover
        raise excs[0]
