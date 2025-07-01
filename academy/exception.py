from __future__ import annotations

import sys
from collections.abc import Iterable
from typing import Any

from academy.identifier import AgentId
from academy.identifier import EntityId


class AgentNotInitializedError(Exception):
    """Agent runtime context has not been initialized.

    This error is typically raised when accessing the runtime context for
    an agent from within a behavior class before the agent has been executed.
    """

    def __init__(self) -> None:
        super().__init__(
            'Agent runtime context has not been initialized. '
            'Has the agent been started?',
        )


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

    An [`UnboundRemoteHandle`][academy.handle.UnboundRemoteHandle] is
    initialized with a target agent ID, but is not attached to an exchange
    client that the handle can use for communication.

    An unbound handle can be turned into a usable handle by binding it to
    an exchange client with
    [`UnboundRemoteHandle.bind_to_client()`][UnboundRemoteHandle.bind_to_client].
    """

    def __init__(self, aid: AgentId[Any]) -> None:
        super().__init__(
            f'Handle to {aid} is not bound to an exchange client. See the '
            'exception docstring for troubleshooting.',
        )


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
