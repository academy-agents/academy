from __future__ import annotations

import dataclasses
from collections.abc import Awaitable
from collections.abc import Callable
from typing import Any

from academy.context import ActionContext
from academy.context import AgentContext


@dataclasses.dataclass(frozen=True)
class ActionCall:
    """Description of one action invocation dispatched by a runtime.

    Instances are passed to each
    [`ActionMiddleware`][academy.middleware.ActionMiddleware] configured on
    the [`RuntimeConfig`][academy.runtime.RuntimeConfig].

    Attributes:
        action: Name of the action being invoked.
        source_id: ID of the entity that requested the action.
        agent_id: ID of the agent executing the action.
        args: Tuple of positional arguments.
        kwargs: Dictionary of keyword arguments.
        tag: Unique tag of the request message that initiated this
            invocation, if the action was initiated by a message.
    """

    action: str
    agent_context: AgentContext[Any]
    action_context: ActionContext
    args: tuple[Any, ...]
    kwargs: dict[str, Any]


ActionHandler = Callable[[ActionCall], Awaitable[Any]]
"""Signature of the next handler passed to an action middleware."""

ActionMiddleware = Callable[[ActionCall, ActionHandler], Awaitable[Any]]
"""Signature of an action middleware.

A middleware is an async callable invoked with the
[`ActionCall`][academy.middleware.ActionCall] and the next handler in the
chain. A middleware may inspect or validate the call before awaiting the
next handler, transform or record the result after, short-circuit by
returning without awaiting the next handler, or deny the call by raising
an exception. Exceptions raised by middleware propagate exactly like
exceptions raised by the action itself (i.e., returned to a remote caller
as a [`UserErrorResponse`][academy.message.UserErrorResponse]).
"""


def _chain_middleware(
    middleware: ActionMiddleware,
    next_handler: ActionHandler,
) -> ActionHandler:
    async def handler(call: ActionCall) -> Any:
        return await middleware(call, next_handler)

    return handler
