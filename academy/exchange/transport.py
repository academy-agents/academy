from __future__ import annotations

import abc
import enum
import sys
from types import TracebackType
from typing import Any
from typing import Protocol
from typing import runtime_checkable
from typing import TYPE_CHECKING
from typing import TypeVar

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from academy.behavior import Behavior
from academy.behavior import BehaviorT
from academy.identifier import AgentId
from academy.identifier import EntityId
from academy.message import Message

if TYPE_CHECKING:
    from academy.exchange import ExchangeFactory


class MailboxStatus(enum.Enum):
    """Exchange mailbox status."""

    MISSING = 'MISSING'
    """Mailbox does not exist."""
    ACTIVE = 'ACTIVE'
    """Mailbox exists and is accepting messages."""
    TERMINATED = 'TERMINATED'
    """Mailbox was terminated and no longer accepts messages."""


@runtime_checkable
class AgentRegistration(Protocol[BehaviorT]):
    """Agent exchange registration information.

    Attributes:
        agent_id: Unique agent identifier returned by the exchange.
    """

    agent_id: AgentId[BehaviorT]


AgentRegistrationT = TypeVar(
    'AgentRegistrationT',
    bound=AgentRegistration[Any],
)
"""Type variable bound [`AgentRegistration`][academy.exchange.transport.AgentRegistration]."""  # noqa: E501
AgentRegistrationT_co = TypeVar('AgentRegistrationT_co', covariant=True)


@runtime_checkable
class ExchangeTransport(Protocol[AgentRegistrationT_co]):
    """Low-level exchange communicator.

    A message exchange hosts mailboxes for each entity (i.e., agent or
    user) in a multi-agent system. This transport protocol defines mechanisms
    for entity management (e.g., registration, discovery, status, termination)
    and for sending/receiving messages from a mailbox. As such, each transport
    instance is "bound" to a specific mailbox in the exchange.

    Warning:
        A specific exchange transport should not be replicated because multiple
        client instances receiving from the same mailbox produces undefined
        behavior.
    """

    @property
    @abc.abstractmethod
    def mailbox_id(self) -> EntityId:
        """ID of the mailbox this client is bound to."""
        ...

    @abc.abstractmethod
    def close(self) -> None:
        """Close the exchange client.

        Note:
            This does not alter the state of the mailbox this client is bound
            to. I.e., the mailbox will not be terminated.
        """
        ...

    @abc.abstractmethod
    def discover(
        self,
        behavior: type[Behavior],
        *,
        allow_subclasses: bool = True,
    ) -> tuple[AgentId[Any], ...]:
        """Discover peer agents with a given behavior.

        Warning:
            Implementations of this method are often O(n) and scan the types
            of all agents registered to the exchange.

        Args:
            behavior: Behavior type of interest.
            allow_subclasses: Return agents implementing subclasses of the
                behavior.

        Returns:
            Tuple of agent IDs implementing the behavior.
        """
        ...

    @abc.abstractmethod
    def factory(self) -> ExchangeFactory[Self]:
        """Get an exchange factory."""
        ...

    @abc.abstractmethod
    def recv(self, timeout: float | None = None) -> Message:
        """Receive the next message sent to the mailbox.

        This blocks until the next message is received, there is a timeout, or
        the mailbox is terminated.

        Args:
            timeout: Optional timeout in seconds to wait for the next
                message. If `None`, the default, block forever until the
                next message or the mailbox is closed.

        Raises:
            MailboxClosedError: If the mailbox was closed.
            TimeoutError: If a `timeout` was specified and exceeded.
        """
        ...

    @abc.abstractmethod
    def register_agent(
        self,
        behavior: type[BehaviorT],
        *,
        name: str | None = None,
        # This is needed by a strange hack in academy/agent.py where we
        # close an agent mailbox and immediately re-register it. This will no
        # longer be needed after Issue #100 and can be removed.
        _agent_id: AgentId[BehaviorT] | None = None,
    ) -> AgentRegistrationT_co:
        """Register a new agent and associated mailbox with the exchange.

        Args:
            behavior: Behavior type of the agent.
            name: Optional display name for the agent.

        Returns:
            Agent registration info.
        """
        ...

    @abc.abstractmethod
    def send(self, message: Message) -> None:
        """Send a message to a mailbox.

        Args:
            message: Message to send.

        Raises:
            BadEntityIdError: If a mailbox for `message.dest` does not exist.
            MailboxClosedError: If the mailbox was closed.
        """
        ...

    @abc.abstractmethod
    def status(self, uid: EntityId) -> MailboxStatus:
        """Check the status of a mailbox in the exchange.

        Args:
            uid: Entity identifier of the mailbox to check.
        """
        ...

    @abc.abstractmethod
    def terminate(self, uid: EntityId) -> None:
        """Terminate a mailbox in the exchange.

        Terminating a mailbox means that the corresponding entity will no
        longer be able to receive messages.

        Note:
            This method is a no-op if the mailbox does not exist.

        Args:
            uid: Entity identifier of the mailbox to close.
        """
        ...


ExchangeTransportT = TypeVar(
    'ExchangeTransportT',
    bound=ExchangeTransport[Any],
)
"""Type variable bound [`ExchangeTransport`][academy.exchange.transport.ExchangeTransport]."""  # noqa: E501


class ExchangeTransportMixin:
    """Magic method mixin for exchange transport implementations.

    Adds `__repr__`, `__str__`, and context manager support.
    """

    def __repr__(self: ExchangeTransportT) -> str:
        return f'{type(self).__name__}({self.mailbox_id!r})'

    def __str__(self: ExchangeTransportT) -> str:
        return f'{type(self).__name__}<{self.mailbox_id}>'

    def __enter__(self: ExchangeTransportT) -> ExchangeTransportT:
        return self

    def __exit__(
        self: ExchangeTransportT,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        self.close()
