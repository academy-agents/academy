from __future__ import annotations

import abc
import logging
from collections.abc import Callable
from collections.abc import Coroutine
from typing import Any
from typing import Generic
from typing import TYPE_CHECKING
from typing import TypeAlias
from typing import TypeVar

from academy.exception import BadEntityIdError
from academy.exchange.client import AgentExchangeClient
from academy.exchange.client import UserExchangeClient
from academy.exchange.transport import AgentRegistration
from academy.exchange.transport import ExchangeTransportT
from academy.exchange.transport import MailboxStatus
from academy.identifier import AgentId
from academy.identifier import EntityId
from academy.identifier import UserId
from academy.message import Message
from academy.message import RequestT_co

if TYPE_CHECKING:
    from academy.agent import AgentT
else:
    AgentT = TypeVar('AgentT')

logger = logging.getLogger(__name__)

RequestHandler: TypeAlias = Callable[
    [Message[RequestT_co]],
    Coroutine[None, None, None],
]


class ExchangeFactory(abc.ABC, Generic[ExchangeTransportT]):
    """Exchange client factory.

    An exchange factory is used to mint new exchange clients for users and
    agents, encapsulating the complexities of instantiating the underlying
    communication classes (the
    [`ExchangeTransport`][academy.exchange.transport.ExchangeTransport]).

    Warning:
        Factory implementations must be efficiently pickleable because
        factory instances are shared between user and agent processes so
        that all entities can create clients to the same exchange.
    """

    @abc.abstractmethod
    async def _create_transport(
        self,
        mailbox_id: EntityId | None = None,
        *,
        name: str | None = None,
        registration: AgentRegistration[Any] | None = None,
    ) -> ExchangeTransportT: ...

    async def create_agent_client(
        self,
        registration: AgentRegistration[AgentT],
        request_handler: RequestHandler[RequestT_co],
    ) -> AgentExchangeClient[AgentT, ExchangeTransportT]:
        """Create a new agent exchange client.

        An agent must be registered with the exchange before an exchange
        client can be created. For example:
        ```python
        factory = ExchangeFactory(...)
        user_client = factory.create_user_client()
        registration = user_client.register_agent(...)
        agent_client = factory.create_agent_client(registration, ...)
        ```

        Args:
            registration: Registration information returned by the exchange.
            request_handler: Agent request message handler.

        Returns:
            Agent exchange client.

        Raises:
            BadEntityIdError: If an agent with `registration.agent_id` is not
                already registered with the exchange.
        """
        agent_id: AgentId[AgentT] = registration.agent_id
        transport = await self._create_transport(
            mailbox_id=agent_id,
            registration=registration,
        )
        assert transport.mailbox_id == agent_id
        status = await transport.status(agent_id)
        if status != MailboxStatus.ACTIVE:
            await transport.close()
            raise BadEntityIdError(agent_id)
        return AgentExchangeClient(
            agent_id,
            transport,
            request_handler=request_handler,
        )

    async def create_user_client(
        self,
        *,
        name: str | None = None,
        start_listener: bool = True,
    ) -> UserExchangeClient[ExchangeTransportT]:
        """Create a new user in the exchange and associated client.

        Args:
            name: Display name of the client on the exchange.
            start_listener: Start a message listener thread.

        Returns:
            User exchange client.
        """
        transport = await self._create_transport(mailbox_id=None, name=name)
        user_id = transport.mailbox_id
        assert isinstance(user_id, UserId)
        return UserExchangeClient(
            user_id,
            transport,
            start_listener=start_listener,
        )
