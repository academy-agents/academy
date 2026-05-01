# ruff: noqa: D102
from __future__ import annotations

import asyncio
import logging
import sys
import time
import uuid
from collections.abc import AsyncGenerator
from typing import Any
from typing import Generic
from typing import Literal
from typing import TYPE_CHECKING

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from aiologic import Lock
from culsans import AsyncQueue
from culsans import AsyncQueueShutDown as QueueShutDown
from culsans import Queue
from pydantic import BaseModel
from pydantic import Field

from academy.exception import BadEntityIdError
from academy.exception import MailboxTerminatedError
from academy.exchange.factory import ExchangeFactory
from academy.exchange.transport import _respond_pending_requests_on_terminate
from academy.exchange.transport import ExchangeTransportMixin
from academy.exchange.transport import MailboxStatus
from academy.identifier import AgentId
from academy.identifier import EntityId
from academy.identifier import UserId
from academy.message import Header
from academy.message import Message
from academy.serialize import NoPickleMixin

if TYPE_CHECKING:
    from academy.agent import Agent
    from academy.agent import AgentT
else:
    from academy.identifier import AgentT

logger = logging.getLogger(__name__)


class _LocalExchangeState(NoPickleMixin):
    """Local process message exchange.

    LocalExchange is an exchange where the mailboxes of the exchange live
    in a single process's memory and is only accessible to clients and
    agents running in that process.

    This class stores the state of the exchange.
    """

    def __init__(self) -> None:
        self.queues: dict[EntityId, AsyncQueue[Message[Any]]] = {}
        self.locks: dict[EntityId, Lock] = {}
        self.agents: dict[AgentId[Any], type[Agent]] = {}
        self.requests: dict[EntityId, dict[uuid.UUID, Header]] = {}
        self.last_active: dict[EntityId, float] = {}


class LocalAgentRegistration(BaseModel, Generic[AgentT]):
    """Agent registration for local exchanges."""

    agent_id: AgentId[AgentT]
    """Unique identifier for the agent created by the exchange."""

    exchange_type: Literal['local'] = Field('local', repr=False)


class LocalExchangeTransport(ExchangeTransportMixin, NoPickleMixin):
    """Local exchange client bound to a specific mailbox."""

    def __init__(
        self,
        mailbox_id: EntityId,
        state: _LocalExchangeState,
    ) -> None:
        self._mailbox_id = mailbox_id
        self._state = state

    @classmethod
    def new(
        cls,
        mailbox_id: EntityId | None = None,
        *,
        name: str | None = None,
        state: _LocalExchangeState,
    ) -> Self:
        """Instantiate a new transport.

        Args:
            mailbox_id: Bind the transport to the specific mailbox. If `None`,
                a new user entity will be registered and the transport will be
                bound to that mailbox.
            name: Display name of the redistered entity if `mailbox_id` is
                `None`.
            state: Shared state among exchange clients.

        Returns:
            An instantiated transport bound to a specific mailbox.
        """
        if mailbox_id is None:
            mailbox_id = UserId.new(name=name)
            state.queues[mailbox_id] = Queue().async_q
            state.locks[mailbox_id] = Lock()
            logger.info(
                'Registered %s in exchange',
                mailbox_id,
                extra={'academy.mailbox_id': mailbox_id},
            )
        return cls(mailbox_id, state)

    @property
    def mailbox_id(self) -> EntityId:
        return self._mailbox_id

    async def close(self) -> None:
        pass

    async def discover(
        self,
        agent: type[Agent] | str,
        *,
        allow_subclasses: bool = True,
    ) -> tuple[AgentId[Any], ...]:
        found: list[AgentId[Any]] = []
        for aid, type_ in self._state.agents.items():
            if isinstance(agent, str):
                mro = type_._agent_mro()
                if agent == mro[0] or (allow_subclasses and agent in mro):
                    found.append(aid)
            elif agent is type_ or (
                allow_subclasses and issubclass(type_, agent)
            ):
                found.append(aid)

        alive = tuple(
            aid for aid in found if not self._state.queues[aid].is_shutdown
        )
        return alive

    def factory(self) -> LocalExchangeFactory:
        return LocalExchangeFactory(_state=self._state)

    async def _recv(self, timeout: float | None = None) -> Message[Any]:
        queue = self._state.queues[self.mailbox_id]
        try:
            return await asyncio.wait_for(queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            raise TimeoutError(
                f'Timeout waiting for next message for {self.mailbox_id} '
                f'after {timeout} seconds.',
            ) from None
        except QueueShutDown:
            raise MailboxTerminatedError(self.mailbox_id) from None

    async def listen(
        self,
        timeout: float | None = None,
    ) -> AsyncGenerator[Message[Any]]:
        while True:
            yield await self._recv(timeout)

    async def register_agent(
        self,
        agent: type[AgentT],
        *,
        name: str | None = None,
    ) -> LocalAgentRegistration[AgentT]:
        aid: AgentId[AgentT] = AgentId.new(name=name)
        self._state.queues[aid] = Queue().async_q
        self._state.locks[aid] = Lock()
        self._state.agents[aid] = agent
        return LocalAgentRegistration(agent_id=aid)

    async def send(self, message: Message[Any]) -> None:
        queue = self._state.queues.get(message.dest, None)
        if queue is None:
            raise BadEntityIdError(message.dest)

        if message.is_request():
            self._state.requests.setdefault(message.dest, {})[message.tag] = (
                message.header
            )
            logger.debug(
                'Tracking in-flight request: tag=%s src=%s dest=%s',
                message.tag,
                message.src,
                message.dest,
                extra={
                    'academy.message_tag': message.tag,
                    'academy.src': message.src,
                    'academy.dest': message.dest,
                },
            )
        elif (
            message.src in self._state.requests
            and message.tag in self._state.requests[message.src]
        ):
            tracked = self._state.requests[message.src].pop(message.tag)
            if not self._state.requests[message.src]:
                del self._state.requests[message.src]
            logger.debug(
                'Response received for in-flight request: '
                'tag=%s src=%s dest=%s',
                tracked.tag,
                tracked.src,
                tracked.dest,
                extra={
                    'academy.message_tag': tracked.tag,
                    'academy.src': tracked.src,
                    'academy.dest': tracked.dest,
                },
            )
        else:
            logger.warning(
                'Response received without corresponding request: '
                'tag=%s src=%s dest=%s',
                message.tag,
                message.src,
                message.dest,
                extra={
                    'academy.message_tag': message.tag,
                    'academy.src': message.src,
                    'academy.dest': message.dest,
                },
            )

        async with self._state.locks[message.dest]:
            try:
                await queue.put(message)
            except QueueShutDown:
                raise MailboxTerminatedError(message.dest) from None

    async def status(self, uid: EntityId) -> MailboxStatus:
        if uid not in self._state.queues:
            return MailboxStatus.MISSING
        async with self._state.locks[uid]:
            if self._state.queues[uid].is_shutdown:
                return MailboxStatus.TERMINATED
            return MailboxStatus.ACTIVE

    async def terminate(self, uid: EntityId) -> None:
        queue = self._state.queues.get(uid, None)
        if queue is None:
            return

        async with self._state.locks[uid]:
            if queue.is_shutdown:
                return

            if isinstance(uid, AgentId):
                self._state.agents.pop(uid, None)
            pending_requests = list(
                self._state.requests.pop(uid, {}).values(),
            )
            await _respond_pending_requests_on_terminate(
                pending_requests,
                self.send,
            )
            queue.shutdown(immediate=True)

    async def update_heartbeat(self) -> None:
        self._state.last_active[self.mailbox_id] = time.time()

    async def heartbeat_status(self, uid: EntityId) -> float | None:
        if uid not in self._state.queues:
            raise BadEntityIdError(uid)

        if self._state.last_active.get(uid) is None:
            return None
        else:
            return time.time() - self._state.last_active[uid]


class LocalExchangeFactory(
    ExchangeFactory[LocalExchangeTransport],
    NoPickleMixin,
):
    """Local exchange client factory.

    A local exchange can be used to pass messages between agents running
    in a single process.
    """

    def __init__(
        self,
        *,
        _state: _LocalExchangeState | None = None,
    ):
        self._state = _LocalExchangeState() if _state is None else _state

    async def _create_transport(
        self,
        mailbox_id: EntityId | None = None,
        *,
        name: str | None = None,
        registration: LocalAgentRegistration[Any] | None = None,  # type: ignore[override]
    ) -> LocalExchangeTransport:
        return LocalExchangeTransport.new(
            mailbox_id,
            name=name,
            state=self._state,
        )
